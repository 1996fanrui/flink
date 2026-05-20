# Code Review Checklist

> 设计文档参考：`requirements/38544/simplify_approach/{overview,input_channel,unspiller,coordination,task_breakdown}.md`
>
> 每项 = 一个**功能点 / 不变量**：描述设计期望 + review 代码时要确认的点。每个功能点下的 bullets 最多 10 条。`(命中过)` 标记已经被 round1–round5 bug 命中过的项。

---

## A. Filter 阶段

### A1. Filter 期间内存 bounded（FLINK-38544 的初衷）

期望：filter 期间内存只持 prefilter + postfilter 各一个 buffer；master 的 `MemorySegmentFactory.allocateUnpooledSegment` heap fallback 必须删除。

Review 要点：
- grep `allocateUnpooledSegment`：filter 路径上不应再调。
- `RecoveredInputChannel.requestBufferBlocking` 只剩 `bufferManager.requestBufferBlocking()` 一条路。
- prefilter buffer = `RecoveredChannelStateHandler.preFilterSegment`，reusable 单实例。
- postfilter buffer = `FilteredBufferWriter.outputBuffer`，从 exclusive pool 取一次，no-op recycler 防止中途归还。
- filter 结束（handler `close()`）prefilter / postfilter 都还给 pool。

### A2. FilteredBufferWriter — 一个 entry 只装一个 channel（命中过）

期望：每个 entry 只装一个 channel；flush 触发点仅 (i) channel 切换 (ii) buffer 满。

Review 要点：
- `beginChannel`：在 `currentChannel != channelInfo && outputBuffer.getSize() > 0` 时 flush。
- `requestBufferBlocking`：buffer 满时 flush，flush 后保留 `currentChannel`。
- `flush()` 写完重置 size / readerIndex 到 0、`currentChannel = null`。
- 没有第三个 flush 触发点（`spillFile.append` 的 caller 仅 `flush()`）。

### A3. Filter 写 spillFile 用 NEW channelInfo（命中过）

期望：`accumulator.beginChannel` 用 NEW（mapped）channelInfo；filter 内部寻址虚拟通道继续用 OLD index。

Review 要点：
- `accumulator.beginChannel(...)` 入参 = `channel.getChannelInfo()`（NEW），不是入参 `channelInfo`（OLD）。
- `filterAndRewrite(channelInfo.getGateIdx(), oldSubtaskIndex, channelInfo.getInputChannelIdx(), ...)` 继续用 OLD。
- 其他用 `channelInfo` 做 spillFile / 物理 channel lookup key 的位置都要确认方向。
- 非 rescale 路径 OLD == NEW，必须 trace rescale upscale/downscale/rebalance 路径。

---

## B. SpillFile 本身

### B1. SpillFile 引用计数 + 生命周期（命中过）

期望：drain + 每个 cpId Step 3 reader 各持一份；producer 也持初始 grant 直到 reader 接手（避免 refCount=0 窗口）；refCount 归零 → `deleteAllSegments`。

Review 要点：
- grep `spillFile.release()` / `.acquire()`：每处必须配对。
- producer acquire 在 `RecoveredChannelStateHandler.ensureSpillFileWriter`；release 在 `StreamTask.handoffSpillReaderToDrain` finally。
- 失败路径 producer release：filter throws / conversion throws / buildReader throws 每条都得有。
- Step 3 reader release 挂到 `getAndRemoveWriteResult(cpId).getInputChannelStateHandles()`；abort 路径同样。
- `deleteAllSegments` 由 `cleanedUp` CAS 守护，二次进入是 no-op。

### B2. SpillFile.close() 是 forced cleanup（production 禁用）

期望：`SpillFile.close()` 强制清理（绕过 refCount）；production filter→drain 路径绝不调用。

Review 要点：
- grep `spillFile.close()`：production 代码里出现一律可疑。
- `FilteredBufferWriter.close()` / `SpillFileWriter.close()` 只 flush 或 `release()`，不能调 `close()`。
- 唯一允许：test tearDown / task-shutdown 兜底；调用点要有 inline 注释说明。
- forced close 能容忍 ref 未归零（CAS 保证 deleteAllSegments 不重复）。

### B3. SpillFile segment 轮转 + append 单写者

期望：`channelIOExecutor` 是唯一 writer；单 segment 顺序 append；超 64MB 轮转；一条 entry 不跨 segment。

Review 要点：
- `SpillFile.append` 不允许加锁（加锁即代表不再相信 single-writer 假设）。
- 单条 payload < segment 上限；`activeSegmentFor(length)` 必须在写入前预留够空间。
- `Entry(channelInfo, segmentIndex, offsetBeforeWrite, length)` 反映**写入前**状态；写入后才更新 `currentEnd`。
- 写入 FileChannel 不被 read 共用；drain 用 `FileChannel.open(path, READ)` 开独立 read handle。

---

## C. Drain 阶段

### C1. SpillFileReader.lock 两条铁则（Principle 1 + 2）

期望：Principle 1 — 所有 recovery-side 对 `recoveredBuffers` 的写在 lock 内（drain + Step 1），end-of-drain `finishReadRecoveredState` 例外。Principle 2 — `currentSegmentIndex/Offset` 推进和 `onRecoveredStateBuffer` 在**同一** sync block。

Review 要点：
- lock 是 named `private final Object lock = new Object();`，禁止 `synchronized(this)`。
- grep `currentSegmentIndex =` / `currentOffset =`：每处在 `synchronized(lock)` 内且同块有 `onRecoveredStateBuffer`。
- grep `onRecoveredStateBuffer(`：drain + Step 1 必须在 lock 内；唯一例外 `RecoveredInputChannel.toInputChannel()` 单线程迁移。
- 锁顺序：`SpillFileReader.lock` → channel monitor，不能反向。
- lock 不能跨 IO / park / blocking call。

### C2. Drain 4 步结构

期望：(A) requestBuffer lock 外；(B) readBytes lock 外；(C) deliver + advance offset 同一 lock 内；(D) finishReadRecoveredState lock 外。

Review 要点：
- (A) (B) 慢操作不能进 lock。
- (C) 4 个动作必须完全在同一 sync block 内。
- 异常路径：(A) 申请到的 buf 在 (B)/(C) 抛错时必须 recycle。
- (D) 必须对 `allChannels` 每个元素都调一次。
- (D) 在 lock 外（end-of-drain 例外）。

### C3. End-of-drain finishReadRecoveredState 必调（含异常路径）

期望：drain 中途抛错时每个 channel 都必须收到 `finishReadRecoveredState()`，否则 `stateConsumedFuture` 不完成，下游 hang。

Review 要点：
- drain 主循环外 try/finally；finally 内对 `allChannels` 全部调。
- 不允许"只对处理过的 channel 调"（rescale 下有些 channel 没收 entry 但仍属本 task）。
- 单 channel finishReadRecoveredState 抛不能中断后续 channel 调用（swallow 或 suppressed）。
- task cancel 路径也得跑。
- (D) 后不要再插逻辑等什么。

---

## D. Filter→Drain 衔接

### D1. 单次 channelIOExecutor submit + drainHandoff（命中过）

期望：recovery 期间 `channelIOExecutor.execute` 只调一次；runnable 内连续 filter → `drainHandoff.get()` → drain → close。

Review 要点：
- grep `channelIOExecutor.execute(`：recovery 期间只能 1 处。
- runnable 内不再 submit 另一个 drain runnable。
- `drainHandoff.get()` 必须能响应 InterruptedException。
- filter-off 路径：跑完 filter 就 return，不进 wait。
- 不需要"延后"或"条件化" shutdown line 862；任何改动需 review 动机。

### D2. drainHandoff 在所有路径必须 complete（防御性）

期望：drainHandoff 在每条退出路径都被 complete（`complete(reader)` / `complete(null)` / `completeExceptionally`）。

Review 要点：
- handoff 正常：`complete(spillReader)` 或 `complete(null)`（无 spillFile）。
- handoff catch：`completeExceptionally(t)` + finally 释放 producer grant，两个动作不能互相吞。
- `allConverted.whenComplete` err 分支：`completeExceptionally(err)` + 释放 producer grant。
- filter 抛异常路径（runnable catch）：`completeExceptionally(e)` + 释放 producer grant。
- task cancel 路径：显式 completeExceptionally 或依赖中断，不能两条都没有。

### D3. Conversion → drain 严格串行（non-negotiable invariant）

期望：filter → conversion → drain 严格串行；drain 开始时所有 channel 已是物理 channel。

Review 要点：
- 时序链：bufferFilteringCompleteFuture → `requestPartitions` mail → conversionDoneFutures → handoff 构造 SpillFileReader → drainHandoff → drain。
- drain 时 `SpillFileReader.allChannels` 必须都是物理 channel。
- filter 时 gate 中 channel 仍是 RecoveredInputChannel；filter 内部不允许触发 conversion。
- 不允许"先 drain 一部分、再 conversion、再 drain"的交错。
- `requestPartitions` 不允许延后到 drain 完。

---

## E. InputChannel 侧

### E1. RecoverableInputChannel 接口 + 不向下 cast

期望：drain 通过接口（`onRecoveredStateBuffer` + `finishReadRecoveredState`）访问 channel，永不 cast。

Review 要点：
- grep `instanceof RecoveredInputChannel` / `LocalInputChannel` / `RemoteInputChannel`：drain 路径一律不应出现。
- `SpillFileReader.channelByInfo` value 类型必须是接口。
- `RecoveredChannelBufferRequester` 持 `RecoveredInputChannel`（filter 期间未转换的源 channel）允许。
- `RecoveredInputChannel.toInputChannel()` 迁移走同一接口入口。
- "判类型走分支"是回退。

### E2. recoveredBuffers 队列只装 drain buffer 和 RecoveryCheckpointBarrier

期望：`recoveredBuffers` 只装两类；recovery 结束后永久空；`toBeConsumedBuffers`（Local）/`receivedBuffers`（Remote）不混 recovery 数据。

Review 要点：
- grep `toBeConsumedBuffers`：Local 中只装 `FullyFilledBuffer` splits。
- LocalInputChannel 构造函数不应再有 `ArrayDeque<Buffer> initialRecoveredBuffers`（FLINK-39018 d1914c63c95 入口）。
- LocalInputChannel.`getNextBuffer` 的 `toBeConsumedBuffers` 分支退回 master 形式。
- `getNextRecoveredBuffer()` 独立方法应已删除。
- `checkpointStarted` 不能扫 `toBeConsumedBuffers` 取 recovery；回归 master `startPersisting(barrier.getId(), emptyList)`。

### E3. recoveredBuffers 锁复用 + 锁顺序

期望：Remote 复用 `synchronized(receivedBuffers)`；Local 用 `synchronized(recoveredBuffers)` 自身；不引入第三个 lock。

Review 要点：
- grep `synchronized(`：channel 路径只见 `receivedBuffers`（Remote）/ `recoveredBuffers`（Local）。
- Local 不能引入新 lock 字段。
- 锁顺序：所有 path 都按 outer→inner（`SpillFileReader.lock` → channel monitor）。
- monitor 内 critical section 不允许 park / IO / sleep。
- `onRecoveredStateBuffer` 内 channel monitor 必须在 `SpillFileReader.lock` 已持有的前提下进入。

### E4. allRecoveredBuffersDelivered 字段

期望：false→true 一次性翻转；只在 `finishReadRecoveredState()` 内；表达 producer 完成，**不**表达 consumer 完成。

Review 要点：
- grep `allRecoveredBuffersDelivered = true`：只有 1 处（`finishReadRecoveredState`）。
- `= false` 不应出现（除字段声明默认值）。
- 翻转必须在 channel internal monitor 内（happens-before 给 consumer）。
- 是 boolean plain field，不允许 `volatile`。
- 不允许 recovery 完成后重置。

### E5. inRecovery 判定 + getNextBuffer 双路径

期望：`inRecovery = !allRecoveredBuffersDelivered || !recoveredBuffers.isEmpty()`；in-recovery 时先 priority event，否则 `recoveredBuffers.poll()`，否则 empty。

Review 要点：
- 该 predicate 在 `getNextBuffer` 和 `checkpointStarted` 两处用，必须同表达式（最好抽 helper）。
- 不允许只判 `recoveredBuffers.isEmpty()` 或只判 `!allRecoveredBuffersDelivered`（漏 boundary）。
- in-recovery 期间从 upstream 只拉 priority event：Remote 经 `addPriorityBuffer`，Local 经 `subpartitionView` priority 入口；普通 data 必须屏蔽。
- 返回 empty 时正确通知上层。

### E6. stateConsumedFuture 精确完成条件

期望：iff `allRecoveredBuffersDelivered && recoveredBuffers.isEmpty()`；触发位是"使两者最后都成立的那次状态变更"。

Review 要点：
- 触发位仅两条：(a) `finishReadRecoveredState()` 跑到时 queue 已空；(b) consumer poll 走最后一条时 flag 已为 true。
- 不允许 EOICS sentinel 入队触发。
- 触发幂等（`CompletableFuture.complete` 自身保证），但语义上"第一个真值"决定。
- 不允许其他位置兜底完成（如 task close）—— 掩盖 producer 漏调 finishReadRecoveredState。
- review 所有 `stateConsumedFuture.complete` 位置确认与设计一致。

---

## F. Checkpoint 3-step 协议

### F1. 3-step 调度器入口（仅两处 UC 入口）

期望：仅在两处 UC barrier 入口调 `onCheckpointStartedForAllInputs`。

Review 要点：
- grep `onCheckpointStartedForAllInputs(`：调用点只能是 `AlternatingWaitingForFirstBarrierUnaligned.barrierReceived` + `AlternatingCollectingBarriers.barrierReceived`。
- 不允许在其他 trigger 入口调（aligned barrier / 主路径不走 3-step）。
- 两处之外的"补充"调用都是 red flag。
- 反过来：这两处必须调；漏掉则 cpId 永远走不了 3-step。

### F2. 调度器内部顺序 + no-op 收敛（无外层 if filter-on）

期望：(Step 1) `snapshotAndInsertBarriers` → (master per-gate iteration) → (Step 3) `addInputDataFromSpill`；feature-off / 全部完成时 trigger 返回空 snap + 不插 barrier，writer no-op；无外层 if filter-on。

Review 要点：
- 调度器无 `if (recoveryCheckpointTrigger != null)` / `if (filterEnabled)` 外层 guard。
- `RecoveryCheckpointTrigger` 默认 no-op 实现返回空 `DiskSnapshot` + 不动 channel。
- `addInputDataFromSpill` writer 端识别空 snap → no-op，不下发 demux。
- 调度器不自写 Step 2 outer loop（Step 2 嵌入 `channel.checkpointStarted`）。
- Step 1 必须先；Step 2 / Step 3 推荐线性形式（Step 2 先 Step 3 后）。

### F3. checkpointStarted 互斥分支（in-recovery vs not-in-recovery）

期望：channel monitor 内根据 inRecovery 走互斥分支；in-recovery 走 `recoveredBuffers` walk，not-in-recovery 走 master `receivedBuffers` persistence。

Review 要点：
- if/else 真互斥；"两个分支都跑一部分"是 bug。
- in-recovery 分支：`b.retainBuffer()` retain，writer iterator 在结束时 `recycleBuffer`。
- in-recovery 分支必须 `it.remove()` 移除 barrier（参见 F5）。
- defensive `receivedBuffersHasNoLiveDataBuffer()`：Remote 遍历 `!isBuffer()`；Local 直接 true。
- not-in-recovery 分支是 master 原样，不能加 recovery 残留。

### F4. checkpointStarted 必须找到 cpId-matched barrier（防御性）

期望：In-recovery 分支必须找到 cpId 匹配的 `RecoveryCheckpointBarrier`；找不到必须抛。

Review 要点：
- walk 退出区分"找到后 break"和"走到尾没找到"；后者必须 throw。
- 队列空但 inRecovery 仍 true → 同样 throw（Step 1 已插过）。
- 找到 barrier 但 cpId 不匹配 → retain 不误移除，继续找；全无匹配 → throw。
- 必须 throw 不能用 assert（assert 是 default-off 的）。
- 不允许"找不到当成空 retained 喂 writer"的 silent fallback。

### F5. RecoveryCheckpointBarrier sentinel 不泄漏

期望：仅 task thread Step 1 插入 + Step 2 remove；operator 看不到；barrier 是 `!isBuffer()` event；带 `getCheckpointId()`。

Review 要点：
- grep `RecoveryCheckpointBarrier`：仅出现在 Step 1 / Step 2 / 类定义本身。
- `getNextBuffer` / network / deser / operator chain 不应有 `instanceof RecoveryCheckpointBarrier`。
- `isBuffer() == false`；如果误设为 true，barrier 被当 data 喂 deser → byte 错乱。
- Step 2 中 `it.remove()` 必须真移除（不仅 break）。
- 跨 cpId barrier 互不影响（按 cpId 匹配）。

### F6. Step 3 — addInputDataFromSpill 写入正确性

期望：异步 demux 按 `chunk.channelInfo` 写到对应 channel output；跳过 `entryPos < startPos`；空 snap no-op。

Review 要点：
- writer 端按 `chunk.channelInfo` 分派，不靠 chunk 顺序推断。
- 跳过 `entryPos < startPos`：按 `(segmentIndex, offset)` 字典序，先 segmentIndex 再 offset。
- 异步 writer 执行不依赖 task thread 活着；回调能独立完成（异常完成 future 仍触发）。
- DiskSnapshot 持有的 SpillFile grant 在 writer 完成回调里 release；short-circuit 退出路径也得释放。

---

## G. 防御性 / 一致性

### G1. 防御性：fail-loud not silent

期望：设计保证"必然能做到"的事情，做不到必须抛，不允许 silent skip。

Review 要点：
- F4 已点 checkpointStarted 找 barrier。
- `RecoveredChannelStateHandler.recover` 的 `getMappedChannels` —— null 必须抛。
- `ChannelStateFilteringHandler.filterAndRewrite` 找 `VirtualChannel` —— 找不到必须抛（master 既有）。
- `SpillFileReader.drain` 找 `channelByInfo.get(e.channelInfo)` —— 找不到必须抛。
- buffer pool 满时不要回 heap fallback；fail-loud → park。
- 任何 `catch (Exception ignored) {}` 一律可疑。

### G2. 异常路径资源不能 leak

期望：drain / filter / handoff / 3-step / writer 任何路径抛异常时已 acquire 的资源都必须释放。

Review 要点：
- filter 路径：`filterAndRewrite` 内已有 buffer 回收 try/catch，review 是否完整。
- handoffSpillReaderToDrain：buildReader 抛 → finally 释放 producer grant + drainHandoff completeExceptionally。
- drain 主循环：(A) 申请到 buf 后 (B)/(C) 抛 → buf 必须 recycle。
- 3-step 调度器 Step 1/2/3 任一抛 → SpillFile grant + retain buffer 释放，不允许整段 try ignore。
- task cancel 路径 channelIOExecutor runnable 中断 → drain 中的 buf / SpillFile grant 是否 leak，专门 trace。

### G3. Future / latch / counter 在所有路径一致

期望：跨线程同步对象在所有可达终态都保持一致；漏完成 hang，多次完成静默错误。

Review 要点：
- `drainHandoff`（见 D2）。
- `bufferFilteringCompleteFuture`：filter 抛错时是否仍 complete（review 改动有没有破坏 master 既有路径）。
- `conversionDoneFutures[i]`：每个 gate 都要 complete（成功或异常）；mailbox mail 没排到呢？
- `allRecoveredBuffersDelivered`（见 E4）。
- `stateConsumedFuture`（见 E6）；不允许多位置同时 complete（语义上"第一个真值"决定）。
- `SpillFile.refCount`（见 B1）；任何 acquire 必须配 release（happy + 异常）。

### G4. Feature-off 路径零 overhead

期望：feature-off 时整个流程等同 master，不进 spillFile / 不构造 SpillFileReader / 不进 3-step 任务级分支。

Review 要点：
- filter-off 路径 `RecoveredChannelStateHandler` 不构造 `SpillFileWriter`（字段保持 null）。
- `getProducedSpillFile()` 返回 null → handoff 早 return → `drainHandoff.complete(null)` → I/O 线程拿 null 直接 return。
- `RecoveryCheckpointTrigger` 默认 no-op + `addInputDataFromSpill` 空 snap no-op 让"无外层 if"在 feature-off 时不产生 overhead。
- `channelIOExecutor` 行为与 master 一致（feature 没开不能跑 wait-blocking runnable）。
