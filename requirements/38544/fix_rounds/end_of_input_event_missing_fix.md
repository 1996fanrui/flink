# EndOfInputChannelStateEvent missing on `isCheckpointingDuringRecoveryEnabled=true` 路径

> 关联：[hang_evidence.md](./hang_evidence.md)（事实证据）、[recovered_buffer_queue_refactor.md](./recovered_buffer_queue_refactor.md)（重构提案）

## 1. Bug 一句话

**当 `inputGate.isCheckpointingDuringRecoveryEnabled() == true` 时，物理 `LocalInputChannel` / `RemoteInputChannel` 转换后的 `recoveredQueue` 永远不会拿到 `EndOfInputChannelStateEvent` sentinel**，导致 fresh-start（无 recovery 状态）场景下：
- `recoveredQueue` 自始至终为空（`totalOffered=0`）
- `finishRecoveredBufferDelivery()` 翻完 `allDelivered=true` 之后没人 `notifyChannelNonEmpty()`，gate 永远等不到 channel 上的数据可用通知
- 上游 subpartition 里堆着 14 个 buffer，下游 sink 永远不来拉

## 2. 直接证据（来自 hang_evidence.md）

| 事件 | 证据 |
|---|---|
| `RecoveredInputChannel.finishReadRecoveredState` 9 次都 push 过 sentinel | log: 9 行 `[HANG-DIAG] RecoveredInputChannel.finishReadRecoveredState ... receivedBuffers.sizeBefore=0`（push 前 size，push 后 size=1） |
| `toInputChannel` 抓取时 sentinel 已经消失 | log: 9 行 `[HANG-DIAG] RecoveredInputChannel.toInputChannel ... remainingBuffers.size=0` |
| sentinel 在 RecoveredInputChannel 内部就被消费 | 代码：`RecoveredInputChannel.getNextRecoveredStateBuffer`（line 240-250）发现 sentinel 就 `stateConsumedFuture.complete(null)` 并 `return null` |
| `LocalInputChannel.recoveredQueue.offer` 整轮 0 次 | log: 全文搜 `[HANG-DIAG] offer` 零结果；9 个 channel `finish` 时 `totalOffered=0 bufferQueueSize=0` |

## 3. 当前代码的"丢 event"流程（cpDuringRecovery=true）

1. filter（channelIOExecutor）调 `inputGate.finishReadRecoveredState()`
2. `RecoveredInputChannel.finishReadRecoveredState`（`RecoveredInputChannel.java:220-228`）push `EndOfInputChannelStateEvent` 进 `receivedBuffers`，complete `bufferFilteringCompleteFuture`
3. `bufferFilteringCompleteFuture` 触发 mailbox 上的 `requestPartitions` mail enqueue
4. inner mailbox loop 在处理 `requestPartitions` mail 之前，**default action（`processInput`）有机会先跑**，它通过 gate 拉到 RecoveredInputChannel，调 `RecoveredInputChannel.getNextRecoveredStateBuffer`，**把 sentinel poll 走 + completes `stateConsumedFuture` + return null**
5. mailbox 接着处理 `requestPartitions` mail → `convertRecoveredInputChannels`：
   - `toInputChannel` 时 `receivedBuffers` 已空，`remainingBuffers.size=0`
   - 物理 channel 构造后，`for (Buffer buf : remainingBuffers) rec.onRecoveredStateBuffer(buf)` 一次都没跑
   - 新建的 `LocalInputChannel.recoveredQueue` **零 buffer**
6. 后续 handoff 阶段 `StreamTask.finishPhysicalRecoveredChannels` 调每个物理 channel 的 `finishRecoveredBufferDelivery()`：
   - 只翻 `allDelivered=true`
   - **不调任何 `notifyChannelNonEmpty()`**
   - gate 永远没人唤醒

## 4. 修复方案

按 `inputGate.isCheckpointingDuringRecoveryEnabled()` 分流 sentinel 的添加位置——**两个 RecoverableInputChannel 实现都需要改**（`LocalInputChannel` 与 `RemoteInputChannel`），因为它们都对应 `RecoverableInputChannel#finishRecoveredBufferDelivery` 这个契约。

### 4.1 `!isCheckpointingDuringRecoveryEnabled()`（master 行为，保持现状）

- `RecoveredInputChannel.finishReadRecoveredState`：照旧 push sentinel 进 `receivedBuffers`
- sentinel 由 `RecoveredInputChannel.getNextRecoveredStateBuffer` 消费、completes `stateConsumedFuture`
- `stateConsumedFuture` 触发 `requestPartitions` → `toInputChannel`（master 流程）

### 4.2 `isCheckpointingDuringRecoveryEnabled()`（新行为，补回丢失的 event）

- `RecoveredInputChannel.finishReadRecoveredState`：**不再 push sentinel**，只 complete `bufferFilteringCompleteFuture`
- `LocalInputChannel.finishRecoveredBufferDelivery` 与 `RemoteInputChannel.finishRecoveredBufferDelivery`：
  - 先 `recoveredQueue.offer(EndOfInputChannelStateEvent.INSTANCE)`（push 进 recoveredQueue，`wasEmpty=true` 是返回值）
  - 然后 `recoveredQueue.finish()`（翻 `allDelivered=true`）
  - 如果 `wasEmpty == true`，跳出锁后调用一次 `notifyChannelNonEmpty()`

### 4.3 为什么 4.2 修复后 work

1. `offer` 把 sentinel 进 `recoveredQueue` → consumer 端 `inRecovery=true` 且 queue 非空，channel 是被 enqueue 的
2. `wasEmpty=true` 触发 `notifyChannelNonEmpty()` → `gate.queueChannel` 把 channel 放回 `inputChannelsWithData`，availFut 完成
3. consumer poll → `getNextBuffer`：
   - inRecovery=true，hasPri=false
   - poll sentinel from recoveredQueue → recoveredQueue.isEmpty() 变 true
   - `wrapRecoveredBufferAsAvailability`：当 `recoveredQueue.isEmpty() && allDelivered=true` 走探 `subpartitionView` 分支，`getAvailabilityAndBacklog(true)` 报上游有数据 → `nextDataType=DATA_BUFFER`
4. `moreAvailable=true` → gate 重新 enqueue channel
5. 下次 poll：`inRecovery=false`（allDelivered=true & queue empty），fall through 到 `subpartitionView.getNextBuffer()`，正常拿上游 buffer

### 4.4 行为等价于 master 的"最后一个 recover buffer 触发探上游"

master 的 `getNextRecoveredBuffer`（`master/.../LocalInputChannel.java:377-396`）在消费 `toBeConsumedBuffers` 最后一个 buffer 时探 `subpartitionView` 并把 `nextDataType` 强制改成 `DATA_BUFFER`。我们这条修复路径等价：通过补一个 sentinel buffer 让 `wrapRecoveredBufferAsAvailability` 走同样的"queue 空 + delivered + 探 view"分支。

## 5. 影响范围

- `RecoveredInputChannel.finishReadRecoveredState`：cpDuringRecovery 分支改为不 push sentinel
- `LocalInputChannel.finishRecoveredBufferDelivery`：加 offer + 条件 notify
- `RemoteInputChannel.finishRecoveredBufferDelivery`：加 offer + 条件 notify（注意 Remote 的锁是 `receivedBuffers`，要把 offer 放进相同 critical section）
- `RecoverableInputChannel` 接口契约可以维持不变（只是实现里多 push 一个 buffer），但 javadoc 里需要说明"实现负责在 cpDuringRecovery 路径下补 EndOfInputChannelStateEvent"
- master 的 `!cpDuringRecovery` 路径**不受影响**

## 6. 验证步骤

1. 实施 §4.2 改动并编译
2. 跑 `rui_tools/loop.sh`（fresh-start case [1] 之前 100% 复现 hang）
3. 预期：测试不再卡死，case [1] 通过或失败但**不挂**
4. 若仍 hang：抓 heap，按 hang_evidence.md §5.1 流程对照检查 `recoveredQueue.size`、`enqueuedBitSet`、`availFut` 状态——理论上 `recoveredQueue.size` 应该至少出现过 1 个 sentinel，gate 的 availFut 应该被完成过

## 7. Follow-up：§4 修复在 cpDuringRecovery=false 路径上过度扩散，引入新 race（2026-05-24）

`loop.sh 20260523_222057` 跑里出现 4 个 `UnalignedCheckpointRescaleWithMixedExchangesITCase.testRescaleFromUnalignedCheckpoint[[1]] / [[2]] / [[4]] / [[5]]` 失败，栈一致：

```
java.lang.IllegalStateException: Queried for a buffer before requesting the subpartition.
    at LocalInputChannel.checkAndWaitForSubpartitionView(LocalInputChannel.java:575)
    at LocalInputChannel.getNextBuffer(LocalInputChannel.java:379)
    at SingleInputGate.readBufferFromInputChannel(...)
```

4 个失败 case 的 `execution.checkpointing.during-recovery.enabled` 全部是 `false`（log 里 `PseudoRandomValueSelector` 行可证）。也就是说 §4 引入的"无差别推 sentinel + notify"在 cpDuringRecovery=false 路径上也跑了，但那条路径上**根本不需要它**。

### 7.1 为什么 cpDuringRecovery=false 不需要 sentinel

§4 的 sentinel 解决的是 cpDuringRecovery=true 路径上独有的"`isInRecovery=true` 阻塞期跨越"问题：drain 完成时上游的 `notifyDataAvailable` 早被消化、没人再 wake task。cpDuringRecovery=false 路径上**不存在这个阻塞期**：

- `RecoveredInputChannel.toInputChannel` 在 cpDuringRecovery=false 路径下被调用时，`RecoveredInputChannel.receivedBuffers` 上的 recovered state 已经全部被 task 消费完了（`stateConsumedFuture` 就是这么 complete 的）
- 物理 channel 一被构造出来，`recoveredQueue` 是空的，没有 SpillFileReader 会再来 push
- 此时只要 `allDelivered=true`，`isInRecovery` 立刻就是 false，channel 进入 master 等价的 normal mode
- wake-up 完全靠 master 协议：`LocalInputChannel.requestSubpartitions` 成功后主动 `notifyDataAvailable(view) → notifyChannelNonEmpty()`、或上游后续 `view.notifyDataAvailable` 回调（master 几年来都这么 work）

### 7.2 §4 在 cpDuringRecovery=false 路径上为什么有害

`LocalInputChannel.requestSubpartitions` 在上游 `ResultPartition` 还没注册时抛 `PartitionNotFoundException`，被 catch 后设 `retriggerRequest=true` 走 Timer 异步重试（master 一直如此）。`subpartitionView` 保持 null 直到某次 retrigger 成功。这本来是良性的：master 路径上 channel 不在 `inputChannelsWithData` 里，task 不会调它的 `getNextBuffer`，等 retrigger 成功的那一刻 channel 自己 `notifyChannelNonEmpty()` 才 wake task。

§4 修复在 cpDuringRecovery=false 路径上**也**推一个 sentinel：

1. sentinel 入 `recoveredQueue` → `notifyChannelNonEmpty()` → channel 被 enqueue
2. task wake → `getNextBuffer` → `isInRecovery=true`（buffers 非空）→ 走 recovery 分支 → poll 走 sentinel
3. buffers 空 + `allDelivered=true` → 下一次 `getNextBuffer` `isInRecovery=false` → 走 normal path → `checkAndWaitForSubpartitionView` 撞上 `subpartitionView==null`（Timer 还在重试中）→ 抛

也就是 §4 的 sentinel 把 task **提前**推进了 normal path，而上游 retrigger 还没成功——这是 §4 之前根本不存在的窗口。

### 7.3 落地修复（已实施 2026-05-24）

最小改动：让 cpDuringRecovery=false 路径上的物理 channel 在**构造时**就把 `allDelivered=true`，根本不再依赖任何后续的 `finishRecoveredBufferDelivery()` 调用。

- `RecoveredBufferQueue`：构造器加 `boolean initiallyDelivered` 参数，初始化字段 `allDelivered = initiallyDelivered`
- `LocalInputChannel` / `RemoteInputChannel` 构造器：`new RecoveredBufferQueue(channelInfo, !inputGate.isCheckpointingDuringRecoveryEnabled())`
- `RecoveredInputChannel.toInputChannel`：删除 cpDuringRecovery=false 分支对 `rec.finishRecoveredBufferDelivery()` 的调用（注释里说明）
- `LocalInputChannel.finishRecoveredBufferDelivery` / `RemoteInputChannel.finishRecoveredBufferDelivery` 方法**保留不动**——`SpillFileReader.drain()` 在 cpDuringRecovery=true 路径上仍然调它，那里的 sentinel + notify 仍是必需的

### 7.4 cpDuringRecovery=true 路径上的残余 race（已实施 2026-05-24）

§4 的"推 sentinel"在 cpDuringRecovery=true 路径上仍然必要（跨越阻塞期 wake 一次），但 push sentinel 跟 wake 之间存在一段窗口：drain 完成推 sentinel 时如果 `requestSubpartitions` 仍在 `PartitionNotFoundException` Timer 重试中（`subpartitionView==null` / `partitionRequestClient==null`），无条件 wake 会让 task 消费 sentinel 翻 `isInRecovery=false` 后走 normal path 撞 `Queried before request`。本次 loop 里这条路径上的 4 个 case `[[3]]` 因为先撞了 Group 1（`Missing RecoveryCheckpointBarrier`，已由 SpillFileReader per-channel barrier 修复）没暴露 Group 2，但 race 客观存在，需要从根上封死。

#### 7.4.1 思路

让 sentinel 的入队跟 master 的 retrigger wake 协议对齐：所有恢复 buffer（含 sentinel）即便提前到了也不消费，必须等 `requestSubpartitions` 真正把 `subpartitionView` / `partitionRequestClient` publish 之后才允许 task 走 normal path。落地两个动作：

1. **`finishRecoveredBufferDelivery` 条件 wake**：sentinel 入队跟 `allDelivered=true` 仍在锁内一起做（保持原有原子性），但出锁后只有 `wasEmpty && (上游字段已 publish)` 才 `notifyChannelNonEmpty()`。`subpartitionView` 为 null 时跳过 wake 是安全的——`requestSubpartitions` 的 retrigger 路径自带 `notifyDataAvailable(view) → notifyChannelNonEmpty()`，那时 sentinel 已经在队列里，task 一来就能消费。
2. **依赖 `volatile` 而非额外锁**：`LocalInputChannel.subpartitionView` 字段（`:78`）和 `RemoteInputChannel.partitionRequestClient` 字段（`:103`）都已经是 `volatile`，且都是"一次性 publish"语义（release 路径置回 null 是终态），单读、不 check-then-act，JMM 保证读到的 null/非 null 跟 wall-clock 顺序一致——这条 wake 决定路径 volatile 就够，不必引 `requestLock` 增加 lock-order 负担。

#### 7.4.2 落地代码

统一原则：**任何进 `recoveredQueue` 的 buffer 都必须等上游 publish 后才允许触发消费**，所以 `onRecoveredStateBuffer`（data buffer / RecoveryCheckpointBarrier）和 `finishRecoveredBufferDelivery`（sentinel）的 wake 路径都加同一条件 wake——避免依赖"现有 caller 都满足 `allDelivered=false` 假设"这种脆弱 invariant。

```java
// LocalInputChannel.onRecoveredStateBuffer / finishRecoveredBufferDelivery
synchronized (recoveredQueue) {
    wasEmpty = recoveredQueue.offer(buffer);   // 或 sentinel + recoveredQueue.finish()
}
if (wasEmpty && subpartitionView != null) {    // 条件 wake，两处一致
    notifyChannelNonEmpty();
}

// RemoteInputChannel.onRecoveredStateBuffer / finishRecoveredBufferDelivery
synchronized (receivedBuffers) {
    wasEmpty = recoveredQueue.offer(buffer);   // 或 sentinel + recoveredQueue.finish()
}
if (wasEmpty && partitionRequestClient != null) {   // 条件 wake，两处一致
    notifyChannelNonEmpty();
}
```

#### 7.4.3 happens-before 论证

记 A = `finishRecoveredBufferDelivery` 线程（cpDuringRecovery=true 路径上是 channelIOExecutor / drain），B = `requestSubpartitions` 成功线程（mailbox），列出所有交错：

- B 写 `subpartitionView` 早于 A 读：volatile 保证 A 读到非 null，A 自己 wake，B 后续 `notifyDataAvailable` 是重复 wake（无害）；
- B 写在 A 读之后：A 读到 null 不 wake，B 之后 `notifyDataAvailable(view)` 自己 wake，task 来 poll 时 sentinel 已经在队列里被消费；
- B 已 wake、task 已 poll empty 退队、A 之后才 push sentinel：A 读 `subpartitionView` 必看到 B 已 publish 的非 null，A wake 兜底，channel 重新入队、消费 sentinel。

没有 lost wake-up 窗口。前提是 `subpartitionView` / `partitionRequestClient` 的 publish 满足"一次性、单调"：master 现状满足（release 终态置 null 不会再翻回非 null），代码 invariant 一致。

#### 7.4.4 跟 §7.3 的关系

§7.3 让 cpDuringRecovery=false 路径压根不调 `finishRecoveredBufferDelivery`、靠"构造即 `allDelivered=true`"避开 sentinel 翻转；§7.4 让 cpDuringRecovery=true 路径上仍然要调的 `finishRecoveredBufferDelivery` 改成条件 wake、对齐 master 的 retrigger wake 协议。两条路径合起来彻底封死 Group 2 的两类窗口（构造期 race + drain 期 race）。

#### 7.4.5 验证

- 已修：编译通过（`finishRecoveredBufferDelivery` 仅改 wake 条件，字段无需新增）
- 跑 `rui_tools/loop.sh` 至少 100 轮，确认 `Queried for a buffer before requesting the subpartition.` 不再复现
- 单元测试视后续讨论补加（典型场景：mock channelIOExecutor 推 sentinel 时 `subpartitionView=null`，要求 task 不被 wake；再触发 `requestSubpartitions` 成功，确认 task 被 wake 并消费 sentinel）

## 8. Follow-up：§7.4 的条件 wake 由 per-channel `upstreamReady` future 取代（2026-05-24）

§7.4 落地的"条件 wake"是 race 修补型方案：buffer/sentinel 仍可在 `subpartitionView==null` 时进 `recoveredQueue`，只是 wake 不发，靠 `requestSubpartitions` 重试成功的 `notifyDataAvailable` 兜底。`loop.sh 20260524_134921` 又复现了 `Missing RecoveryCheckpointBarrier`，定位到根因不是 wake 时序，而是更上游的"Step 1 / Step 2 in-recovery 信号源不一致"——详见 [recovery_in_recovery_flag_unification.md](./recovery_in_recovery_flag_unification.md)。

新方案（per-channel `upstreamReady` future，见 `recovery_in_recovery_flag_unification.md §9`）把"等上游 ready" 下沉到 channel 内部：`onRecoveredStateBuffer` / `finishRecoveredBufferDelivery` 入口先 `upstreamReady.get()`，等到 `requestSubpartitions` 真正成功设上 `subpartitionView` / `partitionRequestClient` 才放行 push。语义不变量：**任何 buffer 或 sentinel 进入 `recoveredQueue` 时，subpartitionView/partitionRequestClient 必已 publish**——`isInRecovery` 翻 false 后走 normal path 必然安全。

由此 §7.4 的条件 wake 可以删除：

- 不再需要"`wasEmpty && subpartitionView != null`"这种判断
- `notifyChannelNonEmpty()` 恢复无条件触发——因为 push 时上游必已 ready、wake 总是合法的
- 删除 §7.4 §7.4.2 §7.4.3 §7.4.5 提到的 Local/Remote 两端 `subpartitionView/partitionRequestClient` 条件分支

§7.3（cpDuringRecovery=false 路径构造时 `allDelivered=true`）保留——它解决的是 cpDuringRecovery=false 路径下根本不该进 recovery 阶段的根因，跟 per-channel future 正交。

时序上的整体走向：

| 路径 | 旧 §7.4 行为 | 新（per-channel future）行为 |
| --- | --- | --- |
| cpDuringRecovery=false | channel 直接 allDelivered=true、不进 recovery、无 sentinel push | 不变（§7.3 保留） |
| cpDuringRecovery=true，真 drain | drain 中途 push 可能在 upstream 未 ready 时入队、靠条件 wake 等回 | drain 在 push 入口就阻塞等 upstream ready；快 channel 不阻塞慢 channel |
| cpDuringRecovery=true，fresh job (`spillFile==null`) fallback | fallback push sentinel + 条件 wake | fallback push sentinel 入口也等 upstream ready（粒度 per-channel；fresh job 这条慢一点也无所谓） |

实施进入下一个 commit；本文档保留作为方案演进的历史记录。

## 9. Follow-up（续）：`EndOfInputChannelStateEvent` sentinel 彻底删除（2026-05-24）

§8 仍假定 sentinel 留在队尾、由 channel-side per-channel future 抑制其副作用。后续推演发现 sentinel 本身已经没用，**整个删除**更干净（详见 [`recovery_in_recovery_flag_unification.md §9.2`](./recovery_in_recovery_flag_unification.md)）：

- sentinel 在 §4 的唯一价值是"drain 完成给 task 一次 wake"，跨越 `notifyDataAvailable` 是 edge-trigger 在 in-recovery 阻塞期被消化的窗口；
- 但这次 wake 不需要靠"sentinel 进队"实现——`finishRecoveredBufferDelivery` 直接无条件调 `notifyChannelNonEmpty()` 即可，task wake 后走 normal path 检查 `subpartitionView`、有数据就读、没有就退队（白 wake 一次的代价跟旧 sentinel 路径基本等价）；
- 删 sentinel 后 `buffers=[] && allDelivered=true → isInRecovery=false`（正确），而不是旧的 `buffers=[sentinel] → isInRecovery=true`（假性 in-recovery、误导 Step 2 进 collect 找 barrier）。

副作用：路径 2（fresh-job + cpDuringRecovery=true）下 trigger=NO_OP、Step 2 现在看 `isInRecovery=false` 不进 collect、自然不会抛 `Missing RecoveryCheckpointBarrier`。`collectPreRecoveryBarrier` 维持严格契约"找不到 barrier 一律抛"，不需要任何 corner-case 容忍。

最终接口形态（单接口、双入口对外语义清晰；两个入口各自落地、不抽 helper）：

- `onRecoveredStateBuffer(Buffer)`：`upstreamReady.join()` → push data buffer 进 `recoveredQueue` + 条件 wake（保持原契约）
- `finishRecoveredBufferDelivery()`：`upstreamReady.join()` → `recoveredQueue.finish()` → **无条件** `notifyChannelNonEmpty()`；**不 push sentinel**

**注意**：channel 端 `upstreamReady.join()` 跟 §9 主流程的 `finalDrainEnabled` 时序重排是**两个并存的机制**、不是替代关系——前者守"上游 handle 真正 publish"约束（应对 `requestSubpartitions` Timer retrigger），后者守"trigger 字段已装上"约束（应对 mail #A → mail #B 间隙）。详情见 [`recovery_in_recovery_flag_unification.md §9.3`](./recovery_in_recovery_flag_unification.md)。
