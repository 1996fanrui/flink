# Channel-State 数据破坏排查：当前分支代码级落地（low-level）

> 分支：`38544-spilling-v2/20260702-01-check-data-corruption`（flink-os-2 仓库，spilling v2 / FLINK-38544 checkpoint-during-recovery 重构版）
> 配套的通用方法论 / 校验规则见 `channel_state_corruption_instrumentation.md`（下称 HL，从参考分支
> `FLINK-40016/cdr-without-spilling-data-loss-repro` 逐字节迁移，未做任何修改）。
> 本文档只讲**当前分支真实代码里**的落地细节：辅助类如何迁入、五个校验阶段挂在哪个类/方法/行、聚合 key 怎么组、
> 与参考分支的落地差异、已知缺陷、编译与复现命令。
> 所有行号以当前分支代码为准；行号会随插桩演进漂移，以方法名为准重新定位。
>
> ⚠️ **现状**：当前分支**尚未加任何插桩**——`ChannelStateInvariant` 类和全部 call site 都还不存在。
> 本文档是首轮插桩的落地映射（哪五个点、怎么挂、key 怎么组），实施后应回填为"已核实的现状记录"。

## 文档范畴（更新前必读，勿越界）

- **本文档 = LOW-LEVEL（代码级落地）**：只写"当前分支代码里具体怎么落地"——类、方法、行号、标签实现、key 分组、已知缺陷、编译/测试命令。
- **不写通用规则/方法论**。校验必须遵循的规则（完整 buffer 校验、上下游容忍度、恢复链不容忍、只有 checkpoint 写入区分上下游、日志分级等）**一律以 HL 文档 `channel_state_corruption_instrumentation.md` §6 为唯一权威**。
- **规则冲突时以 HL 为准**：若本文档的落地方案与 HL 的规则不一致，那是"当前代码有缺陷/待修正"，应记入本文档"已知缺陷 / 待修正项"，**绝不能反过来改 HL 的规则去迁就代码**。
- 更新本文档时：只更新代码事实（随插桩演进）。**禁止**把通用规则搬进来、也**禁止**在这里新立与 HL 冲突的规则。

---

## 0. 当前分支与参考分支的结构差异（决定五个点挂哪里，先读这节）

参考分支（FLINK-40016 修复所在）的恢复是"读 checkpoint → filter → 直接注入 channel"；当前分支（spilling v2）的
数据流多了 **spill 文件往返** 和 **checkpoint 三段写入**，五个校验阶段的挂点随之移动：

```
restore:  readInputData(:68, 两个 read pass：input handles 先、upstreamOutputBufferState 后)
              │ 每个 buffer → handler.recover(...)              ←★ RECOVER_READ
              ▼
          SpillingWithFilteringHandler.filterAndRewrite → segmentSerializer   ←★ RECOVER_REWRITE
              ▼
          AbstractSpillingHandler 落盘 spill 文件（sealCurrentSegment :342）
              ▼
          FetchedChannelStateDrainer.drain(:92)/drainSegment(:122) 按 memorySegmentSize 重新切块
              │ onRecoveredStateBuffer push 进物理 channel        ←★ CHANNEL_RECEIVE
              ▼
运行中:    task 消费 recoveredBuffers；UC barrier 到达：
          ChannelState.onCheckpointStartedForAllInputs(:125)
              ├ (a) channel.checkpointStarted → collectPreRecoveryBarrier    ←★ SNAPSHOT
              ├ (b) channelStateWriter.addInputDataFromSpill(:136)（spill 剩余段回放）
              └ (c) 之后消费中的 maybePersist（getBufferAndAvailability :668-669 / Remote onBuffer :813）
              ▼
          ChannelStateCheckpointWriter.writeInput(:147)/writeInputFromSpill(:166)/writeOutput(:208)
                                                                  ←★ CHECKPOINT_WRITE
```

与参考分支相比的四个关键差异：

1. **CHANNEL_RECEIVE 挂点移动**：参考分支挂在 Local/Remote InputChannel 构造器的 `initialRecoveredBuffers` 迁移循环；
   当前分支是 push 模型，recovered buffer 由 drain 线程经 `onRecoveredStateBuffer` 逐个送入，挂点相应移到该方法，
   flush 挂在 `finishRecoveredBufferDelivery`（EOF sentinel 追加点 = 全部投递完成）。
2. **RECOVER_READ / RECOVER_REWRITE 挂点移动**：参考分支挂在 `InputChannelRecoveredStateHandler.recover/recoverWithFiltering`；
   当前分支拆成三个 handler 子类（`NoSpillingHandler` / `SpillingNoFilteringHandler` / `SpillingWithFilteringHandler`，
   都在 `RecoveredChannelStateHandler.java`），REWRITE 的产物不再直接注入 channel，而是写进 spill segment serializer。
3. **CHECKPOINT_WRITE 多了第三条写入路径**：除 `writeInput`（(a) knownBuffers + (c) maybePersist）和 `writeOutput` 外，
   当前分支新增 `writeInputFromSpill(:166)`——checkpoint-during-recovery 时把 spill 剩余段 (b) 直接回放进 checkpoint。
   **这条路径必须并入同一个 input 累积器**，否则 (a)/(b)/(c) 的接缝（最可疑区域）不在校验范围内。
4. **REWRITE 与 RECEIVE 之间隔了一跳 spill 往返**：参考分支 REWRITE 后直接注入 channel；当前分支中间还有
   "segment 落盘（header 回填 `sealCurrentSegment :342`）→ `FetchedChannelStateReaderImpl` 双游标读回 →
   drainer 按 memorySegmentSize 任意重切块"。**若 RECOVER_REWRITE 干净而 CHANNEL_RECEIVE 已坏，bug 就在这一跳**
   （spill 写/读/Position 游标/snapshot 边界），参考分支没有这一跳。

另一个当前分支特有事实：**上游 output state 在 restore 时不再回注上游 subpartition**，而是由 JM 侧
`TaskStateAssignment.distributeOutputBuffersToDownstream`（`TaskStateAssignment.java:600-637`）重分发为下游的
input handle（`InputChannelInfo(gateIdx, oldUpstreamSubtaskIndex)`），由 `readInputData` 的**第二个 read pass** 读入。
所以恢复链的 input/output 接缝天然落在 RECOVER_READ 的累积器里（见 §2.4 的 key 说明），这正是本轮排查的重点接缝之一。

---

## 1. 辅助类 `ChannelStateInvariant`（从参考分支原样迁入）

目标路径：`flink-runtime/src/main/java/org/apache/flink/runtime/checkpoint/channel/ChannelStateInvariant.java`
（`final`、纯静态、临时诊断代码，定位根因后整类删除）

该类是纯字节校验器（header/stride/framing 走查），不依赖任何分支特有代码，**整类原样迁入、不改一行**：

```bash
git -C ~/code/github/flink show \
  FLINK-40016/cdr-without-spilling-data-loss-repro:flink-runtime/src/main/java/org/apache/flink/runtime/checkpoint/channel/ChannelStateInvariant.java \
  > flink-runtime/src/main/java/org/apache/flink/runtime/checkpoint/channel/ChannelStateInvariant.java
```

开关、常量、方法、`Layer`/`Direction`/`Mode` 枚举、`shape()` 判据、日志标签（`[CS-INV]` / `[CS-INV-ASSERT]` /
`[CS-INV-TOLERATED]` / `[CS-INV-SNAP]` / `[CS-INV-REC]`）与参考分支完全一致，语义见参考分支 low-level 文档 §1；
此处只列 call site 需要用到的接口：

| 接口 | 用途 |
|---|---|
| `isEnabled()` | call site 先判它再干活（`-Dflink.cs.debug=false` 全局关闭） |
| `clearAll()` | 测试每个子 job 启动前清残留累积器（挂点见 §2.6） |
| `append(String key, ByteBuffer buffer)` | 拷贝可读字节进累积器（`duplicate()`，不动 reader index / refcount） |
| `flush(String key, String label, Layer layer[, Mode mode])` | 拼接后整体 `shape()` 校验并打日志；3 参数版固定 `STRICT` |
| `validateSnapshot(String label, long barrierId, List<Buffer> buffers)` | 快照专用，内部自行 retain/recycle、固定 `LENIENT` |
| `key(String identity, String channel, Layer layer, Direction direction)` | 统一聚合 key（§6.6 的维度结构） |
| `Layer` | `SNAPSHOT` / `CHECKPOINT_WRITE` / `RECOVER_READ` / `RECOVER_REWRITE` / `CHANNEL_RECEIVE`，与 HL §6.1.5 一一对应 |
| `Direction` | `INPUT` / `OUTPUT` |
| `Mode` | `STRICT`（不容忍半条）/ `LENIENT`（容忍首尾半条；STRIDE-IRREGULAR 两种 mode 都判真损坏） |

---

## 2. 五个校验点在当前分支的落地映射

所有 call site 都先 `ChannelStateInvariant.isEnabled()` 再动作。累积型（`append`+`flush`）和快照型（`validateSnapshot`）两类。

### 2.1 `CHANNEL_RECEIVE`（drain 推入物理 channel）—— `Mode.STRICT`

| 类（相对路径） | 挂点 | 说明 |
|---|---|---|
| `.../io/network/partition/consumer/LocalInputChannel.java` | `onRecoveredStateBuffer(Buffer)`（`:183`）append；`finishRecoveredBufferDelivery()`（`:200`）flush | 只对 `buffer.isBuffer()` 累积（**必须跳过 event**：`recoveredBuffers` 里还会出现 `RecoveryCheckpointBarrier` / `EndOfFetchedChannelStateEvent` sentinel）。append 放在 `synchronized (recoveredBuffers)` 内、`offerRecoveredBuffer` 成功之后（锁内小拷贝，诊断代码可接受；锁外碰 buffer 有被 task 线程并发 poll+recycle 的风险）。 |
| `.../io/network/partition/consumer/RemoteInputChannel.java` | `onRecoveredStateBuffer(Buffer)`（`:237`）append；`finishRecoveredBufferDelivery()`（`:255`）flush | 同上（Remote 的锁对象是 `receivedBuffers`）。 |

- key = `key(inputGate.getOwningTaskName(), channelInfo + " kind=Local"/"kind=Remote", CHANNEL_RECEIVE, INPUT)`。
  **identity 用 `getOwningTaskName()`**（形如 `"failing-map (10/21)#1"`，含算子名/subtask/attempt）——两个 channel 都持有
  `inputGate`，直接可得；比参考分支用的 `stateWriter.taskLabel()` 多了 attempt 维度。
- append（drain 线程 channelIOExecutor）与 flush（`finishRecoveredBufferDelivery` 也在 drain 线程，见
  `FetchedChannelStateDrainer.drain :111-113`）同线程，无并发问题。
- 生命周期分界 = 一次 recovery（每个物理 channel 只 drain 一次，`finishRecoveredBufferDelivery` 只调一次），天然满足。
- `CHANNEL_RECEIVE` 是恢复链终点，按 HL §6.3 全程不容忍半条 → `STRICT`。drainer 按 `memorySegmentSize` 任意切块
  （`drainSegment :122`），所以**必须拼接后整体校验**（HL §6.1），逐 buffer 看必然假阳。

### 2.2 `SNAPSHOT`（checkpoint 采集在途数据）—— `Mode.LENIENT`

| 类 | 挂点 | 说明 |
|---|---|---|
| `LocalInputChannel.java` | `checkpointStarted(CheckpointBarrier)`（`:336`） | in-recovery 时 `toPersist = collectPreRecoveryBarrier(barrier.getId())`（`:344`，`synchronized (recoveredBuffers)` 内收集）。在**锁外、`startPersisting`（`:349`）之前**对 `toPersist` 跑 `validateSnapshot(label, barrierId, toPersist)`（helper 内部自行 retain/recycle，不动即将交给 writer 的那批 buffer 的 refcount）。非 recovery 时 `toPersist` 为空，无可校验对象——注意与参考分支不同：当前分支 `toBeConsumedBuffers` 只装 `FullyFilledBuffer` 拆分（仅 sort-merge shuffle 产生，本测试不出现），不参与 SNAPSHOT。 |
| `RemoteInputChannel.java` | `checkpointStarted(CheckpointBarrier)`（`:881`） | in-recovery → `collectPreRecoveryBarrier`（`:886`）；否则 `getInflightBuffersUnsafe(barrier.getId())`（`:901`，定义在 `:1054`）。**注意当前分支整个方法体（含 `startPersisting :903`）都在 `synchronized (receivedBuffers)`（`:884`）内**：锁内对 `toPersist` 逐个 `retainBuffer()` 存进临时 list，出锁后 `validateSnapshot`，跑完逐个 `recycleBuffer()`（与参考分支同款"锁外校验"纪律）。 |

- `SNAPSHOT` 用 `validateSnapshot`（固定 `LENIENT`，标签 `[CS-INV-SNAP]`，分组粒度 = 单个 `barrierId` 的一次快照）。
- 当前分支 (a) 批次的首尾半条是业务真实：头部——task 已消费的前一 buffer 可能停在半条 record 中间（spanning 残留在
  反序列化器里）；尾部——同一条 record 可能延续到 spill 剩余段 (b) 里。LENIENT 恰好覆盖，命中打 `[CS-INV-TOLERATED]`。

### 2.3 `CHECKPOINT_WRITE` —— input 用 `Mode.STRICT`，output 用 `Mode.LENIENT`

| 类 | 挂点 | key / 分组 | mode |
|---|---|---|---|
| `.../checkpoint/channel/ChannelStateCheckpointWriter.java` | `writeInput(...)`（`:147`）累积；`completeInput(...)`（`:247`）flush | key(`jobVertexID-subtaskIndex-cp<checkpointId>`, `info.toString()`, `CHECKPOINT_WRITE`, `INPUT`) | `STRICT` |
| 同上 | **`writeInputFromSpill(...)`（`:166`）累积**（当前分支新增路径） | 同上，channel 段用 `seg.channelInfo().toString()` | `STRICT`（并入 input 累积器） |
| 同上 | `writeOutput(...)`（`:208`）累积；`completeOutput(...)`（`:255`）flush | key(`jobVertexID-subtaskIndex-cp<checkpointId>`, `info.toString()`, `CHECKPOINT_WRITE`, `OUTPUT`) | `LENIENT` |

- **`writeInputFromSpill` 的采集方式**：该方法从 `seg.bodyStream()` 流式写入
  （`serializer.writeData(dataStream, seg.bodyStream(), seg.length())`，`:188`），字节不落中间数组。插桩用
  **tee-InputStream** 包装 `seg.bodyStream()`——`read()` 透传的同时把读到的字节 append 进累积器，不改读时序、
  不预读（HL §7 Heisenbug 警告：禁止为了插桩把流式读改成先读进 byte[] 再写）。
- **三条 input 路径共用同一个累积器 key**（同 checkpoint、同 channel）：(a) `startPersisting` 的 knownBuffers 和
  (c) `maybePersist` 的逐 buffer 都经 `addInputData` → `writeInput`；(b) 经 `addInputDataFromSpill`（`ChannelStateWriterImpl:240`
  → `ChannelStateWriteRequest.replayInputDataFromSpill :261` → `writeInputFromSpill`）。同一 subtask 的请求在
  executor 里严格 FIFO（`ChannelStateWriteRequestExecutorImpl` 的 unreadyQueue 保序），mailbox 线程按
  (a)→(b) 提交（`ChannelState.onCheckpointStartedForAllInputs :125-136`），(c) 随消费在其后——**累积顺序 == 落盘顺序
  == 恢复时的读取顺序**，所以这个累积器校验的就是"恢复时该 channel 将读到的字节序列（本 cp 片段）"。
- input 语义按 HL §6.4 用 `STRICT`；但注意 §3.1 的已知粒度局限（per-cp 片段的边缘半条会产生断言级噪声，判读纪律见 §3.3）。
- **没有 jobId/attempt**：`writeInput`/`writeOutput`/`completeInput`/`completeOutput` 和整条
  `ChannelStateWriteRequest` 链路只携带裸 `JobVertexID`/`subtaskIndex`（见 `SubtaskID`），与参考分支相同的结构性限制，
  按纪律不为诊断改业务签名，见 §3.0。

### 2.4 `RECOVER_READ` —— `Mode.STRICT`

| 类 | 挂点 | 说明 |
|---|---|---|
| `.../checkpoint/channel/RecoveredChannelStateHandler.java` | `NoSpillingHandler.recover(...)`（`:204`） | CDR 关闭路径（对照组）。对 `bufferWithContext.context` 的可读字节 append。 |
| 同上 | `SpillingNoFilteringHandler.recover(...)`（`:429`） | 同上，在 `recoverPassThroughToSpill`（`:445`）之前 append。 |
| 同上 | `SpillingWithFilteringHandler.recover(...)`（`:535`） | 在 `filterAndRewrite`（调用点 `:544`，实现 `ChannelStateFilteringHandler.filterAndRewrite :109`）**之前** append 原始读回字节。 |
| 基类 `AbstractInputChannelRecoveredStateHandler` | `close()` / `closeInternal()`（`:160-165`） | 遍历本 pass 见过的所有 RECOVER_READ / RECOVER_REWRITE key 逐个 flush。 |

- key = `key(invariantIdentity(channelInfo), "old=" + channelInfo + " oldSubtask=" + oldSubtaskIndex, RECOVER_READ, INPUT)`。
  - identity 沿用参考分支做法：`channelInfo.getGateIdx()` 定位 `inputGates[gateIdx]`，`instanceof SingleInputGate` 则
    `getOwningTaskName()`，否则 `gate.toString()`。基类字段 `inputGates`（`:98`）直接可用。
  - **channel 段必须带 `oldSubtaskIndex`**：rescale 恢复时多个 old subtask 的同名 channelInfo 经同一 gate 恢复，
    不带会把不同 old subtask 的字节误聚合（§6.6 的去重要求）。
- **当前分支关键点——两个 read pass 落同一个 key**：`SequentialChannelStateReaderImpl.readInputData`（`:68`）先 read
  input handles（`ChannelStateHelper::extractUnmergedInputHandles`），再 read `upstreamOutputBufferState`（上游 output
  重分发的 input handle）。对同一条逻辑通道（old 上游 u → old 下游 d），两个 pass 的 `(channelInfo, oldSubtaskIndex)`
  相同（input handle 本就是 `{gate, u}` + 下游 old subtask d；重分发 handle 按 `TaskStateAssignment:621-630` 构造为
  `InputChannelInfo(gateIdx, u)` + `oldDownstreamSubtaskIndex=d`）→ **两段字节自动先后落进同一累积器，
  input/output 接缝恰好被 STRICT 校验夹住**。若某个 FAIL 里 RECOVER_READ 首次报 `CORRUPT-RECORD-AT`，先核对
  该偏移是否落在两个 pass 的字节分界处（在两个 read 调用之间打一条带累积长度的分界日志即可定位）。
- 恢复链按 HL §6.3 不容忍半条 → `STRICT`。分组粒度 = 一次 recovery pass（handler 实例生命周期）内该 channel 的
  **完整数据流**，这是五个阶段里判据最强的一个。

### 2.5 `RECOVER_REWRITE` —— `Mode.STRICT`

| 类 | 挂点 | 说明 |
|---|---|---|
| `SpillingWithFilteringHandler.recover(...)`（`:535`） | `filterAndRewrite` 前后取 `segmentSerializer.length()` 差量 | `filterAndRewrite` 把重写结果写进 `segmentSerializerFor(mappedInfo)`（`:549`）。基线 length 必须在 `segmentSerializerFor` **返回之后**取（该调用可能触发 `switchChannelIfNeeded :323` seal 前一个 channel 并 `clear()` 重置 serializer）；调用后把 `getSharedBuffer()` 的 `[before, after)` 字节 append。单次 `recover` 调用只写一个 mapped channel 的 serializer，差量内无 channel 切换。 |
| `SpillingNoFilteringHandler.recover(...)`（`:429`） | 同上或直接 append `source` 字节 | pass-through 模式 `recoverPassThroughToSpill`（`:445`）verbatim 写入，REWRITE 字节 == READ 字节，append 相同字节即可（keyed by mapped channelInfo，与 READ 的 old channelInfo 区分）。 |
| `NoSpillingHandler` | 无 REWRITE 挂点 | 无重写步骤，数据直接 `onRecoveredStateBuffer` 注入 → 由 `CHANNEL_RECEIVE` 直接承接（该路径 READ 与 RECEIVE 相邻）。 |

- key = `key(invariantIdentity(channelInfo), "mapped=" + getMappedChannels(channelInfo).getChannelInfo(), RECOVER_REWRITE, INPUT)`。
  **channel 段用 mapped（新/物理）channelInfo**——重写产物按物理 channel 组织（spill segment header 写的就是它，
  `switchChannelIfNeeded :331-333`），多个 old channel 可能汇入同一个 mapped channel，聚合粒度要跟 spill segment 一致。
- flush 同样挂 handler `close()`。
- **REWRITE（本节）与 CHANNEL_RECEIVE（§2.1）之间隔着 spill 落盘→读回→drain 重切块一整跳**（见 §0 差异 4）。
  两边都干净 → spill 往返无罪；REWRITE 干净而 RECEIVE 坏 → 嫌疑集中在 `AbstractSpillingHandler`
  （header 回填 `sealCurrentSegment :342`、64MB 文件轮转 `ensureFileOpen :363`）、`FetchedChannelStateReaderImpl`
  （`committed`/`current` 双游标、`firstSegment` 的 mid-body 续读 `:123-154`）、`FetchedChannelStateDrainer`
  （`drainSegment :122` 的 fill/commit、`snapshotAndInsertBarriers :178` 的边界）。

### 2.6 `clearAll()` 挂点（防跨 job 残留污染）

`flink-tests/.../UnalignedCheckpointTestBase.java` 的 `execute(UnalignedSettings, TestInfo)`（`:156`）方法体最开头调用
`ChannelStateInvariant.clearAll()`——rescale 测试的三段子 job 各自启动前清一次，上一段 job 未 flush 的累积器
（异常退出/取消）不会接到下一段同名 key 上产生假损坏。

### 2.7 各层 key 分组 + mode 一览

| layer | identity 段 | channel 段 | direction | 校验对象 | 是否 = channel 完整数据流 | mode |
|---|---|---|---|---|---|---|
| `CHANNEL_RECEIVE` | `owningTaskName`（含 subtask/attempt，**无 jobId**） | channelInfo + kind | `INPUT` | drain 推入物理 channel 的全部 recovered buffer | 是 | `STRICT` |
| `SNAPSHOT` | `owningTaskName`（同上）+ 单个 `barrierId` | channelInfo + kind | `INPUT`（隐含） | 一次 cp 采到的 (a) 批次 | **否**（单 cp 片段） | `LENIENT` |
| `CHECKPOINT_WRITE`（input） | `jobVertexID-subtask-cp<N>`（**无 jobId/attempt**） | channelInfo | `INPUT` | 单 cp 写入的 input 字节（(a)+(b)+(c) 按落盘顺序） | **否**（单 cp 片段） | `STRICT` |
| `CHECKPOINT_WRITE`（output） | `jobVertexID-subtask-cp<N>`（同上） | subpartitionInfo | `OUTPUT` | 单 cp 写入的 output 字节 | **否**（单 cp 片段） | `LENIENT` |
| `RECOVER_READ` | `owningTaskName`（gate 所属 task） | old channelInfo + oldSubtaskIndex | `INPUT` | 一次 recovery pass 读回的字节（两个 read pass 连续累积，含 input/output 接缝） | 是 | `STRICT` |
| `RECOVER_REWRITE` | `owningTaskName`（同上） | mapped channelInfo | `INPUT` | 一次 recovery pass 重写后的字节 | 是 | `STRICT` |

> jobId 在五个阶段都拿不到（与参考分支相同的结构性限制，见 §3.0）。当前分支比参考分支好的一点：
> `CHANNEL_RECEIVE`/`SNAPSHOT` 也能用 `inputGate.getOwningTaskName()`（含 attempt），不必再依赖
> `stateWriter.taskLabel()`。`CHECKPOINT_WRITE` 仍只有裸 `jobVertexID-subtaskIndex`。

---

## 3. 已知缺陷 / 待修正项（首轮插桩即带入的已知局限）

0. **聚合 key 的 jobId 维度，五个阶段全部拿不到；`CHECKPOINT_WRITE` 还缺 attempt**（HL §6.6 要求的维度里缺失的部分）。
   已核实当前分支每个阶段能触达的路径：
   - `CHANNEL_RECEIVE`/`SNAPSHOT`/`RECOVER_READ`/`RECOVER_REWRITE`：`getOwningTaskName()` 含 subtask+attempt，
     但是人类可读字符串，不含 jobId、也不是结构化 `JobVertexID`。
   - `CHECKPOINT_WRITE`：`ChannelStateCheckpointWriter` 和 `ChannelStateWriteRequest` 链路只带裸
     `JobVertexID`/`subtaskIndex`（`SubtaskID`），不带 jobId/attempt（尽管 `ChannelStateWriterImpl` 有 `taskName`
     字段可作 label，仍无结构化 jobId）。
   要补齐需改生产构造器/方法签名，按纪律不允许为诊断目的改业务签名 → 用 §2.6 的 `clearAll()` 规避跨 job 残留污染，
   key 本身无法区分 job/attempt 这一根本缺陷保留为待办。
1. **`CHECKPOINT_WRITE`/`SNAPSHOT` 按"单个 checkpoint"分片**，校验的是片段而非 channel 完整数据流（HL §8.9 的警告）。
   当前分支这一点比参考分支**更显著**：checkpoint-during-recovery 的 input 片段（(a)+(b)+(c) 拼接）**头部半条是业务真实**
   （task 消费停在 spanning record 中间）、尾部半条同样可能（record 跨过 barrier 由下一 cp 接续）——STRICT 判据下这些会打
   `[CS-INV-ASSERT]` 边缘噪声。**保持 STRICT 是遵守 HL §6.4**；判读纪律见 §3.3。若边缘误报淹没信号，可临时把 input 降为
   `LENIENT` 跑一轮，但必须在 findings 里注明这是对 HL §6.4 的偏离及原因（片段粒度）。
2. **output 侧当前分支没有独立的"恢复读"观测缺口问题**——上游 output state 经 JM 重分发成下游 input handle，
   由 `readInputData` 第二个 read pass 走同一 handler.recover（§2.4 已覆盖）。但 `writeOutput` 侧的**采集来源**
   （上游 subpartition 在 barrier 插入时捕获的在途 buffer，含被下游部分消费的 head buffer 切片）没有对应的
   "采集侧" SNAPSHOT 观测点——output 方向只有 CHECKPOINT_WRITE 一个点，采集→写盘之间的问题无法夹逼。留作后续轮次
   （若 CHECKPOINT_WRITE(output) 首现损坏再补）。
3. **判读纪律：真正可靠的信号是 `STRIDE-IRREGULAR`**（任何 mode 下都判真损坏、不受首尾半条影响），其次是
   非 tolerated 的 `CORRUPT-RECORD-AT`（落在拼接数据中段、且离两个 pass/三条写入路径的接缝日志有明确距离）。
   五个阶段中**首个**报真损坏的阶段就是数据第一次变坏的地方（HL §6.1.5），bug 在它与上一阶段之间。
4. **插桩不得改变数据路径**：`writeInputFromSpill` 的 tee 只许透传 `read()`，禁止预读整段；`validateSnapshot`
   的 retain/recycle 不许动交给 writer 的 refcount；`onRecoveredStateBuffer` 的 append 在锁内做小拷贝。
   任何一处改动后先验证"还能复现"再信结论（HL §7/§8.6）。

---

## 4. 编译与复现命令（当前仓库）

### 4.1 编译（只改 `flink-runtime` 生产代码后）

重新 build 并安装到本地 `~/.m2`：

```bash
cd flink-runtime
../mvnw -T 20 clean install -U -Pfast -DskipTests \
  -Dmaven.javadoc.skip=true -Drat.skip=true -Dcheckstyle.skip=true -Denforcer.skip=true
```

> 约 5 分钟以上。`repro/repro.sh` 假设生产模块已 install 到 `~/.m2`，它只重编 `flink-tests` 的 test-classes。

### 4.2 复现

```bash
bash repro/repro.sh 6 2000 180
#                   │   │    └─ 每次 run 超时(秒)
#                   │   └────── 目标总 run 数
#                   └────────── 并发 worker 数
```

- 当前分支实测命中率约 **0.1–0.3%/run**（远低于参考分支的 ~5%），需要 CPU 争用，预算 ~1000+ runs（见 `repro/repro.sh` 头注）。
- 失败判定（`classify`）与产物（`repro/results/FAIL_w*_*.log`）同参考分支；**命中后立即整目录备份**（HL §9 规则 1）：
  `cp -a repro/results "repro/results-$(date +%Y%m%d_%H%M%S)"`。
- 已有历史现场：`repro/results/FAIL_w10_2.log`、`FAIL_w18_2.log`、`FAIL_w22_2.log`（`Corrupt stream, found tag: -22`，
  出错 channel `InputChannelInfo{gateIdx=0, inputChannelIdx=30}`——注意这是 31-channel gate 的**物理** channel，
  上游 `rescale0` 由 30 rescale 到 31）。

### 4.3 跑相关单测（示例）

```bash
./mvnw -pl flink-runtime \
  -Dtest=ChannelStateCheckpointWriterTest,FetchedChannelStateDrainerTest,FetchedChannelStateDrainerConcurrencyTest,LocalInputChannelTest test
```

> 全局 test 极慢，只跑与改动相关的 test。
