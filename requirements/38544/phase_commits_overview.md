# FLINK-38544 Spilling v2 — Phase Commits 总览

本文档总览当前分支上 FLINK-38544（spilling v2）功能开发的 6 个 phase commits：每个 commit 在做什么、改了哪些文件，以及多个 commit 之间被重复修改的文件归属说明。

## 1. 每个 phase commit 的功能介绍

### Phase 0：前置解耦 `129d7b3`

**Subject**：`[FLINK-38544][network] Decouple LocalInputChannel recovery wiring from toBeConsumedBuffers`

把 `LocalInputChannel` 上原本"既装恢复期 buffer 又装 FullyFilledBuffer 拆分"的单一队列 `toBeConsumedBuffers` 拆成两个职责分明的队列：

- `recoveredBuffers`（新引入）：只承载恢复期从 `RecoveredInputChannel` 迁移过来的 buffer，由 `getNextRecoveredBuffer()` 消费，保留优先级事件 interleaving 和最后 buffer 动态判定 nextDataType 的能力。
- `toBeConsumedBuffers`（保留）：回归到只承载 `FullyFilledBuffer` 部分切片的角色。

这是后续 phase 的纯前置 refactor：无对外 API 变化、无新增测试，由 9 个已有 `LocalInputChannelTest` 测试覆盖。

### Phase 1：通用接口与 sentinel `b892c23`

**Subject**：`[FLINK-38544][network] Phase 1: common interfaces & sentinels for spilling v2`

引入 spilling v2 在 network / checkpoint 模块共用的接口与 sentinel 类型：

- `BufferRequester`：把 buffer 申请抽象为接口，便于 spill reader 等组件复用。
- `ChannelStateWriter` 新增 `addInputDataFromSpill(...)` 入口契约。
- `DiskSnapshot`：表示 `SpillFile` 在某一时刻的可重放磁盘切片（含 ref-count）。
- `RecoveryCheckpointBarrier`：恢复期专用的 checkpoint barrier sentinel。
- `RecoveryCheckpointTrigger`：恢复期 checkpoint 触发器抽象（含 `NO_OP` 实现）。
- `RecoverableInputChannel`：抽出"可接收恢复 buffer 的 channel"接口（`onRecoveredStateBuffer` / `finishRecoveredBufferDelivery` / `awaitUpstreamReady` / `isInRecovery`）。
- `RecoveredInputChannel` 实现新接口（小幅改动）。
- `MockChannelStateWriter` 跟进新接口。

本 phase 不引入运行逻辑，只是为后续 phase 提供"语言"。

### Phase 2：InputChannel 侧 push-based 恢复 `7cff232`

**Subject**：`[FLINK-38544][network] Phase 2: InputChannel side push-based recovery`

把 input channel 侧从"pull receivedBuffers"切换为"push 到 recoveredQueue"模型：

- 新增 `RecoveredBufferQueue`：单 channel 的恢复 buffer 队列，承载状态机（in-recovery → all-delivered）、sequence 计数、监视器同步。
- `LocalInputChannel` / `RemoteInputChannel` 实现 `RecoverableInputChannel`；引入 `recoveredQueue`、`upstreamReady` future、`hasPendingPriorityEvent` 等字段；重写 `getNextBuffer` 路径，区分 in-recovery 分支和 post-recovery 分支。
- `RecoveredInputChannel` 在 phase 1 接口的基础上增加 sentinel 投递条件分支（启用 checkpointing-during-recovery 时不再走 receivedBuffers）。
- `LocalRecoveredInputChannel` / `RemoteRecoveredInputChannel`：物化 channel 的转换路径接入新接口。
- `SingleInputGate` 增加 final-drain 开关、`convertRecoveredInputChannels` 调整、`UnknownInputChannel` / `IndexedInputGate` / `InputGateWithMetrics` 跟进。
- 全部相关测试与 mock 跟进 push 模型。

### Phase 3：SpillFile + filter writer `4121ded`

**Subject**：`[FLINK-38544][checkpoint] Phase 3: SpillFile + filter writer phase`

把"recover 期数据先写盘"这条路径落地：

- 新增 `SpillFile`：分段写盘的 recover-data 容器，支持 append / snapshot / ref-count。
- 新增 `SpillFileWriter`：往 `SpillFile` 追加 buffer 的 writer 抽象。
- 新增 `FilteredBufferWriter`：filter 阶段把 buffer 路由进 `SpillFile`。
- 改造 `ChannelStateFilteringHandler` / `RecoveredChannelStateHandler`：filter-on 路径下，buffer 不再 pin 到堆，统一写 `SpillFile`。
- `SequentialChannelStateReaderImpl` 跟进 filter 路径产物。

测试新增覆盖 SpillFile 行为、Filter 路由、buffer 所有权语义。

### Phase 4：spill reader drain + 移除 heap fallback `4ebb644`

**Subject**：`[FLINK-38544][checkpoint] Phase 4: spill reader drain + heap fallback removal`

消费端 spill reader 与 drain 流程的实装，并彻底移除原来的 heap fallback：

- 新增 `SpillFileReader`：从 `SpillFile` 读出 buffer 并 push 到 `RecoverableInputChannel`，支持并发的 `snapshotAndInsertBarriers` 与 drain 互斥。
- 新增 `SpillFileReaderBootstrap`：组装 SpillFileReader 的引导逻辑。
- 新增 `RecoveredChannelBufferRequester`：drain 阶段向 channel 的 buffer pool 申请 buffer。
- `SequentialChannelStateReader` 新增 `getProducedSpillFile()` 暴露 filter 产物。
- `RecoveredInputChannel` 改为按需 (lazy) 申请 exclusive buffer，避免为无恢复数据的 channel 预占池。
- `StreamTask` 接入新的恢复主流程：构造 SpillFileReader → 发布 trigger → drain → close。原来需要堆兜底的路径被移除。
- 新增针对 drain 顺序、并发、snapshot ref-count、heap fallback 已移除的测试。

### Phase 5：checkpoint 三步协调 `0b90235`

**Subject**：`[FLINK-38544][checkpoint] Phase 5: checkpoint 3-step coordination`

把恢复期间 checkpoint 与 spill reader / filter writer 的协调串起来：

- `ChannelStateWriterImpl` 实现 `addInputDataFromSpill`，把 `DiskSnapshot` 通过既有 writer 队列写入。
- 新增 `ChannelStateWriteRequest.replayInputDataFromSpill`（写请求种类）和 `ChannelStateCheckpointWriter` 的执行分支。
- 新增 `ChannelState`（流式 checkpoint 侧的恢复期 channel-state 视图）。
- `AlternatingCollectingBarriers` / `AlternatingWaitingForFirstBarrierUnaligned` / `SingleCheckpointBarrierHandler` / `InputProcessorUtil`：接入 dispatch hook，使得恢复期 checkpoint 能在 barrier handler 收到 barrier 时调用 `recoveryCheckpointTrigger.snapshotAndInsertBarriers(...)`。
- `MultipleInputStreamTask` / `OneInputStreamTask` / `TwoInputStreamTask`：把 trigger 透传给 barrier handler 工厂。
- 大量集成测试（rescale OOM 回归、unaligned-checkpoint during recovery、dispatch hook 等）覆盖三步协议。

---

## 2. 每个 phase commit 的文件清单

### Phase 0：`129d7b3` (1 个文件)

| 文件 | 类型 |
|------|------|
| `flink-runtime/.../partition/consumer/LocalInputChannel.java` | 改 |

### Phase 1：`b892c23` (8 个文件)

| 文件 | 类型 |
|------|------|
| `flink-runtime/.../checkpoint/channel/BufferRequester.java` | 新增 |
| `flink-runtime/.../checkpoint/channel/ChannelStateWriter.java` | 改 |
| `flink-runtime/.../checkpoint/channel/DiskSnapshot.java` | 新增 |
| `flink-runtime/.../checkpoint/channel/RecoveryCheckpointBarrier.java` | 新增 |
| `flink-runtime/.../checkpoint/channel/RecoveryCheckpointTrigger.java` | 新增 |
| `flink-runtime/.../partition/consumer/RecoverableInputChannel.java` | 新增 |
| `flink-runtime/.../partition/consumer/RecoveredInputChannel.java` | 改 |
| `flink-runtime/.../test/.../channel/MockChannelStateWriter.java` | 改 |

### Phase 2：`7cff232` (24 个文件)

生产代码（10）：
- `flink-runtime/.../partition/consumer/IndexedInputGate.java`
- `flink-runtime/.../partition/consumer/LocalInputChannel.java`
- `flink-runtime/.../partition/consumer/LocalRecoveredInputChannel.java`
- `flink-runtime/.../partition/consumer/RecoveredBufferQueue.java` (新增)
- `flink-runtime/.../partition/consumer/RecoveredInputChannel.java`
- `flink-runtime/.../partition/consumer/RemoteInputChannel.java`
- `flink-runtime/.../partition/consumer/RemoteRecoveredInputChannel.java`
- `flink-runtime/.../partition/consumer/SingleInputGate.java`
- `flink-runtime/.../partition/consumer/UnknownInputChannel.java`
- `flink-runtime/.../taskmanager/InputGateWithMetrics.java`

测试代码（14）：
- `CreditBasedPartitionRequestClientHandlerTest.java`
- `PartitionRequestRegistrationTest.java`
- `InputChannelBuilder.java`
- `LocalInputChannelTest.java`
- `LocalRecoveredInputChannelTest.java` (新增)
- `RecoveredInputChannelTest.java`
- `RemoteInputChannelTest.java`
- `RemoteRecoveredInputChannelTest.java` (新增)
- `TestInputChannel.java`
- `UnionInputGateTest.java`
- `MockIndexedInputGate.java`
- `MockInputGate.java`
- `SingleInputGateBenchmarkFactory.java`
- `AlignedCheckpointsMassiveRandomTest.java`

### Phase 3：`4121ded` (13 个文件)

生产代码（6）：
- `flink-runtime/.../checkpoint/channel/ChannelStateFilteringHandler.java`
- `flink-runtime/.../checkpoint/channel/FilteredBufferWriter.java` (新增)
- `flink-runtime/.../checkpoint/channel/RecoveredChannelStateHandler.java`
- `flink-runtime/.../checkpoint/channel/SequentialChannelStateReaderImpl.java`
- `flink-runtime/.../checkpoint/channel/SpillFile.java` (新增)
- `flink-runtime/.../checkpoint/channel/SpillFileWriter.java` (新增)

测试代码（7）：
- `FilteredBufferWriterTest.java` (新增)
- `GateFilterHandlerBufferOwnershipTest.java`
- `GateFilterHandlerTest.java`
- `InputChannelRecoveredStateHandlerTest.java`
- `RecoveredChannelStateHandlerFilterRoutingTest.java` (新增)
- `SpillFileTest.java` (新增)
- `SpillFileWriterTest.java` (新增)

### Phase 4：`4ebb644` (12 个文件)

生产代码（6）：
- `flink-runtime/.../checkpoint/channel/RecoveredChannelBufferRequester.java` (新增)
- `flink-runtime/.../checkpoint/channel/SequentialChannelStateReader.java`
- `flink-runtime/.../checkpoint/channel/SpillFileReader.java` (新增)
- `flink-runtime/.../checkpoint/channel/SpillFileReaderBootstrap.java` (新增)
- `flink-runtime/.../partition/consumer/RecoveredInputChannel.java`
- `flink-runtime/.../streaming/runtime/tasks/StreamTask.java`

测试代码（6）：
- `ChannelIOExecutorDrainSubmissionTest.java` (新增)
- `DiskSnapshotTest.java` (新增)
- `SpillFileReaderConcurrencyTest.java` (新增)
- `SpillFileReaderTest.java` (新增)
- `SpillFileSnapshotTest.java` (新增)
- `RecoveredInputChannelRequestBufferBlockingHeapFallbackRemovedTest.java` (新增)

### Phase 5：`0b90235` (19 个文件)

生产代码（11）：
- `flink-runtime/.../checkpoint/channel/ChannelStateCheckpointWriter.java`
- `flink-runtime/.../checkpoint/channel/ChannelStateWriteRequest.java`
- `flink-runtime/.../checkpoint/channel/ChannelStateWriterImpl.java`
- `flink-runtime/.../io/checkpointing/AlternatingCollectingBarriers.java`
- `flink-runtime/.../io/checkpointing/AlternatingWaitingForFirstBarrierUnaligned.java`
- `flink-runtime/.../io/checkpointing/ChannelState.java` (新增)
- `flink-runtime/.../io/checkpointing/InputProcessorUtil.java`
- `flink-runtime/.../io/checkpointing/SingleCheckpointBarrierHandler.java`
- `flink-runtime/.../streaming/runtime/tasks/MultipleInputStreamTask.java`
- `flink-runtime/.../streaming/runtime/tasks/OneInputStreamTask.java`
- `flink-runtime/.../streaming/runtime/tasks/TwoInputStreamTask.java`

测试代码（8）：
- `ChannelStateWriterImplAddInputDataFromSpillTest.java` (新增)
- `RescaleFilterLargeRecordOOMRegressionITCase.java` (新增)
- `SpillFileRefCountTest.java` (新增)
- `UnalignedCheckpointDuringRecoveryITCase.java` (新增)
- `AlternatingCollectingBarriersDispatchHookTest.java` (新增)
- `AlternatingWaitingForFirstBarrierUnalignedDispatchHookTest.java` (新增)
- `ChannelStateDispatcherTest.java` (新增)
- `TestBarrierHandlerFactory.java`

---

## 3. 跨 phase 重复修改的文件

通过文件级交叉对比，6 个 phase commits 中共有 **2 个文件**被多个 phase 修改：

| 文件 | 涉及 phase | 次数 |
|------|------------|------|
| `flink-runtime/.../partition/consumer/LocalInputChannel.java` | phase0 + phase2 | 2 |
| `flink-runtime/.../partition/consumer/RecoveredInputChannel.java` | phase1 + phase2 + phase4 | 3 |

### 3.1 `LocalInputChannel.java`（phase0 + phase2）

这是 `LocalInputChannel` 的核心实现类，承担本地 result partition 的 buffer 消费、checkpoint 持久化、subpartition view 协调。

- **phase 0 (`129d7b3`)**：纯前置 refactor。把单一队列 `toBeConsumedBuffers` 拆为 `recoveredBuffers` + `toBeConsumedBuffers`，并恢复 `requestSubpartitions()` 里的 `checkState(toBeConsumedBuffers.isEmpty())` guard。**不增加新行为**，只让代码结构与后续 phase 对齐。该 phase 引入的所有注释（5 处）在 phase 2 中被全部覆盖重写。
- **phase 2 (`7cff232`)**：在 phase 0 的结构基础上让 `LocalInputChannel` 实现 `RecoverableInputChannel`。把 phase 0 的 `recoveredBuffers` 替换为 `RecoveredBufferQueue`（含状态机和监视器），引入 `upstreamReady` future 同步 subpartition view 发布，重写 `getNextBuffer()` 为 in-recovery / post-recovery 双分支，新增 `onRecoveredStateBuffer` / `finishRecoveredBufferDelivery` / `awaitUpstreamReady` / `isInRecovery` 等 push-based 入口。这是该文件**实际运行语义的变化所在**。

**归属说明**：因为 phase 2 完全覆盖了 phase 0 的注释，所以注释规范化 fix 时该文件**整体只能归到 phase 2**，phase 0 部分的注释优化在物理上无法挂回 phase 0 commit。

### 3.2 `RecoveredInputChannel.java`（phase1 + phase2 + phase4）

`RecoveredInputChannel` 是恢复期占位 channel 的抽象基类，最终在 `convertToInputChannelInternal` 时被替换成真正的 `LocalInputChannel` 或 `RemoteInputChannel`。它是整条 spilling v2 链路上少数横跨多个 phase 的核心类，因此是 `fix_commit_convention.md` 中明确列出的**唯一文件归属例外**。

- **phase 1 (`b892c23`)**：让该抽象类实现 phase 1 引入的 `RecoverableInputChannel` 接口契约（接口本身在 phase 1 引入）。改动极小，仅做接口对齐。
- **phase 2 (`7cff232`)**：实质改动落在 `finishReadRecoveredState` 与 `convertToInputChannelInternal`，按 `checkpointingDuringRecoveryEnabled` 区分两条分支：
  - 启用时：filter 路径已把 buffer 写入 `SpillFile`，`receivedBuffers` 不再被推入，sentinel 之后由物理 channel 的 `recoveredQueue` 投递。
  - 未启用时：保留原行为，使用 `receivedBuffers` 与 `stateConsumedFuture`。
  
  并新增 `receivedBuffers.isEmpty()` 不变式校验，作为两条路径汇合点的安全网。
- **phase 4 (`4ebb644`)**：新增 lazy exclusive-buffer assignment 注释及相关行为——只在真有恢复数据要消费时才向 buffer pool 申请 exclusive buffer，避免无 recover 数据的 channel 白占池。

**归属说明**：注释规范化 fix 时，该文件按 `git blame` 行号严格按每段注释的最后修改者归属：121-124 与 192-194 行的 invariant / 分支区分注释由 phase 2 引入并保留，归 phase 2；140-142 行的 `toInputChannelInternal` 方法 javadoc 同样由 phase 2 引入，归 phase 2；323-324 行的 lazy buffer 注释由 phase 4 引入，归 phase 4。phase 1 引入的内容在后续 phase 中已被覆盖，因此 phase 1 在该文件上无注释 fix 改动。

---

## 4. 当前分支状态

- 分支：`38544-spilling-v2/20260528-01-polish-comments-organize-commits`
- 注释规范化 fix 已全部 squash 进对应 phase commit，分支历史保持 6 个干净的 phase commit。
- 工作树 tree hash 与 fix 应用完成时的状态一致，无残留 fix commit。
