# 用户需求 — Phase 2：InputChannel 侧（task thread 一侧）

## 需求偏离

无。

## 背景

[`simplify_approach/input_channel.md`](../simplify_approach/input_channel.md) 规定了 task-thread 一侧的全部改动：三个 channel（`RecoveredInputChannel`、`LocalInputChannel`、`RemoteInputChannel`）通过实现 Phase 1 引入的 `RecoverableInputChannel` 接口完成 recovery 数据的接收、消费、checkpoint 持久化、`stateConsumedFuture` 完成。Phase 2 完整实施 input_channel.md §3 全部内容，**与 Phase 3（spill writer）并行**——双方各自只依赖 Phase 1 的接口。

[`decouple_toBeConsumedBuffers/`](../decouple_toBeConsumedBuffers/) 已经在 `LocalInputChannel` 内拆出一个 `Deque<BufferAndBacklog> recoveredBuffers` 字段（迁移自 `toBeConsumedBuffers`），但元素类型是 `BufferAndBacklog`、消费分支仍是 `getNextRecoveredBuffer()`、字段语义是 FLINK-39018 的 pull 迁移路径。Phase 2 在该重构之上**重塑字段语义**：把 `recoveredBuffers` 改成 `Deque<Buffer>`，新增 `allRecoveredBuffersDelivered` 状态机字段，把 push 路径作为唯一 producer 入口。

## 需求

- **REQ-Y6OP** `LocalInputChannel` 与 `RemoteInputChannel` 都新增 `recoveredBuffers: Deque<Buffer>` 与 `allRecoveredBuffersDelivered: boolean` 两个字段；`recoveredBuffers` 仅承载 recovery 期间通过 `onRecoveredStateBuffer` 投递的 `Buffer`（含 `RecoveryCheckpointBarrier` sentinel）。`LocalInputChannel` 中原本的 `Deque<BufferAndBacklog> recoveredBuffers` 字段类型必须改为 `Deque<Buffer>`。
- **REQ-D62D** `RecoveredInputChannel`、`LocalInputChannel`、`RemoteInputChannel` 必须 `implements RecoverableInputChannel`；`LocalInputChannel` 与 `RemoteInputChannel` 新增的 `onRecoveredStateBuffer(Buffer)`、`finishReadRecoveredState()` 必须分别按 `simplify_approach/coordination.md` §1 锁前置条件实现；`RecoveredInputChannel` 既有同名 public 方法直接满足接口契约，不需要改动方法体（除接口声明外）。
- **REQ-GVDT** `LocalInputChannel.toBeConsumedBuffers` 与 `recoveredBuffers` 的锁复用规则：Remote 复用 master 既有 `synchronized(receivedBuffers)`；Local 用 `synchronized(recoveredBuffers)`——禁止引入第三个锁对象。`onRecoveredStateBuffer` 内部必须取该 channel 自身 monitor 并在末尾调用 `notifyChannelNonEmpty()`（若队列原先为空），保证既有唤醒链路不变。
- **REQ-IQDA** `LocalInputChannel.getNextBuffer()` 与 `RemoteInputChannel.getNextBuffer()` 改为单一 `inRecovery = !allRecoveredBuffersDelivered || !recoveredBuffers.isEmpty()` 判定：
  - `inRecovery && hasPendingPriorityEvent`：走优先事件路径（Remote 经 `receivedBuffers` 头位 priority 项，Local 经 `subpartitionView`）
  - `inRecovery && recoveredBuffers 非空`：弹出 `recoveredBuffers` 队首 `Buffer`，按现有 channel state persister / metrics 等链路包装为 `BufferAndAvailability`
  - `inRecovery && recoveredBuffers 空`：返回 `Optional.empty()`，阻塞普通 upstream 数据
  - `!inRecovery`：走 master 既有路径
- **REQ-G319** `LocalInputChannel.checkpointStarted` 与 `RemoteInputChannel.checkpointStarted` 改写为 in-recovery / not-in-recovery 互斥两分支：
  - in-recovery 分支按 `simplify_approach/input_channel.md` §3.8 走 `recoveredBuffers` 扫描直到 `RecoveryCheckpointBarrier(cpId)` sentinel，将 pre-barrier buffer `retainBuffer()` 后交给 `channelStateWriter.addInputData(...)`；该 sentinel 自身从队列移除
  - 非 in-recovery 分支：Remote 保留 master 既有 receivedBuffers 持久化路径；Local 保留 master 既有 `channelStatePersister.startPersisting(barrier.getId(), Collections.emptyList())`
  - 两分支互斥：in-recovery 时禁止任何上游 live data buffer 同时存在；通过 channel-internal `receivedBuffersHasNoLiveDataBuffer()` 断言守护（Local 实现恒为 true，Remote 实现遍历 `receivedBuffers` 验证 `!Buffer.isBuffer()`）
- **REQ-RYGK** `LocalInputChannel` 与 `RemoteInputChannel` 必须以 `allRecoveredBuffersDelivered && recoveredBuffers.isEmpty()` 作为唯一触发条件完成 `stateConsumedFuture`；触发时机由"最后一次让两者都为 true 的状态翻转"负责（要么 `finishReadRecoveredState()` 在 `recoveredBuffers` 已为空时翻转标志，要么 `getNextBuffer()` 在标志已 true 时弹出最后一项）。禁止往队列插入 EOICS sentinel 来表达完成。
- **REQ-Y4RX** `RecoveredInputChannel.toInputChannel()` 内"把剩余 buffer 交给物理 channel"的迁移过程必须改走新接口：对 `remainingBuffers` 中每个 buffer 调用物理 channel 的 `onRecoveredStateBuffer(buffer)`，遍历完成后调用 `finishReadRecoveredState()`。原本通过构造器 `ArrayDeque<Buffer> initialRecoveredBuffers` 参数完成的"一次性塞队列"路径必须删除；`LocalInputChannel`、`RemoteInputChannel` 构造器移除该参数。
- **REQ-TWEE** `LocalInputChannel.toBeConsumedBuffers` 字段恢复为 FullyFilledBuffer splits 专用：`getNextBuffer()` 中针对 `FullyFilledBuffer` 的拆分逻辑保留，但只有在非 recovery 阶段才可能进入；recovery 期间 `subpartitionView` 路径被 `inRecovery` 守护，`toBeConsumedBuffers` 不可能积累 recovery 数据。
- **REQ-YW7I** `LocalInputChannel` 删除以下 FLINK-39018 / decouple 阶段引入的辅助逻辑（这些逻辑被 Phase 2 的统一 push 路径替代）：私有方法 `getNextRecoveredBuffer()`、构造器内 `BufferAndBacklog` 包装与序列号递增的迁移块、构造器 `initialRecoveredBuffers` 参数本身（与 REQ-Y4RX 协同）。`hasPendingPriorityEvent` / `notifyPriorityEvent` 保留语义但读点改为新的 in-recovery 分支。

## 显式不在范围

- 不引入 `SpillFile` / `SpillFileWriter` / `SpillFileReader` / `FilteredBufferWriter` / `RecoveredChannelBufferRequester`（Phase 3/4）
- 不修改 `ChannelState.onCheckpointStartedForAllInputs` 或 `Alternating*` 入口（Phase 5）
- 不删除 `RecoveredInputChannel.requestBufferBlocking` 中的 heap fallback（Phase 4）
- 不实现 `ChannelStateWriter.addInputDataFromSpill` 真实逻辑（Phase 5）；本 phase 引用现有 `addInputData` 即可
- 不引入 ITCase；本 phase 仅在 `LocalInputChannelTest` / `RemoteInputChannelTest` 等单元测试层面验证
