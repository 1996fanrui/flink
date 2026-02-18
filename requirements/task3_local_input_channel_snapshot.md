# Task 3: LocalInputChannel Snapshot 支持 - 详细设计

**汇总文档**: [split_tasks.md](./split_tasks.md)

## 背景

RecoveredInputChannel 在异步线程中完成 Buffer 过滤后，会转换为物理 Channel（LocalInputChannel / RemoteInputChannel）。转换时，RecoveredInputChannel 中可能仍有已过滤但未被 Task 线程消费的 Buffer。这些 Buffer 需要迁移到物理 Channel，并确保物理 Channel 在 Checkpoint 时能正确快照它们。

本文档记录 LocalInputChannel 为支持 Snapshot 所需的全部改动。

## 1. Buffer 迁移机制

### 问题

RecoveredInputChannel 转换为 LocalInputChannel/RemoteInputChannel 时，`receivedBuffers` 中可能仍有已过滤但未消费的 Buffer。原始的转换逻辑不处理这些剩余 Buffer。

### 解决方案

- `RecoveredInputChannel.toInputChannel()` 提取 `receivedBuffers` 中剩余的 Buffer，传递给物理 Channel 构造器
- `LocalRecoveredInputChannel.toInputChannelInternal()` 将剩余 Buffer 传给 `LocalInputChannel`，添加到 `toBeConsumedBuffers` 队列
- `RemoteRecoveredInputChannel.toInputChannelInternal()` 将剩余 Buffer 传给 `RemoteInputChannel`，添加到 `receivedBuffers` 队列
- `SingleInputGate.convertRecoveredInputChannels()` 在 `synchronized(inputChannelsWithData)` 块中完成转换，确保 channel 队列的移除和重新入队原子性

### RemoteInputChannel 适配

`RemoteInputChannel.getNextBuffer()` 和 `peekNextBufferSubpartitionIdInternal()` 原来要求 `partitionRequestQueue` 已初始化。migrated buffer 可能在 `requestSubpartitions()` 之前就需要被读取，因此将初始化检查改为仅检查错误状态。

## 2. Checkpoint 快照支持

### 问题

原始 `LocalInputChannel.checkpointStarted()` 传递空列表给 `channelStatePersister`，不会快照 `toBeConsumedBuffers` 中的 Buffer。

### 解决方案

- `checkpointStarted()` 遍历 `toBeConsumedBuffers`，收集所有 data buffer（`retainBuffer()`），传给 `channelStatePersister.startPersisting()`
- `getBufferAndAvailability()` 中调整 `channelStatePersister.checkForBarrier()` 和 `maybePersist()` 的调用位置，确保时序正确
- `getBuffersInUseCount()` 计数包含 `toBeConsumedBuffers.size()`

## 3. Priority Event 优先处理

### 问题

当 `toBeConsumedBuffers` 不为空时，Checkpoint Barrier 从 `subpartitionView` 到达。但 `getNextBuffer()` 会先消费 `toBeConsumedBuffers`，导致 Barrier 被延迟处理。

### 解决方案

- 新增 `hasPendingPriorityEvent` volatile 标志
- 重写 `notifyPriorityEvent()`，设置标志位通知有优先级事件待处理
- `getNextBuffer()` 在 `toBeConsumedBuffers` 不为空时检查该标志，优先从 `subpartitionView` 获取 Barrier
- 修正 `nextDataType`：从 `subpartitionView` 获取 Barrier 后，如果 `toBeConsumedBuffers` 仍有数据，`nextDataType` 应指向 `toBeConsumedBuffers` 的头部而非 `subpartitionView` 的下一个元素

## 4. Buffer 可用性修正

### 问题

最后一个 recovered buffer 的 `nextDataType` 在构造时被设为 `NONE`（因为后面没有更多 recovered buffer）。但此时 `subpartitionView` 可能已有上游发来的数据可用，导致 Task 线程认为没有数据而停止消费。

### 解决方案

在消费最后一个 recovered buffer 时，动态检查 `subpartitionView.getAvailabilityAndBacklog()` 是否有数据，若有则修正 `nextDataType`。

## 相关 Commits

- `822d80b9`: POC 实现 + 修复 filter 前 buffer 申请完 network memory 导致 deadlock
- `59cba66c`: 处理 LocalInputChannel 里 buffer 消费顺序 + priority event 问题
- `0c07ef39`: 修复 Local Buffer Pool 没有 snapshot 的问题 + priority event 优先处理
- `9543238e`: 完善 recovered input buffer 中 filtered buffers 迁移的功能
