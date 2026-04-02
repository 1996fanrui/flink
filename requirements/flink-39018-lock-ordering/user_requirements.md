# 需求偏离

无

# 用户需求

## REQ-GDU7 消除 `convertRecoveredInputChannels()` 中的锁顺序反转

**背景**：FLINK-39018 PR（https://github.com/apache/flink/pull/27861#discussion_r3026926909）中，`SingleInputGate.convertRecoveredInputChannels()` 在持有 `inputChannelsWithData` 锁的情况下调用 `toInputChannel()` 和 `releaseAllResources()`，这两个方法内部会获取 `receivedBuffers` 锁（锁顺序 A→B）。而 `RecoveredInputChannel.onRecoveredStateBuffer()` 的锁顺序为 B→A。当前代码通过注释声称两个路径不会并发执行来回避死锁，但 reviewer pnowojski 认为依赖时序假设是脆弱的设计。

**要求**：缩小 `convertRecoveredInputChannels()` 中 `inputChannelsWithData` 锁的持有范围，将 `toInputChannel()` 和 `releaseAllResources()` 移到锁外执行，从结构上消除锁顺序反转，使代码不依赖时序假设即可保证安全。

## REQ-V9VD 删除已失效的锁顺序注释

修复锁顺序后，`convertRecoveredInputChannels()` 方法上的 "Lock ordering note" Javadoc 注释不再适用，需要删除该注释。
