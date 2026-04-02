# 设计文档：消除 `convertRecoveredInputChannels()` 锁顺序反转

## 问题分析

### 涉及的两把锁

| 锁 | 所在类 | 保护的资源 |
|---|---|---|
| `inputChannelsWithData` | `SingleInputGate` | channel 的数据就绪队列 |
| `receivedBuffers` | `RecoveredInputChannel` | channel 接收到的 buffer 队列 |

### 当前锁顺序冲突

**路径 1 — `convertRecoveredInputChannels()`**（`SingleInputGate.java:404`）：
- `synchronized(inputChannelsWithData)` → 调用 `toInputChannel()`（内部 `synchronized(receivedBuffers)`）→ 调用 `releaseAllResources()`（内部 `synchronized(receivedBuffers)`）
- 锁顺序：**A → B**

**路径 2 — `onRecoveredStateBuffer()`**（`RecoveredInputChannel.java:156`）：
- `synchronized(receivedBuffers)` → 调用 `notifyChannelNonEmpty()` → `queueChannel()` → `synchronized(inputChannelsWithData)`
- 锁顺序：**B → A**

经典死锁模式。当前代码通过 Javadoc 注释声称安全（基于时序假设），被 reviewer 否决。

### 举一反三排查

排查 `SingleInputGate` 中所有持有 `inputChannelsWithData` 锁时调用 `InputChannel` 方法的位置：

| 方法 | 是否在锁内调用 `receivedBuffers` 保护的方法 | 状态 |
|---|---|---|
| `convertRecoveredInputChannels()` | 是 — `toInputChannel()`, `releaseAllResources()`, `getBuffersInUseCount()` | **需修复** |
| `close()` | 否 — `releaseAllResources()` 在 `requestLock` 下调用，`inputChannelsWithData` 仅用于 `notifyAll()` | 安全 |
| `transformEvent()` (EndOfPartitionEvent) | 否 — `releaseAllResources()` 在锁外调用 | 安全 |
| `waitAndGetNextData()` | 否 — 仅 dequeue，不调用锁 B 保护的方法 | 安全 |
| 其他 (`getUnfinishedChannels`, `markAvailable`, `queueChannel`) | 否 | 安全 |

**结论**：锁顺序反转仅存在于 `convertRecoveredInputChannels()` 一处。

## 修复方案

遵循业界 "open calls" 模式（参见 `industry_research/lock_ordering_deadlock_prevention.md`，CERT LCK07-J）：将 `toInputChannel()` 和 `releaseAllResources()` 移到 `inputChannelsWithData` 锁外执行。

### 重构后的 `convertRecoveredInputChannels()` 结构

将方法内循环体拆为两个阶段：

**阶段 1 — 锁外：channel 转换与资源释放**
- 调用 `((RecoveredInputChannel) inputChannel).toInputChannel()` 创建新的物理 channel 并提取剩余 buffer
- 调用 `inputChannel.releaseAllResources()` 释放旧 channel 资源
- 调用 `realInputChannel.getBuffersInUseCount()` 获取新 channel 的缓冲数据量

以上三个调用均可能获取 `receivedBuffers` 锁，移到锁外后消除嵌套。

**阶段 2 — 锁内：原子更新数据结构**
- `synchronized(inputChannelsWithData)` 内执行：
  - 从 `inputChannelsWithData` 队列移除旧 channel
  - 清除 `enqueuedInputChannelsWithData` 位
  - 更新 `inputChannelsForCurrentPartition` map（移除旧、添加新）
  - 更新 `channels` 数组
  - 如果新 channel 有缓冲数据，将其加入 `inputChannelsWithData` 队列并设置位

### 安全性分析

**为什么阶段 1 在锁外执行是安全的**：

#### 1. 不会有新 buffer 添加到旧 channel

`convertRecoveredInputChannels()` 仅在 `requestPartitions()` 中被调用（`SingleInputGate.java:364`）。`requestPartitions()` 通过 `stateConsumedFuture` 的回调触发（`StreamTask.java:903-917`）：状态恢复完成 → `finishReadRecoveredState()` 发送 `EndOfInputChannelStateEvent` 并标记 `stateConsumedFuture.isDone()` → 之后 `RecoveredChannelStateHandler` 不再调用 `onRecoveredStateBuffer()`。`toInputChannel()` 内部通过 `Preconditions.checkState(stateConsumedFuture.isDone())` 断言此前置条件。

#### 2. 新 channel 无竞争

`getBuffersInUseCount()` 操作的是阶段 1 新创建的物理 channel。该 channel 尚未写入 `channels` 数组和 `inputChannelsWithData` 队列（在阶段 2 才更新），因此无其他线程持有引用，不存在竞争。

#### 3. 阶段 1 期间旧 channel 仍在队列中 — 线程模型保证安全

**主要保证 — Mailbox 单线程模型**：`convertRecoveredInputChannels()` 通过 `mainMailboxExecutor.execute(inputGate::requestPartitions)` 在 mailbox 线程执行（`StreamTask.java:910`）。`getNextBufferOrEvent()` / `waitAndGetNextData()` 同样在 mailbox 线程执行 — 调用链：`StreamTask.invoke()` → `runMailboxLoop()` → `MailboxProcessor.runMailboxStep()` → `StreamTaskNetworkOutput.emitRecord()` → `StreamInputProcessor.processInput()` → `getNextBufferOrEvent()`。`MailboxExecutorImpl` 保证同一时刻只有一个 mail 在执行（单线程串行），因此 `requestPartitions()`（包含 `convertRecoveredInputChannels()`）作为一个 mail 执行期间，`waitAndGetNextData()` 不可能并发运行。这是 Flink mailbox 线程模型的结构性保证，不是脆弱的时序假设。阶段 1 与阶段 2 之间也不存在中断点，整个 `convertRecoveredInputChannels()` 方法在单个 mail 执行中完成。

**纵深防御 — 即使假设存在并发读取**：
- `toInputChannel()` 清空旧 channel 的 `receivedBuffers` 后，`getNextRecoveredStateBuffer()` 中 `receivedBuffers.poll()` 返回 null → `getNextBuffer()` 返回 `Optional.empty()` → `waitAndGetNextData()` 的循环继续处理下一个 channel，不会产生异常或状态错误
- `releaseAllResources()` 设置 `isReleased = true` 后，`readRecoveredOrNormalBuffer()` 中 `!inputChannel.isReleased()` 检查会跳过该 channel

#### 4. 阶段 1 期间 `queueChannel()` 的幂等性

如果在阶段 1 执行期间有其他线程（如网络线程处理 priority event）对旧 channel 调用 `queueChannel()`，`queueChannelUnsafe()` 会检查 `enqueuedInputChannelsWithData` 位（`SingleInputGate.java:1307`），如果已设置则直接返回 false，不会重复入队。阶段 2 的锁内清除该位并将旧 channel 从 `channels` 数组替换为新 channel 后，不会再有任何代码路径持有旧 channel 引用来触发 `queueChannel()`（`onRecoveredStateBuffer()` 已因状态恢复完成而不再调用，旧 channel 的 `notifyBufferAvailable()` 因 `isReleased=true` 而不会通知）。

### 锁顺序修复后的状态

修复后所有涉及这两把锁的代码路径：
- `onRecoveredStateBuffer()`：B → A（不变）
- `convertRecoveredInputChannels()`：仅 A（阶段 2），B 在锁外独立获取（阶段 1）
- 不存在 A → B 的嵌套路径，锁顺序反转彻底消除

### 验证方法

- **Code Review**：确认 `convertRecoveredInputChannels()` 的 `synchronized(inputChannelsWithData)` 块内不包含任何可能获取 `receivedBuffers` 锁的方法调用（即上文举一反三排查表的更新版本）
- **现有测试回归**：运行 `SingleInputGateTest`、`RecoveredInputChannelTest` 等相关测试确保行为不变

## 修改范围

| 文件 | 修改内容 |
|---|---|
| `SingleInputGate.java` | 重构 `convertRecoveredInputChannels()` 方法：将 `toInputChannel()`、`releaseAllResources()`、`getBuffersInUseCount()` 移到 `synchronized(inputChannelsWithData)` 块之前；删除方法上的 "Lock ordering note" Javadoc |

仅涉及一个文件的一个方法的内部重构，不改变外部行为。
