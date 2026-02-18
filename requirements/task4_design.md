# Task 4: Change the Overall UC Restore Process - 详细设计

## 概述

本文档描述 Task 4 的详细技术设计，核心目标是变更 UC 恢复流程，延迟 Task 进入 RUNNING 状态的时机，使得 Buffer 过滤完成后才能触发 Checkpoint。

---

## 1. 配置开关

**所有 Task 4 的变更仅在以下配置开启时生效：**

```
execution.checkpointing.unaligned.during-recovery.enabled = true
```

默认值为 `false`，即默认不启用。当配置为 `false` 时，所有现有逻辑保持不变。

---

## 2. 核心变更

### 2.1 核心思路

**Checkpoint trigger 逻辑不变**，仍然是所有 Task 进入 RUNNING 后触发。

**核心变化是 RUNNING 状态转换时机**：

| | 配置关闭（当前行为） | 配置开启（Task 4） |
|---|---|---|
| **RUNNING 转换时机** | `stateConsumedFuture` 完成后 | `bufferFilteringCompleteFuture` 完成后 |
| **等待内容** | 恢复的 buffer 数据处理完成 | 恢复的 buffer 过滤完成（不处理数据） |
| **效果** | RUNNING 转换较慢 | RUNNING 转换更快，Checkpoint 更早触发 |

配置开启时，由于 buffer filtering 只做序列化过滤+反序列化（不处理数据），速度很快，RUNNING 转换和 Checkpoint 触发都会更早。

### 2.2 两个 Future 的设计

| Future | 完成时机 | 说明 |
|--------|----------|------|
| `stateConsumedFuture` | 恢复的 buffer **数据处理完成** | 现有逻辑，需要等待数据真正被消费处理，**无论配置是否开启都会完成** |
| `bufferFilteringCompleteFuture` (新增) | 恢复的 buffer **过滤完成** | **仅在配置开启时完成**；只做序列化过滤+反序列化，不处理数据，交给 Task 线程即完成 |

**重要：`bufferFilteringCompleteFuture` 的语义**

从命名上看，`bufferFilteringCompleteFuture` 表示 "buffer filtering 逻辑完成"。因此：
- **配置开启时**：执行 buffer filtering 逻辑，完成后 complete 此 future
- **配置关闭时**：没有 buffer filtering 逻辑，**不应该 complete 此 future**

这意味着在 `InputChannelRecoveredStateHandler.close()` 中，只有当 `filteringHandler != null`（即功能开启）时才应该调用 `finishReadRecoveredState()` 来完成 `bufferFilteringCompleteFuture`。配置关闭时，`finishReadRecoveredState()` 不应该完成 `bufferFilteringCompleteFuture`。

**配置开启时的优势：**
- Buffer filtering 只做过滤，不处理数据，速度很快（前提是 buffer 内存足够）
- 过滤完成后立即切换 RUNNING，Checkpoint 更早触发
- 比等待数据处理完成（`stateConsumedFuture`）快得多

| | 配置关闭时 | 配置开启时 |
|---|------------|------------|
| **RUNNING 触发条件** | `stateConsumedFuture` 完成（数据处理完） | `bufferFilteringCompleteFuture` 完成（过滤完） |
| **`bufferFilteringCompleteFuture` 状态** | **不完成**（无 filtering 逻辑） | 完成 |
| **等待时间** | 较长（需处理数据） | 较短（只做过滤） |

**关键：两个 Future 都需要保留，根据配置选择使用哪个来触发 RUNNING 转换。**

---

## 3. 详细设计

### 3.1 RUNNING 状态转换时机变更（核心）

**当前逻辑：**

Task 在 `stateConsumedFuture` 完成后（恢复的 buffer 数据处理完成）切换到 RUNNING 状态。

**修改方案：**

在 `StreamTask` 中根据配置选择不同的 Future 来触发 RUNNING 转换：
- 配置开启：等待 `bufferFilteringCompleteFuture` 完成
- 配置关闭：等待 `stateConsumedFuture` 完成（保持原有行为）

### 3.2 两个 Future 的架构设计

**现有架构：**

`stateConsumedFuture` 位于 `RecoveredInputChannel` 类中，每个 Channel 有独立的 future。`SingleInputGate.getStateConsumedFuture()` 方法聚合所有 `RecoveredInputChannel` 的 future。

**新增 `bufferFilteringCompleteFuture`：**

采用相同架构：
- 在 `RecoveredInputChannel` 中新增 `bufferFilteringCompleteFuture` 字段
- 在 `SingleInputGate` 中新增 `getBufferFilteringCompleteFuture()` 方法，聚合所有 channel 的 future

**两个 Future 的完成时序（配置开启时）：**

```
Channel State 读取 → Buffer 过滤（序列化+反序列化）→ bufferFilteringCompleteFuture 完成
                                                              ↓
                                                    数据交给 Task 线程处理
                                                              ↓
                                                    数据处理完成 → stateConsumedFuture 完成
```

`bufferFilteringCompleteFuture` 先于 `stateConsumedFuture` 完成。

**配置关闭时的完成时序：**

```
Channel State 读取 → 数据交给 Task 线程处理 → 数据处理完成 → stateConsumedFuture 完成

（bufferFilteringCompleteFuture 不完成，因为没有 filtering 逻辑）
```

### 3.3 Channel 转换时的 Buffer 迁移（关键）

**问题背景：**

当 `requestPartitions()` 被调用时，`SingleInputGate.convertRecoveredInputChannels()` 会将 `RecoveredInputChannel` 转换为物理 channel（`RemoteInputChannel` 或 `LocalInputChannel`）。

**当前代码存在两个问题：**

1. **`toInputChannel()` 检查条件错误**：当前代码检查 `stateConsumedFuture.isDone()`，但配置开启时我们在 `bufferFilteringCompleteFuture` 完成后就调用 `requestPartitions()`，此时 `stateConsumedFuture` 尚未完成，会抛出异常。

2. **过滤后的 Buffer 丢失**：转换后会调用 `releaseAllResources()` 释放 `RecoveredInputChannel` 中所有剩余的 buffer。这些 buffer 虽然已完成过滤，但尚未被 Task 消费，会被直接丢弃。

**修改方案：通过构造器传递 Buffer**

1. 修改 `RecoveredInputChannel.toInputChannel()`：根据配置检查 `bufferFilteringCompleteFuture`（配置开启时）或 `stateConsumedFuture`（配置关闭时）。在调用 `toInputChannelInternal()` 前，取出 `receivedBuffers` 中所有剩余 buffer 并清空队列。

2. 修改 `toInputChannelInternal()` 签名：新增 `ArrayDeque<Buffer> remainingBuffers` 参数，由子类传递给物理 channel 的构造器。

3. 修改 `RemoteInputChannel` 和 `LocalInputChannel` 的现有构造器（不新增构造器）：新增 `@Nullable ArrayDeque<Buffer> initialRecoveredBuffers` 参数。构造器中将传入的 buffer 转换为各自的内部格式（`SequenceBuffer` 或 `BufferAndBacklog`）并加入队列。

4. 更新所有调用方：`LocalRecoveredInputChannel` 和 `RemoteRecoveredInputChannel` 传递 `remainingBuffers`；`UnknownInputChannel` 和 `InputChannelBuilder`（test）传递 `null`。

**Buffer 迁移时序：**

```
bufferFilteringCompleteFuture 完成
        ↓
requestPartitions() 调用
        ↓
convertRecoveredInputChannels()
        ↓
toInputChannel() → 取出 remainingBuffers → 创建新 channel（通过构造器传入 buffer）
        ↓
releaseAllResources()（此时 receivedBuffers 已空，无 buffer 被释放）
        ↓
Task 从新 channel 消费迁移过来的 buffer + 上游新发送的 buffer
```

### 3.4 `finishReadRecoveredState()` 行为修改

**当前问题**：

`RecoveredInputChannel.finishReadRecoveredState()` 无论配置是否开启都会完成 `bufferFilteringCompleteFuture`。

**修改方案**：

只有在配置开启时才完成 `bufferFilteringCompleteFuture`。

**配置标志传递**：

`RecoveredInputChannel` 需要获取 `isUnalignedDuringRecoveryEnabled` 配置标志。可通过 `SingleInputGate` 传递（`RecoveredInputChannel` 已持有 `inputGate` 引用）。`finishReadRecoveredState()` 和 `toInputChannel()` 根据配置执行不同逻辑。

---

### 3.5 防御性检查（Preconditions）

为防止潜在的 bug，在关键位置添加 `checkState` 检查：

1. **`RecoveredInputChannel.toInputChannel()` 入口检查**：在转换前根据配置检查相应 future 是否已完成。配置开启时检查 `bufferFilteringCompleteFuture`，配置关闭时检查 `stateConsumedFuture`。

2. **`toInputChannel()` 后验检查**：在方法末尾验证 `receivedBuffers` 已清空，确保所有 buffer 已成功迁移。

3. **物理 Channel 构造后检查**：验证队列中的 buffer 数量与传入的 `initialRecoveredBuffers` 数量一致。

**注意**：配置关闭时，`bufferFilteringCompleteFuture` 不会被完成，因此不能统一检查此 future。必须根据配置区分检查逻辑。

---

### 3.6 Block/Unblock 上游 Task

**方案：延迟 requestPartitions() 调用**

**当前逻辑：** `stateConsumedFuture` 完成后调用 `requestPartitions()`。

**修改方案：** 在 `StreamTask.restoreInternal()` 中根据配置选择触发条件：
- 配置开启：`bufferFilteringCompleteFuture` 完成后调用 `requestPartitions()`
- 配置关闭：`stateConsumedFuture` 完成后调用 `requestPartitions()`（保持原有行为）

**Barrier 缓存机制：**

- 上游发送的 Barrier 会被缓存在 `PipelinedSubpartition.buffers` 队列中
- 下游调用 `requestPartitions()` 后，通过 `pollBuffer()` 获取缓存的 Barrier
- 详细方案分析见 [task4_block_unblock_analysis.md](./task4_block_unblock_analysis.md)

**与原始需求文档的差异说明：**

原始需求文档 (requirement.md) 第 4.3.2 节描述了 "Request upstream partitions in the beginning"。本设计采用了延迟 `requestPartitions()` 的方案，原因是：

1. 经过代码调研验证，延迟调用 `requestPartitions()` 是可行的（详见 task4_block_unblock_analysis.md）
2. 这种方案实现简单，利用现有 Subpartition 缓存机制
3. Barrier 虽然延迟到达下游，但不影响正确性，因为下游需要等待过滤完成才能 Snapshot

---

## 4. 时序图（配置开启时）

```
        JM                      Task                   channelIOExecutor
         │                        │                           │
         │    deploy task         │                           │
         │───────────────────────>│                           │
         │                        │                           │
         │                        │  initialize operators     │
         │                        │──────────┐                │
         │                        │<─────────┘                │
         │                        │                           │
         │  INITIALIZING          │                           │
         │<───────────────────────│                           │
         │                        │                           │
         │                        │  start buffer recovery    │
         │                        │   & filtering             │
         │                        │──────────────────────────>│
         │                        │                           │ (read & filter...)
         │                        │                           │
         │                        │                           │
         │                        │   filteringComplete       │ filter complete
         │                        │<──────────────────────────│
         │                        │                           │
         │                        │  requestPartitions()      │
         │                        │──────────┐                │
         │                        │<─────────┘                │
         │                        │                           │
         │  RUNNING               │                           │
         │<───────────────────────│                           │
         │                        │                           │
         │  trigger checkpoint    │  (all tasks RUNNING)      │
         │───────────────────────>│                           │
         │                        │                           │
         │                        │  snapshot & ack           │
         │<───────────────────────│                           │
         │                        │                           │
         │                        │  process new data         │
         │                        │                           │
```

**时序说明：**

1. Task 部署后进入 **INITIALIZING** 状态
2. Buffer 过滤在 channelIOExecutor 线程异步执行
3. `bufferFilteringCompleteFuture` 完成后：
   - `requestPartitions()` 被调用，建立与上游连接
   - Task 切换到 **RUNNING** 状态
4. JM 检测到所有 Task RUNNING 后触发 Checkpoint（trigger 逻辑不变）
5. Task 执行 Snapshot 并发送 Ack

**配置关闭时的行为（保持不变）：**

- `stateConsumedFuture` 完成后（恢复的 buffer 数据处理完成）触发 `requestPartitions()` 和 RUNNING 转换
- 等待时间较长

---

## 5. 需要修改的关键类

| 类 | 文件位置 | 修改内容 | 优先级 |
|----|----------|----------|--------|
| `RecoveredInputChannel` | `flink-runtime/.../partition/consumer/` | 1. 新增 `bufferFilteringCompleteFuture` 字段<br>2. 新增 `isUnalignedDuringRecoveryEnabled` 配置标志字段<br>3. 修改 `toInputChannel()` 根据配置检查相应 future 和 buffer 传递逻辑<br>4. 修改 `toInputChannelInternal()` 签名<br>5. 修改 `finishReadRecoveredState()` 仅在配置开启时完成 `bufferFilteringCompleteFuture` | 高 |
| `LocalRecoveredInputChannel` | `flink-runtime/.../partition/consumer/` | 修改 `toInputChannelInternal()` 传递 buffer 给新 channel | 高 |
| `RemoteRecoveredInputChannel` | `flink-runtime/.../partition/consumer/` | 修改 `toInputChannelInternal()` 传递 buffer 给新 channel | 高 |
| `SingleInputGate` | `flink-runtime/.../partition/consumer/` | 新增 `getBufferFilteringCompleteFuture()` 方法，聚合所有 channel 的 future | 高 |
| `RemoteInputChannel` | `flink-runtime/.../partition/consumer/` | 修改现有构造器，新增 `@Nullable initialRecoveredBuffers` 参数 | 高 |
| `LocalInputChannel` | `flink-runtime/.../partition/consumer/` | 修改现有构造器，新增 `@Nullable initialRecoveredBuffers` 参数 | 高 |
| `UnknownInputChannel` | `flink-runtime/.../partition/consumer/` | 更新调用方，传 `null` 给新参数 | 中 |
| `InputChannelBuilder` | `flink-runtime/.../test/.../consumer/` | 更新调用方，传 `null` 给新参数 | 中 |
| `StreamTask` | `flink-streaming-java/.../tasks/` | 根据配置选择使用哪个 Future 触发 `requestPartitions()` 和 RUNNING 转换 | 高 |

**注意：Checkpoint trigger 逻辑不需要修改。**

---

## 6. Source Task 处理

**结论：Source Task 无需特殊处理。**

**分析：**

| 维度 | Source Task | 非 Source Task |
|------|-------------|----------------|
| InputGate | 无 | 有 |
| Input Buffer 过滤 | 不涉及 | 需要过滤 |
| `requestPartitions()` | 不涉及（无上游） | 根据配置延迟调用 |
| `bufferFilteringCompleteFuture` | 不存在（无 InputGate） | 需要等待过滤完成 |
| RUNNING 转换 | 正常流程（无 InputGate 影响） | 根据配置选择 Future |

**处理方式：**

Source Task 没有 InputGate，因此不受 `bufferFilteringCompleteFuture` 逻辑影响，正常进入 RUNNING 状态。

---

## 7. 参考

- [FLIP-547 Wiki](https://cwiki.apache.org/confluence/display/FLINK/FLIP-547%3A+Support+checkpoint+during+recovery)
- [Block/Unblock 方案分析](./task4_block_unblock_analysis.md)
- [原始需求文档](./requirement.md)
- [任务拆分文档](./split_tasks.md)
