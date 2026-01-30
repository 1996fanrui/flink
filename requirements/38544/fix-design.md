# Fix Design: LocalInputChannel 支持 Unaligned Checkpoint During Recovery

## 背景

在 unaligned checkpoint during recovery 场景下：
1. `RecoveredInputChannel` 从 state 中读取上一次 checkpoint 的 inflight buffers
2. 这些 buffer 经过 filter 阶段处理（过滤掉 barrier 等）
3. 切换到 `LocalInputChannel`，filtered buffers 迁移到 `toBeConsumedBuffers`
4. Task 切换到 RUNNING
5. 新的 checkpoint 可能触发，上游发送新 barrier

## 问题分析

### 核心问题：LocalInputChannel 无法优先处理 priority event

**RemoteInputChannel 的机制：**
- 所有数据（迁移的 + 新来的）都在同一个 `PrioritizedDeque<SequenceBuffer> receivedBuffers`
- 新的 priority buffer 通过 `addPriorityBuffer()` 添加到**队列头部**
- `getNextBuffer()` 调用 `receivedBuffers.poll()` **自动优先返回 priority 元素**

**LocalInputChannel 的问题：**
- `toBeConsumedBuffers`（迁移的 buffer）和 `subpartitionView`（上游新数据）是**分离的两个数据源**
- `getNextBuffer()` 先消费完 `toBeConsumedBuffers`，才去 `subpartitionView` 获取
- 即使收到 `notifyPriorityEvent` 通知，`getNextBuffer()` 仍然先消费 `toBeConsumedBuffers`

### 关联问题：checkpointStarted 未保存 toBeConsumedBuffers

当优先处理 barrier 时，`toBeConsumedBuffers` 中还未消费的 buffer 需要被 snapshot。

### 关于 notifyPriorityEvent

**无需额外修改方法本身**。根据 Java 的 "class wins" 规则，父类 `InputChannel.notifyPriorityEvent()` 会被调用。

但需要在调用父类方法的同时，设置状态让 `getNextBuffer()` 知道有 priority event 待处理。

## 技术分析

### Q1: 从 subpartitionView 获取的一定是 priority buffer 吗？

**是的**。

`PipelinedSubpartition` 使用 `PrioritizedDeque`：
- `addPriorityElement()` 把 priority 元素放到队列**头部**
- `poll()` 从头部取，一定先返回 priority 元素

### Q2: 是否需要 PrioritizedDeque？

**不需要**。收到通知后直接从 `subpartitionView` 获取即可。

### Q3: 线程安全问题？

- `notifyPriorityEvent()`：上游 Task 线程调用
- `getNextBuffer()`：下游 Task 线程调用

使用 `volatile` 变量足够。

### Q4: 多个 priority event 怎么办？

**不会发生**。`PipelinedSubpartition.needNotifyPriorityEvent()` 只在 `numPriorityElements == 1` 时返回 true，只有第一个 priority event 会触发通知。

后续的 priority event 不会触发新的通知，但它们仍然在 `PrioritizedDeque` 头部，会被 `poll()` 按顺序返回。

## 修复方案

### 方案：覆盖 notifyPriorityEvent + 修改 getNextBuffer

```java
// LocalInputChannel 新增字段
private volatile boolean hasPendingPriorityEvent = false;

@Override
public void notifyPriorityEvent(int prioritySequenceNumber) {
    hasPendingPriorityEvent = true;
    super.notifyPriorityEvent(prioritySequenceNumber);
}

@Override
public Optional<BufferAndAvailability> getNextBuffer() throws IOException {
    checkError();

    // 如果有 pending priority event，优先从 subpartitionView 获取
    if (hasPendingPriorityEvent && subpartitionView != null) {
        BufferAndBacklog next = subpartitionView.getNextBuffer();
        if (next != null) {
            // 细节1：防御性检查 - 预期是 priority event，否则抛异常
            checkState(
                next.buffer().getDataType().hasPriority(),
                "Expected priority event but got: %s",
                next.buffer().getDataType());

            // 检查是否还有更多 priority event
            // 如果没有了，恢复正常消费顺序
            if (!next.getNextDataType().hasPriority()) {
                hasPendingPriorityEvent = false;
            }

            // 细节2：修正 nextDataType
            // subpartitionView 返回的 nextDataType 是上游的下一个元素类型
            // 但实际下一个消费的应该是 toBeConsumedBuffers 中的第一个元素（如果有）
            Buffer.DataType correctedNextDataType = next.getNextDataType();
            if (!toBeConsumedBuffers.isEmpty()) {
                correctedNextDataType = toBeConsumedBuffers.peek().buffer().getDataType();
            }

            return Optional.of(
                new BufferAndAvailability(
                    next.buffer(),
                    correctedNextDataType,
                    next.buffersInBacklog(),
                    next.getSequenceNumber()));
        }
    }

    // 正常流程：先消费 toBeConsumedBuffers
    if (!toBeConsumedBuffers.isEmpty()) {
        BufferAndBacklog next = toBeConsumedBuffers.removeFirst();
        // ... 现有逻辑
        return getBufferAndAvailability(next);
    }

    // ... 现有逻辑
}
```

**细节说明**：

1. **防御性检查**：如果 `hasPendingPriorityEvent = true` 但拿到的不是 priority event，说明逻辑有问题，应该抛异常。

2. **修正 nextDataType**：
   - `subpartitionView.getNextBuffer()` 返回的 `nextDataType` 是上游队列的下一个元素类型
   - 但实际下一个消费的是 `toBeConsumedBuffers` 的第一个元素（如果有）
   - 如果 `toBeConsumedBuffers` 不为空，需要用 `toBeConsumedBuffers.peek().buffer().getDataType()` 替换

### checkpointStarted 保存 toBeConsumedBuffers

当优先处理 barrier 时，`toBeConsumedBuffers` 中的 buffer 需要被 snapshot：

```java
@Override
public void checkpointStarted(CheckpointBarrier barrier) throws CheckpointException {
    List<Buffer> inflightBuffers = new ArrayList<>();
    for (BufferAndBacklog bufferAndBacklog : toBeConsumedBuffers) {
        if (bufferAndBacklog.buffer().isBuffer()) {
            inflightBuffers.add(bufferAndBacklog.buffer().retainBuffer());
        }
    }
    channelStatePersister.startPersisting(barrier.getId(), inflightBuffers);
}
```

### 关键遗漏：maybePersist 和 checkForBarrier

**问题分析**：

对比 RemoteInputChannel 在 `onBuffer()` 中的逻辑：
```java
// RemoteInputChannel.onBuffer()
final OptionalLong barrierId = channelStatePersister.checkForBarrier(sequenceBuffer.buffer);
if (barrierId.isPresent() && barrierId.getAsLong() > lastBarrierId) {
    lastBarrierId = barrierId.getAsLong();
}
channelStatePersister.maybePersist(buffer);
```

LocalInputChannel 遗漏了：
1. **maybePersist**: 当 checkpoint 开始但 barrier 未到达（BARRIER_PENDING 状态），后续从 `subpartitionView` 获取的 buffer 也需要持久化
2. **checkForBarrier**: 需要检测 barrier 并更新 channelStatePersister 状态

**场景时序**：
```
t1: Task RUNNING
t2: checkpointStarted(barrier_id=5) 被调用
    - toBeConsumedBuffers: [X, Y, Z] 被保存
    - checkpointStatus = BARRIER_PENDING
t3: getNextBuffer() 返回 X（已在 t2 保存，不需要 maybePersist）
t4: getNextBuffer() 返回 Y（已在 t2 保存）
t5: getNextBuffer() 返回 Z（已在 t2 保存）
t6: getNextBuffer() 从 subpartitionView 获取 A（需要 maybePersist!）
t7: getNextBuffer() 从 subpartitionView 获取 B（需要 maybePersist!）
t8: barrier 到达，notifyPriorityEvent 触发
t9: getNextBuffer() 优先获取 barrier（checkForBarrier 更新状态为 BARRIER_RECEIVED）
t10: 后续 buffer 不需要 maybePersist
```

**修复方案**：

在 `getNextBuffer` 从 `subpartitionView` 获取 buffer 后：
```java
// 从 subpartitionView 获取 buffer 后
BufferAndBacklog next = subpartitionView.getNextBuffer();
if (next != null) {
    // 检查是否是 barrier，更新 channelStatePersister 状态
    channelStatePersister.checkForBarrier(next.buffer());
    // 如果 BARRIER_PENDING 状态，持久化 buffer
    channelStatePersister.maybePersist(next.buffer());
}
```

**注意**：`toBeConsumedBuffers` 中的 buffer 不需要 maybePersist，因为它们在 `checkpointStarted` 时已经全部保存了

### 为什么不需要 lastBarrierId

**RemoteInputChannel 需要 lastBarrierSequenceNumber**：
- `receivedBuffers` 同时包含 barrier 前后的 buffer
- `checkpointStarted` 时需要根据 sequenceNumber 筛选出 barrier 之前的 buffer

**LocalInputChannel 不需要**：
- `toBeConsumedBuffers` 本身就全是 barrier 之前的 buffer，无需筛选
- `channelStatePersister` 内部已有 `lastSeenBarrier` 检查，会抛出 `CHECKPOINT_SUBSUMED`
- 只需确保调用 `checkForBarrier` 更新 `channelStatePersister` 状态即可
```

## 数据流时序

```
1. 上游发送普通 buffer A, B, C
2. 上游发送 barrier（priority）
3. PipelinedSubpartition.buffers: [barrier, A, B, C]  // barrier 在头部
4. notifyPriorityEvent() 被调用
5. LocalInputChannel.hasPendingPriorityEvent = true
6. InputGate 把 channel 放到 priority 位置
7. getNextBuffer() 被调用
8. 检测到 hasPendingPriorityEvent = true
9. 跳过 toBeConsumedBuffers，从 subpartitionView 获取
10. 返回 barrier
11. checkpointStarted() 被调用，保存 toBeConsumedBuffers
12. 继续消费 A, B, C 和 toBeConsumedBuffers
```

## 实施顺序

1. **覆盖 notifyPriorityEvent**：设置 `hasPendingPriorityEvent` 状态
2. **修改 getNextBuffer**：
   - 检查状态，优先从 subpartitionView 获取 priority event
   - 对从 subpartitionView 获取的 buffer 调用 `checkForBarrier` 和 `maybePersist`
3. **修改 checkpointStarted**：保存 `toBeConsumedBuffers` 中的 inflight buffer

## 修改文件

`flink-runtime/src/main/java/org/apache/flink/runtime/io/network/partition/consumer/LocalInputChannel.java`

## 验证

运行 LocalInputChannel 相关测试：

```bash
./mvnw test -pl flink-runtime -Dtest=LocalInputChannelTest -P java11-target -P java11
```
