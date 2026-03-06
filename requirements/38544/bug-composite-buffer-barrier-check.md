# Bug: checkForBarrier 提前调用导致 FullyFilledBuffer 上触发 UnsupportedOperationException

## 现象

26/28 个 CI 失败测试均抛出相同异常：

```
java.lang.UnsupportedOperationException
  at AbstractCompositeBuffer.getNioBufferReadable(AbstractCompositeBuffer.java:166)
  at EventSerializer.fromBuffer(EventSerializer.java:429)
  at ChannelStatePersister.parseEvent(ChannelStatePersister.java:169)
  at ChannelStatePersister.checkForBarrier(ChannelStatePersister.java:121)
  at LocalInputChannel.getNextBuffer(LocalInputChannel.java:383)
```

失败测试全部与 batch job 相关（flink-table-planner BatchRestoreTest 系列、flink-connector-files batch 系列、flink-tests batch 系列等）。

## 引入 commit

- `14c196d2cf1ab24ee6fedea5338b9c20c557d8e2` - Support LocalInputChannel checkpoint snapshot for recovered buffers
- `ae068f6f51034ec987be11319cc93efc8ac2a8dd` - Fix LocalInputChannel priority event and buffer availability for recovered buffers

两个 commit 的目标是支持 streaming job 在 recovery 期间的 unaligned checkpoint，但改动影响了 batch job 的执行路径。

## 根因

### 改动前的代码（正确）

`LocalInputChannel.getNextBuffer()` 中，`checkForBarrier` 在 `getBufferAndAvailability()` 内部调用，此时 buffer 已经过类型转换：

```java
// getNextBuffer()
Buffer buffer = next.buffer();

if (buffer instanceof FullyFilledBuffer) {
    // 解包为 partial buffers，加入 toBeConsumedBuffers
    return getBufferAndAvailability(toBeConsumedBuffers.removeFirst());
}
return getBufferAndAvailability(next);

// getBufferAndAvailability() 内部
if (buffer instanceof CompositeBuffer) {
    buffer = ((CompositeBuffer) buffer).getFullBufferData(...);  // 转为普通 Buffer
}
channelStatePersister.checkForBarrier(buffer);  // ✅ 此时 buffer 已是普通 Buffer
```

### 改动后的代码（有问题）

第一个 commit 将 `checkForBarrier` 从 `getBufferAndAvailability()` 内部提前到 `getNextBuffer()` 获取 buffer 之后、FullyFilledBuffer 解包之前：

```java
// getNextBuffer()
Buffer buffer = next.buffer();

channelStatePersister.checkForBarrier(buffer);  // ❌ buffer 可能是 FullyFilledBuffer
channelStatePersister.maybePersist(buffer);

if (buffer instanceof FullyFilledBuffer) {
    // ...
}
```

### 错误调用链

1. Batch job 使用 `SortMergeResultPartition`，`SortMergeSubpartitionReader` 会将多个小 buffer 组装为 `FullyFilledBuffer`
2. `LocalInputChannel.getNextBuffer()` 从 `subpartitionView` 获取到 `FullyFilledBuffer`
3. 第 383 行立即调用 `channelStatePersister.checkForBarrier(buffer)`
4. `checkForBarrier()` → `parseEvent()` → `EventSerializer.fromBuffer(buffer, classLoader)`
5. `EventSerializer.fromBuffer()` 调用 `buffer.getNioBufferReadable()`
6. `FullyFilledBuffer` 继承自 `AbstractCompositeBuffer`，`getNioBufferReadable()` 未实现，抛出 `UnsupportedOperationException`

### 为什么只影响 Batch Job

- Streaming job 使用 `PipelinedResultPartition`，产生普通 `NetworkBuffer`，支持 `getNioBufferReadable()`
- Batch job 使用 `SortMergeResultPartition`，产生 `FullyFilledBuffer`（继承 `AbstractCompositeBuffer`），不支持 `getNioBufferReadable()`

改动本身是为 streaming recovery 设计的，但 `getNextBuffer()` 是 streaming 和 batch 共用的代码路径，提前调用 `checkForBarrier` 波及了 batch 场景。

## 修复方案

将 `checkForBarrier` 和 `maybePersist` 调用移回 `getBufferAndAvailability()` 内部，恢复原有的调用时序：buffer 类型转换完成后再做 barrier 检查。

### 调用位置不同，buffer 状态不同

**有 bug 的代码**（改动前）在 `getNextBuffer()` 中直接调用：

```java
// getNextBuffer() 第 379-398 行
Buffer buffer = next.buffer();           // ← buffer 可能是 FullyFilledBuffer

checkForBarrier(buffer);                 // ❌ 直接对 FullyFilledBuffer 调 checkForBarrier
maybePersist(buffer);

if (buffer instanceof FullyFilledBuffer) {
    // 解包成 partial buffers...
    return getBufferAndAvailability(toBeConsumedBuffers.removeFirst());
}
return getBufferAndAvailability(next);
```

同时 `getBufferAndAvailability()` 里的调用被**删掉了**（替换成了注释）。

**修复后的代码**把 `checkForBarrier` 移回 `getBufferAndAvailability()` 里：

```java
// getNextBuffer()
Buffer buffer = next.buffer();

if (buffer instanceof FullyFilledBuffer) {
    // 解包成 partial buffers，每个 partial buffer 单独通过 getBufferAndAvailability() 返回
    return getBufferAndAvailability(toBeConsumedBuffers.removeFirst());
}
return getBufferAndAvailability(next);

// getBufferAndAvailability()
if (buffer instanceof CompositeBuffer) {
    buffer = ((CompositeBuffer) buffer).getFullBufferData(...);  // ← 转为普通 Buffer
}
checkForBarrier(buffer);                 // ✅ 此时 buffer 一定是普通 Buffer
```

### 关键路径差异

对于 batch job 产生的 `FullyFilledBuffer`：

| | 有 bug 的代码 | 修复后 |
|---|---|---|
| `checkForBarrier` 的调用对象 | `FullyFilledBuffer` 本身 | 解包后的单个 partial buffer |
| buffer 是否支持 `getNioBufferReadable()` | 不支持，抛异常 | 支持，正常工作 |

简单说：`FullyFilledBuffer` 是一个**容器**（里面装着多个小 buffer），不能直接当普通 buffer 用。改动前对容器调了 `checkForBarrier`，改动后对容器里取出的单个 buffer 调，所以不会出错。

### 具体改动：

### 1. 在 `getNextBuffer()` 中移除提前的调用

删除 `LocalInputChannel.java` 第 381-384 行：

```java
// 删除这 4 行
// Check for barrier and persist buffer for unaligned checkpoint.
// This must be done before processing FullyFilledBuffer to ensure proper checkpoint state.
channelStatePersister.checkForBarrier(buffer);
channelStatePersister.maybePersist(buffer);
```

### 2. 在 `getBufferAndAvailability()` 中恢复调用

在 `getBufferAndAvailability()` 方法中，buffer 类型转换之后、`numBytesIn.inc()` 之后，恢复 `checkForBarrier` 和 `maybePersist` 调用：

```java
private Optional<BufferAndAvailability> getBufferAndAvailability(BufferAndBacklog next)
        throws IOException {
    Buffer buffer = next.buffer();
    if (buffer instanceof FileRegionBuffer) {
        buffer = ((FileRegionBuffer) buffer).readInto(inputGate.getUnpooledSegment());
    }
    if (buffer instanceof CompositeBuffer) {
        buffer = ((CompositeBuffer) buffer).getFullBufferData(inputGate.getUnpooledSegment());
    }
    numBytesIn.inc(buffer.readableBytes());
    numBuffersIn.inc();
    channelStatePersister.checkForBarrier(buffer);   // 恢复
    channelStatePersister.maybePersist(buffer);       // 恢复
    // ...
}
```

### 3. priority event 路径保持不变

第二个 commit 在 `hasPendingPriorityEvent` 分支中调用 `channelStatePersister.checkForBarrier(next.buffer())` 是安全的——priority event 直接来自 `subpartitionView.getNextBuffer()`，是普通 buffer 而非 `FullyFilledBuffer`，无需修改。

### 影响范围

仅修改 `LocalInputChannel.java` 一个文件，改动为调用位置的还原。
