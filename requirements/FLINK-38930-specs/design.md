# 技术设计：Filter Recovered Buffers in Channel-State-Unspilling Thread

## 1. 概述

本设计针对 FLIP-547 需求文档 4.5 节 "When and how to filter recovered buffers?" 进行实现。

### 1.1 背景

当前 Flink 的 channel state recovery 逻辑是通过 channel-state-unspilling 和  RescalingStreamTaskNetworkInput 两阶段实现。

RescalingStreamTaskNetworkInput 回调用 DemultiplexingRecordDeserializer 负责数据过滤。

需求是，希望 过滤从 RescalingStreamTaskNetworkInput 移动到 channel-state-unspilling。从而第二阶段使用简单的 StreamTaskNetworkInput 即可。

注：这个改动是为了后续其他 feature 做铺垫。

### 1.2 目标

将过滤逻辑移动到 channel-state-unspilling 线程中执行：
- 在读取 channel state 时就进行过滤
- 将过滤后的记录重新组织到新的 buffer 中
- 只将过滤后的 buffer 放入 InputChannel
- Task 线程直接处理过滤后的 buffer

### 1.3 分阶段实现

按照 MVP 原则，分两步实现：

| 阶段 | 内容 | 说明 |
|------|------|------|
| **Phase 1** | P1: S3 → Filter → Buffer | 只走内存路径，buffer 不足时 BLOCK 等待 |
| **Phase 2** | P2 + P3: Disk Spill 路径 | 引入本地磁盘缓存，解决 buffer 不足问题 |

**本文档只针对 Phase 1 进行设计。**

---

## 2. 核心概念：Virtual Channel

### 2.1 Virtual Channel 的作用

**Virtual Channel 的核心目的是反序列化，不是过滤。**

- 一条记录可能跨越多个 buffer（spanning record）
- Virtual Channel 由 `(oldSubtaskIndex, oldChannelIndex)` 标识
- 每个 Virtual Channel 维护独立的 **deserializer 状态**，确保 spanning record 被正确反序列化
- RecordFilter 是附加在 Virtual Channel 上的过滤逻辑

```java
// DemultiplexingRecordDeserializer.VirtualChannel
static class VirtualChannel<T> {
    // 每个 Virtual Channel 有独立的 deserializer，用于处理 spanning record
    private final RecordDeserializer<DeserializationDelegate<StreamElement>> deserializer;
    // 过滤逻辑，只有 ambiguous 的 VC 才使用真正的过滤器
    private final RecordFilter<T> recordFilter;
    // 用于 Watermark 聚合
    Watermark lastWatermark = Watermark.UNINITIALIZED;
    WatermarkStatus watermarkStatus = WatermarkStatus.ACTIVE;
}
```

### 2.2 为什么需要 Virtual Channel

当发生 rescaling 时，一个物理 channel 可能接收来自多个旧 subtask/channel 的数据。这些数据通过 `SubtaskConnectionDescriptor` 事件来区分：

```
Physical Channel
    ├── Virtual Channel (oldSubtask=0, oldChannel=0)  ← deserializer A
    ├── Virtual Channel (oldSubtask=0, oldChannel=1)  ← deserializer B
    ├── Virtual Channel (oldSubtask=1, oldChannel=0)  ← deserializer C
    └── Virtual Channel (oldSubtask=1, oldChannel=1)  ← deserializer D
```

每个 Virtual Channel 维护独立的 deserializer 状态，确保：
1. 来自同一个 Virtual Channel 的多个 buffer 被同一个 deserializer 处理
2. Spanning record（跨 buffer 的记录）能被正确反序列化

---

## 3. 现有逻辑分析

### 3.1 现有数据流

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                     channel-state-unspilling 线程                            │
├─────────────────────────────────────────────────────────────────────────────┤
│  SequentialChannelStateReaderImpl.readInputData()                           │
│      ↓                                                                       │
│  ChannelStateChunkReader.readChunk()                                        │
│      ↓                                                                       │
│  InputChannelRecoveredStateHandler.recover()                                │
│      - 发送 SubtaskConnectionDescriptor 事件                                │
│      - 发送原始 buffer                                                      │
│      ↓                                                                       │
│  RecoveredInputChannel.onRecoveredStateBuffer(原始 buffer)                  │
└─────────────────────────────────────────────────────────────────────────────┘
                                    ↓
┌─────────────────────────────────────────────────────────────────────────────┐
│                          Task 线程                                          │
├─────────────────────────────────────────────────────────────────────────────┤
│  RescalingStreamTaskNetworkInput.processEvent()                             │
│      - 接收 SubtaskConnectionDescriptor 事件                                │
│      - 调用 DemultiplexingRecordDeserializer.select() 选择 Virtual Channel  │
│      ↓                                                                       │
│  DemultiplexingRecordDeserializer.getNextRecord()                           │
│      ↓                                                                       │
│  VirtualChannel.getNextRecord()                                             │
│      - 使用 deserializer 反序列化（处理 spanning record）                   │
│      - 使用 RecordFilter.filter() 过滤记录                                   │
│      ↓                                                                       │
│  处理过滤后的记录                                                           │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 3.2 关键类和方法

| 类 | 方法 | 作用 |
|---|------|------|
| `SequentialChannelStateReaderImpl` | `readInputData()` | 读取 input channel state 入口 |
| `ChannelStateChunkReader` | `readChunk()` | 读取单个 chunk 到 buffer |
| `InputChannelRecoveredStateHandler` | `recover()` | 将 buffer 放入 RecoveredInputChannel |
| `RecoveredInputChannel` | `onRecoveredStateBuffer()` | 接收恢复的 buffer |
| `DemultiplexingRecordDeserializer` | `select()` | 根据 SubtaskConnectionDescriptor 选择 Virtual Channel |
| `DemultiplexingRecordDeserializer` | `getNextRecord()` | 聚合多个 Virtual Channel 的 Watermark |
| `VirtualChannel` | `getNextRecord()` | **核心逻辑**：反序列化 + 过滤 |
| `RecordFilter` | `filter()` | 判断记录是否属于当前 subtask |

### 3.3 过滤逻辑判断

**第一层：是否需要过滤（Virtual Channel 级别）**

```java
// DemultiplexingRecordDeserializer.create() 中
rescalingDescriptor.isAmbiguous(channelInfo.getGateIdx(), subtask)
        ? recordFilterFactory.apply(channelInfo)  // 需要过滤
        : RecordFilter.acceptAll()                  // 不需要过滤，返回所有记录
```

**第二层：记录是否属于当前 subtask（Record 级别）**

```java
// PartitionerRecordFilter.filter()
// 只有 ambiguous 的 Virtual Channel 才执行此判断
public boolean filter(StreamRecord<T> streamRecord) {
    delegate.setInstance(streamRecord);
    return partitioner.selectChannel(delegate) == subtaskIndex;
}
```

### 3.4 VirtualChannel.getNextRecord() 核心逻辑

```java
// DemultiplexingRecordDeserializer.java:76-97
public DeserializationResult getNextRecord(DeserializationDelegate<StreamElement> delegate)
        throws IOException {
    do {
        // 使用 deserializer 反序列化（处理 spanning record）
        lastResult = deserializer.getNextRecord(delegate);

        if (lastResult.isFullRecord()) {
            final StreamElement element = delegate.getInstance();
            // 对于 Record，使用 recordFilter.filter() 过滤
            if (element.isRecord() && recordFilter.filter(element.asRecord())) {
                return lastResult;
            // 对于 Watermark/WatermarkStatus，直接返回（不过滤）
            } else if (element.isWatermark()) {
                lastWatermark = element.asWatermark();
                return lastResult;
            } else if (element.isWatermarkStatus()) {
                watermarkStatus = element.asWatermarkStatus();
                return lastResult;
            }
        }
        // 循环处理：如果记录被过滤掉，继续读取下一条
    } while (!lastResult.isBufferConsumed());
    return DeserializationResult.PARTIAL_RECORD;
}
```

---

## 4. 代码分析：公共部分 vs 不同部分

### 4.1 公共部分（可复用）

| 代码 | 位置 | 说明 |
|------|------|------|
| `VirtualChannel` 类 | DemultiplexingRecordDeserializer.java:62-110 | 维护 deserializer + recordFilter |
| `VirtualChannel.getNextRecord()` | DemultiplexingRecordDeserializer.java:76-97 | **核心逻辑**：反序列化 + 过滤 |
| `RecordFilterFactory` | RescalingStreamTaskNetworkInput.java:204-248 | 创建 RecordFilter |
| `DeserializerFactory` | RescalingStreamTaskNetworkInput.java:250-268 | 创建 deserializer |
| `RecordFilter` | RecordFilter.java | 过滤逻辑 |
| Virtual Channel 创建逻辑 | DemultiplexingRecordDeserializer.create() | 基于 rescalingDescriptor 的笛卡尔积 |

### 4.2 不同部分

| 方面 | 现有逻辑（Task 线程） | 新逻辑（channel-state-unspilling 线程） |
|------|----------------------|----------------------------------------|
| **处理模式** | 拉模式（Task 线程主动调用 getNextRecord） | 推模式（遍历所有 recovered buffer） |
| **Watermark 处理** | 聚合所有 Virtual Channel 的 Watermark | 直接保留（不需要聚合） |
| **输出** | 返回 DeserializationResult | 返回过滤后的记录列表，重新序列化到新 buffer |
| **接口** | 实现 RecordDeserializer 接口 | 不需要实现该接口 |
| **驱动方** | AbstractStreamTaskNetworkInput | ChannelStateChunkReader |

---

## 5. 新逻辑设计

### 5.1 设计原则

**复用现有代码**：提取公共逻辑，供两种场景（Task 线程和 channel-state-unspilling 线程）复用。

### 5.2 新数据流（Phase 1）

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                     channel-state-unspilling 线程                            │
├─────────────────────────────────────────────────────────────────────────────┤
│  SequentialChannelStateReaderImpl.readInputData()                           │
│      ↓                                                                       │
│  ChannelStateChunkReader.readChunk() - 读取原始 buffer                      │
│      ↓                                                                       │
│  [新] ChannelStateFilteringHandler.filterAndRewrite()                       │
│      - 为每个 Virtual Channel 维护独立的 deserializer                       │
│      - 使用 VirtualChannel.getNextRecord() 反序列化 + 过滤                  │
│      - 将过滤后的记录重新序列化到新 buffer                                  │
│      ↓                                                                       │
│  RecoveredInputChannel.onRecoveredStateBuffer(过滤后的 buffer)              │
└─────────────────────────────────────────────────────────────────────────────┘
                                    ↓
┌─────────────────────────────────────────────────────────────────────────────┐
│                          Task 线程                                          │
├─────────────────────────────────────────────────────────────────────────────┤
│  StreamTaskNetworkInput.emitNext() (不再需要 RescalingStreamTaskNetworkInput)│
│      ↓                                                                       │
│  直接处理过滤后的记录（无需 demultiplexing 和过滤）                         │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 5.3 提取公共代码

将 `VirtualChannel` 及其核心逻辑提取为独立类，供两种场景复用：

```java
/**
 * A virtual channel that handles deserialization and filtering.
 * Maintains deserializer state for spanning records.
 *
 * Extracted from DemultiplexingRecordDeserializer.VirtualChannel.
 */
public class VirtualChannel<T> {
    private final RecordDeserializer<DeserializationDelegate<StreamElement>> deserializer;
    private final RecordFilter<T> recordFilter;
    private Watermark lastWatermark = Watermark.UNINITIALIZED;
    private WatermarkStatus watermarkStatus = WatermarkStatus.ACTIVE;
    private DeserializationResult lastResult;

    // 核心方法：反序列化 + 过滤（从现有代码提取）
    public DeserializationResult getNextRecord(DeserializationDelegate<StreamElement> delegate)
            throws IOException;

    public void setNextBuffer(Buffer buffer) throws IOException;
    public void clear();
    public boolean hasPartialData();

    // Getter for watermark aggregation (used by Task thread logic)
    public Watermark getLastWatermark();
    public WatermarkStatus getWatermarkStatus();
}
```

### 5.4 新增类：ChannelStateFilteringHandler

在 channel-state-unspilling 线程中使用：

```java
/**
 * Filters recovered channel state buffers.
 * Uses VirtualChannel for deserialization and filtering.
 * Re-serializes filtered records to new buffers.
 */
public class ChannelStateFilteringHandler<T> {

    // 为每个 (oldSubtaskIndex, oldChannelIndex) 维护一个 VirtualChannel
    private final Map<SubtaskConnectionDescriptor, VirtualChannel<T>> virtualChannels;
    private final StreamElementSerializer<T> serializer;

    /**
     * Process a buffer from a specific Virtual Channel.
     *
     * @param descriptor Virtual Channel identifier (oldSubtaskIndex, oldChannelIndex)
     * @param sourceBuffer original buffer to filter
     * @param bufferSupplier supplier for new buffers
     * @return filtered buffers (may be empty if all records filtered out)
     */
    public List<Buffer> filterAndRewrite(
            SubtaskConnectionDescriptor descriptor,
            Buffer sourceBuffer,
            BufferSupplier bufferSupplier) throws IOException, InterruptedException {

        VirtualChannel<T> vc = virtualChannels.get(descriptor);
        vc.setNextBuffer(sourceBuffer);

        List<StreamElement> filteredElements = new ArrayList<>();
        DeserializationDelegate<StreamElement> delegate = ...;

        // 使用 VirtualChannel 的核心逻辑
        while (true) {
            DeserializationResult result = vc.getNextRecord(delegate);
            if (result.isFullRecord()) {
                filteredElements.add(delegate.getInstance());
            }
            if (result.isBufferConsumed()) {
                break;
            }
        }

        // 重新序列化到新 buffer
        return serializeToBuffers(filteredElements, bufferSupplier);
    }
}
```

### 5.5 修改现有代码

`DemultiplexingRecordDeserializer` 改为使用提取的 `VirtualChannel` 类：

```java
class DemultiplexingRecordDeserializer<T>
        implements RecordDeserializer<DeserializationDelegate<StreamElement>> {

    // 使用提取的 VirtualChannel 类
    private final Map<SubtaskConnectionDescriptor, VirtualChannel<T>> channels;

    // getNextRecord() 保持不变，仍然做 Watermark 聚合
    @Override
    public DeserializationResult getNextRecord(DeserializationDelegate<StreamElement> delegate) {
        // 调用 currentVirtualChannel.getNextRecord()
        // 聚合 Watermark/WatermarkStatus
    }
}
```

---

## 6. 数据流对比

### 6.1 现有逻辑（Task 线程）

```
RecoveredInputChannel.onRecoveredStateBuffer(原始 buffer)
    ↓
Task 线程: RescalingStreamTaskNetworkInput.emitNext()
    ↓
DemultiplexingRecordDeserializer.select(SubtaskConnectionDescriptor)
    ↓
DemultiplexingRecordDeserializer.getNextRecord()
    ↓
VirtualChannel.getNextRecord() [反序列化 + 过滤]
    ↓
处理过滤后的记录
```

### 6.2 新逻辑（channel-state-unspilling 线程）

```
ChannelStateChunkReader.readChunk()
    ↓
ChannelStateFilteringHandler.filterAndRewrite(descriptor, buffer, ...)
    ↓
VirtualChannel.getNextRecord() [反序列化 + 过滤] (复用公共逻辑)
    ↓
重新序列化到新 buffer
    ↓
RecoveredInputChannel.onRecoveredStateBuffer(过滤后的 buffer)
    ↓
Task 线程: StreamTaskNetworkInput.emitNext() (不需要 demultiplexing)
```

---

## 7. 特殊情况处理

### 7.1 不需要过滤的情况

当 Virtual Channel 不是 ambiguous 时，使用 `RecordFilter.acceptAll()`（返回所有记录）。

判断条件：
```java
boolean needsFiltering = rescalingDescriptor.isAmbiguous(channelInfo.getGateIdx(), oldSubtaskIndex);
```

### 7.2 没有 rescaling 的情况

当 `InflightDataRescalingDescriptor.NO_RESCALE` 时，所有逻辑走原有路径，不进行任何过滤。

### 7.3 Watermark 和 WatermarkStatus 处理

- **现有逻辑（Task 线程）**：聚合所有 Virtual Channel 的 Watermark，取最小值
- **新逻辑（channel-state-unspilling 线程）**：直接保留，不需要聚合

### 7.4 Spanning Record 处理

每个 Virtual Channel 维护独立的 deserializer 状态。当一条记录跨多个 buffer 时：
1. 第一个 buffer 被处理后，deserializer 保存 partial record 状态
2. 后续 buffer 到达同一个 Virtual Channel 时，deserializer 继续反序列化
3. 直到完整记录被反序列化出来

---

## 8. 配置传递链路

需要将 `execution.checkpointing.unaligned.during-recovery.enabled` 配置从 JobManager 传递到 Task：

```
Configuration (execution.checkpointing.unaligned.during-recovery.enabled)
    ↓
ExecutionGraph
    ↓
TaskDeploymentDescriptor
    ↓
StreamTask.restoreStateAndGates()
    ↓
创建 RecordFilterContext（此时 StreamConfig 可用）
    ↓
channelIOExecutor.execute(() -> {
    reader.readInputDataWithFiltering(inputGates, filterContext);
})
```

### 8.1 TypeSerializer 和 StreamPartitioner 获取

**问题**：`SequentialChannelStateReaderImpl` 在 `TaskStateManagerImpl` 构造函数中创建，此时 `StreamConfig` 和 `ClassLoader` 还不可用。

**解决方案**：延迟创建过滤逻辑，在 `StreamTask.restoreStateAndGates()` 中创建 `RecordFilterContext` 并传递。

```java
// StreamTask.restoreStateAndGates()
RecordFilterContext filterContext = createRecordFilterContext();
channelIOExecutor.execute(() -> {
    reader.readInputDataWithFiltering(inputGates, filterContext);
});
```

---

## 9. 修改文件列表

| 文件 | 修改类型 | 说明 |
|------|----------|------|
| `VirtualChannel.java` | 新增 | 从 DemultiplexingRecordDeserializer 提取 |
| `ChannelStateFilteringHandler.java` | 新增 | channel-state-unspilling 线程的过滤处理 |
| `DemultiplexingRecordDeserializer.java` | 修改 | 使用提取的 VirtualChannel |
| `InputChannelRecoveredStateHandler.java` | 修改 | 支持过滤模式 |
| `SequentialChannelStateReaderImpl.java` | 修改 | 传递过滤配置 |
| `StreamTask.java` | 修改 | 创建过滤上下文 |
| `StreamTaskNetworkInputFactory.java` | 修改 | 根据配置选择 Input 类型 |

---

## 10. 任务拆解 (POC)

- [ ] 1. 提取 `VirtualChannel` 为独立类
- [ ] 2. 修改 `DemultiplexingRecordDeserializer` 使用提取的 `VirtualChannel`
- [ ] 3. 创建 `ChannelStateFilteringHandler`
- [ ] 4. 修改 `InputChannelRecoveredStateHandler` 支持过滤
- [ ] 5. 修改 `StreamTask` 传递过滤配置
- [ ] 6. 修改 `StreamTaskNetworkInputFactory` 选择正确的 Input 类型

---

## 11. 关键文件位置

| 文件 | 路径 |
|------|------|
| `DemultiplexingRecordDeserializer.java` | `flink-runtime/src/main/java/org/apache/flink/streaming/runtime/io/recovery/` |
| `RescalingStreamTaskNetworkInput.java` | 同上 |
| `RecordFilter.java` | 同上 |
| `InputChannelRecoveredStateHandler.java` | `flink-runtime/src/main/java/org/apache/flink/runtime/checkpoint/channel/RecoveredChannelStateHandler.java` |
| `SequentialChannelStateReaderImpl.java` | `flink-runtime/src/main/java/org/apache/flink/runtime/checkpoint/channel/` |
| `StreamTask.java` | `flink-runtime/src/main/java/org/apache/flink/streaming/runtime/tasks/` |

---

## 12. 风险与缓解

| 风险 | 缓解措施 |
|------|----------|
| 过滤逻辑与原有 RecordFilter 不一致 | 复用 VirtualChannel 的核心逻辑 |
| Spanning record 处理复杂 | 复用现有 deserializer 状态管理 |
| Buffer 阻塞等待可能导致恢复变慢 | Phase 2 引入 disk spill 解决 |
| 序列化/反序列化性能开销 | Phase 1 先验证正确性，Phase 2 再优化 |
