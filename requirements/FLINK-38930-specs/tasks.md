# 实施计划

## Phase 1: Filter Recovered Buffers in Channel-State-Unspilling Thread

基于 FLIP-547 需求文档 4.5 节的 Phase 1 实现，只走内存路径，buffer 不足时 BLOCK 等待。

> **注意：当前为 POC 阶段，不包含测试任务。**

---

### 阶段一：提取公共代码

- [ ] 1. 提取 `VirtualChannel` 为独立类
  - 从 `DemultiplexingRecordDeserializer.VirtualChannel` 提取为独立的 public 类
  - 位置：`flink-runtime/src/main/java/org/apache/flink/streaming/runtime/io/recovery/VirtualChannel.java`
  - 包含字段：
    - `deserializer`: RecordDeserializer
    - `recordFilter`: Predicate<StreamRecord<T>>
    - `lastWatermark`: Watermark
    - `watermarkStatus`: WatermarkStatus
    - `lastResult`: DeserializationResult
  - 包含方法：
    - `getNextRecord()`: 核心逻辑，反序列化 + 过滤
    - `setNextBuffer()`: 设置下一个 buffer
    - `clear()`: 清理状态
    - `hasPartialData()`: 是否有部分数据
    - `getLastWatermark()`: 获取最后的 watermark
    - `getWatermarkStatus()`: 获取 watermark 状态
  - _需求: Phase 1 - 代码复用

- [ ] 2. 修改 `DemultiplexingRecordDeserializer` 使用提取的 `VirtualChannel`
  - 删除内部类 `VirtualChannel`
  - 改为使用新提取的独立 `VirtualChannel` 类
  - _需求: Phase 1 - 代码复用

---

### 阶段二：实现过滤处理器

- [ ] 3. 创建 `ChannelStateFilteringHandler` 类
  - 位置：`flink-runtime/src/main/java/org/apache/flink/runtime/checkpoint/channel/ChannelStateFilteringHandler.java`
  - 核心字段：
    - `virtualChannels`: Map<SubtaskConnectionDescriptor, VirtualChannel<T>>
    - `serializer`: StreamElementSerializer<T>
  - 核心方法：
    - `filterAndRewrite(descriptor, sourceBuffer, bufferSupplier)`: 过滤并重写 buffer
    - `serializeToBuffers(filteredElements, bufferSupplier)`: 将过滤后的记录序列化到新 buffer
  - 处理逻辑：
    - 使用 VirtualChannel.getNextRecord() 反序列化 + 过滤
    - 将过滤后的记录重新序列化到新 buffer
    - 当 buffer 不足时 BLOCK 等待（Phase 1 策略）
  - _需求: Phase 1 - 核心过滤逻辑

- [ ] 4. 创建 `RecordFilterContext` 类
  - 位置：`flink-runtime/src/main/java/org/apache/flink/streaming/runtime/io/recovery/RecordFilterContext.java`
  - 封装过滤所需的上下文信息：
    - `TypeSerializer`: 用于序列化/反序列化
    - `StreamPartitioner`: 用于判断记录归属
    - `RescalingDescriptor`: 用于判断是否需要过滤
    - `subtaskIndex`: 当前 subtask 索引
  - _需求: Phase 1 - 配置传递

---

### 阶段三：修改 Channel State Recovery 链路

- [ ] 5. 修改 `InputChannelRecoveredStateHandler` 支持过滤模式
  - 位置：`flink-runtime/src/main/java/org/apache/flink/runtime/checkpoint/channel/RecoveredChannelStateHandler.java`
  - 新增过滤模式：
    - 接收 `ChannelStateFilteringHandler` 实例
    - 在 `recover()` 方法中调用过滤逻辑
    - 将过滤后的 buffer 放入 RecoveredInputChannel
  - 保持原有非过滤模式的兼容性
  - _需求: Phase 1 - Recovery 链路改造

- [ ] 6. 修改 `SequentialChannelStateReaderImpl` 传递过滤配置
  - 位置：`flink-runtime/src/main/java/org/apache/flink/runtime/checkpoint/channel/SequentialChannelStateReaderImpl.java`
  - 新增方法 `readInputDataWithFiltering(inputGates, filterContext)`
  - 将 `RecordFilterContext` 传递给 `InputChannelRecoveredStateHandler`
  - _需求: Phase 1 - 配置传递

---

### 阶段四：修改 StreamTask 链路

- [ ] 7. 修改 `StreamTask` 创建并传递过滤上下文
  - 位置：`flink-runtime/src/main/java/org/apache/flink/streaming/runtime/tasks/StreamTask.java`
  - 在 `restoreStateAndGates()` 方法中：
    - 创建 `RecordFilterContext` 实例
    - 从 `StreamConfig` 获取 TypeSerializer 和 StreamPartitioner
    - 调用 `reader.readInputDataWithFiltering(inputGates, filterContext)`
  - 添加配置项 `execution.checkpointing.unaligned.during-recovery.enabled` 的读取
  - _需求: Phase 1 - 配置传递

- [ ] 8. 修改 `StreamTaskNetworkInputFactory` 选择正确的 Input 类型
  - 位置：`flink-runtime/src/main/java/org/apache/flink/streaming/runtime/io/StreamTaskNetworkInputFactory.java`
  - 当启用新过滤模式时：
    - 使用 `StreamTaskNetworkInput` 而非 `RescalingStreamTaskNetworkInput`
    - 因为过滤已在 channel-state-unspilling 线程完成
  - 保持向后兼容：未启用时仍使用 `RescalingStreamTaskNetworkInput`
  - _需求: Phase 1 - Task 线程简化

---

## 文件修改清单

| 文件 | 修改类型 | 对应任务 |
|------|----------|----------|
| `VirtualChannel.java` | 新增 | 任务 1 |
| `DemultiplexingRecordDeserializer.java` | 修改 | 任务 2 |
| `ChannelStateFilteringHandler.java` | 新增 | 任务 3 |
| `RecordFilterContext.java` | 新增 | 任务 4 |
| `RecoveredChannelStateHandler.java` | 修改 | 任务 5 |
| `SequentialChannelStateReaderImpl.java` | 修改 | 任务 6 |
| `StreamTask.java` | 修改 | 任务 7 |
| `StreamTaskNetworkInputFactory.java` | 修改 | 任务 8 |

---

## 依赖关系

```
任务 1 (提取 VirtualChannel)
    ↓
任务 2 (修改 DemultiplexingRecordDeserializer)
    ↓
任务 3 (创建 ChannelStateFilteringHandler) ← 依赖任务 1
    ↓
任务 4 (创建 RecordFilterContext)
    ↓
任务 5 (修改 InputChannelRecoveredStateHandler) ← 依赖任务 3, 4
    ↓
任务 6 (修改 SequentialChannelStateReaderImpl) ← 依赖任务 5
    ↓
任务 7 (修改 StreamTask) ← 依赖任务 4, 6
    ↓
任务 8 (修改 StreamTaskNetworkInputFactory) ← 依赖任务 7
```

---

## 备注

- **当前为 POC 阶段**：不包含单元测试和集成测试
- **Phase 2 不在本次实施范围内**：Disk Spill 路径将在 Phase 1 验证正确性后再进行
- **Buffer 阻塞策略**：Phase 1 当 buffer 不足时 BLOCK 等待，这是已知的限制
