# Design — FLINK-38544 Spilling

## 目标

在 checkpoint channel state recovery 的 filtering 模式下，引入 disk spilling 机制，替换当前无界 Heap Buffer fallback（见 `RecoveredInputChannel.requestBufferBlocking()` 中 TODO 注释），实现有界内存使用。

当 Network Buffer Pool 耗尽时，filtered 数据写入磁盘 spill 文件；当 buffer 重新可用时，从 spill 文件回放数据到 InputChannel，保证 FIFO 顺序。

## 设计原则

1. **内存隔离**：Source Buffer（pre-filter）使用 Heap 内存，与 Network Buffer Pool 隔离，消除 Source Buffer 和 Filtered Buffer 竞争同一 Pool 导致的死锁。
2. **纯字节流 spill**：Spill 文件仅存储原始字节，不含任何元数据（record 边界、channel 上下文、DataType 等）。所有元数据由内存中的 `Queue<SpillEntry>` 管理。这简化了 I/O 层，避免了格式兼容性问题。（参考：业界 spill 系统普遍采用 in-memory index + raw data file 的分离模式，见 `industry_research/spill_metadata_management.md`）
3. **统一写接口**：filterAndRewrite 通过 OutputWriter 统一接口写数据，不感知后端是 Network Buffer 还是 File。上层按需决定后端。
4. **Backend 只降级不升级**：单次 writeToBackend 调用中，backend 只能从 buffer 降级到 file，不可升级。降级产生 disk data 后，后续检查强制走 file 路径。升级机会在下一次 write() 调用时通过 P3 drain 实现。
5. **最小代码侵入**：所有新逻辑（OutputWriter、SpillFile I/O、P3 replay、SpillEntry 管理）在新类中实现。现有文件仅调用 `writer.write()`，内部细节不泄漏到现有代码。

## 现状分析

### 当前过滤恢复流程

```
SequentialChannelStateReaderImpl.readInputData()
  → ChannelStateChunkReader.readChunk()
    → stateHandler.getBuffer()          // 从 RecoveredInputChannel 请求 buffer
    → stateHandler.recover()
      → recoverWithFiltering()
        → filteringHandler.filterAndRewrite()  // 反序列化 → 过滤 → 重新序列化
          → bufferSupplier.requestBufferBlocking()  // 请求 Network Buffer 写出
        → channel.onRecoveredStateBuffer()     // 分发到 InputChannel
```

### 当前问题

`RecoveredInputChannel.requestBufferBlocking()` 在 filtering 模式下：
1. 尝试非阻塞请求 Network Buffer (`bufferManager.requestBuffer()`)
2. 如果失败，**无限制分配 Heap Buffer**（`allocateUnpooledSegment`）

这导致当 S3 数据量大时 Heap 内存无界增长，可能 OOM。代码中已标注 `TODO: replace heap fallback with disk spilling to bound memory usage in FLINK-38544`。

## 架构变更

### 新增类

所有新类位于 `org.apache.flink.runtime.checkpoint.channel` 包下。

#### OutputWriter

**职责**：统一管理 filtered 数据的写出，封装三条数据路径（P1/P2/P3）和 spill 文件管理。

**生命周期**：per-gate 实例，在 `SequentialChannelStateReaderImpl.readInputData()` 方法开头创建（位于 stateHandler 初始化之后）。`readInputData()` 对同一组 InputGate 调用两次 `read()`（extractUnmergedInputHandles 和 getUpstreamOutputBufferState），两次 read() 调用共享同一 OutputWriter 实例。在 `readInputData()` 末尾（两次 read 全部完成后）调用 `close()` blocking drain。

**构造参数**：
- `InputGate gate` — 用于 `gate.getBufferSize()` 获取 buffer size 以确定 SpillEntry 粒度和 replay chunk size
- `InflightDataRescalingDescriptor rescalingDescriptor` — old→new channel 映射，用于 replay 时将 SpillEntry 路由到正确的 RecoveredInputChannel
- `InputGate[] inputGates` — 通过 gateIndex 获取目标 gate 的 channel，配合 rescalingDescriptor 解析目标 RecoveredInputChannel
- `String[] spillDirs` — 来自 `IOManager.getSpillingDirectoriesPaths()`
- `int gateIndex` — 当前 gate 索引
- `AtomicInteger heapBufferCounter` — per-gate Heap Buffer 计数器（与 getBuffer() 共享）

**核心方法**：
- `write(byte[] data, int offset, int length, InputChannelInfo channelInfo)` — 写入 filtered 数据，内部路由到 buffer 或 file。OutputWriter 使用 channelInfo 进行 channel change 检测、SpillEntry 标记、和 replay 时的 channel 路由
- `close()` — flush 当前 backend，blocking drain 所有 disk data，清理 spill 文件

**内部状态**：
- 当前活跃 buffer（可能为 null）
- SpillEntry 队列（write = enqueue，replay = dequeue）
- SpillFile 管理器（当前写入文件、已满待删除文件）
- 上一次写入的 channelInfo（用于 channel change 检测）
- closed 标志

**三条数据路径**：

| 路径 | 条件 | 行为 |
|------|------|------|
| P1 | Buffer 可用，无 disk data | 写入 Network Buffer → InputChannel |
| P2 | Buffer 不可用 | 写入 spill file |
| P3 | 有 disk data（不论 buffer 是否可用） | 优先 replay disk data → InputChannel |

P3 优先于 P1：当 disk 有数据时，新数据必须写入 disk（保证 FIFO），同时 eager drain 已有 disk data。

**writeToBackend 降级机制**：使用 boolean `downgradedToFile` 标志，初始为 false。一旦请求 buffer 失败（或检测到 disk has data）并切换到 file，设为 true。后续 buffer 空间用完后直接走 file 路径，不再尝试请求新 buffer。该标志在每次 `write()` 调用开始时重置为 false（因为 `write()` 入口有 P3 drain 机会）。

#### SpillFileWriter

**职责**：向 spill 文件追加写入原始字节流。

**I/O 模式**：使用 `RandomAccessFile("rw").getChannel()` 获取 `FileChannel`，通过 `FileUtils.writeCompletely()` 处理 partial write。不调用 `force()`/`fsync()`（临时数据，丢失后重新恢复即可）。（参考：Flink SpanningWrapper 采用相同模式，见 `industry_research/spill_io_patterns.md`）

#### SpillFileReader

**职责**：从 spill 文件按 offset 和 length 读取数据块。

**I/O 模式**：使用 `FileInputStream` + `BufferedInputStream`（buffer size 32KB）顺序读取。读取不足预期字节数时抛出 IOException。

#### SpillEntry

**职责**：spill 数据条目的内存元数据。

**粒度**：每个 SpillEntry 固定为 buffer size 大小（从 `InputGate.getBufferSize()` 获取）。write() 内部按 buffer size 分割数据，每个分割块对应一个 SpillEntry。这与 checkpoint snapshot 需求对齐——每个 SpillEntry 可以直接映射到一个 checkpoint 条目。

**字段**：`InputChannelInfo channelInfo`（标识数据来源 channel）、`offset`（long，文件内位置）、`length`（int，固定为 buffer size，最后一个 entry 可能小于 buffer size）。

### 修改的类

#### RecoveredInputChannel

**变更**：新增 `requestBuffer()` 非阻塞方法（返回 null 表示无可用 buffer），供 OutputWriter P1/P2 路径判断使用。现有 `requestBufferBlocking()` 保留，供 close() drain 和非过滤模式使用。

**getBuffer 行为变更**：过滤模式下，pre-filter 的 `getBuffer()` 改为从 Heap 分配（max 5 per gate），不再通过 `requestBufferBlocking()` 可能阻塞。"顺序处理"由 `SequentialChannelStateReaderImpl` 的单线程读取保证（`readInputData` 在 channel-state-unspilling 线程串行执行，不存在并发 getBuffer 调用）。max 5 限制通过 AtomicInteger 计数器实现，per-gate 维护，通过 OutputWriter 构造参数传入。`getBuffer()` 分配时 increment，buffer 由 OutputWriter 在 `write()` 处理完毕后 recycle 并 decrement 共享计数器。超过 5 时阻塞等待（而非抛异常），因为 OutputWriter 处理后会释放 buffer。

#### RecoveredChannelStateHandler.InputChannelRecoveredStateHandler

**变更**：
- `getBuffer()` 在过滤模式下改为 Heap 分配（`MemorySegmentFactory.allocateUnpooledSegment`），max 5 per gate
- `recoverWithFiltering()` 改为调用 `OutputWriter.write()` 替代收集 `List<Buffer>`
- 移除 `bufferSupplier` 参数传递，OutputWriter 内部自行管理 buffer 请求

#### ChannelStateFilteringHandler

**变更**：
- `filterAndRewrite()` 签名变更：接收 `OutputWriter` 替代 `BufferSupplier`，返回 `void` 替代 `List<Buffer>`
- `GateFilterHandler.serializeElement()` 产出 `byte[]`（通过 `outputSerializer.getSharedBuffer()`），length prefix (4 bytes) 和 record bytes 直接写入 `OutputWriter.write(data, offset, length, channelInfo)`
- `writeDataToBuffer` 的 buffer 管理逻辑（full → request new）由 OutputWriter 的 writeToBackend 替代
- `GateFilterHandler.filterAndRewrite()` 已有 oldSubtaskIndex 和 oldChannelIndex 参数，构造 InputChannelInfo 传递给 OutputWriter.write()
- 移除 `BufferSupplier` 接口（不再需要，OutputWriter 内部管理 buffer）

#### SequentialChannelStateReaderImpl

**变更**：
- `readInputData()` 中，过滤模式下为每个 gate 创建 OutputWriter，通过 `InputChannelRecoveredStateHandler` 构造参数传入，再由 `recoverWithFiltering()` 传递给 `filterAndRewrite()`
- read loop 结束后调用 `writer.close()` blocking drain
- OutputWriter 需要 InputGate、rescalingDescriptor、inputGates、spill 目录等（通过构造参数传入）

### Spill 文件管理

**单文件共享**：每个 gate 一个 spill 文件，所有 channel 共享。数据顺序追加（FIFO），内存 `Queue<SpillEntry>` 追踪每条数据的 channelInfo、offset、length。

**文件轮转**：当文件超过 64MB 时创建新文件。旧文件在其所有 entries 被 replay 后删除。（参考：64MB 与 Flink SortMerge shuffle 的 spill 文件大小在同一数量级，见 `industry_research/spill_file_rotation_cleanup.md`）

**Spill 目录**：从 `IOManager.getSpillingDirectoriesPaths()` 获取，与 SpanningWrapper 一致。多目录间 round-robin 轮转。无 `java.io.tmpdir` fallback，无效目录抛 IOException。

**文件创建**：参考 `SpanningWrapper.createSpillingChannel()` 的模式 — round-robin 目录选择，随机文件名，`file.createNewFile()`，目录故障时从数组移除并尝试下一个。

**清理策略**：
- 正常退出：`OutputWriter.close()` 清理所有 spill 文件
- 异常退出：spill 文件创建在 IOManager 管理的目录下（即 `getSpillingDirectoriesPaths()` 返回的目录），这些目录在 TM shutdown 时由 `FileChannelManagerImpl` 整体清理

### Channel Change 检测

OutputWriter 是有状态的长生命周期对象，跨多次 `filterAndRewrite()` 调用持续存在。通过 `lastChannelInfo` 成员变量记录上一次写入的 `InputChannelInfo`，与当前调用的 channelInfo 比较自动检测 channel 变化。变化时 flush 当前 backend，再写入新数据。无需单独的 `notifyChannelChange()` 调用。

**flush 的具体行为**：
- 如果当前 backend 是 buffer（活跃 buffer 有数据）：将 buffer 通过 `channel.onRecoveredStateBuffer()` 发送到 InputChannel，然后清空活跃 buffer 引用
- 如果当前 backend 是 file：仅结束当前 SpillEntry（enqueue 到队列），无需额外操作（file 内容是连续追加的，channel 标识在 SpillEntry 元数据中）

### Byte-Position Switching

OutputWriter 可在任意字节位置切换 buffer 和 file backend。一条 record 的前半段可能在 Network Buffer 中，后半段在 File 中。Task Thread 侧的 SpanningWrapper 透明处理跨 buffer 的 record 重组。

`writeToBackend` 内部是一个循环，处理 remaining data 直到全部写完。当 buffer 满时 flush 到 InputChannel，然后根据 `downgradedToFile` 标志决定下一步：如果未降级，尝试请求新 buffer；如果失败或已降级，写入 file。这自然支持 record 在任意字节位置跨 buffer 和 file。

### Replay 机制

Replay 从 spill 文件读取 buffer-sized chunk（从 `InputGate.getBufferSize()` 获取）到 Network Buffer，通过 `channel.onRecoveredStateBuffer()` 交付给 InputChannel。不需要知道 record 边界。消费侧的 SpanningWrapper 处理跨 buffer record 重组。

P3 eager drain：每次 `write()` 调用中，replay 循环执行直到无可用 buffer 或 disk 为空，最大化吞吐。

### Checkpoint Snapshot Support

当 checkpoint 在 recovery 期间触发且 OutputWriter 有未 replay 的 disk data 时，OutputWriter 提供 `snapshotUnreplayedData(ChannelStateWriter, checkpointId)` 方法：

1. 遍历未 replay 的 SpillEntry 队列（只读遍历，不 dequeue）
2. 对每个 SpillEntry，使用 `SpillFileReader.readNextTo(OutputStream, length)` 从 disk 读取数据，直接流式写入 checkpoint storage（通过 `ChannelStateWriter.addInputData()`）
3. 不消耗 Network Buffer，避免 buffer 竞争

**并发控制**：Replay（dequeue + read）和 checkpoint snapshot（iterate + read）可能并发访问同一个 spill 文件。设计上：
- Replay 从队列头部 dequeue 并读取
- Checkpoint snapshot 遍历队列中尚未 dequeue 的 entries 并读取
- 两者使用独立的 SpillFileReader 实例（独立的 FileInputStream），各自维护读取位置
- SpillEntry 的 offset/length 是不可变的，读取不冲突

**Open questions**（待开发阶段验证）：
- checkpoint snapshot 的调用时机：由 `RecoveredInputChannel` 在 `checkpointStarted()` 中调用，还是由 `InputChannelRecoveredStateHandler` 协调？
- 如果 checkpoint snapshot 和 replay 同时消费同一个 SpillEntry（replay dequeue 了但 checkpoint 还在读），需要确保 SpillFileReader 不被提前关闭

### 资源安全

- `write()`/`close()` 在 closed 状态下抛出 `IllegalStateException`
- `OutputWriter.write()` 内部使用 try-catch，异常时回收已分配但未发送的活跃 buffer，确保无 Network Buffer 泄漏
- `OutputWriter` 实现 `AutoCloseable`，供 `SequentialChannelStateReaderImpl` 使用 try-with-resources 管理
- `SpillFileWriter.close()` 使用 try-finally 保证文件句柄释放
- `OutputWriter.close()` 幂等：重复调用不抛异常
- Spill 文件在 `close()` 时清理。Spill 文件创建在 IOManager 管理的目录下（即 `getSpillingDirectoriesPaths()` 返回的目录），这些目录在 TM shutdown 时由 `FileChannelManagerImpl` 整体清理
- Partial read（读取字节少于预期）抛出 IOException

## 常量定义

| 常量 | 默认值 | 选择理由 |
|------|--------|---------|
| SPILL_FILE_ROTATION_THRESHOLD | 64MB | 与 Flink File Merging Checkpoint (32MB) 和 RocksDB target_file_size (64MB) 在同一数量级，平衡 I/O 效率与磁盘空间回收粒度 |
| REPLAY_CHUNK_SIZE | buffer size (from InputGate config) | 与 Network Buffer 大小对齐，每个 chunk 恰好填满一个 buffer。从 InputGate.getBufferSize() 获取，非硬编码 |
| MAX_HEAP_BUFFERS_PER_GATE | 5 | 5 × 32KB = 160KB per gate，限制 Heap 内存占用。该值与 gate 的 pipeline 深度（1 个 source buffer 在处理 + 几个在队列中）匹配 |
| SPILL_READ_BUFFER_SIZE | buffer size (from InputGate config) | 与 REPLAY_CHUNK_SIZE 对齐 |

**配置策略**：
- `REPLAY_CHUNK_SIZE` 和 `SPILL_READ_BUFFER_SIZE` 从 `InputGate.getBufferSize()` 获取，与 Network Buffer 大小保持一致
- `SPILL_FILE_ROTATION_THRESHOLD` 和 `MAX_HEAP_BUFFERS_PER_GATE` 当前硬编码为常量，默认值满足大多数场景。如未来有调优需求，可通过 `CheckpointingOptions` 暴露为 `ConfigOption`
- Spill 目录从 `IOManager.getSpillingDirectoriesPaths()` 获取，与 SpanningWrapper 一致，无需单独配置

## 不变性约束

- 非过滤模式（unalignedDuringRecoveryEnabled=false 或 NO_RESCALE）完全不受影响：不创建 OutputWriter，不分配 Heap Buffer，走原始路径
- 现有 `ResultSubpartitionRecoveredStateHandler`（output side）不受影响
- 现有 `SpillingAdaptiveSpanningRecordDeserializer` 和 `SpanningWrapper` 不修改

## 提交计划

详见 `commit_plan.md`。7 个 commit 按依赖关系分阶段提交，每个 commit 独立可编译、可测试。
