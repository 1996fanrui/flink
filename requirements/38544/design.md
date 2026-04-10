# Design — FLINK-38544 Spilling

## 目标与范围

### 核心目标

替换 `RecoveredInputChannel.requestBufferBlocking()` 中的 Heap Buffer 回退为磁盘 spilling，限制 checkpoint channel state recovery 期间的内存使用。当 Network Buffer Pool 不足时，将过滤后的数据写入磁盘而非分配无限制的 Heap Buffer。

### 明确不做的事

- 不修改 checkpoint 协议（barrier 处理、ChannelStatePersister、unaligned checkpoint）
- 不修改优先事件处理（priority event、PrioritizedDeque）
- 不修改 channel 转换逻辑（RecoveredInputChannel → Local/Remote 的触发时机和条件）
- 不修改 Task Thread 消费逻辑（SpanningWrapper 跨 buffer 记录重组）
- 不修改 non-filtering 路径（未启用 checkpoint-during-recovery 或 NO_RESCALE 时原有逻辑不变）

### 核心不变量

磁盘数据在字节级别与 Heap Buffer 数据完全等价：相同的字节序列、不同的存储介质。原有 Heap Buffer 能工作的所有场景，磁盘数据加载回 Network Buffer 后必须完全相同地工作。

## 核心架构

三个核心组件解耦了过滤、缓冲管理和消费：

- **filterAndRewrite** 将过滤后的字节写入 `OutputWriter.write(data, length, channelInfo)`，不关心 buffer 分配或磁盘 spilling
- **OutputWriter**（per-task）管理 buffer 分配和磁盘 spilling，将就绪 buffer 投递到目标 channel 的 RecoveredBufferStore
- **RecoveredBufferStore**（per-channel）向 InputChannel 提供就绪 buffer，支持 checkpoint snapshot

数据流：

```
S3 → Heap Buffer (source) → filterAndRewrite → OutputWriter
                                                  ├── P1: Network Buffer → Store → InputChannel
                                                  ├── P2: Disk (spill file)
                                                  └── P3: Disk → Network Buffer → Store → InputChannel
```

### 三条数据路径（REQ-8HRS）

- **P1**：Network Buffer 可用且磁盘无未重放数据 → 写入 buffer → 目标 channel 的 Store
- **P2**：Network Buffer 不可用 → 写入磁盘文件
- **P3**：Network Buffer 可用但磁盘有未重放数据 → 重放最旧的磁盘数据到目标 channel 的 Store（FIFO 顺序）。P3 优先于 P1，确保数据顺序正确

P3 贪心重放：循环直到无 buffer 可用或磁盘为空。

### 线程模型

- **Recovery 线程**（channel-state-unspilling 线程）：执行过滤循环、调用 OutputWriter.write()、flush()、close()。调用 finishReadRecoveredState()
- **Task 线程**：从 InputChannel 消费。调用 store.tryTake()、store.checkpoint()、store.releaseAll()

RecoveredBufferStore 被两个线程并发访问，需要线程安全保证。Spill 文件的 checkpoint 读取和 drain 读取使用独立的 Reader 实例，通过 FileChannel positional read 支持并发。

## 新增组件

### OutputWriter（per-task，REQ-0EG7）

**职责**：管理过滤后数据的 buffer 分配和磁盘 spilling。一个 task 的所有 gate 和 channel 共享一个 OutputWriter。

**包**：`org.apache.flink.runtime.checkpoint.channel`

**接口方法**：

- `write(byte[] data, int length, InputChannelInfo channelInfo)` — 写入过滤后的字节到目标 channel。内部处理：channel 变更检测（flush 当前 buffer）、P3 贪心重放、writeToBackend（P1 或 P2）
- `flush()` — 将活跃 buffer 的部分数据 flush 到目标 Store。flush 后不允许再调用 write()
- `close()` — 阻塞 drain：循环 requestBufferBlocking() → 从磁盘加载 → 投递到目标 Store，直到磁盘为空。清理 spill 文件。标记所有 store 为 complete。幂等

**构造参数**：

- `Map<InputChannelInfo, RecoveredBufferStoreImpl> storesByChannel` — 所有 channel 的 store 引用，用于根据 channelInfo 投递 buffer
- `String[] spillDirs` — 来自 IOManager.getSpillingDirectoriesPaths()（REQ-SPDR）
- `int memorySegmentSize` — buffer 大小，用于磁盘重放时的分块粒度
- `Supplier<Buffer> bufferSupplier` — 非阻塞 buffer 请求（P1、P3 路径）
- `BlockingSupplier<Buffer> blockingBufferSupplier` — 阻塞 buffer 请求（close drain 路径）

**内部状态**：

- 活跃 buffer（当前正在写入的 Network Buffer）
- 当前 channelInfo（检测 channel 变更）
- SpillFileWriter（管理磁盘写入和文件轮转）
- 全局 FIFO SpillEntry 队列（重放顺序）
- flushed 标志（flush 后拒绝 write）
- closed 标志（幂等 close）

**writeToBackend 行为（REQ-WRTR）**：在一次 writeToBackend 调用内，后端只能降级（buffer → file），不能升级。降级后创建磁盘数据，后续字节被强制走文件路径。升级机会在下一次 write() 调用时通过 P3 drain 获得。

**channel 变更检测（REQ-CHDL）**：自动比较当前 channelInfo 与上次调用。如果不同，flush 当前活跃 buffer 到目标 Store 后再写入新数据。

### RecoveredBufferStore（per-channel，REQ-7388）

**职责**：每个 channel 一个实例。持有就绪 buffer 队列，隐藏所有磁盘细节。被 RecoveredInputChannel（recovery 阶段）和 Local/RemoteInputChannel（conversion 后）共同使用。

**包**：`org.apache.flink.runtime.io.network.partition.consumer`

**公共接口**（供 InputChannel 调用，Task 线程）：

- `tryTake()` — 非阻塞获取下一个就绪 buffer，无就绪 buffer 时返回 null
- `peekNextDataType()` — 查看下一个 buffer 的 DataType，不消费
- `isEmpty()` — 无就绪 buffer 且无 pending 磁盘数据
- `isComplete()` — 所有数据已消费且 drain 已完成（OutputWriter 调用了 markComplete）
- `size()` — 就绪 buffer 数量
- `checkpoint(ChannelStateWriter, checkpointId, channelInfo)` — snapshot 就绪 buffer + 磁盘数据（REQ-KM7C）
- `releaseAll()` — 回收所有就绪 buffer，清理资源

**内部方法**（供 OutputWriter 调用，Recovery 线程）：

- `addBuffer(Buffer)` — 添加就绪 buffer。如果队列从空变非空，触发通知回调唤醒 InputChannel
- `markComplete()` — 标记 store 完成。close() drain 结束后调用
- `setNotificationCallback(Runnable)` — 设置通知回调。channel conversion 时需要更新回调指向新的 InputChannel
- `addPendingSpillEntry(SpillEntry)` — OutputWriter spill 数据时调用（P2 路径），store 持有 entry 引用供 checkpoint 和 isEmpty 使用
- `removePendingSpillEntry(SpillEntry)` — OutputWriter 重放磁盘数据时调用（P3/drain 路径）

**队列隐式容量限制**：Store 的就绪 buffer 队列无需显式容量上限。drain loop 通过 requestBufferBlocking() 从 Network Buffer Pool 获取 buffer，pool 大小有限。当所有 pool buffer 都在各 Store 的队列中时，requestBufferBlocking() 自然阻塞，直到 Task 线程消费并回收 buffer。这构成天然背压机制，确保队列总大小不超过 pool 容量。

**Checkpoint 实现（REQ-KM7C）**：

checkpoint() 方法 snapshot 两部分数据：
1. 就绪 buffer：retain 队列中每个 buffer，传给 `ChannelStateWriter.addInputData(CloseableIterator<Buffer>)`（现有 API）
2. 磁盘数据：遍历 pending SpillEntry 列表，对每个 entry 通过 `SpillFileReader.openInputStream(offset, length)` 获取定长 InputStream，传给 `ChannelStateWriter.addInputData(checkpointId, info, seqNum, InputStream, dataLength)`（新增流式重载）。从 spill 文件直接流式写入 checkpoint DataOutputStream，不消耗 Network Buffer Pool 或 heap buffer

Store 持有属于本 channel 的 pending SpillEntry 列表（由 OutputWriter 在 P2 路径时通过 `addPendingSpillEntry()` 添加，drain 重放时通过 `removePendingSpillEntry()` 移除）。checkpoint 遍历此列表读取数据，但不消费 entry（entry 仍留给 drain 重放）。

checkpoint 运行在 Task 线程，drain 运行在 Recovery 线程，两者通过独立 SpillFileReader 实例并发读取同一文件（FileChannel 支持 positional read）。

### Checkpoint 流式写入扩展

扩展 checkpoint 写入管线，增加流式路径，使磁盘数据能不经过 Buffer 直接写入 checkpoint 存储。

**涉及的现有类**（新增方法，不修改现有行为）：

- `ChannelStateWriter`（接口）：新增 `addInputData(long checkpointId, InputChannelInfo info, int startSeqNum, InputStream data, int dataLength)` 重载
- `ChannelStateWriterImpl`：实现新重载，创建流式写入请求并提交到执行器
- `ChannelStateWriteRequest`：新增 `buildStreamingWriteRequest()` 工厂方法，接受 InputStream 而非 CloseableIterator\<Buffer\>
- `ChannelStateCheckpointWriter`：新增 `writeInputStreaming()` 方法，从 InputStream 读数据写入 DataOutputStream

**写入格式兼容**：流式路径写入的格式与现有 Buffer 路径完全一致：`[4字节长度前缀][数据字节]`。Recovery 读取路径无需任何修改。

**I/O 传输**：writeInputStreaming() 通过 `InputStream.transferTo(DataOutputStream)` 流式拷贝，不分配任何 Buffer 对象（Network Buffer Pool 或 heap buffer）。

### SpillFile I/O

**包**：`org.apache.flink.runtime.checkpoint.channel`

#### SpillFileWriter

**职责**：追加原始字节到 spill 文件。管理文件轮转。

- 使用 FileChannel（与 Flink 现有 spill 代码一致，参见业界调研 spill_io_patterns.md）
- 追加写入：`write(byte[] data, int offset, int length)` → 返回文件内 offset
- 文件轮转：当文件超过 64MB 时创建新文件（REQ-SFMG，参见业界调研 spill_file_rotation_cleanup.md）
- 多目录 round-robin 轮转（参见业界调研 spill_file_rotation_cleanup.md）
- 不调用 fsync（临时数据，参见业界调研 spill_io_patterns.md）
- close() 使用 try-finally 保证文件句柄释放（REQ-JD2C）

#### SpillFileReader

**职责**：从 spill 文件顺序读取数据。

- 使用 FileChannel positional read 支持并发访问
- `read(long offset, byte[] buffer, int length)` — 从指定 offset 读取（drain 加载用）
- `openInputStream(long offset, int length)` — 返回定长 InputStream，从指定 offset 读取指定长度。供 checkpoint 流式写入使用（通过 ChannelStateWriter 流式重载直接写入 checkpoint DataOutputStream，不消耗 Network Buffer Pool 或 heap buffer）
- Partial read 检测：读取字节数少于预期时抛 IOException（REQ-T5AJ）

#### SpillEntry

**结构**：`{InputChannelInfo channelInfo, long offset, int length}`（REQ-BFSD）

- channelInfo：目标 channel（post-rescaling），用于重放时投递到正确的 Store
- offset：spill 文件内的字节偏移（long 类型，避免 2GB 限制，参见业界调研 spill_metadata_management.md）
- length：数据长度。变长（一次 write() 调用的数据量，可大于 32KB）
- 不可变对象（参见业界调研 spill_metadata_management.md）

**重放粒度**（REQ-RPLY）：一个 SpillEntry 按 memorySegmentSize（默认 32KB）分块加载到 Network Buffer。一个 SpillEntry 可能需要多个 Network Buffer。

**与文件的关系**：Entry 通过 offset 定位数据在 spill 文件中的位置。文件轮转时，Entry 需要能定位到正确的文件。

**文件轮转后定位**：OutputWriter 内部维护文件序号映射。SpillEntry 在实现层面需要能定位到正确的物理文件（通过文件序号或文件引用）。全局 FIFO 队列保证重放顺序，文件按创建顺序依次读取和删除。

### Spill 文件管理（REQ-SFMG, REQ-CRSR）

- 一个 task 内所有 gate 和 channel 共享单个 spill 文件（与 per-task 过滤线程一致）
- 数据顺序追加（FIFO），内存 Queue\<SpillEntry\> 追踪每个 entry 的元数据
- "磁盘有数据" = 未重放的 entry 存在（queue 非空），不是物理文件存在
- 文件轮转：超过 64MB 创建新文件。旧文件在所有 entry 重放后删除
- 读写游标单调递增，无随机访问需求

## 现有代码修改范围

### RecoveredInputChannel

**文件**：`flink-runtime/.../io/network/partition/consumer/RecoveredInputChannel.java`

修改项：

- **新增 RecoveredBufferStore 字段**：替代 receivedBuffers (ArrayDeque\<Buffer\>)
- **新增 requestBuffer()**：非阻塞 buffer 请求，包装 bufferManager.requestBuffer()。供 OutputWriter P1/P3 路径使用（REQ-GGPR）
- **修改 requestBufferBlocking()**：filtering 模式下移除 Heap Buffer 回退（`MemorySegmentFactory.allocateUnpooledSegment` 调用删除），改为纯阻塞等待 Network Buffer。仅 OutputWriter.close() drain 和 non-filtering 模式使用。non-filtering 模式不变（REQ-GGPR, REQ-NPBY）
- **修改 getNextBuffer()**：从 store.tryTake() 获取
- **删除 onRecoveredStateBuffer()**：OutputWriter 通过 store.addBuffer() 直接投递
- **修改 toInputChannel()**：传递 store 引用给新的物理 channel，不再提取 remainingBuffers
- **修改 finishReadRecoveredState()**：仍完成 bufferFilteringCompleteFuture。EndOfInputChannelStateEvent 的处理方式可能调整（store 完成状态由 markComplete 管理）
- **修改 releaseAllResources()**：调用 store.releaseAll()
- **修改 getBuffersInUseCount()**：返回 store.size()

### LocalRecoveredInputChannel / RemoteRecoveredInputChannel

**文件**：`flink-runtime/.../consumer/LocalRecoveredInputChannel.java`, `RemoteRecoveredInputChannel.java`

修改项：

- **修改 toInputChannelInternal()**：传递 store 给新的物理 channel 构造器，不再传递 remainingBuffers

### LocalInputChannel

**文件**：`flink-runtime/.../consumer/LocalInputChannel.java`

修改项（REQ-TXGD, REQ-G4KW）：

- **新增 RecoveredBufferStore 字段**（可空，仅 recovery 场景有值）
- **删除 initialRecoveredBuffers 构造参数**和 buffer 迁移逻辑（lines 120-141 的 BufferAndBacklog 转换循环）。toBeConsumedBuffers 仅保留给 FullyFilledBuffer splits（正常数据路径，与 recovery 无关）
- **修改 getNextBuffer()**：检查顺序变为 store → toBeConsumedBuffers → subpartitionView。store 非空时优先从 store 获取
- **修改 getNextRecoveredBuffer()**：数据源从 toBeConsumedBuffers 改为 store.tryTake()。优先事件处理（hasPendingPriorityEvent）保持不变
- **修改 checkpointStarted()**：recovered 数据的 checkpoint 委托给 store.checkpoint()，替代遍历 toBeConsumedBuffers 收集 inflightBuffers。priority event 和 ChannelStatePersister 逻辑不变
- **修改 getBuffersInUseCount() / unsynchronizedGetNumberOfQueuedBuffers()**：加上 store.size()
- **修改 releaseAllResources()**：调用 store.releaseAll()
- **Store 生命周期**：当 store.isComplete() 返回 true 时，InputChannel 可以丢弃 store 引用

### RemoteInputChannel

**文件**：`flink-runtime/.../consumer/RemoteInputChannel.java`

修改项（REQ-TXGD, REQ-G4KW）：

- **新增 RecoveredBufferStore 字段**（可空）
- **删除 initialRecoveredBuffers 构造参数**和 buffer 迁移逻辑（lines 162-183 的 SequenceBuffer 转换循环）。receivedBuffers 仅保留给网络数据
- **修改 getNextBuffer()**：store → receivedBuffers
- **删除 checkReadability() hack**：receivedBuffers 仅包含网络数据，必须有 partitionRequestClient 初始化
- **修改 checkpointStarted()**：recovered 数据调用 store.checkpoint()，网络数据调用 getInflightBuffersUnsafe()（现有逻辑不变）。RecoveryMetadata 追加逻辑留在 RemoteInputChannel 中
- **修改 getBuffersInUseCount() / unsynchronizedGetNumberOfQueuedBuffers()**：加上 store.size()
- **修改 releaseAllResources()**：调用 store.releaseAll()

### RecoveredChannelStateHandler

**文件**：`flink-runtime/.../checkpoint/channel/RecoveredChannelStateHandler.java`

修改项：

- **修改 getBuffer() (InputChannelRecoveredStateHandler)**：filtering 模式下，pre-filter source buffer 使用 Heap 内存分配（`MemorySegmentFactory.allocateUnpooledSegment`），隔离于 Network Buffer Pool（REQ-NHLB）。每个 gate 最多 5 个 Heap Buffer（REQ-QY68），通过 AtomicInteger per-gate 计数器控制
- **修改 recoverWithFiltering()**：调用 filterAndRewrite 时传入 OutputWriter 和目标 channelInfo（post-rescaling），不再收集 `List<Buffer>` 和调用 `onRecoveredStateBuffer()`
- **新增 OutputWriter 字段**：构造时接收，传递到 filterAndRewrite

### ChannelStateFilteringHandler

**文件**：`flink-runtime/.../checkpoint/channel/ChannelStateFilteringHandler.java`

修改项（REQ-0EG7）：

- **修改 filterAndRewrite() 签名**：接受 OutputWriter 和目标 InputChannelInfo，返回 void（不再返回 `List<Buffer>`）
- **修改 serializeElement()**：写入 length prefix + record bytes 到 `outputWriter.write(data, length, channelInfo)`。data 来自 `outputSerializer.getSharedBuffer()`
- **删除 writeDataToBuffer()**：buffer 生命周期管理由 OutputWriter 内部处理
- **删除 BufferSupplier 接口**：不再需要

### SequentialChannelStateReaderImpl

**文件**：`flink-runtime/.../checkpoint/channel/SequentialChannelStateReaderImpl.java`

修改项：

- **修改 readInputData()**：
  1. 创建 RecoveredBufferStore（per-channel），与 RecoveredInputChannel 关联
  2. 创建 OutputWriter（per-task），引用所有 channel 的 store
  3. 将 OutputWriter 传递给 InputChannelRecoveredStateHandler
  4. OutputWriter 纳入 try-with-resources 管理生命周期，保证异常时资源清理：
     - try-with-resources 声明 OutputWriter、FilteringHandler、StateHandler
     - try block 内：`read()` 两次 → `outputWriter.flush()`
     - StateHandler.close() 由 try-with-resources 自动调用（finishReadRecoveredState → channel conversion）
     - OutputWriter.close() 由 try-with-resources 自动调用（阻塞 drain + 清理 spill 文件）
     - close 顺序由 try-with-resources 反向保证：先 StateHandler，再 OutputWriter

### Checkpoint 写入管线扩展

**涉及文件**（均在 `flink-runtime/.../checkpoint/channel/` 包下）：

- `ChannelStateWriter.java`：新增 `addInputData(long checkpointId, InputChannelInfo info, int startSeqNum, InputStream data, int dataLength)` 重载
- `ChannelStateWriterImpl.java`：实现新重载
- `ChannelStateWriteRequest.java`：新增 `buildStreamingWriteRequest()` 工厂方法
- `ChannelStateCheckpointWriter.java`：新增 `writeInputStreaming()` 方法

## 生命周期

1. **创建**：readInputData() 创建 Store（per-channel）和 OutputWriter（per-task）
2. **过滤**：readChunk 循环 → getBuffer (Heap) → recover → filterAndRewrite → OutputWriter.write() → P1/P2/P3
3. **Flush**：过滤完成 → outputWriter.flush()，部分数据进入 Store
4. **Channel conversion**：finishReadRecoveredState() → bufferFilteringCompleteFuture complete → Task 线程触发 convertRecoveredInputChannels()。Store 引用从 RecoveredInputChannel 转移到 Local/RemoteInputChannel。Store 的通知回调更新为新 InputChannel
5. **阻塞 drain**：outputWriter.close() 启动 drain 循环，与 Task 线程消费和 checkpoint 并发运行
6. **完成**：drain 结束 → store.markComplete() → InputChannel 检测 isComplete() 后丢弃 store

## Source Buffer 内存隔离（REQ-NHLB, REQ-QY68）

pre-filter source buffer 使用 Heap 内存，与 Network Buffer Pool 完全隔离。这消除了 source buffer 和 filtered buffer 竞争同一 pool 导致的死锁。

- 每个 gate 最多 5 个 Heap Buffer（约 160KB）
- Gate 按 virtual channel 顺序处理（一次处理一个 channel）
- 计数器：AtomicInteger per-gate，allocate 时 increment，source buffer 回收时 decrement
- Non-filtering 模式不分配 Heap Buffer（REQ-NPBY）

## Buffer 请求接口（REQ-GGPR）

RecoveredInputChannel 提供两种 buffer 请求方法：

- `requestBuffer()` — 非阻塞，pool 用尽时返回 null。OutputWriter P1/P3 路径使用
- `requestBufferBlocking()` — 阻塞，等待 buffer 可用。非 filtering 模式和 OutputWriter drain 使用。**filtering 模式下移除 Heap Buffer 回退**

OutputWriter 通过构造器接收这两个方法的函数式接口引用，解耦于 RecoveredInputChannel。

## 资源管理与清理（REQ-JD2C）

三层清理防线（参见业界调研 spill_file_rotation_cleanup.md）：

1. **业务级清理**：单个 spill 文件所有 entry 重放完成后删除该文件
2. **组件级清理**：OutputWriter.close() 删除所有 spill 文件
3. **进程级清理**：TM 的 FileChannelManagerImpl shutdown hook 清理残留文件

安全保证：

- write/close on closed OutputWriter 抛 IllegalStateException
- SpillFileWriter.close() 使用 try-finally 保证文件句柄释放
- OutputWriter.close() 幂等：重复调用不抛异常
- Spill 文件使用 IOManager 提供的目录（REQ-SPDR），无 java.io.tmpdir 回退

## 配置项与默认值

| 配置项 | 默认值 | 选择理由 |
|-------|--------|---------|
| Spill 文件轮转阈值 | 64MB | 与 Flink file-merging (32MB)、RocksDB (64MB) 同一量级，参见 `industry_research/spill_file_rotation_cleanup.md` |
| Source Buffer 并发上限 | 5 per gate（约 160KB） | 限制 pre-filter source buffer 内存，足够支持 gate 按 virtual channel 顺序处理 |
| SpillEntry 重放粒度 | memorySegmentSize（默认 32KB） | 对齐 Network Buffer 大小，减少碎片 |

## 设计决策与业界参考

| 决策项 | 选择 | 业界参考 |
|-------|------|---------|
| I/O API | FileChannel | Flink/Spark 统一选择，参见 `industry_research/spill_io_patterns.md` Topic 1-2 |
| Spill 文件格式 | 纯字节流，无 header/metadata | 与 Spark ExternalSorter、Flink PartitionedFile 一致，参见 `industry_research/spill_metadata_management.md` Topic 2 |
| 元数据管理 | 纯内存 Queue\<SpillEntry\> | 生命周期短、崩溃后不需恢复，参见 `industry_research/spill_metadata_management.md` Topic 1 |
| fsync | 不调用 | 临时数据，Flink/Spark 均不 fsync，参见 `industry_research/spill_io_patterns.md` Topic 3 |
| Partial write | while(hasRemaining) 循环 | Java NIO 标准模式，复用 FileUtils.writeCompletely()，参见 `industry_research/spill_io_patterns.md` Topic 4 |
| 文件轮转阈值 | 64MB | 与 Flink file-merging (32MB)、RocksDB (64MB) 同一量级，参见 `industry_research/spill_file_rotation_cleanup.md` |
| 目录轮转 | Round-robin | 与 Flink FileChannelManagerImpl 一致，参见 `industry_research/spill_file_rotation_cleanup.md` |
| 文件清理 | 三层防线 | 覆盖正常/异常/kill-9，参见 `industry_research/spill_file_rotation_cleanup.md` |
| SpillEntry offset 类型 | long | 避免 2GB 限制，参见 `industry_research/spill_metadata_management.md` Topic 3 |

## 提交策略

参见 `commit_plan.md`。6 个 commit，按依赖关系排列：

1. Source Buffer Heap 分配 + buffer 请求接口
2. SpillFile I/O + RecoveredBufferStore
3. OutputWriter
4. InputChannel 从 RecoveredBufferStore 消费
5. ChannelStateWriter 流式重载（checkpoint 用，可与 C1-C4 并行开发）
6. 集成：filterAndRewrite 写入 OutputWriter
