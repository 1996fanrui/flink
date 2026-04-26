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

- **Recovery 线程**（channel-state-unspilling 线程）：执行过滤循环、依次显式调用 `OutputWriter.write()` → `flush()` → `stateHandler.finishRecovery()` → `OutputWriter.drainPendingSpill()` → `OutputWriter.close()`。调用顺序契约见 `close_drain_separation.md`。
- **Task 线程**：从 InputChannel 消费。调用 store.tryTake()、store.checkpoint()、store.releaseAll()

RecoveredBufferStore 被两个线程并发访问，需要线程安全保证。Spill 文件的 checkpoint 读取和 drain 读取使用独立的 Reader 实例，通过 FileChannel positional read 支持并发。

## 新增组件

### OutputWriter（per-task，REQ-0EG7）

**职责**：管理过滤后数据的 buffer 分配和磁盘 spilling。一个 task 的所有 gate 和 channel 共享一个 OutputWriter。

**包**：`org.apache.flink.runtime.checkpoint.channel`

**接口方法**：

- `write(byte[] data, int length, InputChannelInfo channelInfo)` — 写入过滤后的字节到目标 channel。内部处理：channel 变更检测（flush 当前 buffer）、P3 贪心重放、writeToBackend（P1 或 P2）
- `flush()` — 将活跃 buffer 的部分数据 flush 到目标 Store。flush 后不允许再调用 write()
- `drainPendingSpill()` — 阻塞 drain：逐个从 FIFO 队列取 SpillEntry，每个 entry `requestBufferBlocking()` 获取一个 buffer → 一次磁盘读（`entry.length ≤ memorySegmentSize`）→ 投递到目标 Store，直到队列为空。drain 结束后调用 `markComplete()` 标记每个 Store 完成。**不持 dispatcher monitor**；可被 `Thread.interrupt()` 打断。生产者-消费者语义：拿不到 buffer 就阻塞（详见 `close_drain_separation.md`）
- `close()` — 仅资源释放：清理 spill 文件、关闭 file channel、清字段。短锁、不阻塞、幂等、不抛业务异常。abort 路径直接调用 close 跳过 drain，残留 entries 与文件一并丢弃

**构造参数**：

- `Map<InputChannelInfo, RecoveredBufferStoreImpl> storesByChannel` — 所有 channel 的 store 引用，用于根据 channelInfo 投递 buffer
- `String[] spillDirs` — 来自 IOManager.getSpillingDirectoriesPaths()（REQ-SPDR）
- `int memorySegmentSize` — buffer 大小，用于磁盘重放时的分块粒度
- `Supplier<Buffer> bufferSupplier` — 非阻塞 buffer 请求（P1、P3 路径）
- `BlockingSupplier<Buffer> blockingBufferSupplier` — 阻塞 buffer 请求（`drainPendingSpill` 路径）

**内部状态**：

- 活跃 buffer（当前正在写入的 Network Buffer）
- 活跃 SpillEntry 累积状态（起始 offset、已累积长度、当前 channelInfo）。与活跃 buffer 对称：活跃 buffer 累积内存数据，活跃 SpillEntry 累积磁盘数据
- 当前 channelInfo（检测 channel 变更）
- FilteredSpillFile（管理磁盘写入和文件轮转）
- 全局 FIFO SpillEntry 队列（已密封 entry 的重放顺序）
- flushed 标志（flush 后拒绝 write）
- closed 标志（幂等 close）

**writeToBackend 行为（REQ-WRTR）**：在一次 writeToBackend 调用内，后端只能降级（buffer → file），不能升级。降级后将剩余字节写入活跃 SpillEntry（追加到 spill 文件），活跃 SpillEntry 累积满 memorySegmentSize 时密封并加入 FIFO 队列，开始新的活跃 SpillEntry。升级机会在下一次 write() 调用时通过 P3 drain 获得。

**channel 变更检测（REQ-CHDL）**：自动比较当前 channelInfo 与上次调用。如果不同：flush 当前活跃 buffer 到目标 Store，密封当前活跃 SpillEntry（如有）到 FIFO 队列，再写入新数据。

### RecoveredBufferStore（per-channel，REQ-7388）

**职责**：每个 channel 一个实例。持有就绪 buffer 队列和 pending 磁盘数据计数。被 RecoveredInputChannel（recovery 阶段）和 Local/RemoteInputChannel（conversion 后）共同使用。Store 不持有 SpillEntry 对象，不直接访问磁盘——磁盘数据的全部生命周期（写入、读取、checkpoint）由 OutputWriter 统一管理。

**包**：`org.apache.flink.runtime.io.network.partition.consumer`

**公共接口**（供 InputChannel 调用，Task 线程）：

- `tryTake()` — 非阻塞获取下一个就绪 buffer，无就绪 buffer 时返回 null
- `peekNextDataType()` — 查看下一个 buffer 的 DataType，不消费
- `isEmpty()` — 无就绪 buffer 且 pending 计数为 0
- `isComplete()` — 所有数据已消费且 drain 已完成（OutputWriter 调用了 markComplete）
- `size()` — 就绪 buffer 数量
- `checkpoint(ChannelStateWriter writer, long checkpointId)` — snapshot 就绪 buffer（REQ-KM7C）。store 在构造时已绑定一个 `InputChannelInfo`，无需额外传入。磁盘数据的 checkpoint 由 OutputWriter 统一处理（见下方 Checkpoint 实现）
- `releaseAll()` — 回收所有就绪 buffer，清理资源

**内部方法**（供 OutputWriter 调用，Recovery 线程）：

- `addBuffer(Buffer)` — 添加就绪 buffer。如果队列从空变非空，触发通知回调唤醒 InputChannel
- `markComplete()` — 标记 store 完成。`drainPendingSpill()` 末尾调用
- `setDataAvailableCallback(Runnable)` — 设置数据可用回调（synchronized，保证 channel conversion 时与 addBuffer 的可见性）。channel conversion 时需要更新回调指向新的 InputChannel
- `incrementPending()` — OutputWriter spill 数据时调用（P2 路径），递增 pending 计数
- `decrementPending()` — OutputWriter 重放磁盘数据时调用（P3 eagerDrain 或 `drainPendingSpill` 路径），递减 pending 计数

**为什么 Store 不持有 SpillEntry**：OutputWriter 的 spillEntryQueue 是全局 FIFO（per-task），其中不同 channel 的 entries 是交错的（因为 `extractOffsetsSorted` 按文件 offset 排序，不按 channel 分组）。如果 Store 持有 SpillEntry 对象，会产生两个问题：(1) 双重记账——同一个 SpillEntry 同时在 OutputWriter 队列和 Store 列表中维护，add/remove 需要同步；(2) Store 持有 SpillEntry 意味着 Store 需要 file reader 才能读取数据，但 reader 由 OutputWriter 管理。用 pending 计数替代 SpillEntry 列表，Store 只需知道"是否还有磁盘数据"，不需要知道"磁盘数据在哪里"。

**队列隐式容量限制**：Store 的就绪 buffer 队列无需显式容量上限。drain loop 通过 requestBufferBlocking() 从 Network Buffer Pool 获取 buffer，pool 大小有限。当所有 pool buffer 都在各 Store 的队列中时，requestBufferBlocking() 自然阻塞，直到 Task 线程消费并回收 buffer。这构成天然背压机制，确保队列总大小不超过 pool 容量。

**Checkpoint 实现（REQ-KM7C）**：

Checkpoint 分两阶段完成，就绪 buffer 和磁盘数据分别由不同组件负责：

**阶段 1：就绪 buffer（Store 负责，per-channel 触发）**

每个 channel 触发 `checkpointStarted()` 时调用 `store.checkpoint()`：retain 队列中每个 buffer，传给 `ChannelStateWriter.addInputData(CloseableIterator<Buffer>)`（现有 API）。

**阶段 2：磁盘数据（OutputWriter 负责，等所有 channel 触发后统一执行）**

OutputWriter 等待所有 channel 都触发 checkpoint 后，**一次性顺序遍历** spillEntryQueue，对每个 entry 通过 file reader 的 `openInputStream(offset, length)` 获取 InputStream，传给 `ChannelStateWriter.addInputData(checkpointId, entry.getChannelInfo(), seqNum, InputStream, dataLength)`（流式重载）。

**为什么等所有 channel 触发后批量执行**：

1. **顺序 I/O**：spillEntryQueue 中 entries 按文件 offset 顺序排列，一次遍历 = 顺序读磁盘。如果按 channel 逐个触发，每次需要扫描队列过滤特定 channel 的 entries，导致同一文件的随机读
2. **数据一致性**：每个 channel 内的 FIFO 顺序是 `[ready buffers] → [spill entries]`。阶段 1 先 snapshot 就绪 buffer（per-channel 触发时立即执行），阶段 2 后 snapshot 磁盘数据（全部触发后执行）。先 buffer 后 disk，天然保证顺序
3. **一次读取覆盖全部**：一次遍历写出所有 channel 的所有 pending entries，无需 per-channel 扫描或索引。`addInputData` 接口接受 `channelInfo` 参数，interleaved 写入由 ChannelStateWriter 内部按 channel 聚合

### Checkpoint 流式写入扩展

扩展 checkpoint 写入管线，增加流式路径，使磁盘数据能不经过 Buffer 直接写入 checkpoint 存储。

**涉及的现有类**（新增方法，不修改现有行为）：

- `ChannelStateWriter`（接口）：新增 `addInputData(long checkpointId, InputChannelInfo info, int startSeqNum, InputStream data, int dataLength)` 重载
- `ChannelStateWriterImpl`：实现新重载，创建流式写入请求并提交到执行器
- `ChannelStateWriteRequest`：新增 `buildStreamingWriteRequest()` 工厂方法，接受 InputStream 而非 CloseableIterator\<Buffer\>
- `ChannelStateCheckpointWriter`：新增 `writeInputStreaming()` 方法，从 InputStream 读数据写入 DataOutputStream

**写入格式兼容**：流式路径写入的格式与现有 Buffer 路径完全一致：`[4字节长度前缀][数据字节]`。Recovery 读取路径无需任何修改。

**I/O 传输**：writeInputStreaming() 通过 8KB byte[] 手动循环从 InputStream 读取并写入 DataOutputStream（`transferTo` 无法精确控制写入长度），不分配 Network Buffer Pool 或 heap buffer。

### SpillFile I/O

**包**：`org.apache.flink.runtime.checkpoint.channel`

#### FilteredSpillFile

**职责**：追加原始字节到 spill 文件。管理文件轮转。

- 使用 FileChannel（与 Flink 现有 spill 代码一致，参见业界调研 spill_io_patterns.md）
- 追加写入：`write(byte[] data, int offset, int length)` → 返回文件内 offset。使用 `FileUtils.writeCompletely()` 保证完整写入
- 构造参数仅 `String[] spillDirs`（无 memorySegmentSize，该参数不在 writer 内部使用）
- 文件轮转：当文件超过 64MB 时创建新文件（REQ-SFMG，参见业界调研 spill_file_rotation_cleanup.md）
- 多目录 round-robin 轮转（参见业界调研 spill_file_rotation_cleanup.md）
- 不调用 fsync（临时数据，参见业界调研 spill_io_patterns.md）
- close 后 write 抛 IllegalStateException（REQ-JD2C）
- close() 使用 try-finally 保证文件句柄释放（REQ-JD2C）

#### FilteredSpillFile.Reader

**职责**：从 spill 文件顺序读取数据。

- 使用 FileChannel positional read 支持并发访问
- `read(long offset, byte[] buffer, int length)` — 从指定 offset 读取（drain 加载用）
- `openInputStream(long offset, int length)` — 返回定长 InputStream，从指定 offset 读取指定长度。供 checkpoint 流式写入使用（通过 ChannelStateWriter 流式重载直接写入 checkpoint DataOutputStream，不消耗 Network Buffer Pool 或 heap buffer）
- Partial read 检测：读取字节数少于预期时抛 IOException（REQ-T5AJ）

#### SpillEntry

**结构**：`{InputChannelInfo channelInfo, long offset, int length}`（REQ-BFSD）

- channelInfo：目标 channel（post-rescaling），用于重放时投递到正确的 Store
- offset：spill 文件内的字节偏移（long 类型，避免 2GB 限制，参见业界调研 spill_metadata_management.md）
- length：数据长度。最大为 memorySegmentSize（默认 32KB），最后一个 entry 可能更小
- 不可变对象：仅在密封时创建（参见业界调研 spill_metadata_management.md）

**与 Network Buffer 的 1:1 对应**（REQ-RPLY）：一个 SpillEntry 对应恰好一个 Network Buffer。SpillEntry 的最大 length = memorySegmentSize，因此一次磁盘读取直接加载到一个 Network Buffer，无需分块或拼接。

**累积与密封**：多次 write() 调用的数据累积到同一个 SpillEntry，直到以下任一条件触发密封：
1. 累积长度达到 memorySegmentSize → 密封当前 entry，开始新 entry
2. channelInfo 变更 → 密封当前 entry（可能部分填充），为新 channel 开始新 entry
3. flush() 或 close() → 密封当前 entry（可能部分填充）

OutputWriter 内部追踪当前累积状态（起始 offset、已累积长度、当前 channelInfo），仅在密封时创建不可变的 SpillEntry 对象并加入 FIFO 队列。

**SpillEntry 不持有文件引用**：SpillEntry 是纯元数据，不持有 FilteredSpillFile.Reader、文件路径或任何 I/O 资源引用。文件定位由 OutputWriter 负责：OutputWriter 内部维护 `allFilteredSpillFile.Readers` 列表和 `lastKnownFileCount` 计数器，文件轮转时检测到新文件创建则生成新 reader（同一文件共享同一 reader 实例，避免重复打开 FileChannel）。密封 SpillEntry 时，OutputWriter 记录当前 reader 的关联（entries 在 FIFO 队列中按文件顺序排列，drain 时按队列顺序遍历，天然切换 reader）。全局 FIFO 队列保证重放顺序，文件按创建顺序依次读取和删除。

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
- **修改 requestBufferBlocking()**：filtering 模式下移除 Heap Buffer 回退（`MemorySegmentFactory.allocateUnpooledSegment` 调用删除），改为纯阻塞等待 Network Buffer。仅 `OutputWriter.drainPendingSpill()` 和 non-filtering 模式使用。non-filtering 模式不变（REQ-GGPR, REQ-NPBY）
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

- **修改 getBuffer() (InputChannelRecoveredStateHandler)**：filtering 模式下，pre-filter source buffer 使用 Heap 内存分配（`MemorySegmentFactory.allocateUnpooledSegment`），隔离于 Network Buffer Pool（REQ-NHLB）。每个 task 只分配一个 `MemorySegment` 并反复复用（REQ-QY68）；自定义 BufferRecycler 在 recycle 时翻转 `inUse` 标志；下一次 getBuffer 前 assert 上一次已 recycle，违反则抛 IllegalStateException
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
  4. OutputWriter / StateHandler / FilteringHandler 仍纳入 try-with-resources 管理资源释放，但**业务步骤显式调用**，不再依赖 reverse close 顺序（详见 `close_drain_separation.md`）：
     - try-with-resources 声明 FilteringHandler、OutputWriter、StateHandler
     - try block 内显式按序调用：`read()` 两次 → `outputWriter.flush()` → `stateHandler.finishRecovery()` → `outputWriter.drainPendingSpill()`
     - try-with-resources 退出时反向自动调用三者的 `close()`，三者的 `close()` 都只做资源释放（短锁、非阻塞、幂等）
     - 异常路径（read / flush / finishRecovery / drainPendingSpill 抛异常）：try-with-resources 仍然保证三者 close 被调用，资源被释放，且不会因为残留 spill 阻塞

### Checkpoint 写入管线扩展

**涉及文件**（均在 `flink-runtime/.../checkpoint/channel/` 包下）：

- `ChannelStateWriter.java`：新增 `addInputData(long checkpointId, InputChannelInfo info, int startSeqNum, InputStream data, int dataLength)` 重载
- `ChannelStateWriterImpl.java`：实现新重载
- `ChannelStateWriteRequest.java`：新增 `buildStreamingWriteRequest()` 工厂方法
- `ChannelStateCheckpointWriter.java`：新增 `writeInputStreaming()` 方法

## 生命周期

1. **创建**：readInputData() 创建 Store（per-channel）和 OutputWriter（per-task）
2. **过滤**：readChunk 循环 → getBuffer (Heap) → recover → filterAndRewrite → OutputWriter.write() → P1/P2/P3
3. **Flush**：过滤完成 → `outputWriter.flush()`，部分数据进入 Store；所有 Reader 进入 sealed 状态
4. **Channel conversion**：显式调用 `stateHandler.finishRecovery()` → `inputGate.finishReadRecoveredState()` → bufferFilteringCompleteFuture complete → Task 线程触发 `convertRecoveredInputChannels()`。Store 引用从 RecoveredInputChannel 转移到 Local/RemoteInputChannel。Store 的通知回调更新为新 InputChannel
5. **阻塞 drain**：显式调用 `outputWriter.drainPendingSpill()` 启动 drain 循环，与 Task 线程消费、checkpoint、`onChannelCheckpoint*` 并发运行（**不持 dispatcher monitor**）
6. **完成**：drain 结束 → `store.markComplete()` → InputChannel 检测 `isComplete()` 后丢弃 store
7. **资源释放**：try-with-resources 反向调用 `stateHandler.close()`、`outputWriter.close()`、`filteringHandler.close()`，分别只做各自的资源释放

## Source Buffer 内存隔离（REQ-NHLB, REQ-QY68）

pre-filter source buffer 使用 Heap 内存，与 Network Buffer Pool 完全隔离。这消除了 source buffer 和 filtered buffer 竞争同一 pool 导致的死锁。

**"一次一个" 不变式**：任意时刻 task 内最多 1 个 source heap buffer in-flight（≈ `memorySegmentSize`，默认 32KB）。由 Flink 现有机制结构性保证：
- `ChannelStateChunkReader.readChunk()` 单线程串行循环（`getBuffer → fill → recover(finally recycle)`），下一次 `getBuffer()` 只能在上一次 `recover()` 返回后开始
- `SpillingAdaptiveSpanningRecordDeserializer.getNextRecord()` 在 `isBufferConsumed=true` 时立即回收 `currentBuffer`（`PARTIAL_RECORD` 和 `LAST_RECORD_FROM_BUFFER` 两种情形），`filterAndRewrite()` 的内层 while 循环在同一条件下 break
- 跨 buffer 的字节在 recycle 前总是已经 copy 出源 buffer：`SpanningWrapper.transferFrom` / `addNextChunkFromMemorySegment` 将 partial 字节拷到内部 `byte[]`（≥ 5MB 的 record 则 spill 到 SpanningWrapper 自己的文件）
- 异常路径通过 `ChannelStateFilteringHandler.close() → VirtualChannel.clear() → deserializer.clear()` 收敛回 refcount=0

**实现要点**（REQ-QY68）：
- **复用**：每个 task 内仅分配一个 `MemorySegment`（首次 `getBuffer()` 时懒初始化），反复包装到新的 `NetworkBuffer` 返回给调用方，直到 state handler close 时释放 segment
- **运行时检查**：自定义 `BufferRecycler` 在 recycle 时将 `inUse` 标志置 false；`getBuffer()` 发出下一个 buffer 前 assert `!inUse`，违反则抛 `IllegalStateException`。一旦未来有改动意外破坏"一次一个"不变式，会立即 fail-loud，不会静默出现并发读写同一 segment 的 memory corruption
- **无 semaphore / 无 per-gate 计数器 / 无全局计数**：内存上限由不变式天然保证
- Non-filtering 模式不分配 Heap Buffer（REQ-NPBY）

## Buffer 请求接口（REQ-GGPR）

RecoveredInputChannel 提供两种 buffer 请求方法：

- `requestBuffer()` — 非阻塞，pool 用尽时返回 null。OutputWriter P1/P3 路径使用
- `requestBufferBlocking()` — 阻塞，等待 buffer 可用。非 filtering 模式和 `OutputWriter.drainPendingSpill()` 使用。**filtering 模式下移除 Heap Buffer 回退**

OutputWriter 通过构造器接收这两个方法的函数式接口引用，解耦于 RecoveredInputChannel。

## 资源管理与清理（REQ-JD2C）

三层清理防线（参见业界调研 spill_file_rotation_cleanup.md）：

1. **业务级清理**：单个 spill 文件所有 entry 重放完成后删除该文件
2. **组件级清理**：OutputWriter.close() 删除所有 spill 文件
3. **进程级清理**：TM 的 FileChannelManagerImpl shutdown hook 清理残留文件

安全保证：

- write/close on closed OutputWriter 抛 IllegalStateException
- FilteredSpillFile.close() 使用 try-finally 保证文件句柄释放
- OutputWriter.close() 幂等：重复调用不抛异常
- Spill 文件使用 IOManager 提供的目录（REQ-SPDR），无 java.io.tmpdir 回退

## 配置项与默认值

| 配置项 | 默认值 | 选择理由 |
|-------|--------|---------|
| Spill 文件轮转阈值 | 64MB | 与 Flink file-merging (32MB)、RocksDB (64MB) 同一量级，参见 `industry_research/spill_file_rotation_cleanup.md` |
| Source Buffer 内存占用 | 1 × memorySegmentSize per task（默认 32KB） | "一次一个" 不变式结构性保证；反复复用单个 segment |
| SpillEntry 最大大小 | memorySegmentSize（默认 32KB） | 与 Network Buffer 1:1 对应，一次磁盘读 = 一个 buffer |

## 设计决策与业界参考

| 决策项 | 选择 | 业界参考 |
|-------|------|---------|
| I/O API | FileChannel | Flink/Spark 统一选择，参见 `industry_research/spill_io_patterns.md` Topic 1-2 |
| Spill 文件格式 | 纯字节流，无 header/metadata | 与 Spark ExternalSorter、Flink PartitionedFile 一致，参见 `industry_research/spill_metadata_management.md` Topic 2 |
| 元数据管理 | 纯内存 Queue\<SpillEntry\> | 生命周期短、崩溃后不需恢复，参见 `industry_research/spill_metadata_management.md` Topic 1 |
| fsync | 不调用 | 临时数据，Flink/Spark 均不 fsync，参见 `industry_research/spill_io_patterns.md` Topic 3 |
| Partial write | FileUtils.writeCompletely() | Java NIO 标准模式，复用 Flink 已有工具方法，参见 `industry_research/spill_io_patterns.md` Topic 4 |
| 文件轮转阈值 | 64MB | 与 Flink file-merging (32MB)、RocksDB (64MB) 同一量级，参见 `industry_research/spill_file_rotation_cleanup.md` |
| 目录轮转 | Round-robin | 与 Flink FileChannelManagerImpl 一致，参见 `industry_research/spill_file_rotation_cleanup.md` |
| 文件清理 | 三层防线 | 覆盖正常/异常/kill-9，参见 `industry_research/spill_file_rotation_cleanup.md` |
| SpillEntry offset 类型 | long | 避免 2GB 限制，参见 `industry_research/spill_metadata_management.md` Topic 3 |

## 提交策略

参见 `implementation_plan.md`。6 个 JIRA，按依赖关系排列：

1. [FLINK-39519] Source Buffer Heap 分配（单 segment 复用 + 运行时检查） + 新的 `requestBuffer()` 非阻塞接口、移除 `requestBufferBlocking()` 的 heap fallback
2. [FLINK-39520] SpillFile I/O + RecoveredBufferStore
3. [FLINK-39521] OutputWriter
4. [FLINK-39522] InputChannel 从 RecoveredBufferStore 消费
5. [FLINK-39523] ChannelStateWriter 流式重载（checkpoint 用，可与其他 JIRA 并行开发）
6. [FLINK-39524] 集成：filterAndRewrite 写入 OutputWriter
