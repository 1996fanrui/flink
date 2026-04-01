# 设计文档 — Task 5: 两阶段 Buffer 模型与 Spilling 逻辑

## 1. 设计目标

在 channel state 恢复的过滤场景中，解决 Source Buffer 和 Filtered Buffer 竞争同一 Network Buffer Pool 导致的死锁问题，并为 Network Buffer 不足时提供可靠的 spill-to-disk 兜底机制。

核心原则：
- **内存隔离**：Source Buffer 和 Filtered Buffer 均使用 Heap 内存，与 Network Buffer Pool 完全隔离，消除死锁的资源竞争条件。P1 路径将 Heap Buffer 数据复制到 Network Buffer 再放入 InputChannel，多一次内存拷贝换取过滤永不阻塞。该方案与 PostgreSQL 多内存池隔离、Flink 自身 Framework/Task/Network/Managed 四区隔离设计一致（参考 [死锁预防调研](./industry_research/deadlock_prevention_resource_isolation.md)）
- **非阻塞过滤**：过滤线程在 Network Buffer 不足时通过 spill 到磁盘继续推进，不阻塞。采用业界标准的 Write-Recycle-Read 生命周期（参考 [Spill-to-Disk 调研](./industry_research/spill_to_disk_strategies.md)）
- **数据顺序保证**：磁盘有待 replay 数据时，P3 优先于 P1，保证 FIFO 顺序
- **Checkpoint 兼容**：磁盘上的 spill 数据能正确参与 Checkpoint 快照

## 2. 修改范围概览

| 组件 | 变更类型 | 说明 |
|------|---------|------|
| `RecoveredInputChannel` | 修改 | `requestBuffer()` 移除 LazyFileBuffer 回退，在 unaligned 模式下保持非阻塞（返回 null 当 Pool 不足） |
| `LazyFileBuffer` | 删除 | 移除不可用的 LazyFileBuffer 类及所有引用 |
| `SpillingBufferManager` | 新增 | Spill/Replay 管理器，负责三条路径的调度 |
| `SpillFileWriter` / `SpillFileReader` | 新增 | Spill 文件的写入和读取 |
| `InputChannelRecoveredStateHandler` | 修改 | 集成 SpillingBufferManager，替代当前的 LazyFileBuffer 逻辑 |
| `ChannelStateFilteringHandler` | 修改 | `filterAndRewrite()` 的 BufferSupplier 改为 Heap 内存分配 |
| `ChannelStateByteBuffer` | 修改 | 移除 `wrap(LazyFileBuffer)` 方法 |
| `RecoveredChannelStateHandler.BufferWithContext` | 修改 | 简化，不再需要 LazyFileBuffer 特殊处理 |

## 3. 详细设计

### 3.1 内存隔离：Source Buffer 使用 Heap 内存

**当前问题**：`ChannelStateFilteringHandler.filterAndRewrite()` 内部通过 `BufferSupplier.requestBufferBlocking()` 从 Network Buffer Pool 申请 Buffer 存放 S3 原始数据。多个 Channel 并发申请时可能耗尽 Pool，导致 Filtered Buffer 无法分配，形成死锁。

**设计方案**：

将 `filterAndRewrite()` 拆分为两个阶段的 Buffer 分配：

1. **Source Buffer（读 S3 → 过滤）**：改由 `ChannelStateChunkReader.readChunk()` 调用 `stateHandler.getBuffer()` 时分配。将 `InputChannelRecoveredStateHandler.getBuffer()` 改为使用 Heap 内存（`MemorySegmentFactory.allocateUnpooledSegment`），不再从 `RecoveredInputChannel.requestBuffer()` 获取 Network Buffer。

2. **Filtered Buffer（过滤结果 → InputChannel）**：`filterAndRewrite()` 内部的 `BufferSupplier` 也改为 Heap 内存分配（`MemorySegmentFactory.allocateUnpooledSegment`），使过滤过程完全不依赖 Network Buffer Pool，过滤永远不会阻塞。过滤产出的 Heap Buffer 在 P1 路径中复制到 Network Buffer 后释放，或在 P2/P3 路径中 spill 到磁盘后释放。

**Heap Buffer 数量控制**：每个 Gate 最多持有 5 个 Heap Buffer（通过 `getBuffer()` 的调用控制），每个 Buffer 约 32KB，总计 ≤ 160KB/Gate。由于 Gate 内按 Virtual Channel 顺序处理，同一时刻只有一个 Channel 在使用 Source Buffer。

**Heap Buffer 创建方式**：通过 `MemorySegmentFactory.allocateUnpooledSegment(bufferSize)` 创建 MemorySegment，再包装为 `NetworkBuffer(segment, FreeingBufferRecycler.INSTANCE, ...)` 以满足 Buffer 接口要求。

**GC 影响评估**：Heap Buffer 生命周期极短（单个 readChunk 调用内分配并释放），不会晋升到老年代，对 GC 压力可忽略。不使用对象池的理由：生命周期短于池化管理的开销，且数量受限（每 Gate ≤ 5 个）。

**修改点**：

- `InputChannelRecoveredStateHandler.getBuffer()`：不再调用 `channel.requestBuffer()`，改为创建 Heap Buffer（allocateUnpooledSegment + NetworkBuffer 包装）
- `ChannelStateByteBuffer`：移除 `wrap(LazyFileBuffer)` 分支，统一使用 `wrap(Buffer)`（Heap Buffer 也实现了 Buffer 接口）
- `BufferWithContext`：简化，在过滤场景下 `context` 字段为 Heap Buffer，过滤完成后由 `recover()` 方法负责回收

### 3.2 SpillingBufferManager：三条路径调度

新增 `SpillingBufferManager` 组件，嵌入 `InputChannelRecoveredStateHandler`，统一管理过滤后 Buffer 的去向。

**核心职责**：

| 职责 | 说明 |
|------|------|
| 非阻塞请求 Network Buffer | 集成阶段实现 |
| 阻塞请求 Network Buffer | 仅 Phase 2 使用，集成阶段实现 |
| 将过滤后数据 spill 到磁盘 | 当前 commit 实现 |
| 从磁盘读取数据到 Network Buffer | 当前 commit 实现 |
| 查询是否有待 replay 的磁盘数据 | 纯查询，无副作用 |
| 创建 Checkpoint 快照迭代器 | 当前 commit 实现 |
| 幂等清理所有临时文件和存活 iterator | 当前 commit 实现 |

**hasDiskData() 纯查询设计**：`hasDiskData()` 必须是纯查询方法，实现为 `return !spillFiles.isEmpty() || currentWriter != null`，不调用 `finalizeCurrentWriter()` 等有副作用的方法。`currentWriter != null` 表示有正在写入但尚未 finalize 的数据，也算作磁盘数据。

**closed 状态检查**：`spillBuffer()`、`replayToBuffer()`、`createCheckpointIterator()`、`hasDiskData()` 方法开头必须检查 `Preconditions.checkState(!closed, "SpillingBufferManager is closed")`，防止 close 后操作创建无法清理的文件或读取已删除的文件。

**Buffer 来源**：`SpillingBufferManager` 提供 `tryRequestBuffer()` 方法，内部调用 `channel.requestBuffer()`（在 unaligned 模式下已经是非阻塞的，当 Pool 不足时返回 null）获取 Network Buffer。

**元数据内存管理**：
- 每个 SpillFile 维护 `Queue<EntryMetadata>`，记录该文件中每条 entry 的元数据（dataLength、DataType、oldSubtaskIndex、oldChannelIndex）
- spillBuffer 时：将 buffer 数据写入磁盘（通过 SpillFileWriter），同时将元数据 enqueue 到当前 SpillFile 的队列
- replayToBuffer 时：从队列 dequeue 元数据获取 length 和 DataType，调用 SpillFileReader.readNextTo(buffer, length) 填充数据，再设置 buffer 的 DataType
- createCheckpointIterator 时：快照当前队列的剩余元数据（从 replay 位置开始），用于 Checkpoint 按 entry 粒度写入远端存储

**Spill 文件管理**：
- Spill 文件使用 FIFO 队列管理，保证数据顺序
- 每个 spill 文件大小上限 64MB，超过后创建新文件
- 临时文件存放在 `IOManager.getSpillingDirectoriesPaths()` 返回的目录下，构造函数接受 `String[]` 多目录参数，使用轮询方式分配文件到不同目录
- 文件命名：`channel-state-spill-{attemptId}-{gateIndex}-{sequence}.tmp`

**close() 幂等性与强制清理**：
- `close()` 内部维护 `closed` 状态标记，重复调用直接返回，不抛异常
- 维护 `Set<CheckpointSpillIterator>` 追踪所有存活的 iterator，`close()` 时强制关闭所有 iterator（释放其持有的文件句柄和缓存 Buffer）
- SpillFile.close() 失败时（catch IOException），仍继续尝试 file.delete()，使用 try-finally 确保文件删除不被跳过
- `InputChannelRecoveredStateHandler.close()` 或异常处理中必须通过 try-finally 调用 `spillingManager.close()`

**旧 attempt 文件不清理**：不在构造函数中扫描清理其他 attempt 的 spill 文件。原因：TM 级共享 spill 目录（`flink-io-<UUID>/`）下多个 Task 可能并发运行 channel state 恢复，按 attemptId 过滤删除会误删其他 Task 的文件。文件清理策略：正常退出由 `close()` 清理自身文件；异常退出由 TM 的 `FileChannelManagerImpl` shutdown hook 兜底清理整个 spill 目录。

**Spill 目录来源统一**：构造函数参数改为 `String[] spillDirs`（来自 `IOManager.getSpillingDirectoriesPaths()`），与 `SpanningWrapper`、`ChannelStateFilteringHandler` 的目录获取链路统一。文件创建时在 `spillDirs` 中轮询选择目录，分散 I/O 负载。

**replayToBuffer 异常安全**：`replayToBuffer()` 调用 `SpillFileReader.readNextTo(buffer, length)` 填充数据。SpillFileReader 内部处理异常回退（失败时自行 seek 回读取前位置），保证异常时不会跳过当前 entry，下次调用可以重试同一条数据，不会静默丢失数据。

**replayToBuffer 返回 null 的契约**：当无磁盘数据时返回 null，调用方传入的 `networkBuffer` 未被消费，调用方必须负责回收。Javadoc 中明确标注此契约。

### 3.3 SpillFileWriter/Reader：文件 I/O 层

**职责边界**：SpillFileWriter/Reader 是纯 I/O 层，只负责字节搬运。不感知任何业务语义（channel context、DataType 等元数据），不分配 Buffer。所有元数据由 SpillingBufferManager 在内存中维护。

#### 3.3.1 Spill 文件格式

磁盘上只存储裸的 buffer 字节数据，按 entry 顺序追加写入，无任何元数据（无 length 前缀、无 channel context、无 DataType 标识）。

**元数据内存维护**：每个 entry 的元数据（dataLength、DataType、oldSubtaskIndex、oldChannelIndex）由 SpillingBufferManager 在内存中的 `Queue<EntryMetadata>` 维护。spill 时 enqueue，replay 时 dequeue，Checkpoint 时按位置切片。

**不存磁盘的理由**：spill 文件是 JVM 内秒级生命周期的临时数据，writer 和 reader 始终运行在同一 JVM 进程中。元数据量极小（每 entry 约 16 字节），与 entry 数据本身（数 KB~32KB）相比可忽略。异常退出时 spill 文件由 TM shutdown hook 兜底清理，无需自描述。

#### 3.3.2 SpillFileWriter

纯字节写入，调用方传入 Buffer，Writer 将 buffer 的可读字节追加写入文件。

```java
class SpillFileWriter implements Closeable {
    SpillFileWriter(File file) throws IOException;
    void writeBuffer(Buffer buffer) throws IOException;
    long getBytesWritten();
    void close() throws IOException;
}
```

**close() 设计**：
- 不调用 `force(true)`（spill 文件是秒级生命周期的临时文件，不需要持久性保证，与 SpanningWrapper 行为一致）
- close() 使用 try-finally 保护 `fileChannel.close()`，即使前置清理操作抛异常也保证文件句柄释放

#### 3.3.3 SpillFileReader

纯字节读取，提供两种读取接口适配不同消费场景：

```java
class SpillFileReader implements Closeable {
    SpillFileReader(File file) throws IOException;

    /** Task 消费路径：从文件读取 length 字节填充到调用方提供的 buffer */
    boolean readNextTo(Buffer target, int length) throws IOException;

    /** Checkpoint 路径：从文件读取 length 字节直接写到输出流，不经过 buffer */
    void readNextTo(OutputStream out, int length) throws IOException;

    boolean hasRemaining() throws IOException;
    void close() throws IOException;
}
```

**Task 消费路径（readNextTo(Buffer, int)）**：外部有了 Network Buffer 后，传入 buffer 和从内存元数据获取的 length，Reader 从文件读取指定字节直接写入 buffer 的 MemorySegment。调用方随后从内存元数据设置 buffer 的 DataType。成功返回 true，EOF 返回 false。

**Checkpoint 路径（readNextTo(OutputStream, int)）**：从文件读取指定字节直接写入 Checkpoint Storage 的 OutputStream，零 buffer 中介。磁盘数据直接流式传输到远端存储，不加载到 Network Buffer。

**错误处理原则**：
- 部分读取（读到了部分字节但未达到预期长度）必须抛出 IOException，不返回 false 或损坏数据。仅在完全读取 0 字节时返回 false（正常 EOF）
- readNextTo 内部处理异常回退（失败时自行 seek 回读取前位置），不暴露 position/seek 给外部

### 3.4 与 ChannelStateFilteringHandler 的集成

**当前流程**（`InputChannelRecoveredStateHandler.recover()`）：
1. `getBuffer()` → 从 Network Buffer Pool 获取 Buffer（或 LazyFileBuffer）
2. `ChannelStateChunkReader.readChunk()` → 从 S3 读数据到 Buffer
3. `recover()` → 调用 `filteringHandler.filterAndRewrite()` 过滤，结果放入 InputChannel

**新流程**：
1. `getBuffer()` → 从 Heap 分配 Source Buffer（不占用 Network Buffer Pool）
2. `ChannelStateChunkReader.readChunk()` → 从 S3 读数据到 Heap Buffer
3. `recover()` → 进入新的路径调度逻辑：
   - 调用 `filteringHandler.filterAndRewrite()` 过滤，产出 `List<Buffer>`
   - 此时 `filterAndRewrite` 内部的 `BufferSupplier` 改为使用 Heap Buffer（因为过滤结果可能需要 spill，不需要直接写入 Network Buffer）
   - 过滤完成后，由 `SpillingBufferManager` 决定路径：

**路径调度流程**：

`recover()` 方法首先调用 `filterAndRewrite()` 对 Source Buffer 进行过滤（使用 Heap BufferSupplier），过滤完成后释放 Source Buffer。然后对每个过滤结果 Buffer，通过 `spillingManager.tryRequestBuffer()` 尝试获取 Network Buffer：

1. **P1（有 Network Buffer，无磁盘数据）**：将 Heap Buffer 数据复制到 Network Buffer，放入 InputChannel
2. **P3（有 Network Buffer，有磁盘数据）**：优先用 Network Buffer replay 磁盘数据放入 InputChannel，当前过滤结果 spill 到磁盘，保证 FIFO 顺序
3. **P2（无 Network Buffer）**：直接 spill 到磁盘

每个过滤结果 Buffer 处理完成后立即释放 Heap 内存。

**关键设计决策**：`filterAndRewrite()` 内部的 `BufferSupplier` 也改为 Heap 内存分配。这样过滤过程完全不依赖 Network Buffer Pool，过滤永远不会阻塞。

### 3.5 两阶段处理流程

**Phase 1: S3 Active Loop**（S3 还有数据时）

每次 `readChunk()` 处理一个 Buffer 时执行上述路径调度。过滤线程持续从 S3 读取并过滤数据，不因 Network Buffer 不足而阻塞。

**Phase 2: Disk Cleanup Loop**（S3 已读完，磁盘仍有数据）

`SequentialChannelStateReaderImpl.readInputData()` 的 `read()` 调用结束后，如果 `SpillingBufferManager.hasDiskData()` 为 true，进入 Phase 2：循环阻塞等待 Network Buffer（通过 `requestBufferBlocking()`），从磁盘读取数据并放入 InputChannel，直到所有 spill 数据消费完毕。

Phase 2 可以阻塞等待 Network Buffer，因为此时 S3 数据已全部过滤完成，不存在死锁条件（Source Buffer 已全部释放）。

### 3.6 Checkpoint 与 Spill 文件的交互

Checkpoint 触发时，spill 文件所处的状态决定了处理方式：

**状态一：数据在磁盘上（未加载）**

Checkpoint 需要将磁盘上的 spill 文件内容纳入快照。设计方案：

- `SpillingBufferManager` 提供 `createCheckpointIterator()` 方法，返回一个迭代器，按 entry 粒度提供 channel context 和数据写入能力
- 每个 entry 的 channel context（oldSubtaskIndex、oldChannelIndex）从内存元数据获取，调用方据此确定数据属于哪个 channel
- 数据直接从磁盘通过 `SpillFileReader.readNextTo(OutputStream, int)` 流式写入 Checkpoint Storage 的 OutputStream，不经过 buffer 中介
- 这一设计避免了 Checkpoint 期间申请 Network Buffer 的需求，消除了内存压力问题

**状态二：数据已加载到 Network Buffer**

数据已在 InputChannel 中，走现有的 Buffer 快照逻辑，不需要特殊处理。

**Spill 文件状态管理**：`SpillingBufferManager` 内部维护 `Queue<SpillFile>`，每个 `SpillFile` 记录文件路径和当前读取偏移量。Replay 完成的文件立即从队列移除并删除。Checkpoint 创建 iterator 时，遍历队列中所有未 replay 的文件。

**引用计数与并发控制**：
- `SpillingBufferManager` 对 spill 文件维护引用计数
- `createCheckpointIterator()` 创建时对当前文件列表和内存元数据做快照，并对快照中的文件增加引用计数。持有引用期间 Replay 不删除文件
- Checkpoint iterator 从当前 replay 位置开始迭代（基于内存元数据的消费进度），确保只处理尚未 replay 的数据，避免与已 replay 到 InputChannel 的数据重复
- `createCheckpointIterator()` 返回 `CloseableIterator`，`close()` 时释放引用计数。Checkpoint 异常时通过 `iterator.close()` 确保引用释放
- `SpillingBufferManager.close()` 强制清理所有残留文件（即使有未关闭的 iterator），保证不泄漏

**Iterator 生命周期追踪**：`SpillingBufferManager` 维护 `Set<CheckpointSpillIterator>` 追踪所有存活的 iterator。`createCheckpointIterator()` 注册 iterator，`iterator.close()` 反注册。`SpillingBufferManager.close()` 遍历并强制关闭所有存活 iterator，确保其持有的文件句柄被释放。

### 3.7 移除 LazyFileBuffer

LazyFileBuffer 存在大量 bug 无法工作，需要彻底清理：

**删除的文件**：
- `flink-runtime/src/main/java/org/apache/flink/runtime/io/network/buffer/LazyFileBuffer.java`

**需要修改的引用点**：
- `RecoveredInputChannel.requestBuffer()`：移除 LazyFileBuffer 创建逻辑，改为非阻塞返回 null（由 SpillingBufferManager 处理）
- `ChannelStateByteBuffer.wrap(LazyFileBuffer)`：移除此工厂方法
- `InputChannelRecoveredStateHandler.getBuffer()`：移除 `instanceof LazyFileBuffer` 分支
- 相关测试文件中的 LazyFileBuffer 引用

### 3.8 非过滤场景的处理

当 `unalignedDuringRecoveryEnabled = false` 或 `rescalingDescriptor == NO_RESCALE` 时：
- 不创建 `ChannelStateFilteringHandler`
- 不创建 `SpillingBufferManager`
- 不分配任何 Heap Buffer
- 完全走现有 `RecoveredInputChannel` 逻辑，从 Network Buffer Pool 阻塞申请 Buffer
- **不做任何改动**

判断点在 `StreamTaskNetworkInputFactory.create()` 和 `SequentialChannelStateReaderImpl.readInputData()` 中，已有条件判断逻辑。

## 4. 配置项设计

| 配置项 | 值 | 是否可配置 | 理由 |
|--------|-----|-----------|------|
| Heap Buffer 数量上限 | 5 个/Gate | 硬编码 | 与 Flink Overdraft Buffer 机制设计一致（每 Gate 5 个），每个 Buffer 32KB，总计 160KB，开销可忽略。暴露为配置项收益低但增加配置复杂度 |
| Spill 文件大小上限 | 64MB | 硬编码 | 平衡文件数量和单次读写粒度。过小导致频繁文件切换，过大导致单文件读写延迟增加 |
| Spill 目录 | `IOManager.getSpillingDirectoriesPaths()` | 复用现有配置 `io.tmp.dirs` | 默认值为系统临时目录，不新增配置项 |
| SpillingBufferManager 启用条件 | `unalignedDuringRecoveryEnabled = true && rescalingDescriptor != NO_RESCALE` | 自动判断 | 满足条件时自动启用，不需要额外配置开关 |

## 5. 开发分阶段计划

### Phase 1：移除 LazyFileBuffer

- 删除 `LazyFileBuffer.java` 及所有引用
- `RecoveredInputChannel.requestBuffer()` 在 `unalignedDuringRecoveryEnabled` 模式下改为非阻塞（返回 null 当 Pool 不足）
- 保留 Heap Buffer 作为临时回退（当前已有的 `allocateUnpooledSegment` 逻辑）
- 确保所有现有测试通过

### Phase 2：实现 Spilling 核心逻辑

- 新增 `SpillingBufferManager`、`SpillFileWriter`、`SpillFileReader`
- 改造 `InputChannelRecoveredStateHandler.getBuffer()` 使用 Heap 内存
- 改造 `InputChannelRecoveredStateHandler.recover()` 集成三条路径调度
- 修改 `filterAndRewrite()` 的 `BufferSupplier` 为 Heap 分配
- 实现 Phase 1/Phase 2 两阶段处理流程

### Phase 3：Checkpoint 集成

- `SpillingBufferManager.createCheckpointIterator()` 实现
- 集成到 Checkpoint 快照流程中
- 处理 Checkpoint 时磁盘数据的正确上传

## 6. 风险与约束

| 风险 | 影响 | 缓解措施 |
|------|------|---------|
| Heap Buffer 数量控制不当导致 OOM | 高 | 每 Gate 最多 5 个 Heap Buffer（160KB），顺序处理 Channel |
| Spill 磁盘空间不足 | 中 | 使用 IOManager 配置的 spill 目录，磁盘写入失败时抛异常终止恢复 |
| Checkpoint 期间 spill 文件被修改 | 高 | Checkpoint iterator 创建时对当前文件列表做快照，iterator 消费的文件在 replay 前不删除 |
| Phase 2 阻塞等待 Buffer 时间过长 | 低 | Phase 2 时 Source Buffer 已全部释放，Network Buffer 可用性取决于 Task 消费速度 |
| Spill 文件泄漏（异常退出或 close 未调用） | 中 | 双重保障：1) close() 幂等设计，try-finally 确保调用；2) TM 的 FileChannelManagerImpl shutdown hook 兜底清理整个 spill 目录 |

实现阶段需要添加关键 metrics（spill 字节数、文件数、Phase 2 等待时长等）用于运维监控。
