# Spill 文件元数据管理策略调研

## 调研背景

为 Flink checkpoint channel state recovery 中 OutputWriter 的 spilling 功能设计提供参考。核心问题：多 channel 数据写入单个 spill 文件时，如何管理每个数据块的元信息（channelInfo, offset, length）。当前设计选择纯内存 `Queue<SpillEntry>` 管理元数据，spill 文件仅存储原始字节流。

---

## Topic 1: Spill 文件元数据管理——纯内存 vs 磁盘索引

### Sources

| 系统 | 参考来源 |
|------|---------|
| Spark ExternalSorter | [ExternalSorter.scala](https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/util/collection/ExternalSorter.scala) |
| Spark ExternalAppendOnlyMap | [ExternalAppendOnlyMap.scala](https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/util/collection/ExternalAppendOnlyMap.scala) |
| Spark ShuffleExternalSorter | [ShuffleExternalSorter.java](https://github.com/apache/spark/blob/master/core/src/main/java/org/apache/spark/shuffle/sort/ShuffleExternalSorter.java) |
| Flink SortMerge Shuffle | [PartitionedFileWriter.java](https://github.com/apache/flink/blob/master/flink-runtime/src/main/java/org/apache/flink/runtime/io/network/partition/PartitionedFileWriter.java), [PartitionedFile.java](https://github.com/apache/flink/blob/master/flink-runtime/src/main/java/org/apache/flink/runtime/io/network/partition/PartitionedFile.java) |
| Apache Kafka | [Kafka Log Internals](https://kafka.apache.org/documentation/#log) |

### Industry Consensus

**主流方案分为两类，取决于数据生命周期和规模：**

**方案 A：纯内存元数据（Spark 系列）**
- Spark ExternalSorter：`SpilledFile` case class 在内存中保存 `elementsPerPartition` 数组和 `serializerBatchSizes` 数组。通过 `scanLeft` 累加计算偏移量，完全不写索引文件。
- Spark ShuffleExternalSorter：`SpillInfo` 对象在内存中维护 `partitionLengths[]` 数组，记录每个 partition 在 spill 文件中的长度。整个 `LinkedList<SpillInfo>` 保持在内存中。
- Spark ExternalAppendOnlyMap：`batchSizes` ArrayBuffer 在内存中记录每个 batch 的字节大小，通过累加计算偏移。
- **适用场景**：spill 文件的生命周期短（单次 shuffle 内）、元数据量可控（partition 数有限）、进程崩溃后重算而非恢复。

**方案 B：独立索引文件（Flink SortMerge、Kafka）**
- Flink PartitionedFileWriter：数据文件 + 独立索引文件。索引条目为固定 16 字节（8 字节 offset + 8 字节 length），按 `(region × numSubpartitions + subpartition) × INDEX_ENTRY_SIZE` 计算位置。写入时先缓存在内存 ByteBuffer 中（最多 4MB），再批量刷盘。
- Kafka：`.log` 数据文件 + `.index` 稀疏索引文件 + `.timeindex` 时间索引文件。索引文件持久化到磁盘，崩溃后可用于恢复。
- **适用场景**：数据需要跨进程/跨重启访问、元数据量大（subpartition 数 × region 数）、需要故障恢复能力。

### Common Pitfalls

1. **内存元数据丢失**：进程崩溃后纯内存元数据不可恢复。Spark 的解决方式是重算；如果重算代价高或不可行，必须持久化索引。
2. **元数据膨胀**：Flink PartitionedFileWriter 限制索引缓存最大 4MB，超出后刷盘。不设上限可能导致 OOM。
3. **空分区处理**：Flink 的 `updateEmptySubpartitionOffsets()` 专门处理空 subpartition 的偏移计算，避免空分区导致索引断裂。如果忽略空 channel 场景，offset 计算会错乱。
4. **并发安全**：Spark 的 `spills` 集合在 spill 和 merge 阶段可能并发访问。Flink 使用 `@GuardedBy("lock")` 保护。

### Recommendation

**当前设计（纯内存 `Queue<SpillEntry>`）合理，但需明确前提条件：**

- 本场景中 spill 文件用于 checkpoint 期间临时缓存 channel 数据，生命周期绑定到 checkpoint 操作。若 checkpoint 失败，整个 checkpoint 会被废弃，不需要从 spill 文件恢复元数据——与 Spark 的 "崩溃则重算" 模式一致。
- 元数据量可控：每个 `SpillEntry` 仅包含 channelInfo + offset + length，单个 channel 的条目数与 buffer 数量成正比，不会无限膨胀。
- 建议增加元数据量的防御性上限检查（类似 Flink PartitionedFileWriter 的 4MB 限制），避免极端场景下 OOM。

---

## Topic 2: 纯字节流 vs 结构化 Spill 文件格式

### Sources

| 系统 | 格式 | 参考来源 |
|------|------|---------|
| Spark ExternalSorter | 纯字节流（batch 边界仅内存追踪） | [ExternalSorter.scala](https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/util/collection/ExternalSorter.scala) |
| RocksDB WAL | 结构化（block + record header） | [RocksDB WAL Format](https://github.com/facebook/rocksdb/wiki/Write-Ahead-Log-File-Format) |
| LevelDB WAL | 结构化（32KB block + 7 字节 record header） | [LevelDB Log Format](https://github.com/google/leveldb/blob/main/doc/log_format.md) |
| Flink PartitionedFile | 数据文件为纯字节流 + 独立索引文件 | [PartitionedFile.java](https://github.com/apache/flink/blob/master/flink-runtime/src/main/java/org/apache/flink/runtime/io/network/partition/PartitionedFile.java) |

### Industry Consensus

**纯字节流（Pure Byte Stream）**

代表系统：Spark ExternalSorter/ExternalAppendOnlyMap、Flink PartitionedFile（数据文件部分）。

特征：
- 文件中仅包含用户数据的序列化字节，无 header / footer / checksum / 分隔符。
- 所有定位信息（offset、length、partition 归属）在外部维护（内存或独立索引文件）。
- 写入路径最简：直接 `write(bytes)` 即可，无需封装 / 对齐 / 填充。
- 读取路径依赖外部元数据：`seek(offset) + read(length)` 即可精确读取。

优势：
- 零开销：无 header / padding 浪费，磁盘利用率 100%。
- 写入吞吐最大化：连续写入无中断。
- 实现简单：写入和读取逻辑极简。

劣势：
- 文件自身不可自描述，脱离元数据后无法解析。
- 不支持文件级损坏检测和恢复。

**结构化格式（Structured Format with Headers）**

代表系统：RocksDB WAL、LevelDB WAL。

特征：
- 固定大小 block（32KB）作为对齐单元。
- 每条 record 附带 header：CRC32 校验 (4B) + 长度 (2B) + 类型 (1B) = 7 字节 header。
- 大记录通过 FIRST / MIDDLE / LAST 类型跨 block 分片。
- block 尾部不足 7 字节时用零填充。

优势：
- 自描述：文件可独立解析，无需外部元数据。
- 损坏恢复："just go to next block boundary and scan"（LevelDB 原文），跳过损坏 block 继续读取。
- 数据完整性：CRC32 校验每条 record。

劣势：
- 空间开销：每条 record 7 字节 header + block 尾部 padding。
- 写入复杂度：需要处理 record 分片、block 边界对齐、CRC 计算。
- 吞吐受限：额外计算和对齐操作影响写入速度。

### Common Pitfalls

1. **过度设计**：对短生命周期的临时 spill 文件引入 CRC / header / block 对齐是过度设计。Spark 处理 PB 级 shuffle 数据均使用纯字节流，验证了该方案在大规模场景下的可行性。
2. **恢复需求误判**：WAL 需要结构化格式是因为必须在崩溃后从文件恢复数据。如果 spill 文件在进程崩溃后会被丢弃（如 Spark shuffle），结构化格式的恢复能力毫无用处。
3. **混合方案**：Flink PartitionedFile 的方案（纯字节流数据 + 独立索引文件）是二者之间的折中——数据文件保持纯字节流以最大化吞吐，索引文件提供定位能力。

### Recommendation

**纯字节流是正确选择。** 理由：

- 当前场景中 spill 文件生命周期短、不需要崩溃恢复、不需要文件自描述能力。
- 写入路径的性能是关键（checkpoint 期间不能阻塞数据处理过长时间），纯字节流的写入吞吐最优。
- 这与 Spark ExternalSorter 和 Flink PartitionedFile 数据文件的设计选择完全一致。
- 不建议引入 header / CRC / block 对齐等复杂度，这些在本场景中收益为零。

---

## Topic 3: WAL/Journal 文件的元数据追踪模式

### Sources

| 系统 | 参考来源 |
|------|---------|
| PostgreSQL WAL | [PostgreSQL WAL Internals](https://www.postgresql.org/docs/current/wal-internals.html) |
| RocksDB WAL | [RocksDB WAL File Format](https://github.com/facebook/rocksdb/wiki/Write-Ahead-Log-File-Format) |
| LevelDB WAL | [LevelDB Log Format](https://github.com/google/leveldb/blob/main/doc/log_format.md) |
| Apache Kafka Log | [Kafka Log Design](https://kafka.apache.org/documentation/#log) |

### Industry Consensus

WAL/Journal 系统的元数据追踪模式可归纳为三种：

**模式 1：嵌入式元数据（Self-Contained Records）**

代表：RocksDB WAL、LevelDB WAL。

- 每条 record 自带完整元数据 header（CRC + length + type）。
- 文件可独立扫描恢复，不依赖外部索引。
- 适用场景：崩溃恢复是核心需求，必须能从任意 block 边界开始恢复读取。

**模式 2：位置标识符（Position Identifier / LSN）**

代表：PostgreSQL WAL。

- 使用 Log Sequence Number (LSN) 作为单调递增的字节偏移量标识每条 record 的位置。
- LSN 本质上就是文件中的字节 offset，但被抽象为逻辑标识符在系统各组件间传递。
- checkpoint 记录中保存 LSN 位置，恢复时从该 LSN 开始 redo。
- 适用场景：需要跨组件引用 WAL 中特定位置（如 replication、recovery point）。

**模式 3：独立索引文件（Separate Index）**

代表：Apache Kafka。

- 数据文件 (`.log`) + 稀疏偏移索引 (`.index`) + 时间索引 (`.timeindex`)。
- 索引文件可损坏后重建（从数据文件扫描重建）。
- 适用场景：需要高效随机访问 + 数据文件量大 + 索引可重建。

### Common Pitfalls

1. **LSN/Offset 溢出**：PostgreSQL WAL 使用 64 位 LSN，实际上不会溢出。但如果使用 32 位 offset 追踪 spill 文件位置，单文件超过 2GB 时会溢出。应始终使用 `long` (64 位) 存储 offset。
2. **索引与数据不一致**：独立索引文件模式下，如果写入数据后崩溃、索引未更新，会导致数据丢失（索引不知道新数据的存在）。Kafka 的解决方案是恢复时重建索引。
3. **扫描恢复的性能代价**：嵌入式元数据模式在恢复时需要线性扫描整个文件，文件越大恢复越慢。RocksDB 的 32KB block 对齐机制缓解了此问题（可以跳过损坏 block）。

### Recommendation

**本设计的 `SpillEntry(channelInfo, offset, length)` 本质上是模式 2（Position Identifier）的简化版。** 建议：

- `offset` 使用 `long` 类型，确保单个 spill 文件可超过 2GB。
- 将 `SpillEntry` 视为类似 LSN 的位置标识符在系统内传递：OutputWriter 写入时生成 SpillEntry，后续读取时通过 SpillEntry 中的 offset + length 定位数据。
- 不需要引入嵌入式元数据模式（模式 1），因为不需要崩溃恢复能力。
- 不需要引入独立索引文件（模式 3），因为元数据量小且生命周期短，内存完全能承载。

---

## 总结：对当前设计的综合建议

| 设计决策 | 当前选择 | 业界对标 | 结论 |
|---------|---------|---------|------|
| 元数据存储位置 | 纯内存 `Queue<SpillEntry>` | Spark ExternalSorter（纯内存） | 合理。生命周期短、崩溃后不需恢复 |
| Spill 文件格式 | 纯字节流 | Spark spill files、Flink PartitionedFile 数据文件 | 合理。零开销、最大写入吞吐 |
| 元数据结构 | `SpillEntry(channelInfo, offset, length)` | Spark `SpillInfo.partitionLengths[]`、PostgreSQL LSN | 合理。类似 LSN 的位置标识模式 |
| 独立索引文件 | 不使用 | Flink PartitionedFile、Kafka `.index` | 合理。元数据量小，内存足够 |
| 文件内嵌元数据 | 不使用 | RocksDB/LevelDB WAL headers | 合理。不需要崩溃恢复和自描述能力 |

**核心结论**：当前"纯内存元数据 + 纯字节流数据文件"的设计与 Spark ExternalSorter 的架构完全一致，是短生命周期 spill 场景的业界标准做法。无需引入独立索引文件或文件内嵌元数据等额外复杂度。

**补充建议**：
1. 确保 `offset` 字段为 `long` 类型（避免 2GB 限制）。
2. 考虑对 `Queue<SpillEntry>` 的大小设置防御性上限或监控，类似 Flink PartitionedFileWriter 的 4MB 索引缓存限制。
3. SpillEntry 应为不可变对象（immutable），避免在并发场景中出现状态不一致。
