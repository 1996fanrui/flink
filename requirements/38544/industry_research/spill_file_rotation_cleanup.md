# Spill 文件轮转与清理策略调研

## 1. Spill 文件大小阈值选择

### Topic

流处理/批处理系统中 spill 文件的大小阈值设计，以及 64MB 作为 OutputWriter per-gate spill 文件轮转阈值是否合理。

### Sources

| 系统 | 参数 | 默认值 | 来源 |
|------|------|--------|------|
| Flink File Merging Checkpoint | `FILE_MERGING_MAX_FILE_SIZE` | **32MB** | `CheckpointingOptions.java` |
| Flink SpillingAdaptiveSpanningRecordDeserializer | `DEFAULT_THRESHOLD_FOR_SPILLING` | **5MB**（触发 spill 的阈值，非文件大小上限） | `SpillingAdaptiveSpanningRecordDeserializer.java` |
| Flink Sort-Merge Shuffle | write buffer | **8MB**（单次写入 buffer 大小） | `SortMergeResultPartition.java` |
| Flink Batch Shuffle Framework Off-Heap | framework off-heap batch-shuffle size | **64MB** | `taskmanager.memory.framework.off-heap.batch-shuffle.size` |
| Flink Network Memory Min | network memory min | **64MB** | `taskmanager.memory.network.min` |
| RocksDB | `write_buffer_size` (memtable) | **64MB**（典型生产配置） | [RocksDB Tuning Guide](https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide) |
| RocksDB | `target_file_size_base` | `max_bytes_for_level_base / 10`（约 **64MB**） | [RocksDB Tuning Guide](https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide) |
| Spark | `spark.shuffle.spill.diskWriteBufferSize` | **1MB**（写磁盘的 buffer 大小，非文件上限） | [Spark Configuration](https://spark.apache.org/docs/latest/configuration.html) |
| Spark | `spark.shuffle.file.buffer` | **32KB**（shuffle output stream buffer） | [Spark Configuration](https://spark.apache.org/docs/latest/configuration.html) |

### Industry Consensus

- **32MB - 64MB 是主流区间**：Flink 的 file merging checkpoint 使用 32MB，RocksDB 的 memtable flush 和 SST target 文件大小在 64MB 左右，Flink batch shuffle 分配 64MB off-heap 内存。
- **没有"标准答案"**：Spark 不设置单个 spill 文件的大小上限，而是按内存压力触发 spill，每次 spill 产生一个文件，文件大小取决于当时内存中积累的数据量。
- **文件大小阈值本质上是 IO 效率与资源回收粒度的 trade-off**：
  - 文件太小（< 16MB）：频繁创建/关闭文件，文件系统元数据开销大，随机 IO 增加。
  - 文件太大（> 256MB）：单个文件生命周期过长，回收不及时导致磁盘占用攀升；异常退出时丢失更多未持久化数据。

### Common Pitfalls

1. **混淆 write buffer size 和 file size limit**：write buffer 是内存中的缓冲大小（如 Spark 的 1MB），file size limit 是磁盘文件的轮转阈值，两者独立。
2. **忽略压缩对实际文件大小的影响**：如果启用 spill 压缩（如 Flink `TABLE_EXEC_SPILL_COMPRESSION_ENABLED`），64MB 的逻辑数据可能只占用 20-30MB 磁盘空间，轮转条件应基于压缩前还是压缩后的大小需明确。
3. **文件大小阈值不可配置**：硬编码阈值在某些场景下不适用（高吞吐需要更大文件，资源受限环境需要更小文件）。

### Recommendation

**64MB 合理，建议保留为默认值并提供可配置能力**。

- 与 Flink 生态一致（batch shuffle 64MB、file merging 32MB，同一量级）。
- 与 RocksDB 的 memtable/SST 文件大小一致（64MB）。
- 建议通过 `ConfigOption<MemorySize>` 暴露为可配置参数，默认 64MB，允许用户根据磁盘性能和数据规模调整。
- 阈值检查应基于**未压缩的写入字节数**（即写入文件的实际字节数），而非逻辑数据大小。

---

## 2. 多磁盘目录轮转策略

### Topic

当配置多个 spill 目录（分布在不同磁盘上）时，如何在目录间分配新的 spill 文件。

### Sources

| 系统 | 策略 | 来源 |
|------|------|------|
| Flink `FileChannelManagerImpl` | **Round-robin**：`nextPath.getAndIncrement() % paths.length` | [FileChannelManagerImpl.java](https://github.com/apache/flink/blob/master/flink-runtime/src/main/java/org/apache/flink/runtime/io/disk/FileChannelManagerImpl.java) |
| Flink `IOManagerAsync` | **Round-robin**：为每个目录创建独立的 reader/writer 线程 | [IOManagerAsync.java](https://github.com/apache/flink/blob/master/flink-runtime/src/main/java/org/apache/flink/runtime/io/disk/iomanager/IOManagerAsync.java) |
| Spark `DiskBlockManager` | **Hash-based**：`dirId = hash % localDirs.length`，按 block ID 的 hash 分配 | [DiskBlockManager.scala](https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/storage/DiskBlockManager.scala) |
| Trino `FileSingleStreamSpillerFactory` | **Round-robin + 健康检查**：`roundRobinIndex = (roundRobinIndex + i + 1) % spillPathsCount`，跳过不健康的路径 | [FileSingleStreamSpillerFactory.java](https://github.com/trinodb/trino/blob/master/core/trino-main/src/main/java/io/trino/spiller/FileSingleStreamSpillerFactory.java) |
| Trino `FileSingleStreamSpiller` | **Round-robin（页级别）**：`fileIndex = (fileIndex + 1) % fileCount`，每个 page 写到不同文件 | [FileSingleStreamSpiller.java](https://github.com/trinodb/trino/blob/master/core/trino-main/src/main/java/io/trino/spiller/FileSingleStreamSpiller.java) |

### Industry Consensus

- **Round-robin 是绝对主流**：Flink 和 Trino 都使用 round-robin。实现简单、负载均衡效果好（前提是各磁盘性能一致）。
- **Hash-based 适合需要确定性定位的场景**：Spark 使用 hash 因为需要根据 block ID 快速定位文件所在目录，无需额外索引。
- **容量感知（capacity-aware）在生产系统中很少见**：主流系统都没有实现基于剩余容量的动态分配。Trino 的健康检查是最接近的方案，但仅检查目录是否可用（磁盘空间是否低于阈值），不做精确的容量权重分配。

### Common Pitfalls

1. **不检查目录健康状态**：磁盘故障或满盘时，round-robin 会持续向故障磁盘写入并失败。Trino 的做法值得借鉴：缓存路径健康状态（5 分钟有效期），跳过不健康路径。
2. **异构磁盘性能差异**：如果 SSD 和 HDD 混用，round-robin 会在慢磁盘上产生瓶颈。但这个场景在实践中较少出现（运维通常保证磁盘同构）。
3. **目录创建时机不当**：目录应在初始化时创建并验证权限，而非首次写入时才创建。Flink 在 `FileChannelManagerImpl` 构造函数中完成这一操作。

### Recommendation

**采用 Round-robin 策略，辅以目录可用性检查**。

- 轮转粒度：**per file**（每个新的 spill 文件分配到下一个目录），而非 per entry。这与 Flink `FileChannelManagerImpl` 一致。
- 增加目录可用性检查：在选择目录时跳过不可写或磁盘空间不足的目录。不需要精确的容量权重，简单的可用性判断即可。
- 使用 `AtomicLong` 的计数器实现 round-robin，保证线程安全且无锁。

---

## 3. Spill 文件清理策略

### Topic

spill 文件在正常退出和异常退出时的清理机制，以及 JVM shutdown hook 的使用方式。

### Sources

| 系统 | 正常清理 | 异常清理 | 来源 |
|------|----------|----------|------|
| Flink `FileChannelManagerImpl` | `close()` 方法递归删除目录，使用 `AtomicBoolean` 防止重复清理 | JVM shutdown hook（通过 `ShutdownHookUtil.addShutdownHook`） | [FileChannelManagerImpl.java](https://github.com/apache/flink/blob/master/flink-runtime/src/main/java/org/apache/flink/runtime/io/disk/FileChannelManagerImpl.java) |
| Flink `IOManagerAsync` | `close()` 方法先终止 IO 线程，再调用父类清理目录 | JVM shutdown hook（通过 `ShutdownHookUtil.addShutdownHook`） | [IOManagerAsync.java](https://github.com/apache/flink/blob/master/flink-runtime/src/main/java/org/apache/flink/runtime/io/disk/iomanager/IOManagerAsync.java) |
| Spark `DiskBlockManager` | `doStop()` 递归删除 localDirs（当 `deleteFilesOnStop=true`） | JVM shutdown hook（优先级 `TEMP_DIR_SHUTDOWN_PRIORITY + 1`） | [DiskBlockManager.scala](https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/storage/DiskBlockManager.scala) |
| Spark `ExternalSorter` | `stop()` 方法遍历 `spilledFiles` 逐个删除 | 无独立机制，依赖 `DiskBlockManager` 的 shutdown hook | [ExternalSorter.scala](https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/util/collection/ExternalSorter.scala) |
| Trino `FileSingleStreamSpillerFactory` | `@PreDestroy` 注解的 `destroy()` 方法关闭线程池 | `@PostConstruct cleanupOldSpillFiles()`：启动时清理上次遗留的 spill 文件（匹配 `spill*.bin` pattern） | [FileSingleStreamSpillerFactory.java](https://github.com/trinodb/trino/blob/master/core/trino-main/src/main/java/io/trino/spiller/FileSingleStreamSpillerFactory.java) |
| Trino `FileSingleStreamSpiller` | `Closer` 资源管理器自动清理文件和内存 | 通过 `Closer` 注册的资源保证 close 时清理 | [FileSingleStreamSpiller.java](https://github.com/trinodb/trino/blob/master/core/trino-main/src/main/java/io/trino/spiller/FileSingleStreamSpiller.java) |

### Industry Consensus

**三层清理防线是业界共识**：

1. **第一层：业务级清理**（entries replay 完成后删除文件）—— 正常运行时的主要清理路径。
2. **第二层：组件级清理**（`close()` / `stop()` 方法）—— Task 正常结束时清理所有 spill 文件。
3. **第三层：进程级清理**（JVM shutdown hook 或启动时清理）—— 防止异常退出后的文件泄漏。

**Trino 的"启动时清理"模式**尤其值得注意：不依赖 shutdown hook 在退出时清理（shutdown hook 不保证一定执行，如 `kill -9`），而是在下次启动时扫描并清理遗留文件。

### Common Pitfalls

1. **JVM shutdown hook 不保证执行**：`kill -9`、OOM Killer、机器断电等场景下 shutdown hook 不会执行。不能仅依赖 shutdown hook 作为唯一清理机制。
2. **shutdown hook 中的死锁**：如果 shutdown hook 尝试获取已被其他线程持有的锁，会导致 JVM 挂起。Flink 使用 `AtomicBoolean` 的 CAS 操作避免这个问题。
3. **清理顺序不当**：必须先停止 IO 线程，再删除文件。否则 IO 线程可能在文件被删除后尝试写入，导致异常。Flink `IOManagerAsync` 先终止线程再调用父类清理。
4. **忽略清理失败**：文件删除可能因为被其他进程占用而失败。应记录日志但不抛异常，避免影响其他清理操作。
5. **Spill 目录与系统临时目录混用**：Trino 文档明确警告不要使用系统盘做 spill 目录，尤其不要用 JVM 运行所在的磁盘。

### Recommendation

**采用三层清理防线 + 启动时清理**：

1. **业务级清理**：单个 spill 文件中所有 entries 被 replay 完成后，立即删除该文件。这是最主要的清理路径。
2. **组件级清理**：OutputWriter `close()` 时删除所有 spill 文件。使用 `AtomicBoolean` 防止重复清理。
3. **JVM shutdown hook**：通过 `ShutdownHookUtil.addShutdownHook` 注册，保证正常关闭时的清理。在 `close()` 被正常调用后应移除 hook（避免重复执行）。
4. **启动时清理**：在 spill 目录中使用带有唯一前缀的文件名模式（如 `channel-spill-*.bin`），TaskManager 启动时扫描并清理上次遗留的 spill 文件。这是对 `kill -9` 等场景的终极保障。

---

## 4. 文件轮转时机

### Topic

比较按文件大小、按时间、按 entry 数量三种轮转策略的优劣。

### Sources

| 策略 | 使用系统 | 来源 |
|------|----------|------|
| 按大小轮转 | Flink File Merging（32MB）、RocksDB（64MB target_file_size_base） | `CheckpointingOptions.java`、[RocksDB Tuning Guide](https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide) |
| 按内存压力触发 | Spark ExternalSorter（内存占用超阈值时 spill，不限文件大小） | [ExternalSorter.scala](https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/util/collection/ExternalSorter.scala) |
| 按元素数量触发 | Spark ShuffleExternalSorter（`numElementsForSpillThreshold`） | [ShuffleExternalSorter.java](https://github.com/apache/spark/blob/master/core/src/main/java/org/apache/spark/shuffle/sort/ShuffleExternalSorter.java) |
| 按磁盘空间占比 | Trino（`maxUsedSpaceThreshold` 百分比） | [FileSingleStreamSpillerFactory.java](https://github.com/trinodb/trino/blob/master/core/trino-main/src/main/java/io/trino/spiller/FileSingleStreamSpillerFactory.java) |
| 按批次大小 | Spark ExternalSorter（`serializerBatchSize` 控制序列化批次边界） | [ExternalSorter.scala](https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/util/collection/ExternalSorter.scala) |

### Industry Consensus

- **按大小轮转是最主流的策略**：Flink file merging 和 RocksDB 都采用按文件大小轮转。原因是文件大小直接关联 IO 性能和磁盘占用，是最可预测的指标。
- **按时间轮转在 spill 场景中几乎不使用**：时间轮转适合日志系统（如 logback、log4j），不适合 spill 文件。因为 spill 的写入速率波动很大，按时间轮转可能产生大小差异极大的文件。
- **按 entry 数量轮转有局限性**：entry 大小不均匀时，相同数量的 entries 可能产生差异很大的文件。Spark 使用 `numElementsForSpillThreshold` 主要是为了控制内存中 pointer array 的大小，而非控制文件大小。

### Common Pitfalls

1. **仅按大小轮转忽略了"长尾文件"问题**：如果写入速率很低，最后一个文件可能长时间处于打开状态但远未达到阈值。这不一定是问题（文件会在 entries replay 后删除），但需要确保 close 时正确处理未满的文件。
2. **阈值检查的时机**：应在每次 entry 写入后检查文件大小，而非在写入前检查。否则单个超大 entry 可能导致文件远超阈值。
3. **忽略 file header/metadata 的大小**：文件大小阈值应包含 header 和元数据，否则实际文件大小会略超阈值。

### Recommendation

**采用按文件大小轮转，64MB 阈值**。

- 轮转时机：每次 `writeEntry()` 完成后检查当前文件大小。如果超过 64MB，关闭当前文件并在下一个 spill 目录创建新文件。
- 不引入按时间或按 entry 数量的轮转。按大小是最简单且最符合 IO 特性的策略。
- 特殊处理：如果单个 entry 的序列化大小超过 64MB（极端情况），允许当前文件超过阈值，在该 entry 写入完成后再轮转。不要将单个 entry 拆分到两个文件中。

---

## 总结矩阵

| 维度 | 建议 | 核心依据 |
|------|------|----------|
| 文件大小阈值 | **64MB**（可配置） | 与 Flink file merging（32MB）和 RocksDB（64MB）同一量级 |
| 目录轮转策略 | **Round-robin + 可用性检查** | Flink、Trino 的一致做法 |
| 清理策略 | **三层防线 + 启动时清理** | 覆盖正常退出、异常退出、kill -9 三种场景 |
| 轮转触发条件 | **按文件大小** | 业界主流做法，IO 性能可预测 |
