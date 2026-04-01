# 验收方案 — Task 5: 两阶段 Buffer 模型与 Spilling 逻辑

## 状态表

| 编号 | 测试内容概要 | 需求ID列表 | 状态 | 测试执行方 | 备注 |
|------|------------|-----------|------|-----------|------|
| AT-2W3J | P1 Memory Path 正常工作 | REQ-8HRS | 待测试 | 代码自动化 | |
| AT-GE7G | P2 Spill Path 正常工作 | REQ-8HRS,REQ-0EG7 | 待测试 | 代码自动化 | |
| AT-SX5O | P3 Replay Path 正常工作 | REQ-8HRS,REQ-0EG7 | 待测试 | 代码自动化 | |
| AT-QUBL | P3 优先于 P1 保证数据顺序 | REQ-8HRS | 待测试 | 代码自动化 | |
| AT-36DP | Source Buffer 使用 Heap 内存不竞争 Pool | REQ-NHLB | 待测试 | 代码自动化 | |
| AT-UE7O | Gate 内 Virtual Channel 顺序处理 | REQ-QY68 | 待测试 | 代码自动化 | |
| AT-LOBV | Checkpoint 快照磁盘 spill 数据含 channel context | REQ-2N5H,REQ-D9PQ | 待测试 | 代码自动化 | |
| AT-EPHC | Checkpoint 快照已加载数据走现有逻辑 | REQ-2N5H | 待测试 | 代码自动化 | |
| AT-U7Q2 | 非过滤场景不受影响 | REQ-NPBY | 待测试 | 代码自动化 | |
| AT-IYKG | LazyFileBuffer 已移除 | REQ-J6QM | 待测试 | Agent 执行 | |
| AT-OD4D | Spill 文件状态可区分 | REQ-U7R8 | 待测试 | 代码自动化 | |
| AT-41PK | Heap Buffer 数量上限控制 | REQ-QY68 | 待测试 | 代码自动化 | |
| AT-II0K | Phase 2 Disk Cleanup Loop 正常工作 | REQ-8HRS,REQ-0EG7 | 待测试 | 代码自动化 | |
| AT-43JR | Checkpoint iterator 引用计数及对照组 | REQ-2N5H,REQ-U7R8 | 待测试 | 代码自动化 | |
| AT-O5AX | close() 幂等性、强制清理、iterator 追踪 | REQ-U7R8,REQ-JD2C | 待测试 | 代码自动化 | |
| AT-5097 | Spill 文件 64MB 分片 | REQ-8HRS | 待测试 | 代码自动化 | |
| AT-JXIR | 旧 attempt 残留文件不清理 | REQ-G4RP | 待测试 | 代码自动化 | |
| AT-UFNZ | UnalignedCheckpointRescaleITCase 集成测试 | REQ-NHLB,REQ-8HRS,REQ-NPBY | 待测试 | 代码自动化 | |
| AT-RV6E | SpillFileReader 直写 target Buffer 无双重拷贝 | REQ-PV3D,REQ-F2HQ | 待测试 | 代码自动化 | |
| AT-YN8B | DataType 完整序列化与反序列化 | REQ-WB9F | 待测试 | 代码自动化 | |
| AT-C3MK | Spill 目录接受 String[] 多目录参数 | REQ-M8KE | 待测试 | 代码自动化 | |
| AT-HW4P | 文件截断/损坏时 reader 抛 IOException | REQ-T5AJ | 待测试 | 代码自动化 | |
| AT-P7JG | spillBuffer 在 closed 后调用抛异常 | REQ-JD2C | 待测试 | 代码自动化 | |
| AT-KD5T | dataLength 负数或超大值时抛 IOException | REQ-T5AJ | 待测试 | 代码自动化 | |
| AT-VF2X | replayToBuffer 异常不跳过 entry | REQ-T5AJ | 待测试 | 代码自动化 | |
| AT-BW9L | hasDiskData 纯查询无副作用 | REQ-K7NW | 待测试 | 代码自动化 | |
| AT-QE6N | ByteBuffer 复用验证 | REQ-F2HQ | 待测试 | 代码自动化 | |
| AT-MJ3S | copyBufferData 容量校验和 readerIndex 重置 | REQ-F2HQ | 待测试 | 代码自动化 | |
| AT-XK8R | Checkpoint iterator 从 replay 位置开始读 | REQ-D9PQ | 待测试 | 代码自动化 | |
| AT-LN5V | 大数据量多次轮转后全部 replay | REQ-AX4C | 待测试 | 代码自动化 | |

---

## 验收步骤

### [L1-测试] AT-2W3J P1 Memory Path 正常工作

**测试目标**：当 Network Buffer Pool 有空闲 Buffer 且磁盘无数据时，过滤结果直接写入 Network Buffer 进入 InputChannel。

**命令**：
```bash
./mvnw test -pl flink-runtime -Dtest=SpillingBufferManagerTest#testMemoryPath -P java11-target -P java11
```

**预期结果**：不产生 spill 文件，测试通过 exit code = 0

---

### [L1-测试] AT-GE7G P2 Spill Path 正常工作

**测试目标**：当 Network Buffer Pool 无空闲 Buffer 时，过滤结果 spill 到本地磁盘文件。

**命令**：
```bash
./mvnw test -pl flink-runtime -Dtest=SpillingBufferManagerTest#testSpillAndHasDiskData -P java11-target -P java11
```

**预期结果**：spillBuffer 写入 spill 文件，hasDiskData() 返回 true，测试通过 exit code = 0

---

### [L1-测试] AT-SX5O P3 Replay Path 正常工作

**测试目标**：spill 数据通过 replayToBuffer 加载到 Network Buffer，数据一致且文件被删除。

**命令**：
```bash
./mvnw test -pl flink-runtime -Dtest=SpillingBufferManagerTest#testSpillAndReplay -P java11-target -P java11
```

**预期结果**：replay 数据与原始数据一致，replay 完成后 hasDiskData() 为 false，测试通过 exit code = 0

---

### [L1-测试] AT-QUBL P3 优先于 P1 保证数据顺序

**测试目标**：磁盘有数据时新数据 spill 到磁盘，replay 按 FIFO 顺序。

**命令**：
```bash
./mvnw test -pl flink-runtime -Dtest=SpillingBufferManagerTest#testReplayPriorityOverMemoryPath -P java11-target -P java11
```

**预期结果**：先 spill 的数据先被 replay，测试通过 exit code = 0

---

### [L1-测试] AT-36DP Source Buffer 使用 Heap 内存不竞争 Pool

**测试目标**：getBuffer() 返回 Heap Buffer，Network Buffer Pool 数量不减少。

**命令**：
```bash
./mvnw test -pl flink-runtime -Dtest=InputChannelRecoveredStateHandlerTest#testSourceBufferUsesHeapMemory -P java11-target -P java11
```

**预期结果**：测试通过 exit code = 0

---

### [L1-测试] AT-UE7O Gate 内 Virtual Channel 顺序处理

**测试目标**：同一时刻只有一个 Channel 的数据在被处理。

**命令**：
```bash
./mvnw test -pl flink-runtime -Dtest=SequentialChannelStateReaderImplTest#testSequentialChannelProcessing -P java11-target -P java11
```

**预期结果**：测试通过 exit code = 0

---

### [L1-测试] AT-LOBV Checkpoint 快照磁盘 spill 数据含 channel context

**测试目标**：createCheckpointIterator() 返回的 SpillEntry 包含正确的 buffer 数据和 channel context（oldSubtaskIndex、oldChannelIndex）。

**命令**：
```bash
./mvnw test -pl flink-runtime -Dtest=SpillingBufferManagerTest#testCheckpointWithDiskData -P java11-target -P java11
```

**预期结果**：iterator 产出的 SpillEntry 包含正确的 buffer 内容和 channel context，测试通过 exit code = 0

---

### [L1-测试] AT-EPHC Checkpoint 快照已加载数据走现有逻辑

**测试目标**：数据全部 replay 后 createCheckpointIterator() 返回空 iterator。

**命令**：
```bash
./mvnw test -pl flink-runtime -Dtest=SpillingBufferManagerTest#testCheckpointAfterFullReplay -P java11-target -P java11
```

**预期结果**：hasDiskData() 为 false，iterator 为空，测试通过 exit code = 0

---

### [L1-测试] AT-U7Q2 非过滤场景不受影响

**测试目标**：非过滤场景走原有路径，不创建 SpillingBufferManager。

**命令**：
```bash
./mvnw test -pl flink-runtime -Dtest=SequentialChannelStateReaderImplTest#testNoFilteringScenario -P java11-target -P java11
```

**预期结果**：测试通过 exit code = 0

---

### [L2-Agent] AT-IYKG LazyFileBuffer 已移除

**测试目标**：LazyFileBuffer.java 已删除，代码无残留引用。

**采集命令**：
```bash
ls flink-runtime/src/main/java/org/apache/flink/runtime/io/network/buffer/LazyFileBuffer.java 2>&1
grep -r "LazyFileBuffer" --include="*.java" flink-runtime/src/ 2>&1
```

**判定命令**：
```bash
test ! -f flink-runtime/src/main/java/org/apache/flink/runtime/io/network/buffer/LazyFileBuffer.java && \
  ! grep -rq "LazyFileBuffer" --include="*.java" flink-runtime/src/ && \
  echo "PASS" || echo "FAIL"
```

**客观证据**：判定命令输出 "PASS"

---

### [L1-测试] AT-OD4D Spill 文件状态可区分

**测试目标**：SpillingBufferManager 能区分"在磁盘上"和"已加载"两种状态。

**命令**：
```bash
./mvnw test -pl flink-runtime -Dtest=SpillingBufferManagerTest#testSpillAndHasDiskData -P java11-target -P java11
```

**预期结果**：spill 后 hasDiskData() 为 true，replay 后为 false，测试通过 exit code = 0

---

### [L1-测试] AT-41PK Heap Buffer 数量上限控制

**测试目标**：每个 Gate 最多持有 5 个 Heap Buffer。

**命令**：
```bash
./mvnw test -pl flink-runtime -Dtest=InputChannelRecoveredStateHandlerTest#testHeapBufferLimit -P java11-target -P java11
```

**预期结果**：测试通过 exit code = 0

---

### [L1-测试] AT-II0K Phase 2 Disk Cleanup Loop 正常工作

**测试目标**：S3 读取完成后通过 drain loop 消费完所有磁盘数据。

**命令**：
```bash
./mvnw test -pl flink-runtime -Dtest=SpillingBufferManagerTest#testPhase2DiskCleanupLoop -P java11-target -P java11
```

**预期结果**：所有 5 条数据按顺序 replay，hasDiskData() 最终为 false，测试通过 exit code = 0

---

### [L1-测试] AT-43JR Checkpoint iterator 引用计数及对照组

**测试目标**：iterator 持有引用时 replay 不删文件；无 iterator 引用时 replay 后文件被删除（对照组）。

**命令**：
```bash
./mvnw test -pl flink-runtime -Dtest=SpillingBufferManagerTest#testCheckpointIteratorRefCounting -P java11-target -P java11
./mvnw test -pl flink-runtime -Dtest=SpillingBufferManagerTest#testReplayDeletesFileWithoutIteratorRef -P java11-target -P java11
```

**预期结果**：
- 有 iterator 引用时 replay 后文件仍存在
- 无 iterator 引用时 replay 后文件被删除
- 两个测试均通过 exit code = 0

---

### [L1-测试] AT-O5AX close() 幂等性、强制清理、iterator 追踪

**测试目标**：close() 重复调用不报错，强制关闭所有存活 iterator 并删除文件。

**命令**：
```bash
./mvnw test -pl flink-runtime -Dtest=SpillingBufferManagerTest#testCloseIdempotency -P java11-target -P java11
./mvnw test -pl flink-runtime -Dtest=SpillingBufferManagerTest#testCloseDeletesSpillFiles -P java11-target -P java11
./mvnw test -pl flink-runtime -Dtest=SpillingBufferManagerTest#testCloseForceClosesAliveIterators -P java11-target -P java11
```

**预期结果**：close 后所有文件删除，iterator 被强制关闭，重复 close 不抛异常，测试通过 exit code = 0

---

### [L1-测试] AT-5097 Spill 文件 64MB 分片

**测试目标**：写入超过 size limit 后自动创建新文件。

**命令**：
```bash
./mvnw test -pl flink-runtime -Dtest=SpillingBufferManagerTest#testSpillFileRotationOnSizeLimit -P java11-target -P java11
```

**预期结果**：产生多个 spill 文件，测试通过 exit code = 0

---

### [L1-测试] AT-JXIR 旧 attempt 残留文件不清理

**测试目标**：构造 SpillingBufferManager 时不删除其他 attempt 的文件，避免误删并发 Task 文件。

**命令**：
```bash
./mvnw test -pl flink-runtime -Dtest=SpillingBufferManagerTest#testNoOldAttemptFileCleanup -P java11-target -P java11
```

**预期结果**：其他 attempt 的 spill 文件在构造后仍然存在，测试通过 exit code = 0

---

### [L1-测试] AT-UFNZ UnalignedCheckpointRescaleITCase 集成测试

**测试目标**：端到端 unaligned checkpoint rescale 恢复正确。

**命令**：
```bash
./mvnw test -pl flink-tests -Dtest=UnalignedCheckpointRescaleITCase -P java11-target -P java11
```

**预期结果**：所有 rescale 场景测试通过 exit code = 0

---

### [L1-测试] AT-RV6E SpillFileReader 直写 target Buffer 无双重拷贝

**测试目标**：readNextTo(target) 直接将数据从磁盘读入 target Buffer 的 MemorySegment，不经过中间 byte[] 拷贝，并返回正确的 channel context。

**命令**：
```bash
./mvnw test -pl flink-runtime -Dtest=SpillFileWriterReaderTest#testReadNextToTargetBuffer -P java11-target -P java11
```

**预期结果**：target Buffer 包含正确数据和 DataType，ReadResult 包含正确的 subtaskIndex 和 channelIndex，测试通过 exit code = 0

---

### [L1-测试] AT-YN8B DataType 完整序列化与反序列化

**测试目标**：所有 DataType 变体（DATA_BUFFER、DATA_BUFFER_WITH_CLEAR_END、EVENT_BUFFER、PRIORITIZED_EVENT_BUFFER 等）经过 spill-read 后完整保留。

**命令**：
```bash
./mvnw test -pl flink-runtime -Dtest=SpillFileWriterReaderTest#testAllDataTypesPreserved -P java11-target -P java11
```

**预期结果**：每种 DataType 经写入-读取后与原始类型完全一致，测试通过 exit code = 0

---

### [L1-测试] AT-C3MK Spill 目录接受 String[] 多目录参数

**测试目标**：SpillingBufferManager 构造函数接受多目录参数，文件分散到不同目录。

**命令**：
```bash
./mvnw test -pl flink-runtime -Dtest=SpillingBufferManagerTest#testMultipleSpillDirectories -P java11-target -P java11
```

**预期结果**：spill 文件分布在多个目录中，测试通过 exit code = 0

---

### [L1-测试] AT-HW4P 文件截断/损坏时 reader 抛 IOException

**测试目标**：读取被截断的 spill 文件时抛出 IOException 而非返回 null 或损坏数据。

**命令**：
```bash
./mvnw test -pl flink-runtime -Dtest=SpillFileWriterReaderTest#testTruncatedFileThrowsIOException -P java11-target -P java11
```

**预期结果**：读取不完整的 entry 时抛出 IOException，消息包含预期字节数和实际字节数，测试通过 exit code = 0

---

### [L1-测试] AT-P7JG spillBuffer 在 closed 后调用抛异常

**测试目标**：close() 后调用 spillBuffer/replayToBuffer/createCheckpointIterator 抛出 IllegalStateException。

**命令**：
```bash
./mvnw test -pl flink-runtime -Dtest=SpillingBufferManagerTest#testOperationsAfterCloseThrow -P java11-target -P java11
```

**预期结果**：每个方法都抛出 IllegalStateException，测试通过 exit code = 0

---

### [L1-测试] AT-KD5T dataLength 负数或超大值时抛 IOException

**测试目标**：构造 spill 文件中 dataLength 为负数或超大值时，reader 抛出 IOException。

**命令**：
```bash
./mvnw test -pl flink-runtime -Dtest=SpillFileWriterReaderTest#testInvalidDataLengthThrowsIOException -P java11-target -P java11
```

**预期结果**：dataLength 为 -1 或 Integer.MAX_VALUE 时抛出 IOException，测试通过 exit code = 0

---

### [L1-测试] AT-VF2X replayToBuffer 异常不跳过 entry

**测试目标**：replayToBuffer 中 copyBufferData 抛异常时，下次调用 replayToBuffer 重试同一条 entry 而非跳过。

**命令**：
```bash
./mvnw test -pl flink-runtime -Dtest=SpillingBufferManagerTest#testReplayExceptionDoesNotSkipEntry -P java11-target -P java11
```

**预期结果**：异常后重试成功获取同一条数据，不丢失 entry，测试通过 exit code = 0

---

### [L1-测试] AT-BW9L hasDiskData 纯查询无副作用

**测试目标**：hasDiskData() 不调用 finalizeCurrentWriter，currentWriter 不为 null 时返回 true 但不关闭 writer。

**命令**：
```bash
./mvnw test -pl flink-runtime -Dtest=SpillingBufferManagerTest#testHasDiskDataPureQuery -P java11-target -P java11
```

**预期结果**：多次调用 hasDiskData() 不改变内部状态，currentWriter 仍可继续写入，测试通过 exit code = 0

---

### [L1-测试] AT-QE6N ByteBuffer 复用验证

**测试目标**：SpillFileWriter/Reader 的 header ByteBuffer 为实例字段复用，多次写入/读取不分配新对象。

**命令**：
```bash
./mvnw test -pl flink-runtime -Dtest=SpillFileWriterReaderTest#testWriteAndReadMultipleBuffersInOrder -P java11-target -P java11
```

**预期结果**：多次写入读取功能正确（ByteBuffer 复用不影响正确性），测试通过 exit code = 0

---

### [L1-测试] AT-MJ3S copyBufferData 容量校验和 readerIndex 重置

**测试目标**：target 容量不足时抛出 IllegalArgumentException；target 有非零 readerIndex 时正确重置后拷贝。

**命令**：
```bash
./mvnw test -pl flink-runtime -Dtest=SpillingBufferManagerTest#testCopyBufferDataCapacityCheck -P java11-target -P java11
./mvnw test -pl flink-runtime -Dtest=SpillingBufferManagerTest#testCopyBufferDataResetsReaderIndex -P java11-target -P java11
```

**预期结果**：容量不足时抛出清晰错误，readerIndex 非零时正确处理，测试通过 exit code = 0

---

### [L1-测试] AT-XK8R Checkpoint iterator 从 replay 位置开始读

**测试目标**：部分 replay 后创建 checkpoint iterator，iterator 只返回未 replay 的数据（不重复已 replay 的数据）。

**命令**：
```bash
./mvnw test -pl flink-runtime -Dtest=SpillingBufferManagerTest#testCheckpointIteratorStartsFromReplayPosition -P java11-target -P java11
```

**预期结果**：spill 3 条数据，replay 1 条后创建 iterator，iterator 只返回剩余 2 条数据，测试通过 exit code = 0

---

### [L1-测试] AT-LN5V 大数据量多次轮转后全部 replay

**测试目标**：大量数据写入触发多次文件轮转后，全部数据按顺序 replay 正确。

**命令**：
```bash
./mvnw test -pl flink-runtime -Dtest=SpillingBufferManagerTest#testLargeDataMultiRotationReplay -P java11-target -P java11
```

**预期结果**：所有数据按 FIFO 顺序 replay，内容一致，所有文件清理完毕，测试通过 exit code = 0
