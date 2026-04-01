# 验收方案 — Task 5: 两阶段 Buffer 模型与 Spilling 逻辑

## 状态表

| 编号 | 测试内容概要 | 需求ID列表 | 状态 | 测试执行方 | 备注 |
|------|------------|-----------|------|-----------|------|
| AT-2W3J | P1 Memory Path 正常工作 | REQ-8HRS | 通过 | 代码自动化 | `SpillingBufferManagerTest#testMemoryPath` |
| AT-GE7G | P2 Spill Path 正常工作 | REQ-8HRS,REQ-0EG7 | 通过 | 代码自动化 | `SpillingBufferManagerTest#testSpillAndHasDiskData` |
| AT-SX5O | P3 Replay Path 正常工作 | REQ-8HRS,REQ-0EG7 | 通过 | 代码自动化 | `SpillingBufferManagerTest#testSpillAndReplay` |
| AT-QUBL | P3 优先于 P1 保证数据顺序 | REQ-8HRS | 通过 | 代码自动化 | `SpillingBufferManagerTest#testReplayPriorityOverMemoryPath` |
| AT-36DP | Source Buffer 使用 Heap 内存不竞争 Pool | REQ-NHLB | 通过 | 代码自动化 | `InputChannelRecoveredStateHandlerTest#testSourceBufferUsesHeapMemory` |
| AT-UE7O | Gate 内 Virtual Channel 顺序处理 | REQ-QY68 | 通过 | 代码自动化 | `SequentialChannelStateReaderImplTest#testSequentialChannelProcessing` |
| AT-LOBV | Checkpoint 快照磁盘 spill 数据 | REQ-2N5H | 通过 | 代码自动化 | `SpillingBufferManagerTest#testCheckpointWithDiskData` |
| AT-EPHC | Checkpoint 快照已加载数据走现有逻辑 | REQ-2N5H | 通过 | 代码自动化 | `SpillingBufferManagerTest#testCheckpointAfterFullReplay` |
| AT-U7Q2 | 非过滤场景不受影响 | REQ-NPBY | 通过 | 代码自动化 | `SequentialChannelStateReaderImplTest#testNoFilteringScenario` |
| AT-IYKG | LazyFileBuffer 已移除 | REQ-J6QM | 通过 | Agent 执行 | `grep -r LazyFileBuffer` 返回空，文件已删除 |
| AT-OD4D | Spill 文件状态可区分 | REQ-U7R8 | 通过 | 代码自动化 | `SpillingBufferManagerTest#testSpillAndHasDiskData` |
| AT-41PK | Heap Buffer 数量上限控制 | REQ-QY68 | 通过 | 代码自动化 | `InputChannelRecoveredStateHandlerTest#testHeapBufferLimit` |
| AT-II0K | Phase 2 Disk Cleanup Loop 正常工作 | REQ-8HRS,REQ-0EG7 | 通过 | 代码自动化 | `SpillingBufferManagerTest#testPhase2DiskCleanupLoop` |
| AT-43JR | Checkpoint iterator 与 Replay 并发引用计数 | REQ-2N5H,REQ-U7R8 | 通过 | 代码自动化 | `SpillingBufferManagerTest#testCheckpointIteratorRefCounting` |
| AT-O5AX | close() 幂等性和强制清理 | REQ-U7R8 | 通过 | 代码自动化 | `SpillingBufferManagerTest#testCloseIdempotency` + `#testCloseDeletesSpillFiles` |
| AT-5097 | Spill 文件 64MB 分片 | REQ-8HRS | 通过 | 代码自动化 | `SpillingBufferManagerTest#testSpillFileRotationOnSizeLimit` |
| AT-JXIR | 旧 attempt 残留文件清理 | REQ-U7R8 | 通过 | 代码自动化 | `SpillingBufferManagerTest#testOldAttemptFileCleanup` |
| AT-UFNZ | UnalignedCheckpointRescaleITCase 集成测试 | REQ-NHLB,REQ-8HRS,REQ-NPBY | 通过 | 代码自动化 | 48 tests all passed |

---

## 验收步骤

### [L1-测试] AT-2W3J P1 Memory Path 正常工作

**测试目标**：当 Network Buffer Pool 有空闲 Buffer 且磁盘无数据时，过滤结果直接写入 Network Buffer 进入 InputChannel。

**测试方法**：单元测试

**命令**：
```bash
./mvnw test -pl flink-runtime -Dtest=SpillingBufferManagerTest#testMemoryPath -P java11-target -P java11
```

**预期结果**：
- 过滤后的数据通过 Network Buffer 直接放入 InputChannel
- 不产生任何 spill 文件
- 测试通过，exit code = 0

---

### [L1-测试] AT-GE7G P2 Spill Path 正常工作

**测试目标**：当 Network Buffer Pool 无空闲 Buffer 时，过滤结果 spill 到本地磁盘文件。

**测试方法**：单元测试，模拟 Buffer Pool 耗尽场景

**命令**：
```bash
./mvnw test -pl flink-runtime -Dtest=SpillingBufferManagerTest#testSpillPath -P java11-target -P java11
```

**预期结果**：
- `SpillingBufferManager.tryRequestBuffer()` 返回 null
- 过滤结果被写入 spill 文件
- Spill 文件格式正确（length-prefixed: 4 bytes length + N bytes data + 1 byte type flag）
- 测试通过，exit code = 0

---

### [L1-测试] AT-SX5O P3 Replay Path 正常工作

**测试目标**：当 Network Buffer Pool 有空闲 Buffer 且磁盘有已 spill 数据时，从磁盘读取数据到 Network Buffer 进入 InputChannel。

**测试方法**：单元测试，先 spill 数据再模拟 Buffer 可用

**命令**：
```bash
./mvnw test -pl flink-runtime -Dtest=SpillingBufferManagerTest#testReplayPath -P java11-target -P java11
```

**预期结果**：
- 从磁盘读取的数据与原始 spill 数据一致
- Replay 完成后 spill 文件被删除
- 数据通过 Network Buffer 进入 InputChannel
- 测试通过，exit code = 0

---

### [L1-测试] AT-QUBL P3 优先于 P1 保证数据顺序

**测试目标**：当磁盘有待 replay 数据时，即使新来了过滤结果且有 Network Buffer 可用，也优先 replay 磁盘数据。

**测试方法**：单元测试，验证 FIFO 顺序

**命令**：
```bash
./mvnw test -pl flink-runtime -Dtest=SpillingBufferManagerTest#testReplayPriorityOverMemoryPath -P java11-target -P java11
```

**预期结果**：
- 磁盘有数据时，新的过滤结果走 P2 spill
- P3 replay 的数据在 InputChannel 中的顺序早于新的过滤结果
- 测试通过，exit code = 0

---

### [L1-测试] AT-36DP Source Buffer 使用 Heap 内存不竞争 Pool

**测试目标**：Source Buffer 使用 `MemorySegmentFactory.allocateUnpooledSegment` 分配 Heap 内存，不从 Network Buffer Pool 获取。

**测试方法**：单元测试，验证 `InputChannelRecoveredStateHandler.getBuffer()` 分配的 Buffer 类型

**命令**：
```bash
./mvnw test -pl flink-runtime -Dtest=InputChannelRecoveredStateHandlerTest#testSourceBufferUsesHeapMemory -P java11-target -P java11
```

**预期结果**：
- `getBuffer()` 返回的 Buffer 底层使用 Heap MemorySegment
- Network Buffer Pool 的 Buffer 数量不减少
- 测试通过，exit code = 0

---

### [L1-测试] AT-UE7O Gate 内 Virtual Channel 顺序处理

**测试目标**：Gate 内的多个 Virtual Channel 按顺序处理，不并发。

**测试方法**：单元测试，通过 `SequentialChannelStateReaderImpl.readInputData()` 的执行顺序验证

**命令**：
```bash
./mvnw test -pl flink-runtime -Dtest=SequentialChannelStateReaderImplTest#testSequentialChannelProcessing -P java11-target -P java11
```

**预期结果**：
- 同一时刻只有一个 Channel 的数据在被处理
- Channel 处理顺序与 `extractOffsetsSorted()` 排序一致
- 测试通过，exit code = 0

---

### [L1-测试] AT-LOBV Checkpoint 快照磁盘 spill 数据

**测试目标**：Checkpoint 触发时，磁盘上未加载的 spill 数据能被正确纳入快照。

**测试方法**：单元测试，模拟 Checkpoint 触发时 SpillingBufferManager 仍有磁盘数据

**命令**：
```bash
./mvnw test -pl flink-runtime -Dtest=SpillingBufferManagerTest#testCheckpointWithDiskData -P java11-target -P java11
```

**预期结果**：
- `createCheckpointIterator()` 返回的 iterator 包含所有未 replay 的磁盘数据
- iterator 产出的 Buffer 内容与原始 spill 数据一致
- 测试通过，exit code = 0

---

### [L1-测试] AT-EPHC Checkpoint 快照已加载数据走现有逻辑

**测试目标**：当 spill 数据在 Checkpoint 前已通过 P3 加载到 InputChannel 时，走现有 Buffer 快照逻辑，不做特殊处理。

**测试方法**：单元测试，模拟数据全部 replay 后触发 Checkpoint

**命令**：
```bash
./mvnw test -pl flink-runtime -Dtest=SpillingBufferManagerTest#testCheckpointAfterFullReplay -P java11-target -P java11
```

**预期结果**：
- `hasDiskData()` 返回 false
- `createCheckpointIterator()` 返回空 iterator
- Checkpoint 正常完成，走现有 InputChannel 快照逻辑
- 测试通过，exit code = 0

---

### [L1-测试] AT-U7Q2 非过滤场景不受影响

**测试目标**：当 `unalignedDuringRecoveryEnabled = false` 或 `rescalingDescriptor == NO_RESCALE` 时，恢复路径完全不变。

**测试方法**：单元测试，验证非过滤场景下不创建 SpillingBufferManager 和 ChannelStateFilteringHandler

**命令**：
```bash
./mvnw test -pl flink-runtime -Dtest=SequentialChannelStateReaderImplTest#testNoFilteringScenario -P java11-target -P java11
```

**预期结果**：
- 不创建 `ChannelStateFilteringHandler`
- 不创建 `SpillingBufferManager`
- 不分配任何 Heap Buffer
- 走现有 `RecoveredInputChannel` 阻塞申请 Buffer 逻辑
- 测试通过，exit code = 0

---

### [L2-Agent] AT-IYKG LazyFileBuffer 已移除

**测试目标**：确认 `LazyFileBuffer.java` 文件已删除，代码中无残留引用。

**采集命令**：
```bash
# 检查文件是否存在
ls flink-runtime/src/main/java/org/apache/flink/runtime/io/network/buffer/LazyFileBuffer.java 2>&1

# 搜索代码中的引用
grep -r "LazyFileBuffer" --include="*.java" flink-runtime/src/ 2>&1
```

**判定命令**：
```bash
# 文件不存在且无引用则通过
test ! -f flink-runtime/src/main/java/org/apache/flink/runtime/io/network/buffer/LazyFileBuffer.java && \
  ! grep -rq "LazyFileBuffer" --include="*.java" flink-runtime/src/ && \
  echo "PASS" || echo "FAIL"
```

**客观证据**：`ls` 返回 "No such file or directory"，`grep` 返回空结果，判定命令输出 "PASS"。

---

### [L1-测试] AT-OD4D Spill 文件状态可区分

**测试目标**：SpillingBufferManager 内部能区分"在磁盘上（未加载）"和"已加载到 Network Buffer"两种状态。

**测试方法**：单元测试

**命令**：
```bash
./mvnw test -pl flink-runtime -Dtest=SpillingBufferManagerTest#testSpillFileStateTracking -P java11-target -P java11
```

**预期结果**：
- Spill 后 `hasDiskData()` 返回 true
- Replay 后对应文件从队列移除，文件被删除
- 全部 replay 后 `hasDiskData()` 返回 false
- 测试通过，exit code = 0

---

### [L1-测试] AT-41PK Heap Buffer 数量上限控制

**测试目标**：每个 Gate 最多持有 5 个 Heap Buffer，防止 Heap 内存无限增长。

**测试方法**：单元测试，验证 Heap Buffer 分配数量控制

**命令**：
```bash
./mvnw test -pl flink-runtime -Dtest=InputChannelRecoveredStateHandlerTest#testHeapBufferLimit -P java11-target -P java11
```

**预期结果**：
- 同一 Gate 最多同时持有 5 个 Heap Buffer
- 超过上限时抛出异常或阻塞等待释放
- 测试通过，exit code = 0

---

### [L1-测试] AT-II0K Phase 2 Disk Cleanup Loop 正常工作

**测试目标**：S3 数据全部读取并过滤完毕后，磁盘上剩余的 spill 数据通过 Phase 2 阻塞等待 Network Buffer 完成 replay。

**测试方法**：单元测试，模拟 S3 读取完成后仍有磁盘数据的场景

**命令**：
```bash
./mvnw test -pl flink-runtime -Dtest=SpillingBufferManagerTest#testPhase2DiskCleanupLoop -P java11-target -P java11
```

**预期结果**：
- Phase 1 结束后 `hasDiskData()` 返回 true
- Phase 2 通过 `requestBufferBlocking()` 逐个 replay 磁盘数据
- 所有磁盘数据 replay 完成后 `hasDiskData()` 返回 false
- 数据顺序与原始 spill 顺序一致
- 测试通过，exit code = 0

---

### [L1-测试] AT-43JR Checkpoint iterator 与 Replay 并发引用计数

**测试目标**：Checkpoint iterator 持有 spill 文件引用期间，Replay 不删除被引用的文件；iterator close 后引用释放，文件可被正常 replay 和删除。

**测试方法**：单元测试，模拟 Checkpoint iterator 和 Replay 并发访问场景

**命令**：
```bash
./mvnw test -pl flink-runtime -Dtest=SpillingBufferManagerTest#testCheckpointIteratorRefCounting -P java11-target -P java11
```

**预期结果**：
- createCheckpointIterator() 创建后，文件引用计数 > 0
- Replay 尝试删除被引用文件时跳过（不删除）
- iterator.close() 后引用计数归零
- 引用释放后 Replay 可正常删除文件
- 测试通过，exit code = 0

---

### [L1-测试] AT-O5AX close() 幂等性和强制清理

**测试目标**：SpillingBufferManager.close() 可重复调用不报错，且强制清理所有残留文件（即使有未关闭的 iterator）。

**测试方法**：单元测试

**命令**：
```bash
./mvnw test -pl flink-runtime -Dtest=SpillingBufferManagerTest#testCloseIdempotencyAndForceCleanup -P java11-target -P java11
```

**预期结果**：
- 第一次 close() 删除所有 spill 文件
- 第二次 close() 不抛异常，直接返回
- 即使有未关闭的 Checkpoint iterator，close() 也强制删除所有文件
- 测试通过，exit code = 0

---

### [L1-测试] AT-5097 Spill 文件 64MB 分片

**测试目标**：当单个 spill 文件写入数据超过 64MB 时，自动创建新的 spill 文件。

**测试方法**：单元测试，写入超过 64MB 的数据

**命令**：
```bash
./mvnw test -pl flink-runtime -Dtest=SpillingBufferManagerTest#testSpillFileSizeLimit -P java11-target -P java11
```

**预期结果**：
- 写入超过 64MB 后产生 2 个以上的 spill 文件
- 每个文件大小不超过 64MB（+ 最后一个 Buffer 的大小容差）
- Replay 时按文件创建顺序依次读取
- 测试通过，exit code = 0

---

### [L1-测试] AT-JXIR 旧 attempt 残留文件清理

**测试目标**：SpillingBufferManager 初始化时清理 spill 目录中旧 attempt 的残留文件。

**测试方法**：单元测试，预先创建符合命名模式的旧文件

**命令**：
```bash
./mvnw test -pl flink-runtime -Dtest=SpillingBufferManagerTest#testOldAttemptFileCleanup -P java11-target -P java11
```

**预期结果**：
- 初始化前 spill 目录存在旧 attempt 的 `channel-state-spill-*` 文件
- 初始化后旧 attempt 文件被删除
- 当前 attempt 的文件不受影响
- 测试通过，exit code = 0

---

### [L1-测试] AT-UFNZ UnalignedCheckpointRescaleITCase 集成测试

**测试目标**：端到端验证 unaligned checkpoint 在 rescale 场景下的完整恢复流程，覆盖两阶段 Buffer 模型、spilling 逻辑和非过滤场景。

**测试方法**：集成测试，通过 `UnalignedCheckpointRescaleITCase` 验证 rescale（并行度变化）场景下 checkpoint 恢复的正确性

**命令**：
```bash
./mvnw test -pl flink-tests -Dtest=UnalignedCheckpointRescaleITCase -P java11-target -P java11
```

**预期结果**：
- 所有 rescale 场景（scale up / scale down）的 checkpoint 恢复正确完成
- 恢复后数据一致性验证通过（无数据丢失或重复）
- 过滤场景正确触发两阶段 Buffer 模型（内存隔离 + spill 逻辑）
- 非 rescale 场景走原有路径不受影响
- 测试通过，exit code = 0
