# User Requirements — Task 5: 两阶段 Buffer 模型与 Spilling 逻辑

## 需求偏离

| 需求编号 | 原因 | 替代方案 |
|---------|------|---------|
| REQ-NHLB | 原始描述为 Filtered Buffer 使用 Network Buffer Pool 分配。实际设计中 Filtered Buffer 在过滤阶段使用 Heap 内存分配，过滤完全不依赖 Network Buffer Pool（避免阻塞），仅在 P1 路径最终交付时复制到 Network Buffer | Filtered Buffer 过滤阶段使用 Heap 分配，P1 路径交付时复制到 Network Buffer 进入 InputChannel。内存隔离目标通过 Source/Filtered 均使用 Heap 实现，Network Buffer 仅用于最终交付 |
| ~~REQ-2N5H~~ | ~~已消除偏离~~ | 通过元数据内存化设计，Checkpoint 路径直接从磁盘流式写入 Checkpoint Storage（SpillFileReader.readNextTo(OutputStream, int)），不再需要加载到 Network Buffer |

---

## 需求列表

### REQ-NHLB 内存隔离消除死锁

Source Buffer（过滤前）和 Filtered Buffer（过滤后）均使用 Heap 内存分配，过滤过程完全不依赖 Network Buffer Pool，从根本上消除死锁。P1 路径交付时将 Heap Buffer 数据复制到 Network Buffer 进入 InputChannel。

### REQ-QY68 Source Buffer 并发控制

Gate 内部按 Virtual Channel 顺序处理（一个 Channel 处理完再处理下一个），避免多个 Channel 同时持有 Source Buffer。每个 Gate 最多持有 5 个 Heap Buffer（约 160KB），防止 Heap 内存无限增长。

### REQ-8HRS 三条数据路径

过滤后的 Filtered Buffer 支持三条路径进入 InputChannel：
- P1 Memory Path：Network Buffer Pool 有空闲且磁盘无数据时，直接写入 Network Buffer → InputChannel
- P2 Spill Path：Network Buffer Pool 无空闲时，spill 到本地磁盘
- P3 Replay Path：Network Buffer Pool 有空闲且磁盘有数据时，从磁盘读取 → Network Buffer → InputChannel

磁盘有数据时 P3 优先于 P1，保证数据顺序。P2 和 P3 始终配对。

### REQ-0EG7 Spill 文件的 Task 消费

Task 永远不直接消费磁盘数据。Spill 文件必须先加载到 Network Buffer，再放入 InputChannel 供 Task 消费。加载完成后对应磁盘文件可立即清理。

### REQ-2N5H Spill 文件的 Checkpoint 快照

Checkpoint 触发时，磁盘上未加载的 spill 数据直接备份到 Checkpoint Storage，不需要先加载到 Network Buffer。如果数据在 Checkpoint 前已通过 P3 加载到 InputChannel，则走现有 Buffer 快照逻辑，不需要特殊处理。

### REQ-NPBY 非过滤场景不受影响

未开启 unaligned checkpoint recovery 或并行度未变（`NO_RESCALE`）时，完全走原有 channel state 恢复路径，不涉及两阶段 Buffer 模型，不申请任何 Heap Buffer，不做任何改动。

### REQ-J6QM 移除不可用的 LazyFileBuffer

LazyFileBuffer 存在大量 bug 无法工作，需要清理移除，避免维护负担。用新的 Spilling 逻辑替代其功能。

### REQ-U7R8 Spill 文件状态可区分

Spill 文件必须能区分"在磁盘上（未加载）"和"已加载到 Network Buffer"两种状态，以便 Task 消费和 Checkpoint 快照采取正确行为。

### REQ-PV3D SpillFileReader 接口层次正确

SpillFileReader（I/O 层）是纯字节搬运工具，不分配 Buffer，不感知业务语义（channel context、DataType 等元数据均由 SpillingBufferManager 在内存中维护）。针对两种消费场景提供两种读取接口：
- Task 消费路径：readNextTo(Buffer target, int length)，将磁盘数据直接读入调用方提供的 Network Buffer
- Checkpoint 路径：readNextTo(OutputStream out, int length)，将磁盘数据直接流式写入 Checkpoint Storage 的 OutputStream，零 buffer 中介

### REQ-M8KE Spill 目录来源统一

SpillingBufferManager 的 spill 目录必须接入 Flink 已有的 `IOManager.getSpillingDirectoriesPaths()` 链路，接受 `String[]` 多目录参数，与 SpanningWrapper、ChannelStateFilteringHandler 等场景统一目录获取代码。

### REQ-G4RP 移除 cleanupOldAttemptFiles

SpillingBufferManager 不应在构造时清理其他 attempt 的 spill 文件。TM 级共享 spill 目录下多个 Task 可能并发运行，按 attemptId 过滤删除会误删正在运行的其他 Task 文件。正常退出由 `close()` 清理自身文件，异常退出由 TM 的 `FileChannelManagerImpl` shutdown hook 兜底。

### REQ-WB9F DataType 完整保留

Buffer 的 DataType 信息必须完整保留，不允许丢失细分类型（如 DATA_BUFFER_WITH_CLEAR_END、PRIORITIZED_EVENT_BUFFER 等）。DataType 作为元数据由 SpillingBufferManager 在内存中维护，不写入磁盘。

### REQ-T5AJ 读写健壮性

- SpillFileReader 的部分读取（partial read）必须抛出 IOException 而非静默返回 false 或损坏数据
- SpillFileReader 内部处理异常回退（失败时自行 seek 回读取前位置），不暴露 position/seek 给外部
- replayToBuffer 异常时不允许静默跳过 entry 导致数据丢失，必须保证可重试或明确报错

### REQ-JD2C 资源管理安全性

- spillBuffer/replayToBuffer/createCheckpointIterator/hasDiskData 等方法在 closed 状态下调用必须抛出异常，防止 close 后创建无法清理的文件
- SpillFileWriter.close() 必须用 try-finally 保证 fileChannel 关闭，防止 force() 异常导致句柄泄漏
- SpillingBufferManager.close() 必须追踪所有存活的 CheckpointSpillIterator，close 时强制关闭，防止文件句柄和 Buffer 泄漏
- SpillingBufferManager.close() 中 SpillFile.close() 失败时仍必须尝试删除文件

### REQ-K7NW hasDiskData 纯查询

`hasDiskData()` 必须是纯查询方法，不允许有 `finalizeCurrentWriter` 等修改状态的副作用。

### REQ-F2HQ 性能优化

- SpillFileWriter.close() 不需要 `force(true)` 刷盘，spill 文件是秒级生命周期的临时文件，不需要持久性保证
- 元数据（length、DataType、channel context）存内存不存磁盘，减少 I/O 量（每 entry 省去 13 字节元数据写入）

### REQ-D9PQ Checkpoint Iterator 正确性

- Checkpoint iterator 必须从当前 replay 进度开始迭代，不允许重复已被 replay 的数据（基于内存元数据的消费进度定位，非文件偏移量定位）
- Checkpoint iterator 必须提供每条 entry 的 channel context（old subtask index、old channel index），从内存元数据获取

### REQ-AX4C 测试覆盖补充

补充以下关键测试场景：
- SpillFileReader 文件截断时抛出 IOException
- spillBuffer 在 closed 状态后调用抛出异常
- 大数据量 spill（多次文件轮转后全部 replay）
- Checkpoint iterator 引用计数补充对照组：无 iterator 引用时 replay 后文件应被删除
- Checkpoint 路径直接流式写入 OutputStream 验证
