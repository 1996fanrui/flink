# User Requirements — Task 5: 两阶段 Buffer 模型与 Spilling 逻辑

## 需求偏离

无

---

## 需求列表

### REQ-NHLB 内存隔离消除死锁

Source Buffer（过滤前）使用 Heap 内存分配，Filtered Buffer（过滤后）使用 Network Buffer Pool 分配，两者不竞争同一资源池，从根本上消除死锁。

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
