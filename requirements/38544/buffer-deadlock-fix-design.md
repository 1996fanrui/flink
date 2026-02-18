# Buffer 死锁问题修复设计

## 问题背景

在 Unaligned Checkpoint 恢复过程中，当启用 `execution.checkpointing.unaligned.during-recovery.enabled = true` 时，buffer filtering 功能导致测试卡住。

## 问题分析

### 死锁原因

Buffer filtering 过程中存在循环依赖：

1. 读取 checkpoint 数据需要申请 buffer（source buffer）
2. 过滤后序列化需要申请新 buffer（output buffer）
3. 两者从同一个有限的 LocalBufferPool 申请
4. source buffer 要等 output buffer 写完才能释放
5. 当 source buffer 占满 pool 时，output buffer 无法申请 → **死锁**

## 方案对比

| | 限制数量方案 | 堆内存方案 |
|---|---|---|
| 思路 | 限制 source buffer 数量，为 output buffer 预留空间 | source buffer 使用堆内存，与 LocalBufferPool 隔离 |
| 死锁风险 | 需精确控制比例 | 完全隔离，无风险 |
| 实现复杂度 | 高 | 低 |

### 限制数量方案的难点

1. **容量不可靠**：`getMaxNumberOfMemorySegments()` 只是上限，实际可用数量由 NetworkBufferPool 动态分配，可能远小于上限
2. **动态变化**：`currentPoolSize` 会被 NetworkBufferPool 随时调整，难以准确计数
3. **需要区分调用场景**：source buffer 和 output buffer 都调用同一个 `requestBufferBlocking()` 方法，需要额外机制区分

## 推荐方案：堆内存

### 核心思想

当 `execution.checkpointing.unaligned.during-recovery.enabled = true` 时，source buffer 使用 Java 堆内存而非 LocalBufferPool，与 output buffer 完全隔离。

### 实现方式

使用 Flink 现有 API：
- `MemorySegmentFactory.allocateUnpooledSegment(size)` - 分配堆内存
- `FreeingBufferRecycler.INSTANCE` - 回收时直接释放


### 改动范围

仅需修改 filter 前的 buffer 申请逻辑（`InputChannelRecoveredStateHandler.getBuffer()` 或相关位置），对原有流程无影响。

Buffer size 直接使用 segment size 的默认大小即可，避免大量的传参。

### TODO

- [ ] 后期需控制堆内存 buffer 的数量上限，防止堆内存 OOM
