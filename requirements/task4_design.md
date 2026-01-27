# Task 4: Change the Overall UC Restore Process - 详细设计

## 概述

本文档描述 Task 4 的详细技术设计，核心目标是变更 UC 恢复流程，使 Checkpoint 能够在 Recovery 阶段更早触发。

---

## 1. 配置开关

**所有 Task 4 的变更仅在以下配置开启时生效：**

```
execution.checkpointing.unaligned.during-recovery.enabled = true
```

默认值为 `false`，即默认不启用。

---

## 2. 核心变更

### 2.1 Checkpoint 触发时机

| | 当前 | Task 4 之后 |
|---|---|---|
| **Checkpoint 触发条件** | 所有 Task 进入 **RUNNING** | 所有 Task 进入 **INITIALIZING** |

### 2.2 Checkpoint 流程拆分

Checkpoint 流程分为两个阶段：

| 阶段 | 执行位置 | 等待 Buffer 过滤？ |
|------|----------|-------------------|
| **Trigger** | JM (CheckpointCoordinator) | ❌ 不等待 |
| **Snapshot** | 每个 Task | ✅ 等待本 Task 的 Buffer 过滤完成 |

**关键点：**
- Checkpoint **Trigger** 不需要等待 Buffer 过滤
- 每个 Task 执行 **Snapshot** 时，需等待自己的 Input/Output Buffer 过滤完成

---

## 3. 详细设计

### 3.1 Checkpoint Trigger 变更

**变更点：CheckpointCoordinator**

当前检查 Task 状态为 RUNNING，需改为检查 INITIALIZING 或 RUNNING。

```
Before: all tasks in RUNNING → trigger checkpoint
After:  all tasks in INITIALIZING or RUNNING → trigger checkpoint
```

**注意：** 仅当配置 `execution.checkpointing.unaligned.during-recovery.enabled = true` 时生效。

### 3.2 Task Snapshot 等待逻辑

**变更点：Task Snapshot 流程**

当 Task 收到 Checkpoint Barrier 并执行 Snapshot 时：

```
1. 收到 Checkpoint Barrier
2. 检查 Buffer 过滤是否完成
   - 如果未完成：等待过滤完成
   - 如果已完成：继续
3. 执行 Snapshot（包括 Input/Output Channel State）
4. 发送 Acknowledge 给 JM
```

**实现方式：**
- 引入 `bufferFilteringCompleteFuture: CompletableFuture<Void>`
- Snapshot 时 await 此 Future

### 3.3 Block/Unblock 上游 Task

**方案：延迟 requestPartitions() 调用**

- 下游 Task 在所有恢复的 Buffer 过滤完成并放入 InputChannel 后，才调用 `requestPartitions()`
- 上游发送的 Barrier 会被缓存在 ResultSubpartition 中，下游请求后获取
- 详细方案分析见 [task4_block_unblock_analysis.md](./task4_block_unblock_analysis.md)

---

## 4. 时序图

```
        JM                      Task                   channelIOExecutor
         │                        │                           │
         │    deploy task         │                           │
         │───────────────────────>│                           │
         │                        │                           │
         │                        │  initialize operators     │
         │                        │──────────┐                │
         │                        │<─────────┘                │
         │                        │                           │
         │  INITIALIZING          │                           │
         │<───────────────────────│                           │
         │                        │                           │
         │                        │  start buffer recovery    │
         │                        │──────────────────────────>│
         │                        │                           │
         │  trigger checkpoint    │                           │ (filtering...)
         │───────────────────────>│                           │
         │                        │                           │
         │                        │  wait for filter done     │
         │                        │·························>│
         │                        │                           │
         │                        │                           │ filter complete
         │                        │<──────────────────────────│
         │                        │                           │
         │                        │  snapshot & ack           │
         │<───────────────────────│                           │
         │                        │                           │
         │                        │  requestPartitions()      │
         │                        │──────────┐                │
         │                        │<─────────┘                │
         │                        │                           │
         │  RUNNING               │                           │
         │<───────────────────────│                           │
         │                        │                           │
         │                        │  process new data         │
         │                        │                           │
```

---

## 5. 需要修改的关键类

| 类 | 修改内容 |
|----|----------|
| `CheckpointCoordinator` | 修改触发条件：当配置开启时，INITIALIZING 也可触发 |
| `StreamTask` | Snapshot 时等待 Buffer 过滤完成 |
| `SingleInputGate` | 控制 `requestPartitions()` 调用时机 |

---

## 6. 待确认问题

1. **Source Task 处理**
   - Source Task 没有上游，是否需要特殊处理？

---

## 7. 参考

- [FLIP-547 Wiki](https://cwiki.apache.org/confluence/display/FLINK/FLIP-547%3A+Support+checkpoint+during+recovery)
- [Block/Unblock 方案分析](./task4_block_unblock_analysis.md)
- [原始需求文档](./requirement.md)
- [任务拆分文档](./split_tasks.md)
