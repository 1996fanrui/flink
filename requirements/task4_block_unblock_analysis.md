# Task 4: Block/Unblock 上游 Task 方案分析

## 背景

在 Task 4 中，需要确保恢复的 Buffer 在新数据之前被消费。这需要阻塞上游 Task 发送新数据，直到所有恢复的 Input/Output Buffer 都已过滤并放入 InputChannel。

同时，Checkpoint Barrier 也需要从上游发送到下游。因此需要分析：**如何在阻塞数据的同时，确保 Checkpoint Barrier 能够正常传递？**

---

## 两种方案

### 方案 1: 延迟 requestPartitions()

- 下游 Task 在所有恢复的 Buffer 过滤完成前，不调用 `requestPartitions()`
- 上游发送的 Barrier 会被缓存在 ResultSubpartition 中
- 过滤完成后，下游调用 `requestPartitions()`，此时接收到缓存的 Barrier

### 方案 2: 提前 requestPartitions() + Credit 控制

- 下游 Task 提前调用 `requestPartitions()`
- 通过不分配 Credit 来阻塞上游发送数据
- Barrier 作为优先级事件，不需要 Credit 即可发送
- 过滤完成后，开始分配 Credit，数据开始流动

---

## 方案 1 代码调研

### 核心问题

当下游未调用 `requestPartitions()` 时，上游发送 Barrier 会发生什么？

### 调研结论

**Barrier 会被成功缓存，不会阻塞上游。**

| 步骤 | 行为 | 关键方法 |
|------|------|----------|
| 1 | Barrier 通过广播发送到所有 Subpartition | `BufferWritingResultPartition.broadcastEvent()` |
| 2 | Barrier 添加到 `buffers` 队列，无论 readView 是否存在 | `PipelinedSubpartition.add()` |
| 3 | 尝试通知下游，但 readView 为 null 时静默跳过 | `PipelinedSubpartition.notifyPriorityEvent()` |
| 4 | 下游调用 requestPartitions() 后创建 readView | `SingleInputGate.requestPartitions()` |
| 5 | 下游 pollBuffer() 获取到缓存的 Barrier | `PipelinedSubpartition.pollBuffer()` |

### 关键发现

- `PipelinedSubpartition.add()` 只在 `isFinished` 或 `isReleased` 时失败，不检查 readView
- `notifyPriorityEvent()` 和 `notifyDataAvailable()` 内部检查 `readView != null`，为 null 时静默返回
- 上游不会阻塞，Barrier 添加后立即返回

---

## 方案对比

### 优缺点分析

| 维度 | 方案 1: 延迟 requestPartitions() | 方案 2: Credit 控制 |
|------|----------------------------------|---------------------|
| **实现复杂度** | 简单 | 复杂 |
| **代码改动范围** | 小（SingleInputGate 层面） | 大（Credit 分配链路） |
| **Barrier 传递** | 缓存在上游，延迟接收 | 立即接收 |
| **利用现有机制** | 是 | 需要修改流控逻辑 |
| **风险** | 低 | 中 |

### 方案 1 优缺点

**优点：**
- 实现简单，只需控制 `requestPartitions()` 调用时机
- 利用 Flink 现有的 Subpartition 缓存机制
- 代码改动范围小，风险低
- 已通过代码调研验证可行

**缺点：**
- Barrier 延迟到达下游（但不影响正确性，因为下游需要等待过滤完成才能 Snapshot）

### 方案 2 优缺点

**优点：**
- Barrier 可立即到达下游
- 连接提前建立

**缺点：**
- 需要修改 Credit 分配逻辑
- 代码改动范围大
- 需要确保 Barrier 不受 Credit 控制影响（需额外验证）
- 实现复杂度高

---

## 推荐

**推荐方案 1: 延迟 requestPartitions()**

理由：
1. 已验证可行
2. 实现简单
3. 风险低
4. Barrier 延迟不影响正确性（下游无论如何都要等过滤完成）

---

## 参考

- [任务拆分文档](./split_tasks.md)
- [Task 4 设计文档](./task4_design.md)
