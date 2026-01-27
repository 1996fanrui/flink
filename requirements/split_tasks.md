# FLIP-547 任务拆分文档

## 概述

本文档将 FLIP-547 (Support Checkpoint During Recovery) 的完整需求拆分为 4 个可独立开发的任务。

**原始需求文档**: [requirements/requirement.md](./requirement.md)

## 为什么拆分为 4 个任务

FLIP-547 的核心目标是支持在 Recovery 阶段触发 Checkpoint，以解决以下问题：
- Recovery 阶段可能持续数小时，期间无法触发 Checkpoint
- 重启或扩缩容会丢失已恢复的进度
- 上游系统被阻塞，可能导致级联故障

为实现这一目标，需要解决的核心挑战是：**确保 Exactly-Once 语义**，特别是在多次扩缩容场景下避免数据重复。

解决方案的核心思路是：**在 Checkpoint 前过滤并重组 Buffer，确保每条记录只被快照一次**。

基于此，我们将实现拆分为 4 个任务：

| 任务 | 关注点 | 状态 | 优先级 |
|------|--------|------|--------|
| Task 1 | 数据路径变更（Buffer 在哪里恢复） | ✅ 已合并 | 必需 |
| Task 2 | 核心过滤机制 | 待开发 | 必需 |
| Task 3 | 内存压力处理 | 待开发 | 可选（优化） |
| Task 4 | 控制面变更（生命周期、Checkpoint 触发） | 待开发 | 必需 |

## 任务依赖关系

```
Task 1 (已完成)
    │
    ├──→ Task 2 (过滤逻辑) ──────────────────┐
    │                                        ├──→ 首版/POC 完成
    └──→ Task 4 (控制面变更，可与 Task 2 并行) ──┘
                                             │
                                             └──→ Task 3 (可选优化，后续迭代)
```

---

## Task 1: Recover Output Buffers on Downstream Task Side Directly

**状态**: ✅ 已合并到 Apache Flink

### 职责

将上游 Task 的 Output Buffer 直接在下游 Task 侧恢复，而不是在上游 Task 侧恢复后再发送。

### 核心变更

1. **JobMaster 侧变更**
   - 将 Output Buffer 的分配逻辑从上游 Task 侧移动到 JobMaster 侧
   - JobMaster 根据 ResultSubpartitionInfo、subtask ID 等元数据分配 State Handler
   - 将 Output Buffer 的 State Handler 发送给下游 Task（而非上游 Task）

2. **下游 Task 侧变更**
   - 直接读取 Output Buffer 并添加到对应的 InputChannel

### 核心原则

无论 Output Buffer 从上游还是下游恢复，Buffer 分配策略保持不变。例如：如果原本某个 Output Buffer 会被发送到下游 Subtask3 的 InputChannel5，那么新方案中 JobMaster 会直接将 State Handler 发送给 Subtask3，由 Subtask3 读取并添加到 InputChannel5。

### 收益

- 无需从上游 Task 发送 Output Buffer 到下游 Task
- 下游 Task 侧可以方便地执行过滤和重组逻辑（为 Task 2 奠定基础）

---

## Task 2: Filtering Records in Async Thread

**状态**: 待开发

### 职责

在异步线程中过滤恢复的 Buffer，将原始 Buffer 转换为过滤后的 Buffer。

**核心路径**: Original Buffers → Filtered Buffers

### 核心变更

1. **过滤逻辑实现**
   - 在 Channel-state-unspilling 线程（可重命名为 Channel-state-handling）中执行过滤
   - 根据 Key Group Range 过滤记录
   - 将过滤后的记录重组到新的 Buffer 中

2. **Virtual Channel 处理**
   - 过滤逻辑通过 Virtual Channel 处理
   - 过滤后的 Buffer 直接放入 Real InputChannel（RemoteInputChannel 或 LocalInputChannel）
   - Task 线程处理数据或 Checkpoint 时无需再执行过滤逻辑

3. **数据流（P1: S3-To-Memory Path）**
   ```
   S3 → Filter → Network Buffer → Input Channel
   ```
   - 这是最理想的路径
   - 当 Network Buffer 可用时，从 S3 读取数据，过滤后直接放入 Buffer

### 与现有 Checkpoint 逻辑的关系

过滤后的 Buffer 放入 InputChannel 后，就是普通的 Network Buffer。现有的 UC Checkpoint 逻辑会自动快照这些 Buffer，无需额外处理。

### 收益

- Task 线程可以尽早开始处理数据（第一个过滤后的 Buffer 生成后即可开始）
- 过滤后的 Buffer 只包含当前 Subtask 需要的记录，解决了数据膨胀问题
- 确保 Exactly-Once 语义

---

## Task 3: Spilling Buffers to Local Disk

**状态**: 待开发

**优先级**: 可选（优化项，首版可跳过）

### 为什么 Task 3 是可选的

Task 3 是一个**优化项**，而非正确性必需：

| 场景 | 无 Task 3 | 有 Task 3 |
|------|-----------|-----------|
| Network Memory 充足 | ✓ 正常工作 | ✓ 正常工作 |
| Network Memory 不足 | ✓ 可工作（Checkpoint 延迟完成） | ✓ 可工作（Checkpoint 更快完成） |
| 正确性保证 | ✓ 保证 | ✓ 保证 |

**无 Task 3 时的行为：**
- 当 Network Memory 充足时：Task 2 的 P1 路径正常工作，无任何问题
- 当 Network Memory 不足时：过滤线程阻塞等待 Buffer 可用，Checkpoint 完成时间延迟（必须等待所有 S3 数据过滤完成）
- **仍然优于现有逻辑**：现有逻辑在 Recovery 阶段完全无法触发 Checkpoint

因此，首版/POC 可以仅实现 Task 2 + Task 4，Task 3 作为后续优化项。

### 职责

当 Network Memory 不足时，将过滤后的 Buffer 写入本地磁盘，确保 Checkpoint 能够尽早触发。并在后台异步地将磁盘数据恢复到 Network Memory。

### 核心变更

1. **Spill 路径（P2: S3-To-Disk-Spill Path）**
   ```
   S3 → Filter → Local Disk
   ```
   - 当 Network Buffer 不可用时，仍然从 S3 读取并过滤数据
   - 将过滤后的结果写入本地磁盘
   - 确保过滤工作持续进行，不被 Buffer 不足阻塞

2. **Replay 路径（P3: Disk-To-Memory Path）**
   ```
   Local Disk → Network Buffer → Input Channel
   ```
   - 当 Network Buffer 可用时，优先从本地磁盘读取已过滤的数据
   - 将数据放入 Input Channel

3. **处理流程**

   **Phase 1: S3 Active Loop**
   - 主要操作模式，只要 S3 还有数据就保持在此阶段
   - 使用非阻塞方式请求 Buffer
   - 如果获取到 Buffer：优先执行 P3（磁盘有数据时），否则执行 P1
   - 如果未获取到 Buffer：执行 P2，确保过滤工作持续进行

   **Phase 2: Disk-Only Cleanup Loop**
   - S3 数据处理完毕后进入此阶段
   - 使用阻塞方式请求 Buffer
   - 仅执行 P3，清理磁盘缓存
   - 直到磁盘清空

4. **Checkpoint 期间的磁盘数据处理**
   - Checkpoint 时需要上传 Network Buffer 和 Local Disk 中的所有过滤后的 Buffer

### 收益

- 即使 Network Buffer 不足，Checkpoint 也能触发（过滤工作不被阻塞）
- 过滤后的数据可以从磁盘上传到 Checkpoint Storage

---

## Task 4: Change the Overall UC Restore Process

**状态**: 待开发

### 职责

变更整体的 UC 恢复流程，包括 Task 生命周期、Checkpoint 触发时机、上游阻塞机制等。

### 核心变更

1. **Task INITIALIZING 阶段变更**
   - 在初始化开始时请求上游 Partition
   - 允许接收上游 Task 的事件（如 Checkpoint Barrier）
   - 不允许在初始化阶段接收数据（确保 Output Buffer 在 Input Buffer 之后消费）
   - Task ExecutionState 更早地从 INITIALIZING 切换到 RUNNING
   - 只初始化 Task，不处理数据，预期非常快

2. **Task RUNNING 阶段变更**
   - 所有 Task 的 ExecutionState 为 RUNNING 后，即可触发 Checkpoint
   - 在异步线程中读取、分配和过滤恢复的 Input & Output Buffer
   - Task 线程开始消费过滤后的 Buffer
   - 所有恢复的 Buffer 放入 Real InputChannel 后，才开始消费新数据

3. **Block/Unblock 上游 Task**
   - 初始阶段阻塞上游 Task（类似背压机制）
   - 确保新生成的 Buffer 在恢复的 Buffer 之后被消费
   - 当所有 Input Buffer 和 Output Buffer 放入 InputChannel 后，解除上游 Task 的阻塞
   - 新生成的 Buffer 无需过滤，直接放入 InputChannel

4. **Checkpoint 触发逻辑**
   - 在 Recovery 阶段允许触发 Checkpoint
   - **Phase 1 期间的 Checkpoint 阻塞**：如果在 Phase 1（S3 Active Loop）期间触发 Checkpoint，必须等待所有 S3 数据过滤完成后 Checkpoint 才能完成
   - Phase 2 进入后，表示已准备好进行 Checkpoint

### 收益

- Checkpoint 可以在 Recovery 阶段触发
- 保证恢复的 Buffer 在新数据之前被处理
- 确保 Checkpoint 完整性

---

## 开发顺序建议

### 首版/POC（必需任务）

1. **Task 2** (过滤逻辑) - 核心功能，实现 P1 路径
2. **Task 4** (控制面变更) - 可与 Task 2 并行开发

### 后续优化

3. **Task 3** (Spill 逻辑) - 可选优化，处理 Network Memory 不足场景

---

## 参考

- [FLIP-547 Wiki](https://cwiki.apache.org/confluence/display/FLINK/FLIP-547%3A+Support+checkpoint+during+recovery)
- [FLINK-35761](https://issues.apache.org/jira/browse/FLINK-35761)
- [原始需求文档](./requirement.md)
