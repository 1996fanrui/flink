# Flink 恢复期增量检查点方案 (Project "Recovery-Checkpoint")

这是我们为 Flink 设计的一个支持在 Unaligned Checkpoint (UCX) 恢复期间进行 Exactly-once 增量检查点的详细技术方案。本文档记录了最终方案以及方案的演进思路，以备后续查阅。

## 核心原则

1.  **无元数据依赖链**: 任何恢复期检查点 (`chk-N`) 的元数据都直接指向最原始的、包含真实数据的数据检查点 (`chk-1`)，并记录相对于 `chk-1` 的**绝对消费进度**。这确保了恢复时间是 O(1) 的。
2.  **混合状态兼容**: 方案必须能处理在同一次 Checkpoint 中，部分 Subtask 已完成恢复（进入实时数据处理），而另一部分仍在恢复历史数据的“混合状态”。
3.  **基于现有代码结构**: 所有新设计都必须与 Flink `main` 分支的代码结构兼容，通过实现现有接口和新增可共存的数据结构来实现，而非修改核心数据结构。
4.  **Exactly-once 语义**: 方案必须在所有场景（包括 Rescale）下保证端到端的 Exactly-once 语义。

---

## 模块一：核心数据结构 (基于 Flink 现有代码)

我们将在 `org.apache.flink.runtime.state` 包下引入以下新的、可序列化的类。

### 1. `ConsumptionProgress.java`

用于精确描述在一个逻辑 Buffer 流中的消费点。

```java
// package org.apache.flink.runtime.state;
public final class ConsumptionProgress implements Serializable, Comparable<ConsumptionProgress> {
    private final int bufferIndex; // Buffer 在逻辑序列中的索引
    private final int offsetInBuffer; // Buffer 内部的字节偏移量
    // ... 构造函数, getters, compareTo, equals/hashCode ...
}
```

### 2. `RecoveryContinuationHandle.java`

一个特殊的 `StreamStateHandle` 实现，仅包含元数据。

```java
// package org.apache.flink.runtime.state;
public final class RecoveryContinuationHandle implements StreamStateHandle {
    private final long originalCheckpointId; // 指向 chk-1
    private final ResultPartitionID originalPartitionId; // chk-1 中的具体分区 ID
    private final Map<KeyGroupRange, ConsumptionProgress> absoluteKeyGroupRangeProgress; // 核心进度图
    // ... 构造函数, getters ...
}
```

### 3. `ContinuingInputChannelStateHandle.java`

恢复期检查点的核心状态对象，实现了 `InputStateHandle` 接口。

```java
// package org.apache.flink.runtime.state;
public final class ContinuingInputChannelStateHandle implements InputStateHandle {
    private final InputChannelInfo info;
    private final RecoveryContinuationHandle delegate;
    // ... 构造函数, getters ...
}
```

---

## 模块二：Task 状态机与核心流程

### 1. `StreamTask` 状态机

`StreamTask` 需要引入一个明确的状态机来管理恢复过程：
*   `CREATED`: 任务已创建。
*   `RESTORING`: 任务正在从一个 Checkpoint 恢复。在此状态下制作的 Checkpoint 是“恢复期检查点”。
*   `RUNNING`: 任务已完成**所有**恢复数据的消费，进入纯实时数据处理阶段。在此状态下制作的 Checkpoint 是标准的 UCX Checkpoint。
*   `FINISHED` / `CANCELED` / `FAILED`: 终态。

### 2. 恢复流程 (从 `chk-N` 启动)

#### 2.1. `StreamTask` 初始化

*   `StreamTask` 收到 `chk-N` 的 `TaskStateSnapshot`，进入 `RESTORING` 状态。
*   它遍历 `OperatorSubtaskState` 中的 `inputChannelState` (`StateObjectCollection<InputStateHandle>`)。
*   **分发逻辑**:
    *   如果 `handle` 是 `ContinuingInputChannelStateHandle`: 创建 `RestoringInputChannel`，并通过 RPC 向 JM 获取 `chk-1` 的物理文件句柄，连同 `chk-N` 的元数据一起交给它初始化。
    *   如果 `handle` 是 `InputChannelStateHandle`: 创建 `RecoveredInputChannel`，让它读取 `chk-N` 中的 Buffer 数据。

#### 2.2. `RestoringInputChannel` 的工作 (最终的、正确的 Exactly-once 逻辑)

`RestoringInputChannel` 是保证 Exactly-once 的核心，其工作流程如下：

*   **初始化阶段 (加载全局知识，并进行局部优化)**:
    1.  加载从 `chk-N` 传来的**完整** `absoluteKeyGroupRangeProgress` Map，作为**只读的**历史进度图 `historyProgressMap`。**加载全部历史仍然是必要的**，因为当前 Subtask 在物理上仍然可能读到不属于自己的数据，它需要完整的历史图来对这些“路过”的数据执行正确的防重过滤���
    2.  **【关键优化】** 遍历 `historyProgressMap`，但**只考虑**那些与当前 Subtask 负责的 `targetKeyGroupRange` 有交集的 Key Group Range。
    3.  在这些相关的 Range 中，找到进度的**局部最小值**，记为 `startProgress`。这是本次恢复的起始点。这个优化是安全的，因为元数据的强语义保证了属于当前 Subtask 的数据不可能出现在其局部最小进度之前。
    4.  打开 `chk-1` 的物理文件，并精确定位到 `startProgress` 指定的位置。

*   **消费与处理循环 (双重过滤)**:
    1.  从 `startProgress` 的位置开始，顺序地反序列化下一条记录。
    2.  对于这条记录，获取其物理位置 `recordProgress` 和逻辑归属 `recordKeyGroup`。
    3.  **执行第一道过滤 (防重过滤)**:
        *   在**完整**的 `historyProgressMap` 中，查找 `recordKeyGroup` 对应的历史进度 `historyProgressForKeyGroup`。
        *   **比较**: `if (recordProgress.compareTo(historyProgressForKeyGroup) < 0)` -> **丢弃** (该记录已被处理并快照)。
    4.  **执行第二道过滤 (归属过滤)**:
        *   如���记录通过了第一道过滤，再判断 `recordKeyGroup` 是否在当前 Subtask 负责的 `targetKeyGroupRange` 内。
        *   **如果在**: **递交给算子处理**。
        *   **如果不在**: **丢弃**。

*   **完成**: 当 `RestoringInputChannel` 读到 `chk-1` 对应文件的末尾，它的历史使命完成。

### 3. 快照流程 (制作 `chk-N+1`)

#### 3.1. 快照贡献原则 (只贡献自己的部分)

在制作快照时，每个 `RestoringInputChannel` 只需上报它自己所负责的 `targetKeyGroupRange` 内的消费进度。它不需要、也不应该包含其他不相关 Key Group Range 的进度信息。这保证了快照的上报过程是轻量且高效的。全局的进度图由 `JobManager` 在收集完所有 Subtask 的部分贡献后，统一组装而成。

#### 3.2. `CheckpointedInputGate` 协调

*   如果 `channel` 是 `RestoringInputChannel`: 返回一个 `ContinuingInputChannelStateHandle`，其中包含指向 `chk-1` 的引用，以及一张**只包含当前 Subtask 所负责的 Key Group Range** 的最新绝对进度图（**部分进度**）。
*   如果 `channel` 是 `RemoteInputChannel` (已完成恢���): 执行标准 UCX 快照，返回一个标准的 `InputChannelStateHandle`。

#### 3.3. `JobManager` 组装 (全局视图)

*   `CheckpointCoordinator` 收到所有 Subtask 提交的、可能包含两种不同类型 Handle 的 `InputStateHandle` 集合。
*   它将所有 `ContinuingInputChannelStateHandle` 中的**部分进度图**进行**合并 (Map union)**，形成一张描述 `chk-N+1` 时刻**全局**状态的、完整的 `absoluteKeyGroupRangeProgress` 图。
*   它将这个合并后的完整进度图，连同其他 Handle，一起存入 `chk-N+1` 的最终元数据中。

---

## 模块三：JobManager 的职责

1.  **提供 RPC 服务**: `CheckpointCoordinator` 必须实现 `getOriginalChannelStateHandle` 接口，以便 `TaskManager` 能够根据 `checkpointId` 和 `partitionId` 查询到原始数据文件的 `StreamStateHandle`。
2.  **垃圾回收 (GC)**: `CompletedCheckpointStore` 的 GC 逻辑必须能够理解 `RecoveryContinuationHandle` 中对 `originalCheckpointId` 的引用。一个 Checkpoint (`chk-1`) 只有在没有��何其他 Checkpoint 的元数据引用它时，才能被删除。
3.  **切断恢复链**: 当一个任务的状态从 `RESTORING` 转换到 `RUNNING` 后，`JobManager` 应在下一次触发一个**标准的、完全的 UCX Checkpoint**。这个新的 Checkpoint 将不再包含任何 `ContinuingInputChannelStateHandle`，从而“固化”了所有恢复进度，切断了对 `chk-1` 的依赖，允许 `chk-1` 在未来被正常 GC。

---

## 模块四：设计演进与优化思路（重要上下文）

1.  **为什么不用 Bitmap 方案?**
    *   **状态膨胀**: 对于海量小记录的场景，记录级的位图本身可能变得非常大。
    *   **分布式合并难题 (致命缺陷)**: 多个 Subtask 分布在不同 TM 上，要低延迟、高并发地维护一个共享的、全局一致的位图，需要极其复杂的分布式协调机制，在工程上不具备可行性。

2.  **为什么不用“元数据依赖链”方案?**
    *   **问题**: 即 `chk-N` 的元数据只引用 `chk-(N-1)`。
    *   **风险**: 导致恢复时间随链长 O(N) 增长（需要依次解析所有元数据并合并进度），GC 逻辑复杂，且链上��一环节元数据损坏都会导致全链失败。
    *   **结论**: “无链方案”（所有恢复期 CP 都直接引用 `chk-1`）是唯一能保证 O(1) 恢复时间的方案。

3.  **为什么快照时“只贡献部分”，恢复时“加载全部”？**
    *   **快照时只贡献部分**: 这是关键的性能优化。每个 Subtask 只需上报自己负责的进度，极大减少了网络和 JM 的负担。JM 负责最后的全景图组装。
    *   **恢复时加载全部**: 这是保证 Exactly-once 的前提。每个 Subtask 必须拥有全局视野，才能对流经自己的、不属于自己的数据做出正确的“跳过”判断。

4.  **【关键澄清】恢复时的起始点优化**
    *   **最终结论**: 恢复任务可以安全地从其**局部最小进度**开始读取，而无需从全局最小进度开始。
    *   **原因**: Checkpoint 元数据的强语义 (`Progress(KG_X) = Offset_Y` 意味着 `Offset_Y` 之前没有 `KG_X` 的数据) 保证了属于当前 Subtask 的数据不可能出现在其局部最小进度之前。
    *   **效果**: 这个优化显著减少了恢复期间不必要的 I/O，提升了恢复���度，同时不破坏 Exactly-once 语义。
