# Flink 恢复期增量检查点方案 (Project "Recovery-Checkpoint")

这是我们为 Flink 设计的一个支持在 Unaligned Checkpoint (UCX) 恢复期间进行 Exactly-once 增量检查点的详细技术方案。本文档记录了最终方案以及方案的演进思路，以备后续查阅。

## 核心原则

1.  **无元数据依赖链**: 任何恢复期检查点 (`chk-N`) 的元数据都直接指向最原始的、包含真实数据的数据检查点 (`chk-1`)，并记录相对于 `chk-1` 的**绝对消费进度**。这确保了恢复时间是 O(1) 的，避免了因依赖链过长导致的恢复时间线性增长和故障放大效应。
2.  **混合状态兼容**: 方案必须能处理在同一次 Checkpoint 中，部分 Subtask 已完成恢复（进入实时数据处理），而另一部分仍在恢复历史数据的“混合状态”。
3.  **基于现有代码结构**: 所有新设计都必须与 Flink `main` 分支的代码结构兼容，通过实现现有接口和新增可共存的数据结构来实现，而非修改核心数据结构��
4.  **Exactly-once 语义**: 方案必须在所有场景（包括 Rescale）下保证端到端的 Exactly-once 语义。

---

## 模块一：核心数据结构 (基于 Flink 现有代码)

我们将在 `org.apache.flink.runtime.state` 包下引入以下新的、可序列化的类。

### 1. `ConsumptionProgress.java`

用于精确描述在一个逻辑 Buffer 流中的消费点。它解决了仅用 `long` 型 offset 无法区分 Buffer 的问题。

```java
// package org.apache.flink.runtime.state;
public final class ConsumptionProgress implements Serializable, Comparable<ConsumptionProgress> {
    private final int bufferIndex; // Buffer 在逻辑序列中的索引
    private final int offsetInBuffer; // Buffer 内部的字节偏移量
    // ... 构造函数, getters, compareTo, equals/hashCode ...
}
```

### 2. `RecoveryContinuationHandle.java`

一个特殊的 `StreamStateHandle` 实现，仅包含元数据，不包含真实数据流。

```java
// package org.apache.flink.runtime.state;
public final class RecoveryContinuationHandle implements StreamStateHandle {
    private final long originalCheckpointId; // 指向 chk-1
    private final ResultPartitionID originalPartitionId; // chk-1 中的具体分区 ID
    private final Map<KeyGroupRange, ConsumptionProgress> absoluteKeyGroupRangeProgress; // 核心进度图
    // ... 构造函数, getters ...
    
    @Override
    public FSDataInputStream openInputStream() {
        throw new UnsupportedOperationException("Metadata-only handle.");
    }
}
```

### 3. `ContinuingInputChannelStateHandle.java`

恢复期检查点的核心状态对象，实现了 `InputStateHandle` 接口，可与标准的 `InputChannelStateHandle` 并存于同一个 `StateObjectCollection<InputStateHandle>` 集合中。

```java
// package org.apache.flink.runtime.state;
public final class ContinuingInputChannelStateHandle implements InputStateHandle {
    private final InputChannelInfo info; // 关联的 InputChannel，解决了早期设计中忽略此信息的问题
    private final RecoveryContinuationHandle delegate; // 包含元数据的代理
    // ... 构造函数, getters ...

    @Override
    public StreamStateHandle getDelegate() { return delegate; }
}
```

---

## 模块二：Task 状态机与核心流程

### 1. `StreamTask` 状态机

`StreamTask` 需要引入一个明确的状态机来管理恢复过程：
*   `CREATED`: 任务已创建。
*   `RESTORING`: 任务正在从一个 Checkpoint 恢复。在此状态下，它正在消费恢复数据（无论是来自 `chk-1` 还是 `chk-N`）。在此状态下制作的 Checkpoint 是“恢复期检查点”。
*   `RUNNING`: 任务已完成**所有**恢复数据的消费，正在处理纯实时的网络数据。在此状态下制作的 Checkpoint 是标准的 UCX Checkpoint。
*   `FINISHED` / `CANCELED` / `FAILED`: 终态。

**状态转换关键点**: `RESTORING` -> `RUNNING` 的转换发生在 `InputGate` 下的所有恢复通道 (`RestoringInputChannel`, `RecoveredInputChannel`) 都耗尽了它们的恢复数据，并被替换为标准的 `RemoteInputChannel` 之后。

### 2. 恢复流程 (从 `chk-N` 启动)

1.  **`StreamTask` 初始化**:
    *   `StreamTask` 收到 `chk-N` 的 `TaskStateSnapshot`，进入 `RESTORING` 状态。
    *   它遍历 `OperatorSubtaskState` 中的 `inputChannelState` (`StateObjectCollection<InputStateHandle>`)。
    *   **分发逻辑**:
        *   如果 `handle` 是 `ContinuingInputChannelStateHandle` (仍在恢复):
            1.  创建 `RestoringInputChannel`。
            2.  从 `handle` 中解析出 `RecoveryContinuationHandle` (元数据)。
            3.  向 `JobManager` 发起 RPC (`getOriginalChannelStateHandle(originalCheckpointId, originalPartitionId)`)，获取 `chk-1` 中对应的、包含真实数据的 `StreamStateHandle` (物理文件句柄)。
            4.  将**物理文件句柄**和**恢复进度元数据**都交给 `RestoringInputChannel` 初始化。
        *   如果 `handle` 是 `InputChannelStateHandle` (已完成恢复):
            1.  创建 `RecoveredInputChannel`。
            2.  将这个 `handle` 直接交给它，让它读取 `chk-N` 中对应的那个小文件里的 Buffer 数据。

2.  **`RestoringInputChannel` 的工作 (Exactly-once 核心)**:
    *   **初始化 (加载全局知识)**: 加载 `chk-N` 的**完整** `absoluteKeyGroupRangeProgress` 作为**只读的**历史进度图 `historyProgress`。根据 `historyProgress` 中所有进度的最小值，计算出全局起始点 (`minProgress`)。
    *   **读取**: 打开 `chk-1` 的物理文��，跳到 `minProgress` 指定的 `bufferIndex` 和 `offsetInBuffer`，开始读取。
    *   **精确跳过**: 对于解析出的每条记录，计算其位置 `currentProgress` 和所属的 `KeyGroup`。在 `historyProgress` 中查找该 `KeyGroup` 的历史进度 `historyProgressForKeyGroup`。如果 `currentProgress.compareTo(historyProgressForKeyGroup) < 0`，则**丢弃**该记录，因为它已被处理并快照。否则，处理该记录。

### 3. 快照流程 (制作 `chk-N+1`)

1.  **`CheckpointedInputGate` 协调**:
    *   遍历所有 `InputChannel`。
    *   如果 `channel` 是 `RestoringInputChannel`:
        *   调用其 `snapshotState()`。该方法返回一个 `ContinuingInputChannelStateHandle`。其内部的 `RecoveryContinuationHandle` 包含：
            *   指向 `chk-1` 的引用。
            *   一张**只包含当前 Subtask 所负责的 Key Group Range** 的最新绝对进度图。这是一个**部分的、自己贡献的**进度。
    *   如果 `channel` 是 `RemoteInputChannel` (已完成恢复):
        *   执行标准 UCX 快照，返回一个标准的 `InputChannelStateHandle`。

2.  **`JobManager` 组装 (全局视图)**:
    *   `CheckpointCoordinator` 收到所有 Subtask 提交的、可能包含两种不同类型 Handle 的 `InputStateHandle` 集合。
    *   它将所有 `ContinuingInputChannelStateHandle` 中的**部分进度图**进行**合并 (Map union)**，形成一张描述 `chk-N+1` 时刻**全局**状态的、完整的 `absoluteKeyGroupRangeProgress` 图。
    *   它将这个合并后的完整进度图，连同其他 Handle，一起存入 `chk-N+1` 的最终元数据中。

---

## 模块三：JobManager 的职责

1.  **提供 RPC 服务**: `CheckpointCoordinator` 必须实现 `getOriginalChannelStateHandle` 接口，以便 `TaskManager` 能够根据 `checkpointId` 和 `partitionId` 查询到原始数据文件的 `StreamStateHandle`。
2.  **垃圾回收 (GC)**: `CompletedCheckpointStore` 的 GC 逻辑必须能够理解 `RecoveryContinuationHandle` 中对 `originalCheckpointId` 的引用。一个 Checkpoint (`chk-1`) 只有在没有任何其他 Checkpoint 的元数据引用它时，才能被删除。
3.  **切断恢复链**: 当���个任务的状态从 `RESTORING` 转换到 `RUNNING` 后，`JobManager` 应在下一次触发一个**标准的、完全的 UCX Checkpoint**。这个新的 Checkpoint 将不再包含任何 `ContinuingInputChannelStateHandle`，从而“固化”了所有恢复进度，切断了对 `chk-1` 的依赖，允许 `chk-1` 在未来被正常 GC。

---

## 模块四：设计演进与优化思路（重要上下文）

1.  **为什么不用 Bitmap 方案?**
    *   **状态膨胀**: 对于海量小记录的场景，记录级的位图本身可能变得非常大。
    *   **分布式合并难题 (致命缺陷)**: 多个 Subtask 分布在不同 TM 上，要低延迟、高并发地维护一个共享的、全局一致的位图，需要极其复杂的分布式协调机制，在工程上不具备可行性。

2.  **为什么不用“元数据依赖链”方案?**
    *   **问题**: 即 `chk-N` 的元数据只引用 `chk-(N-1)`。
    *   **风险**: 导致恢复时间随链长 O(N) 增长（需要依次解析所有元数据并合并进度），GC 逻辑复杂，且链上任一环节元数据损坏都会导致全链失败。
    *   **结论**: “无链方案”（所有恢复期 CP 都直接引用 `chk-1`）是唯一能保证 O(1) 恢复时间的方案。

3.  **为什么快照时“只贡献部分”，恢复时“加载全部”？**
    *   **快照时只贡献部分**: 这是关键的性能优化。每个 Subtask 只需上报自己负责的进度，极大减少了网络和 JM 的负担。JM 负责最后的全景图组装。
    *   **恢复时加载全部**: 这是保证 Exactly-once 的前提。每个 Subtask 必须拥有全局视野，才能计算出正确的全局最小起始点（防止数据丢失），并对流经自己的、不属于自己的数据做出正确的“跳过”判断（防止数据重复）。
