# FLIP-547 任务拆分文档

## 概述

本文档将 FLIP-547 (Support Checkpoint During Recovery) 的完整需求拆分为 5 个任务。

**原始需求文档**: [requirements/requirement.md](./requirement.md)

## 任务拆分背景

FLIP-547 的核心目标是支持在 Recovery 阶段触发 Checkpoint，以解决以下问题：
- Recovery 阶段可能持续数小时，期间无法触发 Checkpoint
- 重启或扩缩容会丢失已恢复的进度
- 上游系统被阻塞，可能导致级联故障

为实现这一目标，需要解决的核心挑战是：**确保 Exactly-Once 语义**，特别是在多次扩缩容场景下避免数据重复。

解决方案的核心思路是：**在 Checkpoint 前过滤并重组 Buffer，确保每条记录只被快照一次**。

基于此，我们将实现拆分为以下任务：

| 任务 | Jira ID | 关注点 | 状态 | 优先级 |
|------|---------|--------|------|--------|
| Task 1 | [FLINK-38542](https://issues.apache.org/jira/browse/FLINK-38542) | 数据路径变更（Buffer 在哪里恢复） | ✅ 已合并 (commit 686c00f8) | 必需 |
| Task 2 | [FLINK-38930](https://issues.apache.org/jira/browse/FLINK-38930) | 核心过滤机制 | 🔍 社区 Review 中 | 必需 |
| Task 3 | [FLINK-39018](https://issues.apache.org/jira/browse/FLINK-39018) | LocalInputChannel Snapshot 支持 | 🔍 社区 Review 中 | 必需 |
| Task 4 | [FLINK-38543](https://issues.apache.org/jira/browse/FLINK-38543) | 控制面变更（生命周期、Checkpoint 触发） | 🔍 社区 Review 中 | 必需 |
| Task 5 | [FLINK-38544](https://issues.apache.org/jira/browse/FLINK-38544) | 内存压力处理 | ✅ POC 完成（LazyFileBuffer），整体首版跳过 | 可选（优化） |

**相关 Commits（按任务分组，commit ID 可能因 rebase 变化，以 message 为准）:**

**Task 1 (FLINK-38542):**
- [FLINK-38542][checkpoint] Recover output buffers of upstream task on downstream task side directly
- [FLINK-38542][checkpoint] Randomize UNALIGNED_ALLOW_ON_RECOVERY for testing

**Task 2 (FLINK-38930) 前置重构:**
- [hotfix][runtime] Extract RecordFilter as the interface
- [hotfix] Extract VirtualChannel as the public class
- [FLINK-38541][checkpoint] Introducing config option: execution.checkpointing.unaligned.during-recovery.enabled
- [FLINK-38541][checkpoint] Randomize UNALIGNED_DURING_RECOVERY_ENABLED for testing

**Task 2 (FLINK-38930) 核心:**
- [FLINK-38930][checkpoint] Filtering record before processing without spilling strategy

**Task 3 (FLINK-39018):**
- [hotfix][network] Fix LocalInputChannel.getBuffersInUseCount to include toBeConsumedBuffers
- [FLINK-39018][checkpoint] Support LocalInputChannel checkpoint snapshot for recovered buffers
- [FLINK-39018][network] Fix LocalInputChannel priority event and buffer availability for recovered buffers

**Task 4 (FLINK-38543):**
- [FLINK-38543][network] Buffer migration from RecoveredInputChannel to physical channels
- [FLINK-38543][checkpoint] Fix Mailbox loop interrupted before recovery finished
- [FLINK-38543][checkpoint] Introduce bufferFilteringCompleteFuture for earlier RUNNING state transition
- [FLINK-38543][checkpoint] Change overall UC restore process for checkpoint during recovery
- [FLINK-39018][checkpoint] Notify PriorityEvent to downstream task even if it is blocked to ensure the checkpoint barrier can be handled by downstream task

**Task 5 (FLINK-38544，首版跳过):**
- [FLINK-38544][checkpoint] Use heap buffer as simplified spilling logic during recovery
- [hotfix] 引入 LazyFileBuffer 优化 checkpoint 恢复内存使用

**测试相关:**
- [FLINK-39140][test] Allow multiple rescales in Unaligned Checkpoint ITCases to perform checkpointing during recovery
- [FLINK-39140][test] Disable CUSTOM_PARTITIONER in unaligned checkpoint it case since it does not work well
- [FLINK-39140][test] Fix MAX_RETAINED_CHECKPOINTS not effective in UnalignedCheckpointRescaleWithMixedExchangesITCase
- [FLINK-39140][test] Change record type from Long to String in UnalignedCheckpointRescaleWithMixedExchangesITCase
- [hotfix][runtime] Including task name and subtask index into channel-state-unspilling thread name

## 任务依赖关系

```
Task 1 (✅ 已合并)
    │
    ├──→ Task 2 (🔍 社区 Review 中)
    │        │
    │        └──→ Task 3 (🔍 社区 Review 中) ──────┐
    │                                              │
    └──→ Task 4 (🔍 社区 Review 中) ───────────────┼──→ 首版开发完成，等待社区 Review
                                                   │
                                                   └──→ Task 5 (✅ POC 完成，首版跳过，后续优化)
```

---

## Task 1: Recover Output Buffers on Downstream Task Side Directly (FLINK-38542)

**Jira**: [FLINK-38542](https://issues.apache.org/jira/browse/FLINK-38542)
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

## Task 2: Filtering Records in Async Thread (FLINK-38930)

**Jira**: [FLINK-38930](https://issues.apache.org/jira/browse/FLINK-38930)
**状态**: 🔍 已提交 PR，社区 Review 中

### 职责

在异步线程中过滤恢复的 Buffer，将原始 Buffer 转换为过滤后的 Buffer。

**核心路径**: Original Buffers → Filtered Buffers

### 核心变更

1. **过滤逻辑实现**
   - 在 Channel-state-unspilling 线程中执行过滤
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

### 与 Task 3 的关系

过滤后的 Buffer 最初存放在 RecoveredInputChannel 中。当 RecoveredInputChannel 转换为物理 Channel 后，需要 Task 3 的工作使 LocalInputChannel 支持快照这些 Buffer。

### 收益

- Task 线程可以尽早开始处理数据（第一个过滤后的 Buffer 生成后即可开始）
- 过滤后的 Buffer 只包含当前 Subtask 需要的记录，解决了数据膨胀问题
- 确保 Exactly-Once 语义

---

## Task 3: LocalInputChannel Snapshot 支持 (FLINK-39018)

**Jira**: [FLINK-39018](https://issues.apache.org/jira/browse/FLINK-39018)
**状态**: 🔍 已提交 PR，社区 Review 中

**详细设计文档**: [task3_local_input_channel_snapshot.md](./task3_local_input_channel_snapshot.md)

### 职责

当 RecoveredInputChannel 转换为 LocalInputChannel 后，使 LocalInputChannel 能正确支持 Unaligned Checkpoint 的快照和事件处理。这包括将已过滤但未消费的 Buffer 迁移到 LocalInputChannel，并确保 Checkpoint 流程的正确性。

### 核心变更

1. **Checkpoint 快照支持** - LocalInputChannel 需要在 Checkpoint 时正确快照从 RecoveredInputChannel 迁移过来的 Buffer，原始实现不会快照这些 Buffer
2. **Priority Event 优先处理** - 当 LocalInputChannel 中有未消费的 recovered buffer 时，Checkpoint Barrier 等优先级事件仍能被优先处理，不被 recovered buffer 阻塞
3. **Buffer 可用性修正** - 确保 recovered buffer 消费完毕后能正确衔接 subpartitionView 的数据，避免 Task 线程误认为没有数据而停止消费

### 收益

- 物理 Channel 能正确快照 migrated buffer，保证 Checkpoint 完整性
- Checkpoint Barrier 不会被 recovered buffer 阻塞，保证 Barrier 时效性
- Buffer 消费顺序和可用性正确，数据不会丢失

---

## Task 4: Change the Overall UC Restore Process (FLINK-38543)

**Jira**: [FLINK-38543](https://issues.apache.org/jira/browse/FLINK-38543)
**状态**: 🔍 已提交 PR，社区 Review 中

**详细设计文档**: [task4_design.md](./task4_design.md)

### 职责

变更整体的 UC 恢复流程，包括 Checkpoint 触发时机、Task Snapshot 等待逻辑、上游阻塞机制等。

### 核心变更概述

1. **RUNNING 转换时机变更** - Task 在 Buffer 过滤完成后即可进入 RUNNING（无需等待数据处理完成），Checkpoint 触发逻辑不变
2. **Task Snapshot 等待逻辑** - 每个 Task 在 Snapshot 时等待其 Buffer 过滤完成
3. **Block/Unblock 上游 Task** - 阻塞上游直到恢复完成

### 收益

- Checkpoint 可以在 Recovery 阶段更早触发
- 保证恢复的 Buffer 在新数据之前被处理
- 确保 Checkpoint 完整性

---

## Task 5: 内存压力处理 (FLINK-38544)

**Jira**: [FLINK-38544](https://issues.apache.org/jira/browse/FLINK-38544)
**状态**: ✅ POC 完成（LazyFileBuffer），整体首版跳过

**优先级**: 可选（优化项，首版可跳过）

### 为什么 Task 5 是可选的

Task 5 是一个**优化项**，而非正确性必需：

| 场景 | 无 Task 5 | 有 Task 5 |
|------|-----------|-----------|
| Network Memory 充足 | ✓ 正常工作 | ✓ 正常工作 |
| Network Memory 不足 | ✓ 可工作（Checkpoint 延迟完成） | ✓ 可工作（Checkpoint 更快完成） |
| 正确性保证 | ✓ 保证 | ✓ 保证 |

**无 Task 5 时的行为：**
- 当 Network Memory 充足时：Task 2 的 P1 路径正常工作，无任何问题
- 当 Network Memory 不足时：过滤线程阻塞等待 Buffer 可用，Checkpoint 完成时间延迟（必须等待所有 S3 数据过滤完成）
- **仍然优于现有逻辑**：现有逻辑在 Recovery 阶段完全无法触发 Checkpoint

因此，首版/POC 可以仅实现 Task 2 + Task 3 + Task 4，Task 5 作为后续优化项。

### 职责

当 Network Memory 不足时，提供 fallback 机制避免过滤线程阻塞或死锁，并确保 Checkpoint 能够尽早触发。

### 核心变更

1. **LazyFileBuffer（✅ POC 已完成）**
   - 当 Buffer Pool 耗尽时，使用文件后备 Buffer 避免 deadlock
   - 写入阶段数据写入临时文件，读取时才加载到内存
   - 解决了过滤线程阻塞式申请 buffer 导致的死锁问题

2. **Spill 路径（P2: S3-To-Disk-Spill Path，待实现）**
   ```
   S3 → Filter → Local Disk
   ```
   - 当 Network Buffer 不可用时，仍然从 S3 读取并过滤数据
   - 将过滤后的结果写入本地磁盘
   - 确保过滤工作持续进行，不被 Buffer 不足阻塞

3. **Replay 路径（P3: Disk-To-Memory Path，待实现）**
   ```
   Local Disk → Network Buffer → Input Channel
   ```
   - 当 Network Buffer 可用时，优先从本地磁盘读取已过滤的数据
   - 将数据放入 Input Channel

4. **Checkpoint 期间的磁盘数据处理（待实现）**
   - Checkpoint 时需要上传 Network Buffer 和 Local Disk 中的所有过滤后的 Buffer

### 收益

- 避免 Network Memory 不足时的死锁（LazyFileBuffer 已解决）
- 即使 Network Buffer 不足，Checkpoint 也能触发（过滤工作不被阻塞）
- 过滤后的数据可以从磁盘上传到 Checkpoint Storage

---

## 开发顺序建议

### 首版/POC（必需任务）

1. ~~**Task 2** (过滤逻辑)~~ - 🔍 社区 Review 中
2. ~~**Task 3** (LocalInputChannel Snapshot 支持)~~ - 🔍 社区 Review 中
3. ~~**Task 4** (控制面变更)~~ - 🔍 社区 Review 中

### 后续优化

4. ~~**Task 5** (内存压力处理)~~ - ✅ POC 完成（LazyFileBuffer），完整 Spill/Replay 逻辑首版跳过

---

## 参考

- [FLIP-547 Wiki](https://cwiki.apache.org/confluence/display/FLINK/FLIP-547%3A+Support+checkpoint+during+recovery)
- [FLINK-35761](https://issues.apache.org/jira/browse/FLINK-35761)
- [原始需求文档](./requirement.md)
