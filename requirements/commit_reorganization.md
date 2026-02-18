# FLIP-547 Commit 重组方案

## 最终 Commit 序列

| ID | Jira ID | Commit 标题 | 操作 | 来源 | 涉及文件 |
|----|---------|------------|------|------|----------|
| A | [hotfix] | Extract RecordFilter as the interface | 原样保留 | `206503d` | 5 files |
| B | [hotfix] | Extract VirtualChannel as the public class | 原样保留 | `1c930fe` | 2 files |
| C | [hotfix] | Including task name and subtask index into channel-state-unspilling thread name | 原样保留 | `a80b0b6` | 1 file |
| D | [FLINK-38541] | Introducing config option: execution.checkpointing.unaligned.during-recovery.enabled | 原样保留 | `2512f0f` | 1 file |
| E | [FLINK-38541] | Randomize UNALIGNED_DURING_RECOVERY_ENABLED for testing | 原样保留 | `ca4d67e` | 1 file |
| F | [FLINK-38930] | Filtering record before processing without spilling strategy | 原样保留 | `19b8960` | 13 files |
| G | [FLINK-38930] | Add partial data check after filtering | 需拆出 | `33dc50b` 中 `SequentialChannelStateReaderImpl` 部分 | 1 file |
| H | [FLINK-38930] | Fix RecordFilterContext for Union downscale scenario | 需拆出 | `5d223d3` 中 RecordFilterContext 相关部分（对应旧分支 `4f5ef2b`） | 3 files |
| I1 | [FLINK-39018] | Support LocalInputChannel checkpoint snapshot for recovered buffers | 需拆出 | `d6d1fbf` 中 LocalInputChannel.checkpointStarted 部分 | 1 file |
| I2 | [FLINK-39018] | Fix LocalInputChannel priority event and buffer availability for recovered buffers | 需拆出 | `d6d1fbf` 中 LocalInputChannel priority event + availability 部分 | 1 file |
| J | [FLINK-38543] | Buffer migration from RecoveredInputChannel to physical channels | 需拆出 | `d6d1fbf` 中 toInputChannel + `5d223d3` 中 RemoteInputChannel + 级联 | ~10 files |
| K | [FLINK-38543] | Fix Mailbox loop interrupted before recovery finished | 需拆出 | `5d223d3` 中 StreamTask 的 CompletableFuture 返回值修复（3 行） | 1 file |
| L | [FLINK-38543] | Introduce bufferFilteringCompleteFuture for earlier RUNNING state transition | 需拆出 | `d6d1fbf` 中 RecoveredInputChannel future + SingleInputGate flag/聚合部分 | 2 files |
| M | [FLINK-38543] | Change overall UC restore process for checkpoint during recovery | 需合并拆出 | `5d223d3` 中 StreamTask RUNNING 转换 + `33dc50b` 中 UnionInputGate + 抽象方法 + Mock + 测试 | ~12 files |
| N | [FLINK-38544] | Introduce LazyFileBuffer for checkpoint recovery memory optimization | 原样保留，需移位 | `6e9e0a7`（当前在最后，移到此位置） | 3 files |
| O | [FLINK-38544] | Replace blocking buffer request with LazyFileBuffer fallback | 需新建 | 合并 `d6d1fbf` + `5d223d3` 中 LazyFileBuffer 使用部分 | ~3 files |
| P | [hotfix] | Add FLIP-547 design documents, requirements, demo job, and tools | 需修改 | `cece419`（社区提交时删除或仅保留 demo job） | 24 files |

---

## 按 Task 分组视图

| Task | Commits | 说明 |
|------|---------|------|
| 前置重构 | A, B, C | hotfix 重构 + 线程名 |
| 前置配置 | D, E | config option + test randomize |
| Task 2 (FLINK-38930) | F, G, H | 过滤核心 + partial check + Union fix |
| Task 3 (FLINK-39018) | I1, I2 | LocalInputChannel snapshot + shuffle 逻辑修正 |
| Task 4 (FLINK-38543) | J, K, L, M | Buffer 迁移 + Mailbox fix + future 基础设施 + 控制面变更 |
| Task 5 (FLINK-38544) | N, O | LazyFileBuffer 引入 + 替换原有阻塞逻辑 |
| 文档 | P | 设计文档、需求、demo job、工具（社区提交时删除） |

**关键原则**：Task 5 (N, O) 放在最后。去掉 Task 5 后，Task 2/3/4 使用原始的阻塞式 `requestBufferBlocking()` 逻辑仍能正常工作（buffer 不足时阻塞等待——只是 Checkpoint 完成时间延迟）。

---

## 各 Commit 详细内容

### A - [hotfix] Extract RecordFilter as the interface `原样保留`

从 `DemultiplexingRecordDeserializer` 中提取 `RecordFilter` 接口。

### B - [hotfix] Extract VirtualChannel as the public class `原样保留`

从 `DemultiplexingRecordDeserializer` 中提取 `VirtualChannel` 类。

### C - [hotfix] channel-state-unspilling thread name `原样保留`

线程名中加入 task name 和 subtask index，方便调试。

### D - [FLINK-38541] Introducing config option `原样保留`

引入 `execution.checkpointing.unaligned.during-recovery.enabled` 配置项。

### E - [FLINK-38541] Randomize for testing `原样保留`

测试环境中随机化新配置项。

### F - [FLINK-38930] Filtering record before processing `原样保留`

Task 2 核心：在异步线程中过滤恢复的 Buffer，包括 `ChannelStateFilteringHandler`、`RecordFilterContext`、`VirtualChannelRecordFilterFactory` 等。使用阻塞式 `requestBufferBlocking()` 申请 buffer。

### G - [FLINK-38930] Add partial data check after filtering `需拆出`

- 来源：`33dc50b` 中 `SequentialChannelStateReaderImpl` 部分
- 内容：`SequentialChannelStateReaderImpl` 增加 `filteringHandler.hasPartialData()` check，确保过滤完成后没有残留的部分数据

### H - [FLINK-38930] Fix RecordFilterContext for Union downscale `需拆出`

- 来源：`5d223d3` 中 RecordFilterContext 相关改动（对应旧分支 `4f5ef2b`）
- 修复 Task 2 (F) 引入的 bug：`createRecordFilterContext()` 遍历 `inputs[]` 并用 `inEdges.get(i)` 取 partitioner，Union 场景下多个物理 gate 对应一个逻辑 input，索引不一致
- 内容：
  - `RecordFilterContext`: `List<InputFilterConfig>` → `InputFilterConfig[]`，inputIndex → gateIndex
  - `StreamTask.createRecordFilterContext()`: 重写为遍历 inEdges
  - `ChannelStateFilteringHandler`: `createFromContext()` gateIndex 适配，`gateHandlers` List → array

### I1 - [FLINK-39018] Support LocalInputChannel checkpoint snapshot for recovered buffers `需拆出`

- 来源：`d6d1fbf` 中 LocalInputChannel 的 snapshot 逻辑部分
- 核心问题：LocalInputChannel 原始实现在 `checkpointStarted()` 时不会快照从 RecoveredInputChannel 迁移过来的 buffer，导致 Checkpoint 数据不完整
- 内容：
  - `LocalInputChannel` 构造器接收 `initialRecoveredBuffers` 参数
  - `LocalInputChannel.checkpointStarted()` 收集 recovered buffer 作为 inflight buffer 持久化
  - `LocalRecoveredInputChannel.toInputChannelInternal()` 传递 remainingBuffers

### I2 - [FLINK-39018] Fix LocalInputChannel priority event and buffer availability for recovered buffers `需拆出`

- 来源：`d6d1fbf` 中 LocalInputChannel 的 shuffle 逻辑部分
- 两个 shuffle 层面的问题修复：
  - **Priority Event 优先处理** - `LocalInputChannel.notifyPriorityEvent()` 确保 Checkpoint Barrier 等优先级事件不被 recovered buffer 阻塞。当 LocalInputChannel 中有未消费的 recovered buffer 时，priority event 仍能被优先处理
  - **Buffer 可用性修正** - 确保 recovered buffer 消费完毕后能正确衔接 subpartitionView 的数据，避免 Task 线程误认为没有数据而停止消费

### J - [FLINK-38543] Buffer migration from RecoveredInputChannel to physical channels `需拆出`

- 来源：`d6d1fbf` 中 RecoveredInputChannel.toInputChannel + `5d223d3` 中 RemoteInputChannel 相关部分
- Buffer 迁移是 Task 4 控制面变更的一部分：当 RecoveredInputChannel 转换为物理 Channel 时，需要将已过滤但未消费的 buffer 迁移到目标 Channel。
- 内容：
  - `RecoveredInputChannel.toInputChannel()`: 提取剩余未消费 buffer，通道转换 readiness check
  - `toInputChannelInternal(ArrayDeque<Buffer>)`: 签名变更，传递剩余 buffer
  - `SingleInputGate.convertRecoveredInputChannels()`: 转换后入队改进
  - `RemoteInputChannel`: 构造器增加 `initialRecoveredBuffers`、buffer 迁移逻辑、`peekNextBufferSubpartitionIdInternal()` / `getNextBuffer()` 放宽 check
  - `RemoteRecoveredInputChannel.toInputChannelInternal(ArrayDeque)`: 传递 remainingBuffers
  - 级联改动：`UnknownInputChannel`、`InputChannelBuilder`、benchmark factory、netty test（构造器加 null 参数）

### K - [FLINK-38543] Fix Mailbox loop interrupted before recovery `需拆出`

- 来源：`5d223d3` 中 StreamTask 的 3 行修复
- 内容：返回 `allRecoveredFuture` 而非 `thenRun` 的结果。`thenRun` 返回的新 future 在 `suspend()` 完成后才 complete，但 mailbox loop 可能在 `suspend()` 返回前就退出了

### L - [FLINK-38543] Introduce bufferFilteringCompleteFuture `需拆出`

- 来源：`d6d1fbf` 中 Task 4 前置基础设施部分
- 内容：
  - `RecoveredInputChannel`: `bufferFilteringCompleteFuture` 字段、getter、`finishReadRecoveredState()` 中 complete
  - `SingleInputGate`: `isUnalignedDuringRecoveryEnabled` flag + setter/getter、`getBufferFilteringCompleteFuture()` 聚合

### M - [FLINK-38543] Change overall UC restore process `需合并拆出`

- 来源：`5d223d3` 中 Task 4 核心 + `33dc50b` 中 UnionInputGate
- 内容：
  - `StreamTask`: `useBufferFilteringFuture` 逻辑、`setUnalignedDuringRecoveryEnabled` 设置、`requestPartitionsTrigger` 选择
  - `InputGate`: `getBufferFilteringCompleteFuture()` 抽象方法
  - `IndexedInputGate`: `setUnalignedDuringRecoveryEnabled()` 抽象方法
  - `InputGateWithMetrics`: 委托两个新方法
  - `UnionInputGate`: `getBufferFilteringCompleteFuture()` 聚合（来自 `33dc50b`）
  - `CheckpointingOptions`: 格式化改动
  - Mock 适配：`MockIndexedInputGate`、`MockInputGate`、`AlignedCheckpointsMassiveRandomTest`
  - 测试：`RecoveredInputChannelTest`、`SingleInputGateTest`、`SingleInputGateBuilder`

### N - [FLINK-38544] Introduce LazyFileBuffer `原样保留，需移位`

- 来源：`6e9e0a7`（移到 Task 5 位置）
- 内容：引入 `LazyFileBuffer` 类、`ChannelStateSerializer` 扩展、单元测试
- 纯新增代码，不修改已有逻辑

### O - [FLINK-38544] Replace blocking buffer request with LazyFileBuffer fallback `需新建`

- 来源：合并 `d6d1fbf` 中 RecoveredInputChannel/RecoveredChannelStateHandler 的 LazyFileBuffer 使用 + `5d223d3` 中 ChannelStateFilteringHandler 的 BufferSupplier 重构
- 用 LazyFileBuffer 非阻塞策略替换 F-M 中的阻塞式 `requestBufferBlocking()` 逻辑
- 内容：
  - `RecoveredInputChannel`: `requestBufferBlocking()` → `requestBuffer()`（先尝试池中取 buffer，取不到则创建 LazyFileBuffer）
  - `RecoveredChannelStateHandler`: `getBuffer()` 和 `recover()` 中处理 LazyFileBuffer 的 `ChannelStateByteBuffer.wrap` 和 `wrapWithoutRecycle`
  - `ChannelStateFilteringHandler.BufferSupplier` 接口：`requestBufferBlocking()` → `requestBuffer()` 返回 `BufferWithContext`
  - `serializeToBuffers()` / `writeDataToBuffer()`: 通过 `ChannelStateByteBuffer.writeBytes` 写入

### P - [hotfix] Add FLIP-547 design documents, requirements, demo job, and tools `需修改`

- 来源：`cece419`
- 内容：设计文档、需求文档、demo job、工具脚本、CLAUDE.md 改动
- 处理方案：
  - 社区提交时此 commit 应删除（社区不需要 requirements/设计文档/工具脚本）
  - demo job 如果需要保留，独立为一个 commit

---

## 操作步骤

1. 新分支基于 `ca4d67e`（即 commit E）
2. cherry-pick `19b8960` → commit F（**保留阻塞式 buffer 申请**）
3. 从 `33dc50b` 拆出 `SequentialChannelStateReaderImpl` 改动 → commit G
4. 从 `5d223d3` 拆出 RecordFilterContext + StreamTask.createRecordFilterContext + ChannelStateFilteringHandler gateIndex 改动 → commit H
5. 从 `d6d1fbf` 拆出 LocalInputChannel.checkpointStarted snapshot 逻辑 → commit I1
6. 从 `d6d1fbf` 拆出 LocalInputChannel priority event + buffer availability 逻辑 → commit I2
7. 合并 `d6d1fbf` 中 RecoveredInputChannel.toInputChannel/SingleInputGate 转换 + `5d223d3` 中 RemoteInputChannel + 级联 null 参数 → commit J
8. 从 `5d223d3` 拆出 StreamTask CompletableFuture 返回值修复 → commit K
9. 从 `d6d1fbf` 拆出 RecoveredInputChannel future + SingleInputGate flag/聚合 → commit L
10. 合并 `33dc50b` UnionInputGate + `5d223d3` Task 4 核心 → commit M
11. cherry-pick `6e9e0a7` → commit N（LazyFileBuffer 类引入）
12. 合并 `d6d1fbf` + `5d223d3` 中 LazyFileBuffer 使用部分，替换阻塞式 buffer 申请 → commit O
13. 处理 `cece419` → commit P（社区提交时删除或仅保留 demo job）
