# FLIP-547 Commit 重组方案

## 最终 Commit 序列

| ID | Jira ID | Commit 标题 | SHA | 涉及文件 |
|----|---------|------------|-----|----------|
| A | [hotfix] | Extract RecordFilter as the interface | `206503d` | 5 files |
| B | [hotfix] | Extract VirtualChannel as the public class | `1c930fe` | 2 files |
| C | [hotfix] | Including task name and subtask index into channel-state-unspilling thread name | `a80b0b6` | 1 file |
| D | [FLINK-38541][checkpoint] | Introducing config option: execution.checkpointing.unaligned.during-recovery.enabled | `2512f0f` | 1 file |
| E | [FLINK-38541][checkpoint] | Randomize UNALIGNED_DURING_RECOVERY_ENABLED for testing | `ca4d67e` | 1 file |
| F | [FLINK-38930][checkpoint] | Filtering record before processing without spilling strategy | `f8dda87` | 13 files |
| G | [hotfix] | Fix LocalInputChannel.getBuffersInUseCount to include toBeConsumedBuffers | `a61e52c` | 1 file |
| H | [FLINK-39018][checkpoint] | Support LocalInputChannel checkpoint snapshot for recovered buffers | `9c37ab0` | 1 file |
| I | [FLINK-39018][network] | Fix LocalInputChannel priority event and buffer availability for recovered buffers | `f62a733` | 1 file |
| J | [FLINK-38543][network] | Buffer migration from RecoveredInputChannel to physical channels | `4997c85` | 11 files |
| K | [FLINK-38543][checkpoint] | Fix Mailbox loop interrupted before recovery finished | `f017dee` | 1 file |
| L | [FLINK-38543][checkpoint] | Introduce bufferFilteringCompleteFuture for earlier RUNNING state transition | `e3ccea5` | 2 files |
| M | [FLINK-38543][checkpoint] | Change overall UC restore process for checkpoint during recovery | `1d52a83` | 12 files |
| N | [FLINK-38544][network] | Introduce LazyFileBuffer for checkpoint recovery memory optimization | `1cd2d1e` | 3 files |
| O | [FLINK-38544][checkpoint] | Replace blocking buffer request with LazyFileBuffer fallback | `0461f32` | 3 files |
| P | [hotfix] | Add FLIP-547 design documents, requirements, demo job, and tools | `b9ade4c` | 25 files |

---

## 按 Task 分组视图

| Task | Commits | 说明 |
|------|---------|------|
| 前置重构 | A, B, C | hotfix 重构 + 线程名 |
| 前置配置 | D, E | config option + test randomize |
| Task 2 (FLINK-38930) | F | 过滤核心 + partial check + Union fix |
| 前置 bugfix | G | LocalInputChannel.getBuffersInUseCount 修复 |
| Task 3 (FLINK-39018) | H, I | LocalInputChannel checkpoint snapshot + shuffle 逻辑修正 |
| Task 4 (FLINK-38543) | J, K, L, M | Buffer 迁移 + Mailbox fix + future 基础设施 + 控制面变更 |
| Task 5 (FLINK-38544) | N, O | LazyFileBuffer 引入 + 替换原有阻塞逻辑 |
| 文档 | P | 设计文档、需求、demo job、工具（社区提交时删除） |

**关键原则**：Task 5 (N, O) 放在最后。去掉 Task 5 后，Task 2/3/4 使用原始的阻塞式 `requestBufferBlocking()` 逻辑仍能正常工作（buffer 不足时阻塞等待——只是 Checkpoint 完成时间延迟）。

---

## 各 Commit 详细内容

### A - [hotfix] Extract RecordFilter as the interface

从 `DemultiplexingRecordDeserializer` 中提取 `RecordFilter` 接口。

### B - [hotfix] Extract VirtualChannel as the public class

从 `DemultiplexingRecordDeserializer` 中提取 `VirtualChannel` 类。

### C - [hotfix] channel-state-unspilling thread name

线程名中加入 task name 和 subtask index，方便调试。

### D - [FLINK-38541][checkpoint] Introducing config option

引入 `execution.checkpointing.unaligned.during-recovery.enabled` 配置项。

### E - [FLINK-38541][checkpoint] Randomize for testing

测试环境中随机化新配置项。

### F - [FLINK-38930][checkpoint] Filtering record before processing without spilling strategy

Task 2 核心：在异步线程中过滤恢复的 Buffer。使用阻塞式 `requestBufferBlocking()` 申请 buffer。
- `ChannelStateFilteringHandler` 含 per-gate `GateFilterHandler`
- `RecordFilterContext` 含 `VirtualChannelRecordFilterFactory`
- `SequentialChannelStateReaderImpl` 增加 `hasPartialData()` check
- 修复 Union downscale 场景：`RecordFilterContext` 改为按 gateIndex 索引，`StreamTask.createRecordFilterContext()` 遍历 inEdges

### G - [hotfix] Fix LocalInputChannel.getBuffersInUseCount

修复 `getBuffersInUseCount()` 未计入 `toBeConsumedBuffers` 中的 buffer 数量。

### H - [FLINK-39018][checkpoint] Support LocalInputChannel checkpoint snapshot for recovered buffers

- `LocalInputChannel.checkpointStarted()` 收集 `toBeConsumedBuffers` 中的 buffer 作为 inflight buffer 持久化
- 将 `checkForBarrier`/`maybePersist` 提前到 buffer 获取点（FullyFilledBuffer 处理之前），确保 checkpoint 状态时序正确

### I - [FLINK-39018][network] Fix LocalInputChannel priority event and buffer availability for recovered buffers

两个 shuffle 层面的问题修复：
- **Priority Event 优先处理** - `notifyPriorityEvent()` 设置标志，`getNextBuffer()` 优先从 subpartitionView 获取 priority event，确保 Checkpoint Barrier 不被 recovered buffer 阻塞
- **Buffer 可用性修正** - 最后一个 recovered buffer 消费完毕后动态检查 subpartitionView 是否有数据，避免 Task 线程误认为没有数据

### J - [FLINK-38543][network] Buffer migration from RecoveredInputChannel to physical channels

当 RecoveredInputChannel 转换为物理 Channel 时，将已过滤但未消费的 buffer 迁移到目标 Channel。
- `RecoveredInputChannel.toInputChannel()`: 提取剩余未消费 buffer
- `toInputChannelInternal(ArrayDeque<Buffer>)`: 签名变更，传递剩余 buffer
- `LocalInputChannel`: 构造器接收 `initialRecoveredBuffers`，迁移 buffer 到 `toBeConsumedBuffers`
- `SingleInputGate.convertRecoveredInputChannels()`: synchronized 转换 + 新 channel 入队
- `RemoteInputChannel`: 构造器增加 `initialRecoveredBuffers`、buffer 迁移逻辑
- 级联改动：`UnknownInputChannel`、`InputChannelBuilder`、benchmark factory、netty test

### K - [FLINK-38543][checkpoint] Fix Mailbox loop interrupted before recovery finished

返回 `allRecoveredFuture` 而非 `thenRun` 的结果。`thenRun` 返回的新 future 在 `suspend()` 完成后才 complete，但 mailbox loop 可能在 `suspend()` 返回前就退出了。

### L - [FLINK-38543][checkpoint] Introduce bufferFilteringCompleteFuture

Task 4 前置基础设施：
- `RecoveredInputChannel`: `bufferFilteringCompleteFuture` 字段、getter、`finishReadRecoveredState()` 中 complete
- `SingleInputGate`: `isUnalignedDuringRecoveryEnabled` flag + setter/getter、`getBufferFilteringCompleteFuture()` 聚合

### M - [FLINK-38543][checkpoint] Change overall UC restore process

Task 4 核心控制面变更：
- `StreamTask`: `useBufferFilteringFuture` 逻辑、`setUnalignedDuringRecoveryEnabled` 设置、`requestPartitionsTrigger` 选择
- `InputGate`: `getBufferFilteringCompleteFuture()` 抽象方法
- `IndexedInputGate`: `setUnalignedDuringRecoveryEnabled()` 抽象方法
- `InputGateWithMetrics`: 委托两个新方法
- `UnionInputGate`: `getBufferFilteringCompleteFuture()` 聚合
- Mock 适配 + 测试

### N - [FLINK-38544][network] Introduce LazyFileBuffer

引入 `LazyFileBuffer` 类、`ChannelStateSerializer` 扩展、单元测试。纯新增代码，不修改已有逻辑。

### O - [FLINK-38544][checkpoint] Replace blocking buffer request with LazyFileBuffer fallback

用 LazyFileBuffer 非阻塞策略替换 F-M 中的阻塞式 `requestBufferBlocking()` 逻辑：
- `RecoveredInputChannel`: `requestBufferBlocking()` → `requestBuffer()`（先尝试池中取 buffer，取不到则创建 LazyFileBuffer）
- `RecoveredChannelStateHandler`: `getBuffer()` 和 `recover()` 中处理 LazyFileBuffer
- `ChannelStateFilteringHandler.BufferSupplier` 接口：`requestBufferBlocking()` → `requestBuffer()` 返回 `BufferWithContext`

### P - [hotfix] Add FLIP-547 design documents, requirements, demo job, and tools

设计文档、需求文档、demo job、工具脚本。社区提交时此 commit 应删除。
