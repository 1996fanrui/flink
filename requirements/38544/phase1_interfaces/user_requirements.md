# 用户需求 — Phase 1：公共接口与 sentinel 类型骨架

## 需求偏离

无。所有偏离均为"simplify_approach 文本示例不完整、本 phase 落地时补齐"性质，已在对应 REQ 注释中说明（如 REQ-AYII 中 `snapshotAndInsertBarriers` 增补 `long checkpointId` 参数；REQ-9MCR 中 `DiskSnapshot` 内含 `StartPos` 内部类供 Phase 4 填实），不属于对原始需求的偏离。

## 背景

FLINK-38544 的整体设计（[`simplify_approach/`](../simplify_approach/)）需要在 `channelIOExecutor`（unspilling thread）与 task thread 之间通过三个 Java 接口、一个 sentinel `Buffer`、一个 `DiskSnapshot` 类完成协作。Phase 1 的目标是**仅落骨架**：让接口与类型先存在、可编译、可被两侧（Phase 2 InputChannel 侧 / Phase 3 + 4 Spill 写盘读盘侧）作为依赖 import，使两侧可以**完全并行**展开。本 phase **不引入任何业务逻辑、不修改任何运行时行为**。

## 需求

- **REQ-AYII** 在 `org.apache.flink.runtime.checkpoint.channel` 包内新增 `RecoveryCheckpointTrigger` 接口，仅声明 `DiskSnapshot snapshotAndInsertBarriers(long checkpointId)`；javadoc 必须按 `simplify_approach/overview.md` §6.1 写明锁前置条件（调用方禁止持有 `SpillFileReader.lock`，实现内部自取）以及 4 步原子行为，并说明 `checkpointId` 用于构造塞入每个 channel 的 `RecoveryCheckpointBarrier` sentinel（注：原始 simplify_approach §6.1 文本省略了该参数，本设计在落地时补齐——见 design.md §3.1 的 driver 说明）。
- **REQ-43Q8** 在 `org.apache.flink.runtime.io.network.partition.consumer` 包内新增 `RecoverableInputChannel` 接口，声明 `onRecoveredStateBuffer(Buffer)` 与 `finishReadRecoveredState()`；javadoc 按 `simplify_approach/overview.md` §6.2 写明各方法的锁前置条件与"end-of-drain 异常"语义。
- **REQ-KDF1** 在 `org.apache.flink.runtime.checkpoint.channel` 包内新增 `BufferRequester` 接口，声明 `Buffer requestBufferBlocking(InputChannelInfo)` 与 `releaseExclusiveBuffers()`；javadoc 按 `simplify_approach/overview.md` §6.3 写明"调用方禁止持有 `SpillFileReader.lock`"与"end-of-drain 单线程释放"语义。
- **REQ-9FMG** 新增 `RecoveryCheckpointBarrier` sentinel：实现 `Buffer` 或继承现有 `Buffer` 子类，必须暴露 `long getCheckpointId()`；构造器接收 `cpId`。骨架阶段允许在内部最小实现 `Buffer` 接口必需的方法（可代理到一个空的 `MemorySegment` 或直接返回常量），但不引入任何调用方。
- **REQ-9MCR** 新增 `DiskSnapshot` 类（package：`org.apache.flink.runtime.checkpoint.channel`），实现 `CloseableIterator<DiskSnapshot.Chunk>` 与必要的 `close()`；嵌套 `Chunk` 字段包含 `InputChannelInfo channelInfo`、`byte[] data`、`int length`；同时嵌套 `StartPos` 数据类（字段 `int segmentIndex`、`long offset`）供 Phase 4 在 lock 内填实迭代起点。骨架阶段 `hasNext()` 固定返回 `false`、`next()` 抛 `NoSuchElementException`、`close()` 为空；提供 `static DiskSnapshot empty()` 静态方法返回上述骨架实例供 Phase 5 feature-off 路径复用。具体迭代逻辑由 Phase 4 实现。
- **REQ-GYJN** 在 `ChannelStateWriter` 接口（现有）追加 `void addInputDataFromSpill(long checkpointId, CloseableIterator<DiskSnapshot.Chunk> chunks)`；同步在所有现有实现中提供 default 空实现（no-op）或显式空 override，保证 Phase 1 之后所有 callers / impls 仍能编译。
- **REQ-KX4N** 将 `RecoveredInputChannel.releaseAllResources()` 的访问修饰符由 package-private 提升为 `public`。仅可见性变化，方法体不动。

## 显式不在范围

- 不修改 `LocalInputChannel`、`RemoteInputChannel`、`RecoveredInputChannel` 的字段或方法体（除 REQ-KX4N 的可见性提升）
- 不引入 `SpillFile` / `SpillFileWriter` / `SpillFileReader` / `FilteredBufferWriter` / `RecoveredChannelBufferRequester`（属于 Phase 3/4）
- 不引入 `ChannelState.onCheckpointStartedForAllInputs` 与 `Alternating*` 钩子（属于 Phase 5）
- 不引入新的单元测试用例（本 phase 仅声明骨架，无可断言行为）
- 不删除 `RecoveredInputChannel.requestBufferBlocking` 中现有 heap fallback（属于 Phase 4）
