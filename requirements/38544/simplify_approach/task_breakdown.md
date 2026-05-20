# Task Breakdown

> **Follow-up (2026-05-24):** 后续若干轮 fix 已落地，见 [`../fix_rounds/`](../fix_rounds/)：
> per-channel `isInRecovery()` 修了 Step 1/Step 2 谓词、cpDuringRecovery=false 路径
> 构造时 `allDelivered=true`、per-channel `upstreamReady` future 取代了之前的 conditional
> wake 修补。本 breakdown 反映的是原始阶段切分，不再单独列上述 fix；待开发任务以
> [`recovery_in_recovery_flag_unification.md §9.4`](../fix_rounds/recovery_in_recovery_flag_unification.md)
> 给出的 `deliverRecoveredInternal` 抽象为最新落地参照。

> 基于 simplify_approach 设计文档拆分的后续开发阶段。当前分支已完成 `LocalInputChannel` 的 `recoveredBuffers` / `toBeConsumedBuffers` 解耦（commit `292cc4b9e2d`，FLINK-39018 的 pull 迁移路径仍保留），此处只列尚未落地的工作。

## 依赖图

```
Phase 1 (common interfaces)
   │
   ├──► Phase 2 (InputChannel side)               ┐
   │                                              │
   └──► Phase 3 (spill writer) ──► Phase 4 (spill reader)
                                                  │
                                                  ▼
                                          Phase 5 (3-step coordination)
```

- Phase 1 完成后，Phase 2 与 Phase 3 完全并行（互相只依赖接口）。
- Phase 4 依赖 Phase 3 的 `SpillFile` 写入产物。
- Phase 5 需要 Phase 2 的 `checkpointStarted` 双分支与 Phase 4 的 `snapshotAndInsertBarriers` 同时到位。
- **5 个阶段共用同一开发分支（当前 `38544-spilling-v2/20260521-poc`），不拆 PR、不发 PR**；每个阶段以**单一 commit** 形态推进，commit 之间禁止 amend，确保所有阶段进展都可追溯到独立 commit。

## Phase 1 — Common interfaces & sentinels

只落签名与骨架，零业务逻辑。落完后两侧可分别 mock 对方接口并行开发。

- 三个接口（声明见 [`overview.md`](./overview.md#6-cross-thread-java-interfaces) §6）：
  - `RecoveryCheckpointTrigger`（task thread → unspilling thread）
  - `RecoverableInputChannel`（unspilling thread → 物理 channel）
  - `BufferRequester`（unspilling thread → buffer pool）
- `RecoveryCheckpointBarrier` sentinel（携带 `getCheckpointId()`）。
- `DiskSnapshot` 类骨架 + 内部 `Chunk` 类型。
- `ChannelStateWriter.addInputDataFromSpill(long, CloseableIterator<Chunk>)` 仅声明 + default no-op。
- `RecoveredInputChannel.releaseAllResources()` 访问修饰符由 package-private 提升为 `public`（仅可见性变化）。

无新增 test。后续阶段编译能通过该接口骨架即视为完成。

## Phase 2 — InputChannel side

实施 [`input_channel.md`](./input_channel.md) §3 全部内容，**与 Phase 3 并行**。

- 重塑字段：
  - `recoveredBuffers` 字段类型由现在的 `Deque<BufferAndBacklog>` 改为 `Deque<Buffer>`；同时新增于 `RemoteInputChannel`。
  - 新增 `allRecoveredBuffersDelivered: boolean`（Local + Remote）。
  - 删除 `LocalInputChannel` 现有的 `getNextRecoveredBuffer()` 私有方法、`initialRecoveredBuffers` 构造参数及对应迁移块。
- 三个 channel 实现 `RecoverableInputChannel`：`RecoveredInputChannel`、`LocalInputChannel`、`RemoteInputChannel`。
- `getNextBuffer()` 改写为单一 `inRecovery` 判定路径，priority-event 沿用 master 通道（Remote 经 `addPriorityBuffer` / `receivedBuffers` 头位；Local 经 `subpartitionView`）。
- `checkpointStarted()` 改为 in-recovery / not-in-recovery 互斥两分支（参见 [`input_channel.md`](./input_channel.md) §3.8 与 [`coordination.md`](./coordination.md) §3.3）；新增 `receivedBuffersHasNoLiveDataBuffer()` 断言。
- `stateConsumedFuture` 完成条件改为 `allRecoveredBuffersDelivered && recoveredBuffers.isEmpty()`；触发位点由该完成条件变成 true 的最后一处状态翻转负责。
- `RecoveredInputChannel.toInputChannel()` 改走新接口：对剩余 buffer 逐个调用物理 channel 的 `onRecoveredStateBuffer(...)`，结束时 `finishReadRecoveredState()`；其他 `RecoveredInputChannel` 现有 API 不动（仍服务 filter-off 路径）。
- 锁复用：Remote 复用 master 既有 `synchronized(receivedBuffers)`；Local 用 `synchronized(recoveredBuffers)`。不引入第三个锁对象。

测试：扩展 `LocalInputChannelTest` 既有 9 个回归用例（覆盖 `inRecovery` 四种边界），新增 `RemoteInputChannelTest` 对应用例，新增 `checkpointStarted` 互斥分支用例与 `stateConsumedFuture` 完成路径用例。

## Phase 3 — Spill writer side (filter phase)

实施 [`unspiller.md`](./unspiller.md) §2 / §2a / §2b，**与 Phase 2 并行**。

- 新增类：
  - `FilteredBufferWriter`：一对 `prefilterBuffer` + `postfilterBuffer`，post 满后 flush 到 `SpillFile`。
  - `SpillFile`：on-disk 对象；单线程 append；超 64 MB 段轮转；in-memory `entries` 队列保存 `(channelInfo, offset, length)`。
  - `SpillFileWriter`：phase 1 facade；`write(InputChannelInfo, Buffer)`；`close()` flush 余下 post buffer 并冻结文件。
- filter 通路改写（`RecoveredChannelStateHandler.recover` 的 filter 分支）：
  - destination 由 `channel.onRecoveredStateBuffer(...)` 切到 `filteredBufferWriter.write(channelInfo, buf)`。
  - `bufferSupplier` 由 `channel::requestBufferBlocking` 切到 `FilteredBufferWriter` 提供的可复用 `prefilterBuffer` 源。
- `bufferFilteringCompleteFuture` 完成前先调用 `SpillFileWriter.close()`；完成后 `SpillFile` 交给 Phase 4 的 `SpillFileReader`。

测试：`FilteredBufferWriter` 累加与 flush；`SpillFile` 写入 → 字节级回读；`SpillFileWriter.close` 之后再次写入应抛错；段轮转跨 64 MB 边界正确。

## Phase 4 — Spill reader side (drain + Step 1)

实施 [`unspiller.md`](./unspiller.md) §3 / §4 与 [`overview.md`](./overview.md) §6.1 / §6.3。

- `SpillFile` 扩展：`snapshot()` 克隆 `entries` 列表、`(segmentIndex, offset)` 进度访问、`SpillFileSegment.peekNextEntry/pollNextEntry/readBytesAt`。
- 新增 `SpillFileReader implements RecoveryCheckpointTrigger, Closeable`：
  - 字段：`spillFile`、`allChannels`、`channelByInfo`、`bufferRequester`、私有 `Object lock`、`@GuardedBy("lock")` 的 `currentSegmentIndex` / `currentOffset`。
  - `drain()` 主循环：(A) lock 外 `requestBufferBlocking`，(B) lock 外 `seg.readBytesAt`，(C) lock 内 `onRecoveredStateBuffer` + 推进 offset，(D) lock 外 `finishReadRecoveredState`。
  - `snapshotAndInsertBarriers()`：lock 内拍 `DiskSnapshot.startPos` + 对每个 channel `onRecoveredStateBuffer(barrier)`，lock 外返回。
  - `close()`：`bufferRequester.releaseExclusiveBuffers()` + `spillFile.close()`。
- `DiskSnapshot` 完整实现：`List<SpillFileSegment snapshot>` + `startPos`；`CloseableIterator<Chunk>` 跳过 `entryPos < startPos` 的条目。
- `RecoveredChannelBufferRequester implements BufferRequester`：`requestBufferBlocking(channelInfo)` 委托给 `RecoveredInputChannel.requestBufferBlocking()`；`releaseExclusiveBuffers()` 遍历调用 `releaseAllResources()`。
- 删除 `RecoveredInputChannel.requestBufferBlocking` 中 heap fallback 整块（仅保留 `bufferManager.requestBufferBlocking()`）—— **FLINK-38544 最终消除点**。
- 接线：在 `channelIOExecutor` 中 conversion 完成后 submit `spillFileReader.drain()`；end-of-drain 由各 channel 自行完成 `stateConsumedFuture`。

测试：`SpillFileReader.drain` 端到端交付；snapshot 与 drain 并发不出现"半态"条目（Principle 1 + 2 原子性）；heap fallback 移除后 filter-on 路径仅经 disk；buffer pool 全占时 drain 正确 park 在 lock 外。

## Phase 5 — Checkpoint 3-step coordination

实施 [`coordination.md`](./coordination.md) §3。

- 新增 `ChannelState.onCheckpointStartedForAllInputs(CheckpointBarrier, ChannelStateWriter)` 调度器：
  - Step 1：`snap = recoveryCheckpointTrigger.snapshotAndInsertBarriers()`。
  - Step 2：master 既有 `for (input : inputs) input.checkpointStarted(barrier)` 循环（互斥分支已在 Phase 2 嵌入到 channel 内部）。
  - Step 3：`writer.addInputDataFromSpill(barrier.getId(), snap)`。
- 两处 UC 入口各加一行调用：
  - `AlternatingWaitingForFirstBarrierUnaligned.barrierReceived`
  - `AlternatingCollectingBarriers.barrierReceived`
- `ChannelStateWriter.addInputDataFromSpill` 完整实现：async writer 线程按 `chunk.channelInfo` demux 写入对应 channel 的 checkpoint output；空 `DiskSnapshot` 走 no-op。
- `SpillFile` 引用计数生命周期：
  - drain：`SpillFileReader` 构造时 +1，drain 退出（§4 step (D) 之后）-1。
  - 每个 in-recovery cpId：在 Step 1 lock 内构造 `DiskSnapshot` 时 +1；回调挂在 `ChannelStateWriter.getAndRemoveWriteResult(cpId).getInputChannelStateHandles()` 完成时 -1。
  - 引用归零删段；`ChannelStateWriter.abort(cpId, cause, cleanup)` 异常完成的回调走同一释放路径。
- feature-off / recovery 全部完成时：trigger 返回空 `DiskSnapshot`、writer 端空 snap no-op，外层不引入 `if (filter-on)` 分支。

测试：ITCase 覆盖 UC during recovery（Step 1/2/3 原子性 + correctness 证明）、rescale + filter + large record（heap fallback 移除后 OOM 不再触发）、abort 路径段清理、feature-off 通路不引入额外开销；master 既有 `UnalignedCheckpointRescaleITCase` 回归。

## Commit 策略

- **单分支多 commit**：所有 5 个 phase 共用一个开发分支（当前 `38544-spilling-v2/20260521-poc`）；**不拆 PR、不发 PR**，仅在分支上按 phase 顺序推进 commit。
- **每个 phase = 一个 commit**：完成本 phase 全部代码 + 测试 + 验收命令本地 PASS 后，作为单一 commit 落到开发分支；commit message 前缀 `[FLINK-38544][network]` 或 `[FLINK-38544][checkpoint]` 等按 touched module 选定，commit 描述链接到本 phase 的 `requirements/38544/phaseN_*/` 三件套。
- **禁止 amend**：commit 一旦推上分支不允许 `git commit --amend`、不允许 `git rebase -i` 修改历史；后续如果需要修正前一 phase，必须以新 commit 形态追加（在分支上向前演进），保留每个 commit 作为可追溯里程碑。
- **测试落地**：每个 phase 的 commit 包含 `phaseN_*/acceptance_test.md` 列出的所有 L1 用例；commit 之前由 `flink-test-runner` sub agent 跑相关 test 必须 PASS。
