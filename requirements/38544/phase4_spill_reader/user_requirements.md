# 用户需求 — Phase 4：Spill 读盘侧（drain + Step 1）

## 需求偏离

无。

## 背景

[`simplify_approach/unspiller.md`](../simplify_approach/unspiller.md) §3 / §4 与 [`simplify_approach/coordination.md`](../simplify_approach/coordination.md) §1 规定了 drain 期段：`SpillFileReader` 通过单个 `Object lock` 串接"读 entry → 取 buffer → 读盘 → 投递 channel + 推进 offset"。task thread 唯一会与 drain 在锁上相会的位点是 checkpoint 触发瞬间，由 `RecoveryCheckpointTrigger.snapshotAndInsertBarriers()` 完成 Step 1。

Phase 4 依赖 Phase 3 引入的 `SpillFile`（写入端）、Phase 1 引入的接口骨架、Phase 2 在三个 channel 上落实的 `RecoverableInputChannel` 实现。本 phase 在 drain 路径接入完成的同时，**删除 `RecoveredInputChannel.requestBufferBlocking` 的 heap fallback**——这是 FLINK-38544 的最终目标动作。

## 需求

- **REQ-C0TF** `SpillFile` 扩展只读 / 快照能力（Phase 3 引入的 SpillFile 在 Phase 4 增量扩展）：
  - `snapshot()` 返回当前 `entries` 队列的**不可变副本** + 当前 `(segmentIndex, currentEnd)` 的快照值；调用时序由 `SpillFileReader.lock` 保证
  - `SpillFileSegment` 暴露 `peekNextEntry()`、`pollNextEntry()`、`readBytesAt(long offset, int length, byte[] dest)` 给 `SpillFileReader.drain()` 使用；段读取支持多个独立读句柄（drain 一个 + 每个 in-recovery cpId 一个），不同读句柄不共享文件位置
- **REQ-P0YT** 新增 `SpillFileReader` 类（package：`org.apache.flink.runtime.checkpoint.channel`）：
  - 实现 `RecoveryCheckpointTrigger, Closeable`
  - 字段：`SpillFile spillFile`、`List<RecoverableInputChannel> allChannels`、`Map<InputChannelInfo, RecoverableInputChannel> channelByInfo`（构造时一次性派生）、`BufferRequester bufferRequester`、私有 `Object lock`、`@GuardedBy("lock") int currentSegmentIndex`、`@GuardedBy("lock") long currentOffset`
  - 构造：`SpillFileReader(SpillFile, List<RecoverableInputChannel>, BufferRequester)`，构造时即 +1 SpillFile 引用计数（实际计数器接入在 Phase 5）
- **REQ-BSEN** `SpillFileReader.drain()` 主循环按 `simplify_approach/unspiller.md` §3 §4 步骤：
  - (A) lock 外：`bufferRequester.requestBufferBlocking(entry.channelInfo)` 拿到 buffer
  - (B) lock 外：`seg.readBytesAt(entry.offset, entry.length, buf 的 MemorySegment.asByteArray())` 把磁盘字节读入 buf
  - (C) lock 内：`channel.onRecoveredStateBuffer(buf)` + `seg.pollNextEntry()` + 更新 `currentSegmentIndex/Offset`
  - (D) lock 外：所有段消费完后，对每个 `RecoverableInputChannel` 调用 `finishReadRecoveredState()`
  - drain 调用方：`channelIOExecutor`（conversion 完成后 submit；本 phase 引入接入点）
- **REQ-AKWY** `SpillFileReader.snapshotAndInsertBarriers()` 实现 `RecoveryCheckpointTrigger`：
  - 调用方禁止持有 `SpillFileReader.lock`
  - 取 lock → 调用 `spillFile.snapshot()` 形成 `DiskSnapshot.segmentList` + `startPos = (currentSegmentIndex, currentOffset)` → 对 `allChannels` 中每个 `RecoverableInputChannel` 调用 `onRecoveredStateBuffer(new RecoveryCheckpointBarrier(cpId))` → 出 lock → 返回 `DiskSnapshot`
  - feature off / recovery 完成（drain 已结束）时返回 empty `DiskSnapshot`（迭代器立即结束），不插任何 barrier
- **REQ-8HW2** `DiskSnapshot` 完整实现（Phase 1 骨架在本 phase 填实）：
  - 字段：immutable `segmentList`（每段为只读视图）+ `startPos`（cpId 启动瞬间的 offset 快照）
  - `next()` / `hasNext()` 迭代按"entries 顺序"输出 `Chunk`，跳过 `entryPos < startPos` 的条目（drain 已投递部分）
  - `close()` 释放本 reader 持有的 SpillFile 引用计数（计数器具体语义在 Phase 5 引入；本 phase 暴露 hook）
- **REQ-A0A5** 新增 `RecoveredChannelBufferRequester implements BufferRequester`（package：`org.apache.flink.runtime.checkpoint.channel`）：
  - 持 `Map<InputChannelInfo, RecoveredInputChannel> channelMap`
  - `requestBufferBlocking(InputChannelInfo)` 委托给 `RecoveredInputChannel.requestBufferBlocking()`
  - `releaseExclusiveBuffers()` 遍历 channelMap 调用 `RecoveredInputChannel.releaseAllResources()`（依赖 Phase 1 的可见性提升）
- **REQ-N3L3** **删除** `RecoveredInputChannel.requestBufferBlocking` 中的 heap fallback（`MemorySegmentFactory.allocateUnpooledSegment` + `NetworkBuffer(FreeingBufferRecycler)` 这两行整体移除，回归到只走 `bufferManager.requestBufferBlocking()`）。需同步移除 `checkpointingDuringRecoveryEnabled()` 分支，因为此时 filter 路径已不经过本方法（Phase 3）。
- **REQ-M4EO** drain 接入：在 `channelIOExecutor` 既有"conversion 完成后" 时机 submit `spillFileReader::drain`；submit 的具体接入点必须保证：
  - filter-off 路径**不**实例化 `SpillFileReader`、不 submit drain
  - filter-on 路径：filter 完成 → conversion 完成（master 既有 mailbox 路径，由 task thread 驱动）→ task thread submit `drain` 到 `channelIOExecutor`
  - drain 异常时通过 `setError(...)` 等既有 channelIOExecutor 错误传递机制冒泡到 task

## 显式不在范围

- 不引入 `ChannelState.onCheckpointStartedForAllInputs` dispatcher（Phase 5）
- 不接入 `AlternatingWaitingForFirstBarrierUnaligned.barrierReceived` / `AlternatingCollectingBarriers.barrierReceived`（Phase 5）
- 不实现 `ChannelStateWriter.addInputDataFromSpill` async demux 真实写盘（Phase 5）
- 不引入 SpillFile 引用计数器与 cpId-level 释放回调（Phase 5；本 phase 只在 SpillFileReader.close / DiskSnapshot.close 中预留 hook）
- 不引入 ITCase；本 phase 在单元测试 + 并发 stress 测试层面验证
