# 用户需求 — Phase 3：Spill 写盘侧（filter 阶段）

## 需求偏离

无。

## 背景

[`simplify_approach/unspiller.md`](../simplify_approach/unspiller.md) §2 / §2a / §2b 规定了 filter 阶段如何将 master 现有的 heap fallback 替换为"写到 spill 文件"路径。Phase 3 落入 `channelIOExecutor` 的 filter 期段：filter 输出由原来"塞进 channel 的 `recoveredBuffers`" 改为"经 `FilteredBufferWriter` 累加到 spill 文件"；`channelIOExecutor` 的 filter 主循环复用既有 `RecoveredChannelStateHandler.recover` 形态。Phase 3 **与 Phase 2 并行**——双方只依赖 Phase 1 引入的接口。本 phase **不**移除 `RecoveredInputChannel.requestBufferBlocking` 的 heap fallback（Phase 4 负责），仅切换 filter 内部的 buffer 来源与目的地。

## 需求

- **REQ-3P7A** 新增 `SpillFile` 类（package：`org.apache.flink.runtime.checkpoint.channel`），承载多段 append-only 写入：
  - 单线程写入（`channelIOExecutor`）
  - 段大小默认 `64 MiB`，超过即轮转新段（与 Flink `ChannelStateWriterImpl` 既有段大小风格对齐）
  - 内存 `entries` 队列：每条 entry 记录 `(InputChannelInfo channelInfo, long offset, int length)`
  - 提供 `append(InputChannelInfo, ByteBuffer payload)` 写入入口、`close()` 冻结
  - 在 `close()` 之后再次 `append` 必须抛 `IllegalStateException`
- **REQ-OY79** 新增 `FilteredBufferWriter` 类：
  - 持有 1 个 `prefilterBuffer`（供 filter 读 state buffer）+ 1 个 `postfilterBuffer`（累加 filter 输出）
  - `prefilterBuffer` 与 `postfilterBuffer` 用 Flink 标准 `MemorySegment` + `Buffer`，由 task-level pre-allocated pool 提供（与 master 既有 buffer 来源对齐，不允许 heap fallback）
  - `postfilterBuffer` 满时通过 `SpillFile.append(channelInfo, postfilterBuffer)` 刷盘并复位
  - 提供 `Buffer getPrefilterBuffer()`、`write(InputChannelInfo, Buffer)`、`close()` 三个公开方法
  - `close()` 把 `postfilterBuffer` 剩余内容 flush 到 `SpillFile` 后调用 `SpillFile.close()`；此后再调用 `write` 抛 `IllegalStateException`
- **REQ-GQHL** 新增 `SpillFileWriter` 类：phase 1 façade，持有 `SpillFile` 与 `FilteredBufferWriter`：
  - 构造器：`SpillFileWriter(SpillFile, FilteredBufferWriter)`
  - `write(InputChannelInfo, Buffer)` 委托给 `FilteredBufferWriter.write`
  - `close()`（实现 `Closeable`）依次委托 `FilteredBufferWriter.close()`、`SpillFile.close()`；幂等
- **REQ-JSGX** `RecoveredChannelStateHandler.recover` 的 filter 分支改写：
  - filter 输出目的地由 `channel.onRecoveredStateBuffer(buf)` 改为 `spillFileWriter.write(channelInfo, buf)`
  - `bufferSupplier` 由 `channel::requestBufferBlocking` 改为 `filteredBufferWriter::getPrefilterBuffer`（始终复用同一个 prefilterBuffer，避免反复申请 / 释放）
  - 非 filter 分支（filter-off 路径）保持 master 既有行为，调用 `channel.onRecoveredStateBuffer(buf)` 不变
- **REQ-8C3Y** `bufferFilteringCompleteFuture`（master 既有 future）触发时机不动；触发前由 `RecoveredChannelStateHandler.close()` 或等价 hook 调用 `spillFileWriter.close()`，保证文件在 future 完成之前已冻结。完成后 `SpillFile` 实例交付给 Phase 4 的 `SpillFileReader`（本 phase 仅在内存中持有引用，不接 drain）。
- **REQ-9JHL** 测试覆盖：
  - `SpillFileTest`：写入 → byte 级回读、段轮转跨 64 MiB 边界正确、`close()` 后写入抛 `IllegalStateException`、`entries` 队列与实际文件 offset 一致
  - `FilteredBufferWriterTest`：累加未满时不刷盘、累加满后刷盘并复位、`close()` 把剩余 flush
  - `SpillFileWriterTest`：facade 委托链路、`close()` 幂等
  - filter 路径集成测试（依赖现有 filter 测试 fixture）：filter 完成后 `SpillFile` 内 entry 数量 = 输入 buffer 数量；filter-off 路径不创建 `SpillFile`

## 显式不在范围

- 不移除 `RecoveredInputChannel.requestBufferBlocking` 中的 heap fallback（Phase 4）
- 不实现 `SpillFileReader` / `DiskSnapshot` 完整迭代逻辑 / `RecoveredChannelBufferRequester`（Phase 4）
- 不引入 `ChannelState.onCheckpointStartedForAllInputs` 或 `Alternating*` 钩子（Phase 5）
- 不引入 `ChannelStateWriter.addInputDataFromSpill` 真实实现（Phase 5）
- 不引入 ITCase；本 phase 在单元测试 + filter fixture 集成层面验证
