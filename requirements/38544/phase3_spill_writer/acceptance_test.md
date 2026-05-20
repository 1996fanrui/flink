# 验收方案：Phase 3 — Spill 写盘侧（filter 阶段）

> Phase 3 引入 `SpillFile` / `FilteredBufferWriter` / `SpillFileWriter` 三个新类，并把 filter 输出从 channel 切到 spill 文件。验收覆盖单元测试与 filter 路径集成测试。本 phase 不引入 ITCase；端到端验证留给 Phase 5。

## 状态表

| 编号 | 测试内容概要 | 需求ID列表 | 状态 | 测试执行方 | 备注 |
|------|------------|-----------|------|-----------|------|
| AT-M4Z1 | `SpillFile` 单元测试：append → byte 级回读、段轮转跨 64 MiB 边界正确、close 后 append 抛 `IllegalStateException`、entries 与磁盘 offset 一致 | REQ-3P7A, REQ-9JHL | 通过 | 代码自动化 | `SpillFileTest` 5/5 PASS |
| AT-E3L9 | `FilteredBufferWriter` 单元测试：累加 / flush 边界、prefilter buffer 始终为同一实例、close 把剩余 flush、close 后 write 抛 `IllegalStateException` | REQ-OY79, REQ-9JHL | 通过 | 代码自动化 | `FilteredBufferWriterTest` 6/6 PASS |
| AT-1V57 | `SpillFileWriter` facade 单元测试：write 委托、close 委托链路（先 accumulator.close 后 spillFile.close）、close 幂等 | REQ-GQHL, REQ-9JHL | 通过 | 代码自动化 | `SpillFileWriterTest` 4/4 PASS |
| AT-5IG4 | filter 路径集成：filter-on 时输出全部进入 SpillFile（channel 的 `recoveredBuffers` 期间无投递）；filter-off 时不实例化 SpillFile，行为与 master 完全一致 | REQ-JSGX, REQ-8C3Y, REQ-9JHL | 通过 | 代码自动化 | `RecoveredChannelStateHandlerFilterRoutingTest` 6/6 PASS |
| AT-VM5E | `bufferFilteringCompleteFuture` 完成前 `SpillFileWriter.close()` 已被调用（filter 完成时 SpillFile 已冻结） | REQ-8C3Y, REQ-9JHL | 通过 | 代码自动化 | `RecoveredChannelStateHandlerFilterRoutingTest#testBufferFilteringCompleteFutureCompletesAfterSpillFileClosed` 1/1 PASS |

---

## 验收步骤

### [L1-测试] AT-M4Z1 `SpillFile` 单元测试

**目的**：

- `append(channelInfo, payload)` 后通过 entries + segment FileChannel 直接读取应得到一字节不差的原始 payload
- 写入累计字节超过 `DEFAULT_SEGMENT_SIZE_BYTES` 时正确切换新段；entries 中的 `segmentIndex` 与新段编号匹配
- `close()` 之后再 `append` 抛 `IllegalStateException`，且消息中包含 "closed" 字样
- entries 队列与磁盘 offset 完全一致（offset 累计等于已写字节数）

**命令**：

```bash
./mvnw -pl flink-runtime test -P java11-target -P java11 -Dtest='SpillFileTest'
```

**预期结果**：surefire 报告 `SpillFileTest` 全部 PASS，至少包含 `testAppendRoundtrip`、`testSegmentRotationAcrossDefaultSegmentSize`、`testAppendAfterCloseThrows`、`testEntriesMatchDiskLayout` 4 项。

---

### [L1-测试] AT-E3L9 `FilteredBufferWriter` 单元测试

**目的**：

- 累加未满 postfilter buffer 时不调用 `SpillFile.append`
- 累加恰好填满 postfilter buffer 时调用一次 `append` 并取得新 postfilter buffer
- 跨多 buffer boundary 的 write 调用产生多条 entry，且每条 entry 长度与累加片段一致
- `getPrefilterBuffer()` 多次调用必须返回**同一**实例
- `close()` flush 剩余 postfilter 内容（若非空）然后关闭 SpillFile；close 后 `write` 抛 `IllegalStateException`

**命令**：

```bash
./mvnw -pl flink-runtime test -P java11-target -P java11 -Dtest='FilteredBufferWriterTest'
```

**预期结果**：surefire 报告 `FilteredBufferWriterTest` 全部 PASS，至少包含 `testWriteAccumulatesUntilPostfilterBufferFull`、`testWriteSpanningMultipleBuffersProducesMultipleEntries`、`testPrefilterBufferIsStableInstance`、`testCloseFlushesRemainingThenClosesSpillFile`、`testWriteAfterCloseThrows`、`testCloseIsIdempotent` 6 项。

---

### [L1-测试] AT-1V57 `SpillFileWriter` facade 单元测试

**目的**：

- `write(channelInfo, buf)` 委托给 `FilteredBufferWriter.write`
- `close()` 顺序：先 `FilteredBufferWriter.close()`、后 `SpillFile.close()`（通过 InOrder 或 mock 验证）
- `close()` 重复调用幂等（不抛错、不重复关闭 FileChannel）
- `getSpillFile()` 在 close 前返回构造时传入的 SpillFile 实例

**命令**：

```bash
./mvnw -pl flink-runtime test -P java11-target -P java11 -Dtest='SpillFileWriterTest'
```

**预期结果**：surefire 报告 `SpillFileWriterTest` 全部 PASS，至少包含 `testWriteDelegates`、`testCloseOrdering`、`testCloseIsIdempotent`、`testGetSpillFileReturnsConstructorArg` 4 项。

---

### [L1-测试] AT-5IG4 filter 路径集成测试

**目的**：

- filter-on：filter 完成后 `SpillFile.entries` 数量与输入 buffer 数量一致（或语义等价，比如包含正确的 channelInfo 组）；filter 期间 channel 的 `recoveredBuffers` 始终为空（验证 destination 真的切到了 writer 而非 channel）
- filter-off：调用 `RecoveredChannelStateHandler.recover` 时不创建 `SpillFile`、不实例化 `FilteredBufferWriter`、不实例化 `SpillFileWriter`；channel 的 `recoveredBuffers` / `receivedBuffers` 在 filter 完成时已经包含恢复数据（与 master 既有行为一致）

**命令**：

```bash
./mvnw -pl flink-runtime test -P java11-target -P java11 \
    -Dtest='RecoveredChannelStateHandlerFilterRoutingTest'
```

**预期结果**：surefire 报告全部 PASS，至少包含 `testFilterOnRoutesOutputToSpillFile`、`testFilterOnDoesNotInvokeChannelOnRecoveredStateBuffer`、`testFilterOffDoesNotCreateSpillFile`、`testFilterOffMaintainsMasterBehavior` 4 项。

---

### [L1-测试] AT-VM5E `bufferFilteringCompleteFuture` 完成前已 close SpillFile

**目的**：保证 filter 完成时 `SpillFile` 已 close（Phase 4 拿到 SpillFile 时是冻结状态）。通过断言 `bufferFilteringCompleteFuture.isDone()` 前 `SpillFile.isClosed()` 为 true 验证。

**命令**：

```bash
./mvnw -pl flink-runtime test -P java11-target -P java11 \
    -Dtest='RecoveredChannelStateHandlerFilterRoutingTest#testBufferFilteringCompleteFutureCompletesAfterSpillFileClosed'
```

**预期结果**：该单测 PASS。

---

## 备注

- 验收 L1 步骤必须由 `flink-test-runner` sub agent 执行
- 测试用例命名以本文档为准，开发阶段实际命名可微调，但语义覆盖必须等价
- 若 mvn 报 unresolved symbol，先执行 `./mvnw clean install -U -Pfast -DskipTests -P java11-target -P java11` 重新编译再重试
- 本 phase 不涉及 channel 内部字段；filter-off 路径回归依赖 master 既有的 `SequentialChannelStateReaderImplTest` 等测试套，不需要本 phase 额外覆盖
