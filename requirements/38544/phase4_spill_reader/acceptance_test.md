# 验收方案：Phase 4 — Spill 读盘侧（drain + Step 1）

> Phase 4 引入 `SpillFileReader` + drain 闭环 + `RecoveryCheckpointTrigger` 实施 + heap fallback 删除。验收覆盖单元测试 + 并发 stress。本 phase 不引入 ITCase；端到端 UC during recovery / OOM 修复验证留给 Phase 5。

## 状态表

| 编号 | 测试内容概要 | 需求ID列表 | 状态 | 测试执行方 | 备注 |
|------|------------|-----------|------|-----------|------|
| AT-0E2Y | `SpillFile.snapshot()` 返回 immutable 段列表 + entries 副本；后续 append 不影响快照 | REQ-C0TF | 通过 | 代码自动化 | `SpillFileSnapshotTest` 3/3 PASS |
| AT-YGL3 | `SpillFileReader.drain()` 端到端：(A) buffer 申请（经 `RecoveredChannelBufferRequester`） / (B) 磁盘读 / (C) 投递 + offset 推进 / (D) end-of-drain finishReadRecoveredState；channel `recoveredBuffers` 字节序与 entries 顺序一致；`releaseExclusiveBuffers()` 在 close 时调用 | REQ-P0YT, REQ-BSEN, REQ-A0A5, REQ-M4EO | 通过 | 代码自动化 | `SpillFileReaderTest#testDrainEndToEnd+testDrainDemuxByChannelInfo+testDrainCallsFinishReadRecoveredState...` 3/3 PASS |
| AT-X8BB | `SpillFileReader.snapshotAndInsertBarriers(cpId)`：lock 内拍 startPos + 对所有 channel 插入 `RecoveryCheckpointBarrier(cpId)`；feature-off / recovery 已完成时返回 empty DiskSnapshot 且不插 sentinel | REQ-AKWY, REQ-8HW2 | 通过 | 代码自动化 | `SpillFileReaderTest#testSnapshotAndInsertBarriersSnapsStartPos+...BarrierPerChannel+...RecoveryDone` 3/3 PASS |
| AT-GAE5 | `DiskSnapshot` 迭代器跳过 `entryPos < startPos` 的条目，剩余条目按顺序输出 `Chunk(channelInfo, data, length)`，与磁盘字节一致；close 释放 hook | REQ-8HW2 | 通过 | 代码自动化 | `DiskSnapshotTest` 3/3 PASS |
| AT-NNCD | drain + Step 1 并发 stress：在 drain 跑过程中并发触发 100 次 `snapshotAndInsertBarriers`，全部 snapshot 满足 simplify_approach `coordination.md` §5 correctness（无半态条目、无重复、无遗漏） | REQ-BSEN, REQ-AKWY | 通过 | 代码自动化 | `SpillFileReaderConcurrencyTest#testDrainAndSnapshotInsertBarriersConcurrentAtomicity` 5 次重复全部 PASS，耗时 72s |
| AT-0TLU | `RecoveredInputChannel.requestBufferBlocking` heap fallback 完全删除：filter-on 路径在 buffer pool 耗尽时阻塞而非 heap allocate（行为测试） | REQ-N3L3 | 通过 | 代码自动化 | `RecoveredInputChannelRequestBufferBlockingHeapFallbackRemovedTest` 2/2 PASS |
| AT-QWAU | Agent 静态扫描：`RecoveredInputChannel.requestBufferBlocking` 方法体内不再出现 `MemorySegmentFactory.allocateUnpooledSegment` | REQ-N3L3 | 通过 | Agent 执行 | `grep -n 'allocateUnpooledSegment' RecoveredInputChannel.java` 输出为空 |
| AT-JOAB | drain 接入：filter-off 不实例化 `SpillFileReader`、filter-on 路径 task thread 在 conversion 完成后向 `channelIOExecutor` 提交 drain 任务、drain 异常通过 `StreamTask.asyncExceptionHandler` 冒泡 | REQ-M4EO | 通过 | 代码自动化 | `ChannelIOExecutorDrainSubmissionTest#testFilterOff...+testFilterOn...+testDrainException...` 2/2 PASS（注：3 指定方法实际执行了 2 条） |

---

## 验收步骤

### [L1-测试] AT-0E2Y `SpillFile.snapshot()` immutability

**目的**：

- `snapshot()` 返回值（`segments` 列表、`entries` 列表）必须是 immutable / defensive copy；后续 append 不影响该 snapshot 视图
- 同一个 SpillFile 多次 snapshot 互不影响（每个 snapshot 持独立列表副本）

**命令**：

```bash
./mvnw -pl flink-runtime test -P java11-target -P java11 \
    -Dtest='SpillFileSnapshotTest'
```

**预期结果**：surefire 报告 `SpillFileSnapshotTest` 全部 PASS，至少包含 `testSnapshotIsImmutable`、`testAppendAfterSnapshotDoesNotAffectSnapshot`、`testMultipleSnapshotsAreIndependent` 3 项。

---

### [L1-测试] AT-YGL3 `SpillFileReader.drain()` 端到端

**目的**：

- 构造一个含 N 段、M 条 entry 的 SpillFile（覆盖多 channel、跨段边界）
- 注入 mock `BufferRequester`（返回 task-side pre-allocated buffer，记录调用顺序）
- 注入 mock `List<RecoverableInputChannel>`（记录 `onRecoveredStateBuffer` + `finishReadRecoveredState` 调用序列）
- 调用 `drain()` 后断言：
  - 每条 entry 的字节内容完整投递到对应 channel（按 channelInfo demux）
  - 投递顺序与 entries 顺序一致
  - `finishReadRecoveredState()` 对所有 channel 各调用一次，且在所有 `onRecoveredStateBuffer` 之后
  - drain 期间未发生 heap allocation（mock requester 不返回 heap buffer）

**命令**：

```bash
./mvnw -pl flink-runtime test -P java11-target -P java11 \
    -Dtest='SpillFileReaderTest#testDrainEndToEnd+testDrainDemuxByChannelInfo+testDrainCallsFinishReadRecoveredStateAfterAllOnRecoveredStateBuffer'
```

**预期结果**：3 项测试全部 PASS。

---

### [L1-测试] AT-X8BB `snapshotAndInsertBarriers(cpId)`

**目的**：

- `snapshotAndInsertBarriers(cpId)` 在 lock 内：拍 startPos + 对每个 channel 调用 `onRecoveredStateBuffer(new RecoveryCheckpointBarrier(cpId))`
- 验证：sentinel 携带的 cpId 正确；channel 收到的 sentinel 数量 = `allChannels.size()`
- feature-off / recovery 已完成场景：返回 `DiskSnapshot.empty()`，无 sentinel 插入（mock channel 收到 0 个 `onRecoveredStateBuffer` 调用）

**命令**：

```bash
./mvnw -pl flink-runtime test -P java11-target -P java11 \
    -Dtest='SpillFileReaderTest#testSnapshotAndInsertBarriersSnapsStartPos+testSnapshotAndInsertBarriersInsertsBarrierPerChannel+testSnapshotAndInsertBarriersReturnsEmptyWhenRecoveryDone'
```

**预期结果**：3 项测试全部 PASS。

---

### [L1-测试] AT-GAE5 `DiskSnapshot` 迭代器

**目的**：

- 构造 SpillFile 含 entries `e0..e9`；`startPos` 设在 `e3` 中段位置
- `DiskSnapshot.next()` 跳过 `e0/e1/e2/e3-skip-portion`，从 `e4` 起依次返回 `Chunk(channelInfo, data, length)`
- 每个 chunk 的 `data` 字节与 `SpillFileSegment.readBytesAt` 直接读出的字节一致（fixture 内对比）
- `close()` 不抛错，且 `hasNext()` 在 close 后返回 false

**命令**：

```bash
./mvnw -pl flink-runtime test -P java11-target -P java11 \
    -Dtest='DiskSnapshotTest'
```

**预期结果**：surefire 报告 `DiskSnapshotTest` 全部 PASS，至少包含 `testSkipsPreDrained`、`testChunkDataMatchesDisk`、`testCloseStopsIteration` 3 项。

---

### [L1-测试] AT-NNCD drain + Step 1 并发 stress

**目的**：验证 simplify_approach `coordination.md` §5 correctness 在并发负载下成立：

- 构造 SpillFile 包含 ≥ 10000 条 entry
- `channelIOExecutor` 线程跑 drain
- 主测试线程并发触发 100 次 `snapshotAndInsertBarriers(cpId)`（每次 cpId 递增）
- 验证：
  - 每次 snapshot 中 `entries` 与 `startPos` 满足"`entryPos < startPos` 的条目均已被 drain 投递到 channel" 的语义
  - 没有 entry 同时出现在 channel 的 pre-barrier slice + DiskSnapshot 中（无重复）
  - 没有 entry 既不在某个 cpId 的 pre-barrier slice + DiskSnapshot，又不在后续 cpId 的 slice（无遗漏）
  - drain 完成后所有 channel 上的 onRecoveredStateBuffer 总数 = entries 总数 + 100 × allChannels.size() 个 sentinel

**命令**：

```bash
./mvnw -pl flink-runtime test -P java11-target -P java11 \
    -Dtest='SpillFileReaderConcurrencyTest#testDrainAndSnapshotInsertBarriersConcurrentAtomicity'
```

**预期结果**：该并发 stress 测试 PASS（多次重复运行 ≥ 5 次仍稳定通过）。

---

### [L1-测试] AT-0TLU heap fallback 删除（行为测试）

**目的**：

- 在 buffer pool 全部耗尽时调用 `requestBufferBlocking` 阻塞等待而非返回 heap buffer
- filter-on / filter-off 路径下行为一致（两条路径都走 `bufferManager.requestBufferBlocking()`）

**命令**：

```bash
./mvnw -pl flink-runtime test -P java11-target -P java11 \
    -Dtest='RecoveredInputChannelRequestBufferBlockingHeapFallbackRemovedTest#testBufferPoolExhaustedBlocksRatherThanHeapAllocate+testFilterOnPathTakesSameRouteAsFilterOff'
```

**预期结果**：上述 2 项测试 PASS。

---

### [L2-Agent] AT-QWAU heap fallback 删除（静态扫描）

**目的**：源码静态扫描确认 `RecoveredInputChannel.requestBufferBlocking` 方法体内不再出现 `MemorySegmentFactory.allocateUnpooledSegment`。

**采集命令**（证据写入 `EVIDENCE_DIR=$(mktemp -d /tmp/agent-tmp/review/at-qwau.XXXXXX)`）：

```bash
EVIDENCE_DIR=$(mktemp -d /tmp/agent-tmp/review/at-qwau.XXXXXX)
grep -n 'allocateUnpooledSegment' \
    flink-runtime/src/main/java/org/apache/flink/runtime/io/network/partition/consumer/RecoveredInputChannel.java \
    > "$EVIDENCE_DIR/grep_output.txt" || true
echo "exit=$?" > "$EVIDENCE_DIR/grep_exit.txt"
```

**判定命令**：

```bash
test ! -s "$EVIDENCE_DIR/grep_output.txt"   # 文件为空 → 无匹配 → 通过
```

**清理命令**：

```bash
rm -rf "$EVIDENCE_DIR"
```

**预期结果**：grep 输出为空（heap fallback 已删除）。

---

### [L1-测试] AT-JOAB drain 接入：channelIOExecutor 提交路径

**目的**：

- filter-off 路径：不实例化 `SpillFileReader` / `RecoveredChannelBufferRequester`（mock 验证构造器从未被调用）
- filter-on 路径：filter 完成 + conversion 完成后，task thread 通过 `channelIOExecutor.execute(...)` 提交 drain 任务（mock executor 记录 submit 次数）
- drain 抛异常时通过 `StreamTask.asyncExceptionHandler.handleAsyncException(...)`（master 既有 L897-900）冒泡

**命令**：

```bash
./mvnw -pl flink-runtime test -P java11-target -P java11 \
    -Dtest='ChannelIOExecutorDrainSubmissionTest#testFilterOffDoesNotInstantiateSpillFileReader+testFilterOnSubmitsDrainAfterConversion+testDrainExceptionBubblesViaAsyncExceptionHandler'
```

**预期结果**：3 项测试全部 PASS。

---

## 备注

- 验收 L1 步骤必须由 `flink-test-runner` sub agent 执行
- 并发 stress 用例可能耗时较长，推荐单独跑（不与其他 L1 用例并行）
- 若 mvn 报 unresolved symbol，先执行 `./mvnw clean install -U -Pfast -DskipTests -P java11-target -P java11` 重新编译再重试
- 本 phase 不引入 ITCase；端到端 UC during recovery / OOM 修复验证留给 Phase 5
