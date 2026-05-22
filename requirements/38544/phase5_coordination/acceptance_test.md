# 验收方案：Phase 5 — Checkpoint 3-step 协议接线

> Phase 5 把前 4 个 phase 的产出收口成端到端可用的 UC during recovery + OOM 修复 feature。验收覆盖单元测试、ITCase、master 既有 regression。

## 状态表

| 编号 | 测试内容概要 | 需求ID列表 | 状态 | 测试执行方 | 备注 |
|------|------------|-----------|------|-----------|------|
| AT-0MAW | `ChannelState.onCheckpointStartedForAllInputs` 调度器：Step 1 → 每 input.checkpointStarted → Step 3 顺序；外层无 `if (filter-on)`；feature-off 时 trigger + writer 都走 no-op | REQ-H6G4, REQ-9S6W | 通过 | 代码自动化 | `ChannelStateDispatcherTest` 4/4 PASS |
| AT-LL4G | `AlternatingWaitingForFirstBarrierUnaligned` / `AlternatingCollectingBarriers` 各加 1 行调度器调用；钩点在 `triggerGlobalCheckpoint` 之前；不破坏既有 master 行为 | REQ-1G6O | 通过 | 代码自动化 | `AlternatingWaitingForFirstBarrierUnalignedDispatchHookTest` 2/2、`AlternatingCollectingBarriersDispatchHookTest` 3/3、master 回归 `AlternatingCheckpointsTest` 33/33 PASS（doc 中原名 AlternatingControllerTest 已不存在，对应 AlternatingCheckpointsTest） |
| AT-XVY7 | `ChannelStateWriterImpl.addInputDataFromSpill(cpId, snap)`：非空 snap async 写盘；空 snap in-line 早 return；写盘异常通过 ChannelStateWriteResult 传递；chunks 必须 close | REQ-Z2ZC, REQ-9S6W | 通过 | 代码自动化 | `ChannelStateWriterImplAddInputDataFromSpillTest` 4/4 PASS |
| AT-BSDC | `SpillFile` 引用计数器：acquire / release 计数正确；归零删段；abort 路径同 success 释放；删段幂等 | REQ-DJMJ, REQ-AIL8 | 通过 | 代码自动化 | `SpillFileRefCountTest` 5/5 PASS |
| AT-T5QE | UC during recovery ITCase：3-step 协议端到端 correctness（pre-barrier slice + DiskSnapshot 字节集合无重复无遗漏）；master 既有 `UnalignedCheckpointRescaleITCase` 零修改回归 | REQ-PQ31, REQ-H6G4, REQ-Z2ZC | pending 人工验证 | 代码自动化 | flink-tests 模块 Confluent codeartifact 401 Unauthorized，dep resolution 失败，无法在当前环境构建 flink-tests |
| AT-FZXD | Rescale + filter + large record ITCase：复现 master OOM 场景，本 phase 后 task 内存峰值低于阈值，PASS；master 上同测试必然 OOM | REQ-PQ31 | pending 人工验证 | 代码自动化 | 同 AT-T5QE，flink-tests 模块 Confluent codeartifact 401 Unauthorized，dep resolution 失败 |

---

## 验收步骤

### [L1-测试] AT-0MAW `ChannelState.onCheckpointStartedForAllInputs` 调度器

**目的**：

- 验证 3 个 step 顺序：trigger.snapshotAndInsertBarriers(cpId) → 每个 input.checkpointStarted(barrier) → writer.addInputDataFromSpill(cpId, snap)（mock 验证 InOrder）
- feature-on 时 trigger 返回非空 snap，writer 收到非空 snap；feature-off 时 trigger 返回 empty snap，writer 收到 empty snap 并 in-line 早 return（mock 验证未提交 writer 线程）
- 调度器外层不出现 `if (filter-on)` 分支（grep 验证）

**命令**：

```bash
./mvnw -pl flink-runtime test -P java11-target -P java11 \
    -Dtest='ChannelStateDispatcherTest'
```

**预期结果**：surefire 报告 `ChannelStateDispatcherTest` 全部 PASS，至少包含 `testStepOrderingFeatureOn`、`testStepOrderingFeatureOff`、`testEmptySnapshotInlineEarlyReturn`、`testNoIfFilterOnInDispatcher` 4 项。

---

### [L1-测试] AT-LL4G Alternating UC 入口挂钩

**目的**：

- `AlternatingWaitingForFirstBarrierUnaligned.barrierReceived` 触发时按顺序调用 `state.onCheckpointStartedForAllInputs(unalignedBarrier)` → `controller.triggerGlobalCheckpoint(unalignedBarrier)`
- `AlternatingCollectingBarriers.alignedCheckpointTimeout` 触发时按同样顺序调用（注意：此方法不是 `barrierReceived`；`AlternatingCollectingBarriers` 没有自己的 `barrierReceived`，父类是 `final`）
- 移除挂钩调用后两个原有测试套（master 既有 UC 测试）继续 PASS——证明 master 行为不破坏

**命令**：

```bash
./mvnw -pl flink-runtime test -P java11-target -P java11 \
    -Dtest='AlternatingWaitingForFirstBarrierUnalignedDispatchHookTest,AlternatingCollectingBarriersDispatchHookTest,AlternatingControllerTest'
```

**预期结果**：3 个测试类全部 PASS。

---

### [L1-测试] AT-XVY7 `ChannelStateWriterImpl.addInputDataFromSpill`

**目的**：

- 非空 snap：提交一个 writer-thread 任务，按 channelInfo demux 调用既有 input-state-stream 写入；调用次数 = snap chunk 数
- 空 snap：in-line 早 return，writer 线程 0 调用；chunks.close() 被调用
- 任务执行抛异常：异常通过 `ChannelStateWriteResult.getInputChannelStateHandles()` future 的 exceptionally 传递；chunks.close() 仍被调用（finally）

**命令**：

```bash
./mvnw -pl flink-runtime test -P java11-target -P java11 \
    -Dtest='ChannelStateWriterImplAddInputDataFromSpillTest'
```

**预期结果**：surefire 报告全部 PASS，至少包含 `testNonEmptySnapshotAsyncDemux`、`testEmptySnapshotInlineEarlyReturn`、`testWriteFailurePropagatesViaWriteResult`、`testChunksClosedOnSuccessAndFailure` 4 项。

---

### [L1-测试] AT-BSDC `SpillFile` 引用计数器

**目的**：

- `acquire()` / `release()` 计数正确（多次 acquire/release 配对后归零）
- 归零时所有段文件被删除；删段幂等（多次 release 在归零后不重复删除、不抛错）
- abort 模拟：构造 `SpillFileReader` + N 个 DiskSnapshot，逐个 abort（exceptional `release()` 路径）后段文件全部被清理
- 强制 close()（保留入口）即使 refCount > 0 也清理段文件（测试用强制路径）

**命令**：

```bash
./mvnw -pl flink-runtime test -P java11-target -P java11 \
    -Dtest='SpillFileRefCountTest'
```

**预期结果**：surefire 报告全部 PASS，至少包含 `testAcquireReleaseCountsMatch`、`testReachingZeroDeletesSegments`、`testReleaseAfterZeroIsNoOp`、`testAbortPathReleasesViaSameRoute`、`testForceCloseStillCleansSegments` 5 项。

---

### [L1-测试] AT-T5QE UC during recovery ITCase + master regression

**目的**：

- 新 ITCase `UnalignedCheckpointDuringRecoveryITCase` 端到端运行真实 job（含 N channel、跨 task scaling、recovery 期间触发 UC）：
  - checkpoint 完成；持久化的 input channel state handles 字节集合 = 原始 channel state（通过 fixture 直接比对）
  - 同一 cpId 不会写两份持久化数据
  - 多 cpId 嵌套场景（recovery 期间多次 checkpoint）也成立
- master 既有 `UnalignedCheckpointRescaleITCase` 零修改继续 PASS（regression）

**命令**：

```bash
./mvnw -pl flink-runtime verify -P java11-target -P java11 -Pfast \
    -Dit.test='UnalignedCheckpointDuringRecoveryITCase,UnalignedCheckpointRescaleITCase' \
    -DfailIfNoTests=false
```

**预期结果**：失败用例数 = 0；新 ITCase 与 master 既有 ITCase 全部 PASS。

---

### [L1-测试] AT-FZXD Rescale + filter + large record OOM 修复回归

**目的**：

- 新 ITCase `RescaleFilterLargeRecordOOMRegressionITCase` 在固定堆大小（如 `-Xmx512m`）下复现 master OOM 场景（rescale + filter on + 大 record）
- 本 phase merge 后该 ITCase 必然 PASS（task 内存峰值稳定在 prefilter + postfilter + buffer pool 上限内）

注：本步骤只验证"本 phase 后 PASS"。"在 master 分支必然 OOM" 属于人工对照实验，由 PR 描述记录，不在本验收方案内强制执行（避免引入需要清理的远端状态）。

**命令**：

```bash
./mvnw -pl flink-runtime verify -P java11-target -P java11 -Pfast \
    -Dit.test='RescaleFilterLargeRecordOOMRegressionITCase' \
    -DfailIfNoTests=false
```

**预期结果**：该 ITCase PASS；task 堆使用峰值 < 设定阈值（具体阈值在 fixture 内定义）。

---

## 备注

- 验收 L1 步骤必须由 `flink-test-runner` sub agent 执行
- ITCase 跑时间较长，建议单独跑（不与 L1 单元测试同次执行）
- 若 mvn 报 unresolved symbol，先执行 `./mvnw clean install -U -Pfast -DskipTests -P java11-target -P java11` 重新编译再重试
- Phase 5 merge 后必须执行 `/architecture-overview` 更新项目架构文档（强制后续动作）
