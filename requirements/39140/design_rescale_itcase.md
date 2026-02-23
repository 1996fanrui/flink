# UnalignedCheckpointRescaleITCase 改造方案

## 一、现有代码分析

### 1.1 测试总体流程（改造前）

`shouldRescaleUnalignedCheckpoint()` 由两个阶段组成：

- **Prescale 阶段**：以 `oldParallelism` 运行 job，`CheckpointGenerationMode.WAIT_FOR_JOB_RESULT` 模式，`expectedFinalJobStatus=FAILED`。FailingMapper 在完成足够多 checkpoint 后触发失败，restart 策略配为 0 次重试，job 直接失败。`execute()` 获取最后一个完成的 checkpoint 路径返回
- **Postscale 阶段**：以 `newParallelism` 运行 job，从 prescale 的 checkpoint 恢复，`expectedFinalJobStatus=FINISHED`。FailingMapper 立即触发一次失败（`completedCheckpoints` 从 checkpoint 恢复后满足条件，`runNumber=0`），restart 后 `runNumber=1`，不再满足失败条件，job 正常完成

### 1.2 FailingMapper 状态逻辑

`FailingMapper` 持有 `FailingMapperState`（`completedCheckpoints` + `runNumber`），存储为 operator list state，仅 subtask 0 持有状态。

- `completedCheckpoints`：通过 `ListState` 持久化，跨 restart 累积，跨 job 从 checkpoint 恢复
- `runNumber`：**不持久化**，每次 `initializeState()` 设为 `getRuntimeContext().getTaskInfo().getAttemptNumber()`
  - 同一 Job 内 failover：attemptNumber 递增（0→1→2...）
  - **新提交的 Job**（即使从 checkpoint 恢复）：attemptNumber 从 0 重新开始

RescaleITCase 中 `getFailingMapper(minCheckpoints)` 的失败条件：
```
failDuringSnapshot: completedCheckpoints >= minCheckpoints/2 && runNumber == 0
```

### 1.3 LongSource 的终止逻辑

`LongSourceReader.updatePollingState()` 中：
```
numCompletedCheckpoints >= minCheckpoints && numRestarts >= expectedRestarts → finishing = true
```

`numCompletedCheckpoints` 和 `numRestarts` 不持久化在 SourceReader 内部，但通过 Enumerator 的 `EnumeratorState` 持久化到 checkpoint。`minCheckpoints` 和 `expectedRestarts` 是构造参数，不序列化到 state 中。

## 二、改造方案

### 2.1 三阶段执行

| 阶段 | 目的 | 并行度 | 模式 | 预期状态 |
|------|------|--------|------|---------|
| Phase 1 (prescale) | 产生初始 checkpoint | oldParallelism | `WAIT_FOR_JOB_RESULT` + FAILED | FAILED |
| Phase 2 (postscale-cp) | 从 checkpoint1 恢复并产生新 checkpoint | newParallelism | `WAIT_FOR_CHECKPOINT_AND_CANCEL` | CANCELLED |
| Phase 3 (recovery) | 从 checkpoint2 恢复并运行到完成 | 随机选择 old 或 new | restoreCheckpoint + FINISHED | FINISHED |

### 2.2 Phase 2 中 FailingMapper 的问题

Phase 2 从 Phase 1 checkpoint 恢复后：
- `completedCheckpoints` >= 5（从 checkpoint 累积）
- `runNumber = 0`（新 Job 的 attemptNumber 从 0 开始）
- 失败条件 `completedCheckpoints >= minCheckpoints/2 && runNumber == 0` **立刻满足**
- 如果不处理，FailingMapper 会在 Phase 2 的第一次 snapshotState 中抛出异常，导致 checkpoint 失败

### 2.3 当前方案：`minCheckpoints=Integer.MAX_VALUE`

Phase 2 设置 `setMinCheckpoints(Integer.MAX_VALUE)`，使 `completedCheckpoints >= Integer.MAX_VALUE/2` 永远不成立，FailingMapper 永不触发。

**问题**：这种方式用 `minCheckpoints` 参数的副作用来实现"禁用 FailingMapper"的语义。`minCheckpoints` 同时承载了三个语义：
1. Source 终止条件（`numCompletedCheckpoints >= minCheckpoints`）
2. Sink backpressure 控制（`completedCheckpoints < minCheckpoints` 时制造 backpressure）
3. FailingMapper 触发门槛（`completedCheckpoints >= minCheckpoints/2`）

`CheckpointGenerationMode.WAIT_FOR_CHECKPOINT_AND_CANCEL` 模式只需要效果 3（禁用 fail），但连带影响了 1 和 2。效果 1 和 2 在此模式下无害（job 会被外部 cancel，不需要 source 自行 finish），但代码意图不清晰。

### 2.4 待优化方向

引入独立参数将 FailingMapper 的控制与 `minCheckpoints` 解耦。例如在 `getFailingMapper()` 中增加 `failingEnabled` 参数，当 `CheckpointGenerationMode.WAIT_FOR_CHECKPOINT_AND_CANCEL` 模式时传 `false`。这样代码意图显式化，不需要通过理解 `minCheckpoints → getFailingMapper → fail 条件 → Integer.MAX_VALUE/2 永远不满足` 这条间接链路。

## 三、状态兼容性

Phase 2 使用不同的 `minCheckpoints` 构建 DAG 不影响 checkpoint 兼容性。`minCheckpoints` 是构造参数，不序列化到 state 中：
- FailingMapper 状态：只有 `completedCheckpoints` 和 `runNumber`
- LongSource EnumeratorState：只有 `numRestarts`、`numCompletedCheckpoints`、`unassignedSplits`
- VerifyingSink State：只有 `numOutput`、`completedCheckpoints` 等

Phase 3 使用 `minCheckpoints=10`（默认值）恢复，完全兼容。
