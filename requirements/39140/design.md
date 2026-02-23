# 设计文档：增强 Unaligned Checkpoint ITCase 多次重启验证

## 背景与目标

当前 Unaligned Checkpoint ITCase 只验证从正常 checkpoint 恢复的场景。需要让每个测试多做一次 stop-restart-from-checkpoint，验证从 checkpoint 恢复后产生的新 checkpoint 也能正确用于恢复。当 checkpointing during recovery 功能上线后，这些测试自动覆盖 recovery checkpoint 场景。

## 现有代码分析

### 各 ITCase 架构差异

6 个测试分为两类架构，不共享统一的 restart 机制：

**继承 `UnalignedCheckpointTestBase` 的测试（2 个）**：
- **UnalignedCheckpointITCase**：使用 in-job failover（`FailingMapper` 在 `snapshotState()` 时触发 `TestException`，由 `FixedDelayRestartStrategy` 处理）。`LongSource` 通过 `expectedRestarts` + `numCompletedCheckpoints` 计数控制 source 结束时机。单次 `execute()` 调用完成全部逻辑
- **UnalignedCheckpointRescaleITCase**：两阶段外部 stop-restart。prescale 阶段 `CheckpointGenerationMode.WAIT_FOR_JOB_RESULT` 模式 + `expectedFinalJobStatus=FAILED`，postscale 阶段 `setRestoreCheckpoint(checkpointDir)` 恢复。已经是 stop-restart-from-checkpoint 模式

**独立实现的测试（4 个）**：
- **UnalignedCheckpointCompatibilityITCase**：两阶段，run → take checkpoint/savepoint → restore with different UC alignment mode
- **UnalignedCheckpointStressITCase**：**已有多轮 stop-restart 循环**。`runStressTest()` 在 `while (deadline.hasTimeLeft())` 循环中反复调用 `runAndTakeExternalCheckpoint()`
- **UnalignedCheckpointFailureHandlingITCase**：测试 checkpoint 存储层的故障处理（`CheckpointStateOutputStream.close()` 失败后能否成功 checkpoint），不涉及 job restart/recovery
- **UnalignedCheckpointRescaleWithMixedExchangesITCase**：两阶段，run → take checkpoint → restore with different parallelism

### 通用方案不可行的原因

- 只有 2/6 个测试继承 `UnalignedCheckpointTestBase`，在基类上做通用机制无法覆盖另外 4 个测试
- `UnalignedCheckpointTestBase` 的 in-job failover 机制（`FailingMapper` + `LongSource.expectedRestarts` + `FixedDelayRestartStrategy`）与外部 stop-restart 是两套不同机制，强行叠加会导致 restart 计数冲突和 source 提前/延迟结束
- 各测试的拓扑、failure 策略、验证逻辑完全不同，无法用统一的 `restartRounds`/`restartTrigger` 配置

## 设计原则

1. **case by case 适配**：每个测试基于现有逻辑单独增加一轮 stop-restart-from-checkpoint
2. **渐进式验证**：方案必须在当前 master 上运行通过，当 checkpointing during recovery 功能开启后自动覆盖新场景
3. **最小改动**：不改变测试的现有逻辑，仅在最后增加一轮 checkpoint → restart → verify
4. **first checkpoint 保证**：每阶段恢复使用的必须是恢复后产生的第一个 checkpoint。通过代码逻辑保证——等待第一个 checkpoint 完成后立即停止作业并获取该 checkpoint 路径，确保不会使用后续 checkpoint

## 各测试适配方案

### 1. UnalignedCheckpointITCase

**现状**：单次 `execute()` 调用，内部通过 FailingMapper 触发 5 次 in-job failover

**改造**：直接在 `settings` 上切换 `CheckpointGenerationMode`，避免创建新的 `UnalignedSettings` 对象：
- Phase 1：`settings.setCheckpointGenerationMode(WAIT_FOR_CHECKPOINT_AND_CANCEL)` → 执行 → 产生 checkpoint 后 cancel
- Phase 2：`settings.setCheckpointGenerationMode(NONE).setRestoreCheckpoint(path)` → 从 checkpoint 恢复 → 执行原有逻辑（含 FailingMapper 5 次 failover）

Phase 1 使用 `WAIT_FOR_CHECKPOINT_AND_CANCEL` 模式，不需要 FailingMapper 触发失败，不涉及 `minCheckpoints` 和 `expectedFailures` 的交互。FailingMapper 在 Phase 1 中不会触发是因为 `WAIT_FOR_CHECKPOINT_AND_CANCEL` 模式在第一个 checkpoint 完成后立即 cancel job，FailingMapper 没有机会积累足够的 `completedCheckpoints`。

### 2. UnalignedCheckpointRescaleITCase

**现状**：prescale 阶段生成 checkpoint → postscale 阶段从 checkpoint 恢复并运行到结束

**改造**：详细方案见 `design_rescale_itcase.md`。核心思路：
- 插入 phase2（postscale-checkpoint）：从 prescale checkpoint 恢复，使用 `WAIT_FOR_CHECKPOINT_AND_CANCEL` 模式产生 checkpoint
- 新增 phase3：从 phase2 的 checkpoint 恢复，**随机选择 oldParallelism 或 newParallelism 作为并行度**，正常运行到结束

**已知问题：`minCheckpoints=Integer.MAX_VALUE` 的 hack**

Phase 2 使用 `setMinCheckpoints(Integer.MAX_VALUE)` 来间接禁用 FailingMapper。这是因为 FailingMapper 的 fail 条件依赖 `minCheckpoints`（`completedCheckpoints >= minCheckpoints/2 && runNumber == 0`），从 Phase 1 checkpoint 恢复后 `completedCheckpoints` 已有累计值且新 Job 的 `runNumber=0`，会立刻触发 fail。

这种方式存在可读性问题：用一个参数（`minCheckpoints`）的副作用来实现另一个语义（禁用 FailingMapper）。读代码需要理解整条间接链路才能明白意图。

**待优化方向**：引入独立的 `failingEnabled` 参数（或将 FailingMapper 的 fail 条件与 `minCheckpoints` 解耦），使 Phase 2 中禁用 FailingMapper 的意图显式化。根本原因是 `minCheckpoints` 同时承载了"source/sink 运行时长控制"和"FailingMapper 触发门槛"两个语义，`WAIT_FOR_CHECKPOINT_AND_CANCEL` 模式只需要"不要 fail"，但不得不通过抬高整体 `minCheckpoints` 来实现。

### 3. UnalignedCheckpointCompatibilityITCase

**现状**：run → take checkpoint/savepoint → restore with different UC mode → verify

**改造**：在 restore 后增加一轮：
- 第二阶段（restore phase）结束前 take 一个 checkpoint
- 新增第三阶段：从第二阶段的 checkpoint 恢复，运行到结束并验证数据一致性

### 4. UnalignedCheckpointStressITCase

**现状**：已在 `while` 循环中反复做 stop-restart-from-checkpoint，天然覆盖"从恢复后的 checkpoint 恢复"场景

**改造**：不需要额外改造。已满足需求

### 5. UnalignedCheckpointFailureHandlingITCase

**现状**：测试 checkpoint 存储故障处理，使用 `JobManagerCheckpointStorage`（内存存储），不支持 externalized checkpoint 恢复

**改造**：**排除**。该测试的 checkpoint 存储不支持 externalized checkpoint 恢复，强行改造会改变测试本质。详见 user_requirements.md 需求偏离章节

### 6. UnalignedCheckpointRescaleWithMixedExchangesITCase

**现状**：run → take checkpoint → restore with different parallelism → take checkpoint → cancel

**改造**：在 Step 2 后增加 Step 3：
- Step 2 结束时已有一个 checkpoint
- 新增 Step 3：从 Step 2 的 checkpoint 恢复，**随机并行度**，wait for checkpoint，cancel
- 验证第二次恢复成功且能产生有效 checkpoint

## 实现路径

### Phase 1：已覆盖的测试确认
1. 确认 `UnalignedCheckpointStressITCase` 已天然覆盖

### Phase 2：两阶段测试增加第三阶段
1. 改造 `UnalignedCheckpointRescaleITCase`
2. 改造 `UnalignedCheckpointRescaleWithMixedExchangesITCase`
3. 改造 `UnalignedCheckpointCompatibilityITCase`

### Phase 3：单阶段测试增加 checkpoint → restart
1. 改造 `UnalignedCheckpointITCase`

## 兼容性考虑

1. 所有改造不影响测试的现有验证逻辑
2. 方案在当前 master（无 checkpointing during recovery）上即可运行
3. 当 checkpointing during recovery 功能开启后，recovery 期间产生的 checkpoint 自动成为恢复点

## 风险与缓解

| 风险 | 影响 | 缓解措施 |
|-----|------|---------|
| 测试时间增长 | CI 执行时间延长 | 每个测试仅增加一轮 restart |
