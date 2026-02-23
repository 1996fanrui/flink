# UnalignedCheckpointITCase 改造方案

## 1. 改造思路

直接在 `settings` 对象上切换 `CheckpointGenerationMode`，两阶段执行：

- **Phase 1**：`settings.setCheckpointGenerationMode(WAIT_FOR_CHECKPOINT_AND_CANCEL)` → 执行 → 等待第一个 checkpoint 完成后 cancel → 返回 checkpoint 路径
- **Phase 2**：`settings.setCheckpointGenerationMode(NONE).setRestoreCheckpoint(path)` → 从 checkpoint 恢复 → 执行原有逻辑

`settings` 是 `private final` 的，每个 parameterized test instance 独有，无复用问题，可以安全修改。

## 2. Phase 1 中 FailingMapper 不会触发的原因

FailingMapper 的触发条件依赖 `completedCheckpoints` 的累积（如 `completedCheckpoints >= minCheckpoints/4`，即 `>= 2`）。

`WAIT_FOR_CHECKPOINT_AND_CANCEL` 模式在**第一个** checkpoint 完成后立即 cancel job。此时 `completedCheckpoints` 刚从 0 变为 1，不满足任何失败条件。FailingMapper 没有机会触发。

## 3. Phase 2 中 FailingMapper 的行为

从 Phase 1 checkpoint 恢复后：
- `completedCheckpoints` = 0（Phase 1 只完成了 1 个 checkpoint，但 FailingMapper 的 `notifyCheckpointComplete` 在 checkpoint 完成时才递增。从 checkpoint 恢复的值是 snapshot 时的值，即 0）

  注意：FailingMapper 在 `snapshotState()` 后、`notifyCheckpointComplete()` 前做 checkpoint。所以 checkpoint 中保存的 `completedCheckpoints` 值是 snapshot 时刻的值（0），而不是 checkpoint 完成后的值（1）。

- `runNumber = 0`（新 Job 的 attemptNumber 从 0 开始）

因此 Phase 2 中 FailingMapper 的行为与原始单次执行完全一致：从 `completedCheckpoints=0, runNumber=0` 开始，经历 5 次 failover 序列。

## 4. checkCounters 验证

- Phase 1：`WAIT_FOR_CHECKPOINT_AND_CANCEL` 模式不调用 `checkCounters()`
- Phase 2：`NUM_FAILURES` = sink subtask 0 的 `attemptNumber` = 5，匹配 `settings.expectedFailures = 5`
