# 需求偏离

| 需求编号 | 原因 | 替代方案 |
|---------|------|---------|
| 1（FailureHandlingITCase 部分） | 该测试使用 JobManagerCheckpointStorage（内存存储），不支持 externalized checkpoint 恢复，强行改造会改变测试本质 | 从改造范围中排除该测试 |

# 用户需求

## 背景

当前分支正在支持 checkpointing during recovery (FLIP-547 / FLINK-38543)。现有的 Unaligned Checkpoint ITCase 大部分只 rescale/restart 一次，只验证了 job 从一个 normal unaligned checkpoint 恢复的场景。无法验证从 recovery 期间产生的 checkpoint 恢复的正确性。

## 核心需求

1. 每个 Unaligned Checkpoint ITCase 在现有基础上多 restart 一次，保证是从 first checkpoint 恢复即可。不需要通用的 restartRounds 和 restartTrigger 配置
2. 测试先行：方案要能在当前 master 上运行通过，当 checkpointing during recovery 功能上线后，同一套测试自动覆盖 recovery checkpoint 的恢复场景
3. 方案要能适用于现有所有 Unaligned Checkpoint 相关的 ITCase，每个测试 case by case 适配
4. 对于测试 rescale 的 ITCase（如 RescaleITCase、RescaleWithMixedExchangesITCase），新增阶段的并行度应随机选择 prescale 或 postscale 的并行度，确保覆盖不同并行度的 rescale 场景。不测试 rescale 的测试保持相同并行度
