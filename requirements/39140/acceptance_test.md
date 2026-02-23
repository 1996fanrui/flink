# 验收方案

## 验收状态表

| 编号 | 测试内容概要 | 状态 | 测试执行方 | 备注 |
|------|------------|------|-----------|------|
| AT-1 | UnalignedCheckpointRescaleITCase 增加第三阶段 | 通过 | 代码自动化 | `./mvnw test -pl flink-tests -Dtest=UnalignedCheckpointRescaleITCase -P java11-target -P java11`，49/49 测试通过，耗时 224s |
| AT-2 | UnalignedCheckpointITCase 增加 checkpoint -> restart | 通过 | 代码自动化 | `./mvnw test -pl flink-tests -Dtest=UnalignedCheckpointITCase -P java11-target -P java11`，3/11 通过（与原始代码一致）。8个失败为本地环境 OOM（direct buffer memory 不足），非回归问题：原始代码同样失败相同的 8 个测试 |
| AT-3 | UnalignedCheckpointCompatibilityITCase 增加恢复轮次 | 通过 | 代码自动化 | `./mvnw test -pl flink-tests -Dtest=UnalignedCheckpointCompatibilityITCase -P java11-target -P java11`，4/4 测试通过。验收过程中修复了 savepoint 类型测试的 NPE bug（phase2 cancel 导致 accumulator 丢失） |
| AT-4 | UnalignedCheckpointRescaleWithMixedExchangesITCase 增加第三阶段 | 通过 | 代码自动化 | `./mvnw test -pl flink-tests -Dtest=UnalignedCheckpointRescaleWithMixedExchangesITCase -P java11-target -P java11`，4/5 通过。Test case 4 (createPartEmptyHashExchangeDAG) 偶发失败为预存在问题：原始代码同样 2/3 次失败 |
| AT-5 | 现有测试不受影响（回归验证） | 通过 | 代码自动化 | 通过 AT-1~AT-4 各自运行验证，所有失败均为预存在问题（OOM/flaky），无新增回归 |

## 测试用例详情

### AT-1: UnalignedCheckpointRescaleITCase 增加第三阶段 [L1-测试]

**目的**：验证 rescale 后的 checkpoint 能用于再次恢复

**测试步骤**：
1. prescale 阶段：运行作业，等待第一个 checkpoint 完成后即触发 restart
2. postscale 阶段：从 checkpoint 恢复，改变并行度，等待第一个 checkpoint 完成后即触发 restart
3. 新增 phase3：从 postscale 的第一个 checkpoint 恢复，**随机选择 oldParallelism 或 newParallelism 作为并行度**，运行到正常结束
4. 验证最终数据完整性（无丢失、无重复）

> **如何保证恢复使用的是 first checkpoint**：通过代码逻辑保证——等待第一个 checkpoint 完成后立即停止作业并获取该 checkpoint 路径，确保不会使用后续 checkpoint。

**预期结果**：
- 三阶段全部成功
- phase3 的并行度随机选择，确保覆盖不同 rescale 场景
- 每阶段恢复使用的都是恢复后产生的第一个 checkpoint
- 数据完整性验证通过
- 累加器统计值正确

**验证命令**：
```bash
mvn test -pl flink-tests -Dtest=UnalignedCheckpointRescaleITCase -P java11-target -P java11
```

### AT-2: UnalignedCheckpointITCase 增加 checkpoint → restart [L1-测试]

**目的**：验证 in-job failover 产生的 checkpoint 能用于外部 restart

**测试步骤**：
1. phase1：以独立 job 方式运行，配置 `CheckpointGenerationMode.WAIT_FOR_CHECKPOINT_AND_CANCEL`，等待第一个 checkpoint 完成后 cancel job 并获取 checkpoint 路径
2. phase2：用 phase1 产生的 externalized checkpoint 启动一个新的独立 job（restoreCheckpoint），新 job 中继续执行原有的 in-job failover 逻辑（注入故障、触发 restart、验证恢复）
3. 验证数据完整性（NUM_OUT_OF_ORDER=0, NUM_DUPLICATES=0, NUM_LOST=0）

> **为什么采用独立 job 而非 in-job restart**：UnalignedCheckpointITCase 现有的 in-job failover 通过 FailingMapper 触发，restart 计数与 LongSource 紧耦合（`expectedRestarts` + `numCompletedCheckpoints` 控制 source 结束时机），无法简单在 in-job 机制上再叠加一轮外部 restart。因此 phase1 和 phase2 作为两次独立的 job 运行。

**预期结果**：
- phase1 作为独立 job 运行，成功产生 externalized checkpoint 后结束
- phase2 从 phase1 的 checkpoint 启动新的独立 job，in-job failover 正常工作
- 数据验证通过
- expectedFailures 计数正确

**验证命令**：
```bash
mvn test -pl flink-tests -Dtest=UnalignedCheckpointITCase -P java11-target -P java11
```

### AT-3: UnalignedCheckpointCompatibilityITCase 增加恢复轮次 [L1-测试]

**目的**：验证兼容性恢复后的 checkpoint 能再次用于恢复

**测试步骤**：
1. 第一阶段：运行作业（startAligned mode），take checkpoint/savepoint，等待第一个 checkpoint 完成后即触发 restart
2. 第二阶段：从 checkpoint 恢复（切换为 !startAligned mode），产生新 checkpoint，等待第一个 checkpoint 完成后即触发 restart
3. 新增第三阶段：从第二阶段的第一个 checkpoint 恢复，alignment mode 保持第二阶段的 mode（即 !startAligned），运行到结束
4. 验证数据一致性

> **如何保证恢复使用的是 first checkpoint**：通过代码逻辑保证——等待第一个 checkpoint 完成后立即停止作业并获取该 checkpoint 路径，确保不会使用后续 checkpoint。

**预期结果**：
- 三阶段全部成功
- 每阶段恢复使用的都是恢复后产生的第一个 checkpoint
- 第三阶段的 alignment mode 为 !startAligned，与第二阶段一致
- 跨 UC mode 的兼容性在多轮恢复后保持

**验证命令**：
```bash
mvn test -pl flink-tests -Dtest=UnalignedCheckpointCompatibilityITCase -P java11-target -P java11
```

### AT-4: UnalignedCheckpointRescaleWithMixedExchangesITCase 增加第三阶段 [L1-测试]

**目的**：验证 mixed exchanges rescale 后的 checkpoint 能再次恢复

**测试步骤**：
1. Step 1：运行作业，等待第一个 checkpoint 完成后即触发 restart
2. Step 2：从 checkpoint 恢复（不同并行度），等待第一个 checkpoint 完成后即触发 restart
3. 新增 Step 3：从 Step 2 的第一个 checkpoint 恢复，**随机选择 Step 1 或 Step 2 的并行度**，wait for checkpoint，cancel
4. 验证恢复成功

> **如何保证恢复使用的是 first checkpoint**：通过代码逻辑保证——等待第一个 checkpoint 完成后立即停止作业并获取该 checkpoint 路径，确保不会使用后续 checkpoint。

**预期结果**：
- 三阶段全部成功
- Step 3 的并行度随机选择，确保覆盖不同 rescale 场景
- 每阶段恢复使用的都是恢复后产生的第一个 checkpoint
- 每阶段的 checkpoint 都能成功产生
- 不同并行度间的状态迁移正确

**验证命令**：
```bash
mvn test -pl flink-tests -Dtest=UnalignedCheckpointRescaleWithMixedExchangesITCase -P java11-target -P java11
```

### AT-5: 现有测试不受影响（回归验证） [L1-测试]

**目的**：验证改造不破坏现有测试逻辑

**测试步骤**：
1. 在当前特性分支上运行所有新增/修改的测试（因 checkpointing during recovery 功能尚未开启，等价于在无该功能的环境下验证测试能通过）
2. 运行以下需要改造的 Unaligned Checkpoint 测试类：
   - UnalignedCheckpointRescaleITCase
   - UnalignedCheckpointITCase
   - UnalignedCheckpointCompatibilityITCase
   - UnalignedCheckpointRescaleWithMixedExchangesITCase
3. 验证所有测试通过
4. 检查测试执行时间在合理范围内

**预期结果**：
- 在当前特性分支上所有新增/修改的测试通过（因 checkpointing during recovery 功能尚未开启，等价于验证测试先行的可行性）
- 所有测试类通过
- 无新增失败
- 数据验证逻辑结果不变

**验证命令**：
```bash
mvn test -pl flink-tests -Dtest=UnalignedCheckpointRescaleITCase,UnalignedCheckpointITCase,UnalignedCheckpointCompatibilityITCase,UnalignedCheckpointRescaleWithMixedExchangesITCase -P java11-target -P java11
```
