# CI 失败测试分类报告

**分支:** 38544/checkpointing-during-recovery
**日期:** 2026-03-05
**CI Build:** [#73040](https://dev.azure.com/apache-flink/apache-flink/_build/results?buildId=73040&view=results)

## 总览

- CI 失败测试类: 28 个
- 本地复现结果: **28/28 全部可稳定复现** (每个类运行 3 次, 3/3 失败)
- 无 flaky test, 无不可复现的失败
- 错误归类为 **3 个类别**, 对应 3 种不同的解决方案

## 状态总表

| # | 测试类 | 模块 | 类别 | Run1 | Run2 | Run3 | 评估 |
|---|--------|------|------|------|------|------|------|
| 1 | JobIntermediateDatasetReuseTest | flink-runtime | B | FAIL | FAIL | FAIL | REPRODUCIBLE_FAIL |
| 2 | SlotCountExceedingParallelismTest | flink-runtime | A | FAIL | FAIL | FAIL | REPRODUCIBLE_FAIL |
| 3 | SortBatchRestoreTest | flink-table-planner | A | FAIL | FAIL | FAIL | REPRODUCIBLE_FAIL |
| 4 | SortLimitBatchRestoreTest | flink-table-planner | A | FAIL | FAIL | FAIL | REPRODUCIBLE_FAIL |
| 5 | UnionBatchRestoreTest | flink-table-planner | A | FAIL | FAIL | FAIL | REPRODUCIBLE_FAIL |
| 6 | OverAggregateBatchRestoreTest | flink-table-planner | A | FAIL | FAIL | FAIL | REPRODUCIBLE_FAIL |
| 7 | ExpandBatchRestoreTest | flink-table-planner | A | FAIL | FAIL | FAIL | REPRODUCIBLE_FAIL |
| 8 | LimitBatchRestoreTest | flink-table-planner | A | FAIL | FAIL | FAIL | REPRODUCIBLE_FAIL |
| 9 | JoinBatchRestoreTest | flink-table-planner | A | FAIL | FAIL | FAIL | REPRODUCIBLE_FAIL |
| 10 | MatchRecognizeBatchRestoreTest | flink-table-planner | A | FAIL | FAIL | FAIL | REPRODUCIBLE_FAIL |
| 11 | RankBatchRestoreTest | flink-table-planner | A | FAIL | FAIL | FAIL | REPRODUCIBLE_FAIL |
| 12 | WindowTableFunctionEventTimeBatchRestoreTest | flink-table-planner | A | FAIL | FAIL | FAIL | REPRODUCIBLE_FAIL |
| 13 | TableSourceScanBatchRestoreTest | flink-table-planner | A | FAIL | FAIL | FAIL | REPRODUCIBLE_FAIL |
| 14 | FileSourceTextLinesITCase | flink-connector-files | A | FAIL | FAIL | FAIL | REPRODUCIBLE_FAIL |
| 15 | BatchExecutionFileSinkITCase | flink-connector-files | A | FAIL | FAIL | FAIL | REPRODUCIBLE_FAIL |
| 16 | BatchCompactingFileSinkITCase | flink-connector-files | A | FAIL | FAIL | FAIL | REPRODUCIBLE_FAIL |
| 17 | FileSinkSpeculativeITCase | flink-connector-files | A | FAIL | FAIL | FAIL | REPRODUCIBLE_FAIL |
| 18 | TopSpeedWindowingExampleITCase | flink-examples-streaming | A | FAIL | FAIL | FAIL | REPRODUCIBLE_FAIL |
| 19 | StreamingExamplesITCase | flink-examples-streaming | A | FAIL | FAIL | FAIL | REPRODUCIBLE_FAIL |
| 20 | ClosureCleanerITCase | flink-tests | A | FAIL | FAIL | FAIL | REPRODUCIBLE_FAIL |
| 21 | DataStreamBatchExecutionITCase | flink-tests | A | FAIL | FAIL | FAIL | REPRODUCIBLE_FAIL |
| 22 | UpsertTestDynamicTableSinkITCase | flink-tests | A | FAIL | FAIL | FAIL | REPRODUCIBLE_FAIL |
| 23 | BatchFineGrainedRecoveryITCase | flink-tests | A | FAIL | FAIL | FAIL | REPRODUCIBLE_FAIL |
| 24 | SimpleRecoveryFailureRateStrategyITBase | flink-tests | A | FAIL | FAIL | FAIL | REPRODUCIBLE_FAIL |
| 25 | SimpleRecoveryFixedDelayRestartStrategyITBase | flink-tests | A | FAIL | FAIL | FAIL | REPRODUCIBLE_FAIL |
| 26 | PipelinedRegionSchedulingITCase | flink-tests | A | FAIL | FAIL | FAIL | REPRODUCIBLE_FAIL |
| 27 | TaskManagerProcessFailureBatchRecoveryITCase | flink-tests | A | FAIL | FAIL | FAIL | REPRODUCIBLE_FAIL |
| 28 | JobManagerHAProcessFailureRecoveryITCase | flink-tests | C | FAIL | FAIL | FAIL | REPRODUCIBLE_FAIL |

## 类别 A: AbstractCompositeBuffer.getNioBufferReadable() UnsupportedOperationException

**影响范围:** 26/28 个测试类 (占 93%), 涵盖全部 5 个模块
**失败测试方法数:** ~73/80

### 错误堆栈

```
java.lang.UnsupportedOperationException
  at AbstractCompositeBuffer.getNioBufferReadable(AbstractCompositeBuffer.java:166)
  at EventSerializer.fromBuffer(EventSerializer.java:429)
  at ChannelStatePersister.parseEvent(ChannelStatePersister.java:169)
  at ChannelStatePersister.checkForBarrier(ChannelStatePersister.java:121)
  at LocalInputChannel.getNextBuffer(LocalInputChannel.java:383)
```

### 根因分析

当前分支引入了 composite buffer, 但 `AbstractCompositeBuffer.getNioBufferReadable()` 抛出 `UnsupportedOperationException`。当 `LocalInputChannel.getNextBuffer()` 返回 composite buffer 时, `ChannelStatePersister.checkForBarrier()` 调用 `EventSerializer.fromBuffer()` 解析 checkpoint barrier, 该方法依赖 `buffer.getNioBufferReadable()` 获取 NIO buffer 来反序列化事件, 导致所有经过 local input channel 处理数据的 job 都失败。

### 解决方案

需要在以下两个方向之一修复:
1. **在 `AbstractCompositeBuffer` 中实现 `getNioBufferReadable()`**: 将 composite buffer 的内容合并为单个 NIO buffer 返回
2. **修改 `EventSerializer.fromBuffer()` 使其兼容 composite buffer**: 不依赖 `getNioBufferReadable()`, 改用其他 API 读取 buffer 内容

### 受影响测试类列表

- flink-runtime: SlotCountExceedingParallelismTest
- flink-table-planner: SortBatchRestoreTest, SortLimitBatchRestoreTest, UnionBatchRestoreTest, OverAggregateBatchRestoreTest, ExpandBatchRestoreTest, LimitBatchRestoreTest, JoinBatchRestoreTest, MatchRecognizeBatchRestoreTest, RankBatchRestoreTest, WindowTableFunctionEventTimeBatchRestoreTest, TableSourceScanBatchRestoreTest
- flink-connector-files: FileSourceTextLinesITCase, BatchExecutionFileSinkITCase, BatchCompactingFileSinkITCase, FileSinkSpeculativeITCase
- flink-examples-streaming: TopSpeedWindowingExampleITCase, StreamingExamplesITCase
- flink-tests: ClosureCleanerITCase, DataStreamBatchExecutionITCase, UpsertTestDynamicTableSinkITCase, BatchFineGrainedRecoveryITCase, SimpleRecoveryFailureRateStrategyITBase, SimpleRecoveryFixedDelayRestartStrategyITBase, PipelinedRegionSchedulingITCase, TaskManagerProcessFailureBatchRecoveryITCase

---

## 类别 B: JobIntermediateDatasetReuseTest 断言失败

**影响范围:** 1/28 个测试类
**失败测试方法数:** 5/80

### 错误信息

```
AssertionError: Expecting value to be true but was false
  at JobIntermediateDatasetReuseTest.internalTestClusterPartitionReuse:108
```

### 根因分析

5 个测试方法全部在同一个断言点 (`internalTestClusterPartitionReuse:108`) 失败, 表明 cluster partition reuse 逻辑在当前分支被破坏。错误模式与类别 A 不同 — 这里不是 `UnsupportedOperationException`, 而是 partition reuse 的验证条件不满足。

需要进一步分析 `internalTestClusterPartitionReuse` 第 108 行的断言逻辑, 以及当前分支对 partition reuse 的影响。

### 解决方案

需要单独排查:
1. 检查当前分支对 `IntermediateDataSet` / cluster partition 相关代码的改动
2. 分析 `internalTestClusterPartitionReuse:108` 处断言的具体条件
3. 可能与类别 A 存在间接关联 (composite buffer 导致 job 执行异常, 使 partition 未被正确注册)

### 受影响测试类列表

- flink-runtime: JobIntermediateDatasetReuseTest

---

## 类别 C: JobManagerHAProcessFailureRecoveryITCase 环境依赖问题

**影响范围:** 1/28 个测试类
**失败测试方法数:** 1/80

### 错误信息

```
AssertionError: The program encountered a ProgramInvocationException: Job failed
ClassNotFoundException: org.apache.hadoop.hdfs.HdfsConfiguration
```

### 根因分析

该测试需要 Hadoop HDFS 依赖, 本地环境缺少该依赖。此外该测试会启动独立 JVM 进程模拟 JobManager HA 故障恢复, 运行时间约 5 分钟/次。

本地环境缺少 Hadoop HDFS classpath 是主要原因, 可能与类别 A 的 composite buffer 问题叠加。

### 解决方案

1. 确认 CI 环境是否提供 Hadoop HDFS 依赖 — 如果 CI 也缺少, 则这是一个独立的环境配置问题
2. 如果 CI 有 Hadoop 依赖, 则 HDFS 问题仅是本地复现时的干扰, 实际 CI 失败原因可能也是类别 A 的 composite buffer 问题
3. 优先解决类别 A 后再单独验证此测试

### 受影响测试类列表

- flink-tests: JobManagerHAProcessFailureRecoveryITCase

---

## 修复优先级

| 优先级 | 类别 | 影响范围 | 修复后预期解决 |
|--------|------|---------|---------------|
| P0 | A - CompositeBuffer | 26 个类, ~73 个方法 | 修复后预期解决 93% 的失败 |
| P1 | B - PartitionReuse | 1 个类, 5 个方法 | 可能被 P0 间接解决, 需验证 |
| P2 | C - Hadoop依赖 | 1 个类, 1 个方法 | 需单独排查环境问题 |

