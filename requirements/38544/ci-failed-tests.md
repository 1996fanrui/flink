# CI Build #73040 Failed Tests

- **Build**: [#20260305.30](https://dev.azure.com/apache-flink/apache-flink/_build/results?buildId=73040&view=results)
- **PR**: [FLINK-39018][network] Fix LocalInputChannel priority event and buffer availability for recovered buffers
- **Total tests**: 136,112 (133,687 Passed, 80 Failed, 2,345 Others)
- **Pass percentage**: 98.21%
- **Failed classes**: 30
- **Failed test methods**: 80

## Failed Tests (Class + Method)

### org.apache.flink.runtime.jobmaster.JobIntermediateDatasetReuseTest (5 failures)

| # | Method |
|---|--------|
| 1 | `testClusterPartitionReuse` |
| 2 | `testClusterPartitionReuseMultipleParallelism` |
| 3 | `testClusterPartitionReuseWithLessConsumerParallelismThrowException` |
| 4 | `testClusterPartitionReuseWithMoreConsumerParallelismThrowException` |
| 5 | `testClusterPartitionReuseWithTMFail` |

### org.apache.flink.runtime.jobmanager.SlotCountExceedingParallelismTest (3 failures)

| # | Method |
|---|--------|
| 6 | `testNoSlotSharingAndBlockingResultBoth` |
| 7 | `testNoSlotSharingAndBlockingResultReceiver` |
| 8 | `testNoSlotSharingAndBlockingResultSender` |

### org.apache.flink.table.planner.plan.nodes.exec.batch.*RestoreTest (37 failures)

| # | Class | Method |
|---|-------|--------|
| 9 | `SortBatchRestoreTest` | `loadAndRunCompiledPlan(...)[1]` |
| 10 | `SortBatchRestoreTest` | `loadAndRunCompiledPlan(...)[2]` |
| 11 | `SortLimitBatchRestoreTest` | `loadAndRunCompiledPlan(...)[1]` |
| 12 | `SortLimitBatchRestoreTest` | `loadAndRunCompiledPlan(...)[2]` |
| 13 | `UnionBatchRestoreTest` | `loadAndRunCompiledPlan(...)[1]` |
| 14 | `UnionBatchRestoreTest` | `loadAndRunCompiledPlan(...)[2]` |
| 15 | `UnionBatchRestoreTest` | `loadAndRunCompiledPlan(...)[3]` |
| 16 | `OverAggregateBatchRestoreTest` | `loadAndRunCompiledPlan(...)[1]` |
| 17 | `OverAggregateBatchRestoreTest` | `loadAndRunCompiledPlan(...)[2]` |
| 18 | `ExpandBatchRestoreTest` | `loadAndRunCompiledPlan(...)[1]` |
| 19 | `LimitBatchRestoreTest` | `loadAndRunCompiledPlan(...)[1]` |
| 20 | `JoinBatchRestoreTest` | `loadAndRunCompiledPlan(...)[1]` |
| 21 | `JoinBatchRestoreTest` | `loadAndRunCompiledPlan(...)[2]` |
| 22 | `JoinBatchRestoreTest` | `loadAndRunCompiledPlan(...)[3]` |
| 23 | `JoinBatchRestoreTest` | `loadAndRunCompiledPlan(...)[4]` |
| 24 | `JoinBatchRestoreTest` | `loadAndRunCompiledPlan(...)[5]` |
| 25 | `JoinBatchRestoreTest` | `loadAndRunCompiledPlan(...)[6]` |
| 26 | `JoinBatchRestoreTest` | `loadAndRunCompiledPlan(...)[7]` |
| 27 | `JoinBatchRestoreTest` | `loadAndRunCompiledPlan(...)[8]` |
| 28 | `JoinBatchRestoreTest` | `loadAndRunCompiledPlan(...)[9]` |
| 29 | `MatchRecognizeBatchRestoreTest` | `loadAndRunCompiledPlan(...)[1]` |
| 30 | `MatchRecognizeBatchRestoreTest` | `loadAndRunCompiledPlan(...)[2]` |
| 31 | `MatchRecognizeBatchRestoreTest` | `loadAndRunCompiledPlan(...)[3]` |
| 32 | `MatchRecognizeBatchRestoreTest` | `loadAndRunCompiledPlan(...)[4]` |
| 33 | `MatchRecognizeBatchRestoreTest` | `loadAndRunCompiledPlan(...)[5]` |
| 34 | `MatchRecognizeBatchRestoreTest` | `loadAndRunCompiledPlan(...)[6]` |
| 35 | `MatchRecognizeBatchRestoreTest` | `loadAndRunCompiledPlan(...)[7]` |
| 36 | `MatchRecognizeBatchRestoreTest` | `loadAndRunCompiledPlan(...)[8]` |
| 37 | `RankBatchRestoreTest` | `loadAndRunCompiledPlan(...)[1]` |
| 38 | `RankBatchRestoreTest` | `loadAndRunCompiledPlan(...)[2]` |
| 39 | `RankBatchRestoreTest` | `loadAndRunCompiledPlan(...)[3]` |
| 40 | `RankBatchRestoreTest` | `loadAndRunCompiledPlan(...)[4]` |
| 41 | `WindowTableFunctionEventTimeBatchRestoreTest` | `loadAndRunCompiledPlan(...)[2]` |
| 42 | `WindowTableFunctionEventTimeBatchRestoreTest` | `loadAndRunCompiledPlan(...)[4]` |
| 43 | `WindowTableFunctionEventTimeBatchRestoreTest` | `loadAndRunCompiledPlan(...)[6]` |
| 44 | `WindowTableFunctionEventTimeBatchRestoreTest` | `loadAndRunCompiledPlan(...)[9]` |
| 45 | `TableSourceScanBatchRestoreTest` | `loadAndRunCompiledPlan(...)[4]` |

### org.apache.flink.connector.file (6 failures)

| # | Class | Method |
|---|-------|--------|
| 46 | `FileSourceTextLinesITCase` | `testBoundedTextFileSourceWithDynamicParallelismInference(Path, MiniCluster)` |
| 47 | `BatchExecutionFileSinkITCase` | `testFileSink(boolean, Path)[1]` |
| 48 | `BatchExecutionFileSinkITCase` | `testFileSink(boolean, Path)[2]` |
| 49 | `BatchCompactingFileSinkITCase` | `testFileSink(boolean, Path)[1]` |
| 50 | `BatchCompactingFileSinkITCase` | `testFileSink(boolean, Path)[2]` |
| 51 | `FileSinkSpeculativeITCase` | `testFileSinkSpeculative` |

### org.apache.flink.streaming.test (3 failures)

| # | Class | Method |
|---|-------|--------|
| 52 | `TopSpeedWindowingExampleITCase` | `testTopSpeedWindowingExampleITCase` |
| 53 | `StreamingExamplesITCase` | `testWordCount[0]` |
| 54 | `StreamingExamplesITCase` | `testWordCount[1]` |

### org.apache.flink.api.functions.ClosureCleanerITCase (4 failures)

| # | Method |
|---|--------|
| 55 | `testClass` |
| 56 | `testClassWithoutDefaulConstructor` |
| 57 | `testClassWithoutFieldAccess` |
| 58 | `testObject` |

### org.apache.flink.api.datastream.DataStreamBatchExecutionITCase (11 failures)

| # | Method |
|---|--------|
| 59 | `batchBroadcastExecution` |
| 60 | `batchFailoverWithKeyByBarrier` |
| 61 | `batchFailoverWithRebalanceBarrier` |
| 62 | `batchFailoverWithRescaleBarrier` |
| 63 | `batchKeyedBroadcastExecution` |
| 64 | `batchKeyedNonKeyedTwoInputOperator` |
| 65 | `batchMixedKeyedAndNonKeyedMultiInputOperator` |
| 66 | `batchMixedKeyedAndNonKeyedTwoInputOperator` |
| 67 | `batchNonKeyedKeyedTwoInputOperator` |
| 68 | `batchReduceSingleResultPerKey` |
| 69 | `batchSumSingleResultPerKey` |

### org.apache.flink.connector.upserttest.table.UpsertTestDynamicTableSinkITCase (1 failure)

| # | Method |
|---|--------|
| 70 | `testWritingDocumentsInBatchMode(File)` |

### org.apache.flink.test.recovery (8 failures)

| # | Class | Method |
|---|-------|--------|
| 71 | `JobManagerHAProcessFailureRecoveryITCase` | `testDispatcherProcessFailure` |
| 72 | `BatchFineGrainedRecoveryITCase` | `testProgram` |
| 73 | `SimpleRecoveryFailureRateStrategyITBase` | `testFailedRunThenSuccessfulRun` |
| 74 | `SimpleRecoveryFailureRateStrategyITBase` | `testRestart` |
| 75 | `SimpleRecoveryFailureRateStrategyITBase` | `testRestartMultipleTimes` |
| 76 | `SimpleRecoveryFixedDelayRestartStrategyITBase` | `testFailedRunThenSuccessfulRun` |
| 77 | `SimpleRecoveryFixedDelayRestartStrategyITBase` | `testRestart` |
| 78 | `SimpleRecoveryFixedDelayRestartStrategyITBase` | `testRestartMultipleTimes` |

### org.apache.flink.test.scheduling.PipelinedRegionSchedulingITCase (1 failure)

| # | Method |
|---|--------|
| 79 | `testSuccessWithSlotsNoFewerThanTheMaxRegionRequired` |

### org.apache.flink.test.recovery.TaskManagerProcessFailureBatchRecoveryITCase (1 failure)

| # | Method |
|---|--------|
| 80 | `testTaskManagerProcessFailure` |

## Failed CI Jobs

| Job | Status |
|-----|--------|
| e2e_1_ci | Failed (Run e2e tests - exit code 1) |
| e2e_2_ci | Failed (Run e2e tests - exit code 1) |
| test_ci core | Failed (Test - core - exit code 1) |
| test_ci python | Failed (exit code 1) |
| test_ci table | Failed (exit code 1) |
| test_ci connect | Failed (exit code 1) |
| test_ci tests | Failed (exit code 143) |
| test_ci misc | Failed (exit code 1) |
