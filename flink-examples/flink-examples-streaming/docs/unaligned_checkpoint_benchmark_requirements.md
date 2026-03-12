# Unaligned Checkpoint Benchmark 需求文档

## 背景

基于现有的 `UnalignedCheckpointDemo`，我们需要一个基准测试工具，用于衡量 Unaligned Checkpoint 在不同并行度（含 rescale）场景下的两项核心指标：
1. Checkpoint 耗时
2. Task 初始化（INITIALIZING）耗时

工具运行真实的 Flink 作业（MiniCluster 本地模式），不是单元测试。

## 运行模式

统一使用并行度计划数组，不区分 rescale 与 no-rescale 模式：

- 输入一个二维数组 `parallelismPlan`，每组包含 `[upstreamParallelism, downstreamParallelism]`
- 每轮流程：
  1. 以当前轮的并行度提交作业（首轮从零启动，后续轮从 savepoint 恢复）
  2. 等待产生 3 个成功的 checkpoint
  3. 执行 stop-with-savepoint（最后一轮直接 cancel）
  4. 以下一轮的并行度从 savepoint 重新提交作业
- 共执行 N 轮（N = parallelismPlan 的长度）

**Rescale 示例**：`5,5;10,3;3,10;7,7` 表示 4 轮，每轮上下游并行度不同。
**No-Rescale 示例**：`5,5;5,5;5,5;5,5;5,5` 表示 5 轮，每轮并行度固定为 5。

## 作业拓扑

复用 `UnalignedCheckpointDemo` 的拓扑结构，支持上下游算子设置不同并行度：

```
DataGeneratorSource (upstreamParallelism)
  → rebalance()
  → map(sleep 20ms) (downstreamParallelism)
  → DiscardingSink
```

## 采集指标

### REQ-C1: Checkpoint 耗时

- 每轮作业等待 3 个成功 checkpoint
- 记录每个 checkpoint 的 `duration`（总耗时，毫秒）
- 数据来源：REST API `GET /jobs/{jobId}/checkpoints`

**每轮输出**：
| 字段 | 说明 |
|------|------|
| round | 轮次序号 |
| upstream_parallelism | 上游并行度 |
| downstream_parallelism | 下游并行度 |
| checkpoint_1_duration_ms | 第 1 个 checkpoint 耗时 |
| checkpoint_2_duration_ms | 第 2 个 checkpoint 耗时 |
| checkpoint_3_duration_ms | 第 3 个 checkpoint 耗时 |
| avg_checkpoint_duration_ms | 3 个 checkpoint 平均耗时 |

**汇总输出**：所有轮次的 checkpoint 耗时的 avg / p95 / max。

### REQ-C2: Task 初始化耗时

- 每轮作业通过 REST API 获取各 vertex 的 INITIALIZING 状态时间统计
- 数据来源：REST API `GET /jobs/{jobId}/vertices/{vertexId}`
- 响应中的 `tasks-per-state` 下的 `INITIALIZING` 字段包含 p25 / min / avg / median / max / sum / p95 / p75

**每轮输出**：
| 字段 | 说明 |
|------|------|
| round | 轮次序号 |
| upstream_parallelism | 上游并行度 |
| downstream_parallelism | 下游并行度 |
| vertex_name | 算子名称 |
| init_min | INITIALIZING 最小耗时 |
| init_avg | INITIALIZING 平均耗时 |
| init_median | INITIALIZING 中位数 |
| init_max | INITIALIZING 最大耗时 |
| init_p25 | INITIALIZING p25 |
| init_p75 | INITIALIZING p75 |
| init_p95 | INITIALIZING p95 |
| init_sum | INITIALIZING 总耗时 |

**说明**：每个作业只会初始化一次，所以每轮只有一组初始化数据。如果有 10 轮，就有 10 组（每组包含各 vertex 的初始化数据）。

## 参数设计

通过 main args 传入一个参数，表示并行度计划：

- 格式：`上游,下游;上游,下游;...`，分号分隔每轮，逗号分隔上下游并行度
- **不传参数**：默认 `5,5`（1 轮，并行度 5）
- **No-Rescale 示例**：`5,5;5,5;5,5;5,5;5,5`（5 轮固定并行度）
- **Rescale 示例**：`5,5;10,3;3,10;7,7`（4 轮变并行度）

其他配置（REST 端口 12345、作业拓扑、输出目录 `/tmp/benchmark_result`）均为硬编码常量。

## 输出文件

在 `--output` 指定的目录下生成两个 CSV 文件：

1. **checkpoint_results.csv**：每轮的 checkpoint 耗时 + 汇总行
2. **initialization_results.csv**：每轮各 vertex 的 INITIALIZING 时间统计

## 运行环境

- MiniCluster 本地模式（`createLocalEnvironmentWithWebUI`）
- Unaligned Checkpoint 开启
- Adaptive Scheduler
- REST API 可访问（用于指标采集）
- 文件放置于 `UnalignedCheckpointDemo.java` 同目录

## 约束

- 每轮作业必须产生至少 3 个成功 checkpoint 后才能触发 stop/结束
- Rescale 模式下通过 stop-with-savepoint 停止作业，再以新并行度从 savepoint 恢复
- 使用 Java 标准库调用 REST API（`java.net.HttpURLConnection`），不引入额外 HTTP 依赖
