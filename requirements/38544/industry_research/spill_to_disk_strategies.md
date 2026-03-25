# Spill-to-Disk 策略调研：流处理引擎与数据密集型系统

## 1. 信息来源

| 编号 | 来源 | URL |
|------|------|-----|
| S1 | Flink - Large State Tuning | https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/state/large_state_tuning/ |
| S2 | Flink - TaskManager Memory Setup | https://nightlies.apache.org/flink/flink-docs-stable/docs/deployment/memory/mem_setup_tm/ |
| S3 | Flink - Network Memory Tuning | https://nightlies.apache.org/flink/flink-docs-stable/docs/deployment/memory/network_mem_tuning/ |
| S4 | Flink - Checkpointing Under Backpressure | https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/state/checkpointing_under_backpressure/ |
| S5 | Flink - SortMergeResultPartition 源码 | https://github.com/apache/flink |
| S6 | Flink - IOManager 源码 | https://github.com/apache/flink |
| S7 | Spark - Tuning Guide | https://spark.apache.org/docs/latest/tuning.html |
| S8 | Spark - Configuration | https://spark.apache.org/docs/latest/configuration.html |
| S9 | Trino - Spill to Disk | https://trino.io/docs/current/admin/spill.html |
| S10 | Trino - Fault Tolerant Execution | https://trino.io/docs/current/admin/fault-tolerant-execution.html |

## 2. 行业共识

| 维度 | 共识做法 |
|------|----------|
| **触发时机** | 基于内存使用量阈值或记录数阈值触发，在 OOM 之前的安全水位主动 spill |
| **写入策略** | 顺序写入，避免随机 I/O。批量 flush，压缩是默认开启的标配 |
| **读取策略** | 按偏移顺序读取或多路归并。避免随机读，利用顺序读性能 |
| **内存释放** | Spill 完成后立即释放对应内存，spill 的目的就是腾出内存 |
| **文件生命周期** | 由 spill 发起方负责清理，操作完成或任务结束时删除 |
| **多磁盘支持** | 支持多路径轮转写入（round-robin），分散 I/O 负载 |
| **背压优先** | 流处理场景优先使用背压而非 spill。Spill 是批处理或特殊场景的手段 |

## 3. 关键设计模式

- **Buffer 回收与写入解耦**：Buffer 写入 spill 文件后立即回收到池中（Flink ChannelStateWriter: "Buffers are recycled after written"）
- **顺序写入单文件**：过滤后数据按到达顺序追加写入单个 spill 文件，replay 时按顺序读取，避免随机 I/O
- **幂等清理**：Spill 文件清理必须幂等，异常路径与正常完成路径使用相同清理逻辑

## 4. 常见陷阱

| 陷阱 | 说明 |
|------|------|
| Spill 文件泄漏 | 异常路径未清理 spill 文件，必须在 finally 块中确保清理 |
| 回读时内存膨胀 | 从 spill 文件读取时一次性加载过多，必须流式读取 |
| Spill 阈值过低 | 频繁小量 spill 产生大量小文件，固定开销成为瓶颈 |
| 未考虑恢复场景 | Spill 文件在进程重启后不可用但未清理 |

## 5. 推荐

- 复用 Flink IOManager 基础设施（多 temp 目录 round-robin、文件清理）
- 采用 length-prefixed 顺序写入格式
- Buffer 写入 spill 文件后立即回收（Write-Recycle-Read 生命周期）
- Spill 文件作为短生命周期临时数据管理，不跨 checkpoint 边界
