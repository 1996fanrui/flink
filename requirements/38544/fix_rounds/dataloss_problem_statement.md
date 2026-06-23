# 问题描述：rescale 恢复后偶发丢数据

> 本文件**只描述事实**：测试是什么、现象是什么、如何复现、有哪些已确认的客观信息。
> **不含任何根因分析、猜测或修复方向。**

## 1. 现象

`UnalignedCheckpointRescaleITCase` 偶发失败，断言 `NUM_OUTPUTS == NUM_INPUTS` 不成立：

```
org.opentest4j.AssertionFailedError: [NUM_OUTPUTS = NUM_INPUTS]
expected: 565158L
 but was: 536226L
```

- 输出比输入**少** `565158 - 536226 = 28932` 条。
- 方向是 output < input（少，不是多）。
- 任务**正常结束**，不抛异常、不报错；只是最终计数对不上。

## 2. 失败的测试

- 测试类方法：`UnalignedCheckpointRescaleITCase.shouldRescaleUnalignedCheckpoint`
- 失败的参数化用例：`[downscale keyed_broadcast from 7 to 2, sourceSleepMs = 0]`
- 断言位置：`UnalignedCheckpointRescaleITCase.checkCounters`（`UnalignedCheckpointRescaleITCase.java:687`）

## 3. 复现特征

- **偶发**，概率低（< 千分之一量级）。
- 复现需多次运行同一测试。
- 配置项 `execution.checkpointing.during-recovery.enabled`、
  `execution.checkpointing.unaligned.recover-output-on-downstream.enabled`、
  `execution.checkpointing.unaligned.enabled` 在该用例中被 `PseudoRandomValueSelector` 随机选为 true。

## 4. 测试做了什么（来自日志的客观事实）

该测试在一次运行内**链式启动 3 个 Flink job**，每个 job 从前一个 job 的 checkpoint 目录做 restore，并改变并行度：

| Job | job id 前缀 | 各算子并行度（分母 N）| 终态 |
|---|---|---|---|
| Job A（初始，生成 state） | `094174b8…` | failing-map=7, rescale0=10, downscale0=8, Co-Process-Broadcast-Keyed=9, upscale0=14, Sink=1 | FAILED |
| Job B（从 A 的 chk-5 恢复） | `a08e732b…` | failing-map=2, rescale0=3, downscale0=3, Co-Process-Broadcast-Keyed=4, upscale0=4, Sink=1 | CANCELED |
| Job C（从 B 的 chk-6 恢复） | `50c3258c…` | failing-map=7, rescale0=10, downscale0=8, Co-Process-Broadcast-Keyed=9, upscale0=14, Sink=1 | FINISHED |

各次转换的并行度变化：
- **A → B**：并行度下降（如 failing-map 7→2，rescale0 10→3）。这是 **downscale rescale**。
- **B → C**：并行度上升（如 failing-map 2→7，rescale0 3→10）。这是 **upscale rescale**。
- **Job C 内部**：attempt0 → attempt1 经历一次 restart（`failing-map (1/7)#0` 失败，88 tasks 重启，从 Checkpoint 7 恢复），并行度**不变**（7→7）。这是 **同并行度恢复**，不是 rescale。

即：一次失败用例里，**downscale rescale、upscale rescale、同并行度恢复三种恢复都发生了**。

每个 job 都因 `failing-map` 算子被**故意注入失败**而失败/取消/重启（这是测试机制，用来触发恢复）。

## 5. 计数从哪里来（客观事实）

- 下游 Sink 并行度全程为 **1**（`Sink: sink (1/1)`），不是 2。"7→2" 指的是上游算子（rescale0 / failing-map / Co-Process-Broadcast-Keyed 等）的并行度变化。
- 日志中**没有 per-subtask / per-operator 的输入输出计数**，只有 Sink 的汇总 "Last state"。
- 各 job 终态 Sink "Last state"：
  - Job A：`numLostValues=64514, numOutput=237708, completedCheckpoints=5`
  - Job B：`numLostValues=93566, numOutput=198450, completedCheckpoints=5`
  - Job C attempt0：`numLostValues=64725, numOutput=237980, completedCheckpoints=5`
  - Job C attempt1（最终）：`numLostValues=371433, numOutput=536226, completedCheckpoints=12`
- 最终 `numOutput=536226` 与断言 `but was: 536226L` 一致。
- `numOutOfOrderness=0`，`numDuplicates=0`（全程）。
- 测试只在**最后**（Job C FINISHED 之后）检查计数，因此计数缺失可能在**链条中任何一环**累积产生，无法从计数本身判断丢失发生在哪一环。

## 6. 日志里的异常/WARN（客观事实）

- 唯一的实质性 ERROR 是测试失败块本身（断言 565158 ≠ 536226 的 stacktrace）。
- 其余 checkpoint decline / abort 异常均围绕被故意注入失败的 `failing-map`，属测试机制（例如 "Checkpoint was declined" / "aborted due to exception of other subtasks sharing the ChannelState file"）。
- 与 recovery / channel state / barrier / spill / drain / buffer 相关的**独立** Exception 或 stacktrace：日志中未出现。
- 关于这是机场测试的事实：测试随机化模拟作业运行（随机配置、随机并行度变化、注入失败、从 checkpoint 恢复）。日志体现的是**最终结果**，不体现导致丢数据的并发过程，对定位根因帮助有限。

## 7. 相关文件

- 完整失败用例日志（已从 857490 行原始日志剥离出单个失败 case）：
  `requirements/38544/fix_rounds/rescale_dataloss_failed_case.log`（11217 行）
- 精简关键片段：
  `requirements/38544/fix_rounds/rescale_dataloss_failed_case.key.log`（4293 行）
- 原始完整日志：`log/20260623_081547.log`，失败 case 在第 61756~72972 行。

## 8. 任务

定位导致 output < input 偶发静默丢数据的根因。要求有代码证据，不接受无证据的猜测。
