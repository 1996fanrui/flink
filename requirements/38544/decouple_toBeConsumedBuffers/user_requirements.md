# 用户需求

## 需求偏离

无。

## 背景

FLINK-39018 的三次提交（`d1914c63c95`、`cebc174ad5f`、`3aef0932ded`）把 recovery 数据复用在 `LocalInputChannel.toBeConsumedBuffers` 队列上。该队列原本只承载 `FullyFilledBuffer` 拆分后的 partial buffer，FLINK-39018 让它同时承载从 `RecoveredInputChannel` 迁移过来的 recovered buffer，导致：

- 同一队列承担两种不相关的职责（partial buffer 复用 vs. recovery 暂存）
- `getNextBuffer` 在该队列非空时统一走 recovery-aware 分支（含 priority event 拦截），让 FullyFilledBuffer 热路径也被裹上 recovery 语义
- `checkpointStarted` 扫描该队列做 inflight 持久化，使得本意"承接拆分缓冲"的字段被赋予 recovery 检查点责任

## 需求

本次是一次纯重构，**只动 `LocalInputChannel`，把一个队列拆成两个队列，其他逻辑保持不变**。不引入新字段（除了把 recovered buffer 单独放一个 Deque）、不引入新接口、不引入新测试。

- **REQ-7QPN** 在 `LocalInputChannel` 内引入独立的 `recoveredBuffers` 队列字段，仅承载来自 `RecoveredInputChannel.toInputChannel()` 迁移过来的 recovered buffer；`toBeConsumedBuffers` 恢复到 FLINK-39018 之前的纯 `FullyFilledBuffer` 拆分用途。
- **REQ-OGCD** `LocalInputChannel` 构造器接收的 `initialRecoveredBuffers` 一次性迁移到 `recoveredBuffers`（迁移逻辑与 `d1914c63c95` 等价，只是改写目标队列）。
- **REQ-1RUH** `getNextBuffer` 在 `recoveredBuffers` 非空时进入 recovery 分支；`toBeConsumedBuffers` 非空时回到 FLINK-39018 之前的形态，直接经现有 `getBufferAndAvailability` 包装首元素返回，不再裹 recovery-aware 逻辑（恢复 `cebc174ad5f` 改之前的状态）。
- **REQ-8WJ8** `cebc174ad5f` 引入的 `hasPendingPriorityEvent` 优先事件穿插语义与"最后一条 recovered buffer 的 next data type 动态探测"必须完整保留，但全部迁移到读取 `recoveredBuffers` 的新 recovery 分支内执行；FullyFilledBuffer 拆分分支不再触发该逻辑。
- **REQ-MJTH** `checkpointStarted` 的 inflight buffer 扫描改为遍历 `recoveredBuffers`；持久化继续通过 `channelStatePersister.startPersisting(barrier.getId(), inflight)` 完成。
- **REQ-J3CS** 受字段拆分影响的辅助方法（`getBuffersInUseCount`、`unsynchronizedGetNumberOfQueuedBuffers`、`releaseAllResources`、`requestSubpartitions`）必须保持现有语义：缓冲使用量统计涵盖两个队列、release 同时清理两个队列、`requestSubpartitions` 上恢复 `toBeConsumedBuffers` 为空的 invariant 校验（`cebc174ad5f` 移除了它）。

## 显式不在范围

- 不引入 `allRecoveredBuffersDelivered` 字段或任何新状态机字段
- 不引入新接口（如 `RecoverableInputChannel`）、不引入 `RecoveryCheckpointBarrier` sentinel、不改 checkpoint 3-step 协议
- 不动 `RemoteInputChannel`、`RecoveredInputChannel`、`RecoveredChannelStateHandler`、`SingleInputGate` 的 buffer 迁移管线
- **不新增任何测试**——本次是纯重构，依赖 FLINK-39018 及其准备阶段已有测试做回归保证
