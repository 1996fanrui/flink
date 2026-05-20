# 设计：解耦 LocalInputChannel recovery wiring 与 toBeConsumedBuffers

> 范围限定：本设计只把 `LocalInputChannel` 内一个队列拆成两个，其他逻辑保持不变。**不引入新字段（除 `recoveredBuffers` 这一个 Deque）、不引入新接口、不新增测试**。simplify approach 中其他子项均不在本次范围。

## 1. 设计目标

满足 [`user_requirements.md`](./user_requirements.md) 中 REQ-7QPN ~ REQ-J3CS：在 `LocalInputChannel` 中分离两套语义不同的队列，让 FLINK-39018 留下的 recovery 数据有独立的存储位，`toBeConsumedBuffers` 回到 FLINK-39018 之前只承载 `FullyFilledBuffer` 拆分的角色。本次需求是一次纯重构，**不改变任何用户可见行为**：

- recovery 时序（migration → 消费 → priority 事件穿插 → checkpoint 持久化）完全保留
- 不删除 / 不重命名任何公开 API、构造器签名、字段访问修饰符
- 所有 FLINK-39018 及其准备阶段相关测试不需修改即应继续通过

## 2. 现状分析

### 2.1 当前耦合点

参考三次 FLINK-39018 提交（当前分支可见）：

| Commit | LocalInputChannel.java 影响 |
|---|---|
| `d1914c63c95` | 构造器接收 `ArrayDeque<Buffer> initialRecoveredBuffers`，迁移到 `toBeConsumedBuffers`（每个 buffer 包装为 `BufferAndBacklog`，序列号从 `Integer.MIN_VALUE` 起递增） |
| `cebc174ad5f` | `getNextBuffer` 在 `toBeConsumedBuffers` 非空时进入 `getNextRecoveredBuffer()`；新增 `hasPendingPriorityEvent` volatile 字段；override `notifyPriorityEvent`；删除 `requestSubpartitions` 中 `checkState(toBeConsumedBuffers.isEmpty())` |
| `3aef0932ded` | `checkpointStarted` 不再 `startPersisting(barrier.getId(), Collections.emptyList())`，改为遍历 `toBeConsumedBuffers` 收集 inflight buffer 后再 `startPersisting` |

### 2.2 `toBeConsumedBuffers` 的双重身份

- **来源 1（recovery）**：构造器迁移 recovered buffer，存活到所有 recovered buffer 被消费完
- **来源 2（FullyFilledBuffer 拆分）**：`getNextBuffer` 内对 `subpartitionView.getNextBuffer()` 返回的 `FullyFilledBuffer` 进行 partial buffer 拆分，加入队列后立即消费第一个

两类来源时间上 strictly 串行（recovery 在 `requestSubpartitions()` 之前就已经在队列里、subpartition view 拉数据只可能在 `requestSubpartitions()` 之后），但**类型上共用一个 `Deque<BufferAndBacklog>`**，使得 `getNextBuffer` 的早分支与 `checkpointStarted` 的扫描没法区分两种数据，最终导致 FullyFilledBuffer 拆分分支也被裹上 recovery-aware 逻辑。

### 2.3 目标分离

| 字段 | 解耦后的唯一职责 | 生产者 | 消费者 |
|---|---|---|---|
| `recoveredBuffers`（新增） | 构造器迁移过来的 recovered buffer | 构造器 | task thread `getNextBuffer` |
| `toBeConsumedBuffers`（保留） | `FullyFilledBuffer` 拆分出的 partial buffer | task thread `getNextBuffer` 内 re-entrant | 同一 task thread 同一调用栈 |

## 3. 修改范围

### 3.1 LocalInputChannel 字段

新增**一个**字段，位置紧邻原 `toBeConsumedBuffers` 声明：

| 字段 | 类型 | 说明 |
|---|---|---|
| `recoveredBuffers` | `Deque<BufferAndBacklog>`，初始化为 `ArrayDeque` | 仅承载构造器迁移的 recovered buffer；元素类型与 `toBeConsumedBuffers` 保持一致（均为 `Deque<BufferAndBacklog>`），以最小化下游消费代码改动 |

`hasPendingPriorityEvent` 字段保留，不动；但其字段 javadoc 需把 "before toBeConsumedBuffers" 改为 "before recoveredBuffers" 以匹配重构后的语义。

### 3.2 构造器

构造器签名保持不变（保留 `ArrayDeque<Buffer> initialRecoveredBuffers` 参数）。迁移目标由 `toBeConsumedBuffers` 改为 `recoveredBuffers`，迁移逻辑（包装、序列号起点、next data type 取值、backlog 计数、迁移完成后元素计数 checkState）等价于 `d1914c63c95` 当前实现。

### 3.3 `requestSubpartitions()`

恢复 `cebc174ad5f` 移除的 `checkState(toBeConsumedBuffers.isEmpty())`。本次解耦后 `toBeConsumedBuffers` 在 `requestSubpartitions()` 时一定为空（FullyFilledBuffer 拆分只能在 subpartition view 拿到数据之后才会发生）。

### 3.4 `getNextBuffer()`

判断顺序更新为：

1. **recovery 分支**：当 `recoveredBuffers` 非空时进入 §3.5 描述的 recovery 消费路径
2. **FullyFilledBuffer 拆分分支**：当 `toBeConsumedBuffers` 非空时回退到 FLINK-39018 之前的形态——直接取队首元素经现有 `getBufferAndAvailability` 包装返回，不再走 recovery-aware 逻辑
3. **subpartition view 分支**：master 现有代码不变，包括对 `FullyFilledBuffer` 的拆分写回 `toBeConsumedBuffers` 并立即返回首个 partial buffer

两条非默认分支互斥：recovery 期间还未调用 `requestSubpartitions()` 或 view 未返回数据，`toBeConsumedBuffers` 不可能非空；recovery 结束后 `recoveredBuffers` 不会再被写入，永远为空。

### 3.5 `getNextRecoveredBuffer()`

整体语义与 `cebc174ad5f` 引入版本一致，但**所有对 `toBeConsumedBuffers` 的读 / 写都改成对 `recoveredBuffers` 的相同操作**（包括出队、`peek` 修正 next data type 等）：

- `hasPendingPriorityEvent` 优先事件分支：保留 fetch-from-`subpartitionView` 与切回非 priority 时 reset 标志的逻辑；原先"如果 `toBeConsumedBuffers` 非空则用其首元素的 datatype 修正 `expectedNextDataType`"改为读取 `recoveredBuffers` 首元素 datatype
- 普通 recovered buffer 出队：来源由 `toBeConsumedBuffers` 改为 `recoveredBuffers`
- 最后一条 recovered buffer 的 next data type 动态探测：判空条件改为 `recoveredBuffers.isEmpty()`，但向 `subpartitionView.getAvailabilityAndBacklog(true)` 询问的逻辑完全保留

`getNextRecoveredBuffer()` 不再触达 `toBeConsumedBuffers`。

### 3.6 `checkpointStarted(CheckpointBarrier)`

inflight buffer 扫描循环遍历 `recoveredBuffers`（不是 `toBeConsumedBuffers`）。仍然保留"`bufferAndBacklog.buffer().isBuffer()` 才纳入持久化、纳入前 `retainBuffer()`"的过滤规则。`channelStatePersister.startPersisting(barrier.getId(), inflightBuffers)` 调用不变。

`toBeConsumedBuffers` 不进入扫描循环（FullyFilledBuffer 拆分不属于 inflight 的 recovery 数据）。

### 3.7 `notifyPriorityEvent`

保持 `cebc174ad5f` override 的整体行为不变（仍然写入 `hasPendingPriorityEvent` 并调用 `super` 的 priority 事件通知方法）。Priority 事件仍然只能与"recovery 阶段还未消费完"互动；FullyFilledBuffer 拆分阶段 priority 事件不再被这条路径拦截。

字段 javadoc 必须同步：把 "should be consumed before toBeConsumedBuffers" 改为 "should be consumed before recoveredBuffers"。

### 3.8 `getBuffersInUseCount` / `unsynchronizedGetNumberOfQueuedBuffers`

求和时把 `recoveredBuffers.size()` 也加进去：

- `getBuffersInUseCount`：`recoveredBuffers.size() + toBeConsumedBuffers.size() + view-size`
- `unsynchronizedGetNumberOfQueuedBuffers`：同样加和 `recoveredBuffers.size()`

两个队列时间互斥，但接口语义是"当前 channel 内待消费缓冲数量"，求和保持安全且与原版语义一致（原版在两段时间分别只有一个非空队列）。

### 3.9 `releaseAllResources`

在 channel 进入 released 状态之后，沿用 `toBeConsumedBuffers` 现有的释放模式（逐个对 `BufferAndBacklog` 中的 buffer 调用回收接口，然后清空队列）对 `recoveredBuffers` 做相同处理。两个队列的清理顺序无关紧要（时间上不重叠）。

### 3.10 不变之处

- `LocalInputChannelTest` 中与 `toBeConsumedBuffers`/recovery 相关的 9 个测试 `testCheckpointStartedPersistsRecoveredBuffers`、`testPriorityEventConsumedBeforeRecoveredBuffers`、`testPriorityEventFailsFastWhenSubpartitionViewIsNull`、`testPriorityEventFailsFastWhenNonPriorityBufferReturned`、`testPriorityEventFailsFastWhenSubpartitionViewReturnsNull`、`testMultipleConsecutivePriorityEvents`、`testNextDataTypeCorrectedToRecoveredBufferType`（FLINK-39018 引入）以及 `testGetBuffersInUseCountIncludesToBeConsumedBuffers`、`testGetNextBufferWithMigratedRecoveredBuffers`（FLINK-39018 准备阶段引入）全部**不修改**测试代码即应继续通过——它们通过构造器入参验证行为，对内部字段名无依赖。其中 `testNextDataTypeCorrectedToRecoveredBufferType` 内"from toBeConsumedBuffers"注释在重构后语义过时，实施 PR 应同步把注释改为"from recoveredBuffers"，但不影响测试结论
- `RecoveredInputChannel.toInputChannel()`、`SingleInputGate` 中构造器调用、`InputChannelBuilder` 测试 fixture 均不动
- `ChannelStatePersister.startPersisting` 签名 / 行为不动
- 不动 `RemoteInputChannel`

## 4. 不变式与互斥

- **时间互斥**：`recoveredBuffers` 在构造器结束时已是 final 内容（之后只减不增）；`toBeConsumedBuffers` 在 `requestSubpartitions()` 之前永远为空（由 `checkState` 守护），在第一次 `getNextBuffer` 进入 FullyFilledBuffer 拆分前永远为空，所以"recovery 未消费完"与"FullyFilledBuffer 拆分进行中"时间上不重叠
- **写线程**：`recoveredBuffers` 单线程写入（构造器线程）；`toBeConsumedBuffers` 单线程读写（task thread 调用 `getNextBuffer` 时 re-entrant）。`hasPendingPriorityEvent` 跨线程写入（network 线程 `notifyPriorityEvent` 设 true，task thread 读并 reset），volatile 已足够，本次不改

## 5. 代码组织

所有变更集中在单一文件：`flink-runtime/src/main/java/org/apache/flink/runtime/io/network/partition/consumer/LocalInputChannel.java`。**不新建任何文件，不修改任何测试**。

`getNextRecoveredBuffer()` 方法签名 / 可见性不变（保持 `private`）。

## 6. 兼容性

- 构造器签名不变 → 所有 caller（`SingleInputGate`、`InputChannelBuilder` 等）零改动
- 行为不变 → 现有所有测试不需修改

## 7. 验证策略

依赖 FLINK-39018 及其准备阶段已有的 9 个 `LocalInputChannelTest` 测试做回归保证（清单见 §3.10）。无新增测试。验证流程见 [`acceptance_test.md`](./acceptance_test.md)。

## 8. 已驳回的替代方案

- **直接 revert FLINK-39018 三次 commit**：会把 recovery 功能整个去掉，与 user_requirements.md REQ-OGCD/REQ-MJTH 矛盾。recovery 仍然是必需特性
- **引入 `allRecoveredBuffersDelivered` 字段做 forward-compat 占位**：用户明确否决；本次不引入任何 simplify approach 中的新字段。字段需要时由后续需求引入
- **`recoveredBuffers: Deque<Buffer>`**（simplify approach 长期形态）：本次需求是纯解耦，元素类型切换会把 `BufferAndBacklog` 的包装逻辑从构造器迁到消费点，无谓增加改动面；待 simplify approach 一次性切换更干净
- **同时解耦 `RemoteInputChannel` 的 `receivedBuffers`**：不在用户选择的 Local-only 范围内
