# Recovery 路径单队列（stash + EndOfFetchedChannelState sentinel）重构提案

> Scope：`RemoteInputChannel` 与 `LocalInputChannel` 的 recovery 消费路径。统一改成「单一消费队列 + 普通 event 走 stash + 显式 `EndOfFetchedChannelStateEvent` sentinel 翻转 `inRecovery`」的模型，使消费侧回到 master 形态（无 recovery 专用分支）。
>
> 不在本提案范围：`RecoverableInputChannel` 对外接口（`onRecoveredStateBuffer` / `finishRecoveredBufferDelivery` / `requestRecoveryBufferBlocking` / `insertRecoveryCheckpointBarrierIfInRecovery`）的签名保持不变；drain 侧（`SpillFileDrainer`）不变。

## 1. 动机

恢复期要保证「recovered 数据先于 live 数据消费」。两端目前各自用不同的、散落在消费侧的机制实现，复杂度集中在每次读取：

- **Remote**：`inRecovery` 由 `allRecoveredBuffersDelivered` 标志 + `queuedRecoveredBuffers` 计数器隐式推导。消费侧虽已是 master 单队列形态，但 `inRecovery` 状态是「事后从计数器反推」出来的，状态转换没有一个可观测的队列事件标记，`getNextBuffer` 里还要靠 `next.sequenceNumber < 0` 判断是否递减计数器。
- **Local**：维护独立 `RecoveredBufferQueue`，消费侧 `getNextBuffer` 有 `if (inRecovery)` 分支、`peekNextDataType` 三源合并、`pullPriorityFromSubpartitionView` 专用通道、`hasPendingPriorityEvent` 标志。复杂度全在消费侧。

本提案用一个**显式 sentinel `EndOfFetchedChannelStateEvent`** 统一两端：recovered buffer 全部交付后，在消费队列尾部插入该 sentinel；它被消费侧 poll 到时，就是「recovered 数据已全部消费完」的精确时刻，此刻翻转 `inRecovery=false`、unstash、解锁 credit。`inRecovery` 的状态转换从「隐式推导」变成「一个可观测的队列事件」。

## 2. 关键不变式：恢复期消费队列不可能有上游 data buffer

本提案成立的基石是一条**由 credit 机制保证的不变式**：

> `finishRecoveredBufferDelivery()` 调用之前，上游拿不到任何 credit，因此恢复期间上游只会送来 **event**（UC barrier / announcement / `EndOfPartitionEvent` 等），**绝不会送来 data buffer**。

证据链（已逐段核实）：

1. 构造器：`needsRecovery=true` 时 `BufferManager` 以 `notifyInitiallyEnabled=false` 构造 → `notifyAvailable=false`（字段 javadoc：「Gates credit announcements while a recovery drain borrows this channel's buffers」）。
2. 恢复期间 `BufferManager.recycle()` 中 `announceCredit = releasedFloatingBuffer == null && notifyAvailable`；`notifyAvailable=false` → 永不通告 credit。
3. credit gate 仅在 recovery 收尾（见 §3.5）经 `bufferManager.enableNotify()` 打开。
4. Flink credit-based flow control：上游仅在 consumer 通告 credit 后才发送 **data buffer**；**event 不消耗 credit**，可在无 credit 时发送。

结论：恢复期消费队列里只可能出现上游 event（走 stash 或 priority 路径）+ recovered buffer + sentinel，绝无上游 data buffer。这一点同时适用于 Remote 与 Local。

## 3. RemoteInputChannel 设计

### 3.1 时间线

```
inRecovery = true（spill 文件非空时默认）
  ├─ 上游 priority event（UC barrier / announcement）→ receivedBuffers 头部（实时穿过，UC 快速通道）
  ├─ 上游普通 event（非 priority，如 EndOfPartitionEvent）→ stash
  └─ 上游 data buffer → 不可能出现（§2）→ fail-loud
recovered buffer（drain）→ receivedBuffers 尾部
所有 recovered buffer drain 完毕
  → 在 receivedBuffers 尾部插入 EndOfFetchedChannelStateEvent
EndOfFetchedChannelStateEvent 被消费侧 poll 到（不对外交付）
  → 翻转 inRecovery = false
  → 把 stash 中所有 event 按 FIFO 移入 receivedBuffers 尾部
  → 解锁上游 credit（enableNotify）
此后消费侧完全是 master 形态
```

### 3.2 单队列模型

| 来源 | 入队位置 | 入口 | 消费时机 |
|---|---|---|---|
| recovered buffer（drain） | `receivedBuffers` **尾部**（包成 `SequenceBuffer`，recovery seq 从 `Integer.MIN_VALUE` 起递增） | `onRecoveredStateBuffer` | 恢复期按 FIFO 消费 |
| `RecoveryCheckpointBarrier` sentinel | `receivedBuffers` **尾部**（同上） | `insertRecoveryCheckpointBarrierIfInRecovery` | 不对外消费，快照时被收集并移除 |
| `EndOfFetchedChannelStateEvent` sentinel | `receivedBuffers` **尾部**（recovered buffer 全部交付后） | `finishRecoveredBufferDelivery` | 被 poll 到即触发 inRecovery 翻转，不对外交付 |
| 上游 priority event（UC barrier / announcement） | `receivedBuffers` **头部**（master 既有 priority 逻辑） | `onBuffer` | 实时穿过 |
| 上游普通 event（非 priority） | **stash** | `onBuffer` | sentinel 消费后 unstash 回 `receivedBuffers` 尾部 |
| 上游 data buffer | **不可能出现**（§2） | `onBuffer` → fail-loud | n/a |

priority event 走 master 既有 `addPriorityBuffer` 路径，`maybePersist`/`checkForBarrier` 等副作用照常实时执行 —— UC 语义完全不变。

### 3.3 `onBuffer` 恢复期行为

恢复期 `onBuffer` 收到一个 buffer 时：seq 校验 / `++expectedSequenceNumber` / `checkForBarrier` / `maybePersist` / `onSenderBacklog` **照常**（与 buffer 放哪无关）。仅入队分支按来源区分：

- priority event → master 既有 priority 路径（头部）
- 普通 event → **stash**（`requiresAnnouncement` 的 event 仍在头部补一个 announcement priority 元素，保持现状）
- data buffer → **抛异常 fail-loud**（违反 §2，「出现就是 bug」）

### 3.4 sentinel 的插入与消费

- **插入**：`finishRecoveredBufferDelivery()`（drain 收尾）在 recovered buffer 全部入队后，向 `receivedBuffers` 尾部 append 一个 `EndOfFetchedChannelStateEvent` sentinel。此时**不**做 unstash、**不**调用 `enableNotify()`。
- **消费**：消费侧 `getNextBuffer` poll 出 sentinel 时，识别其类型，执行 §3.5 的翻转动作，**不**把它作为 buffer 交付给 operator（poll 出后立即处理并继续取下一个）。

### 3.5 inRecovery 翻转（消费 sentinel 时，任务线程）

sentinel 被消费侧 poll 到时，在持有 `receivedBuffers` 锁的临界区内：

1. 翻转 `inRecovery = false`
2. 把 stash 中所有 event 按 FIFO append 到 `receivedBuffers` 尾部，清空 stash
3. 解锁上游 credit：`bufferManager.enableNotify()`

时序保证：
- stash 的 event 在「credit 打开后上游才会发的 data buffer」之前进入 `receivedBuffers` → 顺序正确
- recovered buffer 此时已被消费完（sentinel 排在所有 recovered buffer 之后），unstash 的 event 排在它们之后 → 「recovered 先于 upstream」语义保持
- 相对当前实现，credit 解锁从生产者线程（`finishRecoveredBufferDelivery`）后移到消费者线程（消费 sentinel 时）。延后到任务真正消费完 recovered 数据才放开上游，时序更收紧。

### 3.6 消费侧回到 master 形态

`getNextBuffer` / `peekNextDataType`（隐含在 `nextDataType` 计算）/ `checkReadability` 删除所有 recovery 相关分支与 `queuedRecoveredBuffers` 计数逻辑，回到 master 单队列形态：直接从 `receivedBuffers` poll，priority 在前、recovered 居中、unstash 后的 event 在后。唯一新增：poll 出 `EndOfFetchedChannelStateEvent` 时执行 §3.5 并跳过对外交付。

### 3.7 `checkpointStarted` 恢复期快照

recovered buffer 入了 `receivedBuffers`，故恢复期不能直接用 master 的 `getInflightBuffersUnsafe`：

- **恢复期**（`inRecovery` 为真）：在 `receivedBuffers` 上跳过 priority 元素后向后扫描到匹配 `barrier.getId()` 的 `RecoveryCheckpointBarrier` sentinel，收集其前的 recovered data buffer 并移除该 sentinel（`collectPreRecoveryBarrier`）。
- **非恢复期**：master 原样 `getInflightBuffersUnsafe`。

`inRecovery` 现在由一个轻量布尔标志直接表示（初值由 spill 文件是否非空决定，由 §3.5 翻转），不再需要 `queuedRecoveredBuffers` 计数器。

## 4. LocalInputChannel 设计

Local 无 `receivedBuffers`（live 数据走 `subpartitionView` 拉取，不入本地队列），故路径与 Remote 不同，但消费顺序原则一致。

### 4.1 时间线

```
inRecovery = true（spill 文件非空时默认）
  └─ inRecovery 期间不从上游 subpartitionView poll 非 priority event
所有 recovered buffer drain 完毕
  → 在 recoveredQueue（见 §4.2，改为独立 Deque<Buffer>）尾部插入 EndOfFetchedChannelStateEvent
EndOfFetchedChannelStateEvent 被消费侧 poll 到（不对外交付）
  → 翻转 inRecovery = false
此后允许从 subpartitionView poll 所有 event
```

### 4.2 移除 `RecoveredBufferQueue`，改用独立 `Deque<Buffer>`

删除 `org.apache.flink.runtime.io.network.partition.consumer.RecoveredBufferQueue`，在 `LocalInputChannel` 内直接维护一个独立 `Deque<Buffer>`（仅承载 recovered buffer + 两类 sentinel）。原 `RecoveredBufferQueue` 封装的 `isInRecovery()`（`!allDelivered || !buffers.isEmpty()` 隐式推导）、`nextSequenceNumber()`、`collectPreRecoveryBarrier()` 等逻辑收进 `LocalInputChannel`，`inRecovery` 改由 §4.4 的显式 sentinel 翻转的布尔标志表示。

> Local 与 Remote 不共用 `RecoveredBufferQueue`：Remote 已把 recovered buffer 直接放进 `receivedBuffers`，Local 的 recovered buffer 与 live 数据天然分属两个来源（本地 Deque vs `subpartitionView`），无需合并到一个队列，但同样用 sentinel 翻转 `inRecovery`。

### 4.3 恢复期消费规则

恢复期（`inRecovery` 为真）`getNextBuffer`：

- 优先消费本地 `Deque<Buffer>`（recovered buffer / sentinel）
- **不**从 `subpartitionView` poll 非 priority event（即原 `if (inRecovery)` 不走 live 拉取分支）
- priority event 仍可实时穿过（保留 `hasPendingPriorityEvent` / `pullPriorityFromSubpartitionView` 快速通道，UC 语义不变）

### 4.4 inRecovery 翻转（消费 sentinel 时）

- **插入**：`finishRecoveredBufferDelivery()` 在 recovered buffer 全部入队后，向本地 `Deque<Buffer>` 尾部 append 一个 `EndOfFetchedChannelStateEvent` sentinel。
- **消费**：消费侧 poll 出该 sentinel 时翻转 `inRecovery = false`，不对外交付；此后允许从 `subpartitionView` poll 所有 event。

Local 无 credit gate 借用问题（live 数据走本地 `subpartitionView`，不依赖 credit announcement），故 §3.5 中的 `enableNotify()` 步骤在 Local 不适用；Local 的「放开上游」即「允许从 `subpartitionView` poll」。

### 4.5 `checkpointStarted` 恢复期快照

与 Remote 同构：恢复期在本地 `Deque<Buffer>` 上扫描到匹配 `RecoveryCheckpointBarrier` sentinel，收集其前的 recovered data buffer 并移除 sentinel；非恢复期无 recovered 数据，按现状处理。

## 5. 不变式校验（fail-loud）

- 恢复期 `onBuffer`（Remote）收到 `buffer.isBuffer()` 为真的 data buffer → 抛异常（不静默吞、不降级）
- stash 的 offer 入口校验：仅接受 `!buffer.isBuffer()` 的 event
- sentinel 消费时校验：`EndOfFetchedChannelStateEvent` 只应在 `inRecovery` 为真时被 poll 到；若 `inRecovery` 已为 false 又出现 sentinel → 抛异常

## 6. `EndOfFetchedChannelStateEvent` 定义

新增一个 runtime event（参照 `RecoveryCheckpointBarrier` 的形态：`RuntimeEvent` 子类 + `EventSerializer` 注册）。它是纯 sentinel，不携带 payload，仅用于在消费队列中标记「recovered 数据到此为止」。两端共用同一事件类型。

> 命名与归属：事件类放在 `org.apache.flink.runtime.checkpoint.channel` 包（与 `RecoveryCheckpointBarrier` 同址）。

## 7. 能删除什么

| 当前构造 | 本提案后 |
|---|---|
| Remote `queuedRecoveredBuffers` 计数器 + `getNextBuffer` 里 `next.sequenceNumber < 0` 递减逻辑 | 删除（改由 sentinel 显式翻转 `inRecovery`） |
| Remote `allRecoveredBuffersDelivered` 标志 | 由单一 `inRecovery` 布尔标志替代 |
| Local `RecoveredBufferQueue` 类 | 删除（改为 `LocalInputChannel` 内独立 `Deque<Buffer>`） |
| Local `getNextBuffer` 的 `if (inRecovery)` live 拉取抑制分支 | 保留语义但由显式 `inRecovery` 标志驱动（不再隐式推导） |
| Remote `finishRecoveredBufferDelivery` 里立即 unstash + `enableNotify` | 后移到消费 sentinel 时（§3.5） |
| 新增 | `EndOfFetchedChannelStateEvent`（两端共用 sentinel） |

净效果：两端消费侧的 recovery 复杂度被收敛成「poll 到 sentinel → 翻转」这一个可观测动作；`inRecovery` 不再靠计数器/空队列隐式推导。

## 8. 重构后的字段终态

只列 recovery 相关的 flag / sequence number / 队列字段，无关字段省略。

### 8.1 `RemoteInputChannel`

| 类别 | 字段 | 说明 |
|---|---|---|
| 队列 | `PrioritizedDeque<SequenceBuffer> receivedBuffers` | 唯一消费队列 |
| 队列 | `ArrayDeque<SequenceBuffer> recoveryEventStash` | 恢复期普通 event 暂存，消费 sentinel 时 unstash |
| flag | `boolean inRecovery` | 初值 `needsRecovery`；消费 sentinel 时翻 `false`。无计数器、无隐式推导 |
| seq | `int recoverySequenceNumber`（从 `Integer.MIN_VALUE` 起递增） | recovered buffer 的 seq |
| seq | `int expectedSequenceNumber`（从 0） | 上游 seq 校验，与 recovery 无关（本就存在） |

特征：**一个队列 + 一个 stash + 一个 flag + 一个 recovery seq**。

### 8.2 `LocalInputChannel`

| 类别 | 字段 | 说明 |
|---|---|---|
| 队列 | `Deque<Buffer> recoveredBuffers` | recovered buffer + 两类 sentinel，取代已删除的 `RecoveredBufferQueue` |
| 队列 | `Deque<BufferAndBacklog> toBeConsumedBuffers` | `FullyFilledBuffer` 拆分用，与 recovery 无关（本就存在） |
| flag | `boolean inRecovery` | 初值 `needsRecovery`；消费 sentinel 时翻 `false`。取代 `RecoveredBufferQueue` 内隐式推导 |
| flag | `volatile boolean hasPendingPriorityEvent` | priority 快速通道（本就存在） |
| seq | `int recoverySequenceNumber`（从 `Integer.MIN_VALUE` 起递增） | 从 `RecoveredBufferQueue` 迁入本类 |

特征：`RecoveredBufferQueue` 类被删除，其职责拆为本类的一个 `Deque` + 一个 `inRecovery` flag + 一个 recovery seq。

### 8.3 两端共用（类，非字段）

`EndOfFetchedChannelStateEvent`：新增 sentinel event 类，入队尾、被消费即翻 `inRecovery`。

## 9. 开放问题

1. Local `pullPriorityFromSubpartitionView` 与本地 `Deque<Buffer>` 中 sentinel 的相对消费顺序：priority 实时穿过时，sentinel 仍须排在所有 recovered data 之后、不被 priority 提前触发，编码时需确认。
2. `EndOfFetchedChannelStateEvent` 的序列化注册（`EventSerializer`）与是否需要进入 spill 文件格式（应仅为内存队列 sentinel，不落盘），编码时确认。
3. Remote 把 `enableNotify()` 后移到消费者线程后，需确认 `finishRecoveredBufferDelivery()` 返回与 credit 解锁之间的窗口不会引入新的死锁或 backlog 计算偏差。
