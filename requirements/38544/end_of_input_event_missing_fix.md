# EndOfInputChannelStateEvent missing on `isCheckpointingDuringRecoveryEnabled=true` 路径

> 关联：[hang_evidence.md](./hang_evidence.md)（事实证据）、[recovered_buffer_queue_refactor.md](./recovered_buffer_queue_refactor.md)（重构提案）

## 1. Bug 一句话

**当 `inputGate.isCheckpointingDuringRecoveryEnabled() == true` 时，物理 `LocalInputChannel` / `RemoteInputChannel` 转换后的 `recoveredQueue` 永远不会拿到 `EndOfInputChannelStateEvent` sentinel**，导致 fresh-start（无 recovery 状态）场景下：
- `recoveredQueue` 自始至终为空（`totalOffered=0`）
- `finishRecoveredBufferDelivery()` 翻完 `allDelivered=true` 之后没人 `notifyChannelNonEmpty()`，gate 永远等不到 channel 上的数据可用通知
- 上游 subpartition 里堆着 14 个 buffer，下游 sink 永远不来拉

## 2. 直接证据（来自 hang_evidence.md）

| 事件 | 证据 |
|---|---|
| `RecoveredInputChannel.finishReadRecoveredState` 9 次都 push 过 sentinel | log: 9 行 `[HANG-DIAG] RecoveredInputChannel.finishReadRecoveredState ... receivedBuffers.sizeBefore=0`（push 前 size，push 后 size=1） |
| `toInputChannel` 抓取时 sentinel 已经消失 | log: 9 行 `[HANG-DIAG] RecoveredInputChannel.toInputChannel ... remainingBuffers.size=0` |
| sentinel 在 RecoveredInputChannel 内部就被消费 | 代码：`RecoveredInputChannel.getNextRecoveredStateBuffer`（line 240-250）发现 sentinel 就 `stateConsumedFuture.complete(null)` 并 `return null` |
| `LocalInputChannel.recoveredQueue.offer` 整轮 0 次 | log: 全文搜 `[HANG-DIAG] offer` 零结果；9 个 channel `finish` 时 `totalOffered=0 bufferQueueSize=0` |

## 3. 当前代码的"丢 event"流程（cpDuringRecovery=true）

1. filter（channelIOExecutor）调 `inputGate.finishReadRecoveredState()`
2. `RecoveredInputChannel.finishReadRecoveredState`（`RecoveredInputChannel.java:220-228`）push `EndOfInputChannelStateEvent` 进 `receivedBuffers`，complete `bufferFilteringCompleteFuture`
3. `bufferFilteringCompleteFuture` 触发 mailbox 上的 `requestPartitions` mail enqueue
4. inner mailbox loop 在处理 `requestPartitions` mail 之前，**default action（`processInput`）有机会先跑**，它通过 gate 拉到 RecoveredInputChannel，调 `RecoveredInputChannel.getNextRecoveredStateBuffer`，**把 sentinel poll 走 + completes `stateConsumedFuture` + return null**
5. mailbox 接着处理 `requestPartitions` mail → `convertRecoveredInputChannels`：
   - `toInputChannel` 时 `receivedBuffers` 已空，`remainingBuffers.size=0`
   - 物理 channel 构造后，`for (Buffer buf : remainingBuffers) rec.onRecoveredStateBuffer(buf)` 一次都没跑
   - 新建的 `LocalInputChannel.recoveredQueue` **零 buffer**
6. 后续 handoff 阶段 `StreamTask.finishPhysicalRecoveredChannels` 调每个物理 channel 的 `finishRecoveredBufferDelivery()`：
   - 只翻 `allDelivered=true`
   - **不调任何 `notifyChannelNonEmpty()`**
   - gate 永远没人唤醒

## 4. 修复方案

按 `inputGate.isCheckpointingDuringRecoveryEnabled()` 分流 sentinel 的添加位置——**两个 RecoverableInputChannel 实现都需要改**（`LocalInputChannel` 与 `RemoteInputChannel`），因为它们都对应 `RecoverableInputChannel#finishRecoveredBufferDelivery` 这个契约。

### 4.1 `!isCheckpointingDuringRecoveryEnabled()`（master 行为，保持现状）

- `RecoveredInputChannel.finishReadRecoveredState`：照旧 push sentinel 进 `receivedBuffers`
- sentinel 由 `RecoveredInputChannel.getNextRecoveredStateBuffer` 消费、completes `stateConsumedFuture`
- `stateConsumedFuture` 触发 `requestPartitions` → `toInputChannel`（master 流程）

### 4.2 `isCheckpointingDuringRecoveryEnabled()`（新行为，补回丢失的 event）

- `RecoveredInputChannel.finishReadRecoveredState`：**不再 push sentinel**，只 complete `bufferFilteringCompleteFuture`
- `LocalInputChannel.finishRecoveredBufferDelivery` 与 `RemoteInputChannel.finishRecoveredBufferDelivery`：
  - 先 `recoveredQueue.offer(EndOfInputChannelStateEvent.INSTANCE)`（push 进 recoveredQueue，`wasEmpty=true` 是返回值）
  - 然后 `recoveredQueue.finish()`（翻 `allDelivered=true`）
  - 如果 `wasEmpty == true`，跳出锁后调用一次 `notifyChannelNonEmpty()`

### 4.3 为什么 4.2 修复后 work

1. `offer` 把 sentinel 进 `recoveredQueue` → consumer 端 `inRecovery=true` 且 queue 非空，channel 是被 enqueue 的
2. `wasEmpty=true` 触发 `notifyChannelNonEmpty()` → `gate.queueChannel` 把 channel 放回 `inputChannelsWithData`，availFut 完成
3. consumer poll → `getNextBuffer`：
   - inRecovery=true，hasPri=false
   - poll sentinel from recoveredQueue → recoveredQueue.isEmpty() 变 true
   - `wrapRecoveredBufferAsAvailability`：当 `recoveredQueue.isEmpty() && allDelivered=true` 走探 `subpartitionView` 分支，`getAvailabilityAndBacklog(true)` 报上游有数据 → `nextDataType=DATA_BUFFER`
4. `moreAvailable=true` → gate 重新 enqueue channel
5. 下次 poll：`inRecovery=false`（allDelivered=true & queue empty），fall through 到 `subpartitionView.getNextBuffer()`，正常拿上游 buffer

### 4.4 行为等价于 master 的"最后一个 recover buffer 触发探上游"

master 的 `getNextRecoveredBuffer`（`master/.../LocalInputChannel.java:377-396`）在消费 `toBeConsumedBuffers` 最后一个 buffer 时探 `subpartitionView` 并把 `nextDataType` 强制改成 `DATA_BUFFER`。我们这条修复路径等价：通过补一个 sentinel buffer 让 `wrapRecoveredBufferAsAvailability` 走同样的"queue 空 + delivered + 探 view"分支。

## 5. 影响范围

- `RecoveredInputChannel.finishReadRecoveredState`：cpDuringRecovery 分支改为不 push sentinel
- `LocalInputChannel.finishRecoveredBufferDelivery`：加 offer + 条件 notify
- `RemoteInputChannel.finishRecoveredBufferDelivery`：加 offer + 条件 notify（注意 Remote 的锁是 `receivedBuffers`，要把 offer 放进相同 critical section）
- `RecoverableInputChannel` 接口契约可以维持不变（只是实现里多 push 一个 buffer），但 javadoc 里需要说明"实现负责在 cpDuringRecovery 路径下补 EndOfInputChannelStateEvent"
- master 的 `!cpDuringRecovery` 路径**不受影响**

## 6. 验证步骤

1. 实施 §4.2 改动并编译
2. 跑 `rui_tools/loop.sh`（fresh-start case [1] 之前 100% 复现 hang）
3. 预期：测试不再卡死，case [1] 通过或失败但**不挂**
4. 若仍 hang：抓 heap，按 hang_evidence.md §5.1 流程对照检查 `recoveredQueue.size`、`enqueuedBitSet`、`availFut` 状态——理论上 `recoveredQueue.size` 应该至少出现过 1 个 sentinel，gate 的 availFut 应该被完成过
