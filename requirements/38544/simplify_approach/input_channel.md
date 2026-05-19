# InputChannel 侧改动

> 范围：`checkpointingDuringRecoveryEnabled=true` + filter 开启时，drain 阶段把 recovered buffer 投递给物理 `InputChannel` 的接入点。功能关闭时不动 master。

## 1. 设计原则

- recovered buffer 进入物理 channel 后，其后续路径（排队 → 唤醒 task 消费 → task 消费）与上游网络 buffer **完全一致**。
- master 上 `notifyChannelNonEmpty / queueChannel / inputChannelsWithData` 这条唤醒链路 **零修改**。
- `InputChannel.getNextBuffer` 主路径不引入新分支（除 §3 各候选方案不可避免的最小改动）。
- 投递动作必须在 `Unspiller.monitor` 内进行（见 [`coordination.md`](./coordination.md) 强原则）。

## 2. master 现状不对称

| Channel | 现有的 push 入口 | 能否直接给 drain 复用 |
|---|---|---|
| `RemoteInputChannel` | `onBuffer(Buffer, sequenceNumber, backlog, subpartitionId)` —— 内部 `synchronized(receivedBuffers) { add } + notifyChannelNonEmpty()` 正是想要的 | **不能直接复用**：同一方法还做 sequence-number 校验 / priority event 分支 / `channelStatePersister.checkForBarrier+maybePersist` / `onSenderBacklog` 等网络协议侧记账，这些副作用在 recovery drain 路径上不应触发 |
| `LocalInputChannel` | **无** push 入口；继承自 `BufferAvailabilityListener`，buffer 由 `subpartitionView.getNextBuffer()` 拉取；channel 内部无 `receivedBuffers` 字段 | **无现成接口**，必须新增机制 |

因此「直接调现有 add buffer 接口」对 Remote 有限可行（要旁路掉网络记账），对 Local 不成立。

## 3. 落地候选（待定）

| 方案 | 形态 | 优缺 |
|---|---|---|
| **A** | 在 `InputChannel` 基类引入 `onRecoveredStateBuffer(Buffer)`：方法体即 master 上 `RecoveredInputChannel.onRecoveredStateBuffer` 的等价物（`synchronized(receivedBuffers) { add } + notifyChannelNonEmpty`）。Local 新增 `receivedBuffers` 队列并把它合进 `getNextBuffer` 优先级。 | 最对称；改动集中；master 现有路径不动。Local 上需要在 `getNextBuffer` 前面加一层「先看 recovered 队列」的分支。 |
| **B** | Remote 复用 `onBuffer` 内核 + Local 单独加 push：把 `RemoteInputChannel.onBuffer` 中「`receivedBuffers.add + notifyChannelNonEmpty`」抽成 package-private 内核给 drain 调；`LocalInputChannel` 仍需新增类似 A 的机制。 | 结构不对称；Remote 改动小但 Local 改动与 A 等价；整体复杂度反而更高。 |
| **C** | 包一层 wrapper `ResultSubpartitionView`：drain 把 recovered buffer 暴露给 wrapper view，channel 仍走 master pull 路径。 | 不动 channel 自身；但侵入 `ResultSubpartitionView` 抽象，改动面比 A/B 都大。 |

**当前文档不锁定方案，等讨论后回填。** 初步倾向 A：与「recovered buffer 与网络 buffer 等价」这一设计原则最贴合，改动可被 reviewer 一眼定位到 `InputChannel` / `LocalInputChannel` 两个文件。

## 4. 不变量（无论选哪条）

- `getNextBuffer` 现有调用方（`InputGate.pollNext`、`StreamTaskNetworkInput` 等）签名与契约不变。
- `notifyChannelNonEmpty / queueChannel / inputChannelsWithData` 链路不变。
- Priority event（`addPriorityBuffer / firstPriorityEvent`）链路不变。
- `RecoveredInputChannel` 上 master 已有的 `onRecoveredStateBuffer` 在 filter-关闭路径上仍然有效，不动。
