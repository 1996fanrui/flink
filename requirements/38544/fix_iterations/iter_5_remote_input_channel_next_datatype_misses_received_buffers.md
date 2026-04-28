# Iter 5: `RemoteInputChannel.getNextBuffer` 算 next dataType 时漏掉 `receivedBuffers` 层

## 现象

iter_4 的 convert notify 修复落地后，`UnalignedCheckpointRescaleITCase` 复跑仍能稳定 hang，hprof 显示 task 在 recovery 完全结束之后才 park 死。`failing-map (1/7)` 60s 后抓到的现场如下：task 状态 `RUNNING`、mailbox park、`availabilityHelper.availableFuture` 永远 incomplete。

证据：`log/20260427_205617/20260427_205617_at60s.hprof`

## 铁证（heap dump 关键状态）

| 字段 | 值 |
|---|---|
| `mailboxProcessor.suspendedDefaultAction` | non-null（task 已 park） |
| `mailbox.queue` size | 0（无任何 mail 待处理） |
| `availabilityHelper.availableFuture.result` | null（incomplete，1 个 listener 在等） |
| `inputChannelsWithData` size | 0 |
| `enqueuedInputChannelsWithData` BitSet | 0（7 个 channel 全部不在 deque 内） |
| 7 个 `RecoveredBufferStoreImpl` | 全部 `pendingCount=0`、`readyBuffers/deferredBuffers` 空（drain 已完成） |

但 4 个 RemoteInputChannel 的 `receivedBuffers` 各卡着 1 个 buffer：

| chIdx | dataType | seqNum | expectedSequenceNumber |
|---|---|---|---|
| 1, 2, 3, 6 | RECOVERY_COMPLETION | 0 | 1 |

`numPriorityElements=0`（不是 priority 路径）；`expectedSequenceNumber=1` 且 buffer `seqNum=0` 说明 `onBuffer` 跑过一次后再没收到任何东西。

## 根因

`RemoteInputChannel.getNextBuffer` 的 recovery 分支算 `nextDataType` 只看 `recoveredStore`：

```java
recovered = recoveredStore.tryTake();
recoveredNextDataType =
        recovered != null ? recoveredStore.peekNextDataType() : DataType.NONE;
```

`peekNextDataType()` 只读 `readyBuffers` 头，不看 `receivedBuffers`。当一次 `tryTake` 把 store 抽空，即使 `receivedBuffers` 已有 buffer 也被忽略 —— gate 收到 `moreAvailable=false`，不再 enqueue 该 channel。

复现链（与现场一一对应）：

1. **convert 时 store 有 P1 数据** → 4 个 RemoteInputChannel Phase 2 把自己加进 `inputChannelsWithData`，bit 置位
2. producer 端发来首条 `RECOVERY_COMPLETION` 走 `onBuffer`：`wasEmpty=true`、写入 `receivedBuffers` 后调 `notifyChannelNonEmpty` → `queueChannel` → bit 已 set → `alreadyEnqueued=true` 返回 false，**notify 被短路掉**
3. task poll channel 出 deque（bit 清零），逐条 take store 数据，每次 `peekNextDataType` 还非 NONE → gate `queueChannelUnsafe` 重排队
4. **最后一次 `tryTake` 把 store 抽空**：`readyBuffers` 空、`pendingCount=0`，`peekNextDataType` 返回 NONE，`moreAvailable=false`，channel **不再被 enqueue**
5. `receivedBuffers` 那条 `RECOVERY_COMPLETION` 永远 invisible，`availableFuture` 永远不 complete，task 永远 park

iter_4 已经修过 convert 的 notify 漏洞，所以 hang 不再发生在 convert 阶段；本轮命中的是 recovery 收尾时的另一条独立 hang 路径。

## 修复方案

把"算 next dataType"抽成 `peekNextDataType()` 私有方法，要求 caller 持 `recoveredStore` 锁（`assert Thread.holdsLock`），按 priority → store ready → store pending(=NONE) → receivedBuffers 三层 lookup 一次性算清楚。

### A. RemoteInputChannel
- 新增 `peekNextDataType()`：`hasPendingPriorityEvent` → `receivedBuffers.peek()` head；否则 `recoveredStore.isEmpty()` → `receivedBuffers.peek()`；否则 `recoveredStore.peekNextDataType()`
- `getNextBuffer()` 收成单段 `synchronized (recoveredStore)`，覆盖"分类 + 取 buffer + 算 nextDataType"全过程，消掉原 3 段锁之间的 TOCTOU
- `pollPendingPriorityEvent` 改成 caller-locked，签名改 `@Nullable SequenceBuffer`，metrics/trace 移到 `getNextBuffer` 出锁后统一处理

### B. LocalInputChannel
- 同名 `peekNextDataType()`，但只看 store 一层。`subpartitionView` 这层 tier 受 AB-BA 约束（producer 侧 `subpartition lock → gate.notifyChannelNonEmpty → inputChannelsWithData lock` 与本地 `gate → store` 拼起来会闭环），必须留在锁外由 caller 处理
- 替换 `getNextRecoveredBuffer` 内两处 inline `recoveredStore.peekNextDataType()`，结构等价无行为变化

## 验证

新增回归测试 `RemoteInputChannelTest.testNextDataTypeReflectsReceivedBuffersWhenRecoveredStoreExhausted`：构造"store 有 N 条数据 + `onBuffer` 抢先入 `receivedBuffers`"场景，断言抽空 store 那一帧返回的 `BufferAndAvailability.moreAvailable() == true`。修复前 fail，修复后通过。

跑过的 test：

| Test class | Pass / Total |
|---|---|
| `RemoteInputChannelTest` | 47 / 47 |
| `LocalInputChannelTest` | 28 / 28 |
| `SingleInputGateTest` | 32 / 32 |
| `RecoveredBufferStoreTest` | 24 / 24 |
| `RecoveredInputChannelTest` | 8 / 8 |

---

本文档冻结，不允许后续修改；如发现新问题，新建 iter_N 文档。
