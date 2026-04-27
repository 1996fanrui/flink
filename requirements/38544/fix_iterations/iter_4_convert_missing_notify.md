# iter_4 convert 漏 `notifyDataAvailable` 导致 100% hang

## 现象

`UnalignedCheckpointRescaleITCase` 100% 卡死。`rui_tools/run_single_test.sh` 复现，2 分钟超时。

heap dump 现场（`log/20260427_183624/20260427_183624_at60s.hprof`，store `0x647110528`）：

| 字段 | 值 |
|---|---|
| `RecoveredBufferStoreImpl.readyBuffers` | head=15 / tail=8（16 槽环形，**9 个元素**，非空） |
| `RecoveredBufferStoreImpl.pendingCount` | 3（drain 还没结束） |
| `SingleInputGate.inputChannelsWithData.deque` | **6 个 channel**，含目标 LocalInputChannel |
| `availabilityHelper.availableFuture.result` | null（**incomplete**） |
| 所有 task 线程 | parked 在 `MailboxProcessor.processMailsWhenDefaultActionUnavailable` |
| `BLOCKED` 线程数 | 0（不是 JVM 级死锁） |

数据齐了、channel 入队了，task 就是不消费。

## mailbox 唤醒链

```
producer.addBuffer
 → DataAvailableListener fire
 → InputChannel.notifyChannelNonEmpty
 → SingleInputGate.queueChannel
 → GateNotificationHelper.notifyDataAvailable   ← 唯一让 future complete 的入口
 → availabilityHelper.availableFuture.complete
 → ResumeWrapper → Suspension.resume
 → enqueue resume mail → 清空 suspendedDefaultAction
 → mailbox 回去跑 runDefaultAction → pollNext
```

只要 `availableFuture` 不 complete，`suspendedDefaultAction` 不会被清，mailbox 就一直卡在 `processMailsWhenDefaultActionUnavailable.take()` 里**根本不会去 `pollNext`**——deque 再有 channel、`readyBuffers` 再有数据都白搭。

## 根因

### 主洞：`SingleInputGate.convertRecoveredInputChannels` 不调 `notifyDataAvailable`

`SingleInputGate.java:446-449`：

```java
synchronized (inputChannelsWithData) {
    int buffersInUseCount = realInputChannel.getBuffersInUseCount();
    ...
    if (buffersInUseCount > 0) {
        inputChannelsWithData.add(realInputChannel);
        enqueuedInputChannelsWithData.set(realInputChannel.getChannelIndex());
        // ★ 缺一句 notifyDataAvailable
    }
}
```

对比正常入队的 `queueChannel`（`SingleInputGate.java:1281-1308`）—— 它用 `GateNotificationHelper`，在 deque 从 0→1 时一定调 `notifyDataAvailable`。convert 这条路径没这一步。

### 副洞放大主洞：`RecoveredBufferStoreImpl.addBufferAfterDiskAndCaptureListener` 的 defer 路径不 fire listener

`RecoveredBufferStoreImpl.java:392-407`：`pendingCount > 0` 时直接 `deferredBuffers.add(buffer); return null;`——listener 不 fire。

后续 drain 把 `pendingCount` 推到 0 时，`addBufferAndCaptureListener` 内部 promotion 把 deferred 静默搬进 ready（`RecoveredBufferStoreImpl.java:338-345`）。`wasEmpty` 在 add **之前**判定，如果 ready 此时已经有 buffer 就 `wasEmpty=false`，**也不 fire**。

## 触发流程（100% 复现）

1. mailbox 在 recovery 中段 park（消费完后 deque 空 → `checkUnavailability` resetUnavailable → emitNext NOTHING_AVAILABLE → suspend → park）。状态：`suspendedDefaultAction != null`，`availableFuture` incomplete。
2. `finishReadRecoveredState` 把 EOR 走 `addBufferAfterDiskAndCaptureListener`：`pendingCount > 0` → defer，**listener 不 fire**。然后 `bufferFilteringCompleteFuture.complete()` → thenRun → enqueue requestPartitions mail。**只有 1 个 mail 被 enqueue，没有 resume mail。**
3. mailbox 收到 mail.put 信号醒来，仍在 `processMailsWhenDefaultActionUnavailable` 里（`suspendedDefaultAction` 还在）。take 到 requestPartitions mail，跑 `convertRecoveredInputChannels`。
4. Phase 2 把 6 个 channel 全 add 进 `inputChannelsWithData`、bit set，**全部不 notify**。`availableFuture` 仍 incomplete。
5. mail 跑完，mailbox 回 while 循环检查 `isDefaultActionAvailable()` → false → take 阻塞 → 又 park。
6. 后续 drain pop 出 buffer（包括 promotion 后的 EOR）调 `addBufferAndCaptureListener`：listener fire → NEW listener → `queueChannel` → `queueChannelUnsafe` 检查 bit → **bit 已 set**（Phase 2 set 的，且 mailbox 没 `pollNext` 过所以 `getChannel` 没机会 clear）→ `alreadyEnqueued=true` → return false → **dedupe 吃掉，不 notify**。
7. 永挂。

## 与 master (`flink-os-4`) 的差异

master 分支的代码参考 /Users/ruifan/code/github/flink-os-4 目录下当前代码即可

master 的 `convertRecoveredInputChannels` **同样没有 notify**——但 master 不挂，原因在 `finishReadRecoveredState`：

```java
// flink-os-4 RecoveredInputChannel.java:209-213
synchronized (receivedBuffers) {
    onRecoveredStateBuffer(EOR);                        // ← 内部 wasEmpty=true 时 notifyChannelNonEmpty
    bufferFilteringCompleteFuture.complete(null);       // ← 触发 requestPartitions mail
}
```

master 里 EOR 走 `receivedBuffers`，`onRecoveredStateBuffer` 在 `wasEmpty` 时调 `notifyChannelNonEmpty` → `queueChannel` → `notifyDataAvailable`。这一步 enqueue resume mail。**resume mail 先于 requestPartitions mail 进队**（前者在 outer sync 里、后者在同一 sync 末尾的 thenRun 里）。

mailbox 一次 batch 把两个 mail 都拿到：先跑 resume → 清 `suspendedDefaultAction`；再跑 convert（同样不带 notify）；mail 跑完 mailbox 已经 awake，紧接着 `runDefaultAction` 去 `pollNext` 把 channel 消费掉。**convert 不带 notify 没事，靠 EOR 那一次 notify 兜底。**

我们这分支 EOR 走 `store.addBufferAfterDiskAndCaptureListener`，`pendingCount>0` 直接 defer **绕过 listener**，**没有 resume mail**——兜底链断了，convert 这个洞就暴露成 100% hang。

## 修复

`SingleInputGate.convertRecoveredInputChannels` 第 433-450 行用 `GateNotificationHelper` 包一下，与 `queueChannel` 对齐：

```java
try (GateNotificationHelper notification =
        new GateNotificationHelper(this, inputChannelsWithData)) {
    synchronized (inputChannelsWithData) {
        int buffersInUseCount = realInputChannel.getBuffersInUseCount();
        if (inputChannelsWithData.contains(inputChannel)) {
            inputChannelsWithData.getAndRemove(ch -> ch == inputChannel);
        }
        enqueuedInputChannelsWithData.clear(inputChannel.getChannelIndex());
        inputChannelsForCurrentPartition.remove(inputChannelInfo);
        inputChannelsForCurrentPartition.put(realInputChannel.getChannelInfo(), realInputChannel);
        channels[inputChannel.getChannelIndex()] = realInputChannel;
        if (buffersInUseCount > 0) {
            inputChannelsWithData.add(realInputChannel);
            enqueuedInputChannelsWithData.set(realInputChannel.getChannelIndex());
            if (inputChannelsWithData.size() == 1) {
                notification.notifyDataAvailable();
            }
        }
    }
}
```

## 防回归

回归 test：构造「mailbox park 状态下、`pendingCount>0` 时调 finishReadRecoveredState、再调 requestPartitions」的场景，断言 mailbox 能消费 channel 数据；不修复时该 test 必挂。
