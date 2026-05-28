# 恢复期 buffer 申请链路统一

## 问题

drain 申请 buffer 走 `BufferRequester` → `RecoveredChannelBufferRequester` → `RecoveredInputChannel.bufferManager`。buffer 申请到后被推给物理 channel（Local/Remote），但 pool 归属仍是 `RecoveredInputChannel`，导致：

- 必须在 `BufferRequester` 上多一个 `releaseExclusiveBuffers()` 让 drain 末尾外部 release。
- `SingleInputGate.convertRecoveredInputChannels` 在 `checkpointingDuringRecoveryEnabled` 下不能直接 `recoveredChannel.releaseAllResources()`，要让 pool 续命到 drain 结束——多一条分支判断。
- Buffer ownership 跨 owner，语义脆弱。

恢复期与上游推数据**时间错开**（drain 在 `convertRecoveredInputChannels` 之后但上游 task 还没起来），所以让物理 channel 自己的 BufferManager 承担 drain buffer 申请不会和上游争 pool。

## 方案

drain 申请的 buffer 改为从**物理 channel 自己的 BufferManager** 拿，谁拿谁 recycle，无跨 owner 释放。

### 接口

`RecoverableInputChannel`：
- **新增**：`Buffer requestRecoveryBufferBlocking()`，语义内含「先 await upstream ready，再阻塞申请 buffer」。
- **删除**：`awaitUpstreamReady()`。语义合并进 `requestRecoveryBufferBlocking` 与 `finishRecoveredBufferDelivery`（finish 内部隐含 await）。
- 方法数净变化：删一个 + 加一个 = 不变。

`RecoverableInputChannel` 实现：
- `RemoteInputChannel`：用既有 `bufferManager.requestBufferBlocking()`。
- `LocalInputChannel`：新增一个**仅恢复期使用**的 `BufferManager`。仅在 `inputGate.isFinalDrainEnabled() == true` 时 setup 期申请 exclusive credit；非恢复路径不分配。

### 删除

- `BufferRequester` 接口
- `RecoveredChannelBufferRequester` 类
- `SpillFileReader` 构造参数 `bufferRequester`
- `SingleInputGate.convertRecoveredInputChannels` 的 `checkpointingDuringRecoveryEnabled` 分支判断（无条件 `releaseAllResources()`）

### drain 流程

```java
for (entry) {
    Buffer buf = ch.requestRecoveryBufferBlocking();  // 内含 await upstream
    seg.readBytesAt(...);
    synchronized (lock) {
        ch.onRecoveredStateBuffer(buf);
        seg.pollNextEntry();
        advance cursor;
    }
}
for (channel) {
    ch.finishRecoveredBufferDelivery();  // 内含 await upstream
}
```

## 净收益

- 删 1 接口 + 1 实现类 + 1 if 分支 + 1 接口方法。
- Buffer ownership 统一在物理 channel；recoveredChannel 在 convert 后可立刻完全释放。
- Local 和 Remote 在恢复期 buffer 来源对称。

## phase 归属（fixup 切分）

- phase 1：`RecoverableInputChannel` 接口方法增删；`BufferRequester` 接口删除。
- phase 2：`LocalInputChannel` 新增恢复期 BufferManager；`RemoteInputChannel` 实现新方法；`SingleInputGate.convertRecoveredInputChannels` 化简。
- phase 4：`SpillFileReader` 构造与 drain 改写；`RecoveredChannelBufferRequester` 删除。
