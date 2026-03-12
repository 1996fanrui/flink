# Fix：Unaligned Checkpoint Barrier 应绕过 Blocked Subpartition

## 问题

Unaligned checkpoint + checkpointing-during-recovery 场景下，rescale 后 checkpoint 偶尔极慢（73s vs 正常 30-70ms）。

## 根因

Recovery 期间，`PipelinedSubpartition` 被 `RECOVERY_COMPLETION` 事件阻塞（`isBlocked=true`）。此时如果触发 checkpoint，unaligned checkpoint barrier（priority event）被加入队列但：

1. **`needNotifyPriorityEvent()`** 因为 `!isBlocked` 条件返回 `false`，不通知下游
2. **`pollBuffer()`** 因为 `isBlocked` 直接返回 `null`，下游无法取出 barrier

Barrier 卡在队列中，直到 `resumeConsumption()` 被调用才能传递。而 `UpstreamRecoveryTracker` 等所有 channel 恢复完才调 `resumeConsumption()`，导致最慢的 channel 拖慢整个 checkpoint。

## 证据

```
22:33:12  Source(5/5) partition-4 被 RECOVERY_COMPLETION 阻塞
22:33:17  Checkpoint-4 触发, barrier 加入队列, needNotifyPriorityEvent=false
          Writer(5/7) 收到 4/5 barriers (1ms 内), 等待 channel 4
          ... 73 秒空等 ...
22:34:31  resumeConsumption() 被调用, barrier 传递, checkpoint 完成 (73,473ms)
```

## 修复方案

三处改动，都在 `PipelinedSubpartition.java`：

### 1. `pollBuffer()`：blocked 时允许返回 priority buffer

```java
// 改前
if (isBlocked) {
    return null;
}

// 改后
if (isBlocked && buffers.getNumPriorityElements() == 0) {
    return null;
}
```

安全性：priority buffer（unaligned checkpoint barrier）的 DataType 是 `PRIORITIZED_EVENT_BUFFER`，其 `isBlockingUpstream=false`，poll 出来后不会再次设置 `isBlocked=true`。poll 完后 `numPriorityElements` 变为 0，后续 `pollBuffer()` 仍因 `isBlocked && numPriorityElements==0` 返回 null——即 **只放行 priority event，数据流仍然被 block**。

### 2. `needNotifyPriorityEvent()`：移除 `!isBlocked` 条件

```java
// 改前
return buffers.getNumPriorityElements() == 1 && !isBlocked;

// 改后
return buffers.getNumPriorityElements() == 1;
```

走到 `needNotifyPriorityEvent` 的 priority event 只有 unaligned checkpoint barrier（代码有 `checkState` 断言保证），移除 `!isBlocked` 不影响其他事件类型。

### 3. `resumeConsumption()`：移除 commit 9a4a929 的补偿逻辑

```java
// 改前 (commit 9a4a929 加的)
void resumeConsumption() {
    int prioritySequenceNumber = DEFAULT_PRIORITY_SEQUENCE_NUMBER;
    synchronized (buffers) {
        checkState(isBlocked, "Should be blocked by checkpoint.");
        isBlocked = false;
        if (buffers.getNumPriorityElements() > 0) {
            prioritySequenceNumber = sequenceNumber;
        }
    }
    notifyPriorityEvent(prioritySequenceNumber);
}

// 改后 (恢复到 9a4a929 之前)
void resumeConsumption() {
    synchronized (buffers) {
        checkState(isBlocked, "Should be blocked by checkpoint.");
        isBlocked = false;
    }
}
```

原因：改动 1 和 2 之后，priority event 在 blocked 时就已经被通知并取走，`resumeConsumption()` 时 `numPriorityElements == 0`，9a4a929 的补偿分支永远不会进入，变成 dead code。

### 同时需要更新的 test

commit 9a4a929 添加的 `testResumeConsumptionNotifiesPendingPriorityEvent` 测试需要更新：该测试验证的是 "blocked 时 priority 通知被抑制，resume 时补偿通知"。修复后行为变为 "blocked 时 priority 通知不被抑制"，测试应改为验证新行为。

## 为什么三处都要改

| 只改 needNotifyPriorityEvent | 只改 pollBuffer | 三处都改 |
|---|---|---|
| 下游收到通知，调 `pollBuffer()` 取 barrier | 不通知下游，下游不知道有 barrier | 通知 + 取出都正常 |
| `pollBuffer()` 返回 null → **assertion crash** | barrier 卡在队列直到 resume | barrier 立即传递 |

## 涉及文件

- `flink-runtime/.../partition/PipelinedSubpartition.java` — 三处改动
- `flink-runtime/.../partition/PipelinedSubpartitionWithReadViewTest.java` — 更新 9a4a929 的测试
