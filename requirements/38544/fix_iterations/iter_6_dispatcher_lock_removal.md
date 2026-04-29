# iter_6 移除 dispatcher 锁以同时根治 AB-BA 死锁与 checkpoint 丢数据

## 背景：两个问题其实是同一根

1. **丢数据现象**：`UnalignedCheckpointRescaleITCase.shouldRescaleUnalignedCheckpoint` 低频失败（约 1/49），表现为 `NUM_OUTPUTS != NUM_INPUTS` 或者 buffer 反序列化异常。即使 commit 6239eff9c19 把 `CHECKPOINTING_DURING_RECOVERY_ENABLED` 和 `UNALIGNED_RECOVER_OUTPUT_ON_DOWNSTREAM` 都关掉，仍然复现，说明 bug 在公共代码路径上。

2. **直接根因**：`RemoteInputChannel.checkpointStarted` 把 `channelStatePersister.startPersisting(...)` 调用挪到了 `synchronized(recoveredStore)` 之外。

## 为什么挪出来：AB-BA 死锁

涉及两把锁：

- **锁 A**：每条 channel 自己的 `RecoveredBufferStoreImpl.this`
- **锁 B**：`FilteredBufferDispatcherImpl.this`（`write`、`onChannelCheckpointStarted`、`onChannelCheckpointStopped`、`onChannelReleased` 都是实例 `synchronized` 方法）

如果 `startPersisting` 包在 `synchronized(recoveredStore)` 内，store 非空时会触发 `store.checkpoint() → coordinator.onChannelCheckpointStarted(...)`，这一回调要取 B：

- Task 线程：A → B
- Recovery 线程在 dispatcher 的 `write` 路径里：B（`write` 是 synchronized）→ `flushCache()` → `store.addBuffer()` → A

只要 filter 开启 + 恢复期间允许 checkpoint，两条路径并发就形成 AB-BA。注释里的"holding the store lock across would deadlock AB-BA"指的就是这条路径。

## 但挪出锁外引入了新 bug：丢数据窗口

`startPersisting` 内会无锁地写 `checkpointStatus = BARRIER_PENDING`。`maybePersist`（在 network 线程的 `onBuffer` 里、持 store 锁）读这个字段决定是否把 post-barrier data spill 到 channel state。

时序：

```
T1  task 持 store 锁，收集 inflightBuffers
T2  task 释放 store 锁                          ← checkpointStatus 还是 COMPLETED
T3  network 线程 onBuffer 抢到 store 锁
     receivedBuffers.add(post-barrier data)
     maybePersist 看到 checkpointStatus == COMPLETED ⇒ 不 spill
T4  task 进 startPersisting，把 checkpointStatus = BARRIER_PENDING
```

T3 的那条 buffer：

- 不在 T1 收集的 inflightBuffers 里
- 不在 channel state file 里（maybePersist 跳过了）
- 但会被 task 后续从 receivedBuffers 消费、process、产 output

而 task state snapshot 是 barrier 那一刻拍的，不包含这次 process 的副作用 ⇒ restore 时 task state 回放到 barrier 时刻、inflight 里又少了这条 record ⇒ **数据丢失**。

窗口很短（几纳秒到几微秒级），所以表现为低频偶发，正好对应"50 个 case 偶尔 1 个失败"。

## 修复方向：移除 dispatcher 锁 B

挪 startPersisting 进店是治表，关键是让 dispatcher 不再有 monitor B。一旦 B 不存在，task 持 A 调 coordinator 不再尝试取任何锁，AB-BA 环消失，`startPersisting` 可以放心包回 `synchronized(recoveredStore)` 内，丢数据窗口同时被封住。

### dispatcher 字段重新分类

| 字段 | 当前 | 重构后 |
|---|---|---|
| `checkpointStartPos: Map<InputChannelInfo, EntryPosition>` | 全局锁 B | `ConcurrentHashMap`，per-key 并发 |
| `waitSet: Set<InputChannelInfo>` | 全局锁 B | `AtomicInteger` 计数：每个 channel `decrementAndGet()`，拿到 0 的线程触发 `drainSpillEntriesToCheckpoint` |
| `currentCheckpointId` / `lastStoppedCheckpointId` | 全局锁 B | `AtomicLong` + CAS 推进 |
| `checkpointSnapshots: List<Reader>` | 全局锁 B | `volatile` 引用 + 不可变 List；写在第一个 channel 进入时 CAS 发布 |
| `cache`/`cachePosition`/`cacheChannel`/`spillFile`/`flushed`/`closed` | 全局锁 B | 单写者（recovery 线程）独占，task 线程不读这些字段；必要时用 volatile 发布给其他线程 |
| `drainHead` | volatile（已经 lock-free） | 不变 |

### 关键 invariant

- 重构后 dispatcher 的任何一个方法都**不再获取任何会反向取 store 锁**的锁
- store 锁内调 coordinator 只命中 CHM/atomic 操作 → 永不阻塞在等"另一把锁"上
- recovery 线程只走 `synchronized(store)` 取 A，不再持 B 调 A，B→A 边消失
- task 线程持 A 调 coordinator，coordinator 不取锁 → A→B 边消失

两条边都消失，环不存在。

### `RemoteInputChannel.checkpointStarted` 的回退

```java
public void checkpointStarted(CheckpointBarrier barrier) throws CheckpointException {
    synchronized (recoveredStore) {
        if (barrier.getId() < lastBarrierId) { /* throw CHECKPOINT_SUBSUMED */ }
        else if (barrier.getId() > lastBarrierId) { resetLastBarrier(); }
        List<Buffer> inflightBuffers = getInflightBuffersUnsafe(barrier.getId());
        // startPersisting 重新进锁，checkpointStatus 切换与 maybePersist 读取互斥恢复
        channelStatePersister.startPersisting(barrier.getId(), recoveredStore, inflightBuffers);
    }
}
```

`checkpointStopped` 同理（对称问题，方向相反，会引发"多 spill"而非丢数据，但应一并修）。

## 与 iter_3 的关系

iter_3 已经把 channel 内 `receivedBuffers` 锁合并进 store 锁。本 iter 在此基础上再消除 dispatcher 的全局 monitor，把 cross-channel 同步全部转成 lock-free 容器 + 原子量。两步合起来：channel 内 1 把锁，dispatcher 0 把锁，整个数据通路上不再存在多锁环。

## 验证

- 跑 `UnalignedCheckpointRescaleITCase` 50 次循环，期望失败次数从 ~1/50 降到 0
- 同时打开 filter（`UNALIGNED_RECOVER_OUTPUT_ON_DOWNSTREAM=true` + `CHECKPOINTING_DURING_RECOVERY_ENABLED=true`）跑一遍，确认没有 deadlock 类型 hang
- 单独覆盖 dispatcher 重构后的并发：多 channel 同时调 `onChannelCheckpointStarted` 时 `waitSet`/`startPos` 的最终一致性、最后一个减到 0 的 channel 触发 drain 唯一一次

## 结论

把 `startPersisting` 包回 store 锁内是必要的，但前提是 dispatcher 那一侧的锁 B 必须先被移除。两件事必须一起做，单独做任何一件都不解决问题：

- 只挪进锁不动 dispatcher：触发原 AB-BA 死锁
- 只动 dispatcher 不挪进锁：丢数据窗口仍在
