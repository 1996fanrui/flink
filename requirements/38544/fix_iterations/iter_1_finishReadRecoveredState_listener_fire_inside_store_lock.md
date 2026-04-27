# Iter 1: `finishReadRecoveredState` 在 store 锁内 fire listener 触发的 AB-BA 死锁

## 现象

`UnalignedCheckpointRescaleITCase.shouldRescaleUnalignedCheckpoint`（case 3：downscale keyed_different_parallelism from 12 to 7, sourceSleepMs = 0）测试 hang，2 分钟后被 JUnit 强制超时。

证据：`log/20260427_130233.log`

```
11297: Test org.apache.flink.test.checkpointing.UnalignedCheckpointRescaleITCase.shouldRescaleUnalignedCheckpoint[downscale keyed_different_parallelism from 12 to 7, sourceSleepMs = 0] failed with:
11298: java.util.concurrent.TimeoutException: shouldRescaleUnalignedCheckpoint(org.junit.jupiter.api.TestInfo) timed out after 2 minutes
...
11445:     at org.apache.flink.test.checkpointing.UnalignedCheckpointTestBase.execute(UnalignedCheckpointTestBase.java:204)
11446:     at org.apache.flink.test.checkpointing.UnalignedCheckpointRescaleITCase.shouldRescaleUnalignedCheckpoint(UnalignedCheckpointRescaleITCase.java:622)
11454: [ERROR]   UnalignedCheckpointRescaleITCase.shouldRescaleUnalignedCheckpoint(TestInfo)[1] » Timeout shouldRescaleUnalignedCheckpoint(org.junit.jupiter.api.TestInfo) timed out after 2 minutes
```

测试在 `waitForAllTaskRunning` 阶段卡死，说明 task 启动期就被阻塞，不是 source emit / checkpoint 阶段的问题。

## 铁证（jstack 摘录）

证据：`log/20260427_130233.jstack`，由 watcher 在测试 hang 期间周期抓取（10s 一次），JVM 内置 deadlock detector 直接给出 verdict。

```
5377: Found one Java-level deadlock:
5378: =============================
5379: "downscale0 (1/4)#0":
5380:   waiting to lock monitor 0x00000001663aa980 (object 0x000000064ab24988, a org.apache.flink.runtime.io.network.partition.consumer.RecoveredBufferStoreImpl),
5381:   which is held by "channel-state-unspilling-downscale0 (1/4)#0 (...)-thread-1"
5382: "channel-state-unspilling-downscale0 (1/4)#0 (...)-thread-1":
5383:   waiting to lock monitor 0x0000000122c63c40 (object 0x000000064aa1cec8, a org.apache.flink.runtime.io.network.partition.PrioritizedDeque),
5384:   which is held by "downscale0 (1/4)#0"
```

两个线程的关键调用栈（同文件 5388-5433 行）：

Task 线程（持 `PrioritizedDeque` 即 gate 的 `inputChannelsWithData` 锁，等 store 锁）：

```
5389:    at o.a.f.r.io.network.partition.consumer.RecoveredBufferStoreImpl.peekNextDataType(RecoveredBufferStoreImpl.java:114)
5390:    - waiting to lock <0x000000064ab24988> (a RecoveredBufferStoreImpl)
5391:    at o.a.f.r.io.network.partition.consumer.RecoveredInputChannel.getNextRecoveredStateBuffer(RecoveredInputChannel.java:216)
5392:    at o.a.f.r.io.network.partition.consumer.RecoveredInputChannel.getNextBuffer(RecoveredInputChannel.java:239)
5393:    at o.a.f.r.io.network.partition.consumer.SingleInputGate.readBufferFromInputChannel(SingleInputGate.java:984)
5394:    at o.a.f.r.io.network.partition.consumer.SingleInputGate.readRecoveredOrNormalBuffer(SingleInputGate.java:970)
5395:    at o.a.f.r.io.network.partition.consumer.SingleInputGate.waitAndGetNextData(SingleInputGate.java:919)
5396:    - locked <0x000000064aa1cec8> (a PrioritizedDeque)
```

Channel-state-unspilling 线程（持 store 锁，等 gate 锁）：

```
5415:    at o.a.f.r.io.network.partition.consumer.SingleInputGate.queueChannel(SingleInputGate.java:1276)
5416:    - waiting to lock <0x000000064aa1cec8> (a PrioritizedDeque)
5417:    at o.a.f.r.io.network.partition.consumer.SingleInputGate.notifyChannelNonEmpty(SingleInputGate.java:1230)
5418:    at o.a.f.r.io.network.partition.consumer.InputChannel.notifyChannelNonEmpty(InputChannel.java:174)
5419:    at o.a.f.r.io.network.partition.consumer.RecoveredInputChannel$$Lambda...onDataAvailable(...)
5420:    at o.a.f.r.io.network.partition.consumer.RecoveredBufferStoreImpl.addBufferAfterDisk(RecoveredBufferStoreImpl.java:346)
5421:    at o.a.f.r.io.network.partition.consumer.RecoveredInputChannel.finishReadRecoveredState(RecoveredInputChannel.java:195)
5422:    - locked <0x000000064ab24988> (a RecoveredBufferStoreImpl)
5423:    at o.a.f.r.io.network.partition.consumer.SingleInputGate.finishReadRecoveredState(SingleInputGate.java:471)
5435: Found 1 deadlock.
```

## 根因

`RecoveredInputChannel.finishReadRecoveredState`（修复前）持有 outer `synchronized(store)` 时调用 `store.addBufferAfterDisk(EndOfInputChannelStateEvent)`；`addBufferAfterDisk` 在内部把队列从空变非空后，会回调 `dataAvailableListener.onDataAvailable()` → `InputChannel.notifyChannelNonEmpty` → `SingleInputGate.queueChannel`，后者要抢 gate 的 `inputChannelsWithData` 锁。

与此同时 task 线程的正常路径是 `SingleInputGate.waitAndGetNextData` 先持 `inputChannelsWithData`，再调用 `RecoveredBufferStoreImpl.peekNextDataType` 抢 store 锁。

两条路径形成对称的 AB-BA：

- Task 线程： gate 锁 → store 锁
- Unspilling 线程： store 锁 → gate 锁

`addBufferAfterDisk` 自身的注释承诺“listener 在 store monitor 之外触发”，但 caller 在外面又包了一层 `synchronized(store)`，把承诺击穿；JVM 内置 deadlock detector 实锤确认（jstack 5377-5435 行）。

## 修复方案

采用 capture-then-fire-outside 模式：在 store 锁内只捕获 listener 引用，释放外层 store 锁后再触发回调。新增三个 `*AndCaptureListener` 变体，旧 API 保留为 thin wrapper（外层无锁场景仍可直接用），caller 持有 outer store 锁时必须改用 capture 变体。

1. `RecoveredBufferStoreImpl`
   - 新增 `addBufferAndCaptureListener` / `addBufferAfterDiskAndCaptureListener` / `decrementPendingAndCaptureListener`，在 `synchronized(this)` 内返回 listener，不在锁内调用 `onDataAvailable()`。
   - 旧 `addBuffer` / `addBufferAfterDisk` / `decrementPending` 改写为薄壳，调用对应 capture 方法并在解锁后 fire；行为与之前一致，未引入语义变化。
   - 三处 javadoc 显式声明：caller 若包了 outer `synchronized(store)`，必须改用 capture 变体，否则 listener 仍会在 outer 锁内运行，AB-BA 风险复发。

2. `RecoveredInputChannel.finishReadRecoveredState`
   - 把 `store.addBufferAfterDisk(...)` 替换为 `store.addBufferAfterDiskAndCaptureListener(...)`，捕获 listener 到局部变量 `listenerToFire`。
   - 在 outer `synchronized(store)` 块外 `if (listenerToFire != null) listenerToFire.onDataAvailable();`，彻底解除 store→gate 锁序。
   - 注释里把 AB-BA 的来龙去脉说清楚，引用 `SingleInputGate.queueChannel` 这条 listener 路径。

3. `FilteredBufferDispatcherImpl.drainPendingSpill`（对称路径）
   - drain phase-2 提交分支同样在 `synchronized(store)` 里同时调用 `addBuffer` + `decrementPending`，两个调用各自都会 fire listener，是同一类 AB-BA 隐患。
   - 改用 `addBufferAndCaptureListener` + `decrementPendingAndCaptureListener` 捕获两个 listener，在锁外按序触发。注释解释了为什么实际只会有一个 listener 非 null（add 后 readyBuffers 非空，decrement 不会 wasEmpty=true），但仍防御式调用以防不变式被破坏；同时显式 dedup `decrementListener != addListener`，避免对同一个 listener 触发两次。
   - 这一处与本次 deadlock 没有直接命中，但它和 `finishReadRecoveredState` 是对称的“outer store 锁内 fire listener”模式；上一轮 `requirements/38544/remaining_drain_buffer_loss.md` 段落 A 已经标识过该路径有“lock holder 在 store 锁内做较重操作”的结构性问题，本次一并消除以避免下一轮迭代再被打回。

## 验证

下一步：在新工具脚本（每 10s jstack + 末尾 heap dump，详见 `rui_tools/run_single_test.sh`）下复跑 `UnalignedCheckpointRescaleITCase`（裁剪到 case 3 单 case 模式以加速复现），由协调者 loop 驱动；若再次 hang，则在 jstack 里检查是否仍是同一对锁，新建 `iter_2_*.md` 跟进；若顺利通过，则收工。本轮尚未跑测试。

---

本文档冻结，不允许后续修改；如发现新问题，新建 iter_N 文档。
