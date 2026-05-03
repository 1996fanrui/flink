# `RecoveredInputChannel` stale-enqueue race 修复方案

> 关联现象与根因分析见 [released-recovered-channel-race.md](released-recovered-channel-race.md)。
> 本文档只描述修复方向、改造范围和正确性论证。

## 修复总原则

**整个项目只有一种锁顺序：`gate → store`，并且全部以"gate 锁内嵌套 store 锁"的形式
出现，不再有任何"串行释放 store 再去拿 gate"的写法。**

涉及的三条路径，改造后全部对齐到这一种形态：

| 路径 | 改造前 | 改造后 |
|---|---|---|
| task 读路径（`waitAndGetNextData` → `getNextRecoveredStateBuffer`） | `gate { store { read } }` | 不变 |
| producer 路径（drain / EOICS 发布） | 仅 `store { add+capture }`，锁外 fire | `gate { /* store 锁由 store 自管 */ add + fire }` |
| conversion（`convertRecoveredInputChannels`） | `store { setListener }` 释放后再 `gate { swap }` | `gate { store { setListener, swap } }` |

只要三条路径共用同一种嵌套顺序，AB-BA 就不可能发生；同一时间窗内只可能有"一条路径
进入临界区，其他路径在 gate 锁外排队"，也就不可能出现 stale 入队。

## gate 锁不暴露字段

外层代码不直接 `synchronized (inputChannelsWithData)`。在 `SingleInputGate` 上加一个
返回锁对象的 getter：

```java
// SingleInputGate
@Internal
public Object getGateLock() {
    return inputChannelsWithData;
}
```

返回类型刻意定为 `Object`：只暴露"这是一把锁"，不暴露它的真实类型
（当前是 `PrioritizedDeque<InputChannel>`），避免调用方误用成队列；后续若把锁实现换
成 `ReentrantLock` 等也只需改 getter 内部和调用处的 `synchronized` 关键字，不影响
其它代码。

调用方统一写法：

```java
synchronized (inputGate.getGateLock()) {
    ...
}
```

## API 契约

所有入口都要求 caller 已持 gate 锁（`assert Thread.holdsLock(gateLock)`）。store 锁的
责任划分按"producer / consumer"分开：

**producer 侧 mutator**（`addBuffer`、`addBufferAfterDisk`）：

- 内部 `synchronized(this) { add; fire if needed }` 一段式，listener fire 进锁里。
- caller 不需要也不应该再 `synchronized(store)`——重入冗余，且把"产生通知"和"加入数据"
  原子化在同一个 store 锁临界区内才能避免本次 race。
- 无返回值。

**consumer 侧 reader**（`tryTake`、`peekNextDataType`）以及 race path 上的
setter `setDataAvailableListener`：

- 保持现有 `@GuardedBy("this")` 契约不变。
- caller 仍然在外层 `synchronized(store)` 里调用——因为 caller 经常要做**复合原子操作**
  （典型：`tryTake() + peekNextDataType()` 必须在同一个 store 锁内观察到一致的快照）。
  这个原子性必须由 caller 的 `synchronized(store)` 提供；如果把锁挪进每个方法内部，
  两次单独的临界区就会被 `releaseAll` 等其它路径切开。
- 入口除了原有的 `assert Thread.holdsLock(this)`，再加一条 `assert Thread.holdsLock(gateLock)`，
  把"gate 锁也必须握住"这个隐含约束显式化。

**store-only reader / setter**（`isEmpty`、`setCoordinator`、`incrementPending`）：

- 保持 `@GuardedBy("this")` 单 assert（只校验 store 锁）。
- `isEmpty` 同时被 race path（task 读经 `LocalInputChannel#getNextBuffer`）和非 race
  path（`RemoteInputChannel#onSenderBacklog`，跑在 netty event loop，无 gate 锁）调
  用，所以不能强制 gate 锁。
- `setCoordinator` / `incrementPending` 只在 recovery flush 之前调一次
  （`FilteredBufferDispatcher` ctor / `write()`），不在 producer / consumer hot path
  上，强行要求 gate 锁会迫使 dispatcher 跨多 gate 持锁，得不偿失。
- 在 store 类 javadoc 里把这一豁免显式写出来。

**lifecycle / coordinator 通路**（`checkpoint`、`releaseAll`、`notifyCheckpointStopped`）：

- 保持现有的"内部 `synchronized(this)` 自管 + 锁外触发 coordinator callback"模式不变；
  与本次 race 修复正交。

为了让 store 能 assert，构造时把所属 gate 的 lock 对象注入进来（`Object` 类型即可，
不引入循环依赖）。`@VisibleForTesting` 单参构造器把 gateLock 设成 store 自身，让现有
test 里 `synchronized(store)` 的写法天然满足两条 assert。

## buffer pool 释放保持在 gate 锁之外

`bufferManager.releaseFloatingBuffers()`（`RecoveredInputChannel.java:187`）和
`bufferManager.releaseAllBuffers()`（`RecoveredInputChannel.java:280`）今天都不在任何
显式锁里，新设计也保持这一点不变——**buffer pool 释放路径不进 gate 锁**。

理由：避免引入 "buffer pool 锁 ↔ gate 锁" 的隐式依赖；今天已经没问题，没必要让 gate 锁
范围去蹭它。具体实现办法见下面 §3 / §4。

## 三条路径的具体改造

### 1. `RecoveredBufferStoreImpl`

**API 变更**：

- 删除 `addBufferAndCaptureListener` / `addBufferAfterDiskAndCaptureListener` 这套二段
  式 capture-fire API。
- `addBuffer(Buffer)` / `addBufferAfterDisk(Buffer)` 改成无返回值，内部一并完成
  fire：`synchronized(this) { add; if (shouldNotify && listener != null) listener.onDataAvailable(); }`。
  因为 caller 已经持有 gate 锁，listener 在 store 锁内 fire 不会形成 AB-BA。
- **不再保留** `dataAvailableListener` 字段的对外读访问：producer 路径已经把 fire 内化，
  无需 `getDataAvailableListener()`，字段彻底降为 store 内部细节。
- 构造器新增 `Object gateLock` 参数；提供 `@VisibleForTesting` 单参构造器把 gateLock 设
  成 store 本身，兼容 test 里 `synchronized(store)` 的既有写法。
- 入口按是否在 race path 上分组加 assert：
  - producer mutator (`addBuffer`、`addBufferAfterDisk`)：`assert Thread.holdsLock(gateLock)`。
  - race path consumer reader 与 setter (`tryTake`、`peekNextDataType`、
    `setDataAvailableListener`)：保持 `@GuardedBy("this")`，入口同时 assert
    `Thread.holdsLock(this)` 与 `Thread.holdsLock(gateLock)`。
  - store-only reader / setter (`isEmpty`、`incrementPending`、`setCoordinator`)：保持
    `@GuardedBy("this")`，只 assert `Thread.holdsLock(this)`。`isEmpty` 被 netty event
    loop 上的 `RemoteInputChannel#onSenderBacklog` 调用，无法要求 gate 锁。

**类 javadoc 改写**：删除 "capture-then-fire-outside" 段落；明写"本类所有方法的 caller
必须先持有 gate 锁；producer mutator 由 store 自管 store 锁、listener 内化 fire；consumer
reader 与剩余 setter 仍要求 caller 自己 `synchronized(store)`，以便复合原子操作（如
`tryTake() + peekNextDataType()`）成立"。

### 2. `FilteredBufferDispatcherImpl.drainPendingSpill`

从 caller（`SequentialChannelStateReaderImpl`）拿到一份"channel → gate"的映射，drain
每条 spill entry 都通过对应 gate 进入临界区。store 的 fire 由 `addBuffer` 内化：

```text
synchronized (inputGate.getGateLock()) {
    reader.skipNextEntry();
    writeChunkToBuffer(buffer, data, entryLength);
    store.addBuffer(buffer);              // 内部 synchronized(store) { add; fire if needed }
    drainHead = computeDrainHeadFrom(i);
}
```

I/O（`requestBufferBlocking`、`reader.readBytesAt`）仍保持在 gate 锁之外，避免长持锁。
临界区内只剩固定字节拷贝 + addBuffer + 可能的 fire，O(1)。

### 3. `RecoveredInputChannel.finishReadRecoveredState` 与 caller 协作

`RecoveredInputChannel.finishReadRecoveredState()` 内部不再有任何 `synchronized` 关键字
——原来那个 outer `synchronized(store)` 是冗余的（内层 `addBufferAfterDisk*` 自管
store 锁）。**buffer manager 的释放从本方法移走**，由 caller 在 gate 锁释放后单独调：

```text
// RC
public void finishReadRecoveredState() throws IOException {
    // 调用方持 gate 锁；本方法只做与 store 协调的工作。
    store.addBufferAfterDisk(EOICS);          // 内部 synchronized(store) { add; fire if needed }
    bufferFilteringCompleteFuture.complete(null);
}

public void releaseRecoveryFloatingBuffers() throws IOException {
    // 调用方在 gate 锁外调用，避免把 buffer pool 释放卷入 gate 锁。
    bufferManager.releaseFloatingBuffers();
}
```

`SingleInputGate.finishReadRecoveredState()`：

```text
List<RecoveredInputChannel> rcs = new ArrayList<>();
synchronized (inputChannelsWithData) {        // 即 getGateLock()
    for (channel : channels) {
        if (channel instanceof RecoveredInputChannel) {
            RecoveredInputChannel rc = (RecoveredInputChannel) channel;
            rc.finishReadRecoveredState();
            rcs.add(rc);
        }
    }
}
for (RecoveredInputChannel rc : rcs) {
    rc.releaseRecoveryFloatingBuffers();      // gate 锁外
}
```

为什么不需要 `!channel.isReleased()` 防御性判断：`finishReadRecoveredState` 由
`RecoveredChannelStateHandler.finishRecovery()` 在 recovery 末尾调用一次。此时两旗
（`drainDone` / `storeTransferred`）都还没立——`drainDone` 在 dispatcher `close()` 调
`releaseExclusiveBuffers` 时才设，更晚；`storeTransferred` 由 conversion 设置，而
conversion 由本方法里的 `bufferFilteringCompleteFuture.complete(null)` 触发，更晚。
所以这一行执行时 `isReleased` 必然为 false，不需要 check，也不应该 check（误导后续
读者以为这里要处理"channel 已释放"的并发情况）。

为什么 `bufferFilteringCompleteFuture.complete(null)` 不需要 store 锁：原代码用
`synchronized(store)` 包住 add+complete，目的是防止 conversion 看到"future done 但
EOICS 没在 store 里"。新设计里 conversion 也要持 gate 锁，整段
`finishReadRecoveredState` 都在 gate 锁里——conversion 在 gate 锁释放前根本跑不起来，
gate 锁就是更粗的屏障。

### 4. `SingleInputGate.convertRecoveredInputChannels`

把当前的"`toInputChannel()`（内部持 store）、释放 store、再 `synchronized(inputChannelsWithData)`"
两段式改成单一嵌套；`markStoreTransferred()` 留在 gate 锁外，避免 buffer pool 释放
路径被卷入 gate 锁：

```text
synchronized (getGateLock()) {
    InputChannel realInputChannel =
            ((RecoveredInputChannel) inputChannel).toInputChannel();   // 内部 synchronized(store)
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
    }
}

((RecoveredInputChannel) inputChannel).markStoreTransferred();   // gate 锁外
```

要点：

- `toInputChannel()` 内的 `synchronized (store) { setDataAvailableListener(physical) }`
  与 swap 现在被同一个 gate 锁包裹，listener 替换和 channels[i] swap 之间不再有任何
  其他线程能插进来。
- `markStoreTransferred()` 在 gate 锁外调用：它自己只是 set `storeTransferred=true`
  + 可能触发 `releaseAllResources()` → `bufferManager.releaseAllBuffers()`，这两段都不
  需要与 gate 锁协调，留在锁外即可。

## 正确性论证

### 死锁

三条路径都是 `gate → store` 同向嵌套；conversion 内部嵌套调用 `toInputChannel`、
`checkpointStopped` 这些方法虽然各自再次 `synchronized(store)`，但仍然落在外层 gate
锁内，依旧是 `gate → store`。Java intrinsic monitor 可重入，没有自死锁问题。

任意两条路径竞争 gate 锁，输的那一方排队等待外层锁，不持有任何与 store 相关的资源；
不存在跨锁的循环等待。buffer pool 释放路径全部留在 gate 锁外，完全不进入 gate 锁的
锁依赖图。

### Race（stale 入队）

producer 的"add + fire"和 conversion 的"setListener + swap channels"现在被同一把 gate
锁串行化：

- drain 抢到 gate 锁更早 → fire 时 store 中 listener 仍是 RC，`queueChannel(RC)` 入队 →
  conversion 后续在 gate 锁内会把 RC 从队列摘掉；
- conversion 抢到 gate 锁更早 → setListener+swap 在同一个临界区完成，drain 随后在 gate
  锁内读到的 listener 已经是 physical 的，`queueChannel(physical)` 入队，正确。

两者之间不再有"drain 持有捕获到的旧 RC listener、conversion 已经把 channels[i] 换
掉"的中间态，因为 listener 的读取与 fire 现在都由 store 内部在 store 锁里完成，而 store
锁本身又夹在 gate 锁内，整体被 gate 锁串行化。

### 两旗 rendezvous 不受影响

`drainDone + storeTransferred` 用于推迟 `RC.releaseAllResources`。`markStoreTransferred`
仍然在 gate 锁外调用，flag 的语义和触发顺序不变。

## Trade-offs

- **gate 锁占用**：drain 每条 spill entry 持 gate 锁的临界区是 O(1)（不含 I/O）。
  conversion 的 gate 锁占用从原来的"swap 一段"扩到"toInputChannel + swap 一段"。
  conversion 是恢复期一次性事件，与 task 稳态读不交叠，影响可忽略。
- **API 变更**：`*AndCaptureListener` 系列删除；`getDataAvailableListener()` 删除；
  `SingleInputGate.getGateLock()` 作为新增 API，标 `@Internal`，仅给本包内部 producer
  使用。store 构造增加 `Object gateLock` 参数，用于 `assert Thread.holdsLock(gateLock)`。
  `RecoveredInputChannel` 暴露 `releaseRecoveryFloatingBuffers()` 给 caller 在 gate 锁外
  调用。

## 验收

- 新增能稳定撞 race 的回归测试：
  - 单元测试粒度：用受控 executor 模拟 "drain 进入 gate→store、conversion 等待 gate"
    的两种顺序，断言队列中不出现 stale RC，也不再触发 `Trying to read from released
    RecoveredInputChannel`。
- `UnalignedCheckpointRescaleITCase` + `UnalignedCheckpointRescaleWithMixedExchangesITCase`
  全套参数化用例 50 轮 loop：零失败、零 NetworkBufferPool 泄漏告警（沿用 FLINK-39519
  已有的验收标准）。

## 不在本次范围内

`RecoveredBufferStoreImpl.checkpoint` / `notifyCheckpointStopped` / `releaseAll` 里的
coordinator callback 同样是 capture-then-fire-outside 模式，但触发它们的 race 路径
与本次问题无关。建议作为后续清理项一并对齐到 `gate → store`，但不放在本次修复里以
控制改动半径。
