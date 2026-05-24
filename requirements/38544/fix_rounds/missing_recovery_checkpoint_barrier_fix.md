# `Missing RecoveryCheckpointBarrier for checkpoint N` 修复方案

> 关联：[recovered_buffer_queue_refactor.md](./recovered_buffer_queue_refactor.md)（RecoveredBufferQueue 抽取）、[end_of_input_event_missing_fix.md](./end_of_input_event_missing_fix.md)（cpDuringRecovery 路径上的 sentinel 缺失）。本方案只解决 Step 1 / Step 2 在 `recoveredQueue` 谓词上的不一致，不动 sentinel 路径。

## 1. Bug 一句话

`SpillFileReader.snapshotAndInsertBarriers`（**Step 1**）以"spill 文件的 drain 游标"判断"还要不要插 `RecoveryCheckpointBarrier`"，而每个 channel 的 `checkpointStarted`（**Step 2**）以 `RecoveredBufferQueue.isInRecovery()` 判断"还要不要从 `recoveredQueue` 抠 pre-barrier 数据"。两侧判定在以下窗口不等价：

> drain 已把最后一条 entry 投递完（cursor 推到末尾）→ Step 1 视角："recoveryAlreadyDone = true" → return `DiskSnapshot.empty()`，**不插 barrier**；但此时 `finishRecoveredBufferDelivery()` 还没翻 `allDelivered` 或队列里 sentinel/data 还没消费完 → Step 2 视角："isInRecovery = true" → 调 `collectPreRecoveryBarrier(cpId)` → 找不到对应 id 的 sentinel → 抛 `IOException: Missing RecoveryCheckpointBarrier for checkpoint N in recoveredBuffers for channel ...`，checkpoint 被 decline。

## 2. 证据

- 失败摘要：`log/flink-test-analyzer_20260523_222057/exception_summary.md` 顶层 root cause `FlinkRuntimeException` 20 例全部源自 `RecoveredBufferQueue.collectPreRecoveryBarrier` 抛出的 `Missing RecoveryCheckpointBarrier`。
- 触发代码点：`RecoveredBufferQueue.java:153-158`。
- Step 1 跳过条件：`SpillFileReader.java:157-159` 的 `recoveryAlreadyDone(diskSnap, startSegmentIndex, startOffset)` 返回 true 时直接 `return DiskSnapshot.empty()`，不插 barrier。
- Step 2 进入 collect 分支条件：`LocalInputChannel.java:200-201` / `RemoteInputChannel.java:853-859` 都是 `if (recoveredQueue.isInRecovery()) toPersist = recoveredQueue.collectPreRecoveryBarrier(barrier.getId());`。
- `isInRecovery` 定义：`RecoveredBufferQueue.java:94-96` `!allDelivered || !buffers.isEmpty()`。
- `allDelivered` 翻 true 的时机：`SpillFileReader.drain()` 的 `for (RecoverableInputChannel ch : allChannels) ch.finishRecoveredBufferDelivery();`（`SpillFileReader.java:137-139`）—— **在 drain loop 的锁外、所有 entry 已经全部投递完之后**。

## 3. race 窗口图示

```
drain loop:
  ... 投递最后一条 entry ...
  synchronized(lock) { onRecoveredStateBuffer(buf); advance cursor }     # cursor 到末尾
  ↑ Step 1 此刻已经认为 recoveryAlreadyDone=true
  ...
  for (ch : allChannels) ch.finishRecoveredBufferDelivery();              # 才翻 allDelivered=true
  ↑ Step 2 在此之前看 isInRecovery 仍然 true
```

第二种窗口：`finishRecoveredBufferDelivery` 跑完之后（`allDelivered=true`、`buffers=[EndOfInputChannelStateEvent]`），但 task 还没消费完 sentinel 时，Step 2 看到 `buffers` 非空 → `isInRecovery=true`，Step 1 仍然 `recoveryAlreadyDone=true`。

## 4. 备选方案对比

| 方案 | 判定位置 | 动作粒度 | 副作用 |
| --- | --- | --- | --- |
| A：task 级条件对齐 | 在 Step 1 里聚合所有 channel 的 `isInRecovery()`：任一 channel 仍在 recovery 才插 | all-or-nothing：要么给**全部** channel 插，要么一个都不插 | 把已退出 recovery 的 channel 拉回 recovery 态（队列被 barrier 撑非空 → `isInRecovery` 又翻 true）；recovery 分支的 `wrapRecoveredBufferAsAvailability` 不识别 `RecoveryCheckpointBarrier` sentinel，会作为 `BufferAndAvailability` 暴露给下游处理链路 |
| B：per-channel 条件对齐 | Step 1 逐个 channel 看 `isInRecovery()`，**只给** in-recovery 的 channel 插 barrier | per-channel | 无 A 的副作用；改动稍大（接口加方法 + 两种 channel 各自实现） |
| C：无脑都插 | 不判断，永远遍历所有 channel 插 barrier | all-or-nothing | A 的全部副作用 + `spillFile.acquire()` 引用计数泄漏（fast-path 注释 `SpillFileReader.java:154-156` 写明"empty 路径 MUST NOT acquire"） |

选 **B**。下文都按 B 落地。

## 5. 修复代码改动

### 5.1 `RecoverableInputChannel`：加一个 `isInRecovery()` 抽象方法

只在接口上声明，让 `SpillFileReader` 通过接口调用，不强行下钻到 `RecoveredBufferQueue`（后者的锁规则两种 channel 不一样：Local 锁 `recoveredQueue` 自身，Remote 锁 `receivedBuffers`，详见 `RecoveredBufferQueue.java:43-49`）。

```
interface RecoverableInputChannel {
    InputChannelInfo getChannelInfo();
    void onRecoveredStateBuffer(Buffer buffer);
    void finishRecoveredBufferDelivery() throws IOException;
    /** 返回该 channel 当前是否仍在 recovery 阶段。实现持自家 monitor。 */
    boolean isInRecovery();
}
```

### 5.2 `LocalInputChannel.isInRecovery()`

```
@Override
public boolean isInRecovery() {
    synchronized (recoveredQueue) {
        return recoveredQueue.isInRecovery();
    }
}
```

### 5.3 `RemoteInputChannel.isInRecovery()`

```
@Override
public boolean isInRecovery() {
    synchronized (receivedBuffers) {
        return recoveredQueue.isInRecovery();
    }
}
```

锁选择跟 `getNextBuffer()` 里 `inRecovery` 读法严格保持一致（`RemoteInputChannel.java:333-335`），不引入新的锁顺序。

### 5.4 `SpillFileReader.snapshotAndInsertBarriers`

把 `recoveryAlreadyDone` 路径换成"per-channel 看 `isInRecovery()`"，并把 `spillFile.acquire()` 与"是否插 barrier"解耦：

```
@Override
public DiskSnapshot snapshotAndInsertBarriers(long checkpointId) throws IOException {
    SpillFile.Snapshot diskSnap;
    int startSegmentIndex;
    long startOffset;
    boolean diskSliceEmpty;

    synchronized (lock) {
        diskSnap = spillFile.snapshot();
        startSegmentIndex = currentSegmentIndex;
        startOffset = currentOffset;
        diskSliceEmpty = recoveryAlreadyDone(diskSnap, startSegmentIndex, startOffset);

        // Per-channel: 只给仍在 recovery 的 channel 插 barrier。
        // 注意：调 ch.isInRecovery() 进入 channel 自家 monitor，
        // 锁顺序仍是 SpillFileReader.lock → channel-internal queue monitor，
        // 与 drain 主路径一致（见类注释）。
        for (RecoverableInputChannel ch : allChannels) {
            if (ch.isInRecovery()) {
                ch.onRecoveredStateBuffer(
                        EventSerializer.toBuffer(
                                new RecoveryCheckpointBarrier(checkpointId), false));
            }
        }

        if (diskSliceEmpty) {
            // disk 上没东西要写 channel state，且 ref-count grant 与 close() 配对，
            // 这里就不 acquire。
            return DiskSnapshot.empty();
        }
        spillFile.acquire();
    }

    return new DiskSnapshot(
            diskSnap, new DiskSnapshot.StartPos(startSegmentIndex, startOffset), spillFile);
}
```

要点：

- **判定与 Step 2 严格对称**：双方都看 `RecoveredBufferQueue.isInRecovery()`，不会再出现"Step 1 跳过、Step 2 仍想找"的窗口。
- **barrier 插入与 `spillFile.acquire()` 解耦**：barrier 由 channel 自身是否 in-recovery 决定，`acquire/close` 由 disk slice 是否非空决定。两条引用计数路径不会被改变。
- **保留 `recoveryAlreadyDone` 的 fast-path 语义**：仅作"disk slice 是否非空"判断使用，不再兼任"是否插 barrier"。

### 5.5 现有 `Missing RecoveryCheckpointBarrier` 单测的语义

`LocalInputChannelTest.java:1194`、`RemoteInputChannelTest.java:2359` 是测**真异常路径**（异常仍应被抛），改动不影响。新增 race 回归测见 §6。

## 6. 测试

### 6.1 回归测：race 窗口 1（drain cursor 到末尾，allDelivered 仍 false）

新增 `SpillFileReaderTest`（如尚未存在则建文件）用例：

1. 构造一个 `SpillFileReader`，其 `SpillFile` 有 1 个 entry。
2. 用一个可控 `RecoverableInputChannel` 假实现：`onRecoveredStateBuffer` 把 buffer 收入内部 deque、`finishRecoveredBufferDelivery` set 一个 latch。
3. 在 drain loop 投完最后一个 entry 之后、`finishRecoveredBufferDelivery` 之前阻塞 drain（用闩锁），让 task 线程在这一刻调 `snapshotAndInsertBarriers(cp=6)`。
4. 断言：假 channel 的 deque 末尾有一个 `RecoveryCheckpointBarrier(6)`；返回的 `DiskSnapshot` 为 `empty`（因为 cursor 到末尾）。

### 6.2 回归测：race 窗口 2（allDelivered=true，buffers 仍含 sentinel）

同上构造，但让 drain 跑到 `finishRecoveredBufferDelivery` 之后、`recoveredQueue` 里只有 `EndOfInputChannelStateEvent` sentinel 时，task 线程进 `snapshotAndInsertBarriers`。断言 sentinel 之后追加了一个 `RecoveryCheckpointBarrier(6)`，`Step 2` 调 `collectPreRecoveryBarrier(6)` 能找到该 barrier 并返回 sentinel 之前的（空）data buffer 列表。

### 6.3 端到端

`UnalignedCheckpointRescaleITCase.shouldRescaleUnalignedCheckpoint[upscale pipeline from 2 to 3, sourceSleepMs = 0]`——本次失败最简单的复现 case，修复后应稳定通过 100 次循环。

### 6.4 已有测试

`SpillFileReaderTest`、`LocalInputChannelTest`、`RemoteInputChannelTest` 全量跑，确保 fast-path 改动没破坏 disk-slice 非空的现有行为。

## 7. 与已有 doc 的关系

- `recovered_buffer_queue_refactor.md`：本修复在抽取出的 `RecoveredBufferQueue` 之上扩展接口（加 `isInRecovery()` 委托方法到外面的 `RecoverableInputChannel`），不改 `RecoveredBufferQueue` 内部字段或锁约束。
- `end_of_input_event_missing_fix.md`：该 fix 解决"cpDuringRecovery=true、fresh-start channel 上 sentinel 没进 recoveredQueue"的 hang。本修复不动 sentinel 路径，只动 Step 1 的 barrier 判定条件。两者正交。

## 8. 不在本次范围

- Group 2 (`IllegalStateException: Queried for a buffer before requesting the subpartition.`)：根因是 `LocalInputChannel.requestSubpartitions()` 在 `PartitionNotFoundException` 路径下不设 `subpartitionView` 而是异步 retrigger，叠加 FLINK-38544 新增的物理 channel sentinel 让 `isInRecovery` 短暂翻 false，恰好夹在 retrigger 完成前。单独修，单独 commit / PR。
