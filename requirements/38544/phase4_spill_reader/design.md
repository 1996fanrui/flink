# 设计：Phase 4 — Spill 读盘侧（drain + Step 1）

> 范围：实施 [`simplify_approach/unspiller.md`](../simplify_approach/unspiller.md) §3 / §4 与 [`simplify_approach/overview.md`](../simplify_approach/overview.md) §6.1 / §6.3 中描述的 drain 期段；扩展 Phase 3 的 `SpillFile` 增加只读 / 快照能力；新增 `SpillFileReader`、`DiskSnapshot` 完整实现、`RecoveredChannelBufferRequester`；删除 `RecoveredInputChannel.requestBufferBlocking` 的 heap fallback；把 drain 接入 `channelIOExecutor`。Phase 4 **依赖** Phase 1 + Phase 2 + Phase 3 全部 merge。

## 0. 与 simplify_approach 原始伪码的差异声明

`simplify_approach/unspiller.md` §3 的 drain 伪码中 `peekNextEntry` 在 `while` 条件中（锁外）。Round-1 review 期间一度把 peek 放入 lock，但分析后认定多余——filter 完成后 entries 不可变，peek 不需要锁保护——本设计回归原始伪码形态（见 §3.2.1）。

## 1. 设计目标

满足 [`user_requirements.md`](./user_requirements.md) 中 REQ-C0TF ~ REQ-M4EO。完成后：

- filter-on 路径的 drain 通过 `SpillFileReader.drain()` 闭环：disk → channel `recoveredBuffers`
- `RecoveredInputChannel.requestBufferBlocking` 不再 heap allocate，**FLINK-38544 的 OOM 路径在本 phase 真正消除**
- `RecoveryCheckpointTrigger.snapshotAndInsertBarriers()` 接入后 Phase 5 可以直接把 dispatcher 钩进去；本 phase 内 trigger 已经具备完整功能，但暂时只通过单元测试调用

## 2. 现状分析

- Phase 3 引入的 `SpillFile` 已经能完成 append + entries 记录 + 段轮转；本 phase 在同一类上扩展只读 / 快照能力
- Phase 1 引入的 `DiskSnapshot` 骨架在本 phase 填实
- master `RecoveredInputChannel.requestBufferBlocking` 的方法体（`RecoveredInputChannel.java:335-361`）当前结构：先 `requestExclusiveBuffers` → 若 `!checkpointingDuringRecoveryEnabled` 走原始 blocking → 否则尝试 `bufferManager.requestBuffer()`，失败则 heap allocate。Phase 4 把 heap fallback + `checkpointingDuringRecoveryEnabled` 分支整体删除，恢复为单一 `bufferManager.requestBufferBlocking()` 路径
- master 的 `channelIOExecutor` 在 `SingleInputGate` 内部 lazy 初始化；conversion 完成由 master 既有 mailbox 驱动

## 3. 修改范围

### 3.1 `SpillFile` 扩展（Phase 3 同文件追加方法）

```java
public final class SpillFile implements Closeable {

    // Phase 3 已有：append / close / segments / entries

    /** 调用方：task thread Step 1，已持有 SpillFileReader.lock。返回不可变快照。 */
    public Snapshot snapshot() {
        return new Snapshot(
            Collections.unmodifiableList(new ArrayList<>(segments)),   // 浅拷贝当前段列表（Segment 内部 currentEnd 此时已不变，filter 完成后只读）
            new ArrayList<>(entries));                                  // entries 深拷贝（值类型 Entry 不可变）
    }

    public static final class Snapshot {
        public final List<SpillFileSegment> segments;
        public final List<Entry> entries;
        Snapshot(...) { ... }
    }

    /** SpillFileSegment 扩展（新增方法）： */
    static final class SpillFileSegment implements Closeable {
        // Phase 3 已有：segmentIndex / path / channel / currentEnd / close

        /** 调用方：drain（独立持有该段读句柄）。 */
        Entry peekNextEntry();
        Entry pollNextEntry();

        /** 调用方：drain 或 cpId-reader。本方法不维护共享文件位置；每个 reader 必须自行持有
         *  通过 `FileChannel.open(path, StandardOpenOption.READ)` 取得的独立句柄，并通过 `FileChannel.read(dst, position)` 读取。 */
        void readBytesAt(long offset, int length, byte[] dest) throws IOException;
    }
}
```

设计要点：

- **段不可变**：filter 完成后 `SpillFileSegment.currentEnd` 不再变化，因此 `snapshot()` 浅拷贝段列表对 cpId-reader 来说是只读访问
- **多读句柄**：drain 与每个 cpId-reader 各自通过 `FileChannel.open(path, StandardOpenOption.READ)` 打开**独立** `FileChannel` 句柄，调用 `read(ByteBuffer, position)` 读取段。每个 reader 持有独立 file position；规避 JDK Javadoc 中"共享 FileChannel 的 positional read 是否真正并发是实现依赖"的不确定性。`readBytesAt` 实现内部封装"取独立 FileChannel + pread" 细节；segment 的 base FileChannel（filter 期写入用）在 Phase 3 filter 完成时 close，其他 reader 不再共享该句柄
- **`peekNextEntry` / `pollNextEntry`**：仅 drain 使用，维护段自身的"下一个待消费 entry"指针；cpId-reader 不调用这些方法，而是通过 `DiskSnapshot` 自身的迭代器内部状态

### 3.2 新增类：`SpillFileReader`

路径：`flink-runtime/src/main/java/org/apache/flink/runtime/checkpoint/channel/SpillFileReader.java`

```java
public final class SpillFileReader implements RecoveryCheckpointTrigger, Closeable {

    private final SpillFile spillFile;
    private final List<RecoverableInputChannel> allChannels;
    private final Map<InputChannelInfo, RecoverableInputChannel> channelByInfo;
    private final BufferRequester bufferRequester;

    /** 全局锁。命名字段，非 this monitor。Guards:
     *  (a) channel `recoveredBuffers` 写入（通过 onRecoveredStateBuffer），
     *  (b) currentSegmentIndex / currentOffset 推进，
     *  (c) Step 1 barrier 插入序列。
     *  end-of-drain finishReadRecoveredState 不在本锁守护范围内（见 simplify_approach/coordination.md §1 例外）。 */
    private final Object lock = new Object();

    @GuardedBy("lock") private int currentSegmentIndex = 0;
    @GuardedBy("lock") private long currentOffset = 0L;

    public SpillFileReader(SpillFile spillFile,
                           List<RecoverableInputChannel> allChannels,
                           BufferRequester bufferRequester);

    /** 调用方：channelIOExecutor，conversion 完成后 submit。
     *  buffer 申请 + 磁盘读取在 lock 外；只有"投递 + 推进 offset" 在 lock 内。
     *  全段消费完后在 lock 外对每个 channel 调用 finishReadRecoveredState()。 */
    public void drain() throws IOException, InterruptedException;

    /** 实现 RecoveryCheckpointTrigger。调用方：task thread Step 1，禁止持有本 lock。 */
    @Override public DiskSnapshot snapshotAndInsertBarriers();   // 实际签名带 cpId 参数；具体见 §3.4

    /** drain 完成后释放：调用 bufferRequester.releaseExclusiveBuffers() 后关闭 SpillFile。 */
    @Override public void close() throws IOException;
}
```

#### 3.2.1 `drain()` 实施

```java
public void drain() throws IOException, InterruptedException {
    for (SpillFileSegment seg : spillFile.segments()) {
        Entry e;
        while ((e = seg.peekNextEntry()) != null) {     // lock-free：filter 完成后 entries 不可变
            RecoverableInputChannel ch = channelByInfo.get(e.channelInfo);
            if (ch == null) {
                throw new IllegalStateException("Drain: no channel for " + e.channelInfo);
            }

            // (A) 锁外：buffer 申请
            Buffer buf = bufferRequester.requestBufferBlocking(e.channelInfo);

            // (B) 锁外：磁盘读
            seg.readBytesAt(e.offset, e.length, buf.getMemorySegment().asByteArray());

            // (C) 锁内：投递 + 推进（Principle 1 + 2 唯一保护区间）
            synchronized (lock) {
                ch.onRecoveredStateBuffer(buf);
                seg.pollNextEntry();
                currentSegmentIndex = seg.segmentIndex;
                currentOffset = e.offset + e.length;
            }
        }
        seg.close();
    }
    // (D) 锁外：finishReadRecoveredState
    for (RecoverableInputChannel ch : allChannels) {
        ch.finishReadRecoveredState();
    }
}
```

注意：

- `peekNextEntry` 在 lock **外**调用——filter 完成（bufferFilteringCompleteFuture 已 complete）之后 `SpillFile.entries` 即冻结，drain 是 entries 的唯一推进者，task thread 的 Step 1 `snapshot()` 只**读**而不修改 entries，因此 peek 阶段无并发写。与 simplify_approach `unspiller.md` §3 原始伪码完全一致；上一版把 peek 误放锁内会增加 Step 1 的锁竞争，无收益。
- `pollNextEntry` 必须在投递 buffer 之后立即调用（同临界区）——Principle 2 强制要求"offset 推进 + 投递"原子。
- `requestBufferBlocking` 内部 park 在 `BufferManager.bufferQueue`（`Object.wait/notifyAll`），不能在 lock 内调用——否则 buffer pool 抖动会卡死 Step 1。
- `readBytesAt` 使用 pread（`FileChannel.read(ByteBuffer, position)`）；每个 reader（drain 一个 + 每个 cpId Step 3 reader）持**独立 FileChannel 句柄**（通过 `FileChannel.open(path, StandardOpenOption.READ)` 各自打开），不共享 file position，规避 JDK Javadoc 中关于"共享 FileChannel 的 positional read 是否真正并发是实现依赖"的不确定性。

#### 3.2.2 `snapshotAndInsertBarriers()` 实施

`RecoveryCheckpointTrigger` 接口（Phase 1 引入）声明 `DiskSnapshot snapshotAndInsertBarriers(long checkpointId)`（cpId 由 Phase 5 dispatcher 透传）。本 phase 直接实现该签名：

```java
@Override
public DiskSnapshot snapshotAndInsertBarriers(long cpId) {
    SpillFile.Snapshot diskSnap;
    int startSegmentIndex;
    long startOffset;
    List<RecoverableInputChannel> channelsSnapshot = allChannels;  // immutable since construction

    synchronized (lock) {
        diskSnap = spillFile.snapshot();
        startSegmentIndex = currentSegmentIndex;
        startOffset = currentOffset;

        // recovery 已结束：drain 已完成（在 (D) 之后 lock 内不再有写入），diskSnap.entries 为空
        // → 返回空 DiskSnapshot，不插任何 sentinel
        if (recoveryAlreadyDone(diskSnap)) {
            return DiskSnapshot.empty();
        }

        for (RecoverableInputChannel ch : channelsSnapshot) {
            ch.onRecoveredStateBuffer(new RecoveryCheckpointBarrier(cpId));
        }
    }

    return new DiskSnapshot(diskSnap, new DiskSnapshot.StartPos(startSegmentIndex, startOffset));
}
```

`recoveryAlreadyDone(diskSnap)` 的判断：`diskSnap.entries` 中所有条目的 `(segmentIndex, offset)` 均严格 < `(currentSegmentIndex, currentOffset)`（按二元组词典序比较），即 drain 已全部消费。仅以此一种形态表达，不引入容易产生运算符优先级歧义的简写。

feature-off 情况下 caller 不应实例化 `SpillFileReader`，因此 `snapshotAndInsertBarriers` 不会被调用；Phase 5 的 dispatcher 通过"`recoveryCheckpointTrigger` 字段是否为 null" 区分 feature on/off，或通过 null-object 模式注入空 trigger（具体由 Phase 5 设计）。

#### 3.2.3 `close()`

```java
@Override
public void close() throws IOException {
    bufferRequester.releaseExclusiveBuffers();   // 释放 source channel exclusive buffers
    spillFile.close();                           // Phase 5 接 ref counter 时改为 release()
}
```

Phase 5 引入 ref counter 后，`close()` 改为 `release()` 调用，由 ref counter 决定真正删段时机。本 phase 暂用直接 close，预留 hook。

### 3.3 `DiskSnapshot` 完整实现（Phase 1 骨架在本 phase 填实）

```java
public final class DiskSnapshot implements CloseableIterator<DiskSnapshot.Chunk> {

    public static final class Chunk {
        public final InputChannelInfo channelInfo;
        public final byte[] data;
        public final int length;
    }

    // StartPos 内部类在 Phase 1 已以骨架形式声明（占位）；本 phase 仅消费

    private final SpillFile.Snapshot snapshot;
    private final StartPos startPos;
    private int entryCursor = 0;
    private boolean closed = false;

    public DiskSnapshot(SpillFile.Snapshot snapshot, StartPos startPos);

    static DiskSnapshot empty();   // hasNext()=false, close()=no-op

    @Override public boolean hasNext() { skipPreDrained(); return entryCursor < snapshot.entries.size(); }
    @Override public Chunk next() { skipPreDrained(); /* read bytes via SpillFileSegment.readBytesAt */ ... }
    @Override public void close();   // Phase 5 接 ref counter 后释放本 reader 引用计数

    /** 跳过 entryPos < startPos 的 entry —— 已被 drain 投递。 */
    private void skipPreDrained() {
        while (entryCursor < snapshot.entries.size()) {
            Entry e = snapshot.entries.get(entryCursor);
            boolean preDrained = (e.segmentIndex < startPos.segmentIndex)
                || (e.segmentIndex == startPos.segmentIndex && e.offset < startPos.offset);
            if (!preDrained) break;
            entryCursor++;
        }
    }
}
```

`next()` 内的 byte 读取通过 `snapshot.segments.get(e.segmentIndex).readBytesAt(...)` 完成；分配的 `byte[]` 大小 = `e.length`。

### 3.4 新增类：`RecoveredChannelBufferRequester`

路径：`flink-runtime/src/main/java/org/apache/flink/runtime/checkpoint/channel/RecoveredChannelBufferRequester.java`

```java
final class RecoveredChannelBufferRequester implements BufferRequester {

    private final Map<InputChannelInfo, RecoveredInputChannel> channelMap;

    RecoveredChannelBufferRequester(Map<InputChannelInfo, RecoveredInputChannel> map) {
        this.channelMap = map;
    }

    @Override
    public Buffer requestBufferBlocking(InputChannelInfo info)
            throws InterruptedException, IOException {
        RecoveredInputChannel ch = channelMap.get(info);
        if (ch == null) throw new IllegalStateException("No source channel for " + info);
        return ch.requestBufferBlocking();
    }

    @Override
    public void releaseExclusiveBuffers() throws IOException {
        for (RecoveredInputChannel ch : channelMap.values()) {
            ch.releaseAllResources();      // 依赖 Phase 1 提升的可见性
        }
    }
}
```

`channelMap` 由 caller（接入点位于 `channelIOExecutor` 启动 drain 前）构造。注意：source 是 `RecoveredInputChannel`（filter 阶段的源 channel），目的是 `RecoverableInputChannel`（physical channel）。两个 map 不是同一个。

### 3.5 删除 `RecoveredInputChannel.requestBufferBlocking` 的 heap fallback

`RecoveredInputChannel.java:335-361` 简化为：

```java
public Buffer requestBufferBlocking() throws InterruptedException, IOException {
    if (!exclusiveBuffersAssigned) {
        bufferManager.requestExclusiveBuffers(networkBuffersPerChannel);
        exclusiveBuffersAssigned = true;
    }
    return bufferManager.requestBufferBlocking();
}
```

删除：

- `inputGate.isCheckpointingDuringRecoveryEnabled()` 分支
- `bufferManager.requestBuffer()` 非阻塞尝试
- `MemorySegmentFactory.allocateUnpooledSegment` + `new NetworkBuffer(memorySegment, FreeingBufferRecycler.INSTANCE)`
- 相关 import（`MemorySegmentFactory`、`MemoryManager`、`NetworkBuffer`、`FreeingBufferRecycler`，如不再被其他位置使用）

Phase 3 之后 filter 路径已不经过此方法（filter 使用 `FilteredBufferWriter` 自己持有的 prefilter/postfilter buffer）；Phase 4 之后 drain 路径通过 `RecoveredChannelBufferRequester` → 此方法获取 buffer，但只在没有 heap fallback 的纯阻塞路径下取。

### 3.6 drain 接入：`channelIOExecutor` 路径

`channelIOExecutor` 字段当前定义在 `StreamTask`（`StreamTask.java:305` 附近），`SingleInputGate` 自身不持有该引用。本设计明确以下注入路径：

- **executor 引用获取**：`StreamTask` 在 `restoreInternal` / `recoverInputs` 链路上已经持有 `channelIOExecutor`；通过新增 `setChannelIOExecutor(Executor)` setter（或在 `SequentialChannelStateReaderImpl` 的构造参数上扩展）把 executor 注入到负责接管 `SpillFile` 与构造 `SpillFileReader` 的对象
- **触发位置**：filter 完成（`bufferFilteringCompleteFuture` 已 complete）→ mailbox 调度 conversion → conversion 完成的回调 chain 中，由 task thread 同步执行：
  1. 从 Phase 3 留下的 `SpillFile` 构造 `SpillFileReader`
  2. 从 `RecoveredInputChannel` map 构造 `RecoveredChannelBufferRequester`
  3. 从 physical channel map 构造 `List<RecoverableInputChannel> allChannels`
  4. `channelIOExecutor.execute(() -> { try { reader.drain(); } catch (Throwable t) { asyncExceptionHandler.handleAsyncException("drain", t); } finally { reader.close(); } })`
- **filter-off**：以上 4 步全部跳过；与 master 路径完全一致；feature flag 由 `inputGate.isCheckpointingDuringRecoveryEnabled()` 守护
- **异常处理**：drain 抛任何异常通过 `StreamTask.asyncExceptionHandler.handleAsyncException(...)`（`StreamTask.java:897-900` 既有机制）冒泡到 task，与 master `channelIOExecutor` 内部异常传递语义一致；reader 在 finally 中 close 释放资源

具体在 `StreamTask` 哪一行串入回调由开发阶段定，但本设计约束：drain submit 必须严格 happen-after conversion 完成，**不允许**直接在 `bufferFilteringCompleteFuture` 的 callback 上 submit（因为此时 conversion 还未发生，`SpillFileReader` 没有 physical channel 引用可用）。

### 3.7 不变之处

- `BufferManager.bufferQueue` / `BufferPool.BufferListener` 链路完全不动
- master 既有 `bufferFilteringCompleteFuture` / `stateConsumedFuture` 触发位置不动；本 phase 通过 channel 内部 `finishReadRecoveredState` 触发 `stateConsumedFuture`（Phase 2 已实现，本 phase 只负责调用）

## 4. 不变式

- **Principle 1**（recovery-side 写入必须在 lock 内）：drain 的 `onRecoveredStateBuffer(buf)` 与 task thread Step 1 的 `onRecoveredStateBuffer(barrier)` 均在 `synchronized(lock)` 内；end-of-drain `finishReadRecoveredState` 是例外，在 lock 外
- **Principle 2**（offset 推进与投递同临界区）：(C) 内严格"投递 → poll → 更新 offset" 顺序，三步同一 `synchronized(lock)`
- **锁顺序**：`SpillFileReader.lock → channel-internal queue monitor`；drain 与 Step 1 都遵守；Step 2（`channel.checkpointStarted` 内 `recoveredBuffers` 扫描）只取 channel monitor，不与 `SpillFileReader.lock` 同时持有
- **微秒级临界区**：(C) 临界区只做 2 个 in-memory 动作（投递 + offset 更新）；buffer 申请 / 磁盘读 / segment close 均在 lock 外
- **`recoveryAlreadyDone(diskSnap)`** 判定语义：`diskSnap.entries` 中所有条目的 `(segmentIndex, offset)` 均严格 < `(currentSegmentIndex, currentOffset)`（二元组词典序）—— drain 已消费所有 entry

## 5. 代码组织

- 新增 3 个文件（`SpillFileReader.java`、`DiskSnapshot.java` 实体补完、`RecoveredChannelBufferRequester.java`）
- 修改 3 个现有文件（`SpillFile.java` 扩展只读 API、`RecoveredInputChannel.java` 删除 heap fallback、`channelIOExecutor` 接入点所在文件）；`RecoveryCheckpointTrigger.java` 接口签名在 Phase 1 已包含 `long checkpointId` 参数，无需本 phase 修改
- 测试新增：`SpillFileReaderTest.java`、`DiskSnapshotTest.java`、`RecoveredChannelBufferRequesterTest.java`、`SpillFileSnapshotTest.java`、`SpillFileReaderConcurrencyTest.java`、`RecoveredInputChannelRequestBufferBlockingHeapFallbackRemovedTest.java`、`ChannelIOExecutorDrainSubmissionTest.java`

**提交策略**：本 phase 与其他 4 个 phase 共用同一开发分支，**不发 PR**；完成后作为**单一 commit** 推到分支，禁止 `git commit --amend` / `git rebase -i` 重写历史。完整规则参 [`../simplify_approach/task_breakdown.md`](../simplify_approach/task_breakdown.md) "Commit 策略" 段。

## 6. 兼容性

- `RecoveryCheckpointTrigger` 接口签名在 Phase 1 已固定为带 cpId，Phase 4 直接实现，无签名 churn
- `RecoveredInputChannel.requestBufferBlocking` 行为变化：filter-off 路径不再经过 `checkpointingDuringRecoveryEnabled` 检查；语义等价（filter-off 时不会走 heap fallback 分支）
- 删除 heap fallback 是用户可见的"OOM 修复" —— 这正是 FLINK-38544 目标

## 7. 验证策略

通过单元测试 + 并发 stress 测试覆盖 drain / Step 1 / heap fallback 删除；具体验收命令见 [`acceptance_test.md`](./acceptance_test.md)。ITCase 留给 Phase 5。

## 8. 已驳回的替代方案

- **Step 1 与 drain 共用一个锁但 Step 1 允许在锁内做磁盘 IO**：违反 simplify_approach `coordination.md` §1"task thread 临界区只做 in-memory 动作" 约束；会让 checkpoint 触发延迟波动
- **`DiskSnapshot` 通过持有 `SpillFile` 引用直接读盘（不预先快照 segments）**：会与 drain 共享 `SpillFileSegment.currentEnd` 等可变状态，需要复杂同步；预先 snapshot 浅拷贝更简单
- **drain 在 (A) buffer 申请阶段就持锁**：参考 simplify_approach `unspiller.md` §4 (A)/(C) 注释——buffer pool 抖动会引发 checkpoint stall
- **保留 heap fallback 作为兜底**：违反 FLINK-38544 目标；heap fallback 在 filter 路径已被 Phase 3 绕开，drain 路径没有"必须 heap" 的需求
- **drain 路径不复用 `RecoveredInputChannel.requestBufferBlocking`，直接经 `BufferManager`**：会让 source channel 的 exclusive buffer pool 不被使用，浪费已申请资源
- **接口签名拆两步演进（先无参再加 cpId）**：会在 Phase 1 → Phase 4 之间产生"立即被改写的中间签名" 的不友好状态；本设计统一在 Phase 1 即落定带 cpId 参数，避免后续 churn
