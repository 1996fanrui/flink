# 设计：Phase 2 — InputChannel 侧（task thread 一侧）

> 范围：完整实施 [`simplify_approach/input_channel.md`](../simplify_approach/input_channel.md) §3 全部内容；包含字段重塑、`getNextBuffer` 改写、`checkpointStarted` 双分支、`stateConsumedFuture` 触发条件改写、`RecoveredInputChannel.toInputChannel()` 迁移路径改走新接口。Phase 2 **与 Phase 3 并行**——两侧只依赖 Phase 1 引入的接口。Phase 2 不引入任何 `SpillFile*` / `BufferRequester` 实现，也不接 `Alternating*` UC 触发；这两项由 Phase 4/5 完成。

## 1. 设计目标

满足 [`user_requirements.md`](./user_requirements.md) 中 REQ-Y6OP ~ REQ-YW7I。Phase 2 完成后：

- 三个 channel 全部 `implements RecoverableInputChannel`；drain（Phase 4 引入）可以通过该接口统一注入 recovery 数据
- `LocalInputChannel.recoveredBuffers` 字段类型由 `Deque<BufferAndBacklog>` 改为 `Deque<Buffer>`，与 Remote 侧同形态
- `checkpointStarted` 内的 in-recovery 分支具备完整 cpId-bounded 扫描能力（即 Phase 5 dispatcher 抛进来的 `RecoveryCheckpointBarrier` sentinel 能被正确捕获）
- `stateConsumedFuture` 由 `(allRecoveredBuffersDelivered && recoveredBuffers.isEmpty())` 决定，与 Phase 4 drain 的"先推 buffer、最后翻 flag"双步语义对齐

## 2. 现状分析

- 当前分支 commit `292cc4b9e2d` 已让 `LocalInputChannel` 拥有 `recoveredBuffers`（`Deque<BufferAndBacklog>`）与 `toBeConsumedBuffers`（FullyFilledBuffer splits）分离的结构，但 `recoveredBuffers` 元素类型、消费路径、字段语义仍是 FLINK-39018 的 pull 迁移路径。
- `RecoveredInputChannel.toInputChannel()` 调用 `toInputChannelInternal(remainingBuffers)`，由 `LocalRecoveredInputChannel` / `RemoteRecoveredInputChannel` 子类把 `ArrayDeque<Buffer>` 透传给 `LocalInputChannel` / `RemoteInputChannel` 的构造器（构造器参数 `ArrayDeque<Buffer> initialRecoveredBuffers`）。
- `RemoteInputChannel` 也有同样的 `initialRecoveredBuffers` 构造器参数，把元素直接塞入 `receivedBuffers`（不是 `recoveredBuffers`，因为目前 Remote 没有 `recoveredBuffers` 字段）。
- `RecoveredInputChannel` 自身在 master 已经有 `public void onRecoveredStateBuffer(Buffer)` 与 `public void finishReadRecoveredState()` 同名方法（服务 filter-off 路径），方法体保留即可直接满足 `RecoverableInputChannel` 接口契约。

## 3. 修改范围

### 3.1 `LocalInputChannel`

字段层面：

- `recoveredBuffers` 字段类型由 `Deque<BufferAndBacklog>` 改为 `Deque<Buffer>`
- 新增 `private boolean allRecoveredBuffersDelivered = false`（无 volatile，由 `synchronized(recoveredBuffers)` monitor 保护）
- 删除 `private volatile boolean hasPendingPriorityEvent`？—— 不删除，但读点迁移；详见 §3.1.3
- 构造器签名移除 `ArrayDeque<Buffer> initialRecoveredBuffers` 参数；构造器体内对应的 `BufferAndBacklog` 包装迁移块整体删除

#### 3.1.1 `implements RecoverableInputChannel`

```java
public class LocalInputChannel extends InputChannel
        implements BufferAvailabilityListener, RecoverableInputChannel {
    ...
}
```

新增方法：

```java
@Override
public void onRecoveredStateBuffer(Buffer buffer) {
    // 调用方：drain（持有 SpillFileReader.lock）或 task thread Step 1（同样持有 lock）
    // 或 RecoveredInputChannel.toInputChannel 内的迁移路径（单线程顺序投递，无 SpillFileReader.lock；
    // 三种调用方在 javadoc 与 §3.3 都已声明）
    boolean wasEmpty;
    synchronized (recoveredBuffers) {
        if (isReleased) { buffer.recycleBuffer(); return; }
        wasEmpty = recoveredBuffers.isEmpty();
        recoveredBuffers.add(buffer);
    }
    if (wasEmpty) {
        notifyChannelNonEmpty();
    }
}

@Override
public void finishReadRecoveredState() throws IOException {     // 接口签名带 throws IOException
    // end-of-drain 例外，调用方无须持有 SpillFileReader.lock
    boolean shouldCompleteFuture;
    synchronized (recoveredBuffers) {
        allRecoveredBuffersDelivered = true;
        shouldCompleteFuture = recoveredBuffers.isEmpty();
    }
    if (shouldCompleteFuture) {
        stateConsumedFuture.complete(null);   // CompletableFuture.complete 本身幂等：多次调用安全（后续返回 false）
    }
}
```

`stateConsumedFuture` 在 master 上仅存在于 `RecoveredInputChannel`（filter-off 路径完成入口）。Phase 2 在 `LocalInputChannel` / `RemoteInputChannel` 上**新增**同名 `CompletableFuture<Void>` 字段（与 super 抽象层对齐；具体加在 `InputChannel` 父类还是分别加在 Local / Remote 子类，由开发期依据 master 实际类层级决定，本设计仅约束语义）。完成调用直接走 `complete(null)`——`CompletableFuture.complete` 多次调用幂等且 not throw。这是本设计选定的唯一完成机制，开发阶段不允许引入额外同步原语。

#### 3.1.2 `getNextBuffer()` 重写

```java
@Override
public Optional<BufferAndAvailability> getNextBuffer() throws IOException {
    checkError();

    boolean inRecovery;
    synchronized (recoveredBuffers) {
        inRecovery = !allRecoveredBuffersDelivered || !recoveredBuffers.isEmpty();
    }

    if (inRecovery) {
        if (hasPendingPriorityEvent) {
            return pullPriorityFromSubpartitionView();   // 沿用 cebc174ad5f 优先事件逻辑
        }
        Buffer buf;
        boolean drainedLast;
        synchronized (recoveredBuffers) {
            if (recoveredBuffers.isEmpty()) {
                return Optional.empty();                 // 阻塞普通 upstream
            }
            buf = recoveredBuffers.poll();
            drainedLast = recoveredBuffers.isEmpty() && allRecoveredBuffersDelivered;
        }
        if (drainedLast) {
            completeStateConsumedFuture();               // §3.7 触发点之一
        }
        return wrapAsBufferAndAvailability(buf);
    }

    // !inRecovery：master 既有路径（toBeConsumedBuffers splits → subpartitionView）
    if (!toBeConsumedBuffers.isEmpty()) {
        return getBufferAndAvailability(toBeConsumedBuffers.removeFirst());
    }
    return masterExistingSubpartitionViewPath();
}
```

- `pullPriorityFromSubpartitionView()` 把现有 `getNextRecoveredBuffer()` 中处理 `hasPendingPriorityEvent` 的分支搬过来（包括 reset 标志、修正 `expectedNextDataType` 为 `recoveredBuffers` 首元素 datatype 的逻辑）。区别：现在 `recoveredBuffers` 元素是 `Buffer`，"首元素 datatype" 用 `recoveredBuffers.peek().getDataType()` 直接拿到。
- `wrapAsBufferAndAvailability(buf)` 复用现有 `getBufferAndAvailability` 风格：包装为 `BufferAndAvailability` 时计算 `nextDataType`：
  - 若 `recoveredBuffers` 非空：取 `recoveredBuffers.peek().getDataType()`
  - 若 `recoveredBuffers` 空但 `allRecoveredBuffersDelivered == false`：取 `Buffer.DataType.NONE`（drain 还会继续推）
  - 若 `recoveredBuffers` 空且 `allRecoveredBuffersDelivered == true`：动态探测 `subpartitionView.getAvailabilityAndBacklog(true)`，与现有"最后一条 recovered buffer 的 next data type 动态探测"保持一致
- `buffersInBacklog` 在 recovery 阶段统一为 0（与现有 FLINK-39018 行为一致），sequence number 从 `Integer.MIN_VALUE` 起递增——递增计数器需要从 channel-level field 维护（取代构造器一次性赋值的旧形态），单线程读写（task thread）。

#### 3.1.3 `hasPendingPriorityEvent` / `notifyPriorityEvent` （**仅 LocalInputChannel**）

`hasPendingPriorityEvent` 与 `notifyPriorityEvent` override 是 `LocalInputChannel` 专属，`RemoteInputChannel` 不引入该字段（Remote 经 `addPriorityBuffer` / `receivedBuffers` 头位 priority 项感知优先事件，无需独立 flag）。Local 保留 master 既有形态：`notifyPriorityEvent` 设 `hasPendingPriorityEvent = true` 并调用 super；字段保留 `volatile`（network 线程写、task thread 读，跨线程访问）。
读点（reset 与 next-data-type 修正）随 `getNextRecoveredBuffer()` 方法体合并到 `getNextBuffer()` 新 in-recovery 分支后被 `pullPriorityFromSubpartitionView()` 持有。

#### 3.1.4 `checkpointStarted(CheckpointBarrier)` 双分支

```java
public void checkpointStarted(CheckpointBarrier barrier) throws CheckpointException {
    synchronized (recoveredBuffers) {     // Local channel monitor
        boolean inRecovery = !allRecoveredBuffersDelivered || !recoveredBuffers.isEmpty();
        if (inRecovery) {
            assert receivedBuffersHasNoLiveDataBuffer();   // Local 实现恒为 true（无 receivedBuffers 字段）

            List<Buffer> retained = new ArrayList<>();
            Iterator<Buffer> it = recoveredBuffers.iterator();
            while (it.hasNext()) {
                Buffer b = it.next();
                if (b instanceof RecoveryCheckpointBarrier
                        && ((RecoveryCheckpointBarrier) b).getCheckpointId() == barrier.getId()) {
                    it.remove();
                    break;
                }
                retained.add(b.retainBuffer());
            }
            channelStateWriter.addInputData(
                    barrier.getId(),
                    channelInfo,
                    ChannelStateWriter.SEQUENCE_NUMBER_RESTORED,
                    CloseableIterator.fromList(retained, Buffer::recycleBuffer));
        } else {
            // master 既有：startPersisting(barrier.getId(), Collections.emptyList())
            channelStatePersister.startPersisting(barrier.getId(), Collections.emptyList());
        }
    }
}
```

`receivedBuffersHasNoLiveDataBuffer()` 为 channel-internal helper：Local 实现恒为 true（Local 没有 receivedBuffers）。

注意：当前 commit `292cc4b9e2d` 把扫描 `recoveredBuffers`（`Deque<BufferAndBacklog>`）作为 in-recovery 分支已经实现一半。Phase 2 必须把"扫描 → barrier sentinel 终止 + retainBuffer"形态完整替换进去，**删除**现存的 `for (BufferAndBacklog bufferAndBacklog : recoveredBuffers)` 简单遍历。

#### 3.1.5 `requestSubpartitions()`

保留 commit `292cc4b9e2d` 恢复的 `checkState(toBeConsumedBuffers.isEmpty())`。
不再 require `recoveredBuffers.isEmpty()`——recovery 数据有可能在 `requestSubpartitions()` 时还在被 drain 投递。

#### 3.1.6 辅助方法

- `getBuffersInUseCount` / `unsynchronizedGetNumberOfQueuedBuffers`：保留 `recoveredBuffers.size() + toBeConsumedBuffers.size() + view.size`
- `releaseAllResources`：保留对两个队列的清理，但 `recoveredBuffers` 内元素类型变 `Buffer`（不再是 `BufferAndBacklog`），清理代码直接对元素 `recycleBuffer()`

### 3.2 `RemoteInputChannel`

字段：

- 新增 `private final Deque<Buffer> recoveredBuffers = new ArrayDeque<>()`（**新字段**，与 Local 同形态）
- 新增 `private boolean allRecoveredBuffersDelivered = false`，由 `synchronized(receivedBuffers)` monitor 保护
- 构造器签名删除 `ArrayDeque<Buffer> initialRecoveredBuffers` 参数；构造器体内现有"把 `initialRecoveredBuffers` 塞入 `receivedBuffers`" 的 for 循环整体删除

#### 3.2.1 `implements RecoverableInputChannel`

```java
public class RemoteInputChannel extends InputChannel
        implements RecoverableInputChannel {
    ...
}
```

`onRecoveredStateBuffer(Buffer)` / `finishReadRecoveredState()` 与 Local 同形态，但使用 `synchronized(receivedBuffers)` 作为 monitor（Remote 复用既有锁；不引入第三个锁对象）。

#### 3.2.2 `getNextBuffer()` 重写

`inRecovery` 判定与分发流程与 Local 一致；区别在分支体：

- in-recovery + 优先事件分支：经 `addPriorityBuffer` / `firstPriorityEvent` 链路（master 既有）从 `receivedBuffers` 头位 priority 项取出
- in-recovery + 普通 recovery 分支：从 `recoveredBuffers` 弹首元素
- 非 in-recovery 分支：master 既有 `receivedBuffers` 路径

`hasPendingPriorityEvent` 字段名在 Remote 不存在；priority 事件通过 master 既有 `addPriorityBuffer` 机制感知（数据已经在 `receivedBuffers` 内、头位优先），不需要新 flag。

#### 3.2.3 `checkpointStarted(CheckpointBarrier)` 双分支

```java
public void checkpointStarted(CheckpointBarrier barrier) throws CheckpointException {
    synchronized (receivedBuffers) {
        boolean inRecovery = !allRecoveredBuffersDelivered || !recoveredBuffers.isEmpty();
        if (inRecovery) {
            assert receivedBuffersHasNoLiveDataBuffer();   // Remote 遍历 receivedBuffers，断言所有元素 !isBuffer()（priority/control only）

            List<Buffer> retained = new ArrayList<>();
            // 与 Local 同样的 recoveredBuffers cpId-bounded 扫描
            ...
            channelStateWriter.addInputData(barrier.getId(), channelInfo, SEQUENCE_NUMBER_RESTORED, ...);
        } else {
            <master 既有 checkpointStarted 实现：channelStatePersister.startPersisting + maybePersist setup>
        }
    }
}
```

`receivedBuffersHasNoLiveDataBuffer()`：遍历 `receivedBuffers` 中所有元素，断言 `!buf.isBuffer()`（即只允许 priority/control buffer）。

### 3.3 `RecoveredInputChannel`

- 类签名追加 `implements RecoverableInputChannel`；既有 `public void onRecoveredStateBuffer(Buffer)` 与 `public void finishReadRecoveredState()` 方法直接满足接口契约，无需改动方法体
- `toInputChannel()` 内"剩余 buffer 交给物理 channel" 的迁移过程改写：

```java
public final InputChannel toInputChannel() throws IOException {
    ...
    final ArrayDeque<Buffer> remainingBuffers = ...;       // master 既有提取逻辑
    final InputChannel inputChannel = toInputChannelInternal();   // 构造器不再需要 initialRecoveredBuffers
    if (inputChannel instanceof RecoverableInputChannel) {
        RecoverableInputChannel rec = (RecoverableInputChannel) inputChannel;
        // 注意：本 phase Migration 路径无 SpillFileReader.lock 保护，由 RecoveredInputChannel 自身 monitor 守护
        // 单线程顺序投递，drain 此时尚未启动（filter-off 路径 / decouple 阶段都是顺序）
        for (Buffer buf : remainingBuffers) {
            rec.onRecoveredStateBuffer(buf);
        }
        rec.finishReadRecoveredState();
    } else {
        // 不应发生：所有 toInputChannelInternal 返回的 channel 在 Phase 2 之后必然 implements RecoverableInputChannel
        throw new IllegalStateException(...);
    }
    return inputChannel;
}
```

`toInputChannelInternal()` 签名同步移除 `ArrayDeque<Buffer> remainingBuffers` 参数；`LocalRecoveredInputChannel` / `RemoteRecoveredInputChannel` 的实现同步移除该参数。

注：`onRecoveredStateBuffer` javadoc 规定调用方必须持有 `SpillFileReader.lock`。本迁移路径**没有** `SpillFileReader.lock`（因为根本未引入 spill），但 simplify_approach 设计本意是 drain push 唯一调用方。`RecoveredInputChannel.toInputChannel()` 是 filter-off 路径的"一次性顺序迁移"，时间上发生在 channel 公开之前（无并发投递者）；本设计在 javadoc 中追加一段"或迁移路径单线程顺序投递" 的例外说明，保留语义一致性。

### 3.4 `LocalRecoveredInputChannel` / `RemoteRecoveredInputChannel`

- `toInputChannelInternal()` 签名删除 `ArrayDeque<Buffer> remainingBuffers` 参数
- 内部不再向构造器透传该参数
- 无其他改动

### 3.5 `SingleInputGate` / `InputChannelBuilder`（测试 fixture）

- `LocalInputChannel` / `RemoteInputChannel` 构造器参数变化对应 caller 同步更新：删除 `initialRecoveredBuffers` 实参
- 测试 fixture 改用"构造 channel + 顺序 `onRecoveredStateBuffer + finishReadRecoveredState`"模式作为等价替代

### 3.6 `LocalInputChannelTest` / `RemoteInputChannelTest`

- 删除：`getNextBuffer` 早分支在 `toBeConsumedBuffers` 上的 recovery-aware 行为单测（这些测试在 commit `292cc4b9e2d` 之后已经迁到 `recoveredBuffers`；Phase 2 进一步把元素类型由 `BufferAndBacklog` 改为 `Buffer`，所有这些测试需要按新形态改写）
- 修改：现有 9 个 FLINK-39018 系列回归测试中的"构造 channel 时塞 `initialRecoveredBuffers`" → 改为"构造 channel → 调用 `onRecoveredStateBuffer` 顺序投递 → `finishReadRecoveredState()`" 模式
- 新增覆盖：
  - `inRecovery` 四种边界：(flag=false,queue=empty)、(flag=false,queue=非空)、(flag=true,queue=非空)、(flag=true,queue=空)
  - `checkpointStarted` in-recovery 分支：cpId-bounded 扫描正确捕获 `RecoveryCheckpointBarrier(cpId)`、pre-barrier buffers `retainBuffer()` 后交给 `addInputData`、sentinel 自身被移除
  - `checkpointStarted` 非 in-recovery 分支：保留 master 既有持久化路径
  - `stateConsumedFuture` 完成路径：
    1. 标志先翻、最后一条 buffer 被消费时触发
    2. 队列先空、`finishReadRecoveredState` 触发
  - `receivedBuffersHasNoLiveDataBuffer()`（Remote）：含 live data 时 assert 触发

## 4. 不变式

- **锁顺序**：`SpillFileReader.lock → channel monitor`。本 phase 不引入 SpillFileReader，但所有 channel-monitor 取锁顺序必须保证 channel monitor 永远是内层锁（不可在 channel monitor 内反向去取 `SpillFileReader.lock`）。
- **monitor 保护路径**（强制枚举，开发期间必须逐项加锁，禁止遗漏）：
  - `LocalInputChannel`（`synchronized(recoveredBuffers)`）：`onRecoveredStateBuffer` 写、`finishReadRecoveredState` 翻 flag、`getNextBuffer` 读 + poll、`checkpointStarted` in-recovery 扫描、`releaseAllResources` 清空两个队列。`getBuffersInUseCount` / `unsynchronizedGetNumberOfQueuedBuffers` 因接口语义本就是"近似值"（master 既有命名 `unsynchronized*` 说明无锁），保留 unsynchronized approximate count，不加锁。
  - `RemoteInputChannel`（`synchronized(receivedBuffers)` 复用 master 既有 monitor）：上述同款路径之外，master 既有 `onBuffer` / 网络入队链路本就在该 monitor 内，本 phase 不增减——只需让 `onRecoveredStateBuffer` / `getNextBuffer` 的 recoveredBuffers 读写共用该 monitor。
- **temporal mutex**：in-recovery 阶段禁止 upstream live data buffer 出现在 `receivedBuffers`；由 `receivedBuffersHasNoLiveDataBuffer()` 断言守护。
- **`hasPendingPriorityEvent`** （仅 LocalInputChannel）：跨线程写（network 线程）/ 读（task thread），保留 `volatile` 修饰；不引入额外锁。
- **`stateConsumedFuture` 完成唯一性**：直接依赖 `CompletableFuture.complete(null)` 自带的幂等语义——多次调用安全（后续 complete 返回 false 但 not throw）。设计阶段已定型，不允许实现阶段引入 AtomicBoolean 或额外同步原语。

## 5. 代码组织

修改集中在 `flink-runtime/src/main/java/org/apache/flink/runtime/io/network/partition/consumer/` 包下：

- `LocalInputChannel.java`、`RemoteInputChannel.java`、`RecoveredInputChannel.java`、`LocalRecoveredInputChannel.java`、`RemoteRecoveredInputChannel.java`、`SingleInputGate.java`
- 测试 fixture：`InputChannelBuilder.java`（如果存在），`LocalInputChannelTest.java`、`RemoteInputChannelTest.java`

不新增文件（接口与 sentinel 由 Phase 1 引入）。

**提交策略**：本 phase 与其他 4 个 phase 共用同一开发分支，**不发 PR**；完成后作为**单一 commit** 推到分支，禁止 `git commit --amend` / `git rebase -i` 重写历史。完整规则参 [`../simplify_approach/task_breakdown.md`](../simplify_approach/task_breakdown.md) "Commit 策略" 段。

## 6. 兼容性

- 构造器签名变更（删除 `initialRecoveredBuffers`）→ 对内部 callers 直接修改（`SingleInputGate`、`InputChannelBuilder`、测试 fixture），不属于公开 API。CLAUDE.md "内部 public method 不保留兼容性" 适用。
- 接口 `RecoverableInputChannel` 由 Phase 1 引入，Phase 2 完成后所有现有 `RecoveredInputChannel` 子类调用方仍可正常工作（`RecoveredInputChannel` 既有同名方法直接满足）。

## 7. 验证策略

通过 `LocalInputChannelTest` / `RemoteInputChannelTest` 单元测试套覆盖；具体验收方法见 [`acceptance_test.md`](./acceptance_test.md)。ITCase 留给 Phase 5。

## 8. 已驳回的替代方案

- **`recoveredBuffers` 保留 `Deque<BufferAndBacklog>` 元素类型**：会让 drain 在 push 时必须先包装 `BufferAndBacklog`，但 drain 不掌握 `nextDataType`、`buffersInBacklog`、`sequenceNumber`，导致 drain 与 channel 间产生冗余包装逻辑——参考 simplify_approach `input_channel.md` §3.2 的字段表
- **`allRecoveredBuffersDelivered` 用 AtomicBoolean**：和现有 monitor-based 唤醒链路风格不一致；通过 channel monitor 守护 + 双重检查的方式更符合 master 既有风格
- **不引入 `RecoveryCheckpointBarrier` cpId-bounded 扫描，由 dispatcher 调用 `recoveredBuffers.size()` 一次性 retain 全部**：会把当前队列内的"barrier 后到达的元素"也错误归入本 cpId 持久化集，违反 simplify_approach `coordination.md` §5 correctness 证明
- **Migration 路径继续走构造器 `initialRecoveredBuffers` 参数**：违反 simplify_approach `input_channel.md` §3.5 第一行——"Migration 还在发生，但表达形式必须迁移到新接口路径"
- **`getNextBuffer` 在 in-recovery 阶段不阻塞普通 upstream，让 Local 的 `subpartitionView.getNextBuffer()` 可以继续被消费**：违反 in-recovery / not-in-recovery 互斥原则与 receivedBuffersHasNoLiveDataBuffer 断言；也会破坏 simplify_approach `coordination.md` §5 的 correctness 证明
