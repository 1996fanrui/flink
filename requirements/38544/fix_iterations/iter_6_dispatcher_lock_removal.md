# iter_6 用 lock ordering 同时根治 AB-BA 死锁与 checkpoint 丢数据

## 背景：两个问题其实是同一根

1. **丢数据现象**：`UnalignedCheckpointRescaleITCase.shouldRescaleUnalignedCheckpoint` 低频失败（约 1/49），表现为 `NUM_OUTPUTS != NUM_INPUTS` 或者 buffer 反序列化异常。即使 commit 6239eff9c19 把 `CHECKPOINTING_DURING_RECOVERY_ENABLED` 和 `UNALIGNED_RECOVER_OUTPUT_ON_DOWNSTREAM` 都关掉，仍然复现，说明 bug 在公共代码路径上。

2. **直接根因**：`RemoteInputChannel.checkpointStarted` 把 `channelStatePersister.startPersisting(...)` 调用挪到了 `synchronized(recoveredStore)` 之外。

## 为什么挪出来：AB-BA 死锁

涉及两把锁：

- **小锁**：每条 channel 自己的 `RecoveredBufferStoreImpl.this`（per-channel monitor）
- **大锁**：`FilteredBufferDispatcherImpl.this`（`write`、`onChannelCheckpointStarted`、`onChannelCheckpointStopped`、`onChannelReleased` 都是实例 `synchronized` 方法，全局 monitor）

如果 `startPersisting` 包在 `synchronized(recoveredStore)` 内，store 非空时会触发 `store.checkpoint() → coordinator.onChannelCheckpointStarted(...)`，这一回调要取大锁：

- Task 线程：小 → 大
- Recovery 线程在 dispatcher 的 `write` 路径里：大（`write` 是 synchronized）→ `flushCache()` → `store.addBuffer()` → 小

只要 filter 开启 + 恢复期间允许 checkpoint，两条路径并发就形成 AB-BA。注释里的"holding the store lock across would deadlock AB-BA"指的就是这条路径。

## 但挪出锁外引入了新 bug：丢数据窗口

`startPersisting` 内会无锁地写 `checkpointStatus = BARRIER_PENDING`。`maybePersist`（在 network 线程的 `onBuffer` 里、持小锁）读这个字段决定是否把 post-barrier data spill 到 channel state。

时序：

```
T1  task 持小锁，收集 inflightBuffers
T2  task 释放小锁                          ← checkpointStatus 还是 COMPLETED
T3  network 线程 onBuffer 抢到小锁
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

## 修复方向：保留两把锁 + 严格 lock ordering

用经典的 lock ordering 防死锁，而不是去消灭某把锁。两把锁都还在，但全局规定一个严格顺序，所有线程按同一个顺序取，AB-BA 自然不可能。

### 锁定义

- **小锁**：每条 channel 的 `RecoveredBufferStoreImpl.this`（per-channel）。语义不变。
- **大锁**：dispatcher 内一个显式 `private final Object dispatcherLock = new Object();`。**不是 `this`，不是 `synchronized` 方法**——显式锁对象让"哪些路径会取这把锁"在代码里一目了然，`synchronized` 方法容易让锁的存在被忽视。

### 四条规则

1. 两把锁都要拿 → **必须先小锁，再大锁**（per-channel → global）
2. 只需要大锁的地方 → 只拿大锁
3. **持大锁时严格禁止再去拿小锁**（任何会反向回到小锁的调用都不允许出现在 `synchronized(dispatcherLock)` 块内）
4. 只需要小锁的地方 → 只拿小锁

### 为什么这就够了

按规则，可能出现的取锁序列只有：
- `小`（task / network 操作 channel-local 状态）
- `大`（dispatcher 内部协调）
- `小 → 大`（task 持小锁调 coordinator）

不会出现 `大 → 小`，因为规则 3 明确禁止。等待图的边只有 `小 → 大` 一条方向，单向无环，死锁不可能。

### Rule 3 的强制要求与 forbidden callees

规则 3 是整个方案的安全基石，必须在实现层面被严格守护。runtime 上**不能**简单用 `assert !Thread.holdsLock(anyStore)` 来卡——因为 task 路径合法持 `store_C` 调 coordinator 进入大锁段，那一刻 task **正在**持有小锁，断言会误报。dispatcher 也无法从内部识别"caller 合法持有的那把小锁"。

落地手段（必须全部做到）：

1. **黑名单 callees**：在 `synchronized(dispatcherLock)` 段内，**禁止**直接或间接调用以下任何方法：
   - `RecoveredBufferStoreImpl.addBuffer(...)`
   - `RecoveredBufferStoreImpl.addBufferAndCaptureListener(...)`
   - `RecoveredBufferStoreImpl.incrementPending(...)`
   - 任何 `synchronized(store) { ... }` 代码块
   - 任何会通过 `ChannelStatePersister.{startPersisting,stopPersisting,maybePersist,checkForBarrier,hasBarrierReceived}` 反向触达小锁的链路
2. **黑名单方法的 javadoc 强制标注** `// MUST NOT be called while holding dispatcherLock`，让 reviewer 在调用点立刻看到约束。
3. **非阻塞 callee 约束**：dispatcher 持大锁时调用的所有外部 API 必须是非阻塞的——既不能反过来取小锁，也不能同步等大锁持有者的输出。`channelStateWriter.addInputDataFromSpill` 满足这一约束（它是 async writer，提交到独立 executor），新增的任何 callee 都要在 review 时按这条 checklist 过。
4. **新增 callee 的 review checklist**：每次往 `synchronized(dispatcherLock)` 段里新加一行调用，必须在 PR 描述里答出"这个 callee 会不会触达任何 store 锁?"。

之所以靠 code review + javadoc 而不是 runtime assert，是因为这条规则是**调用链层面**的——不能仅看 `synchronized(dispatcherLock)` 的入口、必须沿调用链看到底。

### dispatcher 各方法落位

| 方法 | 取大锁? | 内部是否调取小锁? | 备注 |
|---|---|---|---|
| `onChannelCheckpointStarted` | 是 | 否 | `synchronized(dispatcherLock)` 段开头第一句必须是 `if (closed) return;`，让 lifecycle 边界对读代码的人立刻可见。snapshot 是文件 IO，不取小锁；`channelStateWriter.addInputDataFromSpill` 是 async writer，不取小锁。 |
| `onChannelCheckpointStopped` | 是 | 否 | 同上，开头必须 `if (closed) return;`。 |
| `onChannelReleased` | 是 | 否 | 同上，开头必须 `if (closed) return;`。`reader.removeEntriesForChannel` 改 deque（CLD），不取小锁。 |
| `close()` | 是（仅 phase 2） | 否（phase 2） / 是（phase 1，但仅 abort 路径触发） | 见下方"close() 的两段式划分"，把可能取小锁的 `flushCache()` 放在 `synchronized(dispatcherLock)` **外**。 |
| `write` / `flush` / `flushCache` / `writeToSpillFile` | 否 | 取小锁（OK，单独取） | recovery 单线程，自身互斥靠 lifecycle；跨线程发布靠 `volatile spillFile`。**不再 synchronized**——一旦取大锁就违反规则 3（这些方法内会通过 `flushCache → store.addBuffer` 取小锁）。 |
| `drainPendingSpill` | 否 | 取小锁（OK） | 同上，单独取小锁，不与大锁交互。 |
| `eagerDrain` | 否 | 取小锁 | 同上。 |
| `getCurrentDrainHead` | 否 | 否 | 读 `volatile drainHead`。 |

### dispatcher 字段保护方式

| 字段 | 保护 | 说明 |
|---|---|---|
| `cache` / `cachePosition` / `cacheChannel` | recovery 线程私有 | 只有 recovery 线程读写（`write`/`flushCache`），任务线程不访问。 |
| `spillFile` | `volatile` 引用 + 大锁内访问其内部状态 | recovery 线程在 `writeToSpillFile` 中懒初始化（无锁，用 volatile 发布）；task 线程在 `onChannelCheckpointStarted` 等大锁方法内读 `getReaders()`。 |
| `flushed` / `closed` | `volatile` | recovery 单写者；task 通过 volatile 读。 |
| `currentCheckpointId` / `lastStoppedCheckpointId` | 大锁 | 普通 long，只在大锁内读写。 |
| `waitSet` | 大锁 | 普通 `HashSet<InputChannelInfo>`。 |
| `checkpointStartPos` | 大锁 | 普通 `HashMap<InputChannelInfo, EntryPosition>`。 |
| `checkpointSnapshots` | 大锁 | 普通 `List<Reader>`。 |
| `drainHead` | `volatile` | 已经 lock-free，跨 channel 可见性靠 volatile。 |
| `Reader.entries` | 见下 | drain（recovery）vs `removeEntriesForChannel`（task 在大锁内）的并发由 `ConcurrentLinkedDeque` 保证。 |

**`Reader.entries` 用 `ConcurrentLinkedDeque` 是必须项，不是装饰**。两条线在同一个 reader 的 entries 上真实并发：

- recovery 线程在 `drainPendingSpill` 中持 `小锁_C`（per-channel）但 **不持大锁**，调 `reader.skipNextEntry()`
- task 线程在 `onChannelReleased` 中持大锁但 **不持 `小锁_C`**，调 `reader.removeEntriesForChannel()`

两个调用都在改 entries deque，但取的不是同一把锁。如果有人将来"既然有 BIG 了，把 CLD 改回 `ArrayDeque` 吧"，会立刻把 race 重新引回来——deque 的 mutation 不再有任何串行保证，会撞上 `ConcurrentModificationException` 或更糟的静默 corruption。CLD 在这里承担的是"跨锁域的 deque 安全"职责，不能降级。

### close() 的两段式划分

`close()` 是这次新增 fix 的核心——上一版无锁实现里 `dispatcher.close()` 删掉 spill 文件后，并发的 `onChannelCheckpointStarted` 还会去 `FileChannel.open(filePath)` 那个已被删的文件，抛 `NoSuchFileException`。lock ordering 方案靠"snapshot 创建在大锁内、文件删除也在大锁内"封住这条 race。

```java
public void close() throws IOException {
    if (closed) return;

    // Phase 1: flushCache 可能通过 store.addBuffer 取小锁；必须在大锁外。
    // 仅在 abort 路径（caller 没先调 flush()）会真正进入这一段；happy path 下
    // flushed 已经为 true，phase 1 是空操作。
    if (!flushed) {
        flushCache();
        if (spillFile != null) {
            spillFile.finish();
        }
        flushed = true;
    }

    // Phase 2: lifecycle cleanup，持大锁，不取小锁。
    // 这一段必须和 onChannelCheckpointStarted 的 snapshot 创建互斥——大锁就是这条互斥
    // 边。closed 设 true 之后，并发的 onChannelCheckpointStarted 看到 closed 立刻 return，
    // 不会再去 FileChannel.open 已被删除的 spill 文件。
    synchronized (dispatcherLock) {
        closed = true;
        if (spillFile != null) {
            spillFile.close();   // 关 FileChannel + 删文件
            spillFile = null;
        }
        if (currentWindow != null) {
            closeSnapshots(currentWindow.snapshots);
            currentWindow = null;
        }
    }

    bufferRequester.releaseExclusiveBuffers();  // 不取任何锁
}
```

**Phase 1 取小锁的安全性 caveat**（必须明确写明，否则后人会怀疑这是不是又开了一个 race window）：

- happy path：`SequentialChannelStateReaderImpl.readInputData` 在 try-with-resources 退出前显式调 `d.flush()`、`d.drainPendingSpill()`，等到 `d.close()` 自动触发时 `flushed` 已经为 `true`，phase 1 整段被 skip，根本不触达小锁。
- abort path：try-with-resources 因异常提前进入 close()，此时 `finishRecovery()` 还没跑过，physical channel 没生成，task 线程不存在并发的 `onChannelCheckpointStarted`，也就不会有"同时持小锁和等大锁"的对手。phase 1 取小锁是安全的。
- 因此 phase 1 单独取小锁不会与任何 task 路径竞争——它要么不发生（happy path），要么在没有 task 路径并发的窗口里发生（abort path）。

### `RemoteInputChannel.checkpointStarted` 的回退

```java
public void checkpointStarted(CheckpointBarrier barrier) throws CheckpointException {
    synchronized (recoveredStore) {
        if (barrier.getId() < lastBarrierId) { /* throw CHECKPOINT_SUBSUMED */ }
        else if (barrier.getId() > lastBarrierId) { resetLastBarrier(); }
        // startPersisting 重新进锁，checkpointStatus 切换与 maybePersist 读取互斥恢复。
        // 内部会调用 store.checkpoint → coordinator.onChannelCheckpointStarted；后者按
        // lock ordering 取大锁（小→大顺序合法）。从 dispatcher 进入大锁段后，禁止再触达任何
        // 会回头取小锁的调用链——具体的 forbidden callees 见上方"Rule 3 的强制要求"小节
        // (store.addBuffer / store.incrementPending / 任何 synchronized(store) {…})。
        channelStatePersister.startPersisting(
                barrier.getId(), getInflightBuffersUnsafe(barrier.getId()));
    }
}
```

`checkpointStopped` 同理（对称问题，方向相反，会引发"多 spill"而非丢数据，但应一并修）。

### `ChannelStatePersister` 的责任

**`store` 进构造器是 lock ordering 方案的前置条件。** 在 master 上 `ChannelStatePersister` 不持有 store 引用，store 是每次方法调用作为参数传进来的——这种形态下没法稳定地写出 `assert Thread.holdsLock(store)`，因为 persister 自己不知道"该断言哪个 store"。

把 store 移进构造器后：
- 所有读写 persister 状态的方法（`startPersisting` / `stopPersisting` / `maybePersist` / `checkForBarrier` / `hasBarrierReceived`）加 `assert Thread.holdsLock(store)`
- javadoc 用 `@GuardedBy("store")` 标记
- 外层 `RemoteInputChannel` / `LocalInputChannel` 用 master 风格的整段 `synchronized(recoveredStore)` 包住调用方代码

这把"调用方必须持小锁"的契约从约定俗成升级为可被 `-ea` 拦下的运行时不变量。少了这条断言，"小→大"那条合法路径就只能靠人眼检查 caller 持锁，而 lock ordering 方案的安全性恰恰依赖这一步——所以构造器改造是 lock ordering 落地的硬前提，不是顺手做的整理。

## 与 iter_3 的关系

iter_3 已经把 channel 内 `receivedBuffers` 锁合并进小锁。本 iter 在此基础上：保留两把锁，但通过严格的"小→大"lock ordering 让 dispatcher 的大锁可以与小锁安全共存。两步合起来：channel 内 1 把锁，dispatcher 1 把显式锁，整个数据通路上**有锁但无环**。

## 验证

- 跑 `UnalignedCheckpointRescaleITCase` 50 次循环，期望失败次数从 ~1/50 降到 0
- 同时打开 filter（`UNALIGNED_RECOVER_OUTPUT_ON_DOWNSTREAM=true` + `CHECKPOINTING_DURING_RECOVERY_ENABLED=true`）跑一遍，确认没有 deadlock 类型 hang
- 单独覆盖 dispatcher 的并发：多 channel 同时调 `onChannelCheckpointStarted` 时 `waitSet`/`startPos` 的最终一致性、最后一个减到 0 的 channel 触发 drain 唯一一次
- 覆盖 `close()` 与 `onChannelCheckpointStarted` 的边界：close 标记 closed=true 后，并发的 onChannelCheckpointStarted 必须直接放弃，不会去打开已被删除的 spill 文件

## 结论

把 `startPersisting` 包回小锁内是必要的，但前提是 dispatcher 那一侧必须解决 AB-BA。两件事必须一起做：

- 只挪进锁不动 dispatcher：触发原 AB-BA 死锁
- 只让 dispatcher 改造但 startPersisting 仍在锁外：丢数据窗口仍在

iter_6 选择 lock ordering（保留两把锁、用显式 `dispatcherLock` 替代 `synchronized` 方法、严格规定"小→大、持大不取小"），相比"全无锁改造"代码更直白：dispatcher 内字段回归普通 HashMap/Set/long，不再依赖 `AtomicReference<CheckpointWindow>` + CAS retry + 双向所有权移交这类难懂结构。锁存在但有序，因此安全。
