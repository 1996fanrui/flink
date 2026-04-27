# Drain 路径残留的 buffer 丢失与并发问题（中间分析，结论已被推翻）

> **状态：调查中间产物，B 段结论已被推翻。**
>
> 本文写于 `phase2_drain_race.md` 修复（`af462720b8b`）落地后、`1b277597b55`
> 之前，目的是分析压测仍出现 EOF / 计数偏少的"疑似根因"。后续 iterative-
> deliver 流程的根因报告 [`investigation/eof_count_mismatch_root_cause.md`]
> 通过字节级日志证明：
>
> - **真正的根因是 R**：`SingleInputGate.convertRecoveredInputChannels` 在
>   `toInputChannel()` 之后立刻调 `releaseAllResources` → `store.releaseAll()`，
>   把新物理 channel 刚继承的 store 内存 buffer 全部 recycle、磁盘 entries
>   全部丢弃 —— 已由 commit `1b277597b55` 修复（移除该 release 调用）。
> - **§A 仍然是结构性问题**，但已由 commit `f23f1282466` 修复（`addBuffer`
>   两段式：listener 在 store 锁外触发）。
> - **§B3 不是独立根因**，只是 R 释放 store 时触发的副作用：R 走的
>   `onChannelReleased` 是 B3 的触发条件；R 修好后 B3 的窗口随之关闭。
> - **§B1 / §B2** 是 CME loud-failure 路径，本次复现窗口里没有命中，仍可
>   作为 phase-2 阶段的潜在加固方向，但与本轮 EOF / 计数偏少无关。
>
> 文档保留是为了记录调查思路（"先排除 loud failure，再聚焦 silent loss"
> 的方法论）以及后续 §B1 / §B2 加固的备忘。**不要把本文的修复优先级当作
> 设计依据**，请以根因报告为准。

`phase2_drain_race.md` 修了「drain pop 完后还没 addBuffer，Step 1 切入 + waitSet 提前收敛」这条 race。修复落地后压测仍然可见 EOFException 与计数偏少。本文按**「是否会丢数据」**重新排序：

- **B1 / B2 / B3** 是丢数据的疑似真因（plain `ArrayDeque` 跨锁并发）
- **A** 是结构性回归（偶发卡死，**不丢数据**），仅记录便于后续顺手清理

---

## A. 结构性回归：drain 持 store 锁时 addBuffer 可能触发 listener

`f23f12824665` 把 `RecoveredBufferStoreImpl#addBuffer` 改成两段式：synchronized 内只 capture listener 引用，跨出 synchronized 才触发 `onDataAvailable()`，避免与 `SingleInputGate.inputChannelsWithData` 形成 AB-BA 死锁。

但 `FilteredBufferDispatcherImpl#drainPendingSpill` 把 `addBuffer` 整个包在 `synchronized(store)` 里：

```java
synchronized (store) {
    reader.skipNextEntry();
    writeChunkToBuffer(buffer, data, entryLength);
    store.addBuffer(buffer);   // 内部 synchronized 已退出
                               // 但 onDataAvailable() 在我这层 synchronized(store) 内被触发
    store.decrementPending();
    drainHead = computeDrainHeadFrom(i);
}
```

→ 在持 store 锁状态下触发 listener，跟 task 线程「gate 锁 → store 锁」（`SingleInputGate#waitAndGetNextData` 持 `inputChannelsWithData` 同步块内调 `inputChannel.getNextBuffer()` → store.tryTake / peekNextDataType）结构上构成 AB-BA 环。

**实际触发概率**：listener 只在 `readyBuffers.isEmpty() → non-empty` 这次跳变才被 capture（见 `addBuffer` 的 `wasEmpty` 分支），drain 期间 store 大多数时候非空，命中条件极少。当前测试没观察到卡死与此一致，但理论上可能在 task 消费明显快于 drain（盘 cache miss 等）时偶发卡死。

**会丢数据吗？** **不会**。这条只会让 task 卡住，不会让 buffer 字节错位或 entry 静默丢失。EOF / 计数偏少的根因不在这里，看 B 段。

**修复方向**：在 outer `synchronized(store)` 内调一个新的 `addBufferAndCaptureListener(buffer)`（只 add + 返回 listener），listener 引用赋给本地变量；跨出 outer synchronized 后再 `listener.onDataAvailable()`。`decrementPending` 与 `drainHead` 推进保留在 outer 锁内不变（保 Step 1 的原子性）。

---

## B. 三条 plain `ArrayDeque` 并发路径（独立于 A）

`FilteredSpillFile.Reader.entries` 是 `ArrayDeque`，被以下三个动作分别在**三把不同锁**下访问：

| 动作 | 调用方 | 持有的锁 | 操作 |
|---|---|---|---|
| `reader.skipNextEntry()` / `peekNextEntry()` | drainPendingSpill（recovery 线程） | `synchronized(store_X)` | `entries.pollFirst()` / `entries.peekFirst()` |
| `reader.snapshot()` 内的 `entries.addAll(this.entries)` | `onChannelCheckpointStarted`（task 线程） | `synchronized(this/dispatcher)` | iterate `entries` |
| `reader.removeEntriesForChannel()` | `onChannelReleased`（task 线程） | `synchronized(this/dispatcher)` | iterate + `it.remove()` |

下面三条具体路径，但**只有 B3 会静默丢数据**（也就是 EOF / 计数偏少的疑似真因）；B1/B2 的失败模式是抛 CME（loud failure，会让 checkpoint 失败但不会出现"数据莫名少一条"）。

### B1. drain.pollFirst vs snapshot.addAll —— 触发 CME，**不是**静默丢数据

snapshot 拍摄时机被前移到第一个 channel 触发，期间 drain 仍在并发 pop。snapshot 走 dispatcher 锁、drain 走 store 锁，互斥不上。

但 ArrayDeque 的 iterator 在 `next()` 里通过 `nonNullElementAt(es, cursor)` 主动检查"槽位是否被并发清空"，被并发 `pollFirst` 清掉的话**直接抛 CME**。所以这条路径的失败模式是 loud：snapshot 构造失败 → checkpoint 失败 → 测试栈里能直接看到 CME，**而不是静默 EOF**。

> snapshot 一旦构造完成，是独立 deque + 独立 FileChannel，drain 后续怎么 pop 都不会再影响它的视图。所以"snapshot 从 offset 20 开始读" 的语义是稳的，疑虑只在"拍照那一瞬间的迭代"。但 ArrayDeque 已经把这个失败模式 loud 化，本条不构成 EOF 来源。

修复仍然值得做（任何 CME 都意味着 ckpt 不可用），但优先级低于 B3。

### B2. drain.pollFirst vs onChannelReleased.iterator.remove —— 同样触发 CME

rescale 期间 channel 释放与 drain 并发，二者撞同一个 deque。失败模式同 B1（CME，loud failure），同样不直接产生 EOF。

### B3. drain peek-then-skip 之间 head entry 被 `onChannelReleased` 偷走 —— **真静默丢**

drain bundle 是这样的：

```java
Entry entry = reader.peekNextEntry();   // (1) 看到 e
buffer = requestBufferBlocking(entry.getChannelInfo());
reader.readBytesAt(entry.getOffset(), entry.getLength(), data, 0);   // (2) 锁外读盘
synchronized (store) {
    reader.skipNextEntry();             // (3) pop deque 当前 head
    ...
    store.addBuffer(buffer);            // 把 (2) 读到的 e 的字节 addBuffer 到 e.channel
    store.decrementPending();           // 减 e.channel 的 pendingCount
}
```

(1) 与 (3) 之间，如果 `onChannelReleased(e.channel)` 介入，会把 e 连同同 channel 的其他 entries 全部从 deque 删掉。等到 (3) 时 `pollFirst` 弹出的是另一个 entry e'（属于另一个 channel）：

- (2) 读到的是 e 的字节，已经写入 buffer
- buffer 的字节走 e.channel 的 store（即将被 release / 已经 release，addBuffer 直接 recycle）
- pop 出的 e' 被静默丢弃，**永远不会 deliver 到 e'.channel**

→ **e' 真的丢了**。`decrementPending` 还会减错 channel 的计数：e.channel 的 pendingCount 多减一次（可能 < 0），e'.channel 的 pendingCount 没减（与磁盘真实状态不符，影响 `isEmpty()`）。

---

## 推测的 EOF 根因

- **A** 不丢数据，只可能偶发卡死。
- **B1 / B2** 的失败模式是 CME（loud failure），会让 ckpt 失败、堆栈直接抛出，**不会**静默 EOF。
- **B3** 让单条 entry 静默蒸发：被偷走 head 的 channel D 永远没拿到那条 entry，D 的 channel state 缺一条 record → 下游 deserializer 按字节扫描，遇到本来该是 record 头的位置已经是下一条 record 的中段，跨 buffer 拼接失败 → **EOF**；同时下游计数也少一条 → **计数偏少**。

EOF 与计数偏少同时出现，与 B3 单点根因一致（一次丢失同时产生这两个症状）。B1/B2 即使发生也是另一种症状（CME 抛栈），跟当前观察不重合。

---

## 修复优先级

1. **B3** —— EOF 与计数偏少的疑似真因，必须先修。最干净的写法：把 drain bundle 中的 `peekNextEntry()` + `skipNextEntry()` 合并放进同一段（同一把锁）执行，并保证 (peek 看到的 entry) == (skipNextEntry 弹出的 entry) —— 否则回滚（recycle buffer + 不做 addBuffer / decrementPending），跳到下一轮重新 peek。要点是 **peek 后再次确认 head 是同一对象**，避免 `onChannelReleased` 在两步之间偷走 head。
2. **B1 / B2** —— 即使是 CME 而非静默丢，仍会让 ckpt 在 rescale 期间失败。把 `Reader.entries` 的并发模型彻底拍平：

   - 最小代价方案：换成 `ConcurrentLinkedDeque`（弱一致迭代，不抛 CME；`pollFirst` 本身原子）。
   - 更稳的方案：所有 deque 写动作（drain pop / onChannelReleased / snapshot addAll）统一通过 `Reader` 实例锁串行；外层调用方从原本 dispatcher / store 锁解耦。

3. **A** —— 偶发卡死风险，独立修复，不阻塞 EOF 排查。drain bundle 内部用 capture-then-fire-outside 的两段式即可（不在 outer `synchronized(store)` 内调 listener）。

---

## 测试覆盖建议

- 高并发 rescale 触发 `onChannelReleased` 与 `drainPendingSpill` 同时跑的场景，断言每个 ready buffer 与 phase 2 entry 集合的并集 = 写入的 entry 集合，且 `pendingCount` 收敛到 0（不出现负数）。
- drain 持 store 锁时人为触发 `dataAvailableListener`（mock listener 内 `Thread.holdsLock(store)` assert），确保 listener 在锁外触发（覆盖 A）。
- snapshot.addAll 与 drain.pollFirst 并发：1000 次循环跑下来不出 CME、snapshot 内容 + drain 后 store 内容 = 原始 entry 全集（覆盖 B1）。
