# 锁分析 — 为什么当前实现要 dispatcher lock + store monitor + gate lock，以及如何回到 community_master 形态

## 0. 这份文档讲什么

回答两个问题：

1. **当前分支为什么要在 community_master 之上引入 `dispatcherLock` + `RecoveredBufferStoreImpl` monitor + `gateLock` 协作？** 这些锁不是凭空加的，配合 `drainHead` volatile / `checkpointStartPos` 各自有具体的死锁或丢数据场景要挡，注释里能找到出处。
2. **如果只要交付 community_master 等价能力（不要 checkpoint-during-RecoveredInputChannel-stage），是否能把这些锁全部退掉？** 能，前提是把「producer 在 channel conversion 之后继续 produce」这件事退回到 community_master 时序。

阅读对象是 reviewer 和后续维护者；目的是把目前散落在几篇 incident / race doc 里的「为什么要这把锁」收口到一处。

## 1. community_master 的锁形态

只有三层与本话题相关的同步：

| 名称 | 类型 | 在 community_master 的位置 | 保护对象 |
|------|------|---------------------------|----------|
| `synchronized (receivedBuffers)` | intrinsic monitor on 一个 `ArrayDeque` 或 `PrioritizedDeque` | `RecoveredInputChannel` / `RemoteInputChannel` | channel 自己的 buffer 队列、`isReleased` 等 |
| `LocalInputChannel.requestLock` | `Object` monitor | `LocalInputChannel` | partition request 状态机 |
| `SingleInputGate.requestLock` | `Object` monitor | `SingleInputGate` | `channels[]` 数组、partition 注册 |
| `SingleInputGate.inputChannelsWithData` | intrinsic monitor on PrioritizedDeque | `SingleInputGate` | 「哪些 channel 有数据可消费」的中央队列，task 线程从这里 pop，producer 通过 `queueChannel` 把 channel 入队 |

community_master 里 producer 唤醒 task 的链路：

```
producer (channel-state-unspilling 线程 / network 线程)
  └── 持 synchronized(receivedBuffers)
        └── 入队 buffer
        └── 算出 wasEmpty
  └── 释放 receivedBuffers
  └── if wasEmpty → notifyChannelNonEmpty
        └── SingleInputGate.queueChannel
              └── 持 synchronized(inputChannelsWithData)
                    └── 把 channel 加进中央队列
                    └── notification.notifyDataAvailable / notifyPriority
```

注意两层锁**不嵌套**：channel 锁先释放，再去拿 gate 锁。这避免了 channel↔gate 之间的 AB-BA。

唯一的 race 风险：在「释放 channel 锁」与「拿 gate 锁」之间，channel 可能被 conversion 换掉（`RecoveredInputChannel.toInputChannel`）。`SingleInputGate.queueChannel` 里通过遍历 `inputChannelsWithData`、把旧 channel 引用替换为新 channel 引用来兜底：

```
// SingleInputGate#requestPartitions 等位置
synchronized (inputChannelsWithData) {
    if (inputChannelsWithData.contains(oldInputChannel)) {
        inputChannelsWithData.getAndRemove(ch -> ch == oldInputChannel);
        inputChannelsWithData.add(realInputChannel);
    }
}
```

也就是说 community_master 接受「短暂把旧 channel 引用入队」，由 gate 在 conversion 时统一把它换成新引用。

## 2. community_master 为什么不需要更多锁

**关键假设**：producer 在 `toInputChannel()` 触发之前就停止 produce。

具体地：
- `channel-state-unspilling-*` 线程把 S3/state 上的 recovered buffer 都灌进 `RecoveredInputChannel.receivedBuffers`。
- 灌完之后才完成 `stateConsumedFuture`。
- task 线程在 mailbox loop 里看到 `stateConsumedFuture` done → `SingleInputGate.requestPartitions` → 在 `requestLock` 内调 `RecoveredInputChannel.toInputChannel` → `synchronized(receivedBuffers)` 抽走整个队列、构造 `LocalInputChannel` 或 `RemoteInputChannel` → `channels[]` 替换。
- 此刻 producer 已不再 produce；唯一的并发是 task 线程在消费旧 channel 与 conversion 替换旧 channel 之间的窗口，靠 gate 里的引用替换兜底（1 节）。

**所以 community_master 不需要 dispatcher 锁、不需要 store 锁、不需要 drainHead / startPos 这些跨锁可见性字段。**

## 3. 当前分支为什么打破了这个假设

本 branch 的核心新行为是：**`OutputWriter.drainPendingSpill()` 在 `finishRecovery()` 之后才跑**，也就是 conversion **之后**继续把 disk 上的 entry 加载成 buffer 并 produce 给 channel。

这一改动有它本意的合理性（REQ-G4KW）：

- `RecoveredInputChannel.checkpointStarted` 在 community_master 是 `throw CHECKPOINT_DECLINED_TASK_NOT_READY`。
- 本 branch 想支持 unaligned checkpoint-during-recovery（master 历来是用 unbounded heap buffer 撑住的，这条路要换成 disk 之后必须保留 checkpoint 能力）。
- `RecoveredInputChannel` 没有 unaligned checkpoint protocol，所以要尽早 conversion 到 `LocalInputChannel` / `RemoteInputChannel`，让物理 channel 接管 checkpoint。
- conversion 提前到 `bufferFilteringCompleteFuture`（filter 结束就 fire），而非 `stateConsumedFuture`（drain 结束才 fire）。
- 后果：conversion 完成 → 物理 channel 已经在响应 task / checkpoint → 与此同时 drain 线程还在 produce buffer 进 channel 队列。**producer 与 conversion 并发了**。

这就引出了 FLINK-39519 一系列锁问题。

## 4. 三把额外锁分别挡的是什么

### 4.1 store 自身 monitor（`synchronized(this)` on `RecoveredBufferStoreImpl`）

挡的是：consumer（task 线程 `tryTake` / `peekNextDataType`）与 producer（loader `addBuffer` / `incrementPending` / `decrementPending`）对 `readyBuffers` + `pendingCount` 的并发访问。

为什么 community_master 不需要：community_master 直接把 buffer 加进 `RecoveredInputChannel.receivedBuffers`，用 `synchronized(receivedBuffers)` 就够了。本分支抽象出 store 之后，store 变成新的并发热点对象，必须有自己的 monitor。

### 4.2 gate lock（store 借用 `inputGate.getGateLock()` = `inputChannelsWithData`）

挡的是 **FLINK-39519 stale-enqueue race**。具体场景：

```
T1 producer (drain 线程):
    synchronized (store_old) {
        store_old.readyBuffers.add(buf)
        listener = store_old.dataAvailableListener   // 指向 RecoveredInputChannel.notifyChannelNonEmpty
    }
T2 task 线程:
    synchronized (inputChannelsWithData) {
        channels[i] = realInputChannel               // conversion 完成
        // 这里如果 store 的 listener 没被更新，listener 还指向 RecoveredInputChannel
    }
T3 producer 继续:
    listener.onDataAvailable()                       // 唤醒 RecoveredInputChannel —— 但它已经被 detach
```

后果：buffer 加进了 store（实际上 store 引用已转给新 channel），但唤醒打到了已死的 RecoveredInputChannel；新 channel 不知道 store 多了一条 → buffer 卡在 store 里没人来消费。

修复（`RecoveredBufferStoreImpl` 文档里写的）：

> Producer mutators (addBuffer, addBufferAfterDisk): caller holds the gate lock; the store self-manages its own monitor and fires the data-available listener inline. Firing inside the store monitor is safe because the gate lock is held by the caller, so queueChannel re-acquires it as a recursive intrinsic-monitor entry — no AB-BA cycle.

也就是：producer 调 `store.addBuffer` 前要先持 gate 锁。conversion 替换 `dataAvailableListener` 也在 gate 锁内。这样「add buffer + fire listener」与「swap listener + swap channels[i]」被同一把 gate 锁串行化，stale-enqueue 不再发生。

代价：

- 锁序固定为 `gate → store`。任何反向（先持 store 再去拿 gate）的路径都会形成 AB-BA。
- store 的方法签名上有大量 `assert Thread.holdsLock(gateLock)`，要求 caller 提前持锁，调用约定复杂。
- ChannelStatePersister 等老路径需要重新审视会不会无意中走出反向锁序（FLINK-39519 incident 就是 dispatcher monitor 反向锁死锁的代表）。

### 4.3 dispatcher 锁（`FilteredBufferDispatcherImpl.dispatcherLock`）

挡的是 **多个 store 之上的 checkpoint 协调状态**：`waitSet`、`checkpointStartPos`、`checkpointSnapshots`、`currentCheckpointId`、`lastStoppedCheckpointId`。

为什么必须有这把锁：

- checkpoint 触发是 per-channel 的（每个 channel 自己被 task barrier 推动 `checkpointStarted`）。
- 但 phase 2（disk 数据 snapshot）要等所有 channel 都完成 step 1 之后才能开始（保证「step 1 ready buffers 都已落地」 → phase 2 snapshot 才不会漏 entry）。
- 「等所有 channel 都触发」需要一个 task-级别的 waitSet 协调，store 是 per-channel 的，承担不了。
- waitSet / startPos / snapshots 共享同一组协调状态，必须用一把锁串行化（dispatcher 锁）。

进一步：`drainHead` volatile 是为了让每个 channel 在自己的 step 1（持有 store 锁）内能读到「当前 drain 进度」，而不必去拿 dispatcher 锁（拿了就违反 gate → store 锁序）。这是为了配合上面的锁序约束而专门引入的跨锁可见性机制。

### 4.4 文档复杂度的根源

`phase2_drain_race.md` 6 节 + `close_drain_separation.md` C1/C2 契约 + `incident_FLINK_39519_deadlock.md` 5 节内容，本质上都在论证：**在「dispatcher 锁 + store 锁 + gate 锁 + drainHead」这套机制下，特定的 race 已经被关闭、特定的死锁路径不会再发生。**

每次出现新场景（如 ChannelStatePersister 反向调 store），都要重新论证。所以文档越叠越厚。

## 5. 简化原则：在 drain-after-conversion 前提下，把 phase 2 协调收口到 store 内

### 5.1 不能动的硬约束

`design_tradeoffs.md` 的 Tradeoff 2 已经锁死：**drain 必须跑在 conversion 之后**，否则 checkpoint 触发会被推迟到 drain 完成，这是本 branch 不可接受的回归。因此：

- producer 与 conversion 必然并发 → FLINK-39519 stale-enqueue race 是真问题，**store monitor + store 借用 gate lock** 这一层不能退。
- 物理 channel 必须感知 disk 数据 → **Store 抽象不能删**。

可以动的是 **dispatcher 锁 + waitSet + checkpointStartPos + checkpointSnapshots + drainHead + deferredBuffers + pendingCount-and-deferred 二步逻辑**——这一整套是为了挡 phase 2 race 而引入的，**可以用一个结构性改动一起退掉**。

### 5.2 phase 2 race 的根因再述

`phase2_drain_race.md` 里说的窗口本质：

> drain 处理一条 entry e（channel X）的过程分两段：(1) 从 dispatcher 的 FIFO 弹出 e；(2) 把 buffer add 到 store_X.readyBuffers。两段之间不原子。在这个窗口里 checkpoint 触发：dispatcher.waitSet 已经看不到 e（被 pop 了），store_X.readyBuffers 也还没装 e，于是 step 1 ready 快照 + phase 2 disk 快照都会漏掉 e。

根因就一句话：**FIFO（dispatcher 拥有）和 ready 队列（store 拥有）是两份独立数据，loader 在两者之间搬运不原子。**

### 5.3 关键结构性改动：DiskRef 进 store

把 `dispatcher 的 FIFO entry` 和 `store 的 pendingCount` 合并为一个东西：**store 自己持有一个 `Deque<DiskRef> pendingDiskRefs`**。

|  | 当前实现 | 简化后 |
|---|---|---|
| disk entry 的存放 | dispatcher 的 task 级 FIFO `Queue<SpillEntry>` | 每个 store 自己持 `Deque<DiskRef>` |
| disk entry 的元数据 | dispatcher 的 SpillEntry（channelInfo + offset + len） | store 的 DiskRef（offset + len，channelInfo 隐含 = store 自己绑的 channel） |
| 「disk 还有几条」的查询 | dispatcher.waitSet + store.pendingCount 两份维护 | `store.pendingDiskRefs.size()` 单一来源 |
| loader 处理一条 entry | dispatcher.FIFO.pop → store.addBuffer（两段不原子） | 在 `synchronized(store)` 内 pop pendingDiskRefs.head + add 到 readyBuffers（原子） |
| spill 文件读盘顺序 | dispatcher.FIFO 按写入顺序 | OutputWriter 按 spill 文件物理顺序 sequential read，每条 entry 路由到对应 store，store 内 head 必然匹配（断言） |

这一个改动让 phase 2 race 在结构上消失：

- step 1（snapshot ready）和 phase 2（snapshot pending disk）在同一个 store monitor 内**对同一份数据的两个队列同时拍照**——这两个队列都被 store monitor 守护，原子可见。
- loader 处理 entry 的 atomic step：在 store monitor 内一次性完成「pendingDiskRefs.pollFirst() + readyBuffers.add(buffer)」，无中间状态。
- 不需要 task-级 waitSet（每个 channel 的 checkpoint 独立完成，没有 cross-channel 等待）。
- 不需要 drainHead / checkpointStartPos / checkpointSnapshots（这些都是协调 dispatcher.FIFO 和 store 不一致用的，简化后 FIFO 不存在）。

### 5.4 OutputWriter 退化后的形态

OutputWriter / FilteredBufferDispatcher 退化为 **per-task 单线程对象**：

- filter 阶段：cache 满 / channel 切换时决定 P1 还是 P2。P1 → 直接调 `store.addBuffer`；P2 → `spillFile.append(bytes)` 拿到 offset → 在 store monitor 内 append DiskRef 到 `store.pendingDiskRefs`。
- drain 阶段：单线程 sequential read spill 文件，每读出一条 entry 路由到对应 store，在 store monitor 内 `pendingDiskRefs.pollFirst() + readyBuffers.add(buffer)` 原子完成。
- 不再有 dispatcher 锁。
- 不再有 task 级 FIFO 字段（spill 文件物理顺序即 FIFO，loader 不需要内存里再维护一份）。
- close 阶段：删 spill 文件、关 file channel、释放 readers。短锁、非阻塞、幂等。

### 5.5 简化后锁拓扑

| 锁 | 状态 | 用途 |
|----|------|------|
| `synchronized (RecoveredInputChannel.receivedBuffers)` | 保留 | community_master 既有，post-conversion 之前的 producer / consumer |
| `RemoteInputChannel` 自身 `synchronized(receivedBuffers)` | 保留 | community_master 既有，post-conversion 的物理 channel 队列 |
| `LocalInputChannel.requestLock` | 保留 | community_master 既有，partition request |
| `SingleInputGate.requestLock` | 保留 | community_master 既有，channels[] swap |
| `SingleInputGate.inputChannelsWithData` monitor | 保留 | community_master 既有，task wake-up 入队 |
| `BufferManager` 既有锁 | 保留 | community_master 既有 |
| `RecoveredBufferStoreImpl` 自身 monitor | 保留 | 守护 readyBuffers + pendingDiskRefs（取代当前的 readyBuffers + pendingCount + deferredBuffers）+ checkpoint snapshot |
| `RecoveredBufferStoreImpl` 借用的 gate lock（= `inputChannelsWithData`） | 保留 | 挡 FLINK-39519 stale-enqueue race。锁序仍为 `gate → store` |
| `FilteredBufferDispatcherImpl.dispatcherLock` | **删除** | 无内容可守护——waitSet / startPos / snapshots 全部退掉 |
| `drainHead` volatile | **删除** | 无需求——store 内 pendingDiskRefs 自己就反映 drain 进度 |
| `checkpointStartPos` map | **删除** | 不需要按 startPos 过滤 phase 2 entry |
| `checkpointSnapshots` list | **删除** | 不需要在 dispatcher 这一层 pin Reader |
| `waitSet` | **删除** | 每个 store 独立 snapshot，不需要 cross-channel 等待 |
| `currentCheckpointId` / `lastStoppedCheckpointId` | **删除** | 没有 task 级 checkpoint 状态机要追踪 |
| `deferredBuffers` + `pendingCount` 二步逻辑 | **删除** | DiskRef 在 pendingDiskRefs 队列里有真实位置，EOICS 直接按队列顺序 add 即可 |

**额外锁的数量：当前 9 项 → 简化后 2 项**（store monitor + store 借用 gate lock）。这两项是 Tradeoff 2 = A 决定下「producer-conversion 并发安全」所需的最小集合。

## 6. 简化后的几个关键 race 重新审视

### 6.1 FLINK-39519 stale-enqueue race

仍然存在（Tradeoff 2 = A 决定的），但被 store 借用 gate lock 这一层挡住。简化方向不动这一层，沿用当前 branch 的契约：「producer 持 gate lock → 调 store.addBuffer → store 在内部 monitor 内 fire listener」。conversion 替换 listener 也在 gate lock 内。

### 6.2 phase 2 漏 entry race

**不存在**。简化后 step 1 与 phase 2 都在 store monitor 内访问同一个 store 的 readyBuffers + pendingDiskRefs，没有 drain 在中间「FIFO pop 完但 store add 没完」的窗口。

### 6.3 close 与 drain 的死锁（incident_FLINK_39519_deadlock.md）

仍然要靠 `close_drain_separation.md` 的 C1/C2 契约挡：`close()` 只做资源释放、不持任何阻塞锁、不调阻塞 API。drain 由 `drainPendingSpill()` 独立编排。这一层契约保留。

但简化后 `close()` 不再有 dispatcher monitor 这把锁，C1/C2 文档可以删掉一大段（关于 dispatcher monitor 反向锁）的论证。

### 6.4 EndOfInputChannelStateEvent 顺序

不需要 `deferredBuffers`。EOICS 走 producer 路径 add 到 store.readyBuffers 时，如果 pendingDiskRefs 非空，可以选择两条路：

- **简单路**：EOICS 也作为 DiskRef 走 P2 路径（写盘 + append DiskRef），自然排在所有早期 DiskRef 之后。
- **更简单路**：EOICS 直接 add 到 readyBuffers，但 store 暴露的 `isEmpty()` / `peekNextDataType()` 在 pendingDiskRefs 非空时把 readyBuffers 队尾的 EOICS 视为「未就绪」，直到 pendingDiskRefs 排空。

两条都行，没有 race。优先选第二条，少一次写盘。

### 6.5 producer 阻塞拿 buffer 与 task 释放 buffer 的活锁

不存在。drain 阶段 task 在消费 store.readyBuffers，BufferPool 通过 task 消费回流给 drain 的 `requestBufferBlocking`。

## 7. 简化收益与代价

### 7.1 收益

- 锁字段从 9 项额外退到 2 项（store monitor + 借用 gate lock）。
- 删除 `dispatcherLock` / `waitSet` / `checkpointStartPos` / `checkpointSnapshots` / `drainHead` / `currentCheckpointId` / `lastStoppedCheckpointId` / `deferredBuffers` / `pendingCount` 这些字段。
- 删除 `EntryPosition` 类型（只 drainHead 用）。
- 删除 dispatcher 与 store 之间的 `RecoveredBufferStoreCoordinator` 接口（dispatcher 不再需要回调 store 协调状态）。
- 删除 `phase2_drain_race.md` 整篇。
- 大幅简化 `close_drain_separation.md`（仍保留 close ↔ drain 拆分契约，删除 dispatcher monitor 相关论证）。
- `spill_reader_drain_concurrency.md` 简化为单纯说明「checkpoint snapshot 走独立 positional read」。

### 7.2 代价（没有，本质上）

不需要放弃任何当前 branch 的能力：

- 「filter 完成立刻可 checkpoint」保留。
- 「filter ↔ task 消费重叠」保留。
- 「drain 与 task 消费 / checkpoint 并发」保留。
- FLINK-39519 race 保护保留（沿用 store 借用 gate lock）。

代价只是一次结构性重构（dispatcher.FIFO + store.pendingCount → store.pendingDiskRefs），不增加任何运行期开销。

## 8. 收口

| 问题 | 答案 |
|------|------|
| 为什么当前实现要 dispatcher lock | 为了协调 phase 2 跨 channel 的 checkpoint 状态（waitSet / startPos / snapshots） |
| 为什么当前实现要 store monitor | 为了把 producer / consumer 在 store 内部的访问串行化 |
| 为什么当前实现要 gate 锁借用 | 为了挡 FLINK-39519 stale-enqueue race（conversion 期间替换 listener 必须与 producer fire 串行） |
| 为什么 community_master 不需要这些 | community_master 的 producer 在 conversion 之前停止，「producer-conversion 并发」根本场景不存在；checkpoint during recovery 也直接拒绝 |
| 本 branch 能不能退到 community_master 时序 | 不能。「filter 完成立刻可 checkpoint」是本 branch 不可妥协的目标，详见 `design_tradeoffs.md` Tradeoff 2 |
| 简化方案 | 保留 store + gate 锁借用；把 dispatcher 的 FIFO 与 store 的 pendingCount 合并成 store.pendingDiskRefs 单一队列，phase 2 race 在结构上消失，dispatcher 锁与所有跨 channel 协调字段一起退掉 |
| 锁字段净变化 | 9 项额外字段 → 2 项额外字段（store monitor + 借用 gate lock） |
| 代价 | 一次性结构重构，无运行期回归，无能力损失 |
