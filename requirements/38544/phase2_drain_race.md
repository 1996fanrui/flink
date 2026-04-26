# 多 channel checkpoint 漏 entry race

## 问题

Phase 2（disk 快照）等所有 channel 都触发 `onChannelCheckpointStarted` 后才统一对 reader 拍 snapshot。`drainPendingSpill`（recovery 线程上一次性 drain，跟 dispatcher 锁不互斥）里 `reader.readNext()` 把 entry e 从 `reader.entries` pop 出来读盘后，才调 `store.addBuffer()`，pop 和 addBuffer 之间不原子；这个窗口里 Task 线程可以进 `onChannelCheckpointStarted`：建 waitSet 时 `reader.getPendingChannels()` 已经看不到 e（被 pop 了），如果 e 是 channel C 的最后一条 entry，C 会从 waitSet 缺席、waitSet 提前收敛、phase 2 拍 snapshot 时 reader 里也没有 e；与此同时 Step 1 of C 因为还没拿到 e（addBuffer 还没跑），ready 快照里也没有 e —— e **既错过 Step 1 ready，又错过 phase 2**，永久丢失。drain 后续完成 addBuffer 让 e 进 store_C.readyBuffers，Task 后续会消费但不会回到 ckpt N 的 channel state 里。

## 方案概览（6 句）

1. **第一个** channel 进 `onChannelCheckpointStarted` 时立刻给所有 sealed reader 拍 snapshot（独立 FileChannel + entries 浅拷贝），整个 ckpt 期间持有；之后 drain 在原 reader 上继续 pop 不影响 snapshot。
2. 每个 channel C 在自己的 `checkpointStarted` 里、在 `synchronized(store_C)` 内**原子地**完成「snapshot readyBuffers + 记录 `startPos_C = 当前 drainHead(fileIndex, offset)`」。
3. drain bundle 拆三段：磁盘 I/O 锁外，commit 段（`addBuffer + decrementPending + drainHead 推进`）放在 `synchronized(store_X)` 内，**`drainHead` 推进必须是 commit 段最后一行**（保证"drainHead 跨过 e ⇒ e 已在 store_X.readyBuffers"）。
4. wait-set 收敛后 phase 2 在 snapshot 上单次顺序遍历，对每个 entry e（channel X，位置 p_e）按 `p_e < startPos_X` 决定 skip（已被 X 的 Step 1 拍到）或 emit。
5. 每个 channel 的 in-flight 集合 = T_X 时它自己的 ready buffer + 当时还在盘上的 entry，跨 channel 不互相影响；`drainHead` 用 `volatile` 解决跨 channel 锁不可见性，store 锁解决同 channel 内 commit 与 Step 1 互斥，两者职责正交。
6. ckpt 终结（phase 2 提交完成 / 中止）时 reset `checkpointSnapshots` / `checkpointStartPos` / `waitSet` 并 close snapshot Reader 释放 FileChannel。

下面是详细设计与改动点。

## 修复

把现有 `reader.snapshot()` 的调用从「最后一个 channel 触发时」前移到「**第一个** channel 触发时」（`onChannelCheckpointStarted` 中初始化新 ckpt 的分支），生成的 snapshot Reader（独立 FileChannel + entries 浅拷贝 + 预 sealed）整个 ckpt 期间一直持有；每个 channel C 在自己的 `checkpointStarted` 里、在 store 锁内**原子地**完成「snapshot readyBuffers + 记录 `startPos_C = (fileIndex, offset)`」（取此刻 dispatcher 的 `drainHead`）；wait-set 收敛后 phase 2 在 snapshot Reader 上单次顺序遍历，对每个 entry `e`（channel X，位置 `p_e`），按字典序比较：`p_e < startPos_X` → 跳过（T_X 时已落定到 store_X.readyBuffers，被 Step 1 拍到，或已被 Task 在 T_X 之前消费），否则写入 X 的 channel state。等价于每个 channel 一个 filtering→passing 单向模式开关，跨文件、单文件统一逻辑，不需要 per-channel counter。

## 锁的定义

「store 锁」= 每个 input channel 对应的 `RecoveredBufferStoreImpl` 实例本身的 monitor（即 `synchronized(theStore)` / 该类里现有 `synchronized` 方法用的同一把锁）。不是新引入的全局锁，跟 dispatcher 锁无关。每个 channel 一把，互相独立。`drainHead` 是 dispatcher 维护的 `volatile (fileIndex, offset)` 字段，跨线程通过 volatile 语义可见。

## 关键不变式：drainHead 推进必须在 addBuffer 之后

drain 处理一条 entry e（channel X）时分三段，**只有第三段在 store 锁内**：

```
[1. 锁外] requestBufferBlocking + read disk → 字节落到 buffer
[2. 锁外] reader.entries.pollFirst()        // pop（单线程 Recovery，无并发）
[3. synchronized(store_X)]
     store_X.addBuffer(buffer)
     store_X.decrementPending()
     drainHead = nextEntryPosition()         // ← 必须是这一段的最后一行
```

**磁盘 I/O 严禁放在 store 锁内**（一次读可达数毫秒，会卡死所有 channel 的 Step 1）。**`drainHead` 更新严禁出现在 `addBuffer` 之前或锁外**（否则会出现"drainHead 已推过 e 但 e 还没进 store_X.readyBuffers"的窗口 → 同时落空 Step 1 与 phase 2 → e 永久丢）。

由这条排序导出的不变式：

> 任意时刻只要 `drainHead > p_e`（e 是 channel C 的 entry），那么 e 一定已经在 `store_C.readyBuffers` 里，或已被 Task 在 T_C 之前消费掉（落在 operator state）。

直观语义：drainHead 始终落后于"in-flight 那条 entry"一个位置。磁盘 I/O 阶段 drainHead 停在 e 的位置（看作 e 还在盘上）；当 e 被 addBuffer 落地、drainHead 才跨过 e。Step 1 任何时候读到的 drainHead 都是"已落地"的边界，不会出现"声称 drained 但 buffer 没到"。

## Step 1 of C 与 drain 的并发可视化

| C ≠ X 的 drain 在跑 | Step 1 of C 行为 | 后果 |
|---|---|---|
| drain 在锁外做磁盘 I/O | 不互斥，Step 1 直接进 store_C 锁，读 `drainHead`（旧值，没跨过 e） | 正确：drain 还没 commit，e 在 phase 2 里被包进去；C 不关心 X 的 entry |
| drain 在 store_X 锁内 commit | 不互斥（不同 store 的锁），`drainHead` volatile 读拿到旧或新都自洽 | 正确：旧值看作 e 还没 drain（e 进 phase 2 of X），新值看作 e 已 drain（e 在 store_X.readyBuffers，Step 1 of X 后续会拍） |

| C == X 的 drain 在跑 | Step 1 of C 行为 | 后果 |
|---|---|---|
| drain 在锁外做磁盘 I/O | Step 1 直接拿到 store_C 锁，读到 `drainHead` 旧值 | 正确：e 当前不在 readyBuffers、drainHead 在 e 之前 → e 进 phase 2 of C；之后 drain commit、e 也进 store_C 给 Task 消费（Task 后续消费的 effect 落到 ckpt N+1） |
| drain 在 store_C 锁内 commit | Step 1 阻塞等锁，drain 完成后释放 | 正确：Step 1 看到 e 在 readyBuffers + drainHead 已跨过 e → e 走 Step 1 通道，phase 2 跳过 |

---

## 改动点

主要落在 `FilteredBufferDispatcherImpl` 一个类，外加 `RecoveredBufferStoreImpl#checkpoint` 的 Step 1 配合。三块：**记录 offset、过滤、加锁**。

### A. 记录 offset

新增数据结构（`FilteredBufferDispatcherImpl` 字段）：

| 字段 | 类型 | 含义 |
|---|---|---|
| `drainHead` | `volatile EntryPosition` | 全局已 commit 的 drain 边界。`EntryPosition` 是新建的不可变 record `(fileIndex, byteOffset)`，实现 `Comparable<EntryPosition>`（字典序） |
| `checkpointSnapshots` | `List<FilteredSpillFile.Reader>` | 第一个 channel 触发时一次性拍下的 snapshot Reader 列表，整个 ckpt 期间持有 |
| `checkpointStartPos` | `Map<InputChannelInfo, EntryPosition>` | 每个 channel 在自己 trigger 时刻的 drainHead 快照值 |

三处写入时机：

- `drainHead` 由 drain commit 段最后一行写入（见 C 节）
- `checkpointSnapshots` 在 `onChannelCheckpointStarted` 检测到 `checkpointId > currentCheckpointId`（=第一个 trigger）时一次性拍摄
- `checkpointStartPos.put(channelInfo, startPos)` 在 `onChannelCheckpointStarted` 内填，`startPos` 来自 store Step 1 在 store 锁内读到的 drainHead 值（见 C 节）

### B. 过滤

`drainSpillEntriesToCheckpoint` 不再调用 `Reader.snapshot()`（A 节已经提前拍好），改为基于已拍的 `checkpointSnapshots` + `checkpointStartPos` 构造一个 `FilteringDrainChunkIterator`：

```
foreach reader in checkpointSnapshots:
  while reader.hasEntries():
    Entry e = reader.peekNextEntry()
    if e.position < checkpointStartPos.get(e.channelInfo):
      reader.skipNextEntry()             // pop 但不读盘
    else:
      yield reader.readNext()            // pop + 读盘 + emit
```

需要 `FilteredSpillFile.Reader` 新增两个 API（现有 `readNext()` 不变）：

- `Entry peekNextEntry()` —— 看 head entry 不消费
- `void skipNextEntry()` —— pop 不读盘（避免无谓 I/O）

边界：`checkpointStartPos` 里没有的 channel（在 trigger 之前已被 `onChannelReleased`）→ 该 channel 全部 skip。

### C. 加锁

> 仅 `drainPendingSpill` 需要按下面拆三段。`eagerDrain` 在 `write()` 路径、filter 阶段跑，那时还没 `finishRecovery() → channel conversion`，物理 channel 不存在、checkpoint 无从触发，跟 phase 2 物理上不并发，**不改**。

**drain bundle**（拆三段）：

```
[锁外]              buffer = requestBuffer*(ch)
[锁外]              reader.readEntryBytesAt(p_e, buffer)   ← 磁盘 I/O
[锁外]              reader.entries.pollFirst()             ← pop（单线程 Recovery）
[synchronized(store_X)]
                    store_X.addBuffer(buffer)
                    store_X.decrementPending()
                    drainHead = computeNextEntryPosition() ← 必须最后一行
```

`FilteredSpillFile.Reader` 新增 `readEntryBytesAt(position, buffer)` 把读盘从现有 `readNext()` 里解耦出来；`readNext()` 自身不动（phase 2 过滤路径还要用它）。

**Step 1**（`RecoveredBufferStoreImpl#checkpoint`）扩展为「snapshot ready + 读 drainHead」原子：

```java
EntryPosition startPos;
synchronized (this) {
    startPos = (coordinator != null) ? coordinator.getCurrentDrainHead() : null;
    if (!readyBuffers.isEmpty()) {
        ... addInputData ...
    }
    c = coordinator;
}
if (c != null) {
    c.onChannelCheckpointStarted(checkpointId, channelInfo, startPos);  // startPos 带出
}
```

**`RecoveredBufferStoreCoordinator` 接口新增 / 修改**：

- `EntryPosition getCurrentDrainHead()` —— Step 1 在 store 锁内读
- `onChannelCheckpointStarted` 签名加 `EntryPosition startPos` 参数 —— Step 1 把读到的值带出，dispatcher 实现里直接 `checkpointStartPos.put(channelInfo, startPos)`

### D. ckpt 生命周期 + 资源释放

`checkpointSnapshots` / `checkpointStartPos` 是 per-checkpoint 状态，必须在 ckpt 终结时显式 reset，否则：(1) ckpt N+1 来时若 `checkpointId > currentCheckpointId` 分支没把残留清掉，会读到 ckpt N 的过滤参数；(2) 每个 snapshot Reader 自带一个 `FileChannel.open`，不 close 就泄漏 fd。

清理点：

| 触发点 | 行为 |
|---|---|
| `submitPhase2`（wait-set 收敛、提交给 writer） | `checkpointSnapshots` 的 ownership 转给 `FilteringDrainChunkIterator`，由它的 `close()` 关闭所有 snapshot Reader；dispatcher 端字段置 `null` |
| `onChannelCheckpointStopped`（ckpt 中止 / 完成，phase 2 还没提交） | 主动 close `checkpointSnapshots` 里的所有 Reader 释放 FileChannel；`checkpointSnapshots = null`、`checkpointStartPos = null`、`waitSet = null` |
| `onChannelReleased` | snapshot 不需要同步删该 channel 的 entry —— phase 2 过滤逻辑里 `checkpointStartPos.get(channelInfo)` 缺失就 skip 该 channel 的所有 entry，自然达成（无需再调 `removeEntriesForChannel` 改 snapshot） |

边界：snapshot Reader 持有的 FileChannel 跟 `FilteredSpillFile.close()` 物理删除文件的时序 —— Linux 上文件即使被 unlink，已 open 的 fd 仍可读到底，无需特殊协调；但需要在文档里把"依赖 POSIX unlink-after-open 语义"这一点注释清楚。

### 文件级修改清单

| 文件 | 改动 |
|---|---|
| `FilteredBufferDispatcherImpl` | 新增 `drainHead` / `checkpointSnapshots` / `checkpointStartPos` 字段；重构 `drainPendingSpill`（拆三段，drainHead 末尾推进，**eagerDrain 不动**）；改 `onChannelCheckpointStarted`（第一次 trigger 立刻 snapshot Reader + 接收 startPos 入 map）；重写 `drainSpillEntriesToCheckpoint`（用 `FilteringDrainChunkIterator`）；扩展 `onChannelCheckpointStopped` 做 snapshot Reader close + 字段 reset |
| `RecoveredBufferStoreImpl` | `checkpoint()` Step 1 在 store 锁内读 drainHead，传给 coordinator |
| `RecoveredBufferStoreCoordinator` | 加 `getCurrentDrainHead()`；`onChannelCheckpointStarted` 加 `startPos` 参数 |
| `FilteredSpillFile.Reader` | 新增 `peekNextEntry()` / `skipNextEntry()` / `readEntryBytesAt(pos, buffer)`；现有 `readNext()` 不动 |
| `EntryPosition`（新文件） | 不可变 record `(int fileIndex, long offset)`，`Comparable<EntryPosition>` |
