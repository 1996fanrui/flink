# 设计取舍 — 跟锁无关的若干独立决策

> 锁拓扑的分析在 [`lock_analysis.md`](./lock_analysis.md)。本文件**完全不聊锁**，专门梳理我们讨论里聊到的几个独立设计取舍：每个取舍的两条路、各自代价、当前推荐选择、以及它们之间的依赖关系。

## 0. 文档结构

- 先说一个**强需求**——不是 tradeoff，是不可妥协的前提，所有后续选择都必须满足它。
- 然后列出 5 个真正的 tradeoff，每条独立分析。
- 最后是组合自洽性与待确认项。

---

## 强需求（不是 tradeoff）：filter 完成必须立刻能触发 checkpoint

**filter 阶段一旦完成——意思是 S3 / state backend 上的所有 recovered 数据都已经被过滤并落到内存 buffer 或 spill 文件——就必须立刻允许 checkpoint 触发。不能等 drain（把 spill 文件加载回内存）完成。**

理由：

1. **保持与 master 一致的 checkpoint latency**：master 用 unbounded heap fallback 时，filter 完成立刻可 checkpoint（heap buffer 就是普通 buffer）。本 branch 用 disk 替代 heap fallback，必须保留这个 latency 行为——否则就是相对 master 的回归。
2. **drain 时间不可控**：drain 速度受 BufferPool 周转节流，而 BufferPool 周转又依赖 task 消费速度（即用户业务逻辑速度）。等 drain 完才 checkpoint 可能把触发推迟很久，把「保证 checkpoint 触发足够及时」这个本 branch 的核心价值点抹掉。

这条强需求决定了几件事：

- channel conversion（RecoveredInputChannel → Local/Remote）必须在 filter 完成时立即触发，不能等 drain。
- 物理 channel（Local/Remote）必须能在 disk 数据还没排空时支持 checkpoint。
- 物理 channel 必须能感知 disk 数据，否则 checkpoint 漏掉 disk 上的内容 → 故障恢复时丢数据。

所有后续 tradeoff 的 B 方案都必须能满足上面三条；不能满足的方案根本不是合法选项，不列入对比。

---

## Tradeoff 1：filter 完成前是否允许 task 消费

### 两条路

- **A（filter 与 task 消费重叠）**：filter 线程一边过滤 / 写盘，task 线程一边消费 `RecoveredInputChannel` 中已经就绪的 buffer。`bufferFilteringCompleteFuture` 早触发，task 进入 RUNNING。
- **B（filter 完成才允许消费）**：filter 全部完成之前，task 完全看不到任何 recovered buffer。filter 完成 → conversion → task 开始消费。

### 代价比较

| 维度 | A（重叠）| B（不重叠，filter 完才消费）|
|------|---------|----------------------------|
| filter 阶段的并发模式 | producer + consumer 并发，需要协调 ready 队列 | 只有 producer，filter 阶段是单写者 |
| filter 完成 → checkpoint 触发的 latency | 与 B 一样快（filter 完成那一刻就允许） | 与 A 一样快（filter 完成立刻 conversion，conversion 之后立刻可 checkpoint）|
| filter 本身的耗时 | 与 B 一样（filter 只做反序列化 + 过滤 + 再序列化，不跑业务逻辑） | 同左 |
| recovery 总 wall-clock | 不显著比 B 快——recovery 慢的根因是 task 消费用户业务逻辑，filter 期间允许消费并不能加速业务消费 | 同左 |
| BufferPool 周转 | filter 期间 task 能消费回收 buffer，spill 量稍少 | filter 期间 task 不消费，BufferPool 满了就只能 spill，spill 量略多 |
| 与「producer-conversion 并发」的关系 | filter 阶段已经有 producer-consumer 并发；drain 又在 conversion 之后跑 → 多种并发场景叠加 | filter 阶段无并发；conversion 在 filter 完成时一次性发生；drain 在 conversion 之后串行启动 → 并发场景少一种 |

### 当前推荐

**取 B（filter 完才允许消费）**。理由：

1. **B 的核心动机是减少并发场景，从而少引入锁。** filter 阶段不允许消费 → filter 阶段单写者 → 无需在 filter 期间协调 producer-consumer。这一步就把当前 branch 的一类锁需求消掉了。
2. **filter 本身很快**，因为它不跑用户业务逻辑——只做反序列化 + 过滤判断 + 再序列化。recovery 整体慢的根因是后续 task 消费阶段（业务逻辑慢），而本 tradeoff 不影响业务消费的速度。所以选 B 不会让 recovery 总时间显著变长。
3. **B 不违反强需求**。filter 完成立刻 conversion，conversion 之后立刻可 checkpoint，与 A 一样及时。强需求关心的是「filter 完成 → checkpoint 触发」的 latency，B 在这条 latency 上与 A 等价。
4. **A 看似多一段 task 消费重叠**，但这段重叠几乎没有 wall-clock 收益（业务逻辑慢，task 在重叠期消费量本就有限），却引入了 filter 阶段 producer-consumer 并发，多一类锁协调，得不偿失。

### 选 B 之后剩下的并发场景

- filter 阶段：只有 producer，无并发。
- filter 完成 → conversion：一次性发生，与 producer 不并发（producer 已停）。
- conversion 完成 → drain 启动：drain 是 conversion 之后才开始的 producer，与 task 消费并发（与 community_master 上「网络线程入队 + task 线程出队」是同种并发），靠物理 channel 既有锁守护，不引入新锁。
- drain 期间 checkpoint 触发：在物理 channel 上，channel 必须能 snapshot 内存 buffer + 还没排空的 disk 数据。这就引出 Tradeoff 2。

---

## Tradeoff 2：channel 如何感知 disk 数据

### 背景

强需求要求物理 channel 在 disk 还没排空时也能 checkpoint，意味着 channel 必须**有办法**枚举出「我还有哪些 disk 上的数据待消费」并把它们 snapshot 进 channel state。

有两种实现路径都能满足这个需求；它们才是真正的 tradeoff。**「channel 完全不感知 disk」不是合法选项，因为它根本实现不了强需求**——不列入对比。

### 顺序约束（两种方案都必须满足）

无论选 A 还是 B，channel 在消费时必须满足以下顺序约束：

**recovered 数据（包括 ready buffer 与还在 disk 上的 pending DiskRef）必须先于 channel 自身的其他数据源（subpartitionView / 网络 receivedBuffers）被消费。** 否则会乱序——recovered 数据本质是「之前 checkpoint 的 in-flight 数据」，逻辑上必须在「conversion 之后新到的数据」之前被处理。

这意味着 channel 的 `getNextBuffer` 必须有「先检查 recovered 来源，非空则优先返回」的判优逻辑。

### 两条路

- **A（抽出 Store 抽象）**：引入 per-channel 的 `RecoveredBufferStore`，内部持有 `Deque<Buffer> readyBuffers` + `Deque<DiskRef> pendingDiskRefs`，所有 disk-aware 状态封装在 store 里。channel 类持有 store 引用，把 `getNextBuffer` / `checkpointStarted` / `releaseAllResources` / `getBuffersInUseCount` 委托给 store；channel 自己只保留一条「store 非空时优先取 store」的判优规则。
- **B（channel 内嵌单独的 DiskRef 集合）**：channel 类自己新增一个 `Deque<DiskRef> pendingDiskRefs` 字段，与既有的 `receivedBuffers` / `toBeConsumedBuffers` 并列。channel 自己处理多集合：`getNextBuffer` 顺序为 ready buffers → pending DiskRef（走 disk 加载） → 其他来源；`checkpointStarted` 同时遍历 ready + pending 两个集合。

### 代价比较

| 维度 | A（Store 抽象） | B（channel 内集合） |
|------|----------------|---------------------|
| 抽象层数 | 多一层 Store 对象，独立于 channel 生命周期 | 无中间层，channel 直接持 disk 状态 |
| channel 类的改动 | 小——channel 只持 store 引用，操作委托给 store；ready / pending 的内部细节不出现在 channel 类里 | 大——channel 类内嵌 `Deque<DiskRef>` 字段；`getNextBuffer` / `checkpointStarted` / `releaseAllResources` / `getBuffersInUseCount` 都要扩展处理 pending 集合 |
| 复用 | 同一份 store 实现可以同时被 RecoveredInputChannel 与物理 channel 用，ready/pending 判优 + disk 加载 + checkpoint snapshot 写一次 | 每个 channel 类（Local / Remote / Recovered）都要内嵌一遍同样的逻辑（可以提到基类 InputChannel，但要平衡基类污染） |
| 顺序约束如何表达 | channel 一条规则：「store 非空优先取 store」；store 内部自己保证 ready 在 pending 之前。channel 看到的判优维度只有一个 | channel 自己管多源判优：ready → pending → 其他来源，三层 if-else 散落在 `getNextBuffer` 里 |
| 锁协调对象 | store 自身 monitor 守护 readyBuffers + pendingDiskRefs | channel 自身既有 monitor 扩展守护 pendingDiskRefs |
| conversion 时的状态移交 | 把 store 引用从 RecoveredInputChannel 转给物理 channel 即可（一个对象引用切换） | 把 receivedBuffers 与 pendingDiskRefs 两个集合分别移交（两套字段切换） |
| 与「外部 OutputWriter / 调度器」的接口 | 外部通过 `store.addBuffer` / `store.appendPending` 等明确的 API 操作 store | 外部通过 channel 既有的入队入口操作 channel；channel 要暴露新的 pending 入队入口 |

### 当前推荐

**取 A（保留 Store 抽象）**。理由：

1. **避免 channel 类重复实现相同逻辑**。ready/pending 判优、DiskRef → Buffer 加载、checkpoint 同时 snapshot 两个集合——这些逻辑在 Local / Remote / Recovered 三类 channel 上是完全一致的。抽出 Store 一次实现即可；选 B 则要么三处各写一遍（重复），要么提到 InputChannel 基类（污染基类）。
2. **顺序约束在 A 下更清晰**。Channel 只需要一条规则——「store 非空时优先取 store」，ready 在 pending 之前的内部顺序由 store 自己保证，channel 不感知这个细节。选 B 时多源判优逻辑（ready → pending → 其他）全部嵌在 channel 的 `getNextBuffer` 里，规则散开，维护成本高。
3. **conversion 时的状态移交简单**。Store 是单一对象引用，从 RecoveredInputChannel 转给物理 channel 时只需一次切换；选 B 时要分别移交两个集合，且要保证两次切换的原子性，增加协调难度。
4. **外部接口边界清晰**。OutputWriter 通过 `store.addBuffer` / `store.appendPending` 直接操作 store，不需要穿透到 channel 内部细节；channel 与外部 producer 的耦合面收口到 store 一处。

代价（接受）：

- 多一层对象（Store），多一份生命周期管理。但 store 的生命周期与所属 channel 严格绑定，不会出现孤儿对象。
- 锁数量上：A 多一把 store monitor。但这是 Tradeoff 1 = B 之后无法避免的（drain 阶段 loader 与 task 仍要在 readyBuffers + pendingDiskRefs 上互斥），不论选 A 还是 B 都至少需要一把 monitor，A 的 store monitor 与 B 的 channel monitor 扩展在锁数量上等价。

---

## Tradeoff 3：多 channel 共享 spill file vs 独占

### 两条路

- **A（共享单文件）**：一个 task 一个 spill 文件，所有 channel 的 bytes append 进去，`SpillEntry` 用 `channelInfo` 区分。文件超 64 MB rotation。
- **B（每 channel 一个文件）**：每个 channel 一个 spill 文件，写入和清理粒度都是 channel。

### 代价比较

| 维度 | A | B |
|------|---|---|
| file handle 数 | O(rotation 次数) | O(channel 数) |
| rotation 复杂度 | 单线性流，简单 | per-channel 各自 rotation |
| 清理粒度 | 整个文件所有 entry 都 drain 完才删 | 单 channel 完成就能删 |
| 读盘顺序 | 单文件顺序 read，按 FIFO 自然 sequential | per-channel sequential，跨 channel 来回切 |

### 当前推荐

**取 A（共享单文件）**，与当前实现一致。

理由：channel 数可能上千；B 的 file handle 数量级远超 A，rotation 与清理代码也复杂。Spark / Flink 既有 spill 实现都选 A。

跟锁无关——A、B 都不引入新锁，写盘都是单 producer 单写者。

---

## Tradeoff 4：disk → memory 加载的线程模型

### 候选

- **A（单 loader 线程）**：recovery 线程自己跑 drain，循环 `dequeue entry → request buffer → read disk → enqueue channel`。
- **B（每 channel 一个 loader 线程）**：N 个加载线程，每个 channel 独立加载。
- **C（task 线程触发加载）**：task 线程消费时遇到 disk reference 自己读盘。

### 代价比较

| 维度 | A | B | C |
|------|---|---|---|
| 实现复杂度 | 单线程循环，最简单 | 多线程协调，需要 task pool / 线程命名 / 退出条件 | task 线程被阻塞做 I/O，违反 mailbox 模型 |
| I/O 模式 | 单文件顺序 read | 多读者 random read（spill 是共享文件） | random read |
| 与 BufferPool 周转的关系 | 单线程 blocking request，task 消费一条释放一条 | N 个 blocking request 抢同一 pool | task 既消费又 I/O，更慢 |
| 错误处理 | 单线程，try-catch + 任务级错误传播即可 | N 个线程任一失败都要级联 | 错误嵌进消费路径 |

### 当前推荐

**取 A（单 loader 线程）**。

理由：disk 总量受 BufferPool 上限约束（filter 阶段超 pool 才 spill），不会太大；单文件顺序读比并行 random read 更快；实现成本最低。B 的并行只在 disk bandwidth 远大于 BufferPool 周转速度时才有收益，本场景下不成立。C 直接违反 Flink 线程模型，pass。

跟锁无关——只是线程数选 1 还是 N。

---

## Tradeoff 5：是否消除 RecoveredInputChannel

### 两条路

- **A（保留）**：维持 Flink 既有 `RecoveredInputChannel → toInputChannel() → Local/Remote` 流程，与 community_master 一致。
- **B（消除）**：从一开始就创建 `LocalInputChannel` / `RemoteInputChannel`，加一种「暂停 partition request 直到 recovery 完成」机制；recovery 数据由外层 inject 进物理 channel。

### 代价比较

| 维度 | A | B |
|------|---|---|
| Flink 既有契约 | 完全沿用 | 大改 SingleInputGate 创建路径与 rescale 处理流程 |
| 触发 conversion 的 future（`stateConsumedFuture`）语义 | 沿用 | 消失，需要新机制取代 |
| 物理 channel 必须支持「partition 没启动」的状态 | 不需要 | 必须扩展，影响生命周期方法 |
| 改动半径 | 局部（recovery handler 内） | 全局（input gate + 物理 channel + 重新做单测） |

### 当前推荐

**取 A（保留）**。

理由：消除 RecoveredInputChannel 是一个独立、风险大的 refactor，与本 branch 的核心目标（用 disk 替换 heap fallback）无关。即使从架构洁癖角度看 RecoveredInputChannel 有点冗余，本 branch 不是合适的载体。

---

## Tradeoff 6：checkpoint 时是否保留「一次顺序读整个 spill 文件」的 I/O 优化

### 背景

当前 branch 的 `FilteredBufferDispatcherImpl` 引入了一整套跨 channel 协调机制：`dispatcherLock`、`waitSet`、`checkpointStartPos`、`checkpointSnapshots`、`drainHead`、`currentCheckpointId` / `lastStoppedCheckpointId`、`Reader.snapshot()` / `Reader.entries` 的 `ConcurrentLinkedDeque` / `removeEntriesForChannel`、`RecoveredBufferStoreCoordinator` 回调接口、`onChannelCheckpointStarted` / `onChannelCheckpointStopped` / `onChannelReleased` 等。

**这一整套机制只为一件事服务**：checkpoint 触发时，等所有 channel 完成自己的 step 1 ready snapshot（凑齐 waitSet 收敛），然后由 dispatcher **一次性顺序读整个 spill 文件**，按 entry 的 channelInfo 路由到 `ChannelStateWriter`。这样磁盘 I/O 是 sequential 的、单遍扫盘，读盘最快。

代价是：跨 channel 协调状态、phase 2 漏 entry race、close ↔ drain 死锁路径、`dispatcherLock` ↔ `store monitor` 的强锁序耦合、Reader 内 entries 队列被多线程访问等一整圈复杂度。

### 两条路

- **A（保留顺序 I/O）**：当前实现。等所有 channel 收敛后一次顺序读整盘，I/O 模式最优，**整套跨 channel 协调机制不可避免**。
- **B（放弃顺序 I/O，接受 random I/O）**：每个 channel 在自己的 `checkpoint` 路径上独立处理自己的 disk entries（store.pendingDiskRefs）。channel 之间不再等待、不再协调；多 channel 的 InputStream 由 `ChannelStateWriter` 的 executor 按 enqueue 顺序串行消费，磁盘上是按 channel 触发顺序跳着读 offset。

### 代价比较

| 维度 | A（顺序 I/O） | B（random I/O） |
|------|--------------|-----------------|
| 磁盘 I/O 模式（checkpoint 时）| 单遍 sequential | 多个 InputStream 按触发顺序读，offset 跳跃 |
| HDD 性能 | 好 | seek 抖动，慢 |
| SSD / NVMe 性能 | 好 | 几乎一样（seek 代价接近 0）|
| dispatcher 状态机 | `waitSet` / `checkpointStartPos` / `checkpointSnapshots` / `drainHead` / `currentCheckpointId` / `lastStoppedCheckpointId` | 无任何跨 channel 协调状态 |
| `dispatcherLock` | 必须，守护以上协调状态 | **不需要**；dispatcher 退化为 per-task 单线程对象 |
| `Reader` 抽象 | 持 `entries` 队列（`ConcurrentLinkedDeque`）+ `snapshot()` + `removeEntriesForChannel` + `freeze` | 退化为「文件引用」，甚至完全消失——entries 全部挪进 `store.pendingDiskRefs`，checkpoint 自己 `Files.newInputStream(path)` |
| `Coordinator` 接口（dispatcher 被 store 回调）| 必须 | **删除** |
| spill 文件生命周期管理 | dispatcher 跟踪所有 in-flight checkpoint snapshot、ref count 决定何时 close | 依赖 POSIX `unlink-after-open` 语义：filter 线程 close 时直接 unlink，已开的 InputStream 仍能读完。**无 ref count** |
| phase 2 漏 entry race | 存在，需要 drainHead + startPos + 三段拆锁挡（`phase2_drain_race.md` 整篇）| **结构上不存在**——每个 channel 的 ready + pending 在同一个 store monitor 内 atomic snapshot |
| FLINK-39519 / close↔drain 死锁路径 | 需要 `close_drain_separation` 契约挡 | 大幅简化——dispatcher 无锁、close 路径不持任何阻塞锁 |

### 当前推荐

**取 B（放弃 checkpoint 时的顺序 I/O）**。理由：

1. **典型部署是 SSD / NVMe**，seek 代价接近 0，A 的 I/O 优势在主流硬件上不显著。HDD 部署上 B 有 seek 抖动，但 checkpoint 是低频操作 + recovery 本身已经是非热路径，整体 wall-clock 影响可接受。
2. **B 是整个 branch 锁简化的「最大单一收益」**。dispatcher 完全去 lock 化（单线程访问，零 monitor），从根本上消除 `dispatcherLock ↔ store monitor` 的强锁序耦合。
3. **一次性删掉一整套基础设施**：`dispatcherLock` / `waitSet` / `checkpointStartPos` / `checkpointSnapshots` / `drainHead` / `Coordinator` 接口 / `Reader.entries` 并发队列 / `Reader.snapshot()` / ref count——这些都是为「顺序 I/O 优化」付出的工程债。
4. **结构性消除 phase 2 漏 entry race**——不再需要 `phase2_drain_race.md` 那篇 6 节论证的复杂方案。

### 选 B 之后的整体简化路径

dispatcher 退化为 **per-task 的单线程产消费者**：

- **filter 阶段**（filter 线程独占）：写 bytes 进 spill 文件 → 创建 `DiskRef(path, offset, length)` → 在 `synchronized (store)` 内 append 到 `store.pendingDiskRefs`。dispatcher 自身的 cache / spillFile / readers 全部只被 filter 线程访问。
- **drain 阶段**（filter 线程沿用）：按 spill 文件物理顺序逐条读 entry → 通过 channelInfo 路由到 store → 在 `synchronized (store)` 内 `pendingDiskRefs.pollFirst()` + add buffer 到 readyBuffers。仍然是单线程。
- **close 阶段**（filter 线程沿用）：关 FileChannel、unlink spill 文件。POSIX 上 unlink 后已开的 InputStream 仍能读完，所以不需要等 checkpoint snapshot 释放。

**每个 channel 的 checkpoint（task 线程触发）**：

- 进 store monitor：拍 ready buffer snapshot + 浅拷贝 `pendingDiskRefs`
- 出 store monitor
- 对每个 DiskRef，自己 `Files.newInputStream(diskRef.path)` + `skip(offset)` + 读 `length` 字节 → 交给 `ChannelStateWriter.addInputData` 流式重载
- 多个 channel 各自跑、互不等待、互不协调

**最终的简化收益清单**：

| 项 | 当前实现 | 简化后 |
|----|----------|--------|
| 本 branch 引入的额外锁字段 | 9 项 | **1 项**（store monitor） |
| 跨 channel 协调状态 | 6 组（waitSet / startPos / snapshots / drainHead / ckptId / lastStopped）| 0 |
| Reader 复杂度 | entries 队列 + snapshot + removeForChannel + freeze + ConcurrentLinkedDeque | 退化为「文件引用」或完全消失 |
| spill 文件生命周期管理 | dispatcher ref count 跟踪 | POSIX unlink-after-open，无 ref count |
| dispatcher 单线程化 | 否（多线程 + lock）| **是**（零锁） |
| 设计文档负担 | `phase2_drain_race.md` / `spill_reader_drain_concurrency.md` / `close_drain_separation.md` 等大段论证 | 绝大多数可删 |

### 风险

- **HDD 部署上的 checkpoint 速度回归**：需要在典型 HDD 环境下量化 checkpoint 完成时间，确认在可接受范围。如果某些大客户硬件确实是 HDD 且 checkpoint 时长敏感，可以未来再在 `ChannelStateWriter` 的 executor 那层加一个「按 (path, offset) 排序后 dispatch」的 reorder，把 I/O 顺序拗回 sequential。这是单线程内的排序，**不引入锁**。

---

## 综合方案

强需求（filter 完成立刻 checkpoint）是不可妥协的前提，所有 tradeoff 选项都必须满足它。

推荐组合：

| 项 | 选择 | 备注 |
|----|------|------|
| 强需求 | filter 完成立刻可 checkpoint | 不可妥协，所有选项必须满足 |
| Tradeoff 1（filter↔consume 重叠） | B（filter 完才让 task 消费） | 减少 filter 阶段并发场景，少一类锁；filter 本身快，不拖延 checkpoint |
| Tradeoff 2（channel 感知 disk 的方式） | A（保留 Store 抽象） | 避免 Local/Remote/Recovered 三类 channel 重复实现 ready/pending 判优 + disk 加载 + checkpoint snapshot；顺序约束（store 数据先于其他源）在 store 内部封装更清晰 |
| Tradeoff 3（spill 文件共享） | A（共享单文件） | 与当前实现一致 |
| Tradeoff 4（loader 线程数） | A（单线程） | 顺序 I/O，最简单 |
| Tradeoff 5（RecoveredInputChannel） | A（保留） | 与本 branch 目标无关，不动 |
| Tradeoff 6（checkpoint 顺序 I/O） | B（放弃顺序 I/O，接受 random I/O） | 用 checkpoint 时的 seek 换 dispatcher 完全无锁；典型 SSD/NVMe 部署 seek 代价接近 0；同时删掉一整套跨 channel 协调机制 |

整体形态等价于：

- filter 阶段：单写者把数据写到内存 buffer 或 spill 文件，task 不消费。
- filter 完成：立刻 conversion；物理 channel 上线，可以接受 checkpoint。
- drain 阶段：在 conversion 之后启动，loader 单线程把 spill 数据加载回内存，灌进物理 channel；task 同时消费物理 channel。
- checkpoint 触发：每个 channel 自己进 store monitor，atomic snapshot ready buffer + 浅拷贝 pendingDiskRefs；然后各自独立 open InputStream 读对应 spill 文件位置，交给 ChannelStateWriter。channel 之间不等待、不协调。

## 当时讨论过、但被推翻的中间方案

1. **「filter↔task 消费重叠」（Tradeoff 1 = A）**：上一轮我推荐过，理由是「与 community_master 一致」「不损失 wall-clock」。后来认识到 filter 本身很快、recovery 的真正瓶颈在 task 消费业务逻辑，重叠收益不显著；而且重叠引入 filter 阶段 producer-consumer 并发，多一类锁需求，得不偿失。改推 Tradeoff 1 = B。
2. **「drain 跑在 conversion 之前」**：上一轮我推荐过，理由是消除 producer-conversion 并发可以退掉所有额外锁。**被强需求否决**——drain 期间不允许 checkpoint 等待 drain 完成。
3. **「channel 完全不感知 disk」**：上一轮我把它写成 Tradeoff 2 的 B。实际上它根本满足不了强需求（checkpoint 会漏 disk 数据），不是合法选项；正确的 B 是「channel 内嵌 DiskRef 集合」，与「Store 抽象」对比。
4. **「消除 RecoveredInputChannel」**：从架构洁癖看有道理，但与本 branch 目标无关，风险过大。Tradeoff 5 选 A。

## 风险与待确认项（取舍层面的，跟锁无关）

### Tradeoff 1 = B 下的 BufferPool 容量评估

filter 期间 task 不消费，BufferPool 不被回收，spill 量上升。要在 BufferPool 极小 + recovery 状态极大的场景下量化「多 spill 多少 / 多写多少盘」，确认仍在可接受范围。如果不可接受，可以考虑 BufferPool 自适应扩展或允许 Tradeoff 1 = A 的退路。

### Tradeoff 3 + Tradeoff 4 + Tradeoff 6 联合下的清理保证

共享单文件 + 单 loader + checkpoint 走自己 open 的独立 InputStream（Tradeoff 6 = B）：

- 所有 channel 的 `store.pendingDiskRefs` 都为空 → 没有 loader 还会回来读这个文件
- 没有任何 in-flight checkpoint 还在引用该文件的 InputStream

清理时机：**filter 线程的 close 路径直接 unlink 文件并 close 自己的 FileChannel**。POSIX 上 unlink 后已开的 InputStream 仍能读到底，所以即使有 in-flight checkpoint 持着 InputStream，它们也能继续读完。当所有 InputStream close 完毕，操作系统才真正释放磁盘空间——这一切**不需要 ref count，也不需要协调**。

如果在 Windows 等不支持 unlink-after-open 的平台部署，需要单独处理（一种做法是退回 ref count；或者干脆不支持 Windows 部署，Flink 集群本来就不是 Windows-first）。
