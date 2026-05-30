# `read buffer is null` —— recovery 期间 exclusive credit 超发修复

## 1. 症状

`UnalignedCheckpointRescaleITCase` 复现：rescale 重启后的新 job 早期（seq=3/4，上游发头几条 live data 时），consumer 端抛：

```
java.io.IOException: java.lang.IllegalStateException: The read buffer is null in credit-based input channel.
  at CreditBasedPartitionRequestClientHandler.decodeBufferOrEvent(...)
```

即上游发来 live data buffer，consumer 端 netty 解码要从该 channel 的 buffer 池分配一个 buffer 来装，但**池空（availBuffers=0）**，分配不到 → null → 抛 → task fail-fast（无重启）→ 测试失败。

诊断证据（heap dump + 运行时日志实测）：
- 报错时 channel 在 map、未释放，纯粹是池空（exclAvail=0, floatAvail=0）。
- 不是资源泄漏（gate 关闭→channel 释放的不变式完好）。
- 报错时刻 channel 的 2 个 exclusive buffer 被借走未还。

## 2. 根因：credit 账本恒等式在 recovery 期间被破坏

credit-based flow control 的**恒等式**（master 始终维持）：

> 宣告给上游的 credit 总数 == consumer 端池里真正能立即接收 live data 的空 buffer 数。

每个 credit 背后都对应一个真实空着的 buffer。credit 有两类来源：

| credit 来源 | 怎么给上游 | 字段 |
|---|---|---|
| 初始 exclusive credit（= `initialCredit` = `networkBuffersPerChannel`） | `requestSubpartitions()` 时 `PartitionRequest` 直接带 `getInitialCredit()` | 上游 reader 的 `numCreditsAvailable` 初值 |
| 后续 floating / 补充 credit（增量） | `notifyBufferAvailable(n)` 累加 `unannouncedCredit`，从 0 变正的边沿触发 `notifyCreditAvailable` 发出增量 | consumer 端 `unannouncedCredit` |

**spilling-v2 在 recovery 期间从同一个 channel buffer 池借走 exclusive buffer 去装 recovery 数据**（`requestRecoveryBufferBlocking` → 同一个 `bufferManager`）。于是恒等式在两条路径上同时被破坏：

- **途径 A（增量侧）**：recovery 期间 recovery 数据反复借→消费→recycle，每次 recycle 触发 `notifyBufferAvailable(1)`，`unannouncedCredit` 只加不减、累积虚高，汇报给上游 → 超发。这些 recycle 不代表"新增的 live 空位"。
- **途径 B（初始 credit 侧）**：`PartitionRequest` 仍按 `getInitialCredit()` 宣告了 N 个初始 exclusive credit，但这 N 个 exclusive buffer 已被 recovery drain 借走，**池里没有 N 个真实 live 空位**。

两条路径都让"宣告 credit > 真实空 buffer"。上游凭这些不实 credit 发 live data，consumer 池空 → `read buffer is null`。

> `manual_cr/recovery_buffer_ownership.md` §11 的假设「恢复期与上游推数据时间错开，物理 channel 自己的 BufferManager 承担 drain buffer 申请不会和上游争 pool」是**错的**：上游凭初始 exclusive credit 在 recovery 期间就会发 live data，与 recovery drain 借池在时间上重叠。本 fix 修正这一假设带来的 credit 超发。

## 3. master（无恢复）的 credit 机制（事实）

- 起点：`setup()` → `bufferManager.requestExclusiveBuffers(initialCredit)`，下游池里放入 `initialCredit`（本例=2）个 exclusive buffer。`requestSubpartitions()` 构造 `PartitionRequest` 时带 `inputChannel.getInitialCredit()`，上游 reader 初始 `numCreditsAvailable = initialCredit`。此刻 `unannouncedCredit = 0`（初值，见 `RemoteInputChannel:116`）。
- 稳态：上游凭 credit 发 buffer（发一个 `--numCreditsAvailable`），下游消费后 recycle，每 recycle 一个 exclusive 就 `notifyBufferAvailable` → `unannouncedCredit += 1` → 汇报给上游补回一个 credit。下游发信用、上游发 buffer，数量始终对等。
- `unannouncedCredit` 语义：自上次汇报以来新增的、待汇报给上游的增量（`getAndAdd` 加、`getAndResetUnannouncedCredit` 汇报时清零）。借出 buffer 不减它。
- `recycle(MemorySegment)`（`BufferManager`）：归还一个 exclusive segment 时，`addExclusiveBuffer` 若发现持有的 floating 超过 `numRequiredBuffers`，会挤出一个 floating 还回共享池（`releasedFloatingBuffer != null`），此时**不** `notifyBufferAvailable`；否则才 `notifyBufferAvailable(1)`。即 `notifyBufferAvailable(1)` 的 +1 只对应「本 channel 净增一个 exclusive 空位」，floating 的进出已在内部抵消。

## 4. recovery 与 master 的差异（事实）

- master 里 buffer 被占用 = 上游发数据来占用（上游已 `--numCreditsAvailable`），下游不需额外记账。
- recovery 里 buffer 被占用 = 本地 spill drain 借走装 recovery 数据（`requestRecoveryBufferBlocking` → `bufferManager.requestBufferBlocking`），**上游没发、没扣 credit**。
- `requestBufferBlocking`（`BufferManager:186`）：先 `bufferQueue.takeBuffer()`（floating 优先、否则 exclusive），池空再 `bufferPool.requestBuffer()` 从共享 LocalBufferPool 借 floating。**该方法无法区分借出的是 exclusive 还是 floating。**
- 当前代码现状（截至本 fix）：
  - `requestSubpartitions()` 的 `PartitionRequest` 仍带 `getInitialCredit()`（recovery 路径也带，未区分 needsRecovery）。
  - `notifyBufferAvailable` 已加：`recoveredQueue.isInRecovery()` 时只 `getAndAdd` 累加、不 `notifyCreditAvailable`。
  - `finishRecoveredBufferDelivery` / `getNextBuffer` 已加补发逻辑（基于 `unannouncedCredit.get()`）——将被 §6 方案取代（门控下沉到 `BufferManager`、分界点统一为 `allDelivered` 翻转）。

## 5. 待解决的问题（事实，未给方案）

1. **初始 exclusive credit 超发**：recovery 期间那 `initialCredit` 个 exclusive buffer 被 drain 借走装 recovery 数据，但 `PartitionRequest` 仍按 `getInitialCredit()` 告诉上游有这么多 credit，上游据此发 live data，consumer 池空 → `read buffer is null`。
2. **`unannouncedCredit` 在 recovery 期间会虚高**：recovery 期间同一个 buffer 借→消费→recycle 多轮，每轮 recycle `+1`，但借出不减；多轮复用后 `unannouncedCredit` 累积超过真实净空位（例 initialCredit=2，复用后累成 4、6…）。
3. **可选的汇报触发点（两个）**：
   - (a) recovery 交付完成（`recoveredQueue.allDelivered=true`，`finishRecoveredBufferDelivery`）那一刻：此时 recoveredQueue 可能还没被消费空、部分 exclusive buffer 还没 recycle 回池，当前可用 exclusive 数可能 < `initialCredit`。
   - (b) `isInRecovery()` 彻底变 false（`allDelivered && recoveredQueue 空`）那一刻。
   两个时机点都面临同一难点：**如何在该时刻准确拿到「当前可用的 exclusive buffer 数」**——目前没有记录这个信息的字段。
4. **`unsynchronizedGetAvailableExclusiveBuffers()` 不是线程安全的**（方法名 `unsynchronized` 即表明：直接读 `bufferQueue.exclusiveBuffers.size()`，未持 `bufferQueue` 锁）——不能直接用它作为汇报数的来源。
5. **上游 block 的兜底事实**：上游 `PipelinedSubpartition.pollBuffer` 在 `isBlocked && 无 priority` 时 `return null`，有 credit 也不发普通 live data；上游解 block 靠 `resumeConsumption`，由 `UpstreamRecoveryTracker.handleEndOfRecovery` 在所有 channel 的 recovery 数据全部消费完后才触发。

## 6. 方案（定稿）

### 6.1 线程模型（实证）

credit 与 recovery 相关操作分布在三个线程，这是方案设计的前提：

| 操作 | 线程 | 锁 |
|---|---|---|
| `drain()` → `onRecoveredStateBuffer` / `finishRecoveredBufferDelivery`（翻 `allDelivered`） | channelIOExecutor I/O 线程 | `receivedBuffers` |
| `getNextBuffer()`（消费 recovery 数据） | task 主线程 | `receivedBuffers` |
| `recycle(segment)`（buffer 回池） | task 主线程 / 下游算子线程 | `bufferQueue` |
| `onBuffer` → `onSenderBacklog` → `requestFloatingBuffers`（上游来数据） | Netty I/O 线程 | `bufferQueue` |
| floating listener 回调 `notifyBufferAvailable(Buffer)` | Netty / pool 线程 | `bufferQueue` |

### 6.2 根因收敛：两条途径都必须修

**途径 A**：recovery 期 buffer 借→消费→recycle 触发 `notifyBufferAvailable` → credit 被 announce 出去 → 超发。由 §6.3 的门控修复。

**途径 B（双算）**：consumer 端 `PartitionRequest` 带 `getInitialCredit()`（=N），上游 `numCreditsAvailable` 初值 = N。recovery 期这 N 个 exclusive buffer 被借走，全程上游 blocked（`pollBuffer` 对非 priority buffer 直接 `return null`，`PipelinedSubpartition:475`），残留的 N 发不出 live data。但 recovery 退出时 §6.3 的对齐又按「真实可用数」announce 一次——若此刻借走的 buffer 已回池，真实可用=N，于是 `AddCredit(N)` 把上游 credit 累加成 **N + N = 2N**。`AddCredit` 是增量累加，初始那 N **不会被抵消**。resume 后上游凭 2N 发 live data，consumer 池只有 N → 超发。**初始的 N 和退出时 announce 的 N 是同一批 buffer，被双算了。**

> 上游 `initialCredit` 不能直接设 0 来消除初始 N：上游 `CreditBasedSequenceNumberingViewReader` 用 `initialCredit==0` 表征「consumer 是纯 floating channel、无 exclusive buffer」，触发 ① `resumeConsumption` 把 `numCreditsAvailable` 清零（`:160`）② `needAnnounceBacklog` 走 backlog 主动宣告（`:272`）。recovery channel 真实拥有 N 个 exclusive buffer（只是暂借给 drain），不是纯 floating channel，必须保留 `initialCredit=N` 语义。所以只能解耦：`initialCredit` 保留 N，`numCreditsAvailable` 初值设 0。见 §6.6。

### 6.3 单一分界点 + 单一锁（核心）

**分界点**：`allDelivered` 翻转那一刻（`finishRecoveredBufferDelivery`），单调、确定、I/O 线程单点触发。所有 credit 相关分界统一用它，不再用 `isInRecovery()`（后者只留给数据交付侧 `getNextBuffer`/`peekNextDataType`/`checkpointStarted`）。

**门控落点**：产生 credit 的三个源头（`recycle`、`notifyBufferAvailable(Buffer)`、`requestFloatingBuffers`）**全部在 `BufferManager` 内、都持 `bufferQueue` 锁**。故门控字段放 `BufferManager`、`bufferQueue` 锁保护、初值 `= !needsRecovery`。

> 命名：BufferManager 不应感知 "credit"（credit 是 RemoteInputChannel 的概念）。门控字段语义是「是否允许向 inputChannel 发 `notifyBufferAvailable`」——`notifyAvailable`，不叫 `creditGateOpen`。它是 `RecoveredBufferQueue.allDelivered` 在 `bufferQueue` 锁视角下的镜像（锁结构逼出的复制，因 BufferManager 回调不到 RemoteInputChannel 的 `receivedBuffers` 锁），两者必须同步翻转。

- 门控**关**（分界点前）：三处计数后**不向 `inputChannel.notifyBufferAvailable` 传播**，增量丢弃（这些 recycle 不代表真实净空位）。
- 门控**开**（分界点后）：正常传播，回到 master 原生增量逻辑。

门控判断 + 计数都在 `bufferQueue` 锁内串行 → 无漏算窗口（不会出现「buffer 既没被 avail 计入、又被丢弃」）。

**分界点对齐**（`finishRecoveredBufferDelivery`，I/O 线程，两锁先后拿、不嵌套）：
1. `synchronized(receivedBuffers){ recoveredQueue.finish() }` —— 翻 `allDelivered`（数据交付侧，不变）。
2. `bufferManager.enableNotify()` —— `bufferQueue` 锁内翻门控开 + 读 `getAvailableBufferSize()`（exclusive+floating 真实空位），并**自己**调 `inputChannel.notifyBufferAvailable(avail)` 闭环（开关一旦切换，BufferManager 自己 notify 一次，不返回值让 RemoteInputChannel 二次 notify）。开闸前 `unannouncedCredit` 恒为 0（门控掐住所有入口），故 `getAndAdd(avail)` 加到 0 上 = set；与开闸后并发 `+1` 用同一 `AtomicInteger` 原子累加，不丢、不双算（开闸瞬间在池里的 → 计入 avail；之后回池的 → +1）。

### 6.4 锁安全论证

- `recycle` / 三入口只碰 `bufferQueue` 一把锁，**绝不读 `receivedBuffers` 锁下的 `allDelivered`**（改读 `bufferQueue` 锁下的门控镜像）→ 反向锁序不存在 → 不死锁。
- `finishRecoveredBufferDelivery` 先 `receivedBuffers` 后 `bufferQueue`，先后拿、不同时持 → 不死锁。
- 门控字段与 `allDelivered` 语义重复，是锁结构逼出的镜像；两者必须同步翻转（代码注释标注）。

### 6.5 改动清单（途径 A）

**BufferManager**：
- 新增门控字段 `notifyAvailable`（`bufferQueue` 锁保护，构造参数决定初值 `= !needsRecovery`）。
- `recycle` / `notifyBufferAvailable(Buffer)` / `requestFloatingBuffers` 三处：`bufferQueue` 锁内计数后按门控决定是否向外传播增量。
- 新增 `enableNotify()`：锁内翻门控开 + 读真实可用数 + 自己调 `inputChannel.notifyBufferAvailable(avail)` 闭环。
- 构造器新增 `boolean notifyInitiallyEnabled` 参数。
- 删除 §3 残留的 TEMP DIAGNOSTIC 诊断字段/日志。

**RemoteInputChannel**：
- `notifyBufferAvailable`：删 recovery 分支，退回 master 原生。
- `finishRecoveredBufferDelivery`：翻 `allDelivered` 后调 `bufferManager.enableNotify()`（自闭环 notify）。删旧的基于 `unannouncedCredit.get()` 的补发。
- `getNextBuffer`：删 recovery 分支里基于 `unannouncedCredit.get()` 的 credit 触发段。
- 构造时把 `!needsRecovery` 传给 `BufferManager`。

**LocalInputChannel / RecoveredInputChannel**：构造 BufferManager 传 `true`（门常开，行为不变）。

> §5.3 的「两个汇报触发点」已收敛为单一分界点（`allDelivered` 翻转）；§5.4 「如何线程安全拿可用 exclusive 数」由 `enableNotify()` 在 `bufferQueue` 锁内解决。

### 6.6 改动清单（途径 B）

让上游初始 `numCreditsAvailable = 0`，但保留 `initialCredit = N`（exclusive 语义）。

**信号传递**：`PartitionRequest`（`NettyMessage` 内部类）新增 `boolean needsRecovery` 字段，`write`/`readFrom` wire 格式同步（length += 1 byte）。内部协议、同版本收发，无跨版本兼容负担。

- **NettyPartitionRequestClient.requestSubpartition**：构造 `PartitionRequest` 时带 `inputChannel` 的 recovery 标志。
- **PartitionRequestServerHandler**：用 `request.needsRecovery` 构造上游 reader。
- **CreditBasedSequenceNumberingViewReader**：构造器新增参数，`initialCredit` 仍 = `request.credit`（保留 resume 清零 / backlog 宣告语义），但 `numCreditsAvailable` 初值 = `needsRecovery ? 0 : initialCredit`。

由此初始 N 不再凭空存在，credit 完全由 consumer 退出时 `enableNotify` 一次性按真实可用数汇报，与途径 A 的修复闭合成单一 credit 来源。
