# LocalInputChannel / RemoteInputChannel recovery 路径重构提案

> Scope：消除 `LocalInputChannel` 与 `RemoteInputChannel` 中 recovery 相关的代码重复与逻辑分散。包含两块改动，**统一在一个 commit 内完成**：
>
> - **改动 A：RecoveredBufferQueue 组件抽取**（§1-§7）—— 把重复字段与方法封装到一个组件
> - **改动 B：`peekNextDataType()` helper 抽取**（§8）—— 把 6 处分散的 nextDataType 计算收敛到每个 channel 的一个私有方法
>
> 两块改动属于同一议题（两个 channel 里 recovery 路径的逻辑分散），且都触动相同的两个文件、相同的方法体；分开提交会导致中间状态难读（commit 1 落地后 helper 还在访问已被组件接管的字段）。统一一个 commit 提交，前缀仍按项目规范 `[FLINK-38544][refactor] ...`。
>
> 不改 `RecoverableInputChannel` 的公开契约、不改 `InputChannel` 基类、不改 `ResultSubpartitionView` 接口。

## 1. 当前重复点

两个 channel 各自持有以下 recovery 状态，字段名、初始值、`@GuardedBy` 含义、javadoc 几乎逐字相同：

| 字段 | Local | Remote |
|---|---|---|
| `Deque<Buffer> recoveredBuffers` | L96 | L133 |
| `boolean allRecoveredBuffersDelivered` | L110 | L147 |
| `int recoverySequenceNumber` | L117 | L154 |

围绕这三个字段，下列方法在两个类里几乎是 copy-paste：

| 方法 | Local | Remote | 差异 |
|---|---|---|---|
| `onRecoveredStateBuffer` | L170-184 | L225-239 | 锁对象、`isReleased` 判断方式 |
| `finishRecoveredBufferDelivery` | L192-197 | L247-252 | 锁对象 |
| `inRecovery` 谓词 | L208, L401 | L338, L858, L1141 | 写法相同 |
| `collectPreRecoveryBarrier` | L237-262 | L904-929 | 完全相同 |
| `releaseRetainedBuffers` (static) | L264-268 | L931-935 | 完全相同 |
| `isRecoveryCheckpointBarrier` (static) | L270-280 | L937-947 | 完全相同 |
| `releaseAllResources` 中 recovery 部分 | L691-696 | L488-493 | 锁对象 |

唯一实质差异：**锁对象**。Local 锁 `recoveredBuffers` 自身，Remote 锁 `receivedBuffers`（为了让 `getNextBuffer` / `checkpointStarted` 能在一把锁里原子读两个队列，见 simplify_approach §3.3）。

## 2. 提议的组件

```
class RecoveredBufferQueue {
    private final Deque<Buffer> buffers = new ArrayDeque<>();
    private boolean allDelivered = false;
    private int sequenceNumber = Integer.MIN_VALUE;

    // ---------- producer side ----------
    boolean offer(Buffer b);              // returns wasEmpty (caller decides notify)
    void finish();                        // flips allDelivered
    
    // ---------- consumer side ----------
    boolean isInRecovery();               // !allDelivered || !buffers.isEmpty()
    boolean isAllDelivered();
    boolean isEmpty();
    Buffer peek();
    Buffer poll();
    int nextSequenceNumber();             // post-increment, returns recoverySequenceNumber then ++
    
    // ---------- checkpoint / lifecycle ----------
    List<Buffer> collectPreRecoveryBarrier(long checkpointId) throws IOException;
    void releaseAll();                    // recycle all + clear
}
```

**所有方法都是 unsafe 的**：组件不持锁，调用方在外层 `synchronized(...)` 内调用。这样保留了 Remote 端"一把锁原子读两个队列"的设计。

## 3. 锁策略：组件不持锁

- 组件方法全部 unsafe，调用方负责加锁
- Local：把原有 `synchronized(recoveredBuffers)` 改成 `synchronized(recoveredQueue)`
- Remote：维持 `synchronized(receivedBuffers)`，把内部对 `recoveredBuffers` 的访问替换成对 `recoveredQueue` 的访问

组件是 value-holder + 操作集合，不是 thread-safe 容器；语义上等同于把「那一坨重复的字段 + 方法」打包成命名清晰的单元，加锁责任仍在 channel。这样保留了 Remote 端「一把锁原子读两个队列」的设计（simplify_approach §3.3）。

> 已废弃：组件自持锁方案。会让 Remote 在 `getNextBuffer` / `checkpointStarted` 等需要原子读两个队列的地方引入「先拿 `receivedBuffers` 锁再拿组件锁」的双锁顺序约束，与 simplify_approach §3.3「不引入新锁对象」的明确决策冲突。

## 4. 接入示意

### LocalInputChannel

字段：
- 删除：`recoveredBuffers`、`allRecoveredBuffersDelivered`、`recoverySequenceNumber`
- 新增：`private final RecoveredBufferQueue recoveredQueue = new RecoveredBufferQueue();`

方法体改写（示意，未完整列出）：

- `onRecoveredStateBuffer(buf)`：
  ```
  synchronized (recoveredQueue) {
      if (isReleased) { buf.recycleBuffer(); return; }
      wasEmpty = recoveredQueue.offer(buf);
  }
  if (wasEmpty) notifyChannelNonEmpty();
  ```
- `finishRecoveredBufferDelivery()`：
  ```
  synchronized (recoveredQueue) { recoveredQueue.finish(); }
  ```
- `getNextBuffer()` 的 recovery 分支：把对 `recoveredBuffers` 的 peek/poll 全换成 `recoveredQueue.peek()` / `recoveredQueue.poll()`，`!allRecoveredBuffersDelivered || !recoveredBuffers.isEmpty()` 换成 `recoveredQueue.isInRecovery()`。
- `checkpointStarted` 的 recovery 分支：调 `recoveredQueue.collectPreRecoveryBarrier(barrier.getId())`。
- `releaseAllResources`：`synchronized (recoveredQueue) { recoveredQueue.releaseAll(); }`。

### RemoteInputChannel

字段：同上，删除三个，新增一个 `recoveredQueue`。

注意 Remote 的锁仍然是 `receivedBuffers`：
- `onRecoveredStateBuffer(buf)`：外层 `synchronized (receivedBuffers)` 内调 `recoveredQueue.offer(buf)`。
- `getNextBuffer()` 的 recovery 分支：在已有的 `synchronized (receivedBuffers)` 内调组件方法。
- `checkpointStarted` 同理。

组件本身**完全不感知**外层用哪个锁，这是本方案的关键。

## 5. 这个组件能消除什么

| 重复项 | 消除方式 |
|---|---|
| 3 个字段声明 + javadoc 重复 | 字段搬进组件，两个 channel 只剩一行 `recoveredQueue` 持有 |
| `onRecoveredStateBuffer` 方法体重复 | 留 wake-up 壳；逻辑搬进组件 |
| `finishRecoveredBufferDelivery` 方法体重复 | 同上 |
| `inRecovery` 谓词 5 处重复 | 统一调 `recoveredQueue.isInRecovery()` |
| `collectPreRecoveryBarrier` 完全重复 | 移到组件（替代「放到 `RecoverableInputChannel` 静态方法」方案——见 §6） |
| `releaseRetainedBuffers` / `isRecoveryCheckpointBarrier` 两个 static helper 重复 | 组件内 private，自然消失 |
| `releaseAllResources` 里 recovery 清理重复 | 调 `recoveredQueue.releaseAll()` |

预计**两个 channel 合计减少 ~100 行代码**，且所有 recovery 状态机的入口只剩一组方法。

## 6. 与「`collectPreRecoveryBarrier` 放到 `RecoverableInputChannel`」方案的关系

之前讨论是「把 `collectPreRecoveryBarrier` + 两个 static helper 放到 `RecoverableInputChannel` 接口的伴生 util 或 default 方法」。

如果只做 §6 这个最小改动：能消除 `collectPreRecoveryBarrier` / `releaseRetainedBuffers` / `isRecoveryCheckpointBarrier` 三处，但**不能消除**字段重复、`onRecoveredStateBuffer` / `finishRecoveredBufferDelivery` / `inRecovery` / `releaseAll` 这些重复。

如果做了本提案（抽 `RecoveredBufferQueue`），`collectPreRecoveryBarrier` 自然落到组件里（操作组件内部的 buffers），就不再需要放到 `RecoverableInputChannel`。**两个方案不是叠加关系，是替代关系。**

我倾向后者：一次抽干净，避免 recovery 状态分散在两个地方（部分在 channel 字段、部分在 interface util）。

## 7. 风险与开放问题

1. **锁责任无类型系统保护**：本方案依赖 `@GuardedBy` javadoc 而非编译期强制。Remote 的两队列原子性如果未来有人改坏了，编译不会报。
   - 缓解：组件方法 javadoc 明确写「caller must hold appropriate channel monitor」；可加 `assert Thread.holdsLock(...)` 在测试 build 中校验，但需要把锁对象注入或暴露。
2. **`RecoveredInputChannel.toInputChannel()` 迁移路径**：master 上 `RecoveredInputChannel` 把缓存的 buffer 迁移到新 channel 时调 `onRecoveredStateBuffer` 然后 `finishRecoveredBufferDelivery`，此时 channel 还未对外暴露，没有外层锁（见 simplify_approach §3.1）。组件方法 unsafe 没问题，外层不加锁也能跑。
   - 这条迁移路径在两个 channel 都成立，组件抽取不影响它。
3. **`recoverySequenceNumber` 的所有权**：当前是 task 线程单线程递增，搬进组件后 `nextSequenceNumber()` 也是 unsafe，调用方在 task 线程调用即可。语义不变。
4. **组件命名**：`RecoveredBufferQueue` 描述准确但偏长；备选 `RecoveryState` / `RecoveryBuffers` / `DrainedBufferStash`。
5. **测试**：组件需要独立单测覆盖 offer/finish/poll/collectPreRecoveryBarrier 各种边界（空队列、sentinel 找不到、IOException 时的 retain release 顺序等）。当前这些边界散落在两个 channel 的测试里，抽取后可以收敛。

## 8. `peekNextDataType()` helper 抽取

`BufferAndAvailability.nextDataType` 表达「消费完当前 buffer 后下一个 buffer 的类型」，是消费侧（`CheckpointedInputGate` 等）做调度决策的关键信号。当前在两个 channel 里**共 6 处独立计算**这个值，每处优先级链不同：

| 位置 | 优先级链 |
|---|---|
| `LocalInputChannel.getBufferAndAvailability` (L598-602) | `BufferAndBacklog.getNextDataType()` |
| `LocalInputChannel.pullPriorityFromSubpartitionView` (L499-510) | `next.getNextDataType()` → 不再 priority 且 `recoveredBuffers` 非空时覆盖为 `recoveredBuffers.peek().getDataType()` |
| `LocalInputChannel.wrapRecoveredBufferAsAvailability` (L542-562) | `recoveredBuffers.peek()` → `NONE`（drain 未结束）→ `subpartitionView.getAvailabilityAndBacklog(true)` 映射成 `DATA_BUFFER`/`NONE`（**有损**） |
| `RemoteInputChannel.getNextBuffer` recovery 分支 (L357-364) | `recoveredBuffers.peek()` → `NONE`（drain 未结束）→ `receivedBuffers.peek().buffer.getDataType()` → `NONE`（三层嵌套三元） |
| `RemoteInputChannel.pollReceivedBufferAsPriority` (L432-439) | `recoveredBuffers.peek()` → `receivedBuffers.peek()` → `NONE` |
| `RemoteInputChannel.getNextBuffer` master 分支 (L391-394) | `receivedBuffers.peek()` → `NONE` |

### 8.1 现状的危害

- 6 处实现互相不一致 —— 同一概念，6 种不同写法
- F5 发现 sentinel 污染：`RecoveryCheckpointBarrier` 被当成 `EVENT_BUFFER` 透传给下游 deser，根因就是没有 single source of truth 来过滤 sentinel
- E5 发现 inRecovery 跨锁不一致，跟 nextDataType 在同一函数里
- 未来要改任一规则（sentinel 过滤、新增优先级源、Local 端有损探测修复），必须同步改 6 处，靠人脑保证一致

### 8.2 提议

每个 channel 抽一个**私有方法** `peekNextDataType()`，所有构造 `BufferAndAvailability` 的位置都通过它取 nextDataType：

```
@GuardedBy("<channel monitor>")
private Buffer.DataType peekNextDataType();
```

两个 channel **各自实现**（数据源不同：Local 走 `subpartitionView`，Remote 走 `receivedBuffers`）。不抽到 `InputChannel` 基类，避免引入抽象方法让基类变臃肿，也避免和 master 上 `RecoveredInputChannel` 等其它实现强行对齐。

### 8.3 两阶段结构

helper 签名：`peekNextDataType(nextDataTypeOnUpstream: Buffer.DataType): Buffer.DataType`

参数 `nextDataTypeOnUpstream` 由 caller 用自己最权威的源**预先算好**再传入：

| caller | 权威源 |
|---|---|
| Local `getBufferAndAvailability` | `BufferAndBacklog.getNextDataType()`（view 自带） |
| Local `pullPriorityFromSubpartitionView` fallback | 同上 |
| Local `wrapRecoveredBufferAsAvailability` | view 探测（有损：`DATA_BUFFER`/`NONE`） |
| Remote 3 处 | `receivedBuffers.peek()?.buffer.getDataType() ?? NONE` |

helper 逻辑：

```
if (recoveredQueue 非空):                            # 阶段 1：恢复期，队列还有 buffer → 队列头优先
    return recoveredQueue.peek().getDataType()
if (!allRecoveredBuffersDelivered) {                 # 阶段 1：恢复期，队列空但 drain 未完
    return NONE                                       #   仅 Local 走这条（必须 block，防 view live data 泄露）
                                                      #   Remote 不走（§3.8 不变式：in-recovery 时
                                                      #   receivedBuffers 只有 priority/control，安全暴露）
}
return nextDataTypeOnUpstream                        # 阶段 2：非恢复期，用 caller 预算的权威值
```

**priority 处理不进 helper**：priority 路径的 nextDataType 都从 caller 手上的 `BufferAndBacklog.getNextDataType()` 拿（view 自带权威类型，跟 master 同一套机制）。helper 只负责「recovery 队列优先 vs upstream fallback」这一件事。

> F5 的 sentinel 污染（`RecoveryCheckpointBarrier` 残留导致 `peek().getDataType()` 报 `EVENT_BUFFER`）**不在 helper 内修**：每次 peek 做 sentinel 识别需要反序列化整个 event，代价过高。Sentinel 残留应由 snapshot 协议（Step 1 插入 / Step 2 清理）的生命周期保证不出现；helper 假设 queue 内不含 sentinel。这是一个独立 bug 修复议题。

### 8.4 `LocalInputChannel.peekNextDataType()`

设计原则：

- **参数 `nextDataTypeOnUpstream` 由 caller 用自己最权威的源预先算好**（`BufferAndBacklog.getNextDataType()` 或 view 探测）。读这个参数不需要锁
- helper 内部只锁两个真正共享的字段：`recoveredQueue` 与 `allRecoveredBuffersDelivered`
- 「返回 `nextDataTypeOnUpstream`」这条路径**显式留在 synchronized 块之外**，明确表达「不需要 acquire lock」
- 不带 `@GuardedBy` 注解：helper 自包含、不强制 caller 套外层锁

```java
private Buffer.DataType peekNextDataType(Buffer.DataType nextDataTypeOnUpstream) {
    // 仅锁住「读 recovery 队列 + 读 allRecoveredBuffersDelivered」这一段。
    // 两个字段都由 drain 线程写、task 线程读，必须在同一 monitor 内原子读取，
    // 否则会出现「queue 空 + flag 已翻 true 但 drain 中途又 push 了新 buffer」的漏读
    synchronized (recoveredQueue) {
        if (!recoveredQueue.isEmpty()) {
            return recoveredQueue.peek().getDataType();
        }
        if (!allRecoveredBuffersDelivered) {
            // drain 仍在产 + 队列已空 → block 普通上游
            // 理由：subpartitionView 可能有 live data，暴露给 consumer 会被错误调度
            return Buffer.DataType.NONE;
        }
        // fall through 到 synchronized 外
    }
    // 非恢复期：caller 预算好的上游权威值，读参数不需要锁
    return nextDataTypeOnUpstream;
}
```

priority 处理**不在 helper 内**：caller（`pullPriorityFromSubpartitionView`）从 subpartitionView 拉出的 `BufferAndBacklog.getNextDataType()` 自带权威类型，跟 master 一致；helper 只负责「recovery 队列 vs upstream」这一件事。

**Local master path（`getBufferAndAvailability(BufferAndBacklog next)`）不通过 helper**：那里 `next.getNextDataType()` 是 subpartitionView 自己给的权威下一类型，最准。helper 只用于另外 2 处 Local 调用站点（priority pull 的 fallback、recovery buffer wrap）。

### 8.5 `RemoteInputChannel.peekNextDataType()`

```java
private Buffer.DataType peekNextDataType(Buffer.DataType nextDataTypeOnUpstream) {
    // Remote 复用 master 现有 receivedBuffers monitor（simplify_approach §3.3）：
    // recoveredQueue 与 receivedBuffers 共用同一把锁，所以一次 synchronized 就够。
    // 注意：Remote 不需要 Local 的「!allDelivered → NONE」分支，因为 §3.8 不变式保证
    //       in-recovery 期间 receivedBuffers 只允许有 priority/control，暴露 caller 算好的
    //       upstream 值（即 receivedBuffers.peek() 类型）是安全的。
    synchronized (receivedBuffers) {
        if (!recoveredQueue.isEmpty()) {
            return recoveredQueue.peek().getDataType();
        }
        // fall through 到 synchronized 外
    }
    // caller 预算的上游权威值（receivedBuffers.peek() 的 DataType），读参数不需要锁
    return nextDataTypeOnUpstream;
}
```

Remote **全部 3 处调用站点都通过 helper**，不需要 Local 的「master path 绕道」—— `receivedBuffers.peek()` 本身就权威，caller 把这个权威值通过 `nextDataTypeOnUpstream` 传进来。

### 8.6 Master 分支当前是怎么处理 nextDataType 的

这里「master 分支」指 Flink 上游 master 分支（即 38544 改动前的代码），用作对照基线。

#### 8.6.1 `master/LocalInputChannel`

**核心机制**：master 把 nextDataType 设计成「**`BufferAndBacklog` 自带的字段**」。每次从 subpartitionView 拉一个 buffer，subpartitionView 同时把「下一个 buffer 是什么类型」也一并塞进 `BufferAndBacklog` —— **下一类型跟随当前 buffer 一起被带出来**，是 view 给的权威值，不用调用方猜。本地 `toBeConsumedBuffers` 里的 `BufferAndBacklog` 也是同样的结构：每一项在入队时就已经把「下一项的类型」写进自己的 `nextDataType` 字段。

基于这个机制，master 的取值始终是「**从手里这个权威 `BufferAndBacklog` 直接读 `getNextDataType()`**」，只在特定边界做覆盖：

| 触发 | nextDataType 怎么取 | 是否带覆盖 |
|---|---|---|
| 拉 priority 事件（`hasPendingPriorityEvent` 路径） | 从 view 拉 priority `BufferAndBacklog`，**直接读它的 `getNextDataType()`**（view 已经在里面告诉你「priority 之后是什么」） | 若读出来仍是 priority → 不覆盖（继续 priority chain）；若读出来不是 priority 且**本地 `toBeConsumedBuffers` 非空 → 覆盖为本地队列头的 DataType**（"优先消费本地"语义）；若本地也空 → **保留** view 给的值 |
| 弹本地队列头 `BufferAndBacklog`（非 priority） | **直接读它的 `getNextDataType()`**（入队时已经写好） | dynamic upgrade：若是最后一项且 `nextDataType == NONE`、且 view 已经可用 → 覆盖为 `DATA_BUFFER` |
| 从 view 拉普通 `BufferAndBacklog`（无 priority） | **直接读它的 `getNextDataType()`** | 不覆盖 |
| 遇到 `FullyFilledBuffer`（split 路径） | split 出 N 个 partial，**每个 partial 的 nextDataType 都写成 `buffer.getDataType()`**（即 current FullyFilledBuffer 自己的 DataType，作为 split 的特化策略） | n/a（构造期一次性写定，消费时直接读） |

**一句话总结 master Local 的模式**：所有 nextDataType 都从「**手上这个权威 `BufferAndBacklog`**」直接读 `getNextDataType()`；priority 路径里如果本地队列还有 buffer，**本地的权威优先于 view 的权威**（这是「优先消费本地」的体现）。master 从不在消费时「peek 队列再算一遍」。

> 这一段是 helper 设计的对照基线：**本提案的 `nextDataTypeOnUpstream` 参数**就是把这个「权威自带」模式延续下来 —— caller（手里有权威 `BufferAndBacklog` 的那些站点）从 `next.getNextDataType()` 直接拿权威值传进 helper；caller（手里只有裸 `Buffer` 的 recovery 消费路径）才需要 helper 兜底「队列头 vs upstream 探测」。

#### 8.6.2 `master/RemoteInputChannel`

Master 上 `RemoteInputChannel` **没有任何 recovery 逻辑**。recovery 在独立的 `RecoveredInputChannel` 类里完成，转换后才变成 `RemoteInputChannel`，所以 `getNextBuffer` 进入时已经完全是非恢复期：

| 触发条件 | nextDataType 怎么算 |
|---|---|
| 任意 | `receivedBuffers.poll()` 后取 `receivedBuffers.peek().buffer.getDataType()`，空则 `NONE` |

只有一处计算，简单干净。

#### 8.6.3 为什么 38544 分支会复杂 6 倍

Master 把所有 recovery 复杂度都隔离在 `RecoveredInputChannel` 里，转换后再把缓冲迁移到 Local/Remote。本次 38544 改成 drain 直接往 Local/Remote 推 buffer（见 simplify_approach §2 的非协商不变式：upstream subscription 必须早于 drain 完成），nextDataType 的复杂度也跟着搬到了 Local/Remote 内部，于是出现了当前分支 6 处分散计算的局面。helper 抽取是把 master 时代「集中在 `RecoveredInputChannel`」的清晰度找回来。

### 8.7 `allRecoveredBuffersDelivered` × `recoveredQueue` 状态表

priority 处理由 caller 自己用 `BufferAndBacklog.getNextDataType()` 完成（不进 helper），所以 helper 自身只覆盖 3 种状态：

| 状态 | `allDelivered` | `recoveredQueue` | 阶段 | Local helper → 返回 | Remote helper → 返回 |
|---|---|---|---|---|---|
| S1 队列非空 | any | non-empty | 1 | `recoveredQueue.peek().getDataType()` | `recoveredQueue.peek().getDataType()` |
| S2 drain 未完 + 队列空 | false | empty | 1 | `NONE`（block 普通上游） | `nextDataTypeOnUpstream`（§3.8 不变式：in-recovery 时 receivedBuffers 只有 priority/control，安全） |
| S3 post-recovery + 队列空 | true | empty | 2 | `nextDataTypeOnUpstream` | `nextDataTypeOnUpstream` |

`nextDataTypeOnUpstream` 由 caller 算好传进来，**helper 不参与计算**，对应来源见 §8.3 的表。

关键不变式：

- **S1/S3 两边完全对齐**：这是 helper 抽取的主要收益 —— recovery 队列优先与 post-recovery 透传的判定规则两个 channel 完全一致
- **S2 两边不对称且都正确**：
  - Local 必须 `NONE` block —— `subpartitionView` 没法区分 priority 与 live data，暴露会泄露 live
  - Remote 不需要 block —— `receivedBuffers` 在 recovery 期只允许有 priority/control（§3.8 不变式），caller 传进来的 `receivedBuffers.peek()` 类型本身就是 priority 类型，安全暴露
- **S3 信息完整性取决于 caller**：caller 传进来什么权威值，helper 就透传什么。Local master path（caller 是 `getBufferAndAvailability`）传 view 的 `BufferAndBacklog.getNextDataType()` 是完整权威；Local `wrapRecoveredBufferAsAvailability` 传 view 有损探测（见 §8.8）；Remote 三处都传 `receivedBuffers.peek()` 真实类型

### 8.8 Local 端有损探测的处理

`LocalInputChannel.peekNextDataType()` 阶段 2 中探测 `subpartitionView` 的下一类型时，当前 `ResultSubpartitionView` 接口**没有 `peekNextBufferDataType()`**，只能通过 `getAvailabilityAndBacklog(true)` 探测并映射成 `DATA_BUFFER`/`NONE`。实际下一个 buffer 可能是普通 event（非 priority `CheckpointBarrier`、`EndOfPartitionEvent` 等），nextDataType 会报错。

**本提案的处理**：helper 内部保留这个有损探测，不修。彻底修复（给 `ResultSubpartitionView` 加 `peekNextBufferDataType()`）blast radius 太大 —— 接口扩展涉及 tiered storage 等多个实现，应该独立成另一议题。

helper 抽取本身的价值不依赖这条修复：5 处计算先收敛到 1 处，未来真要修 Local 端有损探测时只需要改 helper 一处。

### 8.9 调用站点替换

| 文件 | 站点 | 替换前 | 替换后 |
|---|---|---|---|
| Local | `getBufferAndAvailability` (L598-602) | `next.getNextDataType()` | **保留**（subpartitionView 权威值，比 helper 阶段 2 探测更准） |
| Local | `pullPriorityFromSubpartitionView` (L499-510) | 自己写的 if + synchronized 块 | 若 `next.getNextDataType().hasPriority()` 仍用其值；否则重置 `hasPendingPriorityEvent`，调 `peekNextDataType(next.getNextDataType())` |
| Local | `wrapRecoveredBufferAsAvailability` (L542-562) | 自己写的三段式 | 先算 view 探测值 `upstream`（lossy: `DATA_BUFFER`/`NONE`），再调 `peekNextDataType(upstream)` |
| Remote | `getNextBuffer` recovery 分支 (L357-364) | 三层嵌套三元 | `peekNextDataType(receivedBuffers.peek()?.buffer.getDataType() ?? NONE)` |
| Remote | `getNextBuffer` master 分支 (L391-394) | `receivedBuffers.peek() != null ? ... : NONE` | `peekNextDataType(receivedBuffers.peek()?.buffer.getDataType() ?? NONE)` |
| Remote | `pollReceivedBufferAsPriority` (L432-439) | 自己写的 if/else 三段 | `peekNextDataType(receivedBuffers.peek()?.buffer.getDataType() ?? NONE)` |

6 处中 5 处替换为 helper，1 处（Local master path）保留权威源。`nextDataTypeOnUpstream` 参数由每处 caller 用自己最权威的源算好再传入 —— 准确性跟 master 严格对齐：Local 站点 2 用 view 自带的 `BufferAndBacklog.getNextDataType()`（跟 master `getNextRecoveredBuffer` 同源），Local 站点 3 用 view 探测（master 同样有损），Remote 3 处都用 `receivedBuffers.peek()` 的真实 DataType。

### 8.10 与 RecoveredBufferQueue 组件的关系

两块改动**统一在一个 commit 内完成**（见顶部 Scope）：

- 改动 A（RecoveredBufferQueue）抽**数据 + 状态操作**到组件，触动字段
- 改动 B（`peekNextDataType()`）抽**读时计算**到方法，触动 `BufferAndAvailability` 构造

合并提交的原因：两者都触动相同的两个文件、相同的方法体（`getNextBuffer` / `pullPriorityFromSubpartitionView` / `wrapRecoveredBufferAsAvailability` 等），且改动 B 的 helper 内部要访问改动 A 抽出的 `recoveredQueue.peek()`。分开提交会产生中间状态（helper 仍在 peek 已被组件接管的字段），难审难回滚。

## 9. 不在本提案范围内的事

- recovery 路径 `NetworkActionsLogger.traceInput` 调用的 3 处重复 —— 纯 debug 日志，不影响业务逻辑也不引入 bug，单独清理收益小，跳过
- `ResultSubpartitionView.peekNextBufferDataType()` 接口扩展（修 Local 端有损探测时才需要，见 §8.8）
- `RecoveredInputChannel` 是否也实现 `RecoveredBufferQueue`（master 上它已有自己的逻辑，且即将被 Local/Remote 的迁移路径替代，不在本次重构范围）
