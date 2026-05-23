# nextDataType 取值的四种情况对照

只列四种情况：master Local、master Remote、当前分支新 helper Local、当前分支新 helper Remote。每种情况说明：消费什么 buffer 之后报什么 nextDataType。

---

## 情况 1：master `LocalInputChannel`

核心：master 的 `BufferAndBacklog` 自带 `nextDataType` 字段（入队或 view 返回时就写好）。所有取值都是「从手上这个权威 `BufferAndBacklog` 直接读 `getNextDataType()`」，只在边界做覆盖。

| 进入路径 | 取 nextDataType 的算法 |
|---|---|
| `hasPendingPriorityEvent == true`（recovery 期 priority 中断） | 拉 view 的 `BufferAndBacklog next`，取 `next.getNextDataType()`（view 告诉你 priority 之后是什么）。若仍是 priority → **用 view 值不动**；若不是 priority 且 `toBeConsumedBuffers` 非空 → **覆盖为 `toBeConsumedBuffers.peek().buffer().getDataType()`**；若不是 priority 且队列空 → **保留 view 值** |
| `toBeConsumedBuffers` 非空（无 priority） | 弹出队首 `BufferAndBacklog`，取它**预置的** `getNextDataType()`（入队时写好的下一项类型）。dynamic upgrade：若是最后一项且 `nextDataType == NONE`、view 可用 → 覆盖为 `DATA_BUFFER` |
| `toBeConsumedBuffers` 空（master 主路径） | 拉 view，取 `next.getNextDataType()` 不动 |
| 遇到 `FullyFilledBuffer` | split 出 N 个 partial，每个 partial 的 `nextDataType` 写成 `buffer.getDataType()`（current FullyFilledBuffer 自己的类型） |

**消费顺序**：view priority → recovery 队列 → view 普通。

---

## 情况 2：master `RemoteInputChannel`

Master 的 `RemoteInputChannel` 没有 recovery 逻辑（recovery 在独立的 `RecoveredInputChannel`，转换后才变成 `RemoteInputChannel`），所以只有一条规则：

| 进入路径 | 取 nextDataType 的算法 |
|---|---|
| 任意 | `synchronized(receivedBuffers)` 内：`receivedBuffers.poll()` 后取 `receivedBuffers.peek().buffer.getDataType()`，空则 `NONE` |

`receivedBuffers` 是 `PrioritizedDeque`，priority 自动放头部，`peek()` 同时覆盖 priority 和 regular。

**消费顺序**：priority（自动在头）→ regular → NONE。

---

## 情况 3：当前分支 `LocalInputChannel.peekNextDataType(nextDataTypeOnUpstream)`

helper 签名：`peekNextDataType(Buffer.DataType nextDataTypeOnUpstream): Buffer.DataType`

**为什么要参数**：Local 的 priority 路径里，view 拉出的 `BufferAndBacklog` 自带权威 `getNextDataType()`。caller 把这个权威值（或站点 3 的 view 探测值）传进来，helper 不在内部猜测、也不调 view（view 没有便宜的 peek API）。

```java
private Buffer.DataType peekNextDataType(Buffer.DataType nextDataTypeOnUpstream) {
    synchronized (recoveredQueue) {
        if (!recoveredQueue.isEmpty()) {
            return recoveredQueue.peek().getDataType();
        }
        if (!allRecoveredBuffersDelivered) {
            // drain 仍在产 + 队列空 → block 普通上游（subpartitionView 可能有 live data，不能暴露）
            return Buffer.DataType.NONE;
        }
        // fall through 到 synchronized 外
    }
    return nextDataTypeOnUpstream;
}
```

**调用站点**：

| 站点 | nextDataType 怎么定 |
|---|---|
| `getBufferAndAvailability` (master 路径) | **不通过 helper**：直接 `next.getNextDataType()`（view 权威值；跟 master 情况 1 表第 3 行同源） |
| `pullPriorityFromSubpartitionView` | view 拉 priority 后：若 `next.getNextDataType().hasPriority()` → 用其值不动；否则 `hasPendingPriorityEvent = false`，调 `peekNextDataType(next.getNextDataType())`。**与 master 情况 1 表第 1 行严格一致**：master 是否覆盖到队列头取决于队列是否非空，helper 内的 `if (!recoveredQueue.isEmpty()) return queue head` 完成同一个语义 |
| `wrapRecoveredBufferAsAvailability` | 先算 view 探测值 `upstream = view.getAvailabilityAndBacklog(true).isAvailable() ? DATA_BUFFER : NONE`（有损），再调 `peekNextDataType(upstream)`。**与 master 情况 1 表第 2 行的 dynamic upgrade 对应**：master 在最后一项 + nextDataType=NONE 时探 view；helper 在 `allDelivered && queue 空` 时探 view |

**`nextDataTypeOnUpstream` 永远不会是 priority**：站点 1 不走 helper；站点 2 在 `hasPriority()` 时根本不调 helper（用 view 值不动）；站点 3 的 view 探测产物只能是 `DATA_BUFFER`/`NONE`。所以 helper 内部不需要 priority 检查。

**消费顺序**：view priority（caller 在 helper 外处理）→ recovery 队列 → drain 未完则 block → view 探测（lossy）。

---

## 情况 4：当前分支 `RemoteInputChannel.peekNextDataType()`

helper 签名：`peekNextDataType(): Buffer.DataType` —— **无参数**

**为什么不要参数**：Remote 的 `recoveredQueue` 与 `receivedBuffers` 共用同一把锁（simplify_approach §3.3），helper 在 `synchronized(receivedBuffers)` 内可以直接 peek `receivedBuffers`（拿真实 DataType，跟 master 情况 2 同模式）。引入参数反而让 caller 在外面再算一次 `receivedBuffers.peek()`，多此一举且让 priority 优先级失真。

```java
private Buffer.DataType peekNextDataType() {
    synchronized (receivedBuffers) {
        // (1) Priority 中断（PrioritizedDeque 把 priority 放在头）
        if (receivedBuffers.getNumPriorityElements() > 0) {
            return receivedBuffers.peek().buffer.getDataType();
        }
        // (2) Recovery 队列
        if (!recoveredQueue.isEmpty()) {
            return recoveredQueue.peek().getDataType();
        }
        // (3) 上游普通（drain 未完时 receivedBuffers 受 §3.8 不变式保证只能是 priority，
        //     此处只剩 regular 头或 null，安全返回）
        SequenceBuffer next = receivedBuffers.peek();
        return next != null ? next.buffer.getDataType() : Buffer.DataType.NONE;
    }
}
```

**调用站点**：

| 站点 | 调用 |
|---|---|
| `getNextBuffer` recovery 分支 | `peekNextDataType()` |
| `getNextBuffer` master 分支 | `peekNextDataType()` |
| `pollReceivedBufferAsPriority` | `peekNextDataType()` |

**消费顺序**：priority → recovery 队列 → regular 上游 → NONE。**与 master 情况 2 一致**（priority 通过 PrioritizedDeque 自动优先；只是中间多了一段 recovery 队列）。

---

## 一句话总结

- master Local（情况 1）：从手上 `BufferAndBacklog` 直接读权威 `nextDataType`，priority 路径里覆盖到 queue 头
- master Remote（情况 2）：`receivedBuffers.peek()` 一把搞定，priority 由 `PrioritizedDeque` 自动优先
- 新 Local（情况 3）：master 路径直接读 `BufferAndBacklog`；recovery 路径走 helper，参数承载 view 权威值或探测值
- 新 Remote（情况 4）：helper 内部直接 peek `receivedBuffers`，priority 优先于 recovery 队列；签名跟 master Remote 一样不带参数

四种情况的**消费顺序在两边的有 priority 时都是 priority 优先**，本提案严格对齐 master 这一不变式。
