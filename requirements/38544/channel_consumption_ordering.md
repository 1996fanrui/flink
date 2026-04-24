# Channel Consumption — 多数据源消费目标详述

> 本文档属于 **目标（Goal）** 文档，不是硬性 Requirement。对应 `user_requirements.md` 中的 Goals 段：GOAL-RSCC / GOAL-PEVT / GOAL-NBUP / GOAL-DLLO。
>
> 本文档当前重点展开 **GOAL-RSCC（RecoveredBufferStore 数据消费完整性）**——因为它涉及 NONE 契约、notify 链路、不变式清单等需要显式写清楚的机制细节。其余三条目标（GOAL-PEVT 优先级、GOAL-NBUP 非阻塞投递、GOAL-DLLO 延迟优化）在 §6 作简短定位；需要展开时再补细节。

## 1. 背景：为什么这条 goal 需要单独显式写

引入 `RecoveredBufferStore` 后，Local/RemoteInputChannel 的数据来源从单一"receivedBuffers / subpartitionView"扩展为"store + receivedBuffers / subpartitionView"。其中 store 自身又分为：

- **ready 队列**：已经装进 `Buffer` 的就绪数据，`tryTake()` 可直接取
- **磁盘 pending entries**：已写到 spill 文件、但尚未被 OutputWriter drain 线程加载回 `Buffer` 的数据，消费者看不到直接句柄

`store.addBuffer()` 是这两部分之间的唯一桥：drain 线程每搬一条磁盘 entry 回内存，就调用一次 `addBuffer` 把新 buffer 塞进 ready 队列。**一条磁盘数据被消费的完整链路是 `OutputWriter.drain → store.addBuffer → Task.getNextBuffer → store.tryTake`**。任何一环断掉，都意味着数据丢失或消费者永久 idle。

本目标的意义就是：**显式声明并守住这条链路的完整性**。

## 2. `peekNextDataType` = NONE 是正确契约，不是 gap

曾有讨论质疑："ready 队列空但磁盘还有 pending 时 `peekNextDataType` 返回 NONE，task 线程被 InputGate 从 available 队列移除、进入 idle——是否需要改成返回一个非 NONE 值来避免 idle？"

**结论：不能改，当前 NONE 是唯一正确选择。** 推导如下：

`BufferAndAvailability.nextDataType` 是给 InputGate 的一个**承诺**：告诉 gate"下一次被 poll 时我会吐出这种类型的数据"，gate 据此决定是否把 channel 留在 available 队列里继续轮询。

若谎报非 NONE（比如 `DATA_BUFFER`）而此刻 ready 队列实际为空：

1. Gate 看到 `moreAvailable=true`，channel 继续留在 available 队列
2. Task 线程循环调 `getNextBuffer()`
3. `isEmpty()` 返回 false（pending count > 0），`tryTake()` 返回 null（ready 队列空）
4. 代码 fallthrough 到 `receivedBuffers.poll()` / subpartitionView，也可能为空
5. `getNextBuffer()` 返回 `Optional.empty()`，但 gate 仍认为 channel available
6. Task 线程**立刻再次 poll**，回到 3——**busy-spin，烧 CPU**

因此 NONE 精确表达"此刻无可立即交付的下一条"，gate 据此把 channel 从 available 队列移除，Task 线程让出去处理别的 channel；等磁盘 load 真正完成后由 `addBuffer` 触发的 notify 把 channel 重新加回 available 队列。这是**正确的契约用法**，每次 disk load 一次唤醒往返是可接受的延迟成本——属于 GOAL-RSCC 范围外的 future optimization，不作为 gap 记录。

## 3. 为守住目标必须成立的不变式

"一条也不漏"实质上依赖于以下四条不变式。本 branch 不要求新增机制来证明它们，但任何未来改动必须同时复核这四条：

### 3.1 addBuffer 在"ready 队列空→非空"边界必须可靠触发 notify

`RecoveredBufferStoreImpl.addBuffer(buffer)` 的实现必须保证：当调用发生时若 ready 队列原本为空，则在入队后触发一次 `dataAvailableCallback`（典型映射到 `notifyChannelNonEmpty`）。这一步不能漏，否则 Task 线程永远不会被叫醒来消费这条 buffer。

关注点：
- "队列是否原本为空"的检查与入队操作必须在同一把锁内（或者通过原子操作），避免 drain 线程 A 看到"非空"而实际上 Task 线程 B 刚好 `tryTake` 掉了最后一条
- 回调触发应在锁外发生（避免与回调接收方互相持锁形成死锁），但读取"是否需要触发"的决策必须在锁内

### 3.2 setDataAvailableCallback 在 channel conversion 时的切换不能遗漏

`RecoveredInputChannel` → Local/RemoteInputChannel 转换时，store 引用移交，回调从"recovered channel 的通知方法"切换到"physical channel 的通知方法"（`RemoteInputChannel.java:173` 是后者的连接点）。切换瞬间若有 `addBuffer` 并发发生，`setDataAvailableCallback` 的 synchronized 语义（`interfaces.md:171`）必须保证：

- 要么 addBuffer 看到旧回调并触发（旧 channel 必须在释放前消费或转交未处理的通知）
- 要么 addBuffer 看到新回调并触发（新 channel 接收）
- 绝不能"addBuffer 执行时恰好回调为 null / 处于中间状态、通知被吞掉"

### 3.3 markComplete 前已入队的所有 buffer 必须已触发过 notify

`markComplete()` 表示 drain 结束、store 再也不会新增 buffer。若在 markComplete 之前有某条 `addBuffer` 未能触发 notify（违反 3.1 或 3.2），这条 buffer 就会永久停留在 ready 队列——Task 线程既没有被唤醒、也不会主动再来 poll（nextDataType=NONE 已让 gate 把 channel 摘掉）。

推论：OutputWriter drain 循环的终止条件必须是"磁盘 pending 清零 AND 所有 addBuffer 已完成通知"，而不是"磁盘 pending 清零"就立刻 markComplete。

### 3.4 Task 线程对 ready 队列的可见性

`tryTake` 和 `addBuffer` 跨线程，必须保证 Task 线程能看到 drain 线程刚入队的 buffer。`RecoveredBufferStoreImpl` 的内部容器选择（synchronized queue / concurrent queue / 带显式锁的结构）要保证 happens-before：drain 线程 `addBuffer` 返回 → Task 线程后续 `tryTake` 可见。

## 4. 已识别的风险点（需在后续 review 中逐项验证，不在本文档内修）

上述不变式是否在当前 `RecoveredBufferStoreImpl`、`OutputWriterImpl`、`LocalInputChannel`、`RemoteInputChannel` 中真的守住，需要独立验证。本文档只声明**这些是必须守住的点**，具体验证走 `code_review_summary.md` 或独立 review 流程：

1. `addBuffer` 的"空→非空"判定与入队是否在同一临界区内
2. `setDataAvailableCallback` 与 `addBuffer` 的锁序是否一致，回调是否在锁内 vs 锁外调用
3. channel conversion（`toInputChannel`）与 drain 线程 `addBuffer` 的并发窗口
4. `markComplete` 与最后一批 `addBuffer` 的 happens-before 关系
5. 释放路径（`releaseAll`）与 drain 线程在途 `addBuffer` 的互斥

## 5. 验收思路

当需要正式验证 GOAL-RSCC 时，建议：

1. **单元测试（drain 完整性）**：构造一个 store + OutputWriter，预置 N 条磁盘 entries；模拟 drain 线程按不同节奏调 `addBuffer`；Task 线程通过 `tryTake` 消费；断言最终消费数 = N，没有任何 buffer 被遗漏
2. **并发测试（race 探测）**：在 `addBuffer` 与 `tryTake` 之间插入人工 latency / interleaving；断言无论交错顺序如何，总消费数守恒
3. **channel conversion 测试**：在 drain 进行中触发 `toInputChannel` 切换回调，断言切换前/切换中/切换后的 `addBuffer` 都被正确 notify
4. **markComplete 次序测试**：覆盖"最后一条 addBuffer 尚未触发 notify 即调用 markComplete"的边界，断言该条 buffer 仍被消费

## 6. 其余三条目标的定位

§2–§5 围绕 GOAL-RSCC 展开。这里简述剩余三条目标的语义坐标，便于后续需要展开时在本文档内补充：

### GOAL-PEVT Priority 事件优先消费

期望顺序：priority → recovered store → 普通数据。当前 `getNextBuffer()` 在 store 与 `receivedBuffers` 之间没有 priority 判优，recovery 期间到达的 priority 事件会被 store 数据阻塞。真正实现该目标需要调整 `getNextBuffer()` / `peekNextBufferSubpartitionIdInternal()` 的数据源判优逻辑，使之先检查 priority 元素存在性再决定从哪条数据源取；同时 `peekNextDataType()` 在交界处的返回值要同时考虑 priority / store / subpartitionView 三侧，避免 `BufferAndAvailability.nextDataType` 在交界错误。

### GOAL-NBUP 上游投递路径不阻塞

对三条数据源分别成立：priority 事件绕过 credit，恢复期间仍可 `addPriorityElement` 入 `receivedBuffers`；普通数据只要有 credit 就能入 `receivedBuffers`；store 的 `addBuffer` 路径独立于 network I/O。当前代码通过 `onSenderBacklog` 在 recovery 期间门控普通数据的 credit，这是一种可选策略，不是硬性约束——真正的约束是"顺序正确 + 不死锁"。

### GOAL-DLLO Disk load 延迟优化

GOAL-RSCC 的 "NONE + notifyChannelNonEmpty" 路径每次 disk load 都需要一次 Task 线程的唤醒往返。优化方向可包括：store 内部预取（drain 与消费 pipeline）、批量 notify、或在契约保持正确的前提下让 gate 知晓"磁盘仍 pending"以减少 idle 往返。该目标与 GOAL-RSCC 正交——正确性不依赖该优化。

## 7. 与 design.md 的关系

`design.md` 第 2 节声明"不修改优先事件处理、不修改 Task Thread 消费逻辑"。GOAL-RSCC 完全落在该范围内（只是把 store 引入后的完整性契约显式写清楚）。GOAL-PEVT 的实现若触及 `getNextBuffer()` 的数据源判优顺序，则需要与 design.md 的范围声明重新对齐。本文档只梳理目标语义，不决定哪条目标由哪个 PR / branch 交付。
