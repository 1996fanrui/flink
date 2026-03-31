# InputChannelRecoveredStateHandler#recover Buffer Ownership 重新设计

## 通用设计 Review 检查清单

以下问题适用于任何"在已有代码上新增功能"的场景，不限于 buffer ownership。核心目标：**新代码必须融入已有设计模式，而不是绕过它再用补丁兜底。**

### 检查 1：新代码是否破坏了已有模式？

在动手之前，先回答：**原来的代码用的是什么模式？为什么那个模式是有效的？**

然后检查：**我的新代码是否仍然遵循这个模式？** 如果不遵循，必须有充分理由。

本次案例：原始代码用 `retain + finally 无条件 recycle` 模式，ownership 完全清晰。引入 filtering 时，我没有让新代码遵循同样的模式（对 sourceBuffer 做 retain），而是让 deserializer 直接"偷走"了原始 ref，导致 finally 里不得不用 `isRecycled()` 兜底。

### 检查 2：是否引入了"防御性补丁"？如果是，追问根因

防御性补丁的典型表现：
- `if (!xxx.isRecycled())` — 不确定谁负责回收
- `if (xxx != null)` — 不确定谁负责置空
- `try/catch` 吞掉异常 — 不确定异常是否会发生
- 额外的状态检查 — 不确定状态转换是否正确

**每一个防御性补丁都是一个设计缺陷的信号。** 正确的反应不是"加个检查保险一点"，而是问：**如果我的设计是正确的，我还需要这个检查吗？** 如果不需要，说明设计有问题，应该修设计而不是加补丁。

本次案例：`isRecycled()` 检查就是一个防御性补丁。正确的做法是让 filtering 路径也用 retain 模式，这样 finally 里就可以无条件 recycle，根本不需要 `isRecycled()` 检查。

### 检查 3：新代码路径与已有路径是否对称？

当一个方法有多条路径（如 if/else 分支）时，检查：**各路径是否遵循相同的 ownership/资源管理模式？** 如果不对称，问为什么。

不对称通常意味着某条路径偷懒或设计不一致。如果确实需要不对称，必须能清晰解释原因。

本次案例：non-filtering 路径用 `buffer.retainBuffer()` 传给 channel；filtering 路径直接传原始 buffer 给 deserializer。两条路径的 ownership 模式完全不同，这就是问题的根源。修复后两条路径都用 inline retain，对称且清晰。

### 检查 4：在提出方案前，是否对比了修改前的代码？

**先 diff，再设计。** 看看原来的代码怎么做的、为什么那样做，再决定新代码怎么融入。如果新方案比原来更复杂，必须解释复杂度的来源——是问题本身更复杂了，还是方案设计有问题？

本次案例：原始 recover() 只有 15 行、没有 `isRecycled()` 检查、finally 无条件 recycle。修改后变成需要注释解释 5 种 buffer 生命周期路径。复杂度爆炸本身就是一个信号。

## Buffer Ownership 核心原则

### 原则 1：Ownership 转移 = 新 owner 必须有 try/finally 保护

> If passing the ownership to others, the new owner must have a catch/finally block to recycle buffer.
> And original owner should not care about it.

任何时刻一个 buffer ref 只有一个 owner。当 ownership 转移后，原 owner **不再关心**这个 ref 的生命周期——不 recycle、不检查状态、不做任何兜底。新 owner 必须通过 try/finally 或 try/catch 保证回收。

正面例子：`RecoveredInputChannel#onRecoveredStateBuffer`。调用者传入 buffer 后完全不管，方法内部用 try/finally 确保一定回收（入队或直接 recycle）。

### 原则 2：`isRecycled()` 是 ownership 不清晰的信号

凡是需要判断 `isRecycled()` 来决定是否 recycle 的地方，都意味着代码不确定"这个 buffer 现在归谁管"。正确的设计中，每个 ref 的 owner 是确定的，owner 无条件负责回收，不需要窥探 buffer 的内部状态。

**诊断方法**：在代码中搜索 `isRecycled()`，每一处都是潜在的 ownership 问题。

### 原则 3：需要"一份数据给两个 owner"时，用 retain 分裂出独立 ref

当一个方法需要把 buffer 传给别人、同时自己还要在 finally 里无条件回收时，正确做法是 `retainBuffer()` 产生一个新 ref（ref count +1），把新 ref 交给别人，自己在 finally 里回收原始 ref。两个 owner 各管各的 ref，互不干扰。

```java
// 正确模式
try {
    channel.onRecoveredStateBuffer(buffer.retainBuffer());  // 新 ref 归 channel
} finally {
    buffer.recycleBuffer();  // 原始 ref 归自己，无条件回收
}
```

**反模式**：不做 retain，直接把原始 buffer 传给别人，然后在 finally 里用 `isRecycled()` 判断"别人是不是已经回收了"——这就是两个 owner 共享同一个 ref，ownership 不清晰。

### 原则 4：retain 和 ownership 转移必须在同一表达式完成（inline retain）

`retainBuffer()` 产生新 ref 后，到新 owner 接管之间不能有可能抛异常的代码，否则新 ref 泄漏。最安全的做法是 inline：

```java
// 正确：retain 和传递在同一表达式，无间隙
channel.onRecoveredStateBuffer(buffer.retainBuffer());
recoverWithFiltering(channel, info, buffer.retainBuffer());

// 错误：retain 和传递之间有间隙，someCall() 抛异常则 retainedBuffer 泄漏
Buffer retainedBuffer = buffer.retainBuffer();
someCall();  // ← 如果这里抛异常，retainedBuffer 没人回收
channel.onRecoveredStateBuffer(retainedBuffer);
```

### 原则 5：方法内部跟踪 ownership 转移点，用 boolean 标志而非 isRecycled()

当方法内部需要把 buffer 传给下一层（如 deserializer），在 catch 块中需要判断是否已转移。用显式 `boolean` 标志标记转移点：

```java
boolean ownershipTransferred = false;
try {
    deserializer.setNextBuffer(sourceBuffer);
    ownershipTransferred = true;  // 从此 sourceBuffer 归 deserializer 管
    // ...
} catch (Throwable t) {
    if (!ownershipTransferred) {
        sourceBuffer.recycleBuffer();  // 转移前失败，自己负责回收
    }
    throw t;
}
```

这表达的语义是"我是否已经完成了 ownership 转移"，而不是"buffer 是否碰巧已经被回收了"。Boolean 标志跟踪的是**代码执行到了哪一步**，`isRecycled()` 窥探的是**buffer 的内部状态**——前者是 ownership 逻辑，后者是实现细节。

### 原则 6：批量 buffer 传递时必须处理中途异常

当持有一个 `List<Buffer>` 逐个传递给下游时，如果中途抛异常，必须回收**剩余未传递**的 buffer。用索引跟踪进度：

```java
int i = 0;
try {
    for (; i < buffers.size(); i++) {
        channel.onRecoveredStateBuffer(buffers.get(i));  // 已传递的归 channel
    }
} catch (Throwable t) {
    for (int j = i; j < buffers.size(); j++) {
        buffers.get(j).recycleBuffer();  // 未传递的自己回收
    }
    throw t;
}
```

## 原始代码分析（分支改动前）

原始 `recover()` 的 ownership 非常清晰：

```java
// 原始代码 — ownership 清晰
try {
    if (buffer.readableBytes() > 0) {
        channel.onRecoveredStateBuffer(
                EventSerializer.toBuffer(descriptor, false));
        channel.onRecoveredStateBuffer(buffer.retainBuffer());
    }
} finally {
    buffer.recycleBuffer();  // 无条件回收，不需要 isRecycled 检查
}
```

- `recover()` 始终持有原始 ref（ref=1），finally 里**无条件** recycle
- `retainBuffer()` 产生新 ref（ref=2）交给 channel
- channel 通过 `onRecoveredStateBuffer` 的 try/finally 负责那个 ref
- 两个 owner 各管各的 ref，互不干扰
- 无论 `retainBuffer()` 之前或之后抛异常，finally 都能正确回收原始 ref
- **没有任何 `isRecycled()` 检查**

## 当前代码问题（引入 filtering 后）

引入 filtering 路径后，`recover()` 变成了：

```java
try {
    if (buffer.readableBytes() > 0) {
        if (filteringHandler != null) {
            recoverWithFiltering(channel, channelInfo, oldSubtaskIndex, buffer);
        } else {
            channel.onRecoveredStateBuffer(buffer.retainBuffer());
        }
    }
} finally {
    if (!buffer.isRecycled()) {   // ← 出现 isRecycled 检查 = ownership 不清晰
        buffer.recycleBuffer();
    }
}
```

### 问题根因

`filterAndRewrite` 内部调用 `vc.setNextBuffer(sourceBuffer)` 把 buffer 直接交给 deserializer（不做 retain），deserializer 消费后自动回收。这导致 recover 的 finally 里原来无条件的 `recycleBuffer()` 会 double-recycle，于是加了 `isRecycled()` 检查来"补救"。

这破坏了原始代码清晰的 ownership 模型：
1. `recover()` 原本是 buffer 的唯一 owner（负责 recycle 原始 ref）
2. filtering 路径让 deserializer 偷走了 buffer 的 ownership，但 recover 不知道
3. 不得不用 `isRecycled()` 来"窥探"内部状态，判断 buffer 是否已被别人回收

### 次要问题：resultBuffers 泄漏

`recoverWithFiltering` 中如果 `onRecoveredStateBuffer` 处理到一半抛异常，剩余 filteredBuffers 会泄漏。

## 设计方案

### 核心思路

保持原始代码 `retain + finally 无条件 recycle` 的模式。filtering 路径也对 sourceBuffer 做 retain，让 recover 的 finally 始终能无条件回收自己的 ref，消除所有 `isRecycled()` 检查。

### recover() 方法

两条路径在同一个地方做 `retainBuffer()` 并传给下游，对称且清晰。

```java
@Override
public void recover(
        InputChannelInfo channelInfo,
        int oldSubtaskIndex,
        BufferWithContext<Buffer> bufferWithContext)
        throws IOException, InterruptedException {
    Buffer buffer = bufferWithContext.context;
    try {
        if (buffer.readableBytes() > 0) {
            RecoveredInputChannel channel = getMappedChannels(channelInfo);
            if (filteringHandler != null) {
                // retain inline: retain 和 ownership 转移在同一表达式完成，无间隙
                recoverWithFiltering(
                        channel, channelInfo, oldSubtaskIndex, buffer.retainBuffer());
            } else {
                channel.onRecoveredStateBuffer(
                        EventSerializer.toBuffer(
                                new SubtaskConnectionDescriptor(
                                        oldSubtaskIndex, channelInfo.getInputChannelIdx()),
                                false));
                // retain inline: 同上
                channel.onRecoveredStateBuffer(buffer.retainBuffer());
            }
        }
    } finally {
        buffer.recycleBuffer();  // 无条件回收原始 ref，和原始代码一样
    }
}
```

**和原始代码完全一致的 finally 模式，没有 `isRecycled()` 检查。**

> 注意：retain 不能提取到 if/else 之前做统一 `Buffer retainedBuffer = buffer.retainBuffer()`。
> 因为 non-filtering 路径在 `onRecoveredStateBuffer(retainedBuffer)` 之前还有 `EventSerializer.toBuffer()` 调用，
> 如果该调用抛异常，retainedBuffer 就泄漏了。inline retain 保证 retain 和 ownership 转移在同一表达式完成，没有间隙。

### recoverWithFiltering() 方法

接收的 `retainedBuffer` 已是 retained ref，本方法是其唯一 owner。

```java
/**
 * Takes ownership of {@code retainedBuffer}. Caller must not access it after this call.
 */
private void recoverWithFiltering(
        RecoveredInputChannel channel,
        InputChannelInfo channelInfo,
        int oldSubtaskIndex,
        Buffer retainedBuffer)
        throws IOException, InterruptedException {
    // filterAndRewrite takes ownership of retainedBuffer
    List<Buffer> filteredBuffers =
            filteringHandler.filterAndRewrite(
                    channelInfo.getGateIdx(),
                    oldSubtaskIndex,
                    channelInfo.getInputChannelIdx(),
                    retainedBuffer,
                    channel::requestBufferBlocking);

    // Transfer ownership of each filtered buffer to channel.
    // On partial failure, recycle remaining buffers.
    int i = 0;
    try {
        for (; i < filteredBuffers.size(); i++) {
            channel.onRecoveredStateBuffer(filteredBuffers.get(i));
        }
    } catch (Throwable t) {
        for (int j = i; j < filteredBuffers.size(); j++) {
            filteredBuffers.get(j).recycleBuffer();
        }
        throw t;
    }
}
```

### filterAndRewrite() 方法

接收的 sourceBuffer 是 retained ref，本方法是其唯一 owner，必须保证回收。

用 `boolean sourceBufferOwnershipTransferred` 标志标记 `setNextBuffer` 的 ownership 转移点。这比 `isRecycled()` 更清晰——它表达的是"我是否已经把 ownership 交给了 deserializer"，而不是"buffer 是否碰巧已经被回收了"。

```java
/**
 * Filters a recovered buffer. Takes ownership of {@code sourceBuffer}.
 *
 * @return filtered buffers owned by the caller. Caller is responsible for recycling.
 */
List<Buffer> filterAndRewrite(
        int oldSubtaskIndex,
        int oldChannelIndex,
        Buffer sourceBuffer,
        BufferSupplier bufferSupplier)
        throws IOException, InterruptedException {

    SubtaskConnectionDescriptor key = ...;
    VirtualChannel<T> vc = virtualChannels.get(key);

    boolean sourceBufferOwnershipTransferred = false;
    List<Buffer> resultBuffers = new ArrayList<>();
    Buffer currentBuffer = null;
    try {
        vc.setNextBuffer(sourceBuffer);
        // After setNextBuffer, deserializer owns sourceBuffer and will recycle
        // it when consumed. From this point, we must NOT touch sourceBuffer.
        sourceBufferOwnershipTransferred = true;

        while (true) {
            DeserializationResult result = vc.getNextRecord(deserializationDelegate);
            if (result.isFullRecord()) {
                if (currentBuffer == null) {
                    currentBuffer = bufferSupplier.requestBufferBlocking();
                }
                currentBuffer = serializeElement(...);
            }
            if (result.isBufferConsumed()) {
                break;
            }
        }

        if (currentBuffer != null) {
            if (currentBuffer.readableBytes() > 0) {
                resultBuffers.add(currentBuffer);
                currentBuffer = null;
            } else {
                currentBuffer.recycleBuffer();
                currentBuffer = null;
            }
        }

        return resultBuffers;
    } catch (Throwable t) {
        // sourceBuffer: only recycle if ownership was NOT transferred to deserializer
        if (!sourceBufferOwnershipTransferred) {
            sourceBuffer.recycleBuffer();
        }
        if (currentBuffer != null) {
            currentBuffer.recycleBuffer();
        }
        for (Buffer buf : resultBuffers) {
            buf.recycleBuffer();
        }
        resultBuffers.clear();
        throw t;
    }
}
```

## Ownership 流转总结

```
recover()
├── buffer(ref=1)
│
├── [non-filtering]
│   ├── buffer.retainBuffer() inline → ref=2
│   └── onRecoveredStateBuffer(retainedRef)  ← channel owns retainedRef
│       └── try/finally: enqueue or recycle
│
├── [filtering]
│   ├── buffer.retainBuffer() inline → ref=2
│   └── recoverWithFiltering(retainedRef)  ← takes ownership
│       ├── filterAndRewrite(retainedRef)  ← takes ownership
│       │   ├── setNextBuffer(retainedRef) → deserializer owns retainedRef
│       │   │   (boolean flag marks transfer point)
│       │   ├── catch: recycle retainedRef(if !transferred) + currentBuffer + resultBuffers
│       │   └── return resultBuffers  ← caller owns these
│       ├── for each filteredBuffer:
│       │   └── onRecoveredStateBuffer(filteredBuffer)  ← channel owns it
│       │       └── try/finally: enqueue or recycle
│       └── catch: recycle remaining filteredBuffers
│
└── finally: buffer.recycleBuffer()  ← 无条件回收原始 ref，不检查 isRecycled()
```

**每一层 ownership 转移都有明确的保护机制，没有任何 `isRecycled()` 检查。**
