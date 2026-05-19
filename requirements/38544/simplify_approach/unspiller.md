# Unspiller 组件（async 线程侧）

> 范围：filter 开启时跑在 master 已有的 `channelIOExecutor` 上的全部新行为。功能关闭时这条线程沿用 master 原路径。

## 1. 职责

承担 recovery 的两个时段，前后串行，全部在 `channelIOExecutor` 单线程上：

- **filter 阶段**：从 state handle 读 → filter → 写盘（替代 master 的 heap 兜底）。
- **drain 阶段**：顺序读盘 → 申请物理 channel 的 buffer → 投递到 channel。

## 2. 内部组件

| 组件 | 职责 |
|---|---|
| `FilteredBufferWriter` | filter 阶段写盘前的累积。每 task 一个 `prefilterBuffer`（filter 读源数据用）+ 一个 `postfilterBuffer`（filter 输出累积），后者满即 flush 到 `SpillFile`。**取代 master 上 `RecoveredInputChannel.requestBufferBlocking` 里的 heap 兜底分支**，从源头消除堆增长。 |
| `SpillFile` | 实际落盘对象。多 segment：单文件超 64MB 即 rotate；每条 entry 携带 `(channelInfo, offset, length)` 元数据在内存的 `entries` 队列里。可 `snapshot()` 克隆 entries（不依赖 frozen 状态），用于 checkpoint 拍盘。 |
| `Unspiller` 主体 | drain loop + monitor + 推进 `(currentSegmentIndex, currentOffset)`。是面向 task 线程暴露公共接口的对象。 |

## 3. 公共类与接口

```java
public final class Unspiller implements Closeable {
    private final SpillFile spillFile;
    private final List<InputChannel> allChannels;                       // task 的全部 channel
    private final Map<InputChannelInfo, InputChannel> channelByInfo;    // 由 allChannels 派生
    private final Object monitor = new Object();                        // 全局锁

    // drain 进度，写在 monitor 内
    private int  currentSegmentIndex;
    private long currentOffset;

    /** allChannels 是该 task 的 InputChannel 全集（recovery 期间稳定）。
     *  drain 阶段从 channelByInfo 按 channelInfo 路由；checkpoint Step 1 直接迭代
     *  allChannels 给每个 channel 插 barrier，无需调用方再传。 */
    public Unspiller(SpillFile spillFile, List<InputChannel> allChannels);

    /** Sequentially drains every spill segment to its target channel.
     *  Called by channelIOExecutor after conversion completes. */
    public void drain() throws IOException, InterruptedException;

    /** Step 1 of the checkpoint protocol. See coordination.md. */
    public DiskSnapshot snapshotAndInsertBarriers();
}
```

`DiskSnapshot` 暴露给 `ChannelStateWriter`：

```java
public final class DiskSnapshot implements CloseableIterator<DiskSnapshot.Chunk> {
    // 内部：List<SpillFileSegment 副本> + (currentSegmentIndex, currentOffset)
    // 迭代时跳过 entryPos < startPos 的 entry（这些已进 channel）
    public static final class Chunk { InputChannelInfo channelInfo; byte[] data; int length; }
}
```

## 4. drain loop 形态

每个 entry 一次循环；申请 buffer 在 monitor 外 park，I/O + 投递 + 推进 offset 在 monitor 内一次性完成。

```
drain() {
  for (SpillFileSegment seg : spillFile.segments()) {
    while ((Entry e = seg.peekNextEntry()) != null) {
      InputChannel ch = channelByInfo.get(e.channelInfo);

      // (A) park 在 LocalBufferPool.getAvailableFuture —— 在 monitor 外
      Buffer buf = ch.requestBufferBlocking();

      // (B) 短临界段：I/O + 投递 + 推进 offset；三件事强绑定在同一把锁内
      synchronized (monitor) {
        seg.readBytesAt(e.offset, e.length, buf.getMemorySegment().asByteArray());
        ch.<add-buffer 入口>(buf);              // 由 input_channel.md 的最终方案决定
        seg.pollNextEntry();
        currentSegmentIndex = seg.segmentIndex;
        currentOffset = e.offset + e.length;
      }
    }
    seg.close();
  }
}
```

## 5. 内部不变量

- (A) buffer 申请 park 必须在 monitor 外，否则 buffer pool 阻塞会顺带阻塞 checkpoint Step 1。
- (B) 临界段内**不做任何 park 类操作**（盘读是同步 I/O，可接受）。
- (B) **三件事强耦合在同一临界段**：(读盘 → 投递 channel → 推进 offset) —— 这是 [`coordination.md`](./coordination.md) 第二条强原则的具体体现，缺一不可，否则 checkpoint snapshot 会出现「磁盘上 entry 已被 drain 但 offset 未推进」或反过来的不一致窗口。
- segment 全集迭代完后 drain 返回，由 `channelIOExecutor` 紧接着往每个物理 channel 投递 `EndOfInputChannelStateEvent`（同样在 monitor 内），完成 `stateConsumedFuture`。

## 6. 与 master 的复用 / 改动边界

复用：

- `channelIOExecutor` 本身（master 已有 single-thread executor）。
- `ChannelStateFilteringHandler.filterAndRewrite`（master 已有 filter 实现）。
- `RecoveredChannelStateHandler.recover` 的整体形态（filter 分支的去向从「channel.onRecoveredStateBuffer」改为「filteredBufferWriter.write」）。
- `LocalBufferPool.requestMemorySegmentBlocking` 的 `getAvailableFuture` park 机制。
- `RecoveredInputChannel.bufferFilteringCompleteFuture` / `stateConsumedFuture` 两个 future 衔接点。

改动：

- 删除 `RecoveredInputChannel.requestBufferBlocking` 的 heap 兜底（line 354-360 那段 `MemorySegmentFactory.allocateUnpooledSegment`）—— 整个项目要解决的就是这条路径的 OOM。
- 新增 `FilteredBufferWriter` / `SpillFile` / `Unspiller` / `DiskSnapshot` 四个类。
- filter 阶段 `bufferSupplier` 从 `channel::requestBufferBlocking` 切到 `prefilterBuffer` 复用源。
