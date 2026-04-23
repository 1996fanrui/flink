# Spill Reader 并发消费：两条链路各自独立 Reader，checkpoint 做独立 snapshot

## 两条消费链路

Spill 文件里的 entries 会被**两条独立的链路**各自消费/读取，两条链路语义完全不同，不应互相影响：

| 链路 | 触发时机 | 持有的 Reader | 消费性质 |
|---|---|---|---|
| **Input channel 回放链路** | 回放 buffer 腾出时 (`eagerDrain`) / dispatcher 收尾 (`close()` drain) | Writer 创建的**原 Reader**（`writer.getReaders()`） | **最终处置**：逐个 `readNext()`，每个 entry 的字节读进 network buffer 投给 `RecoveredBufferStore` |
| **Checkpoint 链路** | `onChannelCheckpointStarted` 在 wait-set 收敛时触发 `addInputDataFromSpill` | 对每个原 Reader 调 `snapshot()` 生成一组**独立 Reader 对象**（新 FileChannel + 独立 entries Deque + 预 sealed），交给异步 `DrainChunkIterator` | **拍照备份**：snapshot 消费者逐个 `readNext()`，字节写入 checkpoint 输出流 |

**关键：两条链路的 Reader 对象完全不共享。** 原 Reader 归回放链路独占；snapshot Reader 归 checkpoint drain 独占。两者各有自己的 `FileChannel`、各有自己的 `Deque<Entry>`、各有自己的内部 buffer。磁盘上的字节是同一份（page cache 共享），但在 Java 堆里是两套独立对象。

两条链路共用的唯一东西是 `Entry` 对象引用本身 — `Entry` 是不可变 metadata，共享安全。

---

## 关键前提

1. **Checkpoint 触发时 `addEntry` 已经不再发生**。Recovery 阶段（`filterAndRewrite`）在 `flush()` 之前就已经把所有 spill 数据写完；`onChannelCheckpointStarted` 由各 input channel 的 `checkpoint()` 回调触发，时间上在 recovery 结束之后。所以 checkpoint 拍照时原 `Reader.entries` 内容已经定格（只会被回放链路 `pollFirst` 缩小，不会再 `addLast`）。— 这个前提由 **Reader 的 `sealed` 状态**显式保障，见下一节。

2. **Checkpoint 可能发生多次**。每次 checkpoint 都要对"当前还没被回放掉的 entries"独立拍一张照；下一次 checkpoint 来时，原 Reader.entries 可能已经被回放链路消费掉了一部分，新的 snapshot 里只剩余量。每次 snapshot 之间互相独立，各自是一个独立的 Reader 对象。

3. **回放链路不会因为 checkpoint 发生而停顿**。checkpoint 把异步任务（以及它自己的 snapshot Reader）提交给 writer executor，recovery thread 继续在**原 Reader** 上做它的事。

---

## Sealed 状态：写完的显式标志

之前"Reader.entries 内容已经定格"这个前提完全依赖**时序暗示**（recovery → flush → checkpoint/close drain 的严格先后）。时序契约容易被后续改动意外破坏——比如谁在 recovery 结束后又补一笔 `addEntry` 就会静默损坏 checkpoint。

引入 `Reader.sealed` 把这个不变式变成**代码层面的显式状态**：

```java
public static class Reader implements Closeable {
    ...
    private volatile boolean sealed = false;    // 一次性 false → true

    /** Called when this file will no longer receive new entries. Idempotent. */
    public void seal() { sealed = true; }

    public boolean isSealed() { return sealed; }

    public void addEntry(Entry e) {
        if (sealed) {
            throw new IllegalStateException(
                    "addEntry after seal: " + filePath);  // loud fail, not silent corruption
        }
        entries.addLast(e);
    }
    ...
}
```

### 两个 seal 触发点

| 触发点 | 调用方 | 标记哪个 Reader | 原因 |
|---|---|---|---|
| 文件达到 rotation 阈值 | `FilteredSpillFile.Writer.openNewFile()` 在 rotate（非首次 open）时 | 旧的 current reader（即将被新 reader 取代） | 旧 file 已不会再被写入 |
| 所有 spill 数据写完 | `FilteredBufferDispatcherImpl.flush()`（或 dispatcher 生命周期里等价位置） | 此时的最后一个 reader | recovery 写入阶段整体结束 |

合起来覆盖了**所有**"这个 Reader 不会再来新 entry"的时刻。任一 seal 发生之后，对应 Reader 的 entries 就不再增长，只会被回放链路 `pollFirst` 缩小。

### Sealed 的三个用处

1. **Fast-fail assertion**：`addEntry` 被误调用后立即抛 `IllegalStateException`，而不是把一个不该存在的 entry 加进容器导致下游（checkpoint / 回放）读到 post-recovery 的数据。把一类潜在的静默 bug 变成测试和 staging 能直接发现的 loud failure。

2. **回放链路的"本文件 drain 完"判断**：仅靠 `reader.entries.isEmpty()` 是不够的 — unsealed 的 reader entries 瞬时为空不等于"彻底 drain 完"，后续可能再来新 entry。正确的判空条件是 `isEmpty() && isSealed()`；满足这个才可以跳到下一个 reader。按当前时序回放链路实际只在 `flush()` 之后才跑，所以"在实践中"每次看到的 reader 都已 sealed，但把这个决策**显式化**比依赖时序稳妥得多 — 以后任何流程改动（比如引入"边 recover 边回放"）都不会悄悄破坏正确性。

3. **Checkpoint snapshot 的前置断言**：`addInputDataFromSpill` 触发点（`onChannelCheckpointStarted` wait-set 收敛时）可以加一条 `assert all readers sealed`，明确"进入 checkpoint 链路之前 recovery 写入阶段必须已经结束"。如果有谁把 checkpoint 触发时机提前到 recovery 未完成，断言会立刻触发，暴露设计变化的盲区。

---

## 完整 Reader API

每条消费链路各自持有自己的 Reader（原 Reader 或 snapshot Reader）。Reader 对外只暴露一个消费入口 `readNext()` — Entry 的 offset/length 被 Reader 内部消化，外部完全不接触。

```java
public static class Reader implements Closeable {

    private final FileChannel channel;                       // 本 Reader 独占
    private final Path filePath;                             // 不可变；snapshot 要用它重开新 channel
    private final Deque<Entry> entries = new ArrayDeque<>(); // 本 Reader 独占的 entry 队列
    private volatile boolean sealed = false;                 // 写入阶段是否结束
    private byte[] buf;                                      // lazy；readNext 之间复用

    public Reader(Path filePath) throws IOException {
        this.filePath = filePath;
        this.channel = FileChannel.open(filePath, StandardOpenOption.READ);
    }

    // ---- Write side（recovery thread 写入 spill 数据时）----

    public void addEntry(Entry e) {
        if (sealed) {
            throw new IllegalStateException("addEntry after seal: " + filePath);
        }
        entries.addLast(e);
    }

    public void seal() { sealed = true; }
    public boolean isSealed() { return sealed; }

    // ---- Consume side（回放链路 or checkpoint drain 消费 entries 时）----

    public boolean hasEntries() { return !entries.isEmpty(); }

    /**
     * Consume 下一个 pending entry：内部 {@code entries.pollFirst()} + positional read 到
     * 内部复用 {@link #buf}，返回一个 {@link Chunk}（channelInfo + 字节范围）。
     * 没有更多 entry 时返回 null。
     *
     * <p>返回的 Chunk 的 {@code getData()} 是 Reader 内部复用的 byte[]，下一次 {@code
     * readNext()} 调用会覆盖它；调用方必须在下次 readNext 之前把当前 Chunk 的字节消费完。
     */
    public Chunk readNext() throws IOException {
        Entry entry = entries.pollFirst();
        if (entry == null) {
            return null;
        }
        if (buf == null || buf.length < entry.getLength()) {
            buf = new byte[entry.getLength()];
        }
        ByteBuffer bb = ByteBuffer.wrap(buf, 0, entry.getLength());
        long position = entry.getOffset();
        while (bb.hasRemaining()) {
            int n = channel.read(bb, position);
            if (n < 0) {
                throw new IOException(
                        "Truncated spill file: " + entry.getLength()
                                + " bytes @" + entry.getOffset() + " in " + filePath);
            }
            position += n;
        }
        return new Chunk(entry.getChannelInfo(), buf, entry.getLength());
    }

    // ---- Snapshot（checkpoint 链路跨线程交接点）----

    /**
     * Returns an independent Reader over the same file：
     *   - 自己的 {@link FileChannel}（新 {@code FileChannel.open}）
     *   - entries 的浅拷贝（Entry 不可变，引用共享安全）
     *   - 预置 {@code sealed = true}（snapshot 是定格视图，不再接受 addEntry）
     *   - 自己独立的 internal buf
     *
     * 调用方（dispatcher）必须在它的 synchronized 块里调用，保证 {@code entries.addAll}
     * 与 recovery thread 的 addEntry / readNext 的 pollFirst 不交错。
     *
     * 调用方拥有返回的 Reader，drain 完（或失败）必须 {@link #close}。
     */
    public Reader snapshot() throws IOException {
        Reader snap = new Reader(filePath);
        snap.entries.addAll(this.entries);
        snap.sealed = true;
        return snap;
    }

    // ---- Lifecycle ----

    @Override public void close() throws IOException { channel.close(); }
}
```

**重点：`read(offset, buf, len)` 不对外暴露。** Entry 的 offset/length 是 Reader 内部实现细节 — 外部调 `readNext()` 直接拿到 Chunk。Entry 只在 Writer → Reader `addEntry(e)` 一路是外部可见的，消费侧完全看不见。

### 为什么是全新 Reader，不是"同一个 Reader + 独占 entries 副本"

- 两条链路跑在不同线程：回放链路在 recovery thread，checkpoint drain 在 `ChannelStateWriter` 的 executor 线程。
- 如果共享同一个 Reader 对象，两个线程都会调 `reader.readNext()`（内部改 `entries` + 读/写 `buf`），就不得不加锁或依赖 `FileChannel` 的 positional read 多线程安全保证作为兜底。兜底存在但**脆弱**：未来给 Reader 加任何可变字段（cache / cursor / decompression state）就破了。
- 每条链路各自拥有一个 Reader 对象，消除"共享资源"这件事。**Reader 的生命周期清晰成一条线**：创建 → 被一个消费者独占消费 → close。将来 Reader 内部加任何状态都不会影响并发正确性。
- 代价：`FileChannel.open` 每次要走一次 syscall。Checkpoint 频率低（分钟级），每次 N ≈ 文件数 量级的 syscalls，与 checkpoint 的 I/O 总量相比忽略不计。

### `close()` 生命周期

- 原 Reader 由 `FilteredSpillFile.Writer` 拥有，在 `writer.close()` 时连锁关闭。
- Snapshot Reader 由 checkpoint drain 的 `CloseableIterator` 拥有，iterator 的 `close()` 里 close 所有 snapshot readers（无论 drain 成功还是失败）。

---

## 为什么 snapshot 是 copy，不是 swap / move

**Swap（错误）**：如果 checkpoint 把原 Reader 的 entries 整个拿走（swap 成新空 deque），回放链路就读不到这些 entries 了 — 这些 in-flight 数据就丢了，不会变成 network buffer 投给 input channel。破坏了回放的正确性。

**Copy（正确）**：snapshot 里的 `snap.entries.addAll(this.entries)` 是**浅拷贝** Entry 引用；原 Reader.entries 原封不动，回放链路继续在原 Reader 上 `readNext()`（内部 `entries.pollFirst()` + 从自己的 FileChannel 读字节）。两条链路操作各自独立的 deque，但都指向同一批不可变 Entry 对象。

**为什么是浅拷贝**：`Entry` 是不可变 metadata（所有字段 final，无 setter），多个持有者共享同一个 Entry 引用是安全的 — 任何一方都改不动它。深拷贝（`new Entry(...)` × N 次）没有任何收益，纯粹是 GC 负担。

**Copy 的开销**：只是把 N 个 Entry 引用复制到新 ArrayDeque，O(N) 指针拷贝，不涉及字节。真正的字节读取才是大头，两条链路各自独立做。

---

## 同一段字节会被读两次吗？

**会 — 而且这是正确行为。**

假设某个 entry `e` 的字节范围是 `[off, off+len)`：

- 回放链路：原 Reader 的 `readNext()` 内部走自己的 FileChannel 读 `[off, off+len)` 进内部 buffer，返回 Chunk 给 dispatcher。字节最终进 network buffer 给 input channel。
- Checkpoint 链路：对应 snapshot Reader 的 `readNext()` 内部走自己的 FileChannel 读 `[off, off+len)` 进内部 buffer，返回 Chunk 给 `DrainChunkIterator`。字节最终写进 checkpoint 输出流。

是两个独立的 FileChannel 各读一次同一段磁盘 — 看起来像浪费，但：

1. 两者写入目的地完全不同（network buffer vs checkpoint 文件），不可合并。
2. 时间上两者异步，如果合并就要加协调，反而复杂。
3. Linux 在 OS 层走 page cache — 两个 FD 指向同一 inode 共享同一份 page cache，第二次读大概率命中 cache，不会真的打两次盘。

---

## dispatcher 侧调用

```java
// FilteredBufferDispatcherImpl.drainSpillEntriesToCheckpoint
synchronized (this) {
    assert writer.getReaders().stream().allMatch(FilteredSpillFile.Reader::isSealed)
            : "checkpoint drain requires all readers sealed";
    List<FilteredSpillFile.Reader> snapshots = new ArrayList<>();
    try {
        for (FilteredSpillFile.Reader r : writer.getReaders()) {
            snapshots.add(r.snapshot());   // open 新 FileChannel + copy entries
        }
    } catch (IOException e) {
        // open 失败时回滚：关掉已经创建的 snapshot readers
        for (FilteredSpillFile.Reader s : snapshots) {
            try { s.close(); } catch (IOException ignored) {}
        }
        throw new UncheckedIOException(e);
    }
    channelStateWriter.addInputDataFromSpill(
            checkpointId,
            new DrainChunkIterator(snapshots));   // iterator owns + closes them
}
```

- 同步块保证：snapshot 构造期间回放链路的 `readNext` / `addEntry` 不会和 `snapshot()` 内部的 `entries.addAll` 交错。
- Iterator 拿到的是完全独立的一组 Reader，跟 recovery thread 持有的原 Reader 物理上分离 — iterator 只调每个 snapshot reader 的 `readNext()`，与原 Reader 无任何耦合。
- 下一次 checkpoint 再调 `snapshot()`，看到的是"到那一刻为止、还没被回放消费掉的" entries，各自独立成一组新的 snapshot readers。

---

## 时序图

```
recovery thread                         writer-executor thread
      │                                        │
      ▼                                        │
filterAndRewrite                               │
  writer.write(...)                            │
    → 可能触发 rotation:                       │
        current reader.seal()  ◄───── rotation 触发的 seal
        writer.openNewFile()                   │
    → reader.addEntry(e)                       │
      │                                        │
      ▼                                        │
flush()                                        │
  writer.getCurrentReader().seal()  ◄───── 最后一个 reader 被 seal
  │ (自此所有原 reader 都 sealed)              │
      │                                        │
      │     (checkpoint #1 到达)                │
      ▼                                        │
onChannelCheckpointStarted(c1) 收敛            │
  synchronized(this) {                         │
    assert all readers sealed                  │
    snapshots₁ = [r.snapshot() for r]          │
      ├─ open 新 FileChannel                   │
      ├─ entries.addAll(原 entries)            │
      └─ snap.sealed = true                    │
    addInputDataFromSpill(c1, iter(snaps₁)) ─► │
  }                                            │
      │                                        ▼
      │                       DrainChunkIterator.next()
      │                         chunk = snapReader.readNext()
      │                                (内部 pollFirst + positional read
      │                                 on its own FileChannel)
      │                         yield chunk
      │                       ...
      │                       DrainChunkIterator.close()
      │                         for s in snapshots₁: s.close()
      ▼                                        │
close() drain                                  │
  for r in original readers:                   │
    while r.hasEntries():                      │
      chunk = r.readNext()                     │
      // chunk.data / .length / .channelInfo   │
      buffer.write(chunk); store.add(buffer)   │
    // !r.hasEntries() && r.isSealed()         │
    //   ⇒ 跳下一个 reader                     │
      │                                        │
      │     (checkpoint #2 到达)                │
      ▼                                        │
onChannelCheckpointStarted(c2) 收敛            │
  synchronized(this) {                         │
    snapshots₂ = [r.snapshot() for r]          │
      ← 只拍到"还没被 close drain readNext 走"的 entries
    addInputDataFromSpill(c2, iter(snaps₂)) ─► │
  }                                            ▼
                                       ... 独立消费 snapshots₂ ...
```

Checkpoint #1 的 snapshots、checkpoint #2 的 snapshots、原 readers — 三组 Reader 对象物理上完全分离，各组由一个线程独占使用。三者都指向磁盘上相同的 spill 文件，但走三个独立的 FileChannel（page cache 共享）。

---

## 并发模型总结

| 资源 | 拥有者 | 并发访问 |
|---|---|---|
| 原 `Reader` (含 channel + entries + sealed + buf) | 生产：recovery thread 写（addEntry/seal）；消费：回放链路（recovery thread）`readNext()` | 单线程（recovery thread），无并发 |
| 每组 snapshot `Reader` (含独立 channel + entries 副本 + 自己的 buf) | 构造：recovery thread 在同步块里 `r.snapshot()`；消费+释放：writer executor 线程异步 `readNext()` / `close()` | 构造完成后交接给 executor 线程，之后由 executor 单线程操作 |
| `Reader.sealed` (`volatile boolean`) | 生产者写（rotation / flush）→ 消费者读（assertion / 回放判空） | 跨线程：volatile 保证可见；一次性 false → true 单调，幂等 |
| `Entry` 对象本身（metadata） | 不可变 | 共享安全 |
| 磁盘文件（page cache） | OS 管 | 多个 FileChannel 指向同一 inode 共享 page cache，OS 保证一致 |

**核心不变式**：任何一个 `Reader` 实例在**它被消费的阶段**只被一个线程访问；snapshot 构造是跨线程交接点，被 dispatcher 的 synchronized 块保护。

---

## 与 `spill_file_single_pass_read.md` 的关系

- `spill_file_single_pass_read.md`：定义了 writer 接口 `addInputDataFromSpill(CloseableIterator<Chunk>)`，解决 "一次 checkpoint 每个文件只顺序读一遍" 的 I/O 效率问题。
- 本文：定义了 iterator 背后 Reader 所有权在回放链路与 checkpoint 链路之间的切分（各自独立 Reader），以及 `sealed` 状态作为"写入阶段结束"的显式标志；共同解决"两条链路不共享任何 Reader 对象 + checkpoint 多次各自独立 snapshot + 写完信号不再依赖隐式时序"的并发正确性问题。

两者正交，共同构成完整的 spill drain 设计。
