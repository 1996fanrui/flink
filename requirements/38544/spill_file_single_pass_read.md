# Spill File Single-Pass Read at Checkpoint

## 难点（5 句话概览）

1. **粒度错配**：一个物理 spill 文件里顺序串联着来自多个 input channel 的 network buffer 数据（P2/P3 downgrade-only 语义要求严格按到达顺序落盘），但 Flink checkpoint 的上报接口 `ChannelStateWriter.addInputData(...)` 以 input channel 为天然维度组织上报，"一个文件"和"一次上报调用"在粒度上根本不对齐。
2. **多对多映射**：一个文件里混合着 N 个 channel 的数据，一个 channel 的数据又可能跨越多个物理文件（rotation 阈值切换），任何"按 channel 各自从文件拉取数据"的实现都会退化成对同一个文件在不同 offset 之间反复跳跃的**随机 IO**。
3. **触发时机的协调**：checkpoint 在 Flink 里由 input channel 逐个调用触发（register 模式），真正"可以开始一次性读整个文件"的时间点是**最后一个**持有 disk data 的 channel 完成注册之后；在此之前启动读盘会漏 entry，之后才读盘又必须和 task 线程上"正在消费 ready buffer"的流程协调好先后。
4. **职责分离的张力**：文件结构、entry 分布、reader 生命周期只有 dispatcher 侧看得到；checkpoint 输出流、数据格式、per-channel offset 登记只有 `ChannelStateWriter` 侧看得到 — 要做到"一个文件一次顺序读"，必须在两个组件之间设计一个**不泄漏各自内部状态**的新契约，而不是让任何一侧窥探对方的实现细节。
5. **保证来自接口，而非时序**：目标是每个 spill 文件在一次 checkpoint 周期内**只被顺序读一次**，并且这个保证由接口契约本身提供 — 无论调用者如何调度、底层 executor 是单线程还是并行、未来实现如何演进，都不会破坏这个不变式。

---

## 期望目标

> 一个 file 有很多个 entry / network buffer，checkpoint 时只能读取一遍文件，且必须是顺序 IO。

展开为可验证条款：

- **T1 — 单次打开**：checkpoint 期间，每个物理 spill 文件的 `FileChannel` 只读打开一次（实际上在文件被第一次 seal 时就已经打开，checkpoint 期间 0 次额外打开）。
- **T2 — 顺序读**：同一个文件内的所有 entry 严格按文件内 offset 升序被读取，不出现回跳。
- **T3 — 无共享可变 stream**：跨多个 checkpoint 任务单元不共享任何持有可变位置状态（`currentPosition` 之类）的 `InputStream`。
- **T4 — 单位提交**：一个文件对应一次向 `ChannelStateWriter` 的提交（task），而不是 N 次。
- **T5 — 执行模型无关**：无论 `ChannelStateWriter` 的 executor 是单线程、多线程、还是未来改成并行处理，上述保证都不受影响。

---

## 现状为什么达不到目标

### 现状：per-entry 共享 stream

`FilteredBufferDispatcherImpl.drainSpillEntriesToCheckpoint`（当前实现）按 entry 遍历 `spillEntryQueue`，每个 entry 调用一次：

```java
channelStateWriter.addInputData(
        checkpointId,
        entry.getChannelInfo(),        // per-input-channel
        SEQUENCE_NUMBER_RESTORED,
        currentStream,                  // 跨多次调用共享
        entry.getLength());
```

`currentStream` 是 `SequentialFileChannelInputStream`，内部持有 `currentPosition` 这一**可变位置状态**。N 次 `addInputData` 被异步提交到 writer executor，每次从同一个 stream 读取 `entry.getLength()` 字节并推进位置。

### 问题

- **T5 违反**：正确性依赖 executor 单线程 FIFO。若 executor 并发执行两个任务，两个 `writeInputStreaming` 会同时竞争同一个 `currentPosition`，读出错乱字节，默默损坏 checkpoint。
- **T3 违反**：跨任务共享可变 stream 是一个不变式泄漏，dispatcher 和 writer 之间耦合隐式约定。
- **T4 违反**：每个 entry 一次 task 提交，executor 排队/调度开销乘以 N。

---

## 解决方案：把"一个文件的所有 entry"作为一个整体交给 writer

### 核心思路

Dispatcher 把 entries 按 **physical spill file** 分组；每个文件一次性把 `(Reader, List<Entry>)` 打包提交到 `ChannelStateWriter`。Writer 内部对这一组 entry 做同步顺序读：

```
for (Entry e : entries) {     // entries 已按 offset 升序
    positional read (reader.channel, e.offset, e.length)
    写入 e.channelInfo 对应的 checkpoint offsets
}
```

Writer 侧的这个 loop 天然串行（单个 task 闭包内），不依赖 executor 执行模型；FileChannel 的 positional read 按规范就是线程安全的，即使未来同一个 FileChannel 被多个并发任务同时读，互相也不会干扰。

### 为什么满足 T1–T5

| 目标 | 如何满足 |
|---|---|
| T1 单次打开 | Reader 在文件首次 seal 时已打开；checkpoint 期间无额外打开 |
| T2 顺序读 | Entry 列表按 offset 升序排序后交给 writer，writer 内部 loop 顺序访问 |
| T3 无共享 stream | 方案中**完全删除** `InputStream`，改为传递 `(Reader, Entry)` 元组；writer 内部每次做 positional read，不持有 stream 位置状态 |
| T4 单位提交 | 一个文件一次 `addInputData` 调用 ⇒ 一个 executor task |
| T5 执行模型无关 | 每个 task 自闭包，无跨 task 共享可变状态 |

---

## 接口设计（参考 `writeInputStreaming`）

### 新增一个 group-level writer API

在 `ChannelStateWriter` 接口增加：

```
void addInputDataFromSpill(
        long checkpointId,
        SpillFileHandle handle);
```

`SpillFileHandle` 是一个新的小接口（或抽象类），由 dispatcher 侧构造，writer 侧消费：

```
interface SpillFileHandle {
    /** 顺序读取下一个 entry 的数据到给定 byte[]，并返回对应 channelInfo 和字节数。
     *  返回 null 表示已读完。
     *  writer 侧在一个同步 loop 中反复调用，直到 null。 */
    SpillEntryRead readNext(byte[] buf) throws IOException;

    /** 关闭句柄（可选；Reader 本身由 dispatcher 生命周期管理）。 */
    void close();
}

final class SpillEntryRead {
    InputChannelInfo channelInfo;
    int length;
}
```

- **为什么不直接传 `Reader + List<Entry>` 结构体**：`SpillFileHandle` 封装了"顺序 + 自动推进"，writer 侧完全不需要理解 entry 列表、offset 计算、文件格式；只需要"调 `readNext` 直到拿到 null"。这与现有 `writeInputStreaming(InputStream, int)` 的精神一致（writer 不知道 stream 的来源），但把粒度从"一次一个 channel 的连续字节"升级到"一次一个文件的多个 channel 混合字节"。
- **为什么仍保留 writer 主动驱动读**：这样 writer 可以复用它内部的 8KB 临时 buffer，不需要每 entry 分配；也方便将来在 writer 侧加 rate limit / backpressure。

### 对应的 writer 侧实现

新增 `ChannelStateCheckpointWriter.writeInputFromSpillFile(SpillFileHandle)`。

**关于"为什么要内存 buffer 中转"**：`checkpointStream` 在生产环境底层是 DFS（HDFS/S3/OSS）的 `OutputStream`，不是 `FileChannel` 或 `SocketChannel`，所以 `FileChannel.transferTo(...)` 的 `sendfile(2)` zero-copy 优化走不通，JDK 会 fallback 到 DirectByteBuffer → `byte[]` → `outputStream.write(byte[])`，反而多一层间接。`DataOutputStream` 也没有 buffering 语义，`write(byte[])` 直接透传到底层 `OutputStream`。因此 writer 侧最直接的实现就是用一个定长 `byte[]` 做中转 — checkpoint 瓶颈在 DFS 网络 IO，用户态一次拷贝的开销可以忽略。

```
// SpillFileHandle 每次 readNext 填充入参 buf 的前 n 字节，返回 (channelInfo, n)
byte[] buf = new byte[8192];
SpillEntryRead read;
while ((read = handle.readNext(buf)) != null) {
    ChannelStatePendingResult pending = getChannelStatePendingResult(...);
    long offset = checkpointStream.getPos();
    dataStream.writeInt(read.length);              // 4-byte length prefix，与现有读路径格式一致
    dataStream.write(buf, 0, read.length);         // entry 长度 > buf 时由 handle 内部循环
    long size = checkpointStream.getPos() - offset;
    pending.getInputChannelOffsets()
           .computeIfAbsent(read.channelInfo, k -> new StateContentMetaInfo())
           .withDataAdded(offset, size);
}
```

> 格式（length prefix + data bytes）与现有 `writeInputStreaming` L206–222 一致，保证读路径不需要任何变更。

---

## Register 模式与触发时机

Checkpoint 在 Flink 里是 per-input-channel 触发的，每个 channel 走完自己的 barrier 对齐后调用 `RecoveredBufferStoreImpl.checkpoint(...)`，这会触发 `ChannelCheckpointStartedListener.onChannelCheckpointStarted(cpId, channelInfo)`。

`FilteredBufferDispatcherImpl` 的 wait-set 机制负责收敛：

1. 首次回调时，扫描 `spillEntryQueue` 构造 **wait-set**（所有还持有未 drain entry 的 channel 集合）。
2. 每次回调从 wait-set 中移除该 channel。
3. wait-set 变空 ⇒ 所有持有 disk data 的 channel 已注册完毕，触发 `drainSpillEntriesToCheckpoint(checkpointId)`。

这一"最后一个 channel 触发"的模式保证了：
- 调用 `drainSpillEntriesToCheckpoint` 时，**所有**需要从 spill 文件读取的 entry 都已经通过 `incrementPending()` 登记；
- Drain 是一次性全部执行，不会出现"一部分 entry 走 per-entry 接口、另一部分走 per-file 接口"的混合状态。

换句话说，新接口只在 wait-set 变空这一个入口被调用，**一个 checkpoint 周期内每个 spill 文件只会被 drain 一次**。

---

## Drain 侧的改造

`FilteredBufferDispatcherImpl.drainSpillEntriesToCheckpoint` 改为：

```
// 按 reader (=physical file) 分组
Map<Reader, List<Entry>> byFile = new LinkedHashMap<>();  // 保持文件出现顺序
while (!spillEntryQueue.isEmpty()) {
    Entry e = spillEntryQueue.poll();
    Reader r = spillEntryReaderQueue.poll();
    byFile.computeIfAbsent(r, k -> new ArrayList<>()).add(e);
    storesByChannel.get(e.getChannelInfo()).decrementPending();
}

// 每个文件一次提交
for (Map.Entry<Reader, List<Entry>> g : byFile.entrySet()) {
    SpillFileHandle handle = new SpillFileHandleImpl(g.getKey(), g.getValue());
    channelStateWriter.addInputDataFromSpill(checkpointId, handle);
}
```

`SpillFileHandleImpl` 内部用一个 index 顺序遍历 entry list，每次通过 `reader.read(entry.offset, buf, entry.length)` 做 positional read。由于 entry 按入队顺序就是 offset 升序（writer 永远 append 写），不需要额外排序。

删除项：
- `FilteredSpillFile.Reader.openSequentialStream(long)`
- `FilteredSpillFile.Reader.SequentialFileChannelInputStream`（整个内部类）
- `ChannelStateWriter.addInputData(..., InputStream, int)` 重载及其在 `ChannelStateCheckpointWriter.writeInputStreaming` 中的实现

---

## 验证清单

| 条款 | 验证方式 |
|---|---|
| T1 单次打开 | 单测：构造一个 spill 文件写入来自 3 个 channel 的 5 个 entry，checkpoint 后断言 `FileChannel.open` 仅被调用一次 |
| T2 顺序读 | 单测：拦截 `channel.read(bb, position)` 调用序列，断言 `position` 单调递增 |
| T3 无共享 stream | 代码审查 + `grep openSequentialStream` 确认零残留 |
| T4 单位提交 | 单测：mock `ChannelStateWriter`，断言 `addInputDataFromSpill` 被调用次数 = 物理文件数 |
| T5 执行模型无关 | 单测：用 2-线程并发 executor 运行 writer，checkpoint 产物字节级一致 |

---

## 迁移步骤（后续 PR）

1. 定义 `SpillFileHandle` + `SpillEntryRead` 数据结构。
2. 在 `ChannelStateWriter` / `ChannelStateWriterImpl` / `MockChannelStateWriter` 加 `addInputDataFromSpill`，在 `ChannelStateCheckpointWriter` 加 `writeInputFromSpillFile`。
3. 改 `FilteredBufferDispatcherImpl.drainSpillEntriesToCheckpoint` 为文件分组批量提交。
4. 删除 `openSequentialStream` / `SequentialFileChannelInputStream` / `addInputData(InputStream, int)` 及相关测试。
5. 补充 T1–T5 验证单测。
