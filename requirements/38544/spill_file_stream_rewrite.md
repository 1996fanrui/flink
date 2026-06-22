# Spill 文件读写路径重构：去 Buffer、按 channel 段直写流、读侧分块

> Scope：`org.apache.flink.runtime.checkpoint.channel` 包下 spill 文件的**写路径**（writer + 文件格式）与**读路径**（reader / drainer / checkpoint snapshot）。把「用 network buffer 攒满再 flush + per-buffer 落盘 + 内存里持有 segment 对象」的模型，换成「writer 直接对接 `OutputStream` 按 channel 段直写、读侧按 network segment 上限分块重组」的模型。
>
> 不在本提案范围：consumer 侧（`RemoteInputChannel`/`LocalInputChannel`）的 recovery 队列模型见 [remote_single_queue_stash_refactor.md](remote_single_queue_stash_refactor.md)；`RecoverableInputChannel` 对外接口签名不变；filter 的反序列化/重序列化语义（`VirtualChannel` / record filter）不变。

## 0. 核心原则（不可违背，任何设计都必须服务于此）

下面几条是本重构的**硬约束**。后续所有接口/数据结构设计都是手段，目标恒为满足这几条；任何设计若与之冲突，改设计、不改原则。

1. **顺序遍历，逐段推进，按需从磁盘读。** 读侧顺序扫描文件：读段头拿到 `[channelInfo, bufferLength]` → 按需读 `bufferLength` 字节段体并完整消费 → 推进到下一段头（§4.2）。**不允许**把文件里的段体数据一次性全读进内存，也不维护内存段定位表——段长自描述在 disk 段头里。段头量级 = channel 切换次数，远小于 record 数。

2. **一个 segment = 一个 input channel 的一团数据。** 段 = 同一个 channel 连续写入的字节流（per-channel，段头 metadata 只写一次）。

3. **段体对读侧不透明。** 读侧只顺序搬字节，不解析段体内 record 边界（那是 consumer 反序列化器 `SpanningWrapper` 的职责）。

4. **两个消费场景共用同一套顺序遍历接口，但终点不同。** drain 场景把段字节填进 network **buffer**（≤ `memorySegmentSize`，满即交付、循环填）；snapshot 场景把段字节喂进 checkpoint **stream**。接口同时让外层拿到「当前段的 channelInfo + 有界字节流」并跑这两种消费循环（§4.3/§4.4/§4.5）。

5. **线程交接方式不变（强限制）。** 写侧线程产出结果、交给读侧/drainer 线程的**交接方式不允许变**：原来通过哪个 `CompletableFuture`（如 `CompletableFuture<List<RecoverableInputChannel>>`）、在哪个时机、经哪条调用链交接（writer 产出 `SpillFile` → `reader.getProducedSpillFile()` → `new SpillFileDrainer(...)` → 经 `CompletableFuture<RecoveryCheckpointTrigger>` 交给 barrier handler），重构后**完全保持**。只允许变 future / 交接对象**携带的数据结构**（`SpillFile` → `FetchedChannelState` + 文件路径 list）。交接的时序、future 的身份、wiring 一个字不许动（详见 §8.1）。

6. **文件是纯物理切分；段不跨文件（硬约束）。** 多个文件按写入顺序首尾相接即一条完整的格式流，文件边界**不承载任何逻辑语义**——它只是 storage 分片，控制单文件大小（64MB 软上限）。在此前提下**强制：一个段绝不跨文件**——文件轮换只在一个段完整写完后判定（§3.3），段大则文件大。由此一个段必在单文件内，record 绝不被文件边界切断，读侧读一个段只需打开一个文件、`body()` 流绝不跨文件（§4.3）。

## 1. 动机

PR #23 中 Roman 对 spill 写读路径提了一组评论，归一到同一个根因：**当前实现为了写文件引入了 network buffer、`SpillFileSegment` 对象、per-buffer 元数据，复杂度和 OOM 风险都没必要**。

| 评论位置 | 评论要点 |
|---|---|
| `RecoveredChannelStateHandler:234` / `ChannelStateFilteringHandler:363` | 为什么写文件要用 `Buffer`？直接 `FileOutputStream` + `DataOutputViewStreamWrapper`，serializer 直写流即可，超 64MB 换文件 |
| `SpillFile:92` | `segments` 为什么不能就是一个文件名 list？写在读之前已封口、最多 2 个顺序 reader（drainer + checkpointing），无需为 IO 优化，去掉 `SpillFileSegment` 能大幅简化 |
| `SpillFileReader:60` | metadata 已不再单独存内存（`d8bd72437a5` 已内联进文件） |
| `SpillFile:52` | 命名：`SpillFile` → `FetchedChannelState`，`SpillFileSegment` → `FetchedChannelStateFile`，drainer/reader/writer 等对应改名 |
| `RecoveredChannelStateHandler:209` | `RecoveredChannelStateHandler` 拆三个独立实现（no-spilling / spilling-no-filtering / spilling-with-filtering），不要 if 分支 |

会议结论补充（与 Roman 当面确认）：

- **写侧不限 buffer 大小**：一个 channel 的连续 records 总长即便 100KB，也连续直写，**metadata（gateIdx + channelIdx）只在 channel 切换时写一次**。
- **限长在读侧**：drainer 从磁盘读，磁盘上某段 > 32KB（network segment）时按 segment 上限分块读入 network buffer，loop 直到该 channel/该段全部送进 InputChannel。

## 2. 目标文件格式

内存里只维护**文件路径 list**（按写入顺序，写阶段封口后只读）。段的长度自描述在 disk 段头里，**不再维护内存段定位表**。

磁盘格式（按 channel 段组织，段头 per 段写一次，段头自带段体长度）：

```
[ 4B BE int: input gate idx    ]   <-- 段头：channel 标识 + 段体长度
[ 4B BE int: input channel idx ]
[ 4B BE int: buffer length     ]   <-- 段体总字节数（段头之后到下一段头前）
  [ 4B BE int: record length ]      <-- 段体：N 条 length-prefixed record
  [ N bytes: serialized record ]
  [ 4B BE int: record length ]
  [ N bytes: serialized record ]
  ...
[ 4B BE int: input gate idx    ]   <-- 下一段（channel 切换 或 文件轮换后的续写）
[ 4B BE int: input channel idx ]
[ 4B BE int: buffer length     ]
  [ 4B BE int: record length ]
  [ N bytes: serialized record ]
  ...
```

- 一个「段」= 同一个 channel 连续写入的一批 record（一个 channel = 一个 segment = 一个 buffer），段头只写一次 `[gateIdx][channelIdx][bufferLength]`。
- channel 切换时关闭当前段、开新段（重写段头）。
- 段内 record 数量不限、段总长不限（可远超 32KB）。**段长写在 disk 段头的 `bufferLength` 字段里**——读侧读段头拿到 `bufferLength`、读 `bufferLength` 字节段体、定位到下一段头，**不需要内存段定位表、不扫描 record 定界**。
- 单文件超过 `DEFAULT_SEGMENT_SIZE_BYTES`（64MB）即轮换到下一个文件（§3.3）。

> 与现状的本质差异：现状一个磁盘 entry = 一个 32KB output buffer 的 payload，段头随 buffer 反复写；新格式段头随 channel 切换写一次（含 `bufferLength`），段体 record 流式追加。段长从「内存定位表」改为「disk 段头自描述」，offset 维护在 disk 而非内存。

## 3. 写路径设计

### 3.1 去掉的构造

| 当前构造 | 处置 |
|---|---|
| `SpillFileWriter.outputBuffer`（network buffer 攒满再 flush） | 删除 |
| `RecoveredChannelStateHandler.postFilterSegment`（writer 用的堆 segment） | 删除 |
| `SpillFileWriter.flush()` / `requestBufferBlocking` 的 buffer 满/channel 切换 flush 逻辑 | 删除，改为段切换 + record 直写 |
| `SpillFileSegment`（持有 `FileChannel` + `currentEnd` 的内存对象） | 删除，改为文件路径 list（见 §5） |
| `SpillFile.append(channelInfo, ByteBuffer payload)`（per-buffer payload 落盘） | 删除，改为 record 级直写 |
| `SpillFile.headerBuffer` / `HEADER_BYTES`（per-record 12B 内联头） | 改为段头 `[gateIdx][channelIdx][bufferLength]`（12B，per 段一次）+ record 头 `[recordLen]`（4B，per record） |

### 3.2 writer 新形态（段体攒入 DataOutputSerializer，段封口回填 bufferLength）

段头的 `bufferLength` 必须等整段写完才知道，故段体先攒在一个可回填的内存缓冲里，段封口时回填长度再 flush 到文件流。复用 `org.apache.flink.core.memory.DataOutputSerializer`（参照 `RecordWriter.serializeRecord`：`setPositionUnsafe(4)` 预留长度位 → 写内容 → `writeIntUnsafe(len, 0)` 回填）。

writer 持有：

- 当前 `OutputStream`（`BufferedOutputStream` 包 `FileOutputStream`）—— 段封口后把整段字节 flush 到这里
- 当前段的 `DataOutputSerializer segmentBuffer` —— 攒当前 channel 段的段体（record 流），段封口时在头部回填 `bufferLength`
- 当前段所属 `currentChannel`（`InputChannelInfo`），用于判断 channel 切换
- `runningLength`（当前文件已写字节数，文件轮换的判据）
- 已写文件路径 list

写入流程：

- **开段**（首次写某 channel / channel 切换后）：`segmentBuffer.clear()`，写 `[gateIdx][channelIdx]`，再预留 4 字节给 `bufferLength`（`setPositionUnsafe` 占位）。
- **filtering 模式**：`GateFilterHandler` 反序列化 → filter → 对每条存活 record，`serializer.serialize(element, segmentBuffer)` 写进段缓冲，再 `segmentBuffer.writeInt(recordLength)`（length-prefix）。
- **no-filtering（pass-through）模式**：上游 recovered buffer 字节本身就是 length-prefixed record 序列，整段 `segmentBuffer.write(bytes)`。
- **段封口**（channel 切换 / `close()`）：用 `writeIntUnsafe` 把段体实际字节数回填到段头的 `bufferLength` 位，把 `segmentBuffer` 整段 flush 到文件 `OutputStream`，`runningLength += 段总字节`，再判文件轮换（§3.3）。

> 段体先攒内存（一个 channel 整段，可能 100KB）是「段长写进 disk 段头需回填」的必要代价——`DataOutputSerializer` 是内存缓冲、支持 `writeIntUnsafe` 原地回填，避免了对文件流 seek。段体上限 = 单 channel recovery 数据量（一般不大），且段封口即 flush、不跨段累积，无 OOM 风险。

### 3.3 段切换与文件轮换（段不跨文件，硬约束）

**段不跨文件**是硬约束（§0 原则 6）：一个段（一个 channel 的一团字节）永远完整落在单个文件内。为此，**文件轮换只在一个段完整写完后判定**，绝不在段中间 / record 中间切：

- **写入期**：record（filtering）或 pass-through 字节持续 append 到当前段的 `segmentBuffer`（内存）。**期间不检查轮换、不碰文件流**。
- **段封口时**（channel 切换 → 当前段结束 / writer `close()`）：回填段头 `bufferLength`，把整段 flush 到文件流，`runningLength += 段总字节`；**再**检查 `runningLength >= DEFAULT_SEGMENT_SIZE_BYTES`：超过则关闭当前流、开新文件、`runningLength=0`、路径入 list。下一个段（无论同 channel 还是新 channel）写进新文件。

由此：

- **段不跨文件**：轮换点永远落在两个段之间（段已整段 flush），一个段必在单文件内。
- **record 不跨文件**：段不跨文件 ⇒ record 不被文件边界切断（filtering 与 pass-through 同此结论，pass-through 无需按 record 边界解析）。
- **64MB 是软上限**：若单个 channel 段本身 > 64MB，该段仍完整写进一个文件，文件随之 > 64MB。单 channel recovery 数据一般不大，可接受；换来「段绝不跨文件、record 绝不被切」的读侧最简性。

> 文件是纯物理切分（§0 原则 6），轮换时机的唯一作用是控制单文件大小，不承载逻辑语义。逻辑上所有文件按写入顺序首尾相接即一条完整格式流；但「段不跨文件」保证每段读取只需打开一个文件。

### 3.4 关闭

`close()`：封口当前段（回填 `bufferLength` + flush 整段）、关闭当前流；路径 list 即最终产物，交给读侧。

## 4. 读路径设计

### 4.1 核心原则：段体是不透明字节流，读侧不解析 record

读侧**不关心段体里 record 的格式**。一个段对读侧就是「某个 channel 的一团连续字节」，读侧只做一件事：从段的字节流顺序读，往目标（network buffer 或 checkpoint stream）顺序填，填满 / 段读完即止。record 的 `[recordLen]` 边界是 consumer 反序列化器（`SpanningWrapper`）的事，读侧不 care。

这把「限长在读侧」（Roman 7:33pm）落成：写侧段长不限（一个 channel 100KB 连续写），drainer 交付的 network buffer 上限是一个 `memorySegmentSize`（通常 32KB），读侧就把段字节流**按 32KB 切块**填进一个个 buffer，buffer 满就交付、下一个 buffer 接着从同一段流读，直到段读完 / channel 切换。

### 4.2 段边界由 disk 段头的 `bufferLength` 自描述

文件就是 §2 的格式：段头 `[gateIdx][channelIdx][bufferLength]` + 段体多条 `[recordLen][record]`。段长写在 disk 段头里（offset 维护在 disk 而非内存）。读侧顺序扫描一个文件：

```
read 段头 [gateIdx][channelIdx][bufferLength]   -> 当前段的 channel + 段体字节数
read bufferLength 字节段体                        -> 完整消费（填 buffer / 写 stream），不解析 recordLen
到达 bufferLength 即本段尾 -> 下一个 4B 是下一段头的 gateIdx，回到段头解析
文件读尽 -> 切下一个文件（path list）
```

- 段内**不逐条解析 recordLen 来定界**——段有多长由段头 `bufferLength` 直接给出，读 `bufferLength` 字节即整段。
- **不维护内存段定位表**：段头自描述，读侧靠顺序扫文件 + `bufferLength` 推进。段头量级 = channel 切换次数，远小于 record 数，无 per-record 内存对象、无 OOM（回应 `SpillFileReader:60`）。
- 段不跨文件（§3.3），故每段必在单文件内、`body()` 不跨文件。
- 一个文件内可含多段（§2），相邻段物理连续，段边界由段头 `bufferLength` 界定。

> 「渐进迭代」（§0 原则 1）：读侧顺序扫文件**逐段**推进，每段读段头拿 `bufferLength` 后**按需**读该段字节，不把文件一次性读进内存。

### 4.3 读接口：顺序迭代段 + 按段提供有界字节流

reader 对外不再产出 `Chunk`（1 record/buffer = 1 对象）。而是**顺序迭代段**，每段暴露 channelInfo + 一个**有界到段尾**（`offset + length`）的 `InputStream`。drain 与 snapshot 共用这一个迭代，只是消费终点不同（§0 原则 4）：

```java
@Internal
public final class FetchedChannelStateReader implements Closeable {

    static FetchedChannelStateReader openRoot(FetchedChannelState state);

    /**
     * Sequentially iterates per-channel segments by scanning the files in write order: read a
     * segment header [gateIdx][channelIdx][bufferLength], expose the bufferLength-bounded body, then
     * advance to the next header / next file. Advances one segment at a time; does not materialize
     * file data in memory.
     *
     * <p>Each cursor stays valid only until the next {@code next()}; its body must be fully
     * consumed before advancing.
     */
    CloseableIterator<FetchedSegmentCursor> segments();

    /** Derives an independent reader from the current drain position; call under the drainer lock. */
    FetchedChannelStateReader snapshot();

    @Override public void close() throws IOException;
}

/** A single per-channel segment during iteration. Body bytes are opaque to the reader. */
public interface FetchedSegmentCursor {
    InputChannelInfo channelInfo();
    /** Bounded to this segment's bufferLength bytes; read() returns -1 at segment end. */
    InputStream body();
    /** Segment body length in bytes (from the segment header), used by snapshot as the length prefix. */
    long length();
    /** Commits bytes already read from body() to the reader cursor; called under the drainer lock. */
    void commitConsumed();
}
```

`body()` 是包装流：从当前文件 channel 段头之后的位置起读，读满段头声明的 `bufferLength` 字节即返回 EOF。消费方 `read()` 到 -1 即本段读尽，不越界到下一段头。读侧不解析段体里的 record 边界（§0 原则 3）——段体对它是不透明净字节；recordLen 帧由 consumer 的 `SpanningWrapper` 那层处理。`length()` 即段头读出的 `bufferLength`。

**底层文件对外层完全透明**：因段不跨文件（§0 原则 6），一个 `body()` 流只读单个文件，**绝不跨文件**。文件切换只发生在「读完一个 cursor、`next()` 到下一个 cursor」时——此时若下一段在另一个文件，reader 内部关旧 `FileChannel`、开 `files.get(nextFileIndex)`，外层无感。外层看到的始终是「一串 segment / 一个个 `body()` 流」，从不接触文件路径、文件边界、跨文件拼接。

### 4.4 drain 场景遍历：段流 → 按 segmentSize 切块填 buffer

```java
void drain() throws IOException, InterruptedException {
    ResolvedChannels channels = resolvedChannelsFuture.join();
    try (CloseableIterator<FetchedSegmentCursor> segs = rootReader.segments()) {
        while (segs.hasNext()) {
            FetchedSegmentCursor seg = segs.next();          // read next segment header from disk
            RecoverableInputChannel ch = channels.channelByInfo.get(seg.channelInfo());
            if (ch == null) {                                 // fail-loud (§9, 现状已有)
                throw new IllegalStateException("Drain: no physical channel for " + seg.channelInfo());
            }
            InputStream in = seg.body();                      // bounded to this segment [offset, offset+length)
            Buffer buf = ch.requestRecoveryBufferBlocking();  // capacity = memorySegmentSize
            int cap = buf.getMaxCapacity();
            // Fill buf from the opaque segment stream; on full, deliver under lock and refill.
            while (fill(buf, in, cap - buf.getSize()) > 0) {  // fill returns 0 at segment EOF
                if (buf.getSize() == cap) {
                    synchronized (lock) { ch.onRecoveredStateBuffer(buf); seg.commitConsumed(); }
                    buf = ch.requestRecoveryBufferBlocking();
                }
            }
            if (buf.getSize() > 0) {                          // segment tail (partial buffer)
                synchronized (lock) { ch.onRecoveredStateBuffer(buf); seg.commitConsumed(); }
            } else {
                buf.recycleBuffer();
            }
        }
    }
    synchronized (lock) { drainFinished = true; }
    for (RecoverableInputChannel ch : channels.allChannels) ch.finishRecoveredBufferDelivery();
}

/** Fills up to `remaining` bytes from the opaque segment stream into buf; returns bytes filled, 0 at EOF. */
private static int fill(Buffer buf, InputStream in, int remaining) throws IOException {
    if (remaining == 0) return 0;
    try (ChannelStateByteBuffer view = ChannelStateByteBuffer.wrap(buf)) {
        return view.writeBytes(in, remaining); // reuses the existing stream→buffer primitive
    }
}
```

- `fill` 复用现有 `ChannelStateByteBuffer.wrap(Buffer).writeBytes(InputStream, int)`（已存在的「从流读最多 N 字节填进 buffer」原语），不新写字节搬运代码、**不解析 record**。
- 现 `SpillFileReader.peek()/advance()/Chunk(1 record=1 buffer)` 三件套删除，换成「段流 + fill 切块」。
- 持锁配对：锁内只做「`onRecoveredStateBuffer(buf)` 交付 + `seg.commitConsumed()` 推进 reader 游标」这一原子步（与 snapshot 互斥），磁盘读、buffer 分配在锁外。`commitConsumed()` 把「锁外已从 `in` 读走的字节」提交到 reader 游标，使 snapshot 锁内看到的位置始终落在「已交付字节边界」，与现状 `advance()` 语义一致。

### 4.5 snapshot 场景遍历：段流 → 直接喂 checkpoint stream

`snapshotAndInsertBarriers(checkpointId)` 对仍未 drain 完的段，逐段把段字节**流式**喂给 `ChannelStateCheckpointWriter`，不先读进 `byte[]`：

```java
void writeInputFromSpill(JobVertexID v, int sub, CloseableIterator<FetchedSegmentCursor> segs) {
    ...
    while (segs.hasNext()) {
        FetchedSegmentCursor seg = segs.next();
        long offset = checkpointStream.getPos();
        serializer.writeData(dataStream, seg.body(), seg.length()); // NEW streaming overload
        long size = checkpointStream.getPos() - offset;
        pendingResult.getInputChannelOffsets()
            .computeIfAbsent(seg.channelInfo(), unused -> new StateContentMetaInfo())
            .withDataAdded(offset, size);
    }
    ...
}
```

- 现状 `writeInputFromSpill` 消费 `CloseableIterator<Chunk>`，每 chunk 调 `serializer.writeData(dataStream, chunk.data, chunk.length)` 并按 `chunk.channelInfo` 聚合 `StateContentMetaInfo`。
- 新形态消费 `CloseableIterator<FetchedSegmentCursor>`：长度前缀用 `seg.length()`（段元数据里已知，§4.2，**不需要写侧落盘的 bodyLength**），再从 `seg.body()` 流式拷 `length` 字节进 `dataStream`。`StateContentMetaInfo` 仍按 `channelInfo` 聚合，语义不变。
- 需新增 serializer overload `writeData(DataOutputStream, InputStream, long)`：写 int 长度前缀 + 从流拷 length 字节（与现有 `writeData(stream, byte[], int)` 并存，旧 caller 不动）。

迭代器元素从 `Chunk`（含 `byte[] data`）变为 `FetchedSegmentCursor`（含 `channelInfo()` + `body()` 流 + `length()`）。与 drain 互斥由 drainer 锁保证。

## 5. 去掉 `SpillFileSegment`，改为文件路径 list

按 `SpillFile:92` 评论：写在读之前封口、最多 2 个顺序 reader、无需 IO 优化。

- `SpillFile`（改名后 `FetchedChannelState`，见 §7）内部 `List<SpillFileSegment> segments` → `List<Path> files`。
- 写阶段：当前文件用一个 `FileChannel`/`OutputStream`，轮换时关旧开新、路径入 list。不再为每个文件长期持有 `FileChannel` 对象。
- 读阶段：reader 按 list 顺序逐文件 `FileChannel.open(path, READ)`，读完即关。
- 生命周期：`acquire()/release()` 引用计数与最后一次 release 删除所有文件的逻辑不变，只是删除对象从 `segments` 变为 `files`。

## 6. `RecoveredChannelStateHandler` 抽象基类 + 三个具体实现

按 `RecoveredChannelStateHandler:209`：删掉 `InputChannelRecoveredStateHandler` 里 `checkpointingDuringRecoveryEnabled` × `filteringHandler != null` 的 if 分支，拆成**一个抽象基类承载公共逻辑 + 三个具体子类各承载一种 recover 行为**。

### 6.1 类层次

```
RecoveredChannelStateHandler<Info, Context>            (现有顶层接口，不变)
        ▲
        │ implements
AbstractInputChannelRecoveredStateHandler              (新增抽象基类，<InputChannelInfo, Buffer>)
   ├─ 公共字段: inputGates, channelMapping, rescaledChannels, oldToNewMappings
   ├─ 公共实现: getMappedChannels / calculateMapping / getChannel  (channel 映射)
   ├─ getBuffer(InputChannelInfo): 默认从目标 channel 的 network pool 申请 (no-spilling / no-filtering 共用)
   ├─ close(): 公共生命周期收尾 (子类 super.close() 后清理各自资源)
   └─ abstract recover(info, oldSubtaskIndex, bufferWithContext)   (三子类各自实现)
        ▲
        ├──────────────────────────┬──────────────────────────────┐
        │                          │                              │
NoSpillingHandler        SpillingNoFilteringHandler      SpillingWithFilteringHandler
(checkpointing-          (checkpointing-during-           (checkpointing-during-recovery
 during-recovery off)     recovery on, 无 rescale filter)  on + rescale filter)
```

### 6.2 三个具体实现

| 子类 | 触发条件 | `recover()` 行为 | 独有字段/方法 |
|---|---|---|---|
| `NoSpillingHandler` | `checkpointingDuringRecoveryEnabled == false` | 现状 `else` 分支：`onRecoveredStateBuffer(SubtaskConnectionDescriptor event)` + `onRecoveredStateBuffer(buffer.retainBuffer())`，不落盘 | 无 |
| `SpillingNoFilteringHandler` | spilling on，`filteringHandler == null` | pass-through：上游 buffer 字节本身就是 length-prefixed record 流，按段直写 writer（§3.2）。channel 不变则不切段（§10.2） | `FetchedChannelStateWriter writer` |
| `SpillingWithFilteringHandler` | spilling on，`filteringHandler != null` | `filteringHandler.filterAndRewrite(...)` 反序列化 → filter → 存活 record 序列化直写 writer（§3.2） | `FetchedChannelStateWriter writer`；`filteringHandler`；**pre-filter buffer**：`preFilterSegment` / `preFilterBufferInUse` / `getPreFilterBuffer()`（override `getBuffer`，从隔离堆 segment 发 buffer 而非 network pool） |

### 6.3 哪些逻辑落在基类、哪些落在子类

- **基类（公共）**：`getMappedChannels` / `calculateMapping` / `getChannel`（channel 映射，三子类都要）；`getBuffer` 默认实现（从目标 channel network pool 申请，`NoSpillingHandler` 与 `SpillingNoFilteringHandler` 直接用）；`close()` 公共骨架（`final` 收尾 + 调子类 `closeInternal()` 钩子）。
- **`SpillingWithFilteringHandler` override `getBuffer`**：pre-filter buffer 只有 filtering 模式需要（反序列化要一块隔离堆 segment），故 `preFilterSegment` / `preFilterBufferInUse` / `getPreFilterBuffer()` 全部下沉到此子类，基类与另两个子类都不持有。
- **两个 spilling 子类共享 writer 装配**：`ensureWriter()`（解析目录、`new FetchedChannelStateWriter(...)`）可抽到一个中间层 `AbstractSpillingHandler extends AbstractInputChannelRecoveredStateHandler`，由 `SpillingNoFilteringHandler` 和 `SpillingWithFilteringHandler` 继承；`NoSpillingHandler` 直接继承顶层抽象基类（不持有 writer）。

### 6.4 构造点

工厂在装配时按 `checkpointingDuringRecoveryEnabled` 和 `filteringHandler != null` 二维**选择实例化哪个子类**（一次性决策），不再把这两个标志带进运行期做 if 分支。原 `InputChannelRecoveredStateHandler` 删除。

## 7. 重命名

| 当前 | 改名后 |
|---|---|
| `SpillFile` | `FetchedChannelState` |
| `SpillFileSegment` | （删除，见 §5；若保留文件元信息则为 `FetchedChannelStateFile`） |
| `SpillFileWriter` | `FetchedChannelStateWriter` |
| `SpillFileReader` | `FetchedChannelStateReader` |
| `SpillFileDrainer` | `FetchedChannelStateDrainer` |
| `SpillFileReader.Chunk` | 删除（1 record/buffer = 1 对象）。读侧改为顺序迭代段 `CloseableIterator<FetchedSegmentCursor>`，段长由 disk 段头 `bufferLength` 自描述（§4.2/§4.3），无内存段定位结构 |

删除/改名后全文搜索 `Spill*` 在注释、javadoc、测试、`requirements/` 之外的字符串引用，同步清理（包括 `peekActiveSpillFileForTesting`、`getProducedSpillFile` 等 VisibleForTesting 方法名）。

## 8. 影响面与下游

| 下游 | 影响 |
|---|---|
| `ChannelStateCheckpointWriter.writeInputFromSpill` | 消费对象 `Chunk(byte[])` → `FetchedSegmentCursor(channelInfo + body 流 + length)`；改为流式 `writeData(stream, body, length)`（§4.5），不再一次性持 `byte[]`；`StateContentMetaInfo` 仍按 `channelInfo` 聚合 offset/size，语义不变 |
| `ChannelStateWriterImpl.addInputDataFromSpill` / `ChannelStateWriteRequest.replayInputDataFromSpill` | 迭代器元素类型 `CloseableIterator<Chunk>` → `CloseableIterator<FetchedSegmentCursor>`，签名随之更新；异常路径 `chunks.close()` 不变 |
| `ChannelStateSerializer` | 新增 `writeData(DataOutputStream, InputStream, long)` 流式 overload；旧 `writeData(stream, byte[], int)` / `writeData(Buffer...)` 保留给其他 caller |
| `ChannelState`（streaming io checkpointing） | 持有 `RecoveryCheckpointTrigger` 与 snapshot 迭代器，元素类型改名跟随 |
| `RecoverableInputChannel` | 接口签名不变；`onRecoveredStateBuffer` 接收的 buffer 仍 ≤ `memorySegmentSize`（由读侧 §4.4 切块保证） |
| consumer 侧反序列化 | 不变；交付 buffer 可跨 record 切分（读侧不对齐 record 边界），`SpanningWrapper` 处理 |

### 8.1 线程交接（§0 原则 5 落地）：future 身份/时序/wiring 全不动，只换数据结构

写侧线程产物交给 drainer 线程的链路逐项保持，仅替换携带的数据结构与类名：

| 旧 | 新 | wiring 是否动 |
|---|---|---|
| `CompletableFuture<List<RecoverableInputChannel>> physicalChannelsFuture` | 不变（同一 future、同一泛型） | 否 |
| `reader.getProducedSpillFile()` 返回 `SpillFile` | `getProducedChannelState()` 返回 `FetchedChannelState`（持 `List<Path> files`，§5） | 否（同一取值点、同一 release 调用） |
| `new SpillFileDrainer(SpillFile, future)` | `new FetchedChannelStateDrainer(FetchedChannelState, future)` | 否（构造点不变，只换参数类型与类名） |
| `CompletableFuture<RecoveryCheckpointTrigger>`（drainer 实现该接口） | 不变（`FetchedChannelStateDrainer` 仍 implements `RecoveryCheckpointTrigger`） | 否 |
| `snapshotAndInsertBarriers` 返回 `CloseableIterator<Chunk>` | 返回 `CloseableIterator<FetchedSegmentCursor>` | 否（同一方法、同一调用链 `addInputDataFromSpill`→`replayInputDataFromSpill`→`writeInputFromSpill`） |

跟随改名（仅改名，wiring 不动）：`SequentialChannelStateReader.getProducedSpillFile()` → `getProducedChannelState()`；`SequentialChannelStateReaderImpl.producedSpillFile` → `producedChannelState`；`StreamTask` 局部变量 `producedSpillFile`/`leakedSpillFile` → `producedChannelState`/`leakedChannelState`。

## 9. 不变式（fail-loud）

- 写侧：文件轮换只在一个段完整写完后判定（§3.3）→ 段不跨文件 → record 不被文件边界切断 → 一个段必在单文件内。
- 写侧：段头 `bufferLength` 回填值必须与实际写入段体字节严格一致（读侧只信段头、不扫描 record 定界），不一致即数据损坏 → 写封口时可断言「每个文件内 Σ(段头 12B + bufferLength) == 文件物理大小」。
- 读侧：按段头 `bufferLength` 读出的字节数不足 / 文件提前 EOF → fail-loud（段被截断）。读侧**不**解析段体里的 record（那是 consumer 反序列化器的事），故无 `recordLen` 校验。
- 读侧：迭代严格顺序、逐段推进、按需读盘（§0 原则 1）——不把文件段体一次性读进内存。
- 文件生命周期（**不变**）：靠 `acquire()/release()` 引用计数管理；**中途不清理任何文件**，仅当最后一次 `release()` 使引用计数归零（drain + 所有 snapshot reader 都读完释放）时，才一次性删除**全部**文件。本提案只把删除对象从 `SpillFileSegment` 换成 `Path`（§5），清理时机/计数语义一字不改。
- drainer：段 `channelInfo` 在已解析 channel 集合中找不到 → 抛异常（现状已有）。
- 段头数 = channel 切换次数，远小于 record 总数（无 per-record 内存对象）。

## 10. 开放问题

1. **段头 `bufferLength` 回填时机**：段体先攒进 `DataOutputSerializer`（段头预留 4B），channel 不变则持续 append；段封口时 `writeIntUnsafe` 回填实际段体字节数再 flush。需确认 filtering 模式下「一条上游 buffer 产出 0..N 条存活 record、record 可能横跨上游 buffer」时，只要目标 channel 不变就不封段、record 持续 append 进同一 `segmentBuffer`，封口时 bufferLength 回填正确。
2. **pass-through 模式段合并**：no-filtering 下连续多个上游 buffer 同属一个 channel 时应合并为一个段（段头只写一次、段体跨调用 append），需确认 pass-through 写入能跨多次调用复用 `currentChannel` 不重复开段、封口时统一回填 bufferLength。
3. **drain 锁内 `seg.commitConsumed()` 与 snapshot 的位置一致性**：§4.4 把「锁外从 `body()` 读走字节」与「锁内提交 reader 游标」分离，需确认 snapshot 锁内看到的段游标位置始终是「已交付字节边界」，与现状 `advance()` 语义等价、无竞态。
