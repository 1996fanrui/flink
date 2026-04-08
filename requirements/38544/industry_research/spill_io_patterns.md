# Spill-to-Disk I/O 模式调研

> 调研目标：为 Flink checkpoint channel state recovery 中的 spilling OutputWriter 选择最佳 I/O 实现方案。
> 核心场景：顺序追加写 byte[] 到 spill 文件 + 按固定 chunk size (32KB) 顺序读回。

---

## Topic 1: Java FileChannel vs BufferedOutputStream 性能对比

### Sources

- [Java 11 FileChannel API 官方文档](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/nio/channels/FileChannel.html)
- [Flink BufferReaderWriterUtil 源码](https://github.com/apache/flink/blob/master/flink-runtime/src/main/java/org/apache/flink/runtime/io/network/partition/BufferReaderWriterUtil.java)
- [Spark NioBufferedFileInputStream 源码](https://github.com/apache/spark/blob/master/core/src/main/java/org/apache/spark/io/NioBufferedFileInputStream.java)

### Industry Consensus

| 维度 | FileChannel (NIO) | BufferedOutputStream (BIO) |
|------|-------------------|---------------------------|
| 写入模式 | 支持 gather write (writev)，可将 header+data 一次系统调用写入 | 需要两次 write 调用或先拼接再写 |
| 读取模式 | 支持 positional read，不影响 channel position | 必须顺序读取 |
| Direct Buffer | 原生支持，避免 JVM heap 到 native 内存的拷贝 | 不支持，数据必须经过 heap buffer |
| 零拷贝 | transferTo/transferFrom 可实现零拷贝 | 不支持 |
| 线程安全 | 位置相关操作串行，positional read 可并发 | 不支持并发 |
| 适用场景 | 大数据量顺序 I/O，需要精确控制缓冲策略 | 小文件、简单场景 |

**关键结论**：对于 spill 场景（大量顺序写+顺序读），FileChannel 是行业标准选择。Flink 和 Spark 的 spill/shuffle 实现均使用 FileChannel。

### Common Pitfalls

1. **小数据量使用 FileChannel 反而更慢**：FileChannel 的优势在大数据量 I/O 中才显现。对于极小文件（<几十KB），BufferedOutputStream 的简单缓冲可能更高效。但 spill 场景通常数据量大，此问题不适用。
2. **频繁创建 direct buffer**：DirectByteBuffer 的分配和回收代价高于 heap buffer，必须复用而非每次操作都分配。
3. **未使用 gather write**：分开写 header 和 data 会产生两次系统调用。FileChannel 支持 `write(ByteBuffer[])` 一次性写入多个 buffer（底层使用 writev 系统调用）。

### Recommendation

使用 FileChannel + direct ByteBuffer。具体理由：
- 与 Flink 现有 spill 代码风格一致（`BufferReaderWriterUtil`, `PartitionedFileWriter` 等均使用 FileChannel）
- 支持 gather write 减少系统调用次数
- Direct buffer 避免 heap-to-native 拷贝
- 支持 positional read，便于未来扩展并发读取

---

## Topic 2: Flink/Spark 的 Spill File I/O 实现分析

### Sources

- [Flink BufferReaderWriterUtil](https://github.com/apache/flink/blob/master/flink-runtime/src/main/java/org/apache/flink/runtime/io/network/partition/BufferReaderWriterUtil.java)
- [Flink AsynchronousFileIOChannel](https://github.com/apache/flink/blob/master/flink-runtime/src/main/java/org/apache/flink/runtime/io/disk/iomanager/AsynchronousFileIOChannel.java)
- [Flink PartitionedFileWriter](https://github.com/apache/flink/blob/master/flink-runtime/src/main/java/org/apache/flink/runtime/io/network/partition/PartitionedFileWriter.java)
- [Flink FileUtils.writeCompletely](https://github.com/apache/flink/blob/master/flink-core/src/main/java/org/apache/flink/util/FileUtils.java)
- [Spark ShuffleExternalSorter](https://github.com/apache/spark/blob/master/core/src/main/java/org/apache/spark/shuffle/sort/ShuffleExternalSorter.java)
- [Spark NioBufferedFileInputStream](https://github.com/apache/spark/blob/master/core/src/main/java/org/apache/spark/io/NioBufferedFileInputStream.java)

### Industry Consensus

#### Flink 的 Spill I/O 模式

Flink 在 shuffle/spill 场景中统一使用以下模式：

1. **I/O API**：`java.nio.channels.FileChannel`
2. **Buffer 策略**：Direct ByteBuffer，通过 `ByteBuffer.allocateDirect()` 分配
3. **写入格式**：`[8字节header] + [数据]`，header 包含数据类型(2B) + 压缩标志(2B) + 数据长度(4B)
4. **写入方式**：gather write，将 header buffer 和 data buffer 组成数组通过 `FileChannel.write(ByteBuffer[])` 一次写入
5. **读取方式**：先读 header 解析长度，再读对应长度的数据
6. **Header buffer 复用**：通过 `allocatedWriteBufferArray()` 预分配 `ByteBuffer[]{headerBuf, null}` 复用 header buffer

```java
// Flink 的写入模式（BufferReaderWriterUtil.java）
static long writeToByteChannel(FileChannel channel, Buffer buffer, ByteBuffer[] arrayWithHeaderBuffer) {
    final ByteBuffer headerBuffer = arrayWithHeaderBuffer[0];
    setByteChannelBufferHeader(buffer, headerBuffer);      // 填充 header
    arrayWithHeaderBuffer[1] = buffer.getNioBufferReadable(); // data buffer
    final long bytesExpected = HEADER_LENGTH + dataBuffer.remaining();
    writeBuffers(channel, bytesExpected, arrayWithHeaderBuffer); // gather write
    return bytesExpected;
}
```

#### Spark 的 Spill I/O 模式

1. **写入**：通过 `DiskBlockObjectWriter` 封装，底层使用 `FileOutputStream` + 可配置 buffer size
2. **读取**：`NioBufferedFileInputStream` 使用 FileChannel + 8KB direct buffer（可配置）
3. **合并**：优先使用 `FileChannel.transferTo()` 零拷贝，压缩/加密场景降级为流式读写
4. **中间缓冲**：1MB 的 heap byte array 做中间转换缓冲

#### 关键差异

| 维度 | Flink | Spark |
|------|-------|-------|
| 写入 API | FileChannel (NIO) | FileOutputStream (BIO) |
| 读取 API | FileChannel (NIO) | FileChannel via NioBufferedFileInputStream |
| Direct Buffer | 是（header buffer） | 是（读取侧 8KB direct buffer） |
| 合并优化 | region-based 顺序读 | transferTo 零拷贝 |

### Common Pitfalls

1. **Flink 的 writev 限制**：Linux writev 系统调用限制最多 1024 个 buffer。Flink 在 `writeBuffers` 中处理了这个问题：如果 gather write 未完整写入，则逐个 buffer 循环写入。
2. **Spark 读写 API 不一致**：Spark 写入侧使用 BIO（FileOutputStream），读取侧使用 NIO（FileChannel）。这是历史原因，并非最优选择。

### Recommendation

遵循 Flink 现有模式：
- 写入和读取均使用 FileChannel
- Header buffer 使用 direct ByteBuffer 并复用
- 数据 buffer 直接使用 Flink NetworkBuffer 的 `getNioBufferReadable()` 获取，避免额外拷贝
- 采用 `[header + data]` 的 TLV 格式，与 `BufferReaderWriterUtil` 保持一致

---

## Topic 3: 临时 Spill 文件是否需要 fsync/force

### Sources

- [Java 11 FileChannel.force() 文档](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/nio/channels/FileChannel.html)
- [Flink PartitionedFileWriter](https://github.com/apache/flink/blob/master/flink-runtime/src/main/java/org/apache/flink/runtime/io/network/partition/PartitionedFileWriter.java) -- 无 force 调用
- [Flink SegmentPartitionFileWriter](https://github.com/apache/flink/blob/master/flink-runtime/src/main/java/org/apache/flink/runtime/io/network/partition/hybrid/tiered/file/SegmentPartitionFileWriter.java) -- 无 force 调用
- [Spark ShuffleExternalSorter](https://github.com/apache/spark/blob/master/core/src/main/java/org/apache/spark/shuffle/sort/ShuffleExternalSorter.java) -- 无 fsync 调用

### Industry Consensus

**Flink 和 Spark 的 spill 文件均不调用 fsync/force**。

原因分析：

| 考量维度 | 分析 |
|---------|------|
| 数据丢失后果 | Spill 数据是运行时临时数据。丢失后可通过 checkpoint 重新恢复，不需要持久化保证 |
| 性能代价 | `FileChannel.force()` 触发操作系统刷盘，延迟可达毫秒级别。对高频写入场景影响严重 |
| 丢失概率 | 数据丢失仅在 OS 崩溃/断电时发生。进程崩溃时 OS 会刷脏页，数据不会丢失 |
| 行业实践 | Flink 的 shuffle 文件、Spark 的 spill 文件、RocksDB 的 WAL（可配置关闭 fsync）均支持不 fsync 的模式 |

**关键区分**：
- **Checkpoint 数据**：需要持久化保证，Flink 的 `CheckpointStateOutputStream` 在 close 时会确保持久化
- **Spill 临时数据**：不需要持久化保证，数据丢失可重新从 checkpoint 恢复

### Common Pitfalls

1. **过度持久化**：对临时 spill 文件调用 fsync 会严重降低写入吞吐量（可能降低 2-10x），而收益几乎为零。
2. **混淆 close 和 fsync**：`FileChannel.close()` 不保证数据刷盘。但进程正常退出时 OS 会最终刷盘。对于临时文件，这已足够。
3. **误判 JVM 崩溃场景**：JVM 崩溃（OOM、SegFault）时 OS 仍在运行，脏页最终会被刷盘。只有 OS 崩溃/断电才会丢失未 fsync 的数据。

### Recommendation

**不调用 fsync/force**。理由：
- 本场景的 spill 数据是 checkpoint channel state recovery 期间的临时数据
- 数据丢失的唯一后果是需要重新从 checkpoint 恢复，这在 Flink 中是常规操作
- 与 Flink 现有 spill/shuffle 实现保持一致
- 避免 fsync 的性能损耗，提升恢复速度

---

## Topic 4: Partial Write 处理

### Sources

- [Java 11 FileChannel.write() 文档](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/nio/channels/FileChannel.html) -- "An invocation of this method may or may not transfer all of the requested bytes"
- [Flink FileUtils.writeCompletely()](https://github.com/apache/flink/blob/master/flink-core/src/main/java/org/apache/flink/util/FileUtils.java) -- while 循环处理 partial write
- [Flink BufferReaderWriterUtil.writeBuffer()](https://github.com/apache/flink/blob/master/flink-runtime/src/main/java/org/apache/flink/runtime/io/network/partition/BufferReaderWriterUtil.java) -- while 循环处理 partial write
- [Flink BufferReaderWriterUtil.writeBuffers()](https://github.com/apache/flink/blob/master/flink-runtime/src/main/java/org/apache/flink/runtime/io/network/partition/BufferReaderWriterUtil.java) -- gather write 降级处理

### Industry Consensus

**Java FileChannel.write() 可能 partial write**。这不是异常情况，而是 API 契约的正常行为。

Java 官方文档明确说明：
> "An invocation of this method may or may not transfer all of the requested bytes; whether or not it does so depends upon the natures and states of the channels."

触发 partial write 的常见原因：
1. OS 内核缓冲区满
2. 被信号中断
3. 非阻塞模式下的 channel
4. gather write (writev) 超过系统限制（Linux 为 1024 个 iovec）

### Flink 中的三种处理模式

#### 模式 1：单 buffer 循环写入（最常用）

```java
// FileUtils.writeCompletely() 和 BufferReaderWriterUtil.writeBuffer()
while (src.hasRemaining()) {
    channel.write(src);
}
```

**原理**：`channel.write()` 会自动推进 ByteBuffer 的 position，`hasRemaining()` 检查 position < limit。循环直到全部写入。

#### 模式 2：gather write + 降级

```java
// BufferReaderWriterUtil.writeBuffers()
if (bytesExpected > channel.write(buffers)) {
    for (ByteBuffer buffer : buffers) {
        writeBuffer(channel, buffer);  // 逐个循环写入
    }
}
```

**原理**：先尝试 gather write 一次写入所有 buffer。如果返回的字节数少于预期（说明发生了 partial write），降级为逐个 buffer 循环写入。

#### 模式 3：读取侧 partial read 处理

```java
// BufferReaderWriterUtil.readByteBufferFully()
do {
    if (channel.read(b) == -1) {
        throwPrematureEndOfFile();
    }
} while (b.hasRemaining());
```

**原理**：读取侧也需要循环处理。额外检查 `-1` 返回值（表示 EOF），如果文件提前结束则抛出 IOException。

### Common Pitfalls

1. **忽略 partial write**：直接调用 `channel.write(buffer)` 而不检查返回值或循环，导致数据截断。这是最常见的 NIO 使用错误。
2. **gather write 后未检查**：`channel.write(ByteBuffer[])` 的 partial write 更复杂——部分 buffer 可能完整写入，部分可能部分写入。Flink 的处理方式是降级为逐个写入（依赖 ByteBuffer 的 position 已被推进）。
3. **读取侧未区分 partial read 和 EOF**：`channel.read()` 返回 0 表示暂时无数据可读（非阻塞模式），返回 -1 表示 EOF。两者需要不同处理。
4. **手动管理 position**：应利用 ByteBuffer 自动管理 position 的特性，不要手动计算偏移量。

### Recommendation

采用 Flink 的标准 partial write 处理模式：

1. **写入**：直接复用 `FileUtils.writeCompletely()` 或采用相同的 `while(hasRemaining)` 模式
2. **gather write**：使用 `BufferReaderWriterUtil.writeBuffers()` 的模式——先尝试 gather write，失败则降级逐个写
3. **读取**：使用 `do-while` 循环 + EOF 检查的模式
4. **不要自己造轮子**：Flink 已有成熟的 partial write/read 处理工具方法，直接复用

---

## 综合建议

### 最终 I/O 方案

| 决策项 | 选择 | 理由 |
|-------|------|------|
| I/O API | FileChannel | 行业标准，与 Flink 现有代码一致 |
| Buffer 类型 | Direct ByteBuffer (header), NetworkBuffer 的 NIO view (data) | 避免 heap-to-native 拷贝，复用现有 buffer |
| Header buffer | 预分配并复用，`ByteBuffer.allocateDirect(8)` | Flink 标准做法，避免频繁 GC |
| 写入格式 | `[2B type + 2B compress + 4B length] + [data]` | 与 `BufferReaderWriterUtil` 格式一致 |
| 写入方式 | gather write (`FileChannel.write(ByteBuffer[])`) | 减少系统调用，header+data 一次写入 |
| 读取方式 | 先读 8B header 解析长度，再读对应长度数据 | Flink 标准做法 |
| Chunk size | 32KB (由 network buffer size 决定) | 与 NetworkBuffer 大小对齐 |
| fsync | 不调用 | 临时数据，丢失可重新恢复 |
| Partial write | `while(hasRemaining)` 循环 | Java NIO 标准处理模式 |
| 工具方法 | 复用 `FileUtils.writeCompletely()` 和 `BufferReaderWriterUtil` | 避免重复代码 |

### 可直接复用的 Flink 工具类

| 类 | 用途 |
|---|------|
| `FileUtils.writeCompletely()` | 处理 partial write 的循环写入 |
| `BufferReaderWriterUtil.writeToByteChannel()` | header+data 的 gather write |
| `BufferReaderWriterUtil.readFromByteChannel()` | header+data 的顺序读取 |
| `BufferReaderWriterUtil.allocatedHeaderBuffer()` | 创建 direct header buffer |
| `BufferReaderWriterUtil.readByteBufferFully()` | 处理 partial read 的循环读取 |
