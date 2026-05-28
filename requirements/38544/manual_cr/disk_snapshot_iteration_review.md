# SpillFile 读路径改造

涉及代码：`flink-runtime/.../checkpoint/channel/{SpillFile, DiskSnapshot, SpillFileReader}.java`（phase 3 / phase 4）。

## 目标

drain 和 snapshot 是同一套消费抽象：都按 `(startSegmentIndex, startOffset)` 顺序读 `SpillFile`，读到末尾结束。drain 的起点是 `(0, 0)`、空快照；snapshot 的起点是当前 drain 游标。两条链路共用同一份遍历实现。

满足以下硬约束：

1. **每个 segment 文件，单次消费只打开一次。**  
   drain 一轮 → 每个 segment open 一次；每次 snapshot → 每个 segment 最多 open 一次。多次 snapshot 各自独立打开是接受的。

2. **每次消费仅一个 reusable buffer。**  
   消费体（drain / 单个 `DiskSnapshot`）构造时按"单条 entry 最大字节数 / `MemorySegment` 大小"分配一块 `byte[]`（或 `MemorySegment`），整个遍历过程中所有 entry 都读进这同一块 buffer。`Chunk.data` 永远指向它，仅 `length` 每次更新。禁止逐 entry `new byte[...]`。

3. **顺序流式读取。**  
   一个 segment 内部按 entry 顺序从头读到尾，到末尾关流、切下一个 segment。代码结构本身就要体现"两层遍历：外层 segment，内层 entry"。

## 设计要点

- 在 `SpillFile`（或 segment）一层抽象出一个读接口（暂名 `SpillFileCursor` / `SpillFileReader`），输入 `(startSegmentIndex, startOffset)`，输出按顺序的 `Chunk` 迭代，`close()` 释放当前打开的 segment 句柄。
- drain 链路：用这个接口，起点 `(0, 0)`，遍历到结尾。
- snapshot 链路：`DiskSnapshot` 内部就是这个接口的一个实例，起点是创建时捕获的 `(currentSegmentIndex, currentOffset)`。
- caller 契约：必须在调用下一次迭代前消费完上一个 `Chunk.data`，不允许保存引用——因为 buffer 会被覆盖。

## 影响

- 改动文件：`SpillFile.java`、`DiskSnapshot.java`、`SpillFileReader.java`。
- `Chunk` 对外字段（`channelInfo` / `data` / `length`）不变，但 ownership 语义收紧。下游（`ChannelStateWriterImpl.addInputDataFromSpill` 等）需核对是否保存了 `Chunk.data` 的引用。
- 测试：复用现有 `SpillFileTest` / `SpillFileReaderConcurrencyTest` / `SpillFileSnapshotTest` / `DiskSnapshotTest`，不新增。
