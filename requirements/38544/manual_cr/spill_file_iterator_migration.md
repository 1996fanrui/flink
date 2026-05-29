# Spill File Iterator — 改造清单

设计见 [`../simplify_approach/unspiller.md` §3](../simplify_approach/unspiller.md#3-the-reader-stack-phase-2)。本文档只列**需要修改/新增/删除**的代码项。

## 新增

| 类 | 位置 | 角色 |
|---|---|---|
| `SpillFileReader`（新形态） | `flink-runtime/.../checkpoint/channel/` | 底层 forward iterator (`peek + advance + snapshot + close + asIterator`)；持 1 个 ref-count grant；按 segment 顺序前进，每个 segment 最多 open 一次；单 reusable byte buffer。 |
| `SpillFileDrainer` | `flink-runtime/.../checkpoint/channel/` | 实现 `RecoveryCheckpointTrigger`；持 `SpillFile` + root `SpillFileReader` + `allChannels` + lock；暴露 `drain()` + `snapshotAndInsertBarriers(cp)`。 |

## 重命名

| 旧 | 新 |
|---|---|
| 旧 `SpillFileReader`（drain + trigger 一类） | `SpillFileDrainer`（功能职责整体迁移） |

## 删除

- `DiskSnapshot.java` —— `RecoveryCheckpointTrigger.snapshotAndInsertBarriers` 返回类型改为 `CloseableIterator<SpillFileReader.Chunk>`；下游 `ChannelStateWriter.addInputDataFromSpill` 同步改参数类型。
- `SpillFile.snapshot()` + `SpillFile.Snapshot` 内部类。
- `SpillFile.entries` 扁平 `Deque<Entry>` 字段、`SpillFile.entries()` 方法。
- `SpillFileSegment.readBytesAt`、`SpillFileSegment.drainEntries`、`SpillFileSegment.peekNextEntry`、`SpillFileSegment.pollNextEntry`。
- `SpillFile.readSegmentBytes`（如有）/ `SpillFile.readBytes`（测试 helper）。
- `Entry.segmentIndex` 字段。

## 结构调整

- `SpillFileSegment` 内部加 `List<Entry> entries`，归属由数据结构表达。
- `SpillFile` 构造器加 `int maxEntryLength` 参数。
- `SpillFile.append` 改为把 entry 加到 active segment 的 `entries`，而非顶层 `entries` Deque。

## 接口调整

- `RecoveryCheckpointTrigger.snapshotAndInsertBarriers(long cpId)` 返回类型从 `DiskSnapshot` 改为 `CloseableIterator<SpillFileReader.Chunk>`。
- `ChannelStateWriter.addInputDataFromSpill(long cpId, ...)` 第二个参数类型从 `CloseableIterator<DiskSnapshot.Chunk>` 改为 `CloseableIterator<SpillFileReader.Chunk>`。
- `ChannelStateCheckpointWriter` / `ChannelStateWriterImpl` / `ChannelStateWriteRequest` 内引用 `DiskSnapshot.Chunk` 的位置全部改为 `SpillFileReader.Chunk`。

## StreamTask 接入

- 把构造 `SpillFileReader`（旧）+ 发布为 trigger 的两步换成：
  - 构造 `SpillFileDrainer(spillFile, allChannels)`；
  - `recoveryCheckpointTrigger = drainer`（drainer 自身实现 `RecoveryCheckpointTrigger`）；
  - 把 `drainer::drain` 提交给 `channelIOExecutor`；
  - 任务退出时调 `drainer.close()`。

## 测试

- 删 `RecoveredChannelBufferRequester*Test`（类已不存在；上一轮改造已完成）。
- 改 `SpillFileReaderTest`：测试对象是新形态 `SpillFileReader`，验证 `peek + advance + snapshot` 和 segment 单 open 不变量。
- 改 `SpillFileReaderConcurrencyTest`、`ChannelIOExecutorDrainSubmissionTest`：测试对象是 `SpillFileDrainer`。
- 删 `DiskSnapshotTest`、`SpillFileSnapshotTest`。

## phase 归属

- phase 3（`SpillFile` 文件结构调整、`maxEntryLength` ctor 参数）
- phase 4（`SpillFileReader` 新形态、`SpillFileDrainer` 新类、删 `DiskSnapshot`、`StreamTask` 接入、`ChannelStateWriter*` 接口类型调整、相关测试）
