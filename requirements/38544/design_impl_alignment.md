# FLINK-38544 设计文档 vs 代码实现 对齐检查

> 本文档仅记录设计文档与实际代码之间的**不一致项**。设计文档说 X，代码做了 Y，则记录。
> 不评价实现是否合理，不提供改进建议。


## 实现计划 vs 实际提交

> 注：原 C1（FLINK-39519）在 review 中一度被拆成两步 — C1（heap 分配，保留 FLINK-39519）与 C2（buffer 请求接口，归属 FLINK-39524/Integration）。当前 PR 已将两部分合入同一个 FLINK-39519 JIRA；此表保留 7 行结构以便追溯历史讨论。

| 计划 | JIRA | 内容 |
|------|------|------|
| C1 | FLINK-39519 | Source Buffer Heap 分配（单 segment 复用 + 运行时检查） |
| C2 | FLINK-39524 | Buffer 请求接口（`requestBuffer()` + 删 `requestBufferBlocking` heap fallback）；合并前重排到 C7 旁边 |
| C3 | FLINK-39520 | SpillFile I/O + RecoveredBufferStore |
| C4 | FLINK-39521 | OutputWriter 三条数据路径 + drain 循环 |
| C5 | FLINK-39522 | InputChannel 从 RecoveredBufferStore 消费 |
| C6 | FLINK-39523 | ChannelStateWriter streaming overload（checkpoint） |
| C7 | FLINK-39524 | 集成：filterAndRewrite 写入 OutputWriter |

## 总览

| 阶段 | 不一致项数 |
|------|-----------|
| C1 | 0（按新设计重新实现） |
| C2 | — |
| C3 | 3 |
| C4 | 0 |
| C5 | 2 |
| C6 | 1 |
| C7 | 1 |
| **合计** | **7** |

---

## C1: Source Buffer Heap 分配（重新设计）

原 C1（使用 `Semaphore(5)` + `MAX_HEAP_BUFFERS_PER_GATE`）在 review 中被证明过度设计：Flink 的 `ChannelStateChunkReader` 串行消费 + `SpillingAdaptiveSpanningRecordDeserializer` 的 `isBufferConsumed` 立即回收 + `SpanningWrapper` 跨 buffer 字节总是 copy-out，结构性保证任意时刻最多 1 个 source buffer in-flight。原 C1 被完全重写：

- 单 `MemorySegment` per task（首次 `getBuffer()` 懒初始化），反复复用
- 自定义 BufferRecycler 翻转 `inUse` 标志
- `getBuffer()` 前 assert `!inUse`，违反则抛 `IllegalStateException`
- 移除 `Semaphore[]`、`MAX_HEAP_BUFFERS_PER_GATE` 常量、REQ-QY68 中原有的 "5 per gate" 语义（REQ-QY68 已重写为"复用 + 运行时检查"）

## C2: Buffer 请求接口（原 C1 的一部分，归属 FLINK-39524）

拆分自原 C1，归属 **FLINK-39524 Integration** ticket：
- 新增 `requestBuffer()` 非阻塞接口
- `requestBufferBlocking()` 删 filtering 模式下的 heap fallback

此部分在合并前会被重排到 C7 旁边（同 ticket，同逻辑阶段）。heap fallback 只有在 OutputWriter 接入后才能安全删除，因此归属 Integration 最符合语义。

---

## C3: SpillFile I/O + RecoveredBufferStore

| # | 设计文档描述 | 实际代码实现 | 出处 | 状态 |
|---|-------------|-------------|------|------|
| 1 | SpillEntry 结构为 3 字段：`{InputChannelInfo channelInfo, long offset, int length}` | ~~4 字段~~ → 3 字段 | design.md "SpillEntry 结构" 节；`SpillEntry.java` | **已修复** |
| 2 | closed 状态下 write 抛 **IllegalStateException** | ~~IOException~~ → IllegalStateException | design.md REQ-JD2C；`FilteredSpillFile.write()` | **已修复** |
| 3 | 写入使用 **FileUtils.writeCompletely()** | ~~自行循环~~ → FileUtils.writeCompletely() | spill_io_patterns.md 决策表；`FilteredSpillFile.write()` | **已修复** |

---

## C4: OutputWriter 三条数据路径 + drain 循环

无不一致项。所有设计要求（P1/P2/P3 路径判定、writeToBackend 仅降级、channel 变更检测、flush/close 行为、drain 逻辑、幂等性、资源清理）均与设计文档一致。

---

## C5: InputChannel 从 RecoveredBufferStore 消费

| # | 设计文档描述 | 实际代码实现 | 出处 |
|---|-------------|-------------|------|
| 1 | **删除** `onRecoveredStateBuffer()` 方法，OutputWriter 通过 store.addBuffer() 直接投递 | 方法**保留**，内部改为委托 `store.addBuffer(buffer)` | design.md REQ-7388；`RecoveredInputChannel.java` |
| 2 | **删除** RemoteInputChannel 中的 `checkReadability()` hack | 方法**保留**，仅更新了注释 | design.md REQ-G4KW；`RemoteInputChannel.java` |

---

## C6: ChannelStateWriter streaming overload

| # | 设计文档描述 | 实际代码实现 | 出处 |
|---|-------------|-------------|------|
| 1 | `writeInputStreaming()` 通过 **InputStream.transferTo(DataOutputStream)** 流式拷贝 | 使用 **8KB byte[] 手动循环** 读写 | design.md "Checkpoint 写入管线扩展" 节；`ChannelStateCheckpointWriter.writeInputStreaming()` |

---

## C7: 集成 — filterAndRewrite 写入 OutputWriter

| # | 设计文档描述 | 实际代码实现 | 出处 |
|---|-------------|-------------|------|
| 1 | OutputWriter 构造参数 memorySegmentSize 来自**运行时 buffer 大小配置** | 硬编码为 **MemoryManager.DEFAULT_PAGE_SIZE**（32KB 常量） | design.md OutputWriter 构造参数表；`SequentialChannelStateReaderImpl.createOutputWriter()` |

---

## 不一致项分类汇总

### 接口/结构不一致（设计文档定义的 API 或数据结构与实现不同）

| # | 项目 | 状态 |
|---|------|------|
| C3-1 | SpillEntry 3 字段 vs 4 字段 | **已修复** |
| C3-2 | closed write 抛 IllegalStateException vs IOException | **已修复** |
| C5-1 | onRecoveredStateBuffer() 设计要求删除，实际保留 | 待修复 |
| C5-2 | checkReadability() hack 设计要求删除，实际保留 | 待修复 |

### 实现机制不一致（功能等价但技术手段不同）

| # | 项目 | 状态 |
|---|------|------|
| C3-3 | FileUtils.writeCompletely() vs 自行循环 | **已修复** |
| C6-1 | InputStream.transferTo() vs 8KB 手动循环 | **已更新设计文档**（采纳 8KB 循环） |

### 配置来源不一致（应动态获取但硬编码了默认值）

| # | 项目 | 状态 |
|---|------|------|
| C7-1 | memorySegmentSize 硬编码 DEFAULT_PAGE_SIZE | 待修复 |
