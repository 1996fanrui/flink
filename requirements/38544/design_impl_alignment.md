# FLINK-38544 设计文档 vs 代码实现 对齐检查

> 本文档仅记录设计文档与实际代码之间的**不一致项**。设计文档说 X，代码做了 Y，则记录。
> 不评价实现是否合理，不提供改进建议。


 6 个 Commit 的实现计划 vs 实际提交

  ┌──────┬─────────────────────────────────────────────────────┬──────────────────────────────────────────┐
  │ 计划  │                        内容                          │             对应实际 Commit              │
  ├──────┼─────────────────────────────────────────────────────┼──────────────────────────────────────────┤
  │ C1   │ Source Buffer Heap 分配 + buffer 请求接口             │ e55a7f1                                  │
  ├──────┼─────────────────────────────────────────────────────┼──────────────────────────────────────────┤
  │ C2   │ SpillFile I/O + RecoveredBufferStore                │ 44c700b                                  │
  ├──────┼─────────────────────────────────────────────────────┼──────────────────────────────────────────┤
  │ C3   │ OutputWriter 三条数据路径 + drain 循环                │ d911490                                  │
  ├──────┼─────────────────────────────────────────────────────┼──────────────────────────────────────────┤
  │ C4   │ InputChannel 从 RecoveredBufferStore 消费            │ 90c4e49                                  │
  ├──────┼─────────────────────────────────────────────────────┼──────────────────────────────────────────┤
  │ C5   │ ChannelStateWriter streaming overload（checkpoint）  │ c379f4b                                  │
  ├──────┼─────────────────────────────────────────────────────┼──────────────────────────────────────────┤
  │ C6   │ 集成：filterAndRewrite 写入 OutputWriter              │ e8ee9b2                                  │
  └──────┴─────────────────────────────────────────────────────┴──────────────────────────────────────────┘

## 总览

| 阶段 | 不一致项数 |
|------|-----------|
| C1 | 2 |
| C2 | 3 |
| C3 | 0 |
| C4 | 2 |
| C5 | 1 |
| C6 | 1 |
| **合计** | **9** |

---

## C1: Source Buffer Heap 分配 + buffer 请求接口

| # | 设计文档描述 | 实际代码实现 | 出处 |
|---|-------------|-------------|------|
| 1 | per-gate 并发控制使用 **AtomicInteger** 计数器，allocate 时 increment，recycle 时 decrement | 使用 **Semaphore(5)** 控制并发数，acquire on allocate，release on recycle | design.md "Source Buffer 隔离" 节；`RecoveredChannelStateHandler.java` |
| 2 | Heap Buffer 大小为 **memorySegmentSize**（与 Network Buffer 对齐） | 硬编码为 **MemoryManager.DEFAULT_PAGE_SIZE**（32KB 常量），未从运行时配置获取 | design.md REQ-NHLB；`RecoveredChannelStateHandler.getHeapBuffer()` |

---

## C2: SpillFile I/O + RecoveredBufferStore

| # | 设计文档描述 | 实际代码实现 | 出处 |
|---|-------------|-------------|------|
| 1 | SpillEntry 结构为 3 字段：`{InputChannelInfo channelInfo, long offset, int length}` | 4 字段：增加了 `SpillFileReader fileReader` | design.md "SpillEntry 结构" 节；`SpillEntry.java` |
| 2 | closed 状态下 write 抛 **IllegalStateException** | 抛 **IOException** | design.md REQ-JD2C；`SpillFileWriter.write()` |
| 3 | 写入使用 **FileUtils.writeCompletely()** | 自行实现 `while(bb.hasRemaining()) { channel.write(bb); }` 循环 | spill_io_patterns.md 决策表；`SpillFileWriter.write()` |

---

## C3: OutputWriter 三条数据路径 + drain 循环

无不一致项。所有设计要求（P1/P2/P3 路径判定、writeToBackend 仅降级、channel 变更检测、flush/close 行为、drain 逻辑、幂等性、资源清理）均与设计文档一致。

---

## C4: InputChannel 从 RecoveredBufferStore 消费

| # | 设计文档描述 | 实际代码实现 | 出处 |
|---|-------------|-------------|------|
| 1 | **删除** `onRecoveredStateBuffer()` 方法，OutputWriter 通过 store.addBuffer() 直接投递 | 方法**保留**，内部改为委托 `store.addBuffer(buffer)` | design.md REQ-7388；`RecoveredInputChannel.java` |
| 2 | **删除** RemoteInputChannel 中的 `checkReadability()` hack | 方法**保留**，仅更新了注释 | design.md REQ-G4KW；`RemoteInputChannel.java` |

---

## C5: ChannelStateWriter streaming overload

| # | 设计文档描述 | 实际代码实现 | 出处 |
|---|-------------|-------------|------|
| 1 | `writeInputStreaming()` 通过 **InputStream.transferTo(DataOutputStream)** 流式拷贝 | 使用 **8KB byte[] 手动循环** 读写 | design.md "Checkpoint 写入管线扩展" 节；`ChannelStateCheckpointWriter.writeInputStreaming()` |

---

## C6: 集成 — filterAndRewrite 写入 OutputWriter

| # | 设计文档描述 | 实际代码实现 | 出处 |
|---|-------------|-------------|------|
| 1 | OutputWriter 构造参数 memorySegmentSize 来自**运行时 buffer 大小配置** | 硬编码为 **MemoryManager.DEFAULT_PAGE_SIZE**（32KB 常量） | design.md OutputWriter 构造参数表；`SequentialChannelStateReaderImpl.createOutputWriter()` |

---

## 不一致项分类汇总

### 接口/结构不一致（设计文档定义的 API 或数据结构与实现不同）

| # | 项目 |
|---|------|
| C2-1 | SpillEntry 3 字段 vs 4 字段 |
| C2-2 | closed write 抛 IllegalStateException vs IOException |
| C4-1 | onRecoveredStateBuffer() 设计要求删除，实际保留 |
| C4-2 | checkReadability() hack 设计要求删除，实际保留 |

### 实现机制不一致（功能等价但技术手段不同）

| # | 项目 |
|---|------|
| C1-1 | AtomicInteger vs Semaphore |
| C2-3 | FileUtils.writeCompletely() vs 自行循环 |
| C5-1 | InputStream.transferTo() vs 8KB 手动循环 |

### 配置来源不一致（应动态获取但硬编码了默认值）

| # | 项目 |
|---|------|
| C1-2 | Heap Buffer 大小硬编码 DEFAULT_PAGE_SIZE |
| C6-1 | memorySegmentSize 硬编码 DEFAULT_PAGE_SIZE |
