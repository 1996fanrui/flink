# 人工 Review: b80b4b9d — Spilling Core Components

## 问题汇总

| ID | 严重度 | 类别 | 问题 | 文件 |
|----|-------|------|------|------|
| H-01 | 高 | 架构 | SpillFileReader 抽象层次错误：I/O 层不应分配 Buffer，应由消费方决定 | SpillFileReader.java |
| H-02 | 高 | 架构 | 两条读路径（Task 消费 vs Checkpoint）需求完全不同，接口未定制化 | SpillFileReader.java, SpillingBufferManager.java |
| H-03 | 高 | 架构 | 未复用 Flink 已有 spill 目录获取链路，目录来源应与 SpanningWrapper 等场景统一 | SpillingBufferManager.java |
| H-04 | 高 | 正确性 | cleanupOldAttemptFiles() 根本性错误：TM 共享目录下会误删其他 Task 文件，且职责错位 | SpillingBufferManager.java:221-238 |

---

## 详细问题

### H-01: SpillFileReader 抽象层次错误

**来源**: 用户指出"代码定义不清晰，边界以及接口不清晰"

`SpillFileReader.readNext()` 每次都 `allocateUnpooledSegment` 创建新 Heap Buffer。但对 Task 消费路径，调用方已有 Network Buffer，实际流程变成：
```
磁盘 → 新建 Heap Buffer → 复制到 Network Buffer → 回收 Heap Buffer
```
多一次无意义的内存分配 + 拷贝。

I/O 层应该只负责字节搬运，不应决定 Buffer 分配策略。应提供"读入调用方 Buffer"的能力。

### H-02: 两条读路径未定制化

**来源**: 用户指出"读数据有两种情况，一种是 Task 消费到内存，一种是 Checkpoint，没有清晰的定义"

| 维度 | Task 消费 | Checkpoint 快照 |
|------|----------|----------------|
| Buffer 来源 | 调用方提供 Network Buffer | 需要自行分配 |
| 需要 channel context? | 是 | 否 |
| 文件生命周期 | 读完即删 | 引用计数保护 |

两者在每个维度上都不同，但被强制统一成 `readNext() → SpillEntry` 一个接口。代码中已有"接口不适配"的信号——`CheckpointSpillIterator` 注释承认"Channel context is not needed"。

接口应该定制化：
- Task 消费：`replayNextTo(networkBuffer) → ChannelContext | null`
- Checkpoint：`createCheckpointIterator() → CloseableIterator<Buffer>`

### H-03: Spill 目录来源应统一

**来源**: 用户指出"不是目录复用，是代码复用——拿目录的来源应该是同一块代码"

Flink 中所有 spill 场景的目录获取链路：
```
io.tmp.dirs → IOManager.getSpillingDirectoriesPaths() → String[]
```

| 场景 | 获取方式 |
|------|---------|
| SpanningWrapper | `IOManager.getSpillingDirectoriesPaths()` via StreamTaskNetworkInput |
| ChannelStateFilteringHandler | `filterContext.getTmpDirectories()` ← `IOManager.getSpillingDirectoriesPaths()` via StreamTask:2052 |
| RescalingStreamTaskNetworkInput | `ioManager.getSpillingDirectoriesPaths()` |
| **SpillingBufferManager** | **单个 `String spillDir`，来源不明** |

问题：
1. 参数是单个 `String` 而非 `String[]`，不支持多目录轮询
2. 没有接入已有链路（`RecordFilterContext.getTmpDirectories()`）
3. 同一个 `SequentialChannelStateReaderImpl` 已有 `filterContext.getTmpDirectories()` 调用，应从同一来源获取

### H-04: cleanupOldAttemptFiles() 根本性错误

**来源**: 用户指出"一个 task 结束时自己清自己的，不可能说下一个 task 运行时顺便帮上一个清理"

**目录结构事实**：`flink-io-<UUID>/` 是 TM 级共享目录，所有 Task 的 spill 文件都在里面，没有 per-Task 子目录。

当前代码在构造时扫描删除非当前 attemptId 的文件：
1. **误删风险**：同一 TM 上多个 Task 可能同时做 channel state 恢复，Task A 构造时会删除正在运行的 Task B 的 spill 文件
2. **职责错位**：正常退出 `close()` 已清理自己的文件；异常退出应由 TM 的 shutdown hook（`FileChannelManagerImpl`）兜底
3. 这个方法应直接删除
