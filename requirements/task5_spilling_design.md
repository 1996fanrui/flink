# Task 5: 内存压力处理 - Spilling 详细设计

**汇总文档**: [split_tasks.md](./split_tasks.md)
**Jira**: [FLINK-38544](https://issues.apache.org/jira/browse/FLINK-38544)
**状态**: 开发中

---

## 1. 问题背景

### 1.1 核心问题

Task 2 的过滤逻辑在 channel-state-unspilling 线程中执行，过滤后的 Buffer 需要写入 Network Buffer。当 Network Buffer 不足时，过滤线程会阻塞在 `bufferManager.requestBufferBlocking()` 上，导致：

1. **死锁风险**：过滤线程持有部分资源并等待 Buffer，而 Task 线程可能也在等待过滤完成
2. **Checkpoint 延迟**：过滤无法推进，Checkpoint 必须等待所有 Buffer 过滤完成后才能触发

### 1.2 当前方案现状

| 方案 | Commit Message | 状态 | 说明 |
|------|---------------|------|------|
| Heap Buffer | `[FLINK-38544][checkpoint] Use heap buffer as simplified spilling logic during recovery` | 短期可用 | 简单有效，但存在 OOM 风险 |
| LazyFileBuffer | `[hotfix] 引入 LazyFileBuffer 优化 checkpoint 恢复内存使用` | 不可用 | 实现复杂，存在大量 bug，无法正常运行 |

### 1.3 为什么需要重新设计

**Heap Buffer 方案的问题：**
- 当 Network Buffer 不足时，直接分配堆内存 (`MemorySegmentFactory.allocateUnpooledSegment`)
- 无内存上限控制，大规模恢复场景下可能 OOM
- 堆内存 Buffer 不受 Network Memory 配额管理

**LazyFileBuffer 方案的问题：**
- 实现了完整的 `Buffer` 接口，但 Buffer 接口本身不是为文件后备设计的
- 写入和读取两个阶段的状态管理复杂，容易出错
- 与 `ChannelStateFilteringHandler`、`ChannelStateSerializer`、`RecoveredChannelStateHandler` 等多个组件耦合
- 存在大量运行时 bug，完全无法工作

---

## 2. 设计目标

1. 当 Network Buffer 不足时，过滤线程不阻塞，继续从 S3 读取并过滤数据
2. 过滤后的数据可以落盘（Spill），Buffer 可用时从磁盘读回（Replay）
3. Checkpoint 时，磁盘上的已过滤数据也能被正确快照
4. 实现简单可靠，避免 LazyFileBuffer 的复杂度

## 3. 整体架构

### 3.1 三条数据路径

详见 [task5_data_flow.md](./task5_data_flow.md)（含 Mermaid 流程图）。

```
P1 (Memory Path):     S3 → Filter → Network Buffer → InputChannel
P2 (Spill Path):      S3 → Filter → Local Disk File
P3 (Replay Path):     Local Disk File → Network Buffer → InputChannel
```

P2 和 P3 是配对的——走了 P2 的数据必须经过 P3 才能进入 InputChannel。磁盘有数据时 P3 优先于 P1，保证数据顺序。

### 3.2 两阶段处理流程

**Phase 1: S3 Active Loop**（S3 还有数据时）

```
loop:
    buffer = tryRequestBuffer()  // 非阻塞
    if buffer != null:
        if diskHasData:
            P3: disk → buffer → InputChannel   // 优先清磁盘
        else:
            P1: S3 → filter → buffer → InputChannel
    else:
        P2: S3 → filter → disk                 // Buffer 不足时 spill 到磁盘
```

**Phase 2: Disk Cleanup Loop**（S3 已读完，只剩磁盘数据）

```
loop (until disk empty):
    buffer = requestBufferBlocking()  // 阻塞等待
    P3: disk → buffer → InputChannel
```

Phase 2 开始意味着所有 S3 数据已过滤完毕，可以接受 Checkpoint。

### 3.3 与现有架构的关系

```
                    ┌─────────────────────────────┐
                    │  ChannelStateFilteringHandler │
                    │  (过滤逻辑，不变)              │
                    └──────────┬──────────────────┘
                               │ 产出: List<Buffer>
                               ▼
                    ┌─────────────────────────────┐
                    │    SpillingBufferManager     │  ← 新增组件
                    │  - 决定写入 Memory 还是 Disk  │
                    │  - 管理 Spill/Replay          │
                    └──────────┬──────────────────┘
                               │
                  ┌────────────┴────────────┐
                  ▼                         ▼
          Network Buffer              Local Disk File
          (→ InputChannel)            (→ 等待 Replay)
```

---

## 4. 核心设计

### 4.1 SpillingBufferManager

新增 `SpillingBufferManager`，负责管理 Spill/Replay 逻辑。核心职责：

1. **Buffer 分配决策**：非阻塞 `tryRequestBuffer()`，有则返回，无则返回 null
2. **Spill 管理**：将已过滤的 Buffer 数据写入磁盘文件
3. **Replay 管理**：从磁盘文件读取数据到 Network Buffer
4. **状态查询**：是否有磁盘数据待 replay、是否所有 S3 数据已处理

```java
public class SpillingBufferManager implements Closeable {

    private final BufferSupplier bufferSupplier;
    private final File spillDirectory;

    // Spill 文件队列（FIFO）
    private final Queue<SpillFile> spillFiles;

    // 当前正在写入的 spill 文件
    @Nullable private SpillFileWriter currentWriter;

    // 当前正在读取的 spill 文件
    @Nullable private SpillFileReader currentReader;

    /** 非阻塞请求 Buffer */
    @Nullable
    public Buffer tryRequestBuffer();

    /** 阻塞请求 Buffer（仅 Phase 2 使用） */
    public Buffer requestBufferBlocking();

    /** 将已过滤的 Buffer 数据 spill 到磁盘 */
    public void spillBuffers(List<Buffer> filteredBuffers);

    /** 从磁盘 replay 数据到 Buffer，返回可放入 InputChannel 的 Buffer 列表 */
    public List<Buffer> replayToBuffer(Buffer targetBuffer);

    /** 是否有磁盘数据待 replay */
    public boolean hasDiskData();

    /** 关闭并清理所有临时文件 */
    @Override
    public void close();
}
```

### 4.2 Spill 文件格式

采用简单的顺序写入格式，每个 Buffer 的数据按以下格式存储：

```
[4 bytes: buffer data length][N bytes: buffer data][1 byte: isEvent flag]
[4 bytes: buffer data length][N bytes: buffer data][1 byte: isEvent flag]
...
```

- 使用 `FileChannel` 顺序写入，性能可接受
- 每个 spill 文件大小上限（如 64MB），超过后创建新文件
- Replay 时按顺序读取，读完一个文件后删除

### 4.3 与 ChannelStateFilteringHandler 的集成

当前 `ChannelStateFilteringHandler.filterAndRewrite()` 内部通过 `BufferSupplier` 申请 Buffer。改造方式：

**不改变 `ChannelStateFilteringHandler` 的接口**，而是在调用层（`RecoveredChannelStateHandler`）集成 spilling 逻辑：

```
// 伪代码：RecoveredChannelStateHandler 中的处理逻辑
void handleBuffer(Buffer sourceBuffer) {
    if (spillingManager.hasDiskData()) {
        // 磁盘有数据时，先尝试 replay
        Buffer buf = spillingManager.tryRequestBuffer();
        if (buf != null) {
            List<Buffer> replayed = spillingManager.replayToBuffer(buf);
            addToInputChannel(replayed);  // P3
        }
    }

    // 过滤当前 S3 buffer
    List<Buffer> filtered = filterHandler.filterAndRewrite(..., bufferSupplier);

    Buffer buf = spillingManager.tryRequestBuffer();
    if (buf != null && !spillingManager.hasDiskData()) {
        // 有 Buffer 且磁盘无数据：走 P1，直接放入 InputChannel
        addToInputChannel(filtered);
    } else {
        // 无 Buffer 或磁盘有数据：走 P2，spill 到磁盘
        spillingManager.spillBuffers(filtered);
        recycleFilteredBuffers(filtered);
    }
}
```

**注意**：上述伪代码需要进一步推敲 `filterAndRewrite` 内部的 `BufferSupplier` 行为。当走 P2 时，`filterAndRewrite` 仍需要临时 Buffer 来反序列化和过滤，但过滤后的结果不需要留在 Network Buffer 中——可以 spill 到磁盘后释放。

### 4.4 Checkpoint 期间的处理

当 Checkpoint 触发时，需要快照的数据包括：
1. InputChannel 中的 Network Buffer（已有逻辑处理）
2. 磁盘上的已过滤数据（需要新增逻辑）

**方案**：Checkpoint 时，磁盘上的 spill 文件作为额外的 channel state 上传到 Checkpoint Storage。

具体机制待进一步设计，需要考虑：
- Spill 文件是否需要与 InputChannel 的 Buffer 一起上传
- 恢复时如何区分 Network Buffer 和 Spill 文件的数据

---

## 5. 已知待解决问题

1. **`ChannelStateFilteringHandler.filterAndRewrite` 内部的 Buffer 申请**：当前 `filterAndRewrite` 通过 `BufferSupplier` 申请 Buffer 来存放过滤结果。如果走 P2 路径，这些 Buffer 是临时的（过滤后 spill 到磁盘再释放）。需要确认是否可以复用同一个 Buffer 反复写入和 spill。

2. **Checkpoint 与 Spill 文件的交互**：Phase 1 期间触发 Checkpoint 时，磁盘上可能有未 replay 的数据。这些数据如何参与 Checkpoint 快照？

3. **内存上限控制**：Heap Buffer 方案无内存上限，Spill 方案的临时 Buffer 使用量如何控制？

4. **错误处理**：磁盘写入失败时的 fallback 策略。

---

## 6. 开发计划

### Phase 1：移除 LazyFileBuffer，保留 Heap Buffer 作为临时方案

- 清理 LazyFileBuffer 相关代码（不可用，增加维护负担）
- Heap Buffer 方案作为短期兜底，保证功能可用

### Phase 2：实现 Spilling 逻辑

- 实现 `SpillingBufferManager`（Spill/Replay 管理）
- 改造 `RecoveredChannelStateHandler` 集成 P1/P2/P3 路径
- 实现 Spill 文件的读写

### Phase 3：Checkpoint 与 Spill 集成

- 实现 Checkpoint 时上传磁盘数据
- 恢复时处理 Spill 文件数据

---

## 7. 参考

- [FLIP-547 需求文档 4.5 节](./requirement.md) - 过滤逻辑和三条数据路径
- [split_tasks.md - Task 5](./split_tasks.md) - 任务概述
- [Task 2 设计文档](./FLINK-38930-specs/design.md) - 过滤逻辑的基础设计
