# 设计：Phase 3 — Spill 写盘侧（filter 阶段）

> 范围：实施 [`simplify_approach/unspiller.md`](../simplify_approach/unspiller.md) §2 / §2a / §2b 中描述的 filter 阶段；新增 `SpillFile`、`FilteredBufferWriter`、`SpillFileWriter` 三个 phase-1 类；改写 `RecoveredChannelStateHandler.recover` 的 filter 分支，把"塞 channel"换成"写盘"。**不改任何 InputChannel 内部字段或方法**——本 phase 与 Phase 2 完全并行。

## 1. 设计目标

满足 [`user_requirements.md`](./user_requirements.md) 中 REQ-3P7A ~ REQ-9JHL。完成后：

- filter 阶段（filter-on 路径）将所有过滤后 buffer 写入 `SpillFile`，task 内存占用稳定为 1 prefilter + 1 postfilter，无 heap fallback
- `RecoveredInputChannel.requestBufferBlocking` 的 heap fallback 暂时保留（filter 路径不再触达），由 Phase 4 在 drain 路径接入后真正删除
- filter 完成后产出的 `SpillFile` 对象作为 Phase 4 `SpillFileReader` 的输入

## 2. 现状分析

- `o.a.f.runtime.checkpoint.channel.RecoveredChannelStateHandler.recover` 是 filter 阶段主入口；filter-on 时调用 `ChannelStateFilteringHandler.filterAndRewrite(...)`，其 `bufferSupplier` 由调用方传入。
- 当前 `RecoveredChannelStateHandler.recover` 的 filter 分支把 `bufferSupplier` 绑定到 `channel::requestBufferBlocking`（见 `RecoveredChannelStateHandler.java:220` 附近）。filter 输出最终通过 `channel.onRecoveredStateBuffer(buf)` 落到 channel。
- master 已经存在 `bufferFilteringCompleteFuture` 字段，filter 完成时 complete。
- master `ChannelStateWriterImpl` 内部已有"按段写盘 + 段大小"风格代码可作为 `SpillFile` 段轮转的参考实现，但本设计**不复用**其代码（语义和生命周期不同），仅借鉴段大小默认值（64 MiB）。
- master 没有现成的 `SpillFile` 类型 / 概念；本 phase 直接新建。

## 3. 修改范围

### 3.1 新增类：`SpillFile`

路径：`flink-runtime/src/main/java/org/apache/flink/runtime/checkpoint/channel/SpillFile.java`

职责：管理一个或多个 `SpillFileSegment`（每段对应一个磁盘文件），单线程写入，提供 `entries` 内存视图。

```java
public final class SpillFile implements Closeable {

    /** 段大小默认 64 MiB：与 master `ChannelStateWriterImpl` 既有段大小风格对齐；
     *  Flink 流处理中 64 MiB 是段文件常见默认（参 RocksDB memtable / Flink ChannelStateWriterImpl）；
     *  在 OS 层 IO 调度与 channelIOExecutor 顺序写之间形成的平衡点。 */
    public static final long DEFAULT_SEGMENT_SIZE_BYTES = 64L * 1024 * 1024;

    static final class SpillFileSegment implements Closeable {
        final int segmentIndex;
        final java.nio.file.Path path;
        final FileChannel channel;
        long currentEnd;            // 已写字节数
        // peekNextEntry / pollNextEntry / readBytesAt 等读取入口在 Phase 4 引入
        ...
    }

    static final class Entry {
        final InputChannelInfo channelInfo;
        final int segmentIndex;
        final long offset;
        final int length;
    }

    private final java.nio.file.Path baseDir;
    private final long segmentSizeBytes;
    private final List<SpillFileSegment> segments = new ArrayList<>();
    private final Deque<Entry> entries = new ArrayDeque<>();
    private boolean closed = false;

    public SpillFile(java.nio.file.Path baseDir, long segmentSizeBytes);
    public SpillFile(java.nio.file.Path baseDir);   // 默认段大小

    /** 调用方：单线程（filter 阶段的 channelIOExecutor）。close() 之后调用抛 IllegalStateException。 */
    public void append(InputChannelInfo channelInfo, ByteBuffer payload) throws IOException;

    /** 段快照与读取入口由 Phase 4 引入；本 phase 仅提供 entries 数量 / 段数量等只读 getter 供测试用。 */

    @Override public void close() throws IOException;
}
```

- 段轮转：`append` 写入前若 `current.currentEnd + payload.remaining() > segmentSizeBytes` 则切新段（保证单条 entry 不跨段；payload 大小理论上 ≤ 单 buffer 大小，远小于 64 MiB）
- `entries` 每次 append 后追加一条 `(channelInfo, segmentIndex, offset, length)`；offset 是写入时该段已写字节数
- `close()` 关闭所有段 FileChannel；幂等；置 `closed=true`，之后 `append` 抛 `IllegalStateException`

### 3.2 新增类：`FilteredBufferWriter`

路径：`flink-runtime/src/main/java/org/apache/flink/runtime/checkpoint/channel/FilteredBufferWriter.java`

职责：用一对 prefilter / postfilter buffer 完成 filter 期间的"读源数据 / 累加输出"，输出满即 flush 到 `SpillFile`。

```java
public final class FilteredBufferWriter implements Closeable {

    private final SpillFile spillFile;
    private final Buffer prefilterBuffer;     // filter 读源数据时使用
    private Buffer postfilterBuffer;          // filter 输出累加；满则 flush 后从 buffer pool 取下一块
    private final BufferPoolHook bufferPoolHook;  // 用于在 flush 后获取新的 postfilterBuffer
    private boolean closed = false;

    public FilteredBufferWriter(SpillFile spillFile,
                                Buffer prefilterBuffer,
                                Buffer initialPostfilterBuffer,
                                BufferPoolHook bufferPoolHook);

    /** 供 ChannelStateFilteringHandler.filterAndRewrite 通过 bufferSupplier 路径获取 prefilter buffer。
     *  始终返回同一个实例（不会换 buffer，避免占用 buffer pool 槽位）。 */
    public Buffer getPrefilterBuffer();

    /** 调用方：filter 输出回调，单线程（channelIOExecutor）。
     *  把 buf 的可读字节累加到 postfilterBuffer；累加后 postfilterBuffer 满则 flush 到 spillFile 并取新 buffer。 */
    public void write(InputChannelInfo channelInfo, Buffer buf) throws IOException;

    /** flush postfilterBuffer 剩余内容，然后关闭 spillFile。 */
    @Override public void close() throws IOException;

    interface BufferPoolHook {
        /** 调用方：filter 单线程；阻塞直到 buffer 可用。不允许 heap fallback。 */
        Buffer requestPostfilterBuffer() throws InterruptedException, IOException;
    }
}
```

- `prefilterBuffer` 与初始 `postfilterBuffer` 由调用方提供；调用方负责在 filter 启动时从 task-level pre-allocated pool 申请 2 个 buffer，传入构造器
- `BufferPoolHook` 抽象出"如何获取下一个 postfilter buffer"——简单实现可直接复用某个 channel 的 `requestBufferBlocking()`（任意一个，因为 prefilter / postfilter 是 task 级而非 channel 级），但内部禁止 heap fallback；具体实现挂在 Phase 4 的 `RecoveredChannelBufferRequester` 边界外，避免与 drain 路径耦合。当前 phase 直接使用 master 既有 `RecoveredInputChannel.requestBufferBlocking()` 路径（heap fallback 仍存在但 prefilter / postfilter 阶段不应触达，因为只用 2 个 buffer，远小于 pool 容量）
- `write(InputChannelInfo, Buffer)` 累加逻辑：
  1. 取 `buf` 可读字节区间
  2. 若 `postfilterBuffer` 剩余容量 ≥ 可读字节数，直接 copy 进去
  3. 否则填满 postfilterBuffer 后 flush 到 `SpillFile`，再申请新 postfilterBuffer 并继续 copy 剩余字节；可能多次循环
  4. 每次 flush 调用 `spillFile.append(channelInfo, postfilterBuffer)`；注意 entries 记录的是 flush 时的状态，因此**一次 `write` 调用可能产生多条 entry**（跨 buffer boundary 的情形）

### 3.3 新增类：`SpillFileWriter`

路径：`flink-runtime/src/main/java/org/apache/flink/runtime/checkpoint/channel/SpillFileWriter.java`

```java
public final class SpillFileWriter implements Closeable {
    private final SpillFile spillFile;
    private final FilteredBufferWriter accumulator;
    private boolean closed = false;

    public SpillFileWriter(SpillFile spillFile, FilteredBufferWriter accumulator);

    public void write(InputChannelInfo channelInfo, Buffer buf) throws IOException;

    @Override public void close() throws IOException;     // 幂等；委托 accumulator.close() 后 spillFile.close()

    /** Phase 4 取走 SpillFile：close() 调用之前可由调用方读取该引用。 */
    public SpillFile getSpillFile();
}
```

这是一个薄 façade；其存在意义在于：

- 把"prefilter / postfilter accumulator"与"SpillFile lifecycle"两个职责合并暴露给 filter 路径
- Phase 4 的 `SpillFileReader` 只接收 `SpillFile`，不知道 writer 侧的 accumulator——边界清晰

注：`getSpillFile()` 是设计阶段从 REQ-GQHL"`SpillFileWriter` 是 phase 1 façade，持有 `SpillFile`" 自然引出的访问器（caller 在 `writer.close()` 之前需要取出 `SpillFile` 引用交给 Phase 4 的 `SpillFileReader`）；user_requirements.md 未单列，但属于 REQ-GQHL 的实现细节。

### 3.4 改 `RecoveredChannelStateHandler.recover`

仅 filter 分支改写；filter-off 分支不动。

```java
// 大致形态（伪代码）：
if (filter is on) {
    FilteredBufferWriter accumulator = new FilteredBufferWriter(
        spillFile, prefilterBuffer, initialPostfilterBuffer, bufferPoolHook);
    SpillFileWriter writer = new SpillFileWriter(spillFile, accumulator);

    try {
        filteringHandler.filterAndRewrite(
            state,
            channelInfoSupplier,
            accumulator::getPrefilterBuffer,                  // bufferSupplier 切到 prefilter buffer
            (channelInfo, filteredBuf) -> writer.write(channelInfo, filteredBuf));   // destination 切到 writer
    } finally {
        writer.close();                                       // bufferFilteringCompleteFuture 触发前必然 close
    }

    // 完成后由 caller 取走 spillFile 供 Phase 4 的 drain
    ...
} else {
    // master 既有：filter-off 路径
    ...
}
```

具体接入点 / 构造时机 / `spillFile` 实例如何传给 caller，**由开发阶段在实际代码上确定**——本设计仅约束语义。可能的实现位置：

- `SequentialChannelStateReaderImpl.readInputData` 内（filter 调用栈起点）
- 或在 `ChannelStateFilteringHandler.createFromContext` 后立即包装

`bufferFilteringCompleteFuture` 本身是 master 既有 future，触发位置不动。`writer.close()` 必须在 `bufferFilteringCompleteFuture.complete()` 之前发生，本设计通过 try-finally 保证。

### 3.5 不变之处

- `RecoveredInputChannel.requestBufferBlocking()` 方法体（含 heap fallback）保持不动 —— Phase 4 才删除
- `RecoveredInputChannel.onRecoveredStateBuffer` / `finishReadRecoveredState`（filter-off 路径仍使用）不动
- `ChannelStateFilteringHandler.filterAndRewrite` 方法签名与内部行为不动（filter destination / bufferSupplier 已经是接口参数，本 phase 只是换实参）
- `bufferFilteringCompleteFuture` 字段与触发位置不动

## 4. 不变式

- **单线程写入**：`SpillFile.append`、`FilteredBufferWriter.write`、`SpillFileWriter.write` 全部只能由 `channelIOExecutor` 调用——单线程保证；本 phase 不引入锁
- **常数内存上界**：filter 期间额外内存占用稳定为 prefilter (1 buffer) + postfilter (1 buffer at most + 1 in transition during flush)；磁盘段连续写入；无 heap fallback
- **段不跨 entry**：一条 entry 的 payload 完全落在单段内（payload 长度 ≤ buffer 大小 << 64 MiB）
- **`close()` 幂等**：所有 `close()` 方法重复调用安全

## 5. 代码组织

新增 3 个文件（`SpillFile.java`、`FilteredBufferWriter.java`、`SpillFileWriter.java`）；修改 1 个现有文件（`RecoveredChannelStateHandler.java`）。测试新增：

- `SpillFileTest.java`
- `FilteredBufferWriterTest.java`
- `SpillFileWriterTest.java`
- `RecoveredChannelStateHandlerFilterRoutingTest.java`（或在现有 filter 相关测试基础上扩展）

**提交策略**：本 phase 与其他 4 个 phase 共用同一开发分支，**不发 PR**；完成后作为**单一 commit** 推到分支，禁止 `git commit --amend` / `git rebase -i` 重写历史。完整规则参 [`../simplify_approach/task_breakdown.md`](../simplify_approach/task_breakdown.md) "Commit 策略" 段。

## 6. 兼容性

- filter-off 路径完全不动；feature flag 关闭时本 phase 引入的所有新类型都不会被实例化
- filter-on 路径行为变化对外可见效果：filter 期间不再调用 `channel.onRecoveredStateBuffer`；channel 的 `recoveredBuffers` 在 filter 完成时仍为空。Phase 4 接 drain 之后才会真正向 channel 投递 buffer
- 由于 Phase 2 / Phase 3 并行，在两者均未合入前，filter-on 路径会**暂时**陷入"filter 写盘了但 drain 没人推进 channel"状态——通过 Phase 4 的 drain 接入才会闭环。本设计**允许**这个中间态，因为：
  - Phase 1 之后整个 FLINK-38544 feature flag 仍未开启
  - Phase 3 单独 merge 时通过 feature flag 关闭保护，不影响生产
  - Phase 4 必须等 Phase 3 merge 后启动，merge 顺序由 review 阶段把关

## 7. 验证策略

通过新增单元测试与集成测试覆盖；具体验收命令见 [`acceptance_test.md`](./acceptance_test.md)。

## 8. 已驳回的替代方案

- **`SpillFile` 用单文件不分段**：单段大小不可控；与 master `ChannelStateWriterImpl` 的多段策略风格不一致；段轮转能限制单段最大磁盘占用，便于操作系统层 IO 调度
- **`FilteredBufferWriter` 不持 prefilter / postfilter 内存而是动态从 buffer pool 申请**：违反 simplify_approach `overview.md` §1 "task 内存稳定在常数" 约束；filter 申请频次过高会与 task 内 buffer pool 容量产生不必要争用
- **filter 期间直接保留对 channel 的引用、把 buffer 同时写盘并投递给 channel**：违反 simplify_approach `unspiller.md` §1"filter 不触碰 channel" 的职责切分；也会让 Phase 2 / Phase 3 并行边界破裂
- **`SpillFile.append` 用 mmap**：`channelIOExecutor` 是单线程顺序 append，mmap 无法降低 syscall 数（顺序 write 已经是最优）；mmap 会引入 page cache 同步 / unmap 时机问题，复杂度不划算
- **段大小做成配置项**：本 phase 内 64 MiB 已对齐 master `ChannelStateWriterImpl` 行为；新增配置项缺乏调优场景且增加运维理解成本，未来若需要由独立需求引入
