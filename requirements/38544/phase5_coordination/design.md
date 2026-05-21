# 设计：Phase 5 — Checkpoint 3-step 协议接线

> 范围：实施 [`simplify_approach/coordination.md`](../simplify_approach/coordination.md) §3 全部内容；新增 `ChannelState.onCheckpointStartedForAllInputs` dispatcher；钩入两个 `Alternating*` UC 入口；实现 `ChannelStateWriterImpl.addInputDataFromSpill` writer 端 async demux；接入 `SpillFile` 引用计数器与 cpId-level 释放回调；新增 ITCase 覆盖 UC during recovery + OOM 修复回归。Phase 5 **依赖** Phase 1 ~ Phase 4 全部 merge。

## 1. 设计目标

满足 [`user_requirements.md`](./user_requirements.md) 中 REQ-H6G4 ~ REQ-9S6W。完成后：

- UC during recovery 端到端可用：checkpoint 在 recovery 期间触发，3-step 协议保证持久化 slice 完整、无重复
- FLINK-38544 的 OOM 路径在生产场景下消除（ITCase 验证）
- feature-off 路径零额外开销

## 2. 现状分析

- master 真实的两个 UC 触发点（按当前分支代码定位）：
  - `AlternatingWaitingForFirstBarrierUnaligned.barrierReceived`（UC 自始触发）
  - `AlternatingCollectingBarriers.alignedCheckpointTimeout`（对齐超时切 UC；`AlternatingCollectingBarriers` 自身没有 `barrierReceived` override，父类 `AbstractAlternatingAlignedBarrierHandlerState.barrierReceived` 是 `final`——不可子类钩入）
  - 两处都先 `controller.initInputsCheckpoint(unalignedBarrier)`，再 `for (input : state.getInputs()) input.checkpointStarted(unalignedBarrier)`，最后 `controller.triggerGlobalCheckpoint(unalignedBarrier)`
- `ChannelStateWriterImpl` 是 `ChannelStateWriter` 唯一非 no-op 实现；内部持有 writer 线程，`addInputData` 把数据塞进 writer 线程 queue，writer 线程消费 queue 写入 cpId-bucketed checkpoint output stream
- `ChannelStateWriter.getAndRemoveWriteResult(cpId)` 返回 `ChannelStateWriteResult`；其 `getInputChannelStateHandles()` 返回 `CompletableFuture<Collection<InputChannelStateHandle>>`；future 完成代表对应 cpId 的输入侧 channel state 全部写完
- Phase 4 `SpillFileReader.close()` 临时直接调用 `spillFile.close()`；Phase 5 改为 `release()`，由引用计数器决定真正删段时机
- Phase 1 `DiskSnapshot` 在 Phase 4 已实现完整迭代；Phase 5 让 `close()` 接 ref counter release

## 3. 修改范围

### 3.1 扩展 master 既有 `ChannelState` + 调度器入口

接入点选择：`o.a.f.streaming.runtime.io.checkpointing.ChannelState`（master 既有 final class，当前持 `CheckpointableInput[]`，由 `AlternatingWaitingForFirstBarrierUnaligned`、`AlternatingCollectingBarriers` 等通过构造器持有）。本设计扩展该类：

- 构造器追加两个 final 字段：`RecoveryCheckpointTrigger recoveryCheckpointTrigger`、`ChannelStateWriter channelStateWriter`
- 新增 `onCheckpointStartedForAllInputs(CheckpointBarrier)` 方法

```java
public final class ChannelState {

    private final CheckpointableInput[] inputs;
    private final RecoveryCheckpointTrigger recoveryCheckpointTrigger;   // 由上游构造器注入；feature-off 时是返回 DiskSnapshot.empty() 的 null-object
    private final ChannelStateWriter channelStateWriter;                 // 由上游构造器注入；与 master 既有 writer 同一实例

    public ChannelState(CheckpointableInput[] inputs,
                        RecoveryCheckpointTrigger trigger,
                        ChannelStateWriter writer) { ... }

    public void onCheckpointStartedForAllInputs(CheckpointBarrier barrier) throws ... {
        long cpId = barrier.getId();
        DiskSnapshot snap = null;
        try {
            // Step 1：lock 内拍 DiskSnapshot.startPos + 每个 channel 投递 sentinel
            snap = recoveryCheckpointTrigger.snapshotAndInsertBarriers(cpId);

            // Step 2：master 既有 per-input loop；channel 内部分支由 Phase 2 实现
            for (CheckpointableInput input : inputs) {
                input.checkpointStarted(barrier);
            }

            // Step 3：writer 端 async demux；空 snap 走 in-line no-op
            channelStateWriter.addInputDataFromSpill(cpId, snap);

            // 挂 cpId 完成回调（success / abort 都走同一路径，参见 §3.5 / §3.6）
            attachSnapshotReleaseOnCpIdCompletion(cpId, snap);
            snap = null;   // 已交托给回调链，禁止本方法 finally 再 close
        } finally {
            if (snap != null) {
                // 抛错路径（Step 1 / 2 / Step 3 submit 阶段失败）：手动 close 释放 refCount
                try { snap.close(); } catch (Exception ignored) {}
            }
        }
    }
}
```

调度器注入链路（settle "RecoveryCheckpointTrigger 怎么到 ChannelState"）：

```
StreamTask.restoreInternal()
  → 构造 SpillFileReader（filter 完成 + conversion 完成路径上；见 Phase 4 §3.6）
  → 创建 RecoveryCheckpointTrigger 引用（即 SpillFileReader 实例）
  → 通过 SingleCheckpointBarrierHandler 工厂（master 既有 `unaligned`/`alternating`）的构造扩展把 trigger + writer 传给 ChannelState 构造器
  → ChannelState 持 trigger / writer 字段，传给各 Alternating* 状态
```

具体在 `SingleCheckpointBarrierHandler.unaligned/alternating` 工厂方法（master 既有 L134-188 附近）追加 trigger 与 writer 参数；filter-off 路径传入"empty trigger"（返回 `DiskSnapshot.empty()`）作为 null-object，外层无任何条件分支。

关键点：

- 外层不做 `if (filter-on)`；feature-off 时 trigger 走 null-object（恒返回 empty DiskSnapshot），writer 端 `addInputDataFromSpill` 对空 snap 早 return；空 snap 路径必须 close `chunks`（writer 端 §3.3 注明）
- 调度器**只**负责 Step 1 / Step 3 协调；Step 2 完全沿用 master 既有 `input.checkpointStarted(barrier)`，对该方法的扩展（in-recovery 分支）在 Phase 2 已完成
- 调度器不持锁；任何加锁逻辑由各 step 内部组件自行管理
- snap 抛错路径必须手动 close 释放 refCount（try-finally + 哨兵 null 模式）
- `attachSnapshotReleaseOnCpIdCompletion` 实现细节见 §3.5（基于 `getAndRemoveWriteResult(cpId)` 的 future whenComplete）

### 3.2 钩到两个 UC 触发点（按 master 实际代码定位）

按 master 当前分支代码定位的两个真实 UC 触发点（前一稿写为"`AlternatingCollectingBarriers.barrierReceived`"在 master 不存在，且父类 `AbstractAlternatingAlignedBarrierHandlerState.barrierReceived` 是 `final`——本设计按实际代码改为 `alignedCheckpointTimeout`）：

**钩点 1**: `AlternatingWaitingForFirstBarrierUnaligned.barrierReceived`（master 当前 L75-78）

```java
// before
controller.initInputsCheckpoint(unalignedBarrier);
for (CheckpointableInput input : state.getInputs()) {     // master L75-77
    input.checkpointStarted(unalignedBarrier);
}
controller.triggerGlobalCheckpoint(unalignedBarrier);     // master L78

// after — 用 dispatcher 替换 for-loop（dispatcher 内部含 Step 1 + Step 2 + Step 3）
controller.initInputsCheckpoint(unalignedBarrier);
state.onCheckpointStartedForAllInputs(unalignedBarrier);  // 替换原 for-loop
controller.triggerGlobalCheckpoint(unalignedBarrier);
```

**钩点 2**: `AlternatingCollectingBarriers.alignedCheckpointTimeout`（master 当前 L47-50）

```java
// before
controller.initInputsCheckpoint(unalignedBarrier);
for (CheckpointableInput input : state.getInputs()) {     // master L47-49
    input.checkpointStarted(unalignedBarrier);
}
controller.triggerGlobalCheckpoint(unalignedBarrier);     // master L50

// after
controller.initInputsCheckpoint(unalignedBarrier);
state.onCheckpointStartedForAllInputs(unalignedBarrier);  // 替换原 for-loop
controller.triggerGlobalCheckpoint(unalignedBarrier);
```

两处钩点都在 `controller.initInputsCheckpoint(unalignedBarrier)`（master 既有，保证 cpId 的 `ChannelStateWriteResult` 已注册）**之后**、`controller.triggerGlobalCheckpoint(...)` **之前**。这一时序保证 Step 3 `addInputDataFromSpill(cpId, ...)` 调用时 cpId 对应的 write result 已存在，§3.5 的 `getAndRemoveWriteResult(cpId)` 可正常取得。

### 3.3 `ChannelStateWriterImpl.addInputDataFromSpill(cpId, snap)`

#### 3.3.1 设计约束（实现阶段必须满足）

按 master 现有 `ChannelStateWriteRequest.java` / `ChannelStateWriterImpl.java` / `ChannelStateWriteRequestDispatcherImpl.java` 的类层级与调用模式（具体方法名以 master 现存为准），实现必须满足以下约束：

1. **入队路径与既有 `addInputData` 一致**：通过 `ChannelStateWriterImpl` 内部 `private void enqueue(ChannelStateWriteRequest request, boolean atTheFront)` helper 投递到内部 `ChannelStateWriteRequestExecutor`，**不直接调用** executor 接口（该接口公开的方法是 `submit` / `submitPriority`，无 `enqueue`）。
2. **新增 request 子类（位置：`ChannelStateWriteRequest.java` 同文件）**：直接继承 package-private `abstract class ChannelStateWriteRequest`，**不继承** `CheckpointInProgressRequest`（该子类是 `final`，且其构造器签名与本场景不匹配；通过直接继承 base 类避免对既有 `final` 子类做侵入式改动）。请求子类承担"按 chunk demux 写盘"职责，对外提供 `cancel(Throwable)` 实现以满足 base 类抽象契约。
3. **dispatcher 路由**：本子类对应的执行路径在 `ChannelStateWriteRequestDispatcherImpl.dispatchInternal()` 新增 `instanceof` 分支（与既有 `CheckpointStartRequest` / `CheckpointInProgressRequest` / `CheckpointAbortRequest` 三个 `instanceof` 分支并列），由该分支获取 `ChannelStateCheckpointWriter` 并调用本子类承担的执行方法；具体执行方法名（如 `replay` / `execute` / `apply`）由开发期与现有调度风格对齐确定。
4. **静态工厂**：在 `ChannelStateWriteRequest` 追加 `static replayInputDataFromSpill(JobVertexID, int, long cpId, CloseableIterator<DiskSnapshot.Chunk>)` 工厂方法，与既有 `write(...)` / `start(...)` / `abort(...)` 同样级别公开。
5. **`Chunk.data` → `Buffer` 适配**：`DiskSnapshot.Chunk` 携带 `byte[] data`；既有 `ChannelStateCheckpointWriter` 写入入口（如 `writeInput(JobVertexID, int, InputChannelInfo, Buffer)`）要求 `Buffer`。本子类内部负责适配：通过 master 既有 `MemorySegmentFactory.wrap(byte[])` 包裹为 `MemorySegment`、再以 `NetworkBuffer(MemorySegment, FreeingBufferRecycler.INSTANCE)` 或等价的 unpooled `Buffer` 类构造（在仅本次写盘 lifecycle 持有，写完释放）；该适配是 writer 侧 IO 路径的一次性临时 buffer，不进入 channel buffer pool。
6. **空 snap 早 return**：`addInputDataFromSpill` 入口检测 `!chunks.hasNext()` 时同步 close chunks 并 return，不入队、不进 writer 线程。
7. **入队失败路径**：`enqueue` 抛任何异常时，必须同步 `chunks.close()` 触发 DiskSnapshot.refCount 释放，且通过 master 既有 `failWriteResult(cpId, cause)` 把异常挂在对应 cpId 的 `ChannelStateWriteResult`。
8. **执行失败路径**：写盘抛异常时通过 master 既有失败链路（base 类 `cancel` 或 dispatcher 失败处理）调 `failWriteResult`；`chunks.close()` 在异常路径下也必须执行（建议在 `cancel` override 内做 finally close，或在 dispatcher 失败处理处做 finally close）。

#### 3.3.2 实施轮廓

`ChannelStateWriterImpl.addInputDataFromSpill` 主体伪码（具体 API 名以 master 实际签名为准）：

- 检测 `!chunks.hasNext()` → 同步 close chunks → return
- 否则构造 request：`req = ChannelStateWriteRequest.replayInputDataFromSpill(jobVertexID, subtaskIndex, cpId, chunks)`
- `try { enqueue(req, /*atTheFront=*/ false); } catch (Exception e) { chunks.close(); failWriteResult(cpId, e); }`

新增子类（同 `ChannelStateWriteRequest.java`）职责：

- 字段：cpId + chunks
- 执行方法：循环消费 chunks，每条 chunk 完成 `byte[] → Buffer` 适配后调用 `ChannelStateCheckpointWriter` 既有写入入口（按 chunk.channelInfo demux），finally close chunks
- cancel override：调 super.cancel 走 base 类失败链路，finally close chunks

不论成功 / 失败，`chunks.close()` 必须执行（在执行方法的 finally + cancel 的 finally + 入队失败的同步 close 三处覆盖）。

#### 3.3.3 实现细节的留白

以下细节交开发阶段对照 master 实际代码确定，本设计不强制具体名称，以避免设计文档锁死实现细节（参考 `design_docs_guide.md`"设计文档只描述目标、原则和修改范围"）：

- 新 request 子类的具体类名（建议 `SpillInputReplayRequest` 或 `ReplayInputDataRequest`，按 master 命名惯例）
- 新增子类持有的执行方法名（`replay` / `execute` / `apply` 等）
- `ChannelStateCheckpointWriter` 写入入口的精确签名（master 既有 API；本设计仅约束语义"按 channelInfo demux 写入 cpId 的 input state stream"）
- `byte[] → Buffer` 适配的具体类（`NetworkBuffer` / 其他 unpooled 实现），但必须保证写完后立即释放，**不占用 channel buffer pool**
- dispatcher 新增 `instanceof` 分支的具体放置位置

### 3.4 `SpillFile` 引用计数器

```java
public final class SpillFile implements Closeable {

    // Phase 3 / 4 已有：segments / entries / append / snapshot / close

    private final AtomicInteger refCount = new AtomicInteger(0);
    private final AtomicBoolean cleanedUp = new AtomicBoolean(false);

    /** 调用方：SpillFileReader 构造 / DiskSnapshot 构造（Phase 4 lock 内）。 */
    public void acquire() {
        refCount.incrementAndGet();
    }

    /** 调用方：SpillFileReader.close / DiskSnapshot.close / abort 回调。归零且首次胜出 CAS 时删段。 */
    public void release() throws IOException {
        if (refCount.decrementAndGet() == 0) {
            if (cleanedUp.compareAndSet(false, true)) {
                deleteAllSegments();
            }
        }
    }

    private void deleteAllSegments() throws IOException {
        for (SpillFileSegment seg : segments) {
            seg.close();
            Files.deleteIfExists(seg.path);
        }
    }

    @Override
    public void close() throws IOException {
        // 保留为强制清理入口（测试 / shutdown）；与 release 路径互斥地竞争 CAS，保证 deleteAllSegments 仅执行一次
        if (cleanedUp.compareAndSet(false, true)) {
            deleteAllSegments();
        }
    }
}
```

- `refCount` 用 `AtomicInteger`，`cleanedUp` 用 `AtomicBoolean.compareAndSet`，保证"归零判定 + 清理触发"复合操作原子化；多线程同时 `decrementAndGet` 到 0、或 `release` 与 `close` 并发都最多触发一次 `deleteAllSegments`，规避 double-free
- `SpillFileReader` 构造：`spillFile.acquire()`；`close()`: 改为 `spillFile.release()`（替换 Phase 4 临时 `spillFile.close()`）
- `DiskSnapshot` 构造：Phase 4 中 `SpillFileReader.snapshotAndInsertBarriers` 在 lock 内构造 `DiskSnapshot` 时调用 `spillFile.acquire()`；`DiskSnapshot.close()` 实现改为 `spillFile.release()`
- 不变式："release 到 0 之后" 永远不会再次 `acquire`，因为 acquire 的唯一来源是 `SpillFileReader.<init>`（一次性）与 `snapshotAndInsertBarriers` lock 内（仅在 drain 进行中 / drain 持有引用时调用），归零意味着 drain 已 release 且所有 cpId reader 都 close 完毕，后续不可能再调 snapshotAndInsertBarriers

### 3.5 cpId-level 释放回调 (`attachSnapshotReleaseOnCpIdCompletion`)

时序不变式（由 §3.2 钩点保证）：`controller.initInputsCheckpoint(unalignedBarrier)` 已在调度器调用之前发生，因此该 cpId 的 `ChannelStateWriteResult` 必然已注册到 writer 内部 map（master `ChannelStateWriterImpl.start(cpId)` 路径）。`getAndRemoveWriteResult(cpId)` 调用安全。

```java
private void attachSnapshotReleaseOnCpIdCompletion(long cpId, DiskSnapshot snap) {
    // 时序前提：被钩点位置（§3.2）保证 controller.initInputsCheckpoint 已经跑过，cpId 的 ChannelStateWriteResult 必然存在
    ChannelStateWriteResult writeResult = channelStateWriter.getAndRemoveWriteResult(cpId);
    writeResult.getInputChannelStateHandles().whenComplete((handles, throwable) -> {
        try { snap.close(); } catch (Exception ignored) {}    // 触发 spillFile.release()
    });
}
```

- `whenComplete` 同时覆盖 success 与 exceptionally 路径（abort 经由 `ChannelStateWriter.abort` 异常完成 future，回调照常 fire），无需额外 abort 专用路径
- `snap.close()` 抛 IOException 静默吞掉——`whenComplete` 内异常不向上传播，且 SpillFile 的 `deleteAllSegments` 失败只是临时文件未删除。Flink task teardown 时 Flink 既有的 temporary file cleanup 机制（job-level 临时目录在 task manager shutdown 时整目录删除）作为兜底
- §3.2 调度器内部的 try-finally 哨兵 null 模式保证：若 Step 1 / Step 2 / Step 3 同步阶段抛错（snap 还未交给 callback），由 finally 手动 close；若 `attachSnapshotReleaseOnCpIdCompletion` 成功执行（snap=null 哨兵），由 future callback 负责 close。两条路径不会双重 close（`DiskSnapshot.close` → `spillFile.release` → CAS 保证 deleteAllSegments 仅一次）

### 3.6 Abort 路径

`ChannelStateWriter.abort(cpId, cause, cleanupSubsumedCheckpoints)` 异常完成 `ChannelStateWriteResult.getInputChannelStateHandles()` future，§3.5 的 `whenComplete` 回调照常 fire，触发 `snap.close()` → `spillFile.release()`。无需额外 abort 专用路径。

drain 抛错走 channelIOExecutor 错误传递（Phase 4）；同时 task 进入失败状态，所有 cpId future 被 abort，引用最终归零。

### 3.7 ITCase

新增 2 个 ITCase：

- `UnalignedCheckpointDuringRecoveryITCase`：模拟一个有 N 个 channel、跨 task 重新 scaling 的 job，channel state 较大、recovery 期间触发 UC checkpoint。验证：
  - 端到端 checkpoint 完成
  - 持久化的 input channel state handles 字节集合 = 原始 channel state（无重复、无遗漏）
  - 同一 cpId 不会写两份持久化数据
- `RescaleFilterLargeRecordOOMRegressionITCase`：复现 master 触发 OOM 的场景（FLINK-38544 issue 中描述的 rescale + filter + 大 record），验证：
  - task 内存占用稳定（heap 使用峰值低于 master 同场景）
  - 测试在 master 上必然 OOM、在本 phase 后必然 PASS

master 既有 `UnalignedCheckpointRescaleITCase` 零修改 PASS 作为 regression。

### 3.8 不变之处

- channel 内部 `getNextBuffer` / `checkpointStarted` / `recoveredBuffers` 字段全部不动
- `SpillFileReader.drain` / `snapshotAndInsertBarriers` 内部实现不动
- filter 路径写盘逻辑不动
- `BufferManager` / `BufferPool` / `notifyChannelNonEmpty` 链路不动

## 4. 不变式

- 调度器外层不出现 `if (filter-on)` 分支；feature off / recovery done 通过 trigger 与 writer 的 no-op 路径自然消化
- Step 1 / Step 2 / Step 3 顺序严格：Step 1 必须最先（否则 Step 2 拿不到 sentinel）；Step 2 / Step 3 在 Step 1 之后任意顺序（设计示例按 Step 2 → Step 3）
- `SpillFile` 引用计数归零 ⇔ 全部段文件删除；段删除幂等
- abort 走与 success 同一释放路径，禁止双重释放

## 5. 代码组织

- 修改文件（不新增类，全部在 master 既有文件上扩展）：
  - `ChannelState.java`（构造器追加 `recoveryCheckpointTrigger` + `channelStateWriter` 字段；新增 `onCheckpointStartedForAllInputs` 方法）
  - `SingleCheckpointBarrierHandler.java`（`unaligned`/`alternating` 工厂方法签名扩展，注入 trigger + writer）
  - `AlternatingWaitingForFirstBarrierUnaligned.java`（barrierReceived 内 for-loop 替换为 `state.onCheckpointStartedForAllInputs(...)`）
  - `AlternatingCollectingBarriers.java`（`alignedCheckpointTimeout` 内 for-loop 替换为同款 dispatcher 调用）
  - `ChannelStateWriterImpl.java`（实现 `addInputDataFromSpill` + 新增 `SpillReplayRequest` 内嵌类）
  - `SpillFile.java`（追加 refCount / cleanedUp(AtomicBoolean) / acquire / release / deleteAllSegments；`close()` 改为 CAS 守护）
  - `SpillFileReader.java`（构造器内 `spillFile.acquire()`、`snapshotAndInsertBarriers` 在 lock 内构造 `DiskSnapshot` 之前调 `spillFile.acquire()`；`close()` 改为 `spillFile.release()`）
  - `DiskSnapshot.java`（`close` 改为 `spillFile.release()`）
- 新增 ITCase 2 个 + 单元测试若干：`ChannelStateDispatcherTest`、`ChannelStateWriterImplAddInputDataFromSpillTest`、`SpillFileRefCountTest`、`AlternatingWaitingForFirstBarrierUnalignedDispatchHookTest`、`AlternatingCollectingBarriersDispatchHookTest`、`UnalignedCheckpointDuringRecoveryITCase`、`RescaleFilterLargeRecordOOMRegressionITCase`

**提交策略**：本 phase 与其他 4 个 phase 共用同一开发分支，**不发 PR**；完成后作为**单一 commit** 推到分支，禁止 `git commit --amend` / `git rebase -i` 重写历史。本 phase 是 FLINK-38544 序列的最后一个 commit，落地后即视为整体 feature 交付。完整规则参 [`../simplify_approach/task_breakdown.md`](../simplify_approach/task_breakdown.md) "Commit 策略" 段。

## 6. 兼容性

- `ChannelStateWriter.addInputDataFromSpill` 接口由 Phase 1 引入并 default no-op（见 `phase1_interfaces/design.md` §3.6 + §2 修改文件表，明列 `ChannelStateWriter.java` 与 `ChannelStateWriterNoOp` 同步追加 no-op override）；Phase 5 仅在 `ChannelStateWriterImpl` 中实现非 no-op 版本，其他 mock / fake 继续继承 Phase 1 提供的 default
- Alternating* 入口的 for-loop 替换为单行 dispatcher 调用，调用顺序与既有 master 完全一致（initInputsCheckpoint → dispatcher → triggerGlobalCheckpoint）
- `SpillFile` ref counter 是新增字段，不影响 Phase 3 / 4 外部 API；`close()` 保留作为强制清理入口（测试用）

## 7. 验证策略

- 单元测试覆盖：调度器序列、Alternating 入口挂钩、addInputDataFromSpill async demux、SpillFile ref counter / 删段、abort 路径
- ITCase 覆盖：UC during recovery、OOM 修复回归
- master 既有 `UnalignedCheckpointRescaleITCase` 零修改 regression

具体验收命令见 [`acceptance_test.md`](./acceptance_test.md)。

## 8. 强制后续动作

本 phase 引入端到端 spill-to-disk 路径、新增 dispatcher、写盘 demux 子系统等多处重大架构变更。Phase 5 完成 / merge 后，**必须执行** `/architecture-overview` 更新项目 `docs/architecture_overview.md`，把 unspiller / dispatcher / 3-step 协议加入项目架构全景图。该动作不可省略。

## 9. 已驳回的替代方案

- **调度器外层做 `if (filter-on)`**：违反 simplify_approach `coordination.md` §3.2 "no `if (filter-on)` at this layer" 原则；增加分支会让 feature 开 / 关切换时引入测试漂移
- **`addInputDataFromSpill` 同步在 task thread 写盘**：会让 checkpoint 触发线程承担 IO，与 master 既有 `addInputData` async 风格不一致；也会拉长 Step 3 单次调用耗时
- **`SpillFile` 删段在 `getInputChannelStateHandles` future 之外的位点**：无法保证段删除发生在所有 cpId 已写完之后，可能在 cpId reader 还在读时已删除
- **每个 cpId 独立持有一份 SpillFile 副本**：磁盘成本 O(cpId × file size)，与 simplify_approach `unspiller.md` §2a "multiple readers share the file via independent file handles" 矛盾
- **abort 不释放引用，等 GC 兜底**：违反 CLAUDE.md "禁止资源静默泄漏"；引用计数器的语义本就是显式释放
- **`UnalignedCheckpointDuringRecoveryITCase` 用 mock channel 而非真实 job**：mock 无法覆盖 dispatcher → Alternating 入口 → checkpoint controller 完整链路；本 ITCase 必须是端到端运行真实 job
