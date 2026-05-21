# 用户需求 — Phase 5：Checkpoint 3-step 协议接线

## 需求偏离

无。

## 背景

[`simplify_approach/coordination.md`](../simplify_approach/coordination.md) §3 规定了 checkpoint 3-step 协议：

- Step 1：`recoveryCheckpointTrigger.snapshotAndInsertBarriers(cpId)` —— Phase 4 已实现
- Step 2：master 既有 `IndexedInputGate.checkpointStarted` 循环遍历各 channel，channel 内部按 in-recovery / not-in-recovery 互斥分支处理 —— Phase 2 已实现
- Step 3：`channelStateWriter.addInputDataFromSpill(cpId, snap)` —— 需在本 phase 实现 writer 端 async demux

Phase 5 把这三步统一收口到 `ChannelState.onCheckpointStartedForAllInputs(barrier, writer)` 调度器；把调度器钩到两个 UC 入口；实现 `ChannelStateWriterImpl.addInputDataFromSpill` writer 端真实逻辑；接入 `SpillFile` 引用计数器，按 cpId 生命周期管理段删除。

## 需求

- **REQ-H6G4** 在 master 既有 `ChannelState` 类（`o.a.f.streaming.runtime.io.checkpointing.ChannelState`）上扩展构造器：追加 `RecoveryCheckpointTrigger recoveryCheckpointTrigger` 与 `ChannelStateWriter channelStateWriter` 两个 final 字段；并新增 `onCheckpointStartedForAllInputs(CheckpointBarrier barrier)` 方法：
  - 顺序执行 Step 1（`recoveryCheckpointTrigger.snapshotAndInsertBarriers(barrier.getId())`）→ master 既有 `for (input : inputs) input.checkpointStarted(barrier)` → Step 3（`channelStateWriter.addInputDataFromSpill(barrier.getId(), snap)`）→ 挂 cpId 完成回调（§REQ-DJMJ）
  - feature-off / recovery 完成时 Step 1 返回 empty `DiskSnapshot`、Step 3 在 writer 端走 no-op；外层不允许出现 `if (filter-on)` 分支
  - 注：simplify_approach `coordination.md` §3.2 把 `writer` 作为方法参数，本设计改为构造器注入以适配 master 既有 `ChannelState` 字段风格——`AlternatingWaitingForFirstBarrierUnaligned` / `AlternatingCollectingBarriers` 调用现场无 writer 引用，只能通过 ChannelState 字段访问；语义无差异
- **REQ-1G6O** 把调度器钩到 master 既有的两个真实 UC 触发点（按当前分支代码定位）：
  - `AlternatingWaitingForFirstBarrierUnaligned.barrierReceived` —— UC 自始触发；master 现有 L75-77 `for (input : state.getInputs()) input.checkpointStarted(unalignedBarrier)` 循环 + L78 `controller.triggerGlobalCheckpoint(unalignedBarrier)` 调用
  - `AlternatingCollectingBarriers.alignedCheckpointTimeout` —— 对齐超时切 UC；master 现有 L47-49 同款循环 + L50 `controller.triggerGlobalCheckpoint(unalignedBarrier)` 调用
  - 钩点动作：把上述两处现有的"for-loop + triggerGlobalCheckpoint"中的 for-loop 替换为单行 `state.onCheckpointStartedForAllInputs(unalignedBarrier)`；`triggerGlobalCheckpoint` 保持原样
  - 注：simplify_approach `coordination.md` §3.1 把第二个入口写成 `AlternatingCollectingBarriers.barrierReceived`，但 master 代码中 `AlternatingCollectingBarriers` 没有自己的 `barrierReceived`（继承自 `AbstractAlternatingAlignedBarrierHandlerState`，且父类方法是 `final`）；实际 UC 切换发生在 `alignedCheckpointTimeout`。本设计按 master 实际代码定位钩点
- **REQ-Z2ZC** `ChannelStateWriterImpl.addInputDataFromSpill(cpId, snap)` 完整实现：
  - 委托到既有 writer 线程，按 `Chunk.channelInfo` demux 写入对应 channel 的 checkpoint output stream（复用现有 `addInputData(cpId, channelInfo, ...)` 链路）
  - 空 `DiskSnapshot` 走 no-op（不创建 stream、不产生 IO）
  - 完成 / 失败的 future 用 master 既有 `ChannelStateWriteResult` 链路传递；失败时把异常挂在对应 cpId 的 write result 上
- **REQ-DJMJ** `SpillFile` 引用计数器（refCounter）：
  - 计数器位于 `SpillFile` 自身字段；构造时 = 0；`acquire()` +1、`release()` -1；`release()` 后归零则删除全部段文件
  - drain：`SpillFileReader` 构造时调用 `spillFile.acquire()`；`SpillFileReader.close()` 调用 `spillFile.release()`（替换 Phase 4 直接调 `spillFile.close()` 的临时实现）
  - 每个 in-recovery cpId：在 Step 1 lock 内构造 `DiskSnapshot` 时调用 `spillFile.acquire()`；`DiskSnapshot.close()` 调用 `spillFile.release()`（替换 Phase 4 临时 no-op）
  - 完成回调挂在 `ChannelStateWriter.getAndRemoveWriteResult(cpId).getInputChannelStateHandles()` future 上：成功 / 失败均调用 `DiskSnapshot.close()`，从而 release 引用
- **REQ-AIL8** Abort 路径：
  - `ChannelStateWriter.abort(cpId, cause, cleanupSubsumedCheckpoints)` 异常完成对应 cpId 的 future；本设计要求 abort 后引用计数器同 success 路径释放（DiskSnapshot.close → spillFile.release）
  - 全部 cpId abort + drain 结束后引用归零 → 段文件清理
- **REQ-PQ31** ITCase：
  - 新增 UC during recovery ITCase：模拟 checkpoint 在 recovery 期间触发，验证 3-step 协议端到端 correctness（pre-barrier slice + DiskSnapshot 字节集合无重复无遗漏）
  - 新增 rescale + filter + large record ITCase：复现 master 上的 OOM 场景，验证 Phase 4 删除 heap fallback 后 OOM 不再触发
  - master 既有 `UnalignedCheckpointRescaleITCase` 必须零修改继续 PASS
- **REQ-9S6W** Feature-off 零开销验证：
  - feature-off 时 `recoveryCheckpointTrigger.snapshotAndInsertBarriers(cpId)` 必须只走 null-check / no-op，**不创建** `DiskSnapshot` 实例（或返回单例 `DiskSnapshot.empty()`，无对象分配）
  - feature-off 时 `ChannelStateWriterImpl.addInputDataFromSpill(cpId, emptySnap)` 必须 in-line 早 return，不进入 writer 线程

## 显式不在范围

- 不修改 channel 内部字段或 `getNextBuffer` / `checkpointStarted` 实现（Phase 2 完成）
- 不修改 `SpillFileReader.drain` / `snapshotAndInsertBarriers` 内部实现（Phase 4 完成，本 phase 仅在 dispatcher 层调用）
- 不修改 filter 阶段写盘逻辑（Phase 3 完成）
- 不为 `SpillFile` 引入除引用计数与段清理外的其他变化（如压缩、加密等）
