# Close 与 Drain 的职责分离 — 长期契约

## 背景

Recovery 阶段的 `FilteredBufferDispatcher.close()` 历史上承担两件事：

1. **业务收尾**：把 spill 文件里残留的 entries 阻塞 drain 回 `RecoveredBufferStore`。
2. **资源释放**：`FilteredSpillFile.close()` 关 file channel、删物理文件。

类似的混淆也存在于 `InputChannelRecoveredStateHandler.close()` / `ResultSubpartitionRecoveredStateHandler.close()`：它们的 close 同时调 `inputGate.finishReadRecoveredState()`（业务：触发 channel conversion）和 `preFilterSegment.free()`（资源）。

`SequentialChannelStateReaderImpl.readInputData` 通过 `try-with-resources` 的**反向声明顺序**承载业务时序——读代码必须脑补"reverse close 顺序 = flush → conversion → drain"，业务时序对外不可见。

## 问题

把业务收尾埋进 `close()` 暗中破坏了三件事：

1. **持锁等阻塞资源**：`FilteredBufferDispatcherImpl.close()` 是 `synchronized this`，drain 内部要 `BufferManager.requestBufferBlocking` 等 network buffer。一旦有任何路径反向获取这把 monitor，就死锁。`onChannelCheckpointStopped` 引入后正是踩中这条路径——task 线程持 `PrioritizedDeque` 锁尝试拿 dispatcher monitor，dispatcher monitor 被 unspilling 线程持着等 buffer，buffer 又因 task 线程被阻塞而无法回收。

2. **异常路径走错流程**：cancel / dispose 时也会触发 `close()`，但此时整个 task 都要废弃，把残留 entries 阻塞 drain 回 buffer 没有任何意义，反而会死等永远拿不到的 buffer 直到 watchdog 介入。

3. **业务时序对调用方隐形**：依赖 `try-with-resources` 反向 close 顺序的业务编排极易被改坏——任何调整声明顺序、增删 resource、或者把 close 移出 try-with-resources 的改动都可能静默破坏时序。

## 契约（长期约束）

### C1. close 只做资源释放

所有 recovery 阶段的 `close()` 必须满足：

- **不阻塞**：不在 close 内调用任何阻塞操作（buffer 申请、IO 等待、condition wait）。
- **短锁**：可以持锁，但只覆盖必须互斥的状态字段读写，不覆盖任何阻塞调用。
- **幂等**：多次调用结果一致，第二次起返回 no-op。
- **不抛业务异常**：close 失败只能抛资源相关异常（IOException）；业务异常必须在显式业务方法里抛出，否则会变成 try-with-resources 的 suppressed exception，调试困难。

### C2. 业务收尾必须显式方法

`flush()` / `finishRecovery()` / `drainPendingSpill()` 必须在调用点显式调用，**不能藏到 close 里**。这是这次拆分的核心契约——任何后续改动若试图把业务步骤合回 close（"为了对称"、"为了少调一行"），必须重新评估这份文档。

### C3. drain 不持 dispatcher monitor

`drainPendingSpill()` 不加 `synchronized`。它只读 `flush()` 之后已经 sealed 的 Reader 状态，不需要互斥保护。这一条是 C1 在 dispatcher 上的具体落地——drain 是阻塞操作，必须脱离 dispatcher monitor 才能消除反向锁路径。

### C4. drain 失败语义=生产者-消费者

`drainPendingSpill()` 的失败处理是普通生产者-消费者语义：拿不到 buffer 就阻塞；任务异常时由上层通过 `Thread.interrupt()` 打断，方法抛 `InterruptedException` 退出。不需要内部 timeout。

### C5. 调用顺序

`SequentialChannelStateReaderImpl.readInputData` 的主流程必须按下列顺序显式调用：

```
read(...)
read(...)
dispatcher.flush()              // ① seal Readers，进入定格状态
stateHandler.finishRecovery()   // ② 触发 channel conversion
dispatcher.drainPendingSpill()  // ③ 阻塞 drain 残留 spill
// try-with-resources 自动调 close：仅资源释放
```

`readOutputData` 同步：调 `stateHandler.finishRecovery()` 后让 try-with-resources 自动关资源（output 路径无 dispatcher，不需要 drain）。

## 反例与一致性检查

下列操作违反契约，review 时必须挡住：

- ❌ 在 `*.close()` 内新增任何阻塞调用。
- ❌ 在 `*.close()` 内调用业务方法（`finishReadRecoveredState`、`drainSpillThroughBuffers` 等）。
- ❌ 给 `drainPendingSpill()` 加 `synchronized` 关键字。
- ❌ 通过调整 try-with-resources 声明顺序来改变业务时序（业务时序必须以显式调用呈现）。**兜底**：本条由 code review 静态检查兜底，没有直接对应的 AT；任何使 `flush` / `finishRecovery` / `drainPendingSpill` 的相对顺序错乱的修改都会让 `AT-DRIN` / `AT-FRCV` / `AT-LOCK` 之一失败，间接捕获。
- ❌ 在 `drainPendingSpill()` 内吞掉 `InterruptedException` 或 `IOException`。

## 历史关联

- 暴露问题的死锁案例：FLINK-39519 `[checkpoint] Notify coordinator on checkpoint stop to drop stale wait-set` 引入 `RemoteInputChannel.checkpointStopped → RecoveredBufferStoreImpl.notifyCheckpointStopped → FilteredBufferDispatcherImpl.onChannelCheckpointStopped` 的反向锁路径。栈 / 日志见 `incident_FLINK_39519_deadlock.md`。
- 设计文档同步：`data_flow.md`（场景 3 改名）、`spill_reader_drain_concurrency.md`（close 生命周期段重写）、`interfaces.md`（接口签名同步）、`architecture_overview.md`（close 连锁描述同步）、`acceptance_test.md`（新增反例验收点）。
