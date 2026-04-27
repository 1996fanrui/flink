# iter_3 锁合并规划

## 思路

当前 channel 路径上有 3 把锁：gate 级 1 把（`SingleInputGate.inputChannelsWithData`），channel 级 2 把（`RecoveredBufferStoreImpl.this` + `RemoteInputChannel.receivedBuffers`）。channel 内的 2 把锁保护的都是同一 channel 的私有状态，可以合并；gate 锁保护跨 channel 共享的 deque 与 BitSet，必须保留。合并方向定为「锁 store 对象本身」：store 实例跨 RecoveredInputChannel→Remote/LocalInputChannel 转换不变，dispatcher 已在外部 `synchronized (store)` 做多步原子提交，复用 store 的 monitor 当作 channel 私有锁，跨转换连续性自动满足、不暴露 `getLock()` 接口。

锁序定为 gate → channel-store。消费者方向天然如此（先从 deque 选 channel，再读该 channel 的 store）；生产者侧禁止嵌套，沿用既有的 capture-then-fire-outside 协议（channel 锁内 commit 状态 + 捕获 listener，释放后再 fire，fire 路径会进 gate 锁）。store 公共方法不再自己加 `synchronized`，标 `@GuardedBy("this")` + `assert Thread.holdsLock(this)`，调用者负责持锁，从根上消除「方法各自原子但组合非原子」的伪安全感（典型如 `tryTake` + `peekNextDataType`）。两阶段方法（`*AndCaptureListener`、`checkpoint`、`releaseAll`、`notifyCheckpointStopped`）保持现有结构，对外承诺自管锁。

`convertRecoveredInputChannels` 的 `buffersInUseCount` 在 gate 锁外读、gate 锁内用，是已知 TOCTOU 窗口，单独修：把读移进 gate 锁内。

## 改动清单

### A. RecoveredBufferStoreImpl
- 移除所有 public 方法的 `synchronized` 修饰；标 `@GuardedBy("this")`；入口加 `assert Thread.holdsLock(this)`
- 受影响方法：`tryTake`、`peekNextDataType`、`isEmpty`、`size`、`incrementPending`、`setCoordinator`、`setDataAvailableListener`
- 两阶段方法保持原结构（内部 `synchronized (this)` commit + 外部 fire）：`addBufferAndCaptureListener`、`addBufferAfterDiskAndCaptureListener`、`decrementPendingAndCaptureListener`、`addBuffer`、`addBufferAfterDisk`、`decrementPending`、`checkpoint`、`releaseAll`、`notifyCheckpointStopped`
- 顶部 javadoc 写死契约：monitor 即 channel 私有锁；`@GuardedBy("this")` 方法由调用者持锁；锁序 gate → channel-store；生产者 capture-then-fire-outside

### B. RemoteInputChannel
- 所有 `synchronized (receivedBuffers)` 改为 `synchronized (recoveredStore)`（约 4–5 处：`peekNextBufferSubpartitionIdInternal`、`getNextBuffer` 后段、`pollPendingPriorityEvent`、`onBuffer`/`onBufferReceived` 路径、`spillInflightBuffers` 等，按 grep 结果穷举）
- `getNextBuffer` recovery 分支改为单次 `synchronized (recoveredStore)` 内完成 isEmpty + tryTake + peekNextDataType；priority 路径出锁后处理

### C. LocalInputChannel
- `getNextRecoveredBuffer` 内 store 调用包 `synchronized (recoveredStore)`，tryTake + peekNextDataType 单次原子
- `hasPendingPriorityEvent` 分支内对 store 的访问也包同一锁

### D. RecoveredInputChannel
- `getNextRecoveredStateBuffer` 中 `tryTake` + `peekNextDataType` 包 `synchronized (store)`
- `finishReadRecoveredState` 现有 `synchronized (store)` 不变（已是预期姿势）

### E. FilteredBufferDispatcherImpl
- 现有 `synchronized (store)` 不变，这就是新契约下调用者持锁的预期姿势
- 复核 `writeChannelStateInternal`、其它 store 触碰点是否补全 `synchronized (store)`

### F. SingleInputGate.convertRecoveredInputChannels（TOCTOU 修复）
- 把 `int buffersInUseCount = realInputChannel.getBuffersInUseCount();` 从 gate 锁外移入 gate 锁内
- 锁序 gate → channel-store 满足，不引入反向

### G. 测试
- `RecoveredBufferStoreTest`：所有直接调 store 方法的测试外面包 `synchronized (store)`；新增「不持锁调用 → AssertionError」回归
- 新增 channel 级集成测试：消费者并发 drain 时验证 tryTake + peekNextDataType 组合原子（同一线程视角下 nextDataType 与下一次 tryTake 结果一致）
- 现有 `UnalignedCheckpointRescaleITCase` 在 `-ea` 下跑通

## 落地顺序

1. 先改/补测试（A、G 的测试断言部分）
2. 改 RecoveredBufferStoreImpl（A）
3. 改三个 channel（B、C、D）
4. 改 dispatcher 触碰点复核（E）
5. 改 convertRecoveredInputChannels TOCTOU（F）
6. 委托 flink-test-runner 跑 RecoveredBufferStore* / RemoteInputChannel* / LocalInputChannel* / SingleInputGate* / UnalignedCheckpointRescaleITCase
7. 全部绿后写一份 iter_3 完成总结，把锁契约同步进 `channel_consumption_ordering.md`
