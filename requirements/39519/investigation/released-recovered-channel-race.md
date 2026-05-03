# `Trying to read from released RecoveredInputChannel` race 分析

## 现象

`UnalignedCheckpointRescaleITCase.shouldRescaleUnalignedCheckpoint` 在 attempt #1
（restore from unaligned checkpoint）刚 RUNNING 几毫秒后失败：

```
java.lang.IllegalStateException: Trying to read from released RecoveredInputChannel
  at RecoveredInputChannel.getNextRecoveredStateBuffer(RecoveredInputChannel.java:193)
  at RecoveredInputChannel.getNextBuffer(RecoveredInputChannel.java:232)
  at SingleInputGate.readBufferFromInputChannel(SingleInputGate.java:991)
  at SingleInputGate.readRecoveredOrNormalBuffer(SingleInputGate.java:986)  <-- fallback 分支
```

trace 落在 `SingleInputGate.java:986` 的 fallback，说明进入
`readRecoveredOrNormalBuffer` 时 `isReleased() == true` 已成立，跳过了第一段，
走到 fallback 仍旧调用同一个已释放 RC 的 `getNextBuffer`，触发 precondition。

## 涉及线程

只有 **两个线程** 参与 race：

- **channelIOExecutor**：执行 `drainPendingSpill`，把 spill 文件里的 buffer
  通过 `RecoveredBufferStoreImpl.addBufferAndCaptureListener` 加入 store。
- **mailbox 线程**（即 task 线程，同一根）：
  - 在 `restoreInternal` 阶段跑 `convertRecoveredInputChannels`，把
    `RecoveredInputChannel` 替换成 `LocalInputChannel` / `RemoteInputChannel`，
    并触发 `markStoreTransferred`。
  - restore 结束后跑 `runMailboxLoop`，在 `processInput` 中 poll
    `inputChannelsWithData`、调 `getNextBuffer`。

`drainDone + storeTransferred` 两旗到齐才会调 `RC.releaseAllResources()`
（`isReleased = true`，归还 BufferManager）。

## 时序图（race 触发路径）

```
channelIOExecutor (drain)                  mailbox = task 线程
───────────────────────────                ───────────────────────────
addBufferAndCaptureListener:
  acquire store lock
  capture listener = RC::notify   ←── 此刻 listener 仍是 RC
  release store lock
                                            [restore 阶段]
                                            convertRecoveredInputChannels:
                                              LocalInputChannel 构造器
                                                acquire store lock
                                                setDataAvailableListener(physical)
                                                release store lock
                                              synchronized(inputChannelsWithData):
                                                queue 中移除 RC（若在）
                                                channels[i] = physical
                                              RC.markStoreTransferred()
                                                └─ drainDone 已 true
                                                   → releaseAllResources()
                                                   → RC.isReleased = true
listener.onDataAvailable()        ←── 锁外触发，用的是上面捕获的旧 listener
  → RC.notifyChannelNonEmpty
  → inputGate.queueChannel(RC)    ★ 已释放的 RC 被塞回 queue
                                            [restore 结束，invoke → runMailboxLoop]
                                            getChannel(): poll → 取出 RC
                                            readRecoveredOrNormalBuffer:
                                              RC.isReleased == true
                                              → 跳过第一段
                                              → fallback readBufferFromInputChannel
                                              → RC.getNextBuffer()
                                              → checkState 抛出 ✘
```

关键时间锚点：
1. **listener 捕获在前**：drain 把 RC 引用以方法引用形式捕获到本地变量。
2. **listener 替换发生在捕获之后**：物理 channel 构造器在 store 锁下把 store
   的 `dataAvailableListener` 改写到自己。
3. **释放发生在替换之后**：`markStoreTransferred` 在 conversion 后段触发；
   两旗到齐 → `RC.isReleased = true`。
4. **listener 在锁外触发**：drain 用的是步骤 1 捕获的旧引用，把已死的 RC
   推回 `inputChannelsWithData`。
5. **task 看到 stale 引用**：`getChannel` poll 出 RC，读取时撞 precondition。

## 为什么两旗 rendezvous 没兜住

两旗设计的目标是延后 **释放**（buffer manager + isReleased），让它发生在
conversion 之后——这部分是对的。

漏掉的是 **入队**：`convertRecoveredInputChannels` 在 swap 那一刻只清理
"当时已经在 queue 里的 RC"。对于在 swap **之后** 才被旧 listener 引用塞回
queue 的 RC，没有任何防护，最终被 task 取出来读。

也就是说：listener 捕获-触发的"先 capture 后 fire"模式，把一个本应失效的
channel 引用偷渡到了 conversion 之后的 queue。

## 影响范围

- 仅出现在 unaligned-checkpoint 恢复路径，且 spilled 数据需要在 `drainPendingSpill`
  里被回放（即 dispatcher 真正触发了 spill）。
- 触发概率取决于 drain 与 conversion 的时间交错，rescale 测试因 attempt #1 启动
  时机紧凑而较易撞上。
- 不会丢数据：失败被 `RestartBackoff` 捕获后就直接判任务失败；不会出现"读到
  错误数据但继续运行"。

## 修复方向（待讨论）

- **A 读端兜底**：`SingleInputGate.readRecoveredOrNormalBuffer` 入口判断
  `inputChannel instanceof RecoveredInputChannel && inputChannel.isReleased()`
  → 直接返回 `Optional.empty()`，循环到下一个 channel。
- **B 写端拦截**：override `RecoveredInputChannel.notifyChannelNonEmpty()`，
  `isReleased` 为 true 时不调用 `inputGate.notifyChannelNonEmpty(this)`。

A 是兜底；B 把绝大多数 stale 入队拦在源头但仍有窄窗，所以建议 A+B 同时。
