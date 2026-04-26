# Incident: FLINK-39519 引入 dispatcher.close monitor 反向锁死锁

## 现象

`UnalignedCheckpointRescaleITCase` rescale-restore 阶段卡死。Cancel watchdog 30s 后才介入打断；多个新 task 同时挂在 `FilteredBufferDispatcherImpl.onChannelCheckpointStopped:261`。

## 根因

业务级死锁（JVM `jstack` 不会打 `Found Java-level deadlock`，但语义上不可恢复）：

- **持锁线程** `channel-state-unspilling-*`：
  ```
  FilteredBufferDispatcherImpl.close (synchronized)            ← 持 dispatcher monitor
    └── drainSpillThroughBuffers
          └── BufferManager.requestBufferBlocking
                └── Object.wait()                              ← 仅释放 BufferManager 监视器
  ```
- **等锁线程** task 线程（多个 subtask）：
  ```
  StreamTask.runMailboxLoop
    └── SingleInputGate.requestPartitions      [locked PrioritizedDeque]
          └── RecoveredInputChannel.toInputChannel
                └── RemoteInputChannel.checkpointStopped
                      └── ChannelStatePersister.stopPersisting
                            └── RecoveredBufferStoreImpl.notifyCheckpointStopped
                                  └── FilteredBufferDispatcherImpl.onChannelCheckpointStopped
                                        ← BLOCKED waiting dispatcher monitor
  ```

锁 id 一一配对（`waiting to lock <0xN>` ↔ `locked <0xN>`），9 对 dispatcher 实例 × 9 对线程对应。

## 循环依赖

```
unspilling 线程  ── 持有 dispatcher monitor ──→ 等 buffer
                                                    ↑
task 线程       ── 等 dispatcher monitor ──→ 不能消费、不归还 buffer
```

只要 task 线程被 dispatcher monitor 挡住，就无法处理输入、无法回收 buffer 给 BufferManager；unspilling 线程也就永远拿不到 buffer 退出 `close()` 方法。

## 触发条件

需要同时满足：

1. 启用 unaligned checkpoint + checkpointing-during-recovery（rescale 场景默认开启）。
2. Recovery 阶段有 spill 数据需要 drain（network buffer 紧张时触发 P2 落盘）。
3. `RecoveredInputChannel.toInputChannel` 在 dispatcher `close()` 持锁期间被 task 线程触发——这是 FLINK-39519 commit `98ae352cbd5` 让 `checkpointStopped` 反向调用 dispatcher monitor 后才形成的路径。

## 嫌疑 commit

`98ae352cbd5 [FLINK-39519][checkpoint] Notify coordinator on checkpoint stop to drop stale wait-set`：

- 把 `ChannelStatePersister.stopPersisting` 改为接受 `RecoveredBufferStore` 并调用 `store.notifyCheckpointStopped(id)`。
- `RecoveredBufferStoreImpl.notifyCheckpointStopped` 转发到 `coordinator.onChannelCheckpointStopped`，即 dispatcher 的 `synchronized` 方法。
- 此前 `stopPersisting` 是局部状态更新，不会拿 dispatcher 锁，所以不会撞上 unspilling 线程的 close。

## 修复方向（落地见 close_drain_separation.md）

**不能**只在 dispatcher 内"把锁拆细"或"把 onChannelCheckpointStopped 的实现改成不持锁"——这只是解了表面，根因是 `close()` 在持 monitor 期间调用了阻塞操作。彻底修复必须满足：

- `close()` 不持锁、不阻塞：drain 从 close 拆出来叫 `drainPendingSpill()`，调用方在显式时序里调用。
- 业务收尾不能藏在资源释放语义的方法名下——参见 `close_drain_separation.md` 的契约 C1/C2。

## 证据归档

完整 stack/log 归档在仓库 `log/stuck1.stack` / `log/stuck1.log`。后续若再出现类似问题，先比对锁 id 配对模式与本文档的根因图。
