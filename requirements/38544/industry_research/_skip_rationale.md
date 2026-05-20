# Industry Research — Skip Rationale

> 本次 5 个 phase 全部围绕 Flink 内部 task-thread / channelIOExecutor / buffer pool / channel state writer / checkpoint barrier 等私有运行时组件展开，所有可独立设计的子问题都已被 Flink 既有架构约束。下面逐项说明判断依据，确认无需启动业界规范调研子流程。

## 候选话题与判断

| 候选问题 | 是否业界通用 | 跳过原因 |
|---|---|---|
| spill-to-disk 多段文件 + 段轮转 | 部分通用 | LSM、Kafka log segments 等都有类似形态，但本设计的"段大小 64 MB + append-only + 单线程写"已是 Flink 现有 channel state writer 风格的直接搬用，不引入新决策点 |
| 引用计数管理共享文件生命周期 | 部分通用 | 标准做法（drain +1、每个 cpId reader +1、归零删段），实现位于 `ChannelStateWriter.getAndRemoveWriteResult(cpId).getInputChannelStateHandles()` 既有回调链上 |
| 单锁 + 顺序加锁顺序保证无死锁 | 通用 | 设计已显式描述全局加锁顺序 `SpillFileReader.lock → channel-internal queue monitor`，与 JLS / Goetz 等经典 Java 锁顺序规则一致，无需另外检索 |
| 异步线程与任务线程通过 future 协作 | Flink 内部 | `bufferFilteringCompleteFuture` / `stateConsumedFuture` 都是 master 既有 future，未新增任何 future |
| Checkpoint barrier sentinel | Flink 内部 | `RecoveryCheckpointBarrier` 仅在 task 内部短暂存活，不参与跨进程协议，不存在跨产品通用规范 |
| Buffer pool 与 BufferListener 唤醒 | Flink 内部 | `BufferManager.bufferQueue` + `Object.wait/notifyAll` 是 master 既有的等待机制，本次未触碰 |
| 默认参数（段大小 64 MB、prefilter/postfilter buffer 数量） | Flink 内部经验 | 64 MB 与 Flink 既有 channel state writer 段大小对齐；prefilter/postfilter 各一对来自 simplify_approach overview §1 的"常数内存上界"约束，无业界通用基准 |

## 结论

无需启动业界规范调研子流程：所有设计决策都被既有 Flink 接口（`channelIOExecutor`、`BufferManager`、`ChannelStateWriter`、`Alternating*`、UC barrier handler 等）夹在固定形态中，没有可独立采纳/拒绝的通用方案选项。设计正文直接引用 Flink 既有约束作为依据，无需再外加 RFC / 官方文档。

后续 review 阶段若 reviewer 提出某个具体决策点缺少业界依据（例如段大小默认值、ref counter 释放时机），届时单独启动定向调研补齐。
