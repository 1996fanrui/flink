# Round 5 — Phase3 producer-grant 修复后的剩余失败

Log: `log/20260522_180926.log`

## 整体进展（不要忽略 —— 进步显著）

- 前几轮的失败模式**全部消除**：grep `RejectedExecutionException` = 0，`Drain: no physical channel` = 0，`NoSuchFileException` = 0。
- `UnalignedCheckpointITCase` 11/11 ✅；`UnalignedCheckpointRescaleWithMixedExchangesITCase` 5/5 ✅。
- `UnalignedCheckpointRescaleITCase` 42/50 ✅，剩 **8 个失败**。

## 这一轮**真正的**失败 —— 把测试自己注入的 `FailingMapper$TestException`、`Could not perform checkpoint`、`CancellationException` 全部剔除后，只剩 2 种

| 现象 | 出现次数 | 出现位置 |
|---|---|---|
| `IllegalArgumentException: Stream corrupted. Cannot find the header abcdeafc00000000 in the value …` | 5 次（同 1 个 job 的多次重启） | TM 端 `keyby0` task：`UnalignedCheckpointRescaleITCase.keyBy(i -> withoutHeader(i) % NUM_GROUPS)` 在 `setKeyContextElement` 里检查 record header 时 |
| `IllegalArgumentException: Checkpoint needs at least one vertex that commits the checkpoint` | 6 次 | JM 端 `CheckpointCoordinator.createPendingCheckpoint` → `new PendingCheckpoint(...)` 的 `checkArgument` |

观察到的 corrupt value 有两种：`11000000000000` 和 `abcdea0000001100`。前者完全没有 header；后者是 `0xABCDEA` 但末三字节后跟着 `0x000011`（疑似 byte stream 错位了 1 字节）。

## 这两个问题是不是同一个原因？

**现象层差很多**：一个发生在 TM/task 跑 record 时（drain 完毕之后的运行时），一个发生在 JM/CheckpointCoordinator 构造 PendingCheckpoint 时。

**两个可能性**：

- (A) 是同一根因的两个 cascade：byte corruption 导致下游 task FAILED → 整 graph FAILED → 重启时拓扑/状态不一致 → CheckpointCoordinator 找不到能 commit 的 vertex。
- (B) 是两个独立 bug，恰好都被 rescale 路径暴露。

**现在没法判定**。但都只在 rescale 路径上出现，并且 round1-round4 的修复都没碰到这块代码，所以这两个问题**之前一直存在，只是被前面更严重的 bug 掩盖了**。

## 不做"现场分析"原因

第一个（`Stream corrupted`）是**结果不是原因**：byte 流被破坏只能告诉你"某处出错了"，没法靠 stack trace 反推哪段代码写错了 byte。前几轮可以靠 exception 定位到具体 callsite，这一轮不行。

第二个（`Checkpoint needs at least one vertex`）的 stack trace 也只到 `CheckpointCoordinator.createPendingCheckpoint`，看不到具体哪个 vertex 状态出了问题；如果是 cascade，根因还是在 byte corruption。

所以**这一轮不直接读代码找 bug**，改为先按 `review_checklist.md` 做一遍设计-代码对照 review，把所有可能写错 byte 的点 / 影响 checkpoint commit vertex 集合的点过一遍，更可能定位根因。

## 跟 review checklist 的关联（review 时重点看）

`Stream corrupted`（byte 流错乱）可能相关的 checklist 点：

- **B1 SpillFile 生命周期** —— 如果 spill segment 被提前回收 / 多个 SpillFile 写到同一个目录互踩，drain 读出的 byte 可能错位。
- **C1 SpillFileReader.lock 两条铁则** + **C2 drain 4 步结构** —— 如果 (A)/(B) 慢操作被错误地放进 lock 内、或者 (C) 的 4 个动作顺序被拆，drain 交付到 channel 的 buffer 顺序可能错乱，下游 deser 看到拼接错位的 byte。`SpillFile.SpillFileSegment.readBytesAt(offset, length, dest)` 用 `e.length` 和 `e.offset` —— `SpillFile.append` 里 `Entry(channelInfo, segmentIndex, offsetBeforeWrite, length)` 4 个字段是否反映了 `payload.remaining()` 写入前的状态，必查。
- **A2 FilteredBufferWriter — 一个 entry 只装一个 channel** —— 如果 flush 触发条件错（channel 切换没 flush / buffer 满没 flush / 多走了一次 flush），entry 会混 channel 的 byte 或被截断，drain 出来的 byte 直接错位。
- **A3 + 设计文档新加的 §3a：channelInfo NEW direction** —— round3 修了 `beginChannel`，但凡有第三处地方还在用 OLD 做 spillFile key / 物理 channel lookup，drain 就把 A 的 byte 送到 B。所有 `channelInfo` 作为 key / lookup / mapping 的位置全部 grep 一遍。
- **E5 inRecovery 判定 + getNextBuffer 双路径** —— 如果 recovery 期间没完全屏蔽 ordinary upstream（只让 priority event 过），upstream live data buffer 和 drain buffer 混进同一个消费流，下游 byte 流拼接错乱。
- **F5 RecoveryCheckpointBarrier sentinel 不泄漏** —— barrier 是 `!isBuffer()` event；如果泄漏到 operator deser 路径会被当 data 读，byte 自然不对。
- **D1 + D2 单次 submit + handoff complete** —— 如果 drainHandoff 在错误路径上没 complete / 走漏了 partial drain，下游 byte 流缺片同样表现为 corrupt。

`Checkpoint needs at least one vertex` 可能相关的 checklist 点：

- **F6 Step 3 — addInputDataFromSpill 写入正确性** —— 如果 Step 3 在 abort 路径调用顺序错了 / 重复了，CheckpointCoordinator 端可能记错"哪个 vertex 还能 commit"。
- **B1 SpillFile 生命周期** —— ref-count 出错时 abort 路径的 callback 会不会让 CheckpointCoordinator 那边的 vertex 集合被错误地置空？需要看 abort 路径上 `getAndRemoveWriteResult(cpId).getInputChannelStateHandles()` 的回调和 CheckpointCoordinator 内部状态的耦合。
- 也可能完全是 cascade：第一个 task FAILED 后整个 ExecutionGraph 进入 RESTARTING，期间 CheckpointCoordinator 发起新 checkpoint，发现 vertex 集合空（所有 task 都还没 RUNNING）。这不算 bug，是正常报错；如果是这种情况，修第一个 bug 就够。

## 下一步建议

1. **暂不动代码**。
2. 按 `review_checklist.md` 的功能点逐项 review。建议先做"可能相关"的（上面列出的）；其余功能点作为兜底全量 review。
3. review 时如果在某条下发现实现与设计不符，记录到对应 issue（哪条 checklist + 文件路径 + 行号）。
4. review 完成后再决定哪些不符项需要修复、修复顺序如何。
