# Roman (rkhachatryan) 对 PR #28661 的全部评论

PR: [FLINK-39524][checkpoint] Fetched channel state: spill files, filtered write, reader
（https://github.com/apache/flink/pull/28661）

共 31 条行内评论（1 条为 7 月 17 日的早期评论，其余 30 条为 7 月 31 日一轮 review）+ 1 条 review 总评。
2026-08-05 我回复/处理了 14 条；2026-08-06 他做了第二轮：resolve 掉 9 条，在 7 条线程上追加了新回复（见「第二轮状态」一节）。
原始评论保留英文原文；「他建议什么」和「我的评价」用中文。

评价标注：✅ 合理 / ⚠️ 部分合理或可商榷 / ❌ 不成立 / 💬 已答复或属拆 PR 边界
状态标注（2026-08-06 晚，按 GitHub 实况）：✅ 已 resolve（他关闭了线程）/ 🕓 已处理，待他确认（代码已 push、回复已发，我方最后发言）/ ⏳ 待我们回复（他最后发言或我们从未回复）/ ⏏️ 外部单独处理
已改标注：✅ 已改 / ⏸ 未改（待讨论或后续轮次）/ 💬 回复即可，无代码改动

## 总览

**状态速览（2026-08-08 上午，按 GitHub 线程实况）**：✅ 已 resolve **30** 条 ｜ ⏳ 待我们处理 **1** 条（**#23**）｜ ✅ 上游已修 1 条（#32）

31 个 inline 线程里 30 个已关闭。**#8 他回了 "SGTM" 并 resolve**——接受"接口拆分 + 实现共用"作为 follow-up，**我们欠一个 FLINK JIRA**（记录 drain/checkpoint 侧拆分，`commit()` 位置是主要难点）。

唯一未决的 **#23**：他 2026-08-07 22:40 贴了一份完整 diff 并问"这个改动对不对？对的话请直接应用；不然我们开 follow-up"。详见 #23 小节第四轮。


| # | 位置 | 主题 | 评价 | 当前状态 | 处理结果 |
|---|------|------|------|----------|----------|
| 1 | ChannelState.java | 缺 addInputDataFromSpill？ | 💬 属下一个 PR | ✅ 已 resolve | 💬 已回复过 |
| 2 | FetchedChannelState javadoc | 文档自相矛盾 | ✅ | ✅ 已 resolve | ✅ 已改：改写 grows 段、删 Mutations 段 |
| 3 | Drainer.snapshotAndInsertBarriers | 命名：snapshot 却返回 reader | ⚠️ | 🕓 已处理，待他确认 | ⏸ 回复草稿已就绪 |
| 4 | Reader javadoc | 改名 nextSegment→advanceAndGet | ⚠️ 主观 | ✅ 已 resolve | ✅ 已改名 `advanceAndGetNextSegment`，已回复 |
| 5 | Reader javadoc | "root reader" 概念不正式 | ✅ | ✅ 已 resolve | ✅ 已改为 "main reader"，已回复 |
| 6 | Reader#snapshot javadoc | "committed position" 未定义 | ✅ | ✅ 已 resolve | ✅ commit() javadoc 已重写（谁调/何时调），已回复 |
| 7 | Reader#snapshot | public 接口返回 package-private 类型 | ✅ | ✅ 已 resolve | ✅ 已改：Snapshot 转 @Internal public，reader() public |
| 8 | SpillSegment#commit | 拆 drain / checkpoint 两个接口 | ⚠️ 值得讨论 | ⏳ 待我们回复 | 🔁 第二轮：他要求实现也拆并提议 offline；先 #22 再拆 |
| 9 | Impl.channelState 字段 | 冗余字段 | ✅ NIT | ✅ 已 resolve | ✅ 已改：删字段，改用 snapshot.channelState() |
| 10 | Impl.files 字段 | 冗余字段 | ⚠️ NIT | ✅ 已 resolve | ✅ 已删字段 + 私有 `files()`，已回复 |
| 11 | Impl.fileStream | 改名 currentFileStream | ✅ NIT | ✅ 已 resolve | ✅ 已改 |
| 12 | Impl.currentFileSize | 字段改为返回值 | ⚠️ | 🕓 已处理，待他确认 | ⏸ 保留字段；#20 后 size 来自通道自身 |
| 13 | Impl.currentBody | 与 Segment.body 双引用 | ⚠️ NIT | ⏳ 待我们回复 | ⏸ 保留：#22 后它成了"当前视图"令牌，防过期段窃读 |
| 14 | Impl.followingSegment javadoc | 删除注释（锚点核实后修正对象） | ⚠️ 压缩不全删 | ✅ 已 resolve | ✅ 已改：javadoc 压成一句（另顺手删了 firstSegment 行内注释） |
| 15 | Position.rollToNextFile | 返回 boolean | ⚠️ 不太成立 | 🕓 已处理，待他确认 | ⏸ 回复草稿已就绪 |
| 16 | Impl.openCurrentFile | 重复代码折叠成循环 | ⚠️ 不采纳 | 🕓 已处理，待他确认 | ⏸ 代码不动，回复解释显式两段的用意 |
| 17 | Impl.readHeaderAtCurrent | 校验 gateIdx/channelIdx | ⚠️ 可加非负检查 | ✅ 已 resolve | ✅ 已加非负 checkState，已回复 |
| 18 | Impl.openFileAndSeek | 流没有 buffered | ✅ 好发现 | ✅ 已 resolve | ✅ 已改：BufferedInputStream 包装 |
| 19 | Impl.skipBody | skipOnStream 自查 path | ✅ NIT | ✅ 已 resolve | ✅ 已改：去掉 path 参数，内部自查 |
| 20 | Impl.skipOnStream | 用 IOUtils#skipFully | ⚠️ 有坑，别照搬 | 🕓 已处理，待他确认 | ⏸ 待改：改用可定位通道，`skipOnStream` 整个删除 |
| 21 | Impl.readFully | 用 IOUtils#readFully | ⚠️ 可改但丢错误上下文 | ✅ 已 resolve | ✅ IOUtils.readFully + 已删 catch 包装，已回复 |
| 22 | Position.segmentStartOffset | header 随 snapshot 携带 | ✅ 最有分量的简化建议 | 🕓 已处理，待他确认 | ✅ 已重构：Snapshot 五扁平字段 + Position 四值，删回退链 |
| 23 | BoundedSegmentStream | 委托/static/javadoc | ✅ 与 #22 联动 | 🕓 已处理，待他确认 | ✅ (1) 已改为问 reader 要计数；(3) javadoc 重写；(2) 保持非 static |
| 24 | Snapshot#reader | 为何不允许多 reader | ⚠️ | 🕓 已处理，待他确认 | ⏸ 回复草稿已就绪 |
| 25 | Snapshot 生命周期 | 不 open reader 就泄漏 grant | ⚠️ API 卫生（集成路径不会触发） | 🕓 已处理，待他确认 | ⏸ 待改：Snapshot 加幂等 close + 测试 |
| 26 | AbstractSpillingHandler | 不切换的通道堆内无界累积 | ✅ 最重要问题，必须修 | 🕓 已处理，待他确认 | ✅ 1MB 软阈值 force-seal + 新测试 |
| 27 | sealCurrentSegment | 空段被静默丢弃 | ⚠️ drop 是对的（已核实） | ✅ 已 resolve | ✅ 维持 drop + 两行注释说明，已回复 |
| 28 | ensureFileOpen | 文件名跨任务碰撞 | ❌ UUID 目录已隔离 | ✅ 已 resolve | ✅ 已回复（UUID 目录隔离） |
| 29 | ensureFileOpen | CREATE_NEW 防碰撞 | ✅ 低成本防御 | ✅ 已 resolve | ✅ 已改 CREATE_NEW，已回复 |
| 30 | getProducedChannelState | 未接线，缺 TODO | ✅ 加 TODO | ✅ 已 resolve | ✅ 已改：加 TODO(FLINK-38544) |
| 31 | SequentialChannelStateReaderImpl | 精简注释 + 静默丢状态 TODO | ⚠️/✅ | 🕓 已处理，待他确认 | ✅ 不接线；第三轮他问"是否该 close"，已答+提议 checkState |
| 32 | review 总评 | recoverWithFiltering 双重 recycle | ✅ 已核实成立 | ✅ 上游已修 | ✅ FLINK-40345 已在 master（rebase 后自动带入） |

**当前进度（2026-08-06 晚，代码已 push、9 条回复已 submit）**

三分类，与 GitHub 线程实况一致：

| 状态 | 数量 | 编号 |
|---|---|---|
| ✅ **已 resolve** | 18 | #1、#2、#4、#5、#6、#7、#9、#10、#11、#14、#17、#18、#19、#21、#27、#28、#29、#30 |
| 🕓 **已处理，待他确认** | 10 | #3、#12、#15、#16、#20、#22、#23、#24、#25、#31 |
| ⏳ **待我们回复** | 1 | #8 |
| ⏏️ 外部单独处理 | 1 | #32 |

已推送的 fixup：`53a8d71f639`（#22 + #23）、`bfb5a5ff6d1`（#25 + #3）、`06dbf264b11`（#20）；#12、#15、#16、#24 是纯回复不改代码。提交前 spotless / 编译 / 495 个相关单测全绿。

**待我们回复的 4 条，现状**：

- **#8**（拆接口 + 实现）：他要求实现也拆并提议 offline；#22 已落地，我们承诺的前提条件已满足，可以推进讨论。
- **#13**（`currentBody` 双引用）：方向已讨论到"把段身份放到交出去的对象上"，未定。
- **#26**（写侧 force-seal）：独立必修，代码与回复都还没动。
（#31 已于 2026-08-06 22:00 回复：他提议的 `Optional.ofNullable(...)` 正是下个 PR 的最终形态，但放在本 PR 会失败——内存 handler 产不出容器 ⇒ 恒 empty ⇒ `needsRecovery=false` ⇒ `RecoveredInputChannel#toInputChannel` 的 `checkState(receivedBuffers.isEmpty())` 炸。）

---

## 批次划分（2026-08-06 定）

| 批次 | 条目 | 改代码 | 仅回复 | 说明 |
|---|---|---|---|---|
| **A** | #25、#3、#24 | #25、#3 | #24 | 生命周期语义 → 返回类型，两条绑定；牵动 `40081/pr`。#24 的回复引用"泄漏已修"，必须排在 #25 之后发 |
| **B** | #16、#20、#12、#15 | #20 | #16、#12、#15 | reader 内部清理，与 A 无耦合。#12 的回复提到"size 来自通道"，要等 #20 落地 |
| **C** | #8 | 待定 | — | 结构性拆分，需 offline 对齐；**#3 已与它解耦，不必再等** |
| **D** | #26 | #26 | — | 写侧 force-seal，独立 |

未入批次：**#13**（`currentBody` 双引用）——方向已讨论到"把身份放到交出去的对象上"，等你定，再决定归入哪批。

---

## 第一批（不依赖 #22）——已完成，2026-08-06 提交

第二轮回复原则（人工定）：命名类不掰扯直接改；文档类按他要的粒度补；多余代码直接删；**每条回复最多一句话**。
执行结果：3 个 fixup commit（对应 `991460936` 容器 / `07037494f` 写侧 / `65fe625bf` 读侧）+ 9 条回复一次性 submit；spotless 通过，flink-runtime 编译通过，channel 相关 73 个单测全绿。
遗留提醒：#21 的第一轮回复曾误发到 #20 线程，第二轮已在 #21 线程内补回。

| # | 改动 | 回复 |
|---|------|------|
| #4 | `nextSegment` → `advanceAndGetNextSegment`（含全部 javadoc 引用） | 一句 |
| #5 | javadoc 里 `drain reader` → `main reader` | 一句 |
| #6 | `SpillSegment#commit()` javadoc 精简重写（谁调、何时调）；接口顶部留一句指过去 | 一句 |
| #10 | 删 `files` 字段，加私有 `files()` 取值方法，6 处调用点改写 | 一句 |
| #17 | `readHeaderAtCurrent()` 加 gateIdx/channelIdx 非负 checkState | 一句 |
| #21 | `readFully()` 删掉 catch 包装 | 一句 |
| #27 | `sealCurrentSegment()` javadoc 补一句"整段被过滤掉是正常情况" | 一句 |
| #28 | 无代码改动 | 一句（UUID 目录已隔离） |
| #29 | 无（已改已回复） | 等他 resolve |
| #31 | ✅ 已了结（人工已回复，代码不动） | — |
| #32 | ⏏️ 外部单独处理 | — |

批次外：#26（不依赖 #22 但要定阈值+测试，独立进行）、#20 与 #8 随 #22 一起做。

---

## 1. ChannelState.java — 缺 `addInputDataFromSpill`？（2026-07-17，早期评论）

位置：`flink-runtime/.../streaming/runtime/io/checkpointing/ChannelState.java`（当时的 catch 块附近）
链接：https://github.com/apache/flink/pull/28661#discussion_r3605821064

> Don't we need to call `channelStateWriter.addInputDataFromSpill` in this method?
>
> Like here:
> https://github.com/apache/flink/pull/28554/changes#diff-8104c9791f592faf61f8613d92c48aefcb89dae2e6d89ad02dff96bc80b94c18R136

**他建议**：在 `onCheckpointStartedForAllInputs` 里把 snapshot reader 交给 channel state writer（对照参考实现 PR #28554 的做法）。

**你已回复**："This pr introduced Fetched channel state, but they are not integrated yet. addInputDataFromSpill is not needed for Memory buffer. They will be done in the next pr https://github.com/apache/flink/pull/28662"

**我的评价**：💬 疑问本身合理，但属于拆 PR 的边界问题——本 PR 刻意不接线，集成在 #28662。当前代码的 javadoc 也已写明 "FLINK-38544 transitional: the spilling backend adds a third step handing the trigger's snapshot reader to the channel-state writer"。你的回复已解决，无需改动。

---

## 2. FetchedChannelState.java:43 — javadoc 自相矛盾

链接：https://github.com/apache/flink/pull/28661#discussion_r3693955946

> This seems to contradict the next paragraph ("The file list grows as the writer rotates to new files..."). By the time this container exists, is the list actually fixed, with the growth happening earlier in the writer, before this object is constructed? Might be worth rewording the paragraph to make that explicit.

**他建议**：改写 javadoc，明确"列表增长发生在 writer 阶段、容器构造时已定型"。

**我的评价**：✅ 完全合理。类 javadoc 说 "Sealed container"，构造器注释说 "The list is sealed; it never grows"，但第二段却说 "The file list grows as the writer rotates..."——增长的是 writer（`AbstractSpillingHandler.files`）里的列表，不是这个容器。更糟的是第 50-51 行还有一段 "Mutations (file list appends) are single-writer and intentionally unsynchronized"，这与 sealed 直接矛盾，明显是旧设计残留。这条应该照改：把"增长"归位到 writer，删掉 Mutations 一段。

**回复草稿**：Good catch — reworded: the list is fixed at construction (the growth happens earlier, in the writer), and dropped the stale "Mutations" paragraph as well.

---

## 3. FetchedChannelStateDrainer.java:74 — `snapshotAndInsertBarriers` 命名

链接：https://github.com/apache/flink/pull/28661#discussion_r3693955948

> Why is this method called `snapshot...` but returns a `reader`? Shouldn't it return a snapshot instead?
>
> (Separately: the purpose and naming of `drain()` are unclear to me.)

**他建议**：方法要么返回 snapshot，要么改名；并把 `drain()` 的职责讲清楚。

**处理方案（2026-08-06 二次更新，已核对 `40081/pr` 的真实用法——之前从"可见性"角度答是错的）**：

后续 PR 里调用链是（`ChannelState.onCheckpointStartedForAllInputs`）：

```java
snap = recoveryCheckpointTrigger.snapshotAndInsertBarriers(cpId);   // 返回值
for (CheckpointableInput input : inputs) input.checkpointStarted(barrier);
channelStateWriter.addInputDataFromSpill(cpId, snap);               // 交出所有权
// catch 里：snap.close()
```

`ChannelStateWriter#addInputDataFromSpill` 的契约是 **"takes ownership of reader"**，NO_OP 实现就是直接 `reader.close()`，Impl 里排队的请求在 cancel 时也 close 它。

**所以返回 reader 的理由是所有权语义，不是类型可见性**：交出去的必须是一个**可 close、且立刻有明确归属**的东西。返回 snapshot 也能工作，但那样会在"拿到 snapshot"和"开出 reader"之间留一个窗口——一旦中间抛异常，grant 就没人还，那正是 #25 说的泄漏。换句话说：**#25 修好（snapshot 可 close）之后，返回 snapshot 才是同样安全的选项**；在那之前，reader 是唯一自带 close 契约的形态。另外 `NO_OP` 直接返回 `FetchedChannelStateReader.emptyReader()`，返回 snapshot 就得先造一个空 `FetchedChannelState` 再包一层，更绕。

**结论（2026-08-06 定稿，与 #25 捆绑）**：**改成返回 snapshot**，名字随之名副其实，不再需要改名。

前提是 #25 先把 snapshot 变成可 close（转移语义）——那之后"拿到 snapshot、还没开 reader"这段窗口不再泄漏，返回 snapshot 与返回 reader 同样安全，而方法名和返回值终于一致。

改动清单：

- **本 PR**：`RecoveryCheckpointTrigger#snapshotAndInsertBarriers` 返回类型 `FetchedChannelStateReader` → `FetchedChannelStateSnapshot`；`NO_OP` 从 `FetchedChannelStateReader.emptyReader()` 换成"空 snapshot"（新增 `FetchedChannelState.emptySnapshot()` 之类）；内存版 `FetchedChannelStateDrainer` 跟着改；`ChannelState` 目前丢弃返回值，无需改。
- **`40081/pr` 分支（后续 PR）**：`ChannelState.onCheckpointStartedForAllInputs` 改成 `try (snapshot) { ... channelStateWriter.addInputDataFromSpill(cpId, snapshot.reader()); }`；`ChannelStateWriter#addInputDataFromSpill` 仍收 reader 并接管 close，不变。

代价：**跨两个 PR 的接口调整**，后续分支要跟着动；收益是名实一致 + 泄漏窗口彻底消失。

**回复草稿（25 词）**：Returns the snapshot now, so the name fits — the caller opens the reader. `drain()` is transitional in-memory; the real drainer lands in the next PR.

**我的评价**：⚠️ 部分合理，值得讨论。
- 返回 reader 是刻意的：调用方（`ChannelState`，跨包）只需要"可消费的东西"，而 `FetchedChannelStateSnapshot` 是 package-private 的（见 #7），返回它对跨包调用方毫无用处。所以"返回 snapshot"这个具体修法在当前可见性设计下走不通——除非把 Snapshot 转 public。
- 但"名实不符"确实存在。可选修法：改名（如 `insertBarriersAndSnapshotUndrained`，或干脆按语义命名为"返回 checkpoint 需持久化的未 drain 部分的 reader"）；或采纳 #7 把 Snapshot 转 public 后返回 snapshot。
- `drain()` 在 in-memory 过渡实现里确实名不副实（只是 release + 给每个 channel 追加 end-of-recovered-state sentinel），真正的 drain 语义要等 spilling drainer 落地。javadoc 其实已解释了这一点，可以回复说明而不改代码。

---

## 4. FetchedChannelStateReader.java:28 — javadoc 难读，建议改名

链接：https://github.com/apache/flink/pull/28661#discussion_r3693955949

> This whole javadoc comment is difficult to read. Could we rename `nextSegment` to `advanceAndGet` and remove this comment?

**他建议**：把 `nextSegment` 改名为 `advanceAndGet`，删掉"为什么不用 Iterator"那段 javadoc。

**我的评价**：⚠️ 主观。javadoc 确实可以精简（"为什么不是 Iterator"一段可以压成一句话），但 `advanceAndGet` 未必比 `nextSegment` 好——`nextSegment` 更贴领域语义，`advanceAndGet` 反而更抽象。我的建议：精简注释、保留方法名，回复里说明理由。这条可以谈。

**回复草稿**：Trimmed the javadoc down to two paragraphs; kept the name `nextSegment` since it reads more domain-specific to me than `advanceAndGet`.

### 第二轮（2026-08-06，r3724924102）

> I would prefer the code to be self-documenting (so that there's no need to read the javadoc). How about `advanceAndGetNextSegment`?

**处理决定：不再争，直接照改。** 他提的 `advanceAndGetNextSegment` 同时保住了领域词 `Segment`，我原来的顾虑（`advanceAndGet` 太抽象）已经被他这个折中名字消掉了。

**具体改法**：
- `FetchedChannelStateReader#nextSegment()` → `advanceAndGetNextSegment()`（接口 + `FetchedChannelStateReaderImpl` 实现 + 所有调用点 + 测试；IDE 重命名一把梭）。
- 同步改掉 javadoc 里所有引用：接口自身的「Entry rule」段、`SpillSegment` 类 javadoc 的「valid only until the next `nextSegment()`」、`FetchedChannelStateReaderImpl` 的 `{@link}`、以及 #6 新写的那段。
- 方法 javadoc 里「Advancing and probing are one step; there is no separate `hasNext`」这句在改名后已被名字表达，可以删掉，剩下 entry rule 一段。

**新回复草稿（一句话）**：Renamed to `advanceAndGetNextSegment`.

---

## 5. FetchedChannelStateReader.java:40 — "root reader" 概念

链接：https://github.com/apache/flink/pull/28661#discussion_r3693955950

> Is there still a distinct "root reader" concept, or is that just informal terminology now that there's a single reader implementation for both the drain and snapshot paths? (Relates to the interface-split suggestion further down.)

**他建议**：要么正式定义 "root reader"，要么随接口拆分（#8）消除这个说法。

**我的评价**：✅ 合理。"root reader" 在多处 javadoc 出现，但类型系统里不存在——它只是"从 `Position.atStart()` 打开、被 drain 线程消费的那一个 reader"的非正式称呼。如果接受 #8 的接口拆分它自然消解；如果不拆，至少应在接口 javadoc 里给出定义。文档级修复，成本低。

**回复草稿**：It was just informal terminology — replaced "root reader" everywhere with "drain reader" and defined it in the interface javadoc.

### 第二轮（2026-08-06，r3724933818）

> TBH, drain reader is also not very clear to me :) I'm struggling to come up with a better name though. Maybe main reader?
>
> (this is very NIT anyways)

**处理决定：照改成 "main reader"。** 他自己标了 very NIT，纯术语问题，没有争的价值；而且定义句还在，叫什么都能读懂。

**具体改法**：全仓 `drain reader` → `main reader`（只在 javadoc/注释里，无代码标识符），涉及 `FetchedChannelStateReader`、`FetchedChannelStateReaderImpl`、`FetchedChannelStateSnapshot`、`FetchedChannelState`。接口 javadoc 里的定义句保留并改为：*"The main reader (opened via `FetchedChannelState#reader()`, starting at offset 0) is the single reader that drains recovered data into the input channels; snapshot readers are derived from it."*

**新回复草稿（一句话）**：Renamed to "main reader".

---

## 6. FetchedChannelStateReader.java:58 — "committed position" 未定义

链接：https://github.com/apache/flink/pull/28661#discussion_r3693955954

> What exactly is the "committed position"? And what does it resolve to if nothing has been read yet?

**他建议**：在接口 javadoc 里定义 committed position，并说明未读任何数据时它是什么。

**我的评价**：✅ 合理。定义目前藏在 `FetchedChannelStateReaderImpl` 的 javadoc 里，接口文档应该自足。答案本身简单：committed = 最近一次 `SpillSegment#commit()` 记录的已交付边界；尚未读任何数据时 = reader 的起始位置（此时 snapshot 从头读）。补一两句话即可。

**回复草稿**：The committed position is the delivered boundary recorded by `SpillSegment#commit()` — before anything is committed it's simply the reader's start position; now defined in the interface javadoc.

人工 review 结论：他是没看懂在提问（文档只用术语没给定义），所以回复采用"先答两问、再提文档已补"的语序；定义那半句 javadoc 是专为本条加的（与 #4/#5 同一次重写）。

### 第二轮（2026-08-06，r3724945017）

> Sorry but it doesn't tell me much. Could you clarify in the javadoc - who calls commit and when?
>
> > before anything is committed it's simply the reader's start position; now defined in the interface javadoc.
>
> 👍

**处理决定：照办，把"谁调、何时调"写进 javadoc。** 他接受了"未 commit 时 = 起始位置"那半句，只是嫌 committed position 的定义仍然是循环定义（"committed = commit() 记录的位置"）。要破这个循环，必须落到具体的调用者和调用时机上——这确实是当前 javadoc 没写的（`SpillSegment#commit()` 上只写了 "the drain consumer ... after each buffer delivery"，没说这个 consumer 是谁、也没说本 PR 里还没有生产调用者）。

**具体改法**：把说明集中到 `SpillSegment#commit()`（接口顶部那段只留一句指过去，避免两处重复），改写为：

```java
/**
 * Records the body bytes read from {@link #bodyStream()} so far as delivered.
 *
 * <p>Called once per delivered buffer by the drainer's drain loop, under the same lock as
 * {@link FetchedChannelStateReader#snapshot()}, so a snapshot always resumes on a buffer
 * boundary. Only the main reader commits.
 */
```

接口顶部那段只留一句指过去：*"...records the delivered boundary — the "committed position" — via {@link SpillSegment#commit()}; before anything is committed it equals the reader's start position."*

注意：#22 落地后 commit 的记账方式变成"记录当前段自己的元数据"，但上面这段讲的是**调用者与时机**，语义不受影响，两轮改动不冲突。

**新回复草稿（一句话）**：Clarified in the javadoc — the drainer's drain loop calls it once per delivered buffer, under the same lock as `snapshot()`.

人工第二轮回复：
- 回复语太长了，最多一句话。主要改 code 注释即可。注释也尽量简洁。

---

## 7. FetchedChannelStateReader.java:68 — public 接口返回 package-private 类型

链接：https://github.com/apache/flink/pull/28661#discussion_r3693955955

> This interface is `@Internal public`, but `snapshot()` returns `FetchedChannelStateSnapshot`, which is package-private with only package-private methods — an external caller in a different package can obtain the object but can't do anything useful with it. Is that intentional?

**他建议**：解决可见性不一致——要么 Snapshot 转 public，要么方法不该出现在 public 接口上。

**我的评价**：✅ 完全合理，这是真实的 API 一致性问题。而且往后看：spilling 落地后 `ChannelState`（streaming 包）要把 snapshot reader 交给 channel state writer，跨包使用是必然的，`FetchedChannelStateSnapshot` 大概率要转 `@Internal public`（`reader()`/`release()` 也要 public）。这条建议接受，顺带能解开 #3 的"返回 snapshot"路线。

**回复草稿**：Not intentional — made `FetchedChannelStateSnapshot` `@Internal public` with a public `reader()`.

---

## 8. FetchedChannelStateReader.java:122 — 拆分 drain / checkpoint 两个接口

链接：https://github.com/apache/flink/pull/28661#discussion_r3693955957
（Roman 在总评里列为四条"可大幅简化"建议之一）

> Could we enforce this contract by splitting this interface into separate drain-side and checkpoint-side interfaces, so `commit()` isn't even callable from the snapshot path? I believe this would simplify a lot of the code in this PR.

**他建议**：把接口拆成 drain 侧（有 `commit()` + `snapshot()`）和 checkpoint 侧（只有 `nextSegment()`），让 snapshot 路径在类型层面就调不到 `commit()`。

**我的评价**：⚠️ 值得认真讨论的核心建议，但我对"simplify a lot"存疑。
- 收益是真实的：现在 "Only the drain (root) reader commits; the snapshot reader never does" 只是 javadoc 里的君子契约，拆接口后由编译器保证，"root reader"（#5）的说法也自然消失。
- 但简化幅度有限：实现类大概率仍是同一个 `FetchedChannelStateReaderImpl`（实现两个接口），真正的复杂度——`Position`、首次定位的 prefix 恢复、`BoundedSegmentStream`——一行都不会少。真正能删代码的是 #22。
- 我的立场：作为类型安全改进可以接受；如果和 #22 一起做，配合起来才谈得上"大幅简化"。
- **2026-08-06 讨论结论：先不拆**。拆分的可行形态是先把 `commit()` 从 Segment 搬到 drain 侧 reader（否则掉进泛型坑），再做两层接口——但实现类不变、调用方代码一行不少，唯一收益是 checkpoint 侧的静态类型干净了。它删不掉任何代码；真正删代码的是 #22。回复说明这一点，看他怎么想。

**回复草稿**：

> I prototyped this: since `commit()` sits on `SpillSegment`, a clean split first requires moving it up onto the drain-side reader (a segment is only valid until the next `nextSegment()`, so "the current segment" is unambiguous). It would look like:
>
> ```java
> // checkpoint side: read-only
> public interface FetchedChannelStateReader extends Closeable {
>     Optional<SpillSegment> nextSegment(); // SpillSegment: channelInfo/bodyStream/length, no commit
> }
>
> // drain side
> public interface DrainingChannelStateReader extends FetchedChannelStateReader {
>     void commitDelivered(); // was SpillSegment#commit
>     FetchedChannelStateSnapshot snapshot();
> }
> ```
>
> However, the split itself doesn't reduce the amount of code at all: the implementation stays a single class, every call site keeps the same number of lines (`segment.commit()` merely becomes `reader.commitDelivered()`), and we end up with one more interface than before — the only gain is that the checkpoint path can no longer call `commit()`/`snapshot()` by construction. I'm happy to do this refactoring, but given that it doesn't actually simplify anything — WDYT?
>
> What does bring real simplification is your suggestion in https://github.com/apache/flink/pull/28661#discussion_r3693955989: once the snapshot carries the resume segment, most of the contractual comments this split would enforce disappear anyway.

### 第二轮（2026-08-06，r3724975334）——本轮两条难点之一

> I think this interface split already makes sense - it makes it clear how each call site uses the interface. I don't understand why the implementation is the same? Can't we split it as well?
>
> > What does bring real simplification is your suggestion in #28661 (comment) ...
>
> Probably we should discuss this offline.

**他的立场**：接口拆分他认可了（价值在"调用点意图清晰"，不在删代码），并且反问实现类为什么不能一起拆，同时提议 offline 讨论。

**分析（实现能不能拆？能，但顺序很重要）**：
- 今天不好拆，是因为两侧共用的不是"同一套简单逻辑"，而是**交叉**的：`commit()` 只有 drain 侧用（挂在 `Segment` 上，还要 `committed` 这个 Position 字段）；而 `firstSegment()` 的 rewind + prefix skip 恰恰只有 **snapshot 侧**用（drain reader 从 offset 0 起，永远走不到那条分支）。硬拆成两个实现类，就要把 `openCurrentFile` / `readHeaderAtCurrent` / `openFileAndSeek` / `readBody` / `BoundedSegmentStream` / `Position` 这一整套顺序读机制抽成基类，而基类要同时容纳"回退重读"和"提交边界"两套特有状态——拆完是三个类，代码不减反增。
- **#22 落地后就便宜了**：rewind/prefix 链路整条消失，snapshot 侧只剩"从 `resumeOffset` 起、首段 remaining 已知"的一个入口分支；drain 侧只剩 `committed` 字段 + `commitDelivered()` + `snapshot()`。那时的自然形态是：

```java
// 顺序读机制（files / position / header / bounded body / close）
class SpillFileReader implements FetchedChannelStateReader { ... }

// drain 侧：多一个 committed 字段和两个方法
final class DrainingSpillFileReader extends SpillFileReader
        implements DrainableChannelStateReader { ... }
```

  checkpoint 侧直接 new 基类，drain 侧 new 子类，两个类都不带对方的字段——这才是他要的"实现也拆开"。

**处理决定：接受拆分（接口 + 实现都拆），但排在 #22 之后、同一批重构里做**；先拆再改 #22 等于把要删掉的代码先精心分家一遍。同意 offline 对齐一次，把顺序讲清楚。

**连带**：拆完后 #3（`snapshotAndInsertBarriers` 名实不符）自然收口——drain 侧接口返回 `FetchedChannelStateSnapshot`（#7 已把它转成 `@Internal public`），方法可以老实返回 snapshot，或改名为 `insertBarriersAndSnapshot`。#3 的回复等这里定稿后一起发。

**新回复草稿**：
> Sure, let's sync offline. To make the discussion concrete, my only concern is ordering, not the split itself: today the two sides are interleaved rather than layered — `commit()`/`committed` is drain-only, but the rewind + prefix-skip path in `firstSegment()` is *snapshot-only* (the drain reader starts at offset 0 and never enters it), so splitting the implementation right now means a base class that still has to carry both. Once the snapshot carries the resume segment (r3693955989), the rewind path disappears entirely and the split becomes natural: a `SpillFileReader` with the sequential-read machinery for the checkpoint side, and a `DrainingSpillFileReader extends SpillFileReader` adding `committed` + `commitDelivered()` + `snapshot()`. So I'd like to do r3693955989 first and then split both the interface and the implementation on top of it — with that, `snapshotAndInsertBarriers` (your other comment) can also return the snapshot itself.

---

## 9. FetchedChannelStateReaderImpl.java:67 — 冗余的 `channelState` 字段

链接：https://github.com/apache/flink/pull/28661#discussion_r3693955958

> Why do we need this field if we already have `snapshot`? (`snapshot.channelState()` is accessible.)

**他建议**：删掉 `channelState` 字段，用 `snapshot.channelState()`。

**我的评价**：✅ 合理的 NIT。字段只是缓存，全类仅 `snapshot()` 方法用到一次，删掉无代价。照改即可。

**回复草稿**：Right, removed the field in favor of `snapshot.channelState()`.

---

## 10. FetchedChannelStateReaderImpl.java:68 — 冗余的 `files` 字段

链接：https://github.com/apache/flink/pull/28661#discussion_r3693955960

> Same question — why cache this separately when `snapshot.channelState().files()` gives us the same list?

**他建议**：删掉 `files` 字段。

**我的评价**：⚠️ NIT，可改可不改。与 #9 不同，`files` 在 IO 热路径上被反复使用（`files.get(...)`、`files.size()` 共 6 处），本地缓存一次比每次 `snapshot.channelState().files()`（还包一层 unmodifiableList）可读性更好。如果 #9 改了、`channelState` 字段没了，那 `files` 反而更该保留。可以回复解释保留理由。

人工第二轮回复：
这个删掉没有什么影响吧？如果没有影响就删掉呗，多调用一个栈好像影响很小啊。我看了调用处可挺多的，我们可以把这个字段移除，但是留一个方法，就不需要每一个调用处都写一下那个繁琐的整个调用链路。

**处理决定（2026-08-06 定）：删字段，留一个私有取值方法。** 多一层方法调用会被 JIT 内联掉，热路径没有实际代价；调用点也不必写 `snapshot.channelState().files()` 这串链路。

```java
private List<Path> files() {
    return snapshot.channelState().files();
}
```

6 处 `files.get(...)` / `files.size()` 改成 `files().get(...)` / `files().size()` 即可。

**新回复草稿（一句话）**：Removed the field — a private `files()` accessor keeps the call sites short.

---

## 11. FetchedChannelStateReaderImpl.java:77 — `fileStream` 改名

链接：https://github.com/apache/flink/pull/28661#discussion_r3693955965

> NIT: rename to `currentFileStream` for consistency with `current` / `currentFileSize` / `currentBody`?

**他建议**：`fileStream` → `currentFileStream`。

**我的评价**：✅ 无争议 NIT，照改即可。

**回复草稿**：Renamed.

---

## 12. FetchedChannelStateReaderImpl.java:80 — `currentFileSize` 字段

链接：https://github.com/apache/flink/pull/28661#discussion_r3693955968

> Why do we need this field? Could `openFileAndSeek()` just return the file size instead of storing it?

**他建议**：`openFileAndSeek()` 返回文件大小，删掉字段。

**我的评价**：⚠️ 可商榷。`openFileAndSeek()` 在流已打开时会早退（不重新 stat 文件），而 `openCurrentFile()` 在早退路径上仍需要 size 来判断"文件是否读完"——改成返回值的话，要么每次重新 `Files.size()`（多余 syscall），要么调用方存一份（字段只是换了地方）。size 与打开的流生命周期绑定，作为字段是自洽的。建议回复解释；如果 #16 重构成循环，可顺势再看这里是否自然消解。

**处理方案（2026-08-06 更新）**：保留字段。`openFileAndSeek()` 在流已打开时早退、不重新 stat，而 `openCurrentFile()` 在早退路径上仍要用 size 判断"文件读完没有"——改成返回值只会把字段挪到调用方。#20 落地后 size 直接来自打开的通道，连那次 `Files.size()` 也省了，字段与流同生命周期更明显。

**回复草稿（18 词）**：Kept — `openFileAndSeek()` returns early when the stream is already open, so the size belongs with the stream.

---

## 13. FetchedChannelStateReaderImpl.java:83 — `currentBody` 与 `Segment.body` 双引用

链接：https://github.com/apache/flink/pull/28661#discussion_r3693955969

> This can't be a local — `nextSegment()`'s entry check needs the previous call's state. But do we need both this field and `Segment.body` holding the same object? Could we store the last `Segment` instead and drop one of the two?

**他建议**：保存 `lastSegment` 代替 `currentBody`，消除同一对象的双引用。

**我的评价**：⚠️ 小重构，收益有限。他自己已经承认这不能是局部变量；换成存 `Segment` 只是把引用从"body"换成"持有 body 的段"，字段数不变。如果 #22/#23 采纳后 `BoundedSegmentStream` 瘦身，这里可以顺势整理。单独做意义不大。

**处理方案（#22 工作包）**：重构时定夺，倾向保留 `currentBody`——`nextSegment()` 入口的 checkState 仍需要上一段的剩余量，换成存 `Segment` 只是把引用上移一层，字段数不变。

**✅ 最终定案（2026-08-06 晚）：采纳他的建议——改存 `Segment`，双引用消失。**

`currentBody` 的唯一用途已经是"判断读的是不是当前段"（#23 把计数搬进 Position 之后，过期段继续读会静默读走下一段的字节）。既然只是这个用途，就该按他说的存 `Segment`：指向 body 的引用只剩它自己的 `Segment` 一处，reader 持有的是段本身。

顺带补上一个原本没守住的洞：**`commit()` 也用同一个字段校验**。过期段调 `commit()` 会把当前段的 live 位置发布成已交付（谎报进度，可能整段跳过），现在同样 fail-loud。

**回复草稿（27 词）**：Done — the reader now stores the last `Segment`. It only exists for the guard: reads and commits must come from the latest segment, older ones fail loud.

---

**（历史结论，已被上面取代）保留 `currentBody`，但理由变了。** 入口 checkState 现在看的是 `current.remaining`（不再需要视图），但 `currentBody` 成了"哪个视图是当前的"这个身份令牌——#23 把计数搬进 Position 之后，一个过期的 Segment 如果继续读，会读进下一段的字节里；`read()` 里的 `checkState(currentBody == this, ...)` 就靠它。

**回复草稿**：Kept — with the byte counter moved into the reader's position, this field is what lets `read()` fail loud when a stale segment is read after advancing.

---

## 14. FetchedChannelStateReaderImpl.java:156 — 方法体内冗余注释

链接：https://github.com/apache/flink/pull/28661#discussion_r3693955971

> Should this comment be removed?

**他建议**：删掉锚点处的注释。锚点已用 GitHub 原始数据核实：单行锚定第 156 行，即 `followingSegment()` javadoc 的 `/**` 起始行——所以他指的是 **`followingSegment()` 的方法 javadoc**，不是 diff 上文里 `firstSegment()` 的行内注释。

**我的评价**：⚠️ 不宜全删，压缩为一行。"steady-state 路径不做任何 skip、流此刻恰好停在下一个 header 上"是 `firstSegment`/`followingSegment` 两条路径的分野，也是 `nextSegment` 入口 checkState 的存在理由，代码本身看不出来，应保留；但 "roll to next file"、"reads the header" 是代码自明的，可砍。已照此把 javadoc 压成一句。（此前一轮误把 `firstSegment()` 里的 `// Discard...` 行内注释当成目标删掉了——那段与 firstSegment 的 javadoc 重复，删除本身成立，保留该删除。）

**回复草稿**：Assuming you mean the `followingSegment()` javadoc — compressed it to one line; I'd keep the "no skipping, the stream already sits on the next header" part since that invariant isn't obvious from the code.

人工 review 结论：人工复核发现首轮处理错了对象（误删 `firstSegment` 行内注释、回复与实际不符）；经锚点核实后按"压缩为一行"重新处理，误删部分作为顺手清理保留。

---

## 15. FetchedChannelStateReaderImpl.java:214 — `rollToNextFile()` 返回 boolean

链接：https://github.com/apache/flink/pull/28661#discussion_r3693955972

> NIT: could `rollToNextFile()` return a boolean instead, so the caller doesn't need this separate bounds check?

**他建议**：`Position.rollToNextFile()` 返回"是否越界"，省掉调用方的 `fileIndex >= files.size()` 检查。

**我的评价**：⚠️ 不太成立。`Position` 是纯位置值对象，不知道也不该知道 `files.size()`；要返回越界与否就得把文件数注入 Position，破坏职责边界。他真正想解决的是下一条（#16）的重复代码——用循环重构后这条自然消失。建议回复指出并以 #16 的方案回应。

**处理方案（2026-08-06 更新）**：不改 `Position`。（注意：#16 最终**没有**折叠成循环，所以本条不能再说"随循环消解"，理由回到职责边界本身。）

**回复草稿（21 词）**：Left as is — `Position` doesn't know the file count, and injecting it just to return a boolean would blur that boundary.

---

## 16. FetchedChannelStateReaderImpl.java:217 — `openCurrentFile()` 重复代码

链接：https://github.com/apache/flink/pull/28661#discussion_r3693955974

> This duplicates the `openFileAndSeek()` + bounds-check block above — can we fold both branches into one loop/helper?

**他建议**：把 `openCurrentFile()` 里两段几乎相同的 "openFileAndSeek + 判断 + roll" 折叠成一个循环。

**我的评价**：✅ 合理，好建议。`while (current.fileIndex < files.size()) { openFileAndSeek(); if (readOffset < size) { startSegmentHere; return true; } closeFileStream(); rollToNextFile(); } return false;` 形式更简洁，还顺带回应了 #15。（细节：writer 从不产生空文件，实际最多滚动一次，但循环形式更通用、更不易错。）照改。

**✅ 最终结果（2026-08-08）：采纳他的折中版**

第三轮他直接贴了代码：循环 + `rolled` 标志 + `checkState(!rolled, "Rolled past more than one empty file")`。这一版同时满足两边——**循环消除了重复**（他的诉求），**断言把"最多滚一次"写进代码**（我们的顾虑：循环形状读起来像能滚任意多次），而且不变量被破坏时当场失败，比原来那版显式两段"静默返回 false"更好。照收，只是没引入他 snippet 里的 `isLastFile()` / `fileHasMore()` 两个 helper（本文件其余地方都用行内条件，且 `isLastFile` 的名字与实际条件"该下标上还有文件"对不上）。

**回复草稿（4 词）**：Nice, adopted — thanks.

---

**（历史）处理方案（2026-08-06 定稿：不采纳，代码保持原样）**

折叠成 `while` 循环试过，也一度加过 `checkState`，最终都回退：**代码一个字不改**。

理由停在可读性层面：这里的 roll 只会发生一次，显式两段把这一点直接写在结构里；循环的形状会让人以为可以反复滚动，读者得额外推理才知道实际次数。至于"为什么只会滚一次"——每个文件至少含一个段（文件懒创建，创建后立刻写入非空段）——这句**备而不发**，他追问时再答，回复里不主动展开。

**回复草稿（29 词）**：Kept explicit: the roll can only happen once, and the two branches say so. A loop would read as if it could repeat arbitrarily — correct me if I'm wrong?

---

## 17. FetchedChannelStateReaderImpl.java:233 — 校验 gateIdx/channelIdx

链接：https://github.com/apache/flink/pull/28661#discussion_r3693955975

> Should we validate `gateIdx`/`channelIdx` here too, not just `bufferLength`?

**他建议**：header 解析时对 gateIdx/channelIdx 也做校验。

**我的评价**：⚠️ 部分合理。reader 与 gate 结构解耦，不知道合法的 gate/channel 上界，能做的只有非负检查——但这个检查便宜，且能更早暴露文件损坏/错位（负数几乎必然意味着读偏了），值得加。完整范围校验只能由持有 gate 结构的消费方做。建议：加 `checkState(gateIdx >= 0 && channelIdx >= 0, ...)`，回复说明上界校验在消费侧。

**处理方案（#22 工作包）**：`readHeaderAtCurrent()` 在 #22 后保留，顺手加非负 checkState；上界校验 reader 层做不到（不持有 gate 结构），留给消费方。

**回复草稿**：Added non-negativity checks; the upper bounds can only be validated by the consumer, which owns the gate structures.

人工第二轮回复：
这里 即使做了22，还是需要有个校验啊，就是至少说是必须大于0啊之类的。也就是说我们正常的读取的时候就要做这个校验。就即使没有snapshot，我们读，我们反序的话也会有这个操作，对吧？我的理解。

**你的理解正确，本条改为「不依赖 #22·容易」。** `readHeaderAtCurrent()` 是每读一个段都要走的常规路径（不只是 snapshot 恢复时），#22 不改它的存在，所以校验本来就该在这里、现在就能加：

```java
checkState(gateIdx >= 0 && channelIdx >= 0, "negative channel info in segment header: %s/%s", gateIdx, channelIdx);
```

上界（gate 数、channel 数）reader 层拿不到（不持有 gate 结构），只能由消费方校验。

**新回复草稿（一句话）**：Added non-negativity checks; the upper bounds can only be checked by the consumer, which owns the gate structures.
---

## 18. FetchedChannelStateReaderImpl.java:248 — 读流没有 buffered

链接：https://github.com/apache/flink/pull/28661#discussion_r3693955978

> Is it intentional that this stream isn't buffered?

**他建议**：（隐含）用 BufferedInputStream 包一层。

**我的评价**：✅ 好发现，值得改。每个段 12 字节 header 一次裸 read = 一次 syscall，段多且小时开销可观；写侧已经用了 `BufferedOutputStream`，读侧不对称。安全性上没有障碍：读取严格顺序前向，位置记账按"交付给上层的字节数"而非底层流物理位置，中途也从不 seek，所以 buffered 的预读不会破坏任何不变量（打开时的 `skipOnStream` 在 buffered 流上同样正确）。照改。

**回复草稿**：Not intentional — wrapped it in a `BufferedInputStream` (reading is strictly sequential, so buffering is safe here).

---

## 19. FetchedChannelStateReaderImpl.java:261 — `skipOnStream` 自查 path

链接：https://github.com/apache/flink/pull/28661#discussion_r3693955980

> Could `skipOnStream` look up the file name itself (`files.get(current.fileIndex)`) instead of taking `path` as a parameter?

**他建议**：`skipOnStream` 内部自己取 `files.get(current.fileIndex)`（path 只用于报错信息）。

**我的评价**：✅ 合理的小清理。两个调用点传的都是 `files.get(current.fileIndex)`，内移没有副作用（stream 参数要保留——`openFileAndSeek` 传的是尚未赋给字段的局部流）。照改。

**回复草稿**：Done — `skipOnStream` now looks up the path itself.

---

## 20. FetchedChannelStateReaderImpl.java:266 — 用 `IOUtils#skipFully`

链接：https://github.com/apache/flink/pull/28661#discussion_r3693955984

> Can we use `org.apache.flink.util.IOUtils#skipFully` here instead of hand-rolling this loop?

**他建议**：用现成的 `IOUtils.skipFully` 替换手写 skip 循环。

**我的评价（第一轮）**：⚠️ 有坑，我建议保留现状并回复说明。核实过 `IOUtils.skipFully` 的实现（flink-core `IOUtils.java:161`）：它只循环 `in.skip(len)` 并检查返回值 `< 0`——但 `InputStream.skip` 的契约是 EOF 时返回 0 而不是负数，所以它在某些流上会**死循环**；而 `Files.newInputStream` 返回的 NIO ChannelInputStream 的 skip 又可能直接 seek 越过 EOF 虚报成功，**探测不到截断文件**。手写循环里 `skip 返回 0 → 退化为 read() 兜底` 正是为 fail-loud 设计的。这条是很好的讨论素材：表面是"别造轮子"，实际上现有轮子有缺陷。（另一个选项是修 IOUtils，但那超出本 PR 范围。）

### 第二轮（2026-08-06，r3724987221）——本轮两条难点之二

> Yes, this comment is about `skipOnStream`, sorry for misplacing. I think `skipOnStream` can be replaced by `IOUtils#skipFully`.
>
> `readFully` as well - it was the [next](https://github.com/apache/flink/pull/28661#discussion_r3693955986) comment

**他的立场**：贴错行已澄清；他仍然要求 `skipOnStream` 换成 `IOUtils#skipFully`。

**缺陷复核（结论不变，已再看一遍源码）**：`IOUtils.skipFully`（`flink-core/.../IOUtils.java:161-169`）只做 `while (len > 0) { ret = in.skip(len); if (ret < 0) throw; len -= ret; }`。`InputStream#skip` 的契约是**到达 EOF 时返回 0**，不是负数——而 #18 之后我们的流是 `BufferedInputStream`，缓冲区空且底层已 EOF 时 `skip` 恰好返回 0，于是 `skipFully` 会**原地死循环**（不是抛异常，是挂住任务）。所以直接替换是不安全的。

**建议的处理：不是"保留手写循环"，而是彻底不用 skip——改成打开时直接 seek。** `openFileAndSeek()` 唯一需要跳过的就是打开文件时定位到 `readOffset`（#22 落地后 `skipBody()` 变死代码删除，`skipOnStream` 只剩这一个调用点）。用 NIO 的可定位通道一步到位：

```java
SeekableByteChannel ch = Files.newByteChannel(path, StandardOpenOption.READ);
currentFileSize = ch.size();                 // 顺带省掉一次 Files.size(path) 的 stat
ch.position(current.readOffset);             // O(1) 定位，不读不丢字节
currentFileStream = new BufferedInputStream(Channels.newInputStream(ch));
```

这样 `skipOnStream()`（约 20 行）整个删除，比 `IOUtils.skipFully` 更省（不做任何读丢弃）、也不踩它的坑；fail-loud 不丢失：越界定位由既有的 `readOffset < currentFileSize` 判断挡住，文件被截断则第一次 `readFully` 抛 "Premature EOF"。顺带回应了 #12（`currentFileSize` 现在来自打开的通道本身，与流生命周期绑定得更自然）。

**备选（若他坚持用 IOUtils）**：先在 flink-core 修 `skipFully`（`ret <= 0` 时 fallback 到 `read()`，与我们现在的循环等价），单开一个 ticket，本 PR 再切过去。成本高于上面的 seek 方案，不推荐。

**处理方案（2026-08-06 更新，#22 已落地）**：`skipBody()` 已随 #22 删除，`skipOnStream()` 只剩 `openFileAndSeek()` 里"定位到 `readOffset`"这一个调用点——也就是说需求已经退化成**纯定位**，不该再用 skip 表达。改法：用可定位通道打开文件，`skipOnStream()`（约 20 行）整个删掉；`currentFileSize` 顺便从通道自身拿，省掉一次 `Files.size()` 的 stat（连带回应 #12）。

fail-loud 不丢失：越界定位由既有的 `readOffset < currentFileSize` 判断挡住，文件被截断则第一次 `readFully` 抛 "Premature EOF"。不采用 `IOUtils#skipFully` 的理由不变（`InputStream#skip` 在 EOF 返回 0，而它只在返回负数时退出 ⇒ 配 `BufferedInputStream` 会死循环）。

**新回复草稿（26 词）**：Dropped the skipping instead: the reader now seeks the channel directly. `skipFully` would spin forever on truncation — `skip()` returns 0 at EOF, it only checks negatives.

---

## 21. FetchedChannelStateReaderImpl.java:283 — 用 `IOUtils#readFully`

链接：https://github.com/apache/flink/pull/28661#discussion_r3693955986

> Can we use `org.apache.flink.util.IOUtils#readFully` here instead of hand-rolling this loop?

**他建议**：用 `IOUtils.readFully` 替换手写 readFully。

**我的评价**：⚠️ 与 #20 不同，这个替换是可行的（`IOUtils.readFully` 行为正确：EOF 抛 IOException），但有两个代价：(1) 丢掉带文件名/偏移量的错误信息——对排查损坏 spill 文件很有价值；(2) `current.advanceReadOffset` 的记账要挪到调用之后一次性做（异常路径 offset 不准，但 reader 已经要抛异常了，无所谓）。如果改，建议 catch 住再包装上下文信息重抛。可改可不改，我倾向于改（catch + 包装），能删十几行。

**回复草稿**：Done — switched to `IOUtils.readFully`, wrapping the exception to keep the file/offset context in the message.

### 第二轮（2026-08-06，r3724990647）

> Do we still need to catch the IOException after switching to IOUtils.readFully?

**背景**：这条线程里我从没发过言（上面那份回复误发在 #20 线程），但代码确实已经改成 `IOUtils.readFully` + catch 包装了；他看到新代码后追问 catch 是否多余。第二轮要在这条线程里补上说明。

**处理决定：照他说的删掉 catch。** 现状是：

```java
private void readFully(byte[] buf) throws IOException {
    try {
        IOUtils.readFully(currentFileStream, buf, 0, buf.length);
    } catch (IOException e) {
        throw new IOException("Truncated segment header in file " + files.get(current.fileIndex)
                + " at offset " + current.readOffset, e);
    }
    current.advanceReadOffset(buf.length);
}
```

包装只是往消息里补文件名和偏移量，代价是 8 行样板 + 一层嵌套异常；而这条路径失败必然是 spill 文件损坏/截断这种"任务直接失败"的场景，栈里 `FetchedChannelStateReaderImpl.readHeaderAtCurrent` 已经指明了在读 header，文件名也能从上层的 `FetchedChannelState` 拿到。删掉后方法只剩两行：

```java
private void readFully(byte[] buf) throws IOException {
    IOUtils.readFully(currentFileStream, buf, 0, buf.length);
    current.advanceReadOffset(buf.length);
}
```

（此时 `readFully` 只剩一个调用点、两行，也可以顺手内联进 `readHeaderAtCurrent()`——但保留一个具名方法更好读，倾向不内联。）

**新回复草稿（一句话）**：No — the catch only wrapped the exception to add the file/offset, removed it.


人工第二轮回复：
回复太长了，就说我们之前只是把异常给包装了一层，也可以不需要，就说已经删了就行了。


---

## 22. FetchedChannelStateReaderImpl.java:346 — header 随 snapshot 携带（删 `segmentStartOffset`）

链接：https://github.com/apache/flink/pull/28661#discussion_r3693955989
（总评四条"大幅简化"建议之一）

> Could we delete this field and instead carry the parsed header in `FetchedChannelStateSnapshot` (nullable for the root reader), so we don't need to seek back to the header? I think this would simplify a lot of the code in this PR.

**他建议**：`Position` 删掉 `segmentStartOffset`；snapshot 直接携带已解析的段 header（root reader 为 null），恢复时不再回退到 header 重读。

**我的评价**：✅ 四条"大幅简化"里最有分量、最实的一条，我认为值得采纳。关键观察：`committed.readOffset = segmentStartOffset + HEADER + delivered`——它本来就精确指向交付边界。如果 snapshot 随身带上 header 信息（channelInfo + 段全长 + 已交付字节数），恢复 reader 就可以直接从 `readOffset` 开读剩余部分，于是一整条链路可删：
- `firstSegment()` 的 rewind + prefix skip 逻辑（当前类里唯一的 skip 路径）；
- `Position.rewindToSegmentStart()` / `deliveredBodyBytes()`；
- `BoundedSegmentStream.alreadyDelivered`（联动 #23）；
- 读路径统一成唯一的 steady path。

代价：snapshot 不再是"纯位置"，多带一点不可变元数据（12 字节 header 的解析结果），概念上完全站得住。需要想清楚的是 `commit()` 的记账方式要相应调整（committed 要能重建"段起点"或直接存 header + delivered）。方向对，细节需要设计一轮。

**处理方案（#22 工作包核心，方案已在「集中讨论」定稿）**：Snapshot 改五元组 `{channelState, fileIndex, resumeOffset, channel, remaining}`；commit 永远记录"当前段自己"的元数据（段读完即 `remaining=0`）；`ResumeSegment == null` 语义 = 快照时尚未有任何 commit（从头恢复）；删除 `firstSegment()` 回退链、`rewindToSegmentStart`、`deliveredBodyBytes`、`copyAsDelivered` 偏移换算，`skipBody()` 变死代码一并删。

**✅ 已实施（2026-08-06，fixup `29ee9005821`）**，最终形态与草案的差异：

- Snapshot 是**五个扁平字段**（`channelState, fileIndex, readOffset, channel, remaining`），没有嵌套的 `ResumeSegment` 小类；构造器校验 `(channel == null) == (remaining == 0)`。
- 状态只有两种，判据是 `channel` 是否为 null：null ⇒ `readOffset` 落在段头（"从头开始"只是 `(0,0)` 这个特例）；非 null ⇒ 落在 body 中间，`remaining` 是该段未交付字节数。**不再需要"null = 尚未 commit"这层编码**。
- `Position` 一并收成同样的四个值，reader 持有两个（`current` / `committed`），于是 `commit()` 退化成 `committed.copyFrom(current)` 一行，`snapshot()` 直接读 `committed` 四值。
- 删除：`Position.segmentStartOffset` / `rewindToSegmentStart()` / `deliveredBodyBytes()` / `copyAsDelivered()` / `startSegmentHere()`、`firstSegment()` 回退链、`skipBody()`、`BoundedSegmentStream.alreadyDelivered` + `deliveredFromSegmentHead()`、`currentChannel` 字段。
- 新增测试 `testSnapshotCarriesResumeSegmentOnlyWhenBoundaryIsMidBody` 锁住三种取值。493 个相关测试全绿。

**回复草稿**：Done via 53a8d71f6397b26c815d6f30896157000004d57d, and re-defined FetchedChannelStateSnapshot.

---

## 23. FetchedChannelStateReaderImpl.java:467 — `BoundedSegmentStream` 的三连问

链接：https://github.com/apache/flink/pull/28661#discussion_r3693955990
（总评四条"大幅简化"建议之一）

> I understand this class is needed for tracking bytes read, but I'm not sure it's still necessary if the other suggestions here are applied. If we keep it: (1) why not delegate/wrap and ask `FetchedChannelStateReaderImpl` for the tracked value instead? (2) could we make it static to make its purpose clearer? (3) if (1) is the right read, the class javadoc is now inaccurate.

**他建议**：若其他建议采纳，考虑删掉或瘦身此类；保留的话考虑委托取值、改 static、修 javadoc。

**我的评价**：✅ 与 #22 联动，方向合理。事实上他的 (1) 已经部分成立：`commit()` 记录的 `alreadyDelivered + read` 恰好等于 `current.readOffset - segmentStartOffset - HEADER`（即 `Position.deliveredBodyBytes()`），可以从 Position 推导而不必自己记。若 #22 采纳，`alreadyDelivered` 直接消失，类只剩"限长 + fail-loud EOF"两个职责——那时它是否 static（传 reader 引用）只是风格问题。javadoc 随实现更新。这条跟着 #22 一起处理即可。

**处理方案（#22 工作包）**：随核心改动删 `alreadyDelivered`/`deliveredFromSegmentHead`，类只剩限长 + fail-loud EOF 两职责；保持非 static 内部类（需要 reader 的 `readBody`）；javadoc 重写。

**✅ 已实施**：(1) 采纳——视图不再自己计数，边界直接问 `current.remaining`，只留一个 final `length` 供 `SpillSegment#length()`；(3) javadoc 已重写。(2) 仍是非 static，因为它要用 reader 的 `readBody` 和 `current`。

一个副产品：视图不再自计数后，"过期的段被继续读"会读串到下一段，所以 `read()` 里加了 `checkState(currentBody == this, ...)`，误用变成当场失败（这也是 #13 保留 `currentBody` 的新理由）。

**回复草稿**：Done for (1) and (3) — the view no longer counts anything, it takes the bound from the reader's position, and the javadoc is rewritten. Kept non-static since it needs the reader's `readBody` and position.

### 第三轮（2026-08-07 21:08）——他追问 (2) static

> `Kept non-static since it needs the reader's readBody and position.`
>
> Is there any reason not to pass the necessary objects into its contructor?

**背景回顾（这条已经跨了三轮，先把来龙去脉理清）**

`BoundedSegmentStream` 就是 `SpillSegment#bodyStream()` 交给消费者的那个 `InputStream`。它只有两个职责：**限长**（正好交出本段 body 的字节，绝不读进下一段或下一个文件）和 **fail-loud EOF**（文件提前结束就抛 `EOFException`，而不是静默返回 -1）。

它这三轮的演变：

| 轮次 | 状态 |
|---|---|
| 最初 | 自己带计数器：`read` / `remainingLength`，外加 `alreadyDelivered`（snapshot 从 body 中间恢复时的已交付前缀） |
| #22 落地后 | 前缀概念消失（snapshot 直接带 resume point），`alreadyDelivered` 删除 |
| #23 (1) 采纳后 | **计数器整个搬进 reader 的 `Position`**：视图只剩一个 final `length`（供 `SpillSegment#length()`），边界改问 `current.remaining` |
| #13 定案后 | 因为视图不再自计数，过期的段继续读会**静默读走下一段的字节**，所以 reader 保存 `currentSegment`，`read()` 里校验 `currentSegment.body == this` |

现在他回到最初三连问里的 (2)：既然你说需要 reader 的东西，那**为什么不把这些东西通过构造器传进去**，从而做成 static？

**依赖分析（"传必要对象" ≡ "传整个 reader"）**

视图今天依赖外部三样东西：

| 依赖 | 用途 |
|---|---|
| `readBody(buf, off, len)` | 真正读字节，并推进 `current` |
| `current.remaining` | 边界判断 + EOF 消息里的 `length - remaining` |
| `currentSegment` | `read()` 里的过期校验 |

后两个是**调用时刻的可变状态**，不能在构造时快照。想只传 `Position` + 一个读函数也不行——过期校验要读 `currentSegment`，那是 reader 的字段。所以"把必要对象传进构造器"落地后就是**把 reader 自己传进去**：对象图与今天完全相同，只是把编译器隐式生成的 outer 引用换成一个显式字段。

**一致性问题**：`Segment` 同样是非 static 内部类，同样用 `current`/`committed`/`currentSegment`。要改就该两个一起改，只改一个更乱。

**两个选项**

- **A. 照做**：两个内部类都改 static，构造器各接一个 `FetchedChannelStateReaderImpl reader`，内部改成 `reader.readBody(...)` / `reader.current.remaining` / `reader.currentSegment`。约 5–10 行，行为零变化，收益是依赖显式可见（正是他 (2) 说的 "make its purpose clearer"），代价是每处多一个 `reader.` 前缀。
- **B. 解释后保留**：说明"传必要对象"等价于传整个 reader，static 只是把隐式引用变显式。

**✅ 已实施（2026-08-08，选 A）**：`Segment` 与 `BoundedSegmentStream` 都改成 `private static final`，各自显式持有 `FetchedChannelStateReaderImpl reader`；内部访问改为 `reader.readBody(...)` / `reader.current` / `reader.currentSegment` / `reader.committed`。两个一起改是为了一致——现在这个文件里的四个嵌套类（`Position`、`SegmentHeader`、`Segment`、`BoundedSegmentStream`）全是 static。对外零影响：依赖对象图不变，接口、磁盘格式、测试一行未改。

**回复草稿（6 词）**：Done — both are static now.

### 第四轮（2026-08-07 22:40/22:42）——他直接给了 diff

原话：

> Is ^^^ this a correct change? It wasn't clear to me what's the scope/lifecycle of `BoundedSegmentStream` and it's relation to `current` stream; which clearly signals bad design.
>
> If the above diff is a correct change (which I believe it is) can you please apply it? Otherwise, let's have a follow up to revisit this.

**他的 diff 做了三件事**：

1. `BoundedSegmentStream` 的构造参数从 `reader` 换成 `(InputStream fileStream, Position current, int length)`——依赖变窄、显式。
2. 把 reader 的 `readBody()` 整个删掉，读字节 + `current.advanceBody(n)` 内联进视图的 `read()`。
3. **删掉了过期校验** `checkState(reader.currentSegment != null && reader.currentSegment.body == this, ...)`。

**评估**：1 和 2 是干净的改进，正面回应了他说的"scope/lifecycle 不清楚"——视图从此只依赖它真正用到的两样东西。3 是丢失：

- 视图在构造时捕获 `currentFileStream`。段不跨文件，所以这个引用在本段生命周期内始终有效。
- **跨文件的过期误用**反而会自然 fail-loud：roll 之后旧流已 `close()`，拿旧段继续读会抛 "Stream closed"。
- **同文件内的过期误用仍然静默出错**：旧视图握着同一个还开着的流、共享同一个 `current`，继续读会吃掉下一段的字节并把 `current.remaining` 减掉——正是 #13 那道 guard 防的场景。

**三个选项**：

- **A. 原样应用**：拿到显式依赖，放弃 guard，契约只留在 javadoc。
- **B. 应用他的形状 + 保留 guard（推荐）**：视图加一个 `stale` 标志，reader 在交出新段之前把上一个视图置为 stale（reader 本来就通过 `currentSegment.body` 够得着它）。方向仍是 reader → view，视图不需要反向持有 reader，他要的"依赖窄而显式"和我们要的 fail-loud 同时成立。
- **C. 不动，开 follow-up**：他自己给了这个台阶，但代价是把一处他明确不满意的设计留在合入的代码里。

**回复草稿（A）**：Applied, thanks.

**回复草稿（B）**：Applied — the view now takes just the stream and the position. I kept the stale-segment check, in a form that doesn't need the reader: the reader marks the previous view stale before handing out a new one. Without it, reading a stale segment from the same file would silently consume the next segment's bytes.

---

## 24. FetchedChannelStateSnapshot.java:66 — 为何不允许多 reader

链接：https://github.com/apache/flink/pull/28661#discussion_r3693955992
（总评四条"大幅简化"建议之一）

> Why can't we have multiple readers per snapshot? Is it because the snapshot is mutable? That shouldn't be the case - snapshot is immutable by definition IMO.

**他建议**：取消 1:1 snapshot-reader 约束，snapshot 应是不可变的、可开任意多 reader。

**我的评价**：⚠️ 他对"不可变性"的判断是对的（position 会被 reader 立即 copy，snapshot 本身确实不可变），但 1:1 约束的真正根源不是可变性，而是**生命周期 grant 的记账方式**：snapshot 构造时 acquire 一个 grant，靠"唯一那个 reader 的 close"来归还。允许多 reader 技术上可行（比如每次 `reader()` 各自 acquire、snapshot 的 grant 单独归还），但真实用例只有"一个 checkpoint 写一次"，one-shot 是防误用的保守设计。**这条的正确回应是修 #25 的泄漏**——泄漏修好后（snapshot 有独立的 dispose 语义），多 reader 的限制是否放开就只是口味问题了。适合回复讨论而不是直接改。

**处理方案（2026-08-06 定稿）**：不改代码。回复要落在**业务语义 + 记账方式**上，不要绕"可变性"：

1. 业务上根本不需要一个 snapshot 被读多次——一次 checkpoint 读一遍就完了，所以 1:1 是防误用，不是能力限制。
2. 正因为不需要，我们也就没做"每个 reader 各自计数"的复杂记账：**snapshot 只持有一个 grant，开 reader 等于把这个 grant 的所有权交给 reader，reader close 时归还**，一开一还严格对应。要支持多 reader，就得让每个 reader 各自 acquire，记账立刻变复杂，而收益为零。

**回复草稿（28 词）**：We don't need multiple readers — a snapshot is read once — so rather than refcounting per reader, opening one transfers the snapshot's single grant; the reader's close returns it.

---

## 25. FetchedChannelStateSnapshot.java:73 — 不 open reader 就泄漏 grant

链接：https://github.com/apache/flink/pull/28661#discussion_r3693955995

> If `reader()` is never called on a snapshot, is the lifecycle grant acquired in the constructor ever released? Looks like it would leak.

**他建议**：修掉"snapshot 创建后从未调用 `reader()` 则 grant 永不归还"的泄漏。

**我的评价**：✅ 真实 bug，必须修。构造函数 `acquire()`，唯一的归还路径是 `reader().close()`。一旦 checkpoint 在拿到 snapshot 后、open reader 前被 abort（这是完全现实的异常路径），refCount 永远回不到 0，spill 文件永不删除。修法方向：给 snapshot 加显式的 `dispose()`（幂等，与 reader-close 互斥归还），或者改为 `reader()` 时才 acquire——但后者在"snapshot 已创建、reader 未打开"的窗口里文件可能被 drain 完成方删除，需要权衡。这条没得争，接受并修。

**定性修正（2026-08-06，核对 `40081/pr` 后）**：集成后的代码里是 `return rootReader.snapshot().reader();`——snapshot 一造出来立刻开 reader，**"建了 snapshot 却不开 reader"这条路径在生产流程中不存在**。所以这不是会真实发生的泄漏，而是 API 卫生问题（误用才触发）。仍然值得修（很便宜，且能让 #3 的"返回 snapshot"变成同样安全的选项），但不必按 must-fix 排优先级。

**处理方案（2026-08-06 定稿：grant 的"转移"语义）**

先把现状的记账写清楚，改动才有依据：

```
FetchedChannelState.refCount
  +1  handler 的 closeInternal() 里的 handoff grant   → drainer.drain() 里 release
  +1  每个 snapshot 构造时 acquire                     → 由该 snapshot 的 reader.close() 归还
归零 → 删除全部 spill 文件
```

注意 `reader.close()` 里调的是 `snapshot.release()`：**一个 snapshot 只对应一个 grant，开 reader 等于把归还责任从 snapshot 转移给 reader**，不是各持一个。

改法就是把这个转移语义补完整——Snapshot 实现 `AutoCloseable`，`close()` 幂等：

| 情况 | `close()` 的行为 |
|---|---|
| 没开过 reader | release 自己那个 grant（此时无人在读，安全） |
| 开过 reader | **no-op**——责任已转移，由 `reader.close()` 归还 |

由此得到两条：**提前关 snapshot 不会误删文件**（grant 还在 reader 手里）；`close()` 之后再调 `reader()` 必须 fail-loud。

（另一种设计是 snapshot 与 reader 各持一个 grant、两个都要关。更对称，但要求调用方无论如何都得关 snapshot，容错更差，不采用。）

**测试**：补"建了 snapshot 不开 reader，close 后 refCount 归零、文件被删"；补"close 后再 reader() 抛异常"；另外 #22 新加的 `testSnapshotCarriesResumeSegmentOnlyWhenBoundaryIsMidBody` 连开三个 snapshot 没关，一并关掉。

**回复草稿（22 词）**：Fixed — the snapshot is closable now, releasing the grant when no reader was opened; the integrated path always opens one immediately.

### 第三轮（2026-08-07 21:15）——他问"到底该关谁"

> Thanks. But It seems unclear now how to properly close it: only the reader, the snapshot, or both? And closing both would double-free fetched channel state.

**已于 21:43 回复并随后补充**（人工）：

> The ownership will be passed from snapshot to reader once reader is created from snapshot.
> - Closing the snapshot is a no-op once a reader was opened.
> - Snapshot releases it when non-reader.

三选一的问题由两个 bullet 隐含答成"都关都安全"，double-free 也随之消解。（措辞小瑕疵：`when non-reader` 更自然的写法是 `when no reader was opened`，不值得为此再编辑。）

---

## 26. RecoveredChannelStateHandler.java:337 — 不切换的通道堆内无界累积

链接：https://github.com/apache/flink/pull/28661#discussion_r3693955998

> A channel that never switches never calls `sealCurrentSegment()` — so its segment accumulates unboundedly in heap (`segmentSerializer`), not just missing out on disk rotation. Should we force-seal based on accumulated size here?

**他建议**：在 `segmentSerializerFor`/`switchChannelIfNeeded` 里按累计大小强制 seal。

**处理方案（2026-08-06 已实施）**：软阈值，与文件轮转（`DEFAULT_SPILL_FILE_SIZE_BYTES` = 64MB）同一套路。

- 新常量 `DEFAULT_MAX_SEGMENT_SIZE_BYTES = 1MB`，与文件阈值并排；同样作为构造器参数传入（测试可传小值），构造器**不新增**，只多一个参数。
- 原 `switchChannelIfNeeded` 改名 `startNewSegmentIfNeeded`，早退条件加一项：同通道 **且** `segmentSerializer.length() <= maxSegmentSizeBytes` 才复用当前段。
- 判断放在"下一次写入之前"而不是"本次写完之后"：这样两种开新段的理由（换通道 / 超尺寸）收在同一处，两个 handler 的 `recover()` 一行不动。代价是超标的段多留到下一次 `recover()`——恢复是紧密循环，下一次立刻到；真没有下一次则由 `closeInternal()` seal。堆内峰值仍是"阈值 + 一个 recovery buffer"。
- 阈值比的是 `segmentSerializer.length()`（含 12 字节 header）：header 本身也占堆，没必要为精确扣掉它而多写代码。
- 磁盘格式与 reader 零改动——同一通道出现连续多段本来就合法（文件轮转时就会发生）。
- 新测试 `testSameChannelIsSplitIntoSegmentsWhenSizeBoundIsExceeded`：段阈值传 8 字节，同通道连写三条记录，断言文件长度 = `3 × (header + 4 + 8)`，即写出了三个 header（不生效则只有一个）。

**回复草稿（23 词）**：Yes — added a 1 MB soft bound: a new segment now starts on a channel switch or once the open one outgrew it.

### 第三轮（2026-08-07 21:17）——他贴的是旧代码

他引用了改名前的 `switchChannelIfNeeded` 并问"没切换通道时是不是还在无界累积"。**已于 21:40 回复**（人工）：指出他看的不是最新代码，`switchChannelIfNeeded` 已在 `e0942ba2fce` 改名为 `startNewSegmentIfNeeded` 并引入 `DEFAULT_MAX_SEGMENT_SIZE_BYTES`。

时间线核对过：该 commit 提交于 13:36 UTC 且已在 `origin/39524/pr` 上，他 21:17 UTC 才提问，所以"未拉取最新代码"是事实陈述。

**我的评价**：✅ 本轮最重要的一条，必须修。段只在"通道切换"或 close 时 seal，而恢复数据是按 handle 顺序读的——同一通道的 buffer 往往连续到来。极端但常见的场景：单输入通道的 task 有 1 GB 恢复状态 → 整个 1 GB 堆在 `DataOutputSerializer` 的堆内 buffer 里直到 close，**恰好击穿 spilling 的初衷（有界内存）**。修法直接：`segmentSerializer.length()` 超过阈值（比如复用 64 MB 或单独一个更小的常数）就 force-seal——磁盘格式本就允许同一通道出现连续多段，reader 侧无需任何改动。接受，优先修。

---

## 27. RecoveredChannelStateHandler.java:362 — 空段被静默丢弃

链接：https://github.com/apache/flink/pull/28661#discussion_r3693956000

> Is a zero-body segment an exceptional situation? If not, should we still write the zero length rather than dropping the segment silently?

**他建议**：要么确认空段是异常情况，要么写出 0 长度段而非静默丢弃。

**我的评价**：⚠️ 我认为现状（drop）是对的，回复解释即可。空段不是异常——整段数据被 filter 掉是完全正常的（这正是 filtering 的目的）；而空段对恢复没有任何信息量（没有字节要交付给通道），写出去只是白占 12 字节 header 并让 reader 多空转一轮。`sealCurrentSegment` 的 javadoc 已写明此行为。唯一值得自问的是：有没有下游依赖"每个通道至少出现一个段"？在当前设计里没有（recovered 与否由 handle 层面决定，不由段决定）。可以谈，但我方立场是不改。

人工第二轮回复：

这个我的理解，你需要去看一下快照相关逻辑，就是我们的快照有没有可能会出现这种情况。快照分成两种情况，就是第一个是普通的ChatGPT的快照，第二个是对于我们这种快照磁盘上的数据的时候，有没有可能会出现这种，就是有一个，就是我们整体流程上的快照，就要保证，如果一个segment是空的话，我们就不对它进行快照了。即使会出现，我们也要把这种在快照的时候就要丢弃掉。所以我觉得我们这块不需要兼容，如果真的发生了，我们比这里应该报错。

**核实结果（2026-08-06，只读代码）——空段在写侧是合法且可预期的，不能报错**：

- **写侧会自然产生**：`SpillingWithFilteringHandler.recover()`（`RecoveredChannelStateHandler.java:555-573`）先调 `segmentSerializerFor(channelInfo)`——它在 `switchChannelIfNeeded()` 里**先把 12 字节 header 写进 segmentSerializer**，之后才执行 `filterAndRewrite`。如果这个 buffer 的记录全部不属于本 subtask（rescaling 下完全正常），或者这个 buffer 只装了一条跨 buffer 记录的前半段，就一个 body 字节都不会追加。
- 段只在**通道切换或 close** 时 seal，所以真正 seal 出空段的条件是：**某个通道的全部恢复内容都没有存活字节**——这在 rescaling 时是正常结果，不是异常。此时报错会直接打挂合法的恢复流程。
- **快照/读侧不会看到空段**：writer 既然 drop，文件里根本不存在 body 长度为 0 的段；#22 之后 snapshot 的 `remaining == 0` 表示"边界落在下一个段头"，也不会产出空段给消费方。所以你担心的"快照时要把空段丢掉"这件事，在设计上无处可发生。

**处理决定：维持 drop，不报错**；顺手在 `sealCurrentSegment()` 的 javadoc 里点明"整段被过滤掉是正常情况"，让下次读代码的人不用再问一遍。

**新回复草稿（一句话）**：Not exceptional — a channel whose records are all filtered out legitimately yields no body bytes, so the segment is dropped instead of writing a header-only no-op.

---

## 28. RecoveredChannelStateHandler.java:386 — 文件名跨任务碰撞

链接：https://github.com/apache/flink/pull/28661#discussion_r3693956003

> Can't we get a file name collision here if other tasks are using the same tmp dir?

**他建议**：（隐含）避免多任务共用 tmp 目录时的文件名碰撞。

**我的评价**：❌ 不成立，直接回复指出即可。`baseDir = spillTmpDirectories[0] + "flink-channel-spill-" + UUID.randomUUID()`（构造函数，`RecoveredChannelStateHandler.java:322-323`）——每个 handler 有自己的 UUID 目录，`spill-segment-N.bin` 只在自己目录内编号，跨任务不可能同路径。他应该是漏看了 UUID。（顺带一提：目前永远只用 `spillTmpDirectories[0]`、不做多目录轮转，那是另一个可谈的话题，但与碰撞无关。）

人工第二轮回复：

你说这几个简单，但是这几个你没有生成回复语呀，我们怎么去回复啊？应该就简单给他回复一下，就是有UUID做区分，重复不了。

**新回复草稿（一句话）**：No — each handler writes into its own `flink-channel-spill-<UUID>` directory, so the per-directory `spill-segment-N.bin` numbering can't collide across tasks.

---

## 29. RecoveredChannelStateHandler.java:389 — 用 CREATE_NEW 打开文件

链接：https://github.com/apache/flink/pull/28661#discussion_r3693956005

> Can we open the file for writing in WRITE_IF_NOT_EXISTS mode to fail loudly on collisions?

**他建议**：以"文件必须不存在"的模式创建 spill 文件，碰撞时 fail loud。

**我的评价**：✅ 低成本防御，顺手可改。虽然 #28 说明碰撞在设计上不可能，但 `Files.newOutputStream(path, CREATE_NEW, WRITE)` 一行就能把"UUID 目录残留/重试复用目录"之类的意外变成显式失败而不是静默覆盖，还能顺带把 `new FileOutputStream(path.toFile())` 换成 NIO 风格。接受。

**回复草稿**：Done — the file is now opened with `Files.newOutputStream(CREATE_NEW, WRITE)` to fail loudly on collisions.

---

## 30. RecoveredChannelStateHandler.java:395 — `getProducedChannelState` 未接线

链接：https://github.com/apache/flink/pull/28661#discussion_r3693956010

> Confirmed unused outside tests — is wiring this into `readInputData` tracked as a follow-up (JIRA/TODO), or should this PR add that already? The PR description says no production path produces/consumes spill files yet, so I assume this is intentional — worth a TODO here referencing the follow-up ticket?

**他建议**：加一个 TODO 指向后续接线的 ticket。

**我的评价**：✅ 合理，成本极低。这是拆 PR 的固有现象（他自己也认可是 intentional），加一行 TODO 指向 FLINK-38544 的后续子票 / PR #28662 即可，双方都舒服。接受。

**回复草稿**：Yes, intentional — added a TODO referencing FLINK-38544; the wiring lands with the spilling-backend follow-up (#28662).

**第二轮连带（线程已 resolve，无需再回复，但代码要动）**：#31 第二轮决定直接把 `getProducedChannelState()` 接进 `readInputData`，所以这里加的 TODO 一并删除——接线后它不再是"仅测试使用"的死代码。若想让他知道，可在 #31 的回复里带一句（已带）。

---

## 31. SequentialChannelStateReaderImpl.java:112 — 精简注释 + 静默丢状态的 TODO

链接：https://github.com/apache/flink/pull/28661#discussion_r3693956012

> Could this comment be trimmed down to just the "FLINK-38544 transitional" paragraph below? The paragraph above feels redundant with what the code already expresses.
>
> Separately: Today this is harmless since no production path spills real files yet, but once the handler factory starts selecting the `Spilling*` handlers, this line will silently drop the real `FetchedChannelState` instead of returning it (`readInputData` always returns `Optional.empty()` or a fresh empty container, never `stateHandler.getProducedChannelState()`). Worth a TODO here pointing at whatever ticket wires this up?

**他建议**：(a) 注释只留 "FLINK-38544 transitional" 一段；(b) 加 TODO 提示将来必须改为返回 `stateHandler.getProducedChannelState()`，否则 Spilling handler 被选中后会静默丢状态。

**我的评价**：(a) ⚠️ 偏主观——第一段解释的是"为什么条件是 readAny 而不是 filter ran"，这不是代码自明的，有保留价值，但确实可以压缩到两三行；可以部分让步。(b) ✅ 合理且重要——他对"将来会静默丢真实状态"的判断是对的，这正是 #28662 要接的线，与 #30 是同一件事的两端，加 TODO 指向同一 ticket。接受 (b)，(a) 折中。

**回复草稿**：Compressed the first paragraph to the non-obvious part (why the condition is "readAny" rather than "a filter ran") and added the TODO about returning `getProducedChannelState()` once the `Spilling*` handlers are selected.

### 第二轮（2026-08-06，r3725005316）

> > added the TODO about returning getProducedChannelState() once the Spilling* handlers are selected.
>
> Can't we return it already? We already return - just the wrong value:
> ```
>             return filterContext.isCheckpointingDuringRecoveryEnabled() && readAny
>                     ? Optional.of(new FetchedChannelState(java.util.Collections.emptyList()))
> ```

**他说得对，但有个他看不见的障碍**：`producedChannelState` 是在 `AbstractSpillingHandler.closeInternal()` 里才构建的（`RecoveredChannelStateHandler.java:418-432`：seal 最后一段 → flush/close 文件流 → `new FetchedChannelState(files)` → `acquire()`）。而现在这行 `return` 在 try-with-resources **块内**，求值时 handler 还没 close，`getProducedChannelState()` 必然返回 `null`。所以不是"填个方法名"就行，得先让 handler 关闭再取值。

**处理决定（2026-08-06 定）：不接线，代码不动，保留 TODO，仅回复。** 理由见下面两点；下面这段"如果要接"的改法保留作记录，本轮不采用：

```java
AbstractInputChannelRecoveredStateHandler stateHandler =
        AbstractInputChannelRecoveredStateHandler.create(...);
boolean readAny;
try (ChannelStateFilteringHandler ignored = filteringHandler; stateHandler) {
    readAny = read(...);
    readAny |= read(...);
    if (filteringHandler != null) {
        checkState(!filteringHandler.hasPartialData(), "...");
    }
}
// The handler is closed here: the spilling handler has sealed its last segment and built the
// container, so the produced state is only readable after the try block.
FetchedChannelState produced = stateHandler.getProducedChannelState();
if (produced != null) {
    return Optional.of(produced);
}
// In-memory backend: recovered buffers already live in the channels' queues, so the container
// is an empty placeholder that only signals "recovered data was pushed".
return filterContext.isCheckpointingDuringRecoveryEnabled() && readAny
        ? Optional.of(new FetchedChannelState(Collections.emptyList()))
        : Optional.empty();
```

- 关闭顺序不变（资源按声明逆序关闭：stateHandler 先于 filteringHandler），与现状一致。
- 行为今天完全不变：工厂 `AbstractInputChannelRecoveredStateHandler.create()` 目前只会返回 `NoSpillingHandler`/`FilteringHandler`，它们的 `getProducedChannelState()` 返回 `null`（基类默认实现），所以走的还是原来的 placeholder 分支；等工厂开始选 `Spilling*`，这里自动返回真实容器，不会静默丢状态。
- 那条 `TODO: FLINK-38544 — return stateHandler.getProducedChannelState() ...` 随之删除；#30 的 TODO（`getProducedChannelState()` 未接线）也一并去掉——接线后它不再是死代码。
- 遗留（不在本条范围）：`closeInternal()` 里的 `acquire()` 在**异常路径**上也会执行，此时容器不会被返回给任何人，grant 无人归还。这与 #25 是同一类生命周期问题，放进 #25 的修复一起看。

**为什么最终决定不接（两条，第二条是本质）**：

1. **机械障碍**：如上，那行 `return` 求值时容器还没构建，必然是 `null`，"填对值"这件事本身就不成立。
2. **接了也不对**：这个返回值今天只是**存在性标志**——`StreamTask:948` 用 `state.isPresent()` 决定 `requestPartitions(needsRecovery)`，`StreamTask:985-987` 用它决定要不要建 drainer，没人碰里面的 `files`。内存实现下数据由 handler 直接推进 channel 队列（`NoSpillingHandler.recover()` → `onRecoveredStateBuffer`），转换时再由 `RecoveredInputChannel.toInputChannelInRecovery()` 整体搬进物理 channel。如果现在把真实文件容器返回出去，它会被交给**只会 `release()`、不会读文件**的内存 drainer；而 handler 的 `close()` 里 `acquire()` 了唯一那个 grant，drainer 一 release 就 refCount 归零 → **当场删掉全部 spill 文件**。故障不会消失，只会从"这一行丢状态"挪到"下一步删文件"，且更难发现。
3. 真正的接线在 `40081/pr` 的 `adb15673247`（工厂翻转 + 删 v1 FilteringHandler + StreamTask 真正使用该返回值 + 删掉 `toInputChannelInRecovery` 的搬运循环），producer 与 consumer 必须同一次换。

**新回复草稿（最终版，两段）**：

> This value is only used as a presence flag today: if it's present, `StreamTask` requests partitions with `needsRecovery=true` and builds a drainer; if it's empty, it does neither. It never carries data — the in-memory handler pushes the recovered buffers straight into the channels' queues.
>
> The `Spilling*` handlers are introduced here but not selected by the factory yet, so `getProducedChannelState()` is always null at this point; it starts carrying real data in the next PR.

**新回复草稿**：Almost — the blocker is that `producedChannelState` is only built in the spilling handler's `close()` (it has to seal the last segment and the file stream first), so at that `return` — still inside the try-with-resources — it would always be `null`. I moved the handler out of the resource list and the return below the block, so it now returns `stateHandler.getProducedChannelState()` when there is one and falls back to the empty placeholder otherwise; the TODO is gone. Behaviour is unchanged today since the factory still only builds the in-memory handlers, which produce nothing.


人工第二轮回复：
这个我已经回复了。

**状态：✅ 已回复（2026-08-06，人工）**，代码不动，TODO 保留。本条在本批次里无待办。

---

## 32. Review 总评 — `recoverWithFiltering` catch 块双重 recycle（pre-existing）

链接：https://github.com/apache/flink/pull/28661#pullrequestreview（2026-07-31 总评）

> Thanks for the PR, I've left some comments PTAL.
>
> I believe after applying some suggestions the code can be greatly simplified:
> - r3693955957（#8 接口拆分）
> - r3693955989（#22 header 随 snapshot）
> - r3693955990（#23 BoundedSegmentStream）
> - r3693955992（#24 多 reader）
>
> One more thing found while reading, flagged here instead of inline since it isn't part of this diff (pre-existing from #28651, in `FilteringHandler.recoverWithFiltering`'s catch block): `onRecoveredStateBuffer` only throws after the buffer's ownership has already transferred into the channel's queue, so the catch loop's `for (int j = i; j < filteredBuffers.size(); j++)` recycles the buffer at index `i` while it's already live in that queue. Might be worth a fix there (starting at `j = i + 1`), separately from this PR.

**他建议**：`RecoveredChannelStateHandler.java:710` 的补偿循环从 `j = i + 1` 开始；单独出 fix（不属于本 PR 的 diff，是 #28651 引入的既有问题）。

**我的评价**：✅ 已核实，成立。看 `RecoveredInputChannel.onRecoveredStateBuffer`（`RecoveredInputChannel.java:180-206`）：buffer 在 `synchronized` 块内先入 `receivedBuffers`（同时置 `recycleBuffer = false`），之后唯一可能抛异常的点是 `notifyChannelNonEmpty()`——即**只要它抛出，index i 的 buffer 必然已被队列持有**。此时 catch 从 `j = i` 开始 recycle，就是对已入队 buffer 的双重 recycle → refCount 错乱/池损坏。他给的修法 `j = i + 1` 正确。同意单独开 ticket 修（注意确认 `RecoverableInputChannel` 的其他实现如 `LocalInputChannel.onRecoveredStateBuffer:228` 是否同样满足"抛出必在入队后"的性质）。

人工第二轮回复：
这个我单独处理，在当前 文档里标记下，就说外部单独处理。

**状态：⏏️ 外部单独处理（不在本 PR、也不在本批次）**——由你单独开 ticket 修 `j = i + 1` 并补测试。本文档不再跟踪它的进展，只保留上面的分析结论备查。

---

## 集中讨论：总评点名的四条"大幅简化"建议（#8 / #22 / #23 / #24）

Roman 总评原文：

> I believe after applying some suggestions the code can be greatly simplified:
> - r3693955957（→ 本文档 #8）
> - r3693955989（→ 本文档 #22）
> - r3693955990（→ 本文档 #23）
> - r3693955992（→ 本文档 #24）

这四条不是并列的，依赖关系是：**#22 是根，#23 完全跟随 #22，#8 在 #22 之后做才便宜，#24 只是提问（背后真正要修的是 #25 泄漏）**。牵连的其他评论：#13（currentBody 双引用，随 #22 消解）、#16（openCurrentFile 循环折叠，与 #22 改同一片代码，应一起做）、#12（currentFileSize，同一片代码）、#3（命名，受 #8 结论影响）、#5/#7（已改，为 #8 铺了路）、#25（泄漏，修法受 snapshot 形态影响）。

**建议的决策顺序：先定 #22 → #23/#16/#13 顺势 → 再定 #8 → #24 仅回复 → 最后按定稿的 snapshot 形态修 #25。**

### ~~#22 / #23 的方案讨论~~（已作废，2026-08-06）

这两节写于重构之前（当时还在讨论 `ResumeSegment` 小类、"null = 尚未 commit" 之类的编码），**已被实际实现取代**。以各自评论小节为准。

### #8 拆 drain / checkpoint 接口（r3693955957）——建议折中：commit 上移到 reader，不拆接口

> Could we enforce this contract by splitting this interface into separate drain-side and checkpoint-side interfaces, so `commit()` isn't even callable from the snapshot path? I believe this would simplify a lot of the code in this PR.

**直拆的麻烦**：`commit()` 挂在 `SpillSegment` 上。要让 snapshot 路径拿不到 commit，得让两侧的 `nextSegment()` 返回不同的 Segment 类型——Java 里 `Optional<CommittableSegment>` 不是 `Optional<SpillSegment>` 的子类型，要么接口带泛型参数（`Reader<S extends SpillSegment>`），要么两套 Segment 接口，复杂度不降反升。

**折中方案（我推荐）**：把 `commit()` 从 `SpillSegment` 挪到 reader 级——`FetchedChannelStateReader.commitDelivered()`（提交"当前段已读到的字节数"，反正 Segment 只在下一次 `nextSegment()` 前有效，"当前段"无歧义）。然后拆分变得廉价：`FetchedChannelStateReader`（nextSegment + close，checkpoint 侧用）和 `DrainableChannelStateReader extends FetchedChannelStateReader`（+ commitDelivered + snapshot，drain 侧用）。实现类仍是一个。这样 "only the drain reader commits" 从 javadoc 契约变成类型约束，Segment 接口保持干净，也不需要泛型。

**决策点**：a) 全盘拆（两套 Segment）/ 折中（commit 上移 + 两层 reader 接口）/ 不拆只回复？

**✅ 最终立场（2026-08-06 晚，#22 已落地后复核）：接口也不拆，实现更不拆。**

回复要按这个顺序展开，别一上来就讲我们的方案：

1. **结论前置**：他在评论里很肯定地写了 "I believe this would simplify a lot of the code"——第一句就要正面顶回去：按我的理解，这只会让代码量更大而不是更小；然后才承认接口确实更清晰。
2. **共用的部分**：跨文件滚动、位置记账、header 解析、限长 body、fail-loud EOF、close 与 grant 归还——几乎是整个类。
3. **各自独有的部分**：drain 侧只有 `commit()`/`snapshot()`，checkpoint 侧只有 body 中间恢复入口（drain reader 从 0 开始，永远走不到）。所以拆实现 = 基类扛几乎全部代码 + 两个极薄子类。
4. **接口拆分自己也不免费**：`commit()` 挂在 `SpillSegment` 上，要让 checkpoint 侧在类型上调不到它只有三条路——reader 加泛型、再拆一个 Segment 类型、或把 `commit()` 上移到 drain 侧 reader；每条都波及所有调用点，而实现仍是一个类。
5. **收尾**：倾向单接口 + 单实现，契约写 javadoc；并点出 #22 之后这个拆分要守护的契约性注释大多已经消失；把球交回给他。

### 第三轮（2026-08-07 20:11）——他退到"能不能共用实现"

> Why can't the classes share the common part of the implementation (via inheritance or encapsulation)?

**已于 21:47 回复**（人工）：可以，但成本不低，尤其 `commit()` 挂在 `SpillSegment` 上；提议作为 follow-up，愿意开 JIRA 自己接。等他表态；若他同意，需要开一个 FLINK ticket 记录"drain/checkpoint 侧接口拆分 + 实现共用"。

**回复草稿（五段，结论前置）**：

> My understanding is the opposite of the expectation here: this would add code rather than remove it. I do agree the interfaces themselves would read clearer — but that clarity comes with more types and more code, not less.
>
> Almost everything the two paths do is shared: file rolling, position tracking, header parsing, the bounded body view, fail-loud EOF, close and the lifecycle grant. That is essentially the whole class.
>
> What is unique to each side is a couple of members: `commit()`/`snapshot()` on the drain side, and the mid-body resume entry on the checkpoint side (the drain reader starts at offset 0 and never uses it). So splitting the implementation gives a base class carrying nearly all of the code plus two very thin subclasses.
>
> The interface split isn't free on its own either, because `commit()` lives on `SpillSegment`. Hiding it from the checkpoint side needs either a generic reader, or a second segment type, or moving `commit()` up onto the drain-side reader; each of those touches every call site while the implementation stays a single class.
>
> So I'd rather keep one interface and one implementation and state the contract in the javadoc (only the main reader commits) — especially since the resume point moving into the snapshot already removed most of the contractual comments the split would have enforced. Let me know if you see it differently.

---

**（历史）已定（2026-08-06）：不拆，只回复。** 理由：折中方案下实现类完全不变、调用方代码没有任何简化（drain 侧只是 `segment.commit()` 换成 `reader.commitDelivered()`），唯一收益是 checkpoint 侧静态类型更干净——它消除的是误用可能，删不掉任何代码。真正删代码的是 #22；#22 落地后，这个拆分要守护的契约性注释大多自然消失。回复里说明分析、关联 #22、把球交回给他。

**回复草稿**：

> I prototyped this: since `commit()` sits on `SpillSegment`, a clean split first requires moving it up onto the drain-side reader (a segment is only valid until the next `nextSegment()`, so "the current segment" is unambiguous). It would look like:
>
> ```java
> // checkpoint side: read-only
> public interface FetchedChannelStateReader extends Closeable {
>     Optional<SpillSegment> nextSegment(); // SpillSegment: channelInfo/bodyStream/length, no commit
> }
>
> // drain side
> public interface DrainingChannelStateReader extends FetchedChannelStateReader {
>     void commitDelivered(); // was SpillSegment#commit
>     FetchedChannelStateSnapshot snapshot();
> }
> ```
>
> However, the split itself doesn't reduce the amount of code at all: the implementation stays a single class, every call site keeps the same number of lines (`segment.commit()` merely becomes `reader.commitDelivered()`), and we end up with one more interface than before — the only gain is that the checkpoint path can no longer call `commit()`/`snapshot()` by construction. I'm happy to do this refactoring, but given that it doesn't actually simplify anything — WDYT?
>
> What does bring real simplification is your suggestion in https://github.com/apache/flink/pull/28661#discussion_r3693955989: once the snapshot carries the resume segment, most of the contractual comments this split would enforce disappear anyway.

### ~~#24 的方案讨论~~（已作废，2026-08-06）

见 #24 评论小节（回复要排在 #25 之后发）。

### ~~#22 重构工作包~~ / ~~第二轮追加批次~~（已作废，2026-08-06）

这两节里的方案与回复语都是 #22 重构**之前**写的，现在已被推翻或已执行完毕，**不要再参考**：

- 已执行：#22、#23、#4、#5、#6、#10、#17、#21、#27、#28、#31（详见各自小节与「批次划分」一节）。
- 已改口径：#25（不再按"必修"排优先级，close 采用"转移"语义）、#3（改为返回 snapshot，不改名）、#13（保留 `currentBody` 的理由变了）、#20（不再是"保留手写循环"，改为可定位通道）。

**唯一有效的处理方案与回复语，以各评论小节 + 顶部「批次划分」为准。**

### ~~待你拍板的清单~~（已作废，2026-08-06）

表里的议题要么已定、要么已执行；当前待办与归属看顶部「批次划分」，每条的方案与回复语看各评论小节。

## 当前总体判断（2026-08-06 晚，替换早前版本）

**已完成并回复（19 条）**：#1、#2、#4、#5、#6、#7、#9、#10、#11、#14、#17、#18、#19、#21、#27、#28、#29、#30、#31 —— 其中 18 条他已 resolve。

**代码已改、回复已挂 pending（2 条）**：#22、#23。

**待办（批次 A/B，7 条）**：#25 + #3（生命周期语义 → 返回类型，牵动 `40081/pr`）、#24（回复排在 #25 之后）；#16 + #20（reader 内部清理）、#12 + #15（仅回复）。

**待讨论（1 条）**：#13——方向是"把段身份放到交出去的对象上"，等定。

**未排期（2 条）**：#8（结构性拆分，需 offline）、#26（写侧 force-seal，独立）。

**外部处理（1 条）**：#32（双重 recycle，另开票）。
