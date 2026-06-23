# Background for investigating the recovery dataloss bug

> 本文件是排查 `UnalignedCheckpointRescaleITCase` 偶发丢数据 bug 的**背景知识基线**。
> 目的:给后续排查 agent 一个不跑偏的地基——记录**已确认的架构事实**、**已被坐实/证伪的结论**、
> 以及**必须作废的怀疑方向**。任何排查必须在此基线之上,禁止重新捡起已作废的方向。
>
> ⚠️ 本文件只记录**有代码证据或用户确认的事实** + **明确标注为"怀疑、尚无证据"的待查方向**。
> 禁止把猜测写成结论。

## 0. 失败现象(硬证据)

- 失败测试:`UnalignedCheckpointRescaleITCase.shouldRescaleUnalignedCheckpoint`
- 失败参数:`[downscale keyed_broadcast from 7 to 2, sourceSleepMs = 0]`(downscale 7→2,keyed_broadcast 模式)
- 断言:`checkCounters` 的 `NUM_OUTPUTS == NUM_INPUTS` 失败,**expected 565158,but was 536226**
- **方向:output 比 input 少 28932 条 → 确凿丢数据(不是重复)**
- 概率:偶发,< 千分之一
- 剥离出的干净日志片段(单 case,不含其它测试):
  - 原始:`requirements/38544/fix_rounds/rescale_dataloss_failed_case.log`(11217 行)
  - 精简:`requirements/38544/fix_rounds/rescale_dataloss_failed_case.key.log`(4293 行)
  - 对应原日志 `log/20260623_081547.log` 第 61756~72972 行

## 1. 用户确认的关键架构事实(地基,不可违背)

### 1.1 每次快照必须自包含完整(增量快照语义)

recovery 期间触发的每个 checkpoint,其快照**必须是一份自包含的完整数据**。

- 第 N 次快照与第 N+1 次快照**允许重复**(第 N+1 次可能重新覆盖第 N 次的部分数据)。
- 第 N+1 次快照完成后,第 N 次快照即被丢弃、不再需要。
- **推论:数据"重复"是设计本身,不是 bug。真正的 bug 形态是"某次快照不完整"——漏了某段数据,导致最终丢失。**

→ **任何把"重复捕获 / over-capture"当作 bug 的分析方向一律作废**(见 §3)。

### 1.2 rescale 在 drain 之前已被消化为 non-rescale

发生 rescale(如 downscale 7→2)时,链路是**严格串行的三阶段**:`filter → conversion → drain`。

- **filter/conversion 阶段**:把按**旧 parallelism**组织的 channel state,过滤、重组成按**新 parallelism**组织的数据。rescale 的 channel 重映射(virtual channel 映射、按新并行度归并)**全部发生在这一阶段**。
  - 入口:`ChannelStateFilteringHandler`(`flink-runtime/.../checkpoint/channel/ChannelStateFilteringHandler.java`),`createFromContext` 按 `rescalingDescriptor` 建 per-gate filter handler(:75-100),`filterAndRewrite` 做去重/重写(:109-132)。
- **drain 阶段**:drainer 看到的数据**已经是按新并行度重组好的、non-rescale 的数据**。drain/checkpoint 这一层**不再有 rescale 语义**,也不存在并发进行的 channel 重映射。

→ **推论:downscale 7→2 这个参数,到了 drain + checkpoint 层,与普通 non-rescale recovery 没有本质区别。rescale 的作用只是放大数据量、让并发概率窗口更容易命中,bug 本身大概率在 drain/checkpoint 的通用逻辑里。**

→ **任何把根因归到"drain/checkpoint 阶段的 channel 重映射、broadcast channel 漏接、rescale 时序缝隙"的方向一律作废**(见 §3)。

## 2. 已被两轮排查坐实的事实(两个独立 agent 互相印证,可信)

以下结论由两个独立 flink-code-analyzer agent 各自精读代码得出、且互相一致,可作为已排除项:

- **A 证伪**(锁外读 body vs commit 撕裂):drain 的 body 读与 `seg.commit()` 由**同一条 drain 线程**顺序执行,且 `onRecoveredStateBuffer + commit` 在同一 `synchronized(lock)` 块内(`FetchedChannelStateDrainer.java:131-134, 144-147`)。Step3 的 snapshot reader 是独立实例、持隔离的 Position 副本(`FetchedChannelStateReaderImpl.java:93-94`),且 snapshot reader 从不 commit(只有 root/drain reader commit)。无跨线程读/commit 同一游标。
- **B 证伪**(partial tail / buffer 正好填满边界丢字节):`commit()` 记录 `deliveredFromSegmentHead()=alreadyDelivered+read`,与 buffer 边界无关;`while(fill(...)>0)` 循环对短读继续填同一 buffer。字节区间无缝。其中一个 agent 反编译 Netty `writeBytes` 确认短读语义。
- **C 证伪**(firstSegment skip 算术,含跨文件 rollToNextFile):`deliveredPrefix == bufferLength` 与 `< bufferLength` 两分支算术自洽,`copyAsDelivered`(`:403-407`)与 `deliveredBodyBytes`(`:365-367`)对称,无 double-count。
- **E 证伪**(锁顺序 / 死锁):`snapshotAndInsertBarriers` 与 drain 投递锁序一致(`drainer.lock → channel monitor`),无逆序无死锁。

## 3. 必须作废的怀疑方向(基于 §1 的两条架构事实)

以下方向在用户的两条架构事实下**已不成立**,禁止后续排查重新捡起:

1. ❌ **"多 cp 重复捕获 / over-capture 是 bug"**(原 Agent 2 的首要发现)——作废。
   理由:违背 §1.1。重复是设计本身;且现象是 output **少** 28932,重复只会让 output ≥ input,方向相反。
2. ❌ **"keyed_broadcast / broadcast channel 在 rescale 下被某 subtask 漏接"**——作废。
   理由:违背 §1.2。broadcast 重映射在 filter/conversion 阶段完成,drain 时已无 rescale 语义。
3. ❌ **"downscale 时 barrier 插入与 rescale channel 重映射存在时序缝隙"**——作废。
   理由:违背 §1.2。drain/checkpoint 阶段没有并发进行的 channel 重映射。
4. ⚠️ **"channel 抢先翻 inRecovery=false 导致漏插 barrier"**(原 Agent 1 根因 #1)——**可疑度大幅下降,降级为"待日志验证",不作为预设结论**。
   理由:其论证里"rescale 让 in-recovery channel 更多更易触发"的基础被 §1.2 削弱;且在 §1.1 增量快照语义下,channel 提前退出 recovery 本身不必然丢数据。**仅当日志能证实某 subtask 的某段已消费数据既不在内存侧也不在磁盘侧时,才重新激活此方向。**

## 4. 当前唯一正确的排查姿势

**先让证据说话,再回代码定位。禁止带着预设怀疑去代码里"找证据支撑"。**

下一步应做的纯事实提取(只读 §0 的日志片段,不读代码、不带预设),回答:

1. 丢的 28932 条具体是哪个 subtask 输出缺失?(downscale 后只有 2 个 subtask)
2. 失败前最后几次 checkpoint / recovery 完成的时序是什么?
3. 日志里有无 recovery / channel state / barrier / spill 相关的 Exception 或可疑 WARN?

拿到这三个事实后,再决定代码往哪查。

## 5. 待查方向(全部标注为"怀疑,尚无证据")

⚠️ 以下均为**怀疑,无代码或日志证据**,列出仅供拿到 §4 事实后对照,**禁止当作结论**:

- 怀疑:`committed` 游标在两次 snapshot 之间的推进,与某次 snapshot 拍下的边界之间漏了一个区间(某段数据第 N 次没覆盖、第 N+1 次起点已跳过它)。需日志里的 checkpoint 序列 + committed 推进证实。
- 怀疑(降级后的原 Agent 1 根因 #1):某 channel 在 barrier 那次 poll 之前已在更早一次 poll 翻 `inRecovery=false`,漏插 barrier,`checkpointStarted` 走普通分支静默丢已消费的 recovered 数据。需日志证实某 subtask 确有此现象。

这两条在未被 §4 的事实激活前,不得作为修复依据。
