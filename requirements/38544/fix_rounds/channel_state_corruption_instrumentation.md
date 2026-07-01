# Channel-State 数据破坏排查：插桩、编译、复现流程（当前分支版）

> 分支：`debug/cdr-without-spilling-data-loss-repro-FLINK-40016-based-on-original-v1-debug`
> 现象：`UnalignedCheckpointRescaleITCase#shouldRescaleUnalignedCheckpoint` 在 rescale 恢复路径上偶发
> `java.io.IOException: Corrupt stream, found tag: -NN`（channel-state / unaligned-checkpoint 在途数据恢复阶段）。
>
> **本文档定位**：这是给**一个全新的协调者 agent** 的**交接文档**。它接手后应能不依赖任何历史对话、独立照本文档
> 跑起排查循环。文档提供：**执行主干（工作流程）**、**核心思路（方法论）**、**当前分支的插桩点**、**编译与复现命令**、**读日志定位法**。
> 插桩为临时诊断代码，定位根因后必须全部删除。
>
> ⚠️ **重要（防干扰）**：本文档由另一个分支/另一套 codebase 的排查记录改写而来。**那份记录里的"根因结论"已被彻底删除**，
> 不作任何保留——当前分支的 channel-state 恢复路径与之不同（见 §2 的差异说明），任何外部预设根因都可能误导。
> 本文档只保留可复用的**方法论**和**已按当前分支真实代码核对过的插桩点**（有对等就加、无对等就不加）。
> **根因必须由新 agent 用当前分支自己的复现日志、从零得出**，不许照搬。

---

## 工作流程（给协调者 agent 的执行主干）

**你是协调者。你自己不写代码、不跑 mvn、不读大日志正文；所有具体动作都委托给 sub-agent。** 目标：在当前分支上定位
`Corrupt stream` 数据破坏的**真正根因**（当前分支自己的证据，不许照搬任何外部结论）。

一轮 = 下面 5 步，**循环执行直到定位根因**（参考上限 5 轮，每轮只回答一个问题、逐步缩小范围）：

1. **决定本轮要回答的问题 + 插桩方案**（协调者决策）。第一轮：按 §2.1 加全链路粗插桩，看每个阶段 buffer 的 record framing（stride/header）是否健康，把 bug 框到"某个未插桩的 gap"。之后每轮只针对上一轮暴露的 gap 加更细的插桩。
2. **委托 sub-agent 加日志（改生产代码）**。sub-agent 在 `flink-runtime` 生产代码里加 §2.1 的插桩（新建/扩展 `ChannelStateInvariant`，在对应类方法插调用点），**然后按 §3 编译**，编译通过后**把这轮插桩代码 commit**（commit message 说明本轮加了哪些插桩、要回答什么问题）。⚠️ 插桩是临时诊断代码，但每轮都要 commit，保证"改动可追溯、可回退、下一轮基于干净状态继续"。
3. **委托 sub-agent 跑复现**（§4 的 `repro.sh`，多 worker、后台跑，首次命中即停）。命中后按 §8 规则 1 **立即备份 `repro/results/`**。
4. **委托独立 verification sub-agent 分析命中的 FAIL 日志**（§5 的读法）。它必须判定 **CONCLUSIVE / INSUFFICIENT**：只有日志里出现 **healthy→corrupt 的那一步转变**才算 CONCLUSIVE；否则判 INSUFFICIENT，并给出"下一轮要在哪里加什么插桩"的精确清单。**把本轮 findings 摘要写进 `requirements/38544/fix_rounds/round<N>_findings.md` 并 commit**（大日志被 gitignore，只提交结论与关键行摘录）。
5. **收敛判断**：CONCLUSIVE → 进入根因确认 + 设计修复（修复本身另起流程，不在本文档范围）；INSUFFICIENT → 回到第 1 步，按清单开下一轮。

**分层纪律**：协调者只调度、只读 sub-agent 返回的结论；改代码/编译/跑复现/读大日志一律 sub-agent 做。加日志的 sub-agent 与分析日志的 verification sub-agent **必须是不同的、独立的 sub-agent**（不许自己验证自己）。

**心态**：不要猜、不要信任何预设根因（包括本文档任何"方向假设"——已全部移除）。只让当前分支的字节和日志说话（§1）。

---

## 0. 现象与已知事实

- 报错：`Caused by: java.io.IOException: Corrupt stream, found tag: -NN`，抛在 `StreamElementSerializer.deserialize` 读 tag 时（tag 合法值仅 0~6，读到负数=字节流错位）。
- 最近一次实测命中（`repro/results/FAIL_w1_2.log`）：`found tag: -22`（`0xEA` = header 第 3 字节）。
- 复现率：当前分支实测约 **~5%/run**（29 pass : 1 fail 那一轮）。仍需 CPU 争用；用 `repro.sh` 多 worker 跑几十~上百次内可稳定命中。
- 失败一致发生在**从 rescale savepoint / checkpoint 恢复的那段 job**（测试三段 job 的最后一段），与在途数据恢复阶段有关。

### 测试数据的确定性特征（排查的支点）

`UnalignedCheckpointTestBase`：`HEADER = 0xABCDEAFC << 32`。每条 record 的 value 是 8 字节 long：

```
AB CD EA FC | VV VV VV VV     高4字节固定 header，低4字节递增 value
```

channel-state 里按 `[4字节 length][record 序列化字节]` 逐条框定。**只要在任一阶段把字节剥到 record 层，就能用
`AB CD EA FC` 这个已知 header 验证数据有没有坏。**

### 本次失败 tag 的字节解码（硬特征线索）

| tag | byte | 是否 header 字节 |
|---|---|---|
| -22 | `0xEA` | ✅ header 第3字节 |
| -4 | `0xFC` | ✅ header 第4字节 |

`0xEA/0xFC` 命中 header → 与"读窗落进 header 区"的字节平移一致；平移量是否恒定需靠插桩在复现瞬间抽出硬特征。
**不要照搬其它分支的错位结论**，当前分支必须自己抽。

---

## 1. 核心排查思路（不要猜，让字节自己说话）—— 方法论，可复用

1. **不做不透明的 CRC 校验**——CRC 只能回答"变没变"，回答不了"错位几字节、从哪开始"。
2. **在每个阶段按该阶段的格式把数据剥到 record 层**，验证已知不变式：每条 record 仍是 `[4B len][... AB CD EA FC ...]`、
   且多条 record 的 **header 间隔（stride）恒定**。stride 第一次变化的地方，就是字节流第一次错位的地方。
3. **找"最早在哪个阶段破"**——上游最靠前那个"输入还好、输出已坏"的环节就是责任组件。在流水线上二分定位，
   把"下游报错的症状"变成"上游出错的位置"。
4. **能 fail-fast 断言就断言**（自动停在最早破点，带本地上下文），不能断言再退化为带 `channelInfo` 的结构化日志。
5. **抽硬特征**：corruption 瞬间，dump 出错 buffer 的 header 分布 + hex，看 stride 在哪一条 record 断、错位几字节、
   是否恒定。恒定 ±N → 查"哪一步多/少 N 字节"；不定 → 查 position/offset 大段算错。

### 数据流阶段（按测试三段 job 的时间顺序）

```
Job1                 : 仅内存在途数据 → checkpoint 文件
Job2 (从上一 chk 恢复) : readChunk → filter 重写 → 重新注入 channel        （边恢复边处理）
       (产生新 chk)   : write() 内存在途 + 上游 output buffer  → checkpoint 文件
Job3 (从该 chk 恢复)  : readChunk → filter 重写 → ★抛 Corrupt stream★
```

> 第一份**同时含内存在途数据 + 被重新当作 input 的上游 output buffer**的 channel-state，最可能在接缝/字节账处出问题，
> 与复现规律吻合。**顺序本身不一定是嫌疑**，要查的是接缝处字节是否严丝合缝、各步字节账是否守恒。

---

## 2. 插桩点（临时，定位后删除）—— 已按当前分支真实代码核对

辅助类（需新建）：`flink-runtime/src/main/java/org/apache/flink/runtime/checkpoint/channel/ChannelStateInvariant.java`

- 开关：`-Dflink.cs.debug`（默认 ON，`=false` 关闭逐阶段日志）；`-Dflink.cs.debug.records=true`（默认 OFF，逐条 record 日志，极吵）。
- 核心方法 `shape(bytes)`：扫出所有 `AB CD EA FC` 偏移，输出 `headers=N firstHeaderAt=.. strides=[..]`，stride 不齐打 `*** STRIDE-IRREGULAR ***`，无 header 打 `NO-HEADER`。
- 日志标签：`[CS-INV]` 逐阶段形状；`[CS-INV-REC]` 逐条 record；`[CS-INV-CORRUPT]` 抛错瞬间永远打（含 `recordsOkInThisBuffer` + shape + hex + 栈）；`[CS-INV-ASSERT]` 字节账断言失败。

### 2.1 当前分支**有对等位置**的插桩点（加）

| # | 阶段 | 文件 | 方法 / 行（当前分支） | 抓什么 |
|---|---|---|---|---|
| 1 | filter 输入 | `ChannelStateFilteringHandler.java` | `GateFilterHandler.filterAndRewrite`（`:294`，喂 `vc.getNextRecord` 之前，`:320`）| 反序列化前的原始 buffer 形状 `filter.IN` |
| 1c| filter 崩溃 | 同上 | loop 外 `catch (Throwable t)`（`:347`）| 崩之前成功几条 + 该 buffer header 分布 + hex `[CS-INV-CORRUPT]` |
| 2 | filter 输出 | 同上 | `serializeElement`（`:373`）| 重写出的每条 record 字节 `filter.OUT`（逐条，验 header） |
| 3 | 读 chunk | `SequentialChannelStateReaderImpl.java` | 内部类 `ChannelStateChunkReader.readChunk`（`:211`）| 从 checkpoint 文件读出的 chunk 原始字节 `readChunk.IN@off<偏移>` |
| 3b| output 复用为 input | 同上 | `readInputData` 的**第二个** `read(...getUpstreamOutputBufferState)`（`:83-87`）| 标记"这段 read 是上游 output 被当作 input 恢复"的身份，供交叉核对 |
| 4 | 写-内存(input) | `ChannelStateCheckpointWriter.java` | `writeInput`（`:145`）/ 底层 `write`（`:183`）| 写进 checkpoint 的 input 在途 buffer `ckptWrite.IN@off<偏移>` |
| 5 | 写-output | 同上 | `writeOutput`（`:164`）/ 底层 `write`（`:183`）| 写进 checkpoint 的 output buffer `ckptWrite.OUT@off<偏移>` |
| 6 | filter 重写后注入 | `RecoveredChannelStateHandler.java` | `InputChannelRecoveredStateHandler.recoverWithFiltering`（`:131`，注入点 `channel.onRecoveredStateBuffer` `:149`）| 重写后即将注入 channel 的每个 buffer 形状 `recover.INJECT` |

> 所有 `[CS-INV*]` 日志都带 `ch=`（`InputChannelInfo` / `SubtaskConnectionDescriptor`），便于按出错 channel 过滤。

### 2.2 当前分支**无对等位置**的插桩点（不加，与文档来源分支的差异）

来源文档里以下三个点，当前分支**没有对应代码**，故不加：

- ~~spill 封段 `RecoveredChannelStateHandler.sealCurrentSegment`~~ —— 当前分支 `RecoveredChannelStateHandler` **没有** `sealCurrentSegment`；
  filter 直接在 `recoverWithFiltering` 里做、结果 `onRecoveredStateBuffer` 注入，无独立 spill 封段步骤。
- ~~spill 读头 `FetchedChannelStateReaderImpl.readHeaderAtCurrent`~~ —— 当前分支**没有** `FetchedChannelStateReaderImpl` 这个类，也无独立的 spill-fetch 段头读取。
- ~~写-磁盘 `ChannelStateCheckpointWriter.writeInputFromSpill`~~ —— 当前分支 `ChannelStateCheckpointWriter` 只有 `writeInput`/`writeOutput`/`write`，**无** `writeInputFromSpill`。

> 结论：当前分支的 channel-state 恢复**没有独立 spill 段读回/封段/段头**这条子路径。核心接缝落在
> `readInputData` 里 input-channel-state（第一个 `read`）与 output-as-input（第二个 `read`，`:83-87`）**共用同一批
> recovered channel 的 filter/反序列化器**这个点上——这正是需要重点插桩交叉核对的地方（#3b + #1 + #6）。

---

## 3. 编译流程

只改 `flink-runtime` 生产代码，重新 build 并安装到本地 `~/.m2`：

```bash
cd flink-runtime
../mvnw -T 20 clean install -U -Pfast -DskipTests \
  -Dmaven.javadoc.skip=true -Drat.skip=true -Dcheckstyle.skip=true -Denforcer.skip=true \
  -P java11-target -P java11
```

> 约 5 分钟以上。`repro.sh` 假设生产模块已 install 到 `~/.m2`，它只重编 `flink-tests` 的 test-classes。
> 若编译报错且是插桩导致，先修插桩再重编。

---

## 4. 复现流程

```bash
bash repro/repro.sh 25 2000 600
#                    │   │    └─ 每次 run 超时(秒)
#                    │   └────── 目标总 run 数
#                    └────────── 并发 worker 数
```

`repro.sh` 行为：
1. `narrow.py` 把测试收窄到单个失败参数（结束自动 `git checkout` 还原）。
2. 在线 prime 跑一次下载 surefire 依赖（`repro/results/prime.log`）。
3. N 个 worker 并发离线 loop 跑 `UnalignedCheckpointRescaleITCase#shouldRescaleUnalignedCheckpoint`，**首次失败即停**。
4. 失败判定（`classify`）：日志含 `Caused by: java.io.IOException: Corrupt stream` 或 `Stream corrupted. Cannot find the header`
   或数据丢失断言（`NUM_OUTPUTS = NUM_INPUTS` + `expected:`/`but was:`）→ 记为 FAIL，拷到 `repro/results/FAIL_w<id>_<n>.log`。

产物：`repro/results/FAIL_w*_*.log`（失败现场；加了插桩后会含 `[CS-INV*]` 日志）。

> 当前分支实测约 **~5%/run**。命中后 STOP 文件会让所有 worker 停下。

---

## 5. 命中后如何读日志定位

1. **先看 `[CS-INV-CORRUPT]`**（在 `FAIL_*.log` 里 grep）：
   - 拿到出错 `ch=`、`recordsOkInThisBuffer`、该 buffer 的 shape + hex。
   - `recordsOkInThisBuffer=0` → 在 **buffer 开头**就错位（接缝/边界问题，上一段末尾的 spanning 残留接不上）。
   - `>0` → 在 **buffer 中间**某条 record 的 length 前缀坏了。
2. **按该 `ch=` 过滤所有 `[CS-INV]`**，按时间顺序重建该 channel 的阶段链：
   `ckptWrite.IN/OUT`(写侧) → `readChunk.IN`(读侧) → `filter.IN`/`recover.INJECT`(重写注入)。
   找**第一条**出现 `STRIDE-IRREGULAR` 或 `NO-HEADER` 的阶段——那就是字节第一次变坏的环节。
3. **input vs output-as-input 交叉核对**：重点看 #3b 标记的第二个 `read`（output 被当作 input 恢复）与第一个 `read`（真正的 input-channel-state）
   是否落到**同一个 recovered channel / 同一个反序列化器**，以及接缝处 A 尾 + B 头是否真能组成一条合法 record。
4. **抽硬特征**：把 `[CS-INV-CORRUPT]` hex 里 `AB CD EA FC` 的位置与"读 tag 的位置"比，算错位字节数；多复现几次看是否恒定。
   - 恒定 ±N → 查"哪一步多/少 N 字节"。
   - 不定 → 查 position/offset 大段算错。

> 三段 job 的日志混在同一文件里，`[CS-INV*]` 不带 job id：写侧(`ckptWrite.*`)在较早 job，读侧(`readChunk.IN`/`filter.IN`)在较后 job，按出现顺序区分。

---

## 6. 注意事项

- 插桩为**临时诊断代码**，根因定位后必须删除 `ChannelStateInvariant` 及全部调用点。
- 默认 `flink.cs.debug=ON`、`records=OFF`；若日志过大可临时关 records。
- **回归测试盲区**：现有 spill/channel-state 相关单测多用短 payload、只解码前几字节，从不逐字节比对 → ±N 错位漏检。
  定位根因后补的回归测试必须**逐字节比对** record 内容。
- 小心插桩改变数据路径（Heisenbug 风险）：任何把 lazy 读改成 eager 读之类的插桩，必须显式验证"改完还能复现"再信结论。

---

## 7. 排查过程经验（逐轮收敛，可复用的方法论）

1. **不要猜，让字节自己说话**。利用测试数据的确定性特征（`AB CD EA FC` header + 恒定 stride），在每个阶段把字节剥到 record 层验证不变式，找"最早变坏的阶段"。先用 `shape()` 的 stride/firstHeaderAt 把范围从"整条流水线"收敛到"某个未插桩的 gap"。

2. **分轮收敛，每轮只回答一个问题**。不要一次把所有日志加满：先全链路粗插桩定位到"未插桩的 gap"，再补齐 gap 抓硬特征，再加 handle/文件身份 + offsets + 跨 input/output 的 key 断言坐实。

3. **始终用"独立验证 agent"判定 CONCLUSIVE/INSUFFICIENT**，且标准要严：**日志里必须出现 healthy→corrupt 的那一步转变**，只靠"代码看着不对"不算。诚实判 INSUFFICIENT 并给出"下一轮要加什么"的精确清单，才能让收敛单调。

4. **抓硬特征能快速排除一大批假设**。`recordsOkInThisBuffer=0` + 全程 stride 恒定 → 立刻排除"流中间多/少 1 字节""单条 record length 写错"，直接指向"buffer 起点未对齐 + 无 spanning 前驱"。

5. **插桩可以是观察式，也可以是 fail-fast 断言**。断言"未触发"本身也是关键证据（如证明写侧 key 路由干净）。**断言不触发和触发一样有信息量**。

6. **小心插桩改变数据路径（Heisenbug 风险）**。任何改变读写时机的插桩都要显式验证"改完还能复现"，结论才可信。

7. **复现编排经验**：loop 跑在后台 + monitor 只在真实 trigger（FAIL / `[CS-INV-ASSERT]` / loop 结束）唤醒，不要按 pass 计数发心跳。注意 `repro.sh` 把 assert 触发的失败可能归类为 INFRA 并删日志——要单独监控并在命中时立即拷贝 + `touch STOP`。

8. **先验证判据本身成不成立，再拿它下结论**。例如 record 计数器在单个 channel 内可能本就非单调（keyBy/rebalance 打散），不能用作连续性判据；只有 framing 接缝（partial/header）才是有效信号。

---

## 8. 复现纪律 & 兜底检查（规则）

**规则 1 — 每次复现命中后，必须备份整个运行目录（尤其 FAIL 日志）。**
`repro/repro.sh` 每次启动会 `rm -rf "$RES"`（即 `repro/results/`），**下一次复现会覆盖上一次的现场**。所以每命中一次，就把整个 `repro/results/` 备份到 `repro/` 下一个持久目录：

```bash
# 命中后、开始下一轮复现前：
cp -a repro/results "repro/results-$(date +%Y%m%d_%H%M%S)"   # 或 results-<递增编号>
```

至少要保住 `FAIL_w*_*.log`。`*.log` 被 gitignore，故大日志留在工作目录、结论与关键行摘录进 `round*_findings.md` 提交。

**规则 2 — 拼接层按"前置规则"校验所有物理数据（不是查有没有数据）。**
在拼接 recovered buffer 的那一层（`ChannelStateFilteringHandler` / 每通道的 spanning 反序列化器），除了已有的事后 `hasPartialData()` 断言，
应对**所有恢复出来的物理数据**做 record-framing 前置校验：每条 record 是否符合既定 framing（长度前缀合法、tag 合法、header 按 stride 出现），
以及拼接边界处 A 的尾残 + B 的开头是否真能组成一条合法 record。校验失败即**当场显式失败**（loud early-fail），把静默错位/丢数据变成可定位报错。
注意：**校验是"早暴露"手段，不是根因修复**。（`ChannelStateInvariant.shape()` 就是这种校验的诊断版，可作为正式前置断言的蓝本。）
