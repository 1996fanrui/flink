# Channel-State 数据破坏排查：插桩、编译、复现流程

> 针对 `UnalignedCheckpointRescaleITCase.shouldRescaleUnalignedCheckpoint` 在 rescale 路径上偶发的
> `java.io.IOException: Corrupt stream, found tag: -NN`（channel-state / unaligned-checkpoint 在途数据恢复阶段）。
> 本文档记录排查的**核心思路**、**已加的插桩点**、**编译与复现命令**、**如何读日志定位**。
> 尚未定论；插桩为临时诊断代码，定位根因后必须全部删除。

---

## 0. 现象与已知事实

- 报错：`Caused by: java.io.IOException: Corrupt stream, found tag: -NN`，抛在 `StreamElementSerializer.deserialize` 读 tag 时（tag 合法值仅 0~6，读到负数=字节流错位）。
- 调用链（read 时）：
  `SequentialChannelStateReaderImpl.readChunk` → `SpillingWithFilteringHandler.recover` →
  `ChannelStateFilteringHandler.GateFilterHandler.filterAndRewrite` → `VirtualChannel.getNextRecord` →
  `SpillingAdaptiveSpanningRecordDeserializer`（抛错）。
- 失败一致发生在**最后一个 job**（第 3 次提交、从 savepoint 7 / chk-7 恢复的 rescale job）。前两个 job 分别是
  原始执行（chk-5）和带 savepoint 的停止（chk-7），与 corruption 无关。
- 复现率约 **0.1%~0.3%/run**，需 CPU 争用 + 上千次运行。

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
| -104/-78/-14/-106 | `0x98/0xB2/0xF2/0x96` | ❌ |

`0xEA/0xFC` 命中 header → 与"读窗落进 header 区"的字节平移一致；但另外几个不是 header 字节 → **平移量可能不固定**，
必须靠插桩在复现瞬间抽出我们自己的硬特征，不能照搬其它分支结论。

---

## 1. 核心排查思路（不要猜，让字节自己说话）

1. **不做不透明的 CRC 校验**——CRC 只能回答"变没变"，回答不了"错位几字节、从哪开始"。
2. **在每个阶段按该阶段的格式把数据剥到 record 层**，验证已知不变式：每条 record 仍是 `[4B len][... AB CD EA FC ...]`、
   且多条 record 的 **header 间隔（stride）恒定**。stride 第一次变化的地方，就是字节流第一次错位的地方。
3. **找"最早在哪个阶段破"**——上游最靠前那个"输入还好、输出已坏"的环节就是责任组件。本质是在流水线上二分定位，
   把"下游报错的症状"变成"上游出错的位置"。
4. **能 fail-fast 断言就断言**（自动停在最早破点，带本地上下文），不能断言再退化为带 `channelInfo` 的结构化日志。
5. **抽硬特征**：corruption 瞬间，dump 出错 buffer 的 header 分布 + hex，看 stride 在哪一条 record 断、错位几字节、
   是否恒定。恒定 ±N → 查"哪一步多/少 N 字节"；不定 → 查 position/offset 大段算错。

### 数据流阶段（按测试三段 job 的时间顺序）

```
Job1 (chk-5)         : 仅内存在途数据 → checkpoint 文件         （无 spill）
Job2 (恢复 chk-5)     : readChunk → filter 重写 → spill          （边恢复边 drain）
       (产生 chk-7)   : 内存 write() + 未 drain 完的 spill writeInputFromSpill()  → checkpoint 文件
Job3 (恢复 chk-7)     : readChunk → filter 重写 → ★抛 Corrupt stream★
```

> chk-7 是第一份**同时含内存(MEM)+磁盘(SPILL)两种来源**的 channel-state，所以接缝/字节账问题只在它身上出现，
> 与复现规律吻合。注意：MEM = 已从磁盘加载进内存的 recovered buffer（更老，应在前）；SPILL = 仍在磁盘的
> 未加载部分（更后）。代码顺序（先 `checkpointStarted` 后 `addInputDataFromSpill`）= MEM 在前、SPILL 在后，
> 与"内存在前"一致——**顺序本身不是嫌疑**，要查的是接缝处字节是否严丝合缝、各步字节账是否守恒。

---

## 2. 已加的插桩（临时，定位后删除）

辅助类：`flink-runtime/src/main/java/org/apache/flink/runtime/checkpoint/channel/ChannelStateInvariant.java`

- 开关：`-Dflink.cs.debug`（**默认 ON**，`=false` 关闭逐阶段日志）；`-Dflink.cs.debug.records=true`（默认 OFF，逐条 record 日志，极吵）。
- 核心方法 `shape(bytes)`：扫出所有 `AB CD EA FC` 偏移，输出 `headers=N firstHeaderAt=.. strides=[..]`，stride 不齐打 `*** STRIDE-IRREGULAR ***`，无 header 打 `NO-HEADER`。
- 日志标签：
  - `[CS-INV]`：逐阶段 buffer 的 header 形状（受 `ON` 控制）。
  - `[CS-INV-REC]`：逐条 record（受 `RECORDS` 控制）。
  - `[CS-INV-CORRUPT]`：**抛错瞬间永远打**——含 `recordsOkInThisBuffer`（本 buffer 在崩之前成功解析了几条）+ shape + hex + 异常栈。
  - `[CS-INV-ASSERT]`：字节账断言失败。

插桩点一览（全部在 `flink-runtime`，与辅助类同包，无需 import）：

| # | 阶段 | 文件 | 日志 stage / tag | 抓什么 |
|---|---|---|---|---|
| 1 | filter 输入 | `ChannelStateFilteringHandler.filterAndRewrite` | `filter.IN` | 喂给反序列化器之前的原始 buffer 形状 |
| 1c| filter 崩溃 | 同上（loop 外 catch） | `[CS-INV-CORRUPT]` | 崩之前成功几条 + 该 buffer 的 header 分布 + hex |
| 2 | filter 输出 | `ChannelStateFilteringHandler.serializeElement` | `filter.OUT`（逐条） | 重写出的每条 record 字节（验 header） |
| 3 | 读 chunk | `SequentialChannelStateReaderImpl`(ChannelStateChunkReader)`.readChunk` | `readChunk.IN@off<偏移>` | 从 checkpoint 文件读出的 chunk 原始字节 |
| 4 | 写-内存 | `ChannelStateCheckpointWriter.write` | `ckptWrite.MEM@off<偏移>` | 写进 checkpoint 的内存 buffer |
| 5 | 写-磁盘 | `ChannelStateCheckpointWriter.writeInputFromSpill` | `ckptWrite.SPILL@off<偏移> segLen=.. size=..` | spill 段写入位置/长度对账 |
| 6 | spill 封段 | `RecoveredChannelStateHandler.sealCurrentSegment` | `spillSeal.body` | 重写后落盘 spill 段的 record body |
| 7 | spill 读头 | `FetchedChannelStateReaderImpl.readHeaderAtCurrent` | `spillRead.header gate=.. ch=.. bufLen=.. readOff=..` | spill 12 字节段头 + 读位置 |

> 所有 `[CS-INV*]` 日志都带 `ch=`（`InputChannelInfo` 或 `SubtaskConnectionDescriptor`），便于按出错 channel 过滤。

---

## 3. 编译流程

只改了 `flink-runtime` 生产代码，重新 build 并安装到本地 `~/.m2`：

```bash
cd flink-runtime
../mvnw -T 20 clean install -U -Pfast -DskipTests \
  -Dmaven.javadoc.skip=true -Drat.skip=true -Dcheckstyle.skip=true -Denforcer.skip=true
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
3. 25 个 worker 并发离线 loop 跑 `UnalignedCheckpointRescaleITCase#shouldRescaleUnalignedCheckpoint`，**首次失败即停**。
4. 失败判定（`classify`）：日志含 `Caused by: java.io.IOException: Corrupt stream` 或 `Stream corrupted. Cannot find the header`
   或数据丢失断言 → 记为 FAIL，拷到 `repro/results/FAIL_w<id>_<n>.log`。

产物：`repro/results/FAIL_w*_*.log`（失败现场，含我们的 `[CS-INV*]` 日志）。

> 复现需 CPU 争用 + 上千 run，0.1%~0.3%/run。命中后 STOP 文件会让所有 worker 停下。

---

## 5. 命中后如何读日志定位

1. **先看 `[CS-INV-CORRUPT]`**（在 `FAIL_*.log` 里 grep）：
   - 拿到出错 `ch=`、`recordsOkInThisBuffer`、该 buffer 的 shape + hex。
   - `recordsOkInThisBuffer=0` → 在 **buffer 开头**就错位（接缝/chunk 边界问题，上一 chunk 末尾的 spanning 残留接不上）。
   - `>0` → 在 **buffer 中间**某条 record 的 length 前缀坏了（单 chunk 内部）。
2. **按该 `ch=` 过滤所有 `[CS-INV]`**，按时间顺序重建该 channel 的阶段链：
   `ckptWrite.MEM/SPILL`(Job2 写) → `readChunk.IN`(Job3 读) → `filter.IN`(Job3 过滤)。
   找**第一条**出现 `STRIDE-IRREGULAR` 或 `NO-HEADER` 的阶段——那就是字节第一次变坏的环节，bug 在它和上一阶段之间。
3. **spill 往返核对**：`spillSeal.body`(写出)对 `spillRead.header`/后续 `readChunk.IN`(读回)，看 stride / bufLen 是否一致。
4. **抽硬特征**：把 `[CS-INV-CORRUPT]` 的 hex 里 `AB CD EA FC` 的位置与"读 tag 的位置"比，算错位字节数；多复现几次看是否恒定。
   - 恒定 ±N → 查"哪一步多/少 N 字节"（length 前缀、段头 12 字节、短 buffer 边界）。
   - 不定 → 查 position/offset 大段算错（`FetchedChannelStateReaderImpl` 的 skip-prefix / Position、chunk offset）。

> 三段 job 的日志混在同一文件里，`[CS-INV*]` 不带 job id：写侧(`ckptWrite.*`)发生在 Job2（靠前），读侧(`readChunk.IN`/`filter.IN`)发生在 Job3（靠后），按出现顺序区分。

---

## 6. 注意事项

- 插桩为**临时诊断代码**，根因定位后必须删除 `ChannelStateInvariant` 及全部调用点。
- 默认 `flink.cs.debug=ON`、`records=OFF`，足够定位；若日志过大可临时 `-Dflink.cs.debug.records` 关。
- **回归测试盲区**：现有 spill 相关单测多用 8 字节 payload、只解码前几字节做去重，从不逐字节比对 → ±N 错位漏检。
  定位根因后补的回归测试必须**逐字节比对** entry/record 内容。

---

## 7. 根因结论（已 100% 证实，CONCLUSIVE）

**一句话（Round 4 订正）**：下游某个 input channel 的恢复，把**两段本不连续的在途数据**喂进**同一个**按通道复用的 spanning 反序列化器——先是它自己的 input-channel-state（段 A，结尾是半条 record），紧接着是被 FLINK-38542 重新当成本 channel input 的、上游未发送的 output buffer（段 B，开头也是半条 record，其头早已发给下游、从未快照）。反序列化器把 A 尾部的半条残留接到 B 的 head-less 开头，可这两截**不是同一条 record 的两半** → 拼接错位 → 把数据字节当 length/tag 读 → `Corrupt stream` / 静默丢数据。错误最终抛在**下游 consumer 反序列化器**（`AbstractStreamTaskNetworkInput`），也可能提前抛在 rescale filter 里。

**关键点（为什么是 bug）**：
- A（收到但未消费）和 B（从未发送）在原始流里**并不相邻**——中间隔着"已被下游消费、没进快照"的那段。所以 A 尾部的半条 record 与 B 开头的半条 record 属于**两条不同的 record**，本就不该被 splice 到一起。
- 旧的 output 恢复路径把 buffer 原样 `addRecovered` 重新注入（`RecoveredChannelStateHandler.java:627-656`），**从不逐条反序列化**，所以"从 record 中间开始"无害；新路径把同一批 output 字节**接到下游 input 的 spanning 反序列化器上逐条解析**，A 的尾部残留于是被非法地粘到 B —— 这才是根因。
- 注意：plumbing 层面 A、B 确实落到同一个 `SubtaskConnectionDescriptor(3,20)` / `InputChannelInfo{20}` 反序列化器（共用、会 splice）——bug 不是"没接上"，而是"把本不该连的两段接上了"（**Class B**：两条在途流被复用到同一个反序列化器）。

**Fix site**：
- `TaskStateAssignment.distributeOutputBufferToDownstream`（`flink-runtime/.../checkpoint/TaskStateAssignment.java:600-636`，handle 构造在 `:624-630`）——把 output handle 的 delegate+offsets 包装成 `InputChannelStateHandle`，未携带任何 record 边界上下文。
- 配合 `SequentialChannelStateReaderImpl.readInputData` 的第二个 `read(...)`（`:95-99`，`getUpstreamOutputBufferState`）→ `ChannelStateFilteringHandler` 的逐条反序列化。

**铁证（单一文件 + 单一 offset 列表，两条独立 channel 复现）**：
- 同一物理文件 `5d48e233-…`、`Starting Position: 713, 115820 bytes`、offsets `[72561,73589,74317,75046]`：
  - **写**：`buildHandle ResultSubpartitionStateHandle subPartitionIdx=3`、`ckptWrite.MEM map=OUTPUT`；
  - **读**：`readSequentially.handle InputChannelStateHandle info=InputChannelInfo{inputChannelIdx=20} oldSubtask=3`、`readChunk.IN@off72561`。
  - 映射对得上：`inputChannelIdx=20`=上游 subtask 20，`oldSubtask=3`=subPartitionIdx 3（正是 `TaskStateAssignment.java:621-630` 的转换）。
- 交付顺序：段 A（input-state，len=2142，firstHeaderAt=13，正常）**先**，段 B（output subpart-3 重打包，4 个 chunk 拼成 len=2693、128 header、firstHeaderAt=18）**后**；反序列化器带着 A 尾部约 18 字节的残留去接 B，`recordsOkInThisBuffer=0` → 拼接处首条就崩。
- 全程 `[CS-INV-ASSERT]=0` → 写侧 input/output map 的 key 路由是干净的，input handle 持有 output offsets 是**设计使然**，不是写时记账错。
- 本轮还同时复现了静默丢数据变体（`NUM_OUTPUTS != NUM_INPUTS`），证明这就是真正的丢数据 bug，而非无害的偶发报错。

**最终判定（Round 5，回答"这是边界对不齐还是不该拼"）：`unsound-splice`——不是可通过对齐修的字节 off-by-N。**
- Round 5 的 `[CS-INV-SEAM]` 显示：段 A `prevTrailingPartial=0`（**干净收尾、无残留**），而段 B `curFirstHeaderAt=2`（**从 record 中间开始**、开头约 11 字节是一条 record 的尾巴、其头不在 A 里）。既然 A 没留残留、B 却从半条开始，B 就**不可能是 A 的 splice 续接**；B 开头那截是"头已随已发送的 output buffer 发给下游并被消费、从未快照"的孤儿尾巴。
- record 计数器 `CCCC` 在单个 channel 内**本就非单调**（keyBy/rebalance 打散，健康 channel 也是 LAST<FIRST），故**不能**用计数器判连续性；只有 framing 接缝（partial/header）才是有效信号。
- 与 Round 4 统一：R4 里 A 恰好 mid-record 收尾、R5 里 A 干净收尾，两者都崩——共同不变式是「**B 从 record 中间开始，且其 head 不可靠地存在于 A / 反序列化器中**」，这是"拼了不该拼的两段"的签名，不是 off-by-N。

> 详细证据见 `round3_findings.md`/`round3_evidence.md`、`round4_verify_headsplice.md`、`round5_findings.md`/`round5_evidence.md`（以及 `round1_*`、`round2_*` 的逐轮收敛过程）。

---

## 8. 排查过程经验（逐轮收敛，可复用的方法论）

整个定位用了 **3 轮收敛 + 1 轮复核订正 + 1 轮设计判定（共 5 轮，上限 5 轮）**，每轮 = 加插桩 → 编译 → loop 复现 → 独立 agent 分析判定，每轮都 commit logs+docs。核心经验：

1. **不要猜，让字节自己说话**。最有效的不是 CRC，而是利用测试数据的确定性特征（`AB CD EA FC` header + 21 字节 stride），在每个阶段把字节剥到 record 层验证不变式，找"最早变坏的阶段"。第一轮就靠 `shape()` 的 stride/firstHeaderAt 把范围从"整条流水线"收敛到"某个未插桩的 gap"。

2. **分轮收敛，每轮只回答一个问题**。不要一次把所有日志加满：
   - R1：全链路粗插桩 → 结论"所有已插桩阶段都是好的（stride 21）"，把 bug 框到**未插桩的 gap**（spill 读回/drain/consumer）。
   - R2：补齐 gap 的插桩 → 抓到硬特征"buffer 起点错位、firstHeaderAt 异常、`recordsOkInThisBuffer=0`"，并发现损坏字节其实来自**写为 OUTPUT 的 offset**。
   - R3：加 handle/文件身份 + offsets + 跨 input/output 的 key 断言 → 抓到**单一文件单一 offset 列表既被写为 output 又被读为 input**的铁证，并由独立分析确认这是 `getUpstreamOutputBufferState` 设计路径。
   - R4（复核订正）：应 reviewer 质疑"共用反序列化器不该拼错"，重建该通道**完整有序**的 buffer 序列，发现损坏 buffer **并非**该反序列化器的第一个 buffer——段 A（input-state）先到、结尾留半条残留，段 B（重打包的 output）后到、开头也是半条；订正 R3 的机制为 **Class B：两段本不连续的在途流被复用到同一反序列化器**。教训：**结论要经得起独立复核；reviewer 的直觉常常指向机制的真正细节。**
   - R5（设计判定）：为区分"边界 off-by-N（B 本应续接 A）"vs"不该拼（B 非续接）"，加计数器解码 + 接缝 dump。结果 `prevTrailingPartial=0` + `curFirstHeaderAt=2` 直接**证否 off-by-N**（A 干净收尾却要接 B 的半条），判定 **unsound-splice**；且发现计数器非单调、不能用作连续性判据（差点被带偏——健康 channel 一比对就排除了）。教训：**先验证你的判据本身成不成立（计数器），别拿错误的信号下结论。**

3. **始终用"独立验证 agent"判定 CONCLUSIVE/INSUFFICIENT**，且判定标准要严：**日志里必须出现 healthy→corrupt 的那一步转变**，只靠"代码看着不对"不算。前两轮都诚实地判 INSUFFICIENT 并给出"下一轮要加什么"的精确清单——正是这种不放水让收敛是单调的。

4. **抓硬特征能快速排除一大批假设**。`recordsOkInThisBuffer=0` + 全程 stride=21 立刻排除了"流中间多/少 1 字节""单条 record length 写错"等，直接指向"buffer 起点未对齐 + 无 spanning 前驱"。

5. **插桩可以是观察式，也可以是 fail-fast 断言**。`assertKeyKind` 的"未触发"本身就是关键证据（证明写侧 key 路由干净，把锅从"写时记账错"转到"设计上 input handle 持有 output offsets"）。**断言不触发和触发一样有信息量**。

6. **小心插桩改变数据路径（Heisenbug 风险）**。R2 的 `spillRead.body` 改成了 eager 读，必须显式验证"改完还能复现"——确实仍复现，结论才可信。

7. **复现编排经验**：loop 跑在后台 + monitor 只在真实 trigger（FAIL / `[CS-INV-ASSERT]` / loop 结束）唤醒，不要按 pass 计数发心跳（否则反复唤醒协调者、浪费上下文）。注意 `repro.sh` 把 assert 触发的失败归类为 INFRA 且会删日志——要单独监控并在命中时立即拷贝 + `touch STOP`。

8. **修复方向（供后续，不在本次范围；R5 已判定 unsound-splice）**：核心是**不要把两段本不连续的在途流接进同一个 spanning 反序列化器**。首选修法（已确认）：在 `SequentialChannelStateReaderImpl.readInputData`（`:88-100`）里，把第二个针对 `getUpstreamOutputBufferState` 的 `read(...)` 从"逐条反序列化"的 input 恢复管线中拿掉，改为把重打包的上游 output buffer 当作**原始在途 `Buffer`** 直接 re-inject（回到 FLINK-38542 前 output-recovery 的不变式），使 head-less 的 output 流永不与 input-channel-state 的 spanning 反序列化器共用。次选：若必须共用一个 channel，给 output 段**独立的反序列化器身份** + 携带"从 record 中间开始 / 丢弃前导半条"的标记。补回归测试务必**逐字节比对**，并覆盖"input-state 干净收尾 + 重打包 output 首 buffer 从 record 中间开始"这一拼接场景（R5 现场）。

---

## 9. 复现纪律 & 兜底检查（规则）

**规则 1 — 每次复现命中后，必须备份整个运行目录（尤其 FAIL 日志）。**
`repro/repro.sh` 每次启动会 `rm -rf "$RES"`（即 `repro/results/`），**下一次复现会覆盖上一次的现场**。所以每命中一次，就把整个 `repro/results/` 备份到 `repro/` 下一个持久目录，命名沿用现有约定（参考 `repro/` 里已有的 `results-6`…`results-13`、`results_backup_corrupt*` 等）：

```bash
# 命中后、开始下一轮复现前：
cp -a repro/results "repro/results-$(date +%Y%m%d_%H%M%S)"   # 或 results-<递增编号>
```

至少要保住 `FAIL_w*_*.log` / `CORRUPT_*.log` / `DATALOSS_*.log` 与 `prime.log`。本次排查已按此保留了 `round1_FAIL.log`…`round3_FAIL.log` 等到 `requirements/38544/fix_rounds/`（`*.log` 被 gitignore，故大日志留在工作目录、结论与关键行摘录进 `round*_findings.md` / `round*_evidence.md` 提交）。

**规则 2 — 拼接层按"前置规则"校验所有物理数据（不是查有没有数据）。**
在拼接上游/下游、以及同侧多个恢复 buffer 的那一层（`ChannelStateFilteringHandler` / 每通道的 spanning 反序列化器），除了已有的事后 `hasPartialData()` 断言，应对**所有恢复出来的物理数据**做 record-framing 前置校验：每条 record 是否符合既定 framing（长度前缀合法、tag 合法、header 按 stride 出现），以及拼接边界处 A 的尾残 + B 的开头是否真能组成一条合法 record。校验失败即**当场显式失败**（loud early-fail），把静默错位/丢数据变成可定位的报错。注意：**校验是"早暴露"手段，不是根因修复**——根因仍需按 §8 处理。（`ChannelStateInvariant.shape()` 就是这种校验的诊断版，可作为正式前置断言的蓝本。）
