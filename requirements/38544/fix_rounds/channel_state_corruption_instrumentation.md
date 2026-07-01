# Channel-State 数据破坏排查：方法论与通用校验规则（high-level）

> 分支：`debug/cdr-without-spilling-data-loss-repro-FLINK-40016-based-on-original-v1-debug`
> 现象：`UnalignedCheckpointRescaleITCase#shouldRescaleUnalignedCheckpoint` 在 rescale 恢复路径上偶发
> `java.io.IOException: Corrupt stream, found tag: -NN`（channel-state / unaligned-checkpoint 在途数据恢复阶段）。
>
> **本文档定位**：这是给**一个全新的协调者 agent** 的**交接文档**，只讲**通用方法论 + 通用校验规则**——
> "遇到这类 channel-state 数据破坏问题，从逻辑上怎么排查、校验该遵循什么规则"。它接手后应能不依赖任何历史对话、独立照本文档
> 跑起排查循环。**所有落地细节（具体挂在哪个类/方法/行、编译命令、日志标签实现、当前实现的已知缺陷）见 low-level 文档
> `channel_state_corruption_lowlevel.md`。**
>
> ⚠️ **重要（防干扰）**：本文档由另一个分支/另一套 codebase 的排查记录改写而来。**那份记录里的"根因结论"已被彻底删除**，
> 不作任何保留——当前分支的 channel-state 恢复路径与之不同，任何外部预设根因都可能误导。
> 本文档只保留可复用的**方法论**和**通用校验规则**。
> **根因必须由新 agent 用当前分支自己的复现日志、从零得出**，不许照搬。

## 文档范畴（更新前必读，勿越界）

- **本文档 = HIGH-LEVEL（方法论 + 通用校验规则）**：只写逻辑层面的排查思路、经验、以及校验必须遵循的通用规则（见 §6）。
- **不写任何代码级细节**：类名、方法名、行号、编译命令、日志标签的具体实现、当前代码的落地情况——这些**一律写在 low-level 文档 `channel_state_corruption_lowlevel.md`**。本文档需要提到落地时，只引用一句"见 low-level 文档"。
- **本文档是"校验规则"的唯一权威**：§6 的通用规则（完整 buffer 校验、上下游容忍度、恢复链全程不容忍、只有 checkpoint 写入区分上下游、日志分级）是设计约束。代码实现必须服从本文档；若代码与本文档冲突，是代码待修正，**不是**改本文档去迁就代码。
- 更新本文档时：只增补通用方法论/规则。**禁止**把某个具体代码位置、某次复现的偶然细节写进来——那些属于 low-level 文档或 `round*_findings.md`。

---

## 工作流程（给协调者 agent 的执行主干）

**你是协调者。你自己不写代码、不跑 mvn、不读大日志正文；所有具体动作都委托给 sub-agent。** 目标：在当前分支上定位
`Corrupt stream` 数据破坏的**真正根因**（当前分支自己的证据，不许照搬任何外部结论）。

一轮 = 下面 5 步，**循环执行直到定位根因**（参考上限 5 轮，每轮只回答一个问题、逐步缩小范围）：

1. **决定本轮要回答的问题 + 插桩方案**（协调者决策）。**本轮排查主力手段是 §6 的"按 channel 的完整数据校验"**——不是逐个 buffer 看，
   而是把每个 channel 恢复/写入/重写的**所有 buffer 拼接成完整数据**后整体校验 record framing，看**checkpoint 写入层 / 恢复拼接层 /
   重写到内存层 / 物理 channel 接收层**这几层里**哪一层先坏**（校验失败只打结构化日志、不抛异常）。逐阶段插桩是辅助观察。
   第一轮先把这几层校验挂上，把 bug 框到某一层；之后每轮针对暴露出问题的那一层加更细的插桩。
2. **委托 sub-agent 加日志（改生产代码）**。sub-agent 在 `flink-runtime` 生产代码里加插桩，**然后编译**，编译通过后**把这轮插桩代码 commit**
   （commit message 说明本轮加了哪些插桩、要回答什么问题）。⚠️ 插桩是临时诊断代码，但每轮都要 commit，保证"改动可追溯、可回退、
   下一轮基于干净状态继续"。**具体插桩点、编译命令见 `channel_state_corruption_lowlevel.md`。**
3. **委托 sub-agent 跑复现**（多 worker、后台跑，首次命中即停）。命中后按 §9 规则 1 **立即备份 `repro/results/`**。
4. **委托独立 verification sub-agent 分析命中的 FAIL 日志**（§5 的读法）。它必须判定 **CONCLUSIVE / INSUFFICIENT**：只有日志里出现
   **healthy→corrupt 的那一步转变**才算 CONCLUSIVE；否则判 INSUFFICIENT，并给出"下一轮要在哪里加什么插桩"的精确清单。
   **把本轮 findings 摘要写进 `requirements/38544/fix_rounds/round<N>_findings.md` 并 commit**（大日志被 gitignore，只提交结论与关键行摘录）。
5. **收敛判断**：CONCLUSIVE → 进入根因确认 + 设计修复（修复本身另起流程，不在本文档范围）；INSUFFICIENT → 回到第 1 步，按清单开下一轮。

**分层纪律**：协调者只调度、只读 sub-agent 返回的结论；改代码/编译/跑复现/读大日志一律 sub-agent 做。加日志的 sub-agent 与分析日志的
verification sub-agent **必须是不同的、独立的 sub-agent**（不许自己验证自己）。

**心态**：不要猜、不要信任何预设根因。只让当前分支的字节和日志说话（§1）。

---

## 0. 现象与已知事实

- 报错：`Caused by: java.io.IOException: Corrupt stream, found tag: -NN`，抛在 `StreamElementSerializer.deserialize` 读 tag 时（tag 合法值仅 0~6，读到负数=字节流错位）。
- 最近一次实测命中：`found tag: -22`（`0xEA` = header 第 3 字节）。
- 复现率：当前分支实测约 **~5%/run**。仍需 CPU 争用；用多 worker 跑几十~上百次内可稳定命中。
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

## 2. 插桩落地

插桩为**临时诊断代码**，定位根因后必须全部删除。具体到类/方法/行的插桩点、辅助类 `ChannelStateInvariant` 的方法与日志标签、
以及当前实现的已知缺陷，全部见 low-level 文档 `channel_state_corruption_lowlevel.md`。本文档不出现任何具体代码位置。

---

## 3. 编译流程

只改 `flink-runtime` 生产代码后需重新 build 并安装到本地 `~/.m2`。**具体编译命令见 `channel_state_corruption_lowlevel.md`。**

---

## 4. 复现流程

```bash
bash repro/repro.sh 16 2000 600
#                    │   │    └─ 每次 run 超时(秒)
#                    │   └────── 目标总 run 数
#                    └────────── 并发 worker 数（上限 16，避免 CPU 争用把机器压卡）
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

## 5. 命中后如何读日志定位（方法论）

1. **先看抛错瞬间的诊断日志**（在 `FAIL_*.log` 里 grep 校验日志标签）：拿到出错 `ch=`、buffer 里成功解析了几条 record、该 buffer 的 shape + hex。
   - 成功条数=0 → 在 **buffer 开头**就错位（接缝/边界问题，上一段末尾的 spanning 残留接不上）。
   - >0 → 在 **buffer 中间**某条 record 的 length 前缀坏了。
2. **按该 `ch=` 过滤所有校验日志**，按时间顺序重建该 channel 的阶段链（checkpoint 写入 → 恢复读回 → filter 重写 → 物理 channel 接收）。
   找**第一条**出现 stride 不齐 / 无 header / 中间 record 破损的阶段——那就是字节第一次变坏的环节。
3. **input vs output-as-input 交叉核对**：rescale 恢复时上游 output buffer 会被当作 input 恢复到同一虚拟 channel，重点核对
   这两路是否落到同一个 recovered channel / 同一个反序列化器，以及接缝处 A 尾 + B 头是否真能组成一条合法 record。
4. **抽硬特征**：把抛错瞬间 hex 里 `AB CD EA FC` 的位置与"读 tag 的位置"比，算错位字节数；多复现几次看是否恒定。
   - 恒定 ±N → 查"哪一步多/少 N 字节"。
   - 不定 → 查 position/offset 大段算错。

> 三段 job 的日志混在同一文件里，校验日志不带 job id：写侧在较早 job，读侧/重写侧在较后 job，按出现顺序区分。
> 具体的日志标签名称、字段含义见 `channel_state_corruption_lowlevel.md`。

---

## 6. 核心校验规则（通用，high-level）

> 前提：**已确定一定有 bug**，校验的唯一目的是**逐环节缩小范围，定位数据第一次变坏的地方**。
> 本节只讲通用规则（逻辑层面）；具体挂在哪个类、哪个方法、哪一行，见 low-level 文档
> `channel_state_corruption_lowlevel.md`。

### 6.1 校验单位 = 一个 channel 的**完整数据**（所有 buffer 拼接），不是单个 buffer

- 数据破坏的本质单位是 channel：坏掉的永远是"某个 channel 的数据"。
- 一个 channel 会有多个 buffer，**buffer 只是物理传输上的切分**。把一个 channel 的**所有 buffer 按顺序拼接**，才是这个 channel
  的"完整数据"。**校验必须对这份拼接后的完整数据做，绝不能逐个 buffer 单独校验**（单个 buffer 天然可能从半条 record 开始/结尾）。
- 完整数据的正确形态：一条条 record 严丝合缝，每条 `[4B length]` 合法、tag 合法、header 按固定 stride 周期出现。

### 6.2 上游 vs 下游：**校验的容忍度不同**（关键规则）

同样是"完整数据校验"，对上游和下游要用**不同判据、打不同日志**：

- **下游（input channel）——不容忍任何半条**。下游一个 channel 的完整数据必须从第一条 record 的开头开始、条条完整、首尾无悬挂半条。
  **下游只要出现"半条开头/中间断裂"，就一定是 bug**，必须当作真损坏报出来。
- **上游（ResultSubpartition / output）——容忍首尾半条**。因为一条 record 可能**一半已经发往下游、一半还留在上游**，
  快照上游 output 时，采到的数据**从半条 record 中间开始是业务上真实、正常的现象**。所以上游校验必须**容忍开头/结尾的悬挂半条**，
  只校验中间部分 record 是否等间距连续；**上游出现半条 → 打成"正常/可容忍"级别的日志，不算 ASSERT**。

> **为什么要区分**：下游出现半条 = 一定是 bug；上游出现半条 = 业务上真的可能发生、可容忍。用同一套判据会把上游的正常半条误报成损坏
> （这正是早期几轮产生成千上万条假 ASSERT 的根因）。

### 6.3 恢复阶段是例外：**全程不容忍半条**（即使数据来自上游）

上面 6.2 的"上游可容忍"**只适用于正常运行时快照上游 output**。**恢复（restore）阶段完全不允许半条**，理由：

- 恢复时，同一个通道的数据会被组织进一个**虚拟 channel**——**上游的数据也会挪到下游、按这个虚拟 channel 一起恢复**。
- 既然是"同一个虚拟 channel 内的、本就该连续的完整数据"，那么**把它所有 buffer 连起来校验，就不该出现任何半条**。
  虚拟 channel 会**重写**数据，重写后交给真正的**物理 channel**，物理 channel 侧同样是完整数据、同样不容忍半条。
- 所以恢复链上的每一环（恢复读回的虚拟 channel 数据、重写后注入物理 channel 的数据、以及物理 channel 接收到的数据）
  **都按"完整数据 + 不容忍半条"校验**，一旦出现半条就是真损坏。

### 6.4 结论：只有 **checkpoint 写入**这一环需要区分上下游

- **checkpoint 写入阶段**：同时写 input channel state（下游语义，不容忍）和 upstream output state（上游语义，容忍首尾半条）
  → **这一环必须区分上下游，用不同判据、打不同日志**。
- **恢复链所有环节**（恢复读虚拟 channel → 重写 → 物理 channel 接收）：**一律按完整数据、不容忍半条**，不区分来源。

### 6.5 校验行为（通用约定）

- **只打结构化日志，不抛异常**：不打断数据路径，保留完整现场，事后按 channel 标识跨环节比对"从哪一环开始变坏"。
- **日志按容忍度区分**：不容忍场景的违规打成断言级（真损坏）；上游可容忍的首尾半条打成普通/观测级，**两者日志标签或级别必须不同**，
  避免真损坏被正常半条淹没。
- **真正穿透噪声的信号**：无论上下游，"完整数据中间某条 record 的 stride 突然偏离固定周期"都是真损坏——这个信号不受首尾半条影响，
  是最可靠的判据。

---

## 7. 注意事项

- 插桩为**临时诊断代码**，根因定位后必须删除辅助类及全部调用点（清单见 `channel_state_corruption_lowlevel.md`）。
- **回归测试盲区**：现有 spill/channel-state 相关单测多用短 payload、只解码前几字节，从不逐字节比对 → ±N 错位漏检。
  定位根因后补的回归测试必须**逐字节比对** record 内容。
- 小心插桩改变数据路径（Heisenbug 风险）：任何把 lazy 读改成 eager 读之类的插桩，必须显式验证"改完还能复现"再信结论。

---

## 8. 排查过程经验（逐轮收敛，可复用的方法论）

1. **不要猜，让字节自己说话**。利用测试数据的确定性特征（`AB CD EA FC` header + 恒定 stride），在每个阶段把字节剥到 record 层验证不变式，找"最早变坏的阶段"。先用 stride/firstHeaderAt 把范围从"整条流水线"收敛到"某个未插桩的 gap"。

2. **分轮收敛，每轮只回答一个问题**。不要一次把所有日志加满：先全链路粗插桩定位到"未插桩的 gap"，再补齐 gap 抓硬特征，再加 handle/文件身份 + offsets + 跨 input/output 的 key 断言坐实。

3. **始终用"独立验证 agent"判定 CONCLUSIVE/INSUFFICIENT**，且标准要严：**日志里必须出现 healthy→corrupt 的那一步转变**，只靠"代码看着不对"不算。诚实判 INSUFFICIENT 并给出"下一轮要加什么"的精确清单，才能让收敛单调。

4. **抓硬特征能快速排除一大批假设**。"成功条数=0" + 全程 stride 恒定 → 立刻排除"流中间多/少 1 字节""单条 record length 写错"，直接指向"buffer 起点未对齐 + 无 spanning 前驱"。

5. **插桩可以是观察式，也可以是 fail-fast 断言**。断言"未触发"本身也是关键证据（如证明写侧 key 路由干净）。**断言不触发和触发一样有信息量**。

6. **小心插桩改变数据路径（Heisenbug 风险）**。任何改变读写时机的插桩都要显式验证"改完还能复现"，结论才可信。

7. **复现编排经验**：loop 跑在后台 + monitor 只在真实 trigger（FAIL / ASSERT / loop 结束）唤醒，不要按 pass 计数发心跳。注意 `repro.sh` 把 assert 触发的失败可能归类为 INFRA 并删日志——要单独监控并在命中时立即拷贝 + `touch STOP`。

8. **先验证判据本身成不成立，再拿它下结论**。例如 record 计数器在单个 channel 内可能本就非单调（keyBy/rebalance 打散），不能用作连续性判据；只有 framing 接缝（partial/header）才是有效信号。

9. **区分"单个 checkpoint 的片段"和"channel 完整数据流"**。如果校验累积器只按单个 checkpoint 分片聚合，校验的其实是"一次 checkpoint 里这个 channel 写了什么"，而不是"这个 channel 的完整数据流"——前者天然可能首尾半条，会污染判据。真正要校验"完整数据不容忍半条"时，聚合粒度必须是 channel 完整数据流。

---

## 9. 复现纪律 & 兜底检查（规则）

**规则 1 — 每次复现命中后，必须备份整个运行目录（尤其 FAIL 日志）。**
`repro/repro.sh` 每次启动会 `rm -rf "$RES"`（即 `repro/results/`），**下一次复现会覆盖上一次的现场**。所以每命中一次，就把整个 `repro/results/` 备份到 `repro/` 下一个持久目录：

```bash
# 命中后、开始下一轮复现前：
cp -a repro/results "repro/results-$(date +%Y%m%d_%H%M%S)"   # 或 results-<递增编号>
```

至少要保住 `FAIL_w*_*.log`。`*.log` 被 gitignore，故大日志留在工作目录、结论与关键行摘录进 `round*_findings.md` 提交。

**规则 2 — 拼接层按"前置规则"校验所有物理数据（不是查有没有数据）。**
在拼接 recovered buffer 的那一层，除了已有的事后 `hasPartialData()` 断言，
应对**所有恢复出来的物理数据**做 record-framing 前置校验：每条 record 是否符合既定 framing（长度前缀合法、tag 合法、header 按 stride 出现），
以及拼接边界处 A 的尾残 + B 的开头是否真能组成一条合法 record。校验失败即**当场显式失败**（loud early-fail），把静默错位/丢数据变成可定位报错。
注意：**校验是"早暴露"手段，不是根因修复**。（诊断版校验器可作为正式前置断言的蓝本，实现细节见 `channel_state_corruption_lowlevel.md`。）
