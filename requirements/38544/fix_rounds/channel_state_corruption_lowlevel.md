# Channel-State 数据破坏排查：当前分支代码级落地（low-level）

> 分支：`debug/cdr-without-spilling-data-loss-repro-FLINK-40016-based-on-original-v1-debug`
> 配套的通用方法论 / 校验规则见 `channel_state_corruption_instrumentation.md`（下称 HL）。
> 本文档只讲**当前分支真实代码里**的落地细节：辅助类现状、已加的校验点（类/方法/行）、当前实现的已知缺陷、编译与测试命令。
> 所有行号都以当前分支代码为准；行号会随插桩演进漂移，标"约"的以方法名为准重新定位。

## 文档范畴（更新前必读，勿越界）

- **本文档 = LOW-LEVEL（代码级落地）**：只写"当前分支代码里具体怎么落地"——类、方法、行号、标签实现、key 分组、已知缺陷、编译/测试命令。
- **不写通用规则/方法论**。校验必须遵循的规则（完整 buffer 校验、上下游容忍度、恢复链不容忍、只有 checkpoint 写入区分上下游、日志分级等）**一律以 HL 文档 `channel_state_corruption_instrumentation.md` §6 为唯一权威**。
- **规则冲突时以 HL 为准**：若本文档的代码实现与 HL 的规则不一致，那是"当前代码有缺陷/待修正"，应记入本文档"已知缺陷 / 待修正项"，**绝不能反过来改 HL 的规则去迁就代码**。
- 更新本文档时：只更新代码事实（随插桩演进）。**禁止**把通用规则搬进来、也**禁止**在这里新立与 HL 冲突的规则。

---

## 1. 辅助类 `ChannelStateInvariant`

路径：`flink-runtime/src/main/java/org/apache/flink/runtime/checkpoint/channel/ChannelStateInvariant.java`
（`final`、纯静态、临时诊断代码，定位根因后整类删除）

### 开关（system property）

- `-Dflink.cs.debug`：默认 **ON**；`-Dflink.cs.debug=false` 关闭全部校验（`ENABLED` 字段，约 `:57`）。
- `-Dflink.cs.debug.records=true`：默认 **OFF**，开启逐条 record 日志（极吵，`LOG_RECORDS`，约 `:60`）。

### 常量

- `HEADER = {0xAB,0xCD,0xEA,0xFC}`（约 `:63`）——测试数据每条 record value 的高 4 字节固定头。
- `MIN_TAG=0` / `MAX_TAG=6`（约 `:66/:69`）——`StreamElementSerializer` 合法 tag 范围，用于 framing 走查判 tag 合法性。

### 方法

| 方法 | 约行 | 作用 |
|---|---|---|
| `isEnabled()` | `:76` | 返回 `ENABLED`，call site 先判它再干活 |
| `clearAll()` | 约 `:134`（新增，紧邻 `isEnabled()`） | 无条件 `ACCUMULATORS.clear()`，不受 `ENABLED` gate；供测试在每个 job 启动前调用，清掉上一轮 job 未 `flush` 的残留累积器 |
| `append(String key, ByteBuffer buffer)` | `:86` | 把 buffer 的可读字节**拷贝**进 `key` 对应的累积器；用 `duplicate()`，**不动** reader index / refcount |
| `flush(String key, String taskAndChannel, Layer layer)` | `:104` | 取出并移除 `key` 的累积字节，对**拼接后的完整字节流**跑 `shape()`，打 `[CS-INV] layer=<L>`；`!valid` 再打 `[CS-INV-ASSERT] layer=<L>` |
| `key(String identity, String channel, Layer layer, Direction direction)` | `:134` | 组 accumulator key = `identity \| channel \| layer \| direction` |
| `label(String task, String channel)` | `:208` | 组人类可读标签 = `task=<..> ch=<..>` |
| `validateSnapshot(String taskAndChannel, long barrierId, List<Buffer> buffers)` | `:225` | 快照专用：**自己 retain 一份**副本、拼接、按 `Mode.LENIENT` 跑 `shape()`，打 `[CS-INV-SNAP] layer=SNAPSHOT`；`!valid` 时按 `logViolationIfAny` 分级打 `[CS-INV-ASSERT]`（真损坏）或 `[CS-INV-TOLERATED]`（首尾半条）；`finally` 里 recycle 自己的副本，不影响 caller 的 buffer |
| `shape(byte[] bytes)` / `shape(byte[] bytes, Mode mode)` | `:270` / `:289` | 核心判据，见下。无 `Mode` 参数的重载等价于 `Mode.STRICT` |
| `logViolationIfAny(Shape, String, String)` | `:182` | 按 `shape.toleratedEdgeOnly` 决定打 `[CS-INV-ASSERT]`（断言级）还是 `[CS-INV-TOLERATED]`（观测级），`flush`/`validateSnapshot` 共用 |

### `Layer`（枚举，五个校验阶段的统一命名）

`ChannelStateInvariant.Layer`：`SNAPSHOT`、`CHECKPOINT_WRITE`、`RECOVER_READ`、`RECOVER_REWRITE`、`CHANNEL_RECEIVE`——与 HL §6.1.5
一一对应，取代旧版 `SNAP`/`WRITE`/`RECOVER`/`REWRITE`/`RECV` 字符串。所有 `key()`/`flush()` 调用现在都传这个枚举，日志里 `layer=` 输出的就是
枚举名。

### `Direction`（枚举，区分 input / output）

`ChannelStateInvariant.Direction`：`INPUT` / `OUTPUT`。嵌入聚合 key 的第四段，避免 input-channel 数据与 output-subpartition
数据被误聚合到一起（§6.6 要求的 input/output 维度）。`SNAPSHOT`、`RECOVER_READ`、`RECOVER_REWRITE`、`CHANNEL_RECEIVE` 全部固定
`INPUT`（这四个阶段本就只处理 input-channel 数据）；`CHECKPOINT_WRITE` 按调用的是 `writeInput`/`completeInput`（`INPUT`）还是
`writeOutput`/`completeOutput`（`OUTPUT`）区分。

### `Mode`（`:77`）：区分上下游/恢复链容忍度

- `Mode.STRICT`：不容忍首尾悬挂半条，从第 0 字节起必须自包含。用于一切下游 / 恢复链场景：`CHANNEL_RECEIVE`、`RECOVER_READ`、
  `RECOVER_REWRITE`、`CHECKPOINT_WRITE` 的 input 语义。
- `Mode.LENIENT`：容忍开头到第一个可解析 record 之前的悬挂字节、容忍结尾不足一条完整 record 的悬挂字节，只校验中间 record 的
  length/tag/stride 连续性。用于上游快照场景：`SNAPSHOT`（`validateSnapshot` 固定用 `LENIENT`）、`CHECKPOINT_WRITE` 的 output 语义
  （`completeOutput` 里显式传 `Mode.LENIENT`）。
- **无论哪种模式，中间 record 的 STRIDE-IRREGULAR 都判真损坏**（`shape.strideIrregular` 优先于 `toleratedEdgeOnly`，见下）。

### 日志标签（当前实现）

| 标签 | 出处 | 含义 |
|---|---|---|
| `[CS-INV]` | `flush` | 一个 key 的完整拼接数据形状（每次 flush 一行，带 `layer=`/`mode=` 字段） |
| `[CS-INV-ASSERT]` | `logViolationIfAny`（`flush`/`validateSnapshot` 共用） | 真损坏：`STRICT` 模式下任何违规，或任意模式下命中 `STRIDE-IRREGULAR` |
| `[CS-INV-TOLERATED]` | `logViolationIfAny` | 仅 `LENIENT` 模式下的首尾悬挂半条（观测级，不算断言） |
| `[CS-INV-SNAP]` | `validateSnapshot` | 快照（一个 barrier 收集到的 buffer 集）的形状（固定 `Mode.LENIENT`，`layer=SNAPSHOT`） |
| `[CS-INV-REC]` | `flush`（仅 `LOG_RECORDS=true`）| 逐条 record（at/len/tag） |

> 已废弃：旧版 `SNAP`/`WRITE`/`RECOVER`/`REWRITE`/`RECV` 字符串 layer 名已全部替换为 `Layer` 枚举（`SNAPSHOT`/`CHECKPOINT_WRITE`/
> `RECOVER_READ`/`RECOVER_REWRITE`/`CHANNEL_RECEIVE`）。旧版 `[CS-INV-SNAP-ASSERT]` 标签不再单独存在，SNAP 违规现在也走
> `logViolationIfAny`，按 STRICT/LENIENT 统一分级为 `[CS-INV-ASSERT]` 或 `[CS-INV-TOLERATED]`。HL 旧稿提到的 `[CS-INV-CORRUPT]`、
> `filter.IN/OUT`、`readChunk.IN`、`recover.INJECT` 这类 per-buffer 阶段标签，**当前分支代码里并不存在**。读日志时以本表为准。

### `shape()` 的判据（约 `:270`–`:319`）

对一段拼接好的 `byte[]` 及给定 `mode`：
1. `findHeaderOffsets`：扫出所有 `AB CD EA FC` 出现的偏移。
2. 相邻偏移差 = stride；只要有两个 stride 不相等 → `strideIrregular=true`，`summary()` 里打 `*** STRIDE-IRREGULAR ***`。
   **这一步不区分 mode**——中间 record 的 stride 突变永远是真损坏信号。
3. `walkFraming`（`:364`）：
   - `STRICT`：从 `pos=0` 起硬走；任何一处 `length<=0`、`payloadStart+length>end`、或 `payload[0]` 不在 `[MIN_TAG,MAX_TAG]`
     都记 `firstCorruptRecordAt` 并停（不 resync），dump 前后各 16 字节 hex。
   - `LENIENT`：先用 `skipToFirstParseableRecord`（`:425`）跳过开头到第一个能真正解析出合法 record 的偏移（悬挂头部）；
     走到末尾时，若剩余字节不足 4B、或 `length<=0`、或声明的 payload 长度超出剩余字节——这些都被判定为"结尾悬挂半条"
     （`shape.tailTolerated=true`），不记 `firstCorruptRecordAt`；只有在**还有充足剩余字节、明显不是尾部截断**的情况下遇到的
     违规，才当真损坏记 `firstCorruptRecordAt`。
4. `valid = !strideIrregular && headerCount>0 && firstCorruptRecordAt<0`（两种 mode 共用同一个 valid 定义）。
5. `toleratedEdgeOnly`：`!valid && mode==LENIENT && !strideIrregular && (headerCount==0 || tailTolerated)`——即"仅因为
   LENIENT 允许的首尾悬挂而 invalid"时为 true，供 `logViolationIfAny` 决定打断言级还是观测级。
6. `summary()`：`headerCount==0` 打 `NO-HEADER`；否则打 `headers=N firstHeaderAt=.. strides=[..]`（+ 可能的 STRIDE-IRREGULAR）
   + `parsedRecords=..` + 可能的 `*** CORRUPT-RECORD-AT=.. before=[hex] after=[hex] ***` + 可能的 `tail-tolerated`。

---

## 2. 当前已加的校验点（读代码核实）

所有 call site 都先 `ChannelStateInvariant.isEnabled()` 再动作。累积型（`append`+`flush`）和快照型（`validateSnapshot`）两类。
`flush` 有两个重载：3 参数版固定 `Mode.STRICT`；4 参数版可显式传 `Mode`。五个阶段统一用 `ChannelStateInvariant.Layer` 枚举命名
（不再是散落字符串），聚合 key 统一用 `key(identity, channel, layer, direction)` 组装（`Direction.INPUT`/`OUTPUT` 是新增的第四维度）。

### 2.1 `CHANNEL_RECEIVE`（恢复迁入物理 channel）—— `Mode.STRICT`

| 类（相对路径） | 方法 | key / label | layer / mode |
|---|---|---|---|
| `.../io/network/partition/consumer/LocalInputChannel.java` | 构造器里 `initialRecoveredBuffers` 迁移循环 | key=`(taskLabel, channelInfo+" kind=Local", CHANNEL_RECEIVE, INPUT)`；task=`stateWriter.taskLabel()` | `CHANNEL_RECEIVE` / `STRICT`（3 参数 `flush`） |
| `.../io/network/partition/consumer/RemoteInputChannel.java` | 构造器里 `initialRecoveredBuffers` 迁移循环 | key=`(taskLabel, channelInfo+" kind=Remote", CHANNEL_RECEIVE, INPUT)` | `CHANNEL_RECEIVE` / `STRICT`（3 参数 `flush`） |

- 只对 `buffer.isBuffer()`（跳过 event）累积；用 `getNioBufferReadable()`，不动 reader index。
- **key 按 `taskLabel`（=物理 channel 所在 task）分组**，一个物理 channel 的迁入 buffer 全部拼在一起后 flush 一次 → 这正是"物理 channel 接收侧完整数据"。生命周期分界=这一次构造器调用（每个物理 channel 实例只在构造时迁入一次，天然不跨生命周期）。
- `CHANNEL_RECEIVE` 是恢复链的终点（重写后注入物理 channel），按 §6.3 规则全程不容忍半条，用 `STRICT`。
- **`taskLabel`（=`jobVertexID-subtaskIndex`）没有 jobId/attempt**：`stateWriter.taskLabel()` 的实现类 `ChannelStateWriterImpl`
  没有持有 jobId/attempt 字段，其构造器目前只接收 `jobVertexID`/`subtaskIndex`。要补上需要改 `ChannelStateWriterImpl` 的构造器
  签名（生产 wiring 代码，被 `SubtaskCheckpointCoordinatorImpl.openChannelStateWriter` 等多处业务代码和多个测试类调用）——
  按纪律不允许为此改业务签名，**记为待办**（见 §3）。

### 2.2 `SNAPSHOT`（正常运行时 checkpoint 采样在途数据）—— `Mode.LENIENT`

| 类 | 方法 | 说明 |
|---|---|---|
| `LocalInputChannel.java` | `checkpointStarted(CheckpointBarrier)` | 收集 `toBeConsumedBuffers` 里的 in-flight buffer，`validateSnapshot` 在锁外直接跑（Local 无并发锁问题）|
| `RemoteInputChannel.java` | `checkpointStarted(CheckpointBarrier)` | **锁外校验**：在 `synchronized (receivedBuffers)` 内对 `getInflightBuffersUnsafe` 结果**逐个 `retainBuffer()` 存进 `invariantSnapshot`**，出锁后再 `validateSnapshot`，跑完逐个 `recycleBuffer()`。这样校验的拷贝/日志不占锁、也不碰交给 writer 的那批 buffer 的 refcount。|

- `SNAPSHOT` 标签是 `[CS-INV-SNAP]`，**不走 `flush`/`key`**（`validateSnapshot` 内部自建拼接、内部固定用 `Mode.LENIENT`，日志里
  显式打 `layer=SNAPSHOT`）；其分组粒度=**单个 barrier 的一次快照**（参数 `barrierId`），即"这一个 checkpoint 里这个 channel
  采到的 in-flight 片段"，**不是** channel 完整数据流——这是上游 output 语义（一条 record 可能已半发往下游），按 §6.2 规则容忍
  首尾半条，命中时打观测级 `[CS-INV-TOLERATED]` 而非断言级。生命周期分界=`barrierId`（已天然满足"单次快照"分界）。
- 同样使用 `stateWriter.taskLabel()`，**没有 jobId/attempt**，原因与 §2.1 相同，见 §3 待办。

### 2.3 `CHECKPOINT_WRITE` —— input 用 `Mode.STRICT`，output 用 `Mode.LENIENT`

| 类 | 方法 | key / 分组 | layer / mode |
|---|---|---|---|
| `.../checkpoint/channel/ChannelStateCheckpointWriter.java` | `writeInput(...)` 累积；`completeInput(...)` flush | `invariantWriteKey(JobVertexID,int,InputChannelInfo)` = key(`jobVertexID-subtaskIndex-cp<checkpointId>`, `info.toString()`, `CHECKPOINT_WRITE`, `INPUT`) | `CHECKPOINT_WRITE` / `STRICT`（3 参数 `flush`） |
| 同上 | `writeOutput(...)` 累积；`completeOutput(...)` flush | `invariantWriteKey(JobVertexID,int,ResultSubpartitionInfo)` 重载 = key(`jobVertexID-subtaskIndex-cp<checkpointId>`, `info.toString()`, `CHECKPOINT_WRITE`, `OUTPUT`) | `CHECKPOINT_WRITE` / `LENIENT`（`completeOutput` 显式传 `ChannelStateInvariant.Mode.LENIENT`） |

- **`CHECKPOINT_WRITE` 层现在按 §6.4 规则区分上下游**：input（下游语义）用 `STRICT`；output（上游语义，一条 record 可能半发往下游）
  用 `LENIENT`，命中首尾半条打观测级，不算断言。两者共用同一个 `key`/`label` 结构，仅 `Mode` 和 `Direction` 不同，通过两个重载的
  `invariantWriteKey` 区分参数类型（`InputChannelInfo` vs `ResultSubpartitionInfo`）。
- **key 的 identity 段仍是 `jobVertexID-subtaskIndex-cp<checkpointId>`**——`-cp<checkpointId>` 已经是"单次 checkpoint 写入"
  这个生命周期维度，满足 §6.6 的生命周期分界要求；`info.toString()`（`InputChannelInfo`/`ResultSubpartitionInfo`）已包含
  gateIdx/channelIdx（或 partitionIdx/subPartitionIdx）。
- **没有 jobId/attempt**：`ChannelStateCheckpointWriter`/`writeInput`/`writeOutput`/`completeInput`/`completeOutput` 的方法
  签名只接收裸 `JobVertexID jobVertexID, int subtaskIndex`（见 `SubtaskID`），整条 `ChannelStateWriteRequest` 请求链路都不携带
  jobId/attempt。要补上需要改这些方法和 `ChannelStateWriteRequest` 的业务签名——按纪律不允许，**记为待办**（见 §3）。
- **`CHECKPOINT_WRITE` 累积 key 仍带 `-cp<checkpointId>`** → 按**单个 checkpoint** 分片，不是 channel 完整数据流（见 §3 已知局限
  ——未变更，仍是已知的粒度局限，但因为 output 侧现在已经用 LENIENT 判据，单 cp 片段的首尾半条不会再误报为断言）。

### 2.4 `RECOVER_READ` / `RECOVER_REWRITE` —— `Mode.STRICT`

| 类 | 方法 | key / 分组 | layer / mode |
|---|---|---|---|
| `.../checkpoint/channel/RecoveredChannelStateHandler.java`（内部类 `InputChannelRecoveredStateHandler`）| `recover(InputChannelInfo, int, BufferWithContext)` 累积 | `invariantRecoverKey` = key(`recovery-<identityHashCode(this)>`, `channelInfo`, `RECOVER_READ`, `INPUT`) | `RECOVER_READ` / `STRICT`（3 参数 `flush`） |
| 同上 | `recoverWithFiltering(...)` 累积（对 `filterAndRewrite` 返回的每个 filtered buffer）| `invariantRewriteKey` = key(`recovery-<identityHashCode(this)>`, `channelInfo`, `RECOVER_REWRITE`, `INPUT`) | `RECOVER_REWRITE` / `STRICT`（3 参数 `flush`） |
| 同上 | `close()` flush RECOVER_READ + RECOVER_REWRITE | 遍历 `channelsSeenForInvariantCheck` 逐 channel flush | —— |

- **key 用 `recovery-<identityHashCode(this)>`**，即**每个 recovery pass（一次 `readInputData` 一个 handler 实例）一组**，
  在 `close()` 时对该 pass 见过的每个 channel 一次性 flush → 这**是** channel 完整数据流粒度（与 `CHECKPOINT_WRITE` 的 per-cp
  分片不同，注意区分）；生命周期分界=这次 handler 实例的生命周期（一次 recovery pass），已天然满足。
- `RECOVER_READ` 累积的是恢复读回的原始字节（`recover` 里注入前）；`RECOVER_REWRITE` 累积的是 `filterAndRewrite` 重写后、即将
  `onRecoveredStateBuffer` 注入物理 channel 的字节。
- 按 §6.3 规则，恢复链全程不容忍半条（即使源数据来自上游 output，恢复时已重组进虚拟 channel），两者都用 `STRICT`。
- **没有 jobId/attempt/jobVertexID/subtaskIndex**：`readInputData(InputGate[] inputGates, ...)` 只传 `InputGate[]`，
  `InputGate`/`InputChannel`/`RecoveredInputChannel` 都没有公开访问器能拿到 jobId、attempt、jobVertexID 或 subtaskIndex
  （`SingleInputGate` 只有 `owningTaskName`——人类可读字符串，不是结构化 jobVertexID）。这四个维度在当前代码结构下拿不到，
  要拿到需要新增 getter 或改 `readInputData`/handler 构造器签名——按纪律不允许改业务签名，**记为待办**（见 §3）。key 里只有
  gateIdx/channelIdx（通过 `channelInfo`）和 recovery pass 身份两个维度。
- **output 侧恢复仍无插桩**（`RecoveredChannelStateHandler.recover(ResultSubpartitionInfo, ...)` 没有 `ChannelStateInvariant`
  调用）——这是本轮改动范围之外的既有缺口，未新增也未修正，见 §3。

### 2.5 各层 key 分组 + mode 一览（重点：分组粒度决定校验对象，mode 决定容忍度）

| layer | 分组维度（identity 段） | channel 段 | direction | 校验对象 | 是否 = channel 完整数据流 | mode |
|---|---|---|---|---|---|---|
| `CHANNEL_RECEIVE` | `taskLabel`（jobVertexID-subtaskIndex，**无 jobId/attempt**） | channelInfo+kind | `INPUT` | 物理 channel 迁入的全部 buffer | 是 | `STRICT` |
| `SNAPSHOT` | `taskLabel`（**无 jobId/attempt**） + 单个 `barrierId` | channelInfo+kind | `INPUT`（隐含） | 一次 checkpoint 采到的 in-flight 片段 | **否**（单 cp 片段） | `LENIENT` |
| `CHECKPOINT_WRITE`（input） | `jobVertexID-subtask-cp<N>`（**无 jobId/attempt**） | channelInfo | `INPUT` | 单个 checkpoint 写入的 input 字节 | **否**（单 cp 片段） | `STRICT` |
| `CHECKPOINT_WRITE`（output） | `jobVertexID-subtask-cp<N>`（**无 jobId/attempt**） | subpartitionInfo | `OUTPUT` | 单个 checkpoint 写入的 output 字节 | **否**（单 cp 片段） | `LENIENT` |
| `RECOVER_READ` | `recovery-<handler实例>`（**无 jobId/attempt/jobVertexID/subtaskIndex**） | channelInfo | `INPUT` | 一次 recovery pass 读回的字节 | 是 | `STRICT` |
| `RECOVER_REWRITE` | `recovery-<handler实例>`（**无 jobId/attempt/jobVertexID/subtaskIndex**） | channelInfo | `INPUT` | 一次 recovery pass 重写后的字节 | 是 | `STRICT` |

> jobId/attempt 在**所有五个阶段**都拿不到——当前分支代码里，从 `InputChannel` 构造器到 `ChannelStateWriteRequest`
> 到 `readInputData`，没有一条路径能不改业务签名就把 jobId/attempt 传到诊断代码。`CHANNEL_RECEIVE`/`SNAPSHOT` 还差
> jobVertexID/subtaskIndex 之外的 jobId/attempt；`RECOVER_READ`/`RECOVER_REWRITE` 连 jobVertexID/subtaskIndex 也拿不到。
> gateIdx/channelIdx（或 output 侧等价物）、单次生命周期分界在五个阶段均已补全。

---

## 3. 当前实现的已知缺陷 / 待修正项

0. **聚合 key 的 jobId/attempt 维度，五个阶段全部拿不到**（HL §6.6 要求的维度里，唯一仍缺失的）。已核实每个阶段能触达的
   代码路径：
   - `CHANNEL_RECEIVE`/`SNAPSHOT`：只能拿到 `stateWriter.taskLabel()`（`jobVertexID-subtaskIndex`）。其实现类
     `ChannelStateWriterImpl` 没有 jobId/attempt 字段，构造器目前只接收 `jobVertexID`/`subtaskIndex`（尽管调用它的
     `SubtaskCheckpointCoordinatorImpl.openChannelStateWriter` 手上有 `Environment`，能拿到 `getJobID()`/`getExecutionId()`）。
   - `CHECKPOINT_WRITE`：`ChannelStateCheckpointWriter`/`writeInput`/`writeOutput`/`completeInput`/`completeOutput` 的方法
     签名和 `ChannelStateWriteRequest` 请求链路都只携带裸 `JobVertexID`/`subtaskIndex`（见 `SubtaskID`），不携带 jobId/attempt。
   - `RECOVER_READ`/`RECOVER_REWRITE`：连 jobVertexID/subtaskIndex 都拿不到，`readInputData(InputGate[], ...)` 只传
     `InputGate[]`，`InputGate`/`InputChannel`/`RecoveredInputChannel` 都没有公开访问器暴露这两个维度（`SingleInputGate`
     只有人类可读的 `owningTaskName`）。
   **未修正**：以上每一处要拿到 jobId/attempt（或 `RECOVER_READ`/`RECOVER_REWRITE` 要拿到 jobVertexID/subtaskIndex）都需要
   改生产构造器/方法签名（`ChannelStateWriterImpl`、`ChannelStateCheckpointWriter`、`ChannelStateWriteRequest`、
   `readInputData`/`RecoveredChannelStateHandler` 构造器之一），按纪律不允许为诊断目的改业务签名，因此留作待办，未强行塞入。
   gateIdx/channelIdx（或 output 侧 partitionIdx/subPartitionIdx）、input/output（`Direction`）、单次生命周期分界这三个维度
   在五个阶段均已用统一的 `key(identity, channel, layer, direction)` 结构补全（见 §2.5）。
   key 缺 jobId/attempt 的直接后果——同名 `jobVertexID-subtaskIndex-channel` 的累积器会跨轮 job 复用同一个 map slot，
   若某轮 job 某 key 只 `append` 未 `flush`（异常退出/取消）就会残留，被下一轮同名 key 的 `append` 接上产生假损坏——
   已用 `ChannelStateInvariant.clearAll()` 规避：`UnalignedCheckpointTestBase.execute(...)` 方法体最开头调用它，
   三个 rescale 子 job（prescale/phase2/phase3，`UnalignedCheckpointRescaleITCase` 里的 `super.execute(...)` 调用点）
   各自启动前都会清空一次，跨轮残留不会污染下一轮的 accumulator。这只解决"残留污染"，不解决"key 本身无法区分 job/attempt"
   这个根本缺陷，故仍保留为待办。

1. **`CHECKPOINT_WRITE`/`SNAPSHOT` 累积器仍按"单个 checkpoint"分片（`SNAPSHOT` 按 `barrierId`，`CHECKPOINT_WRITE` 按
   `-cp<N>`）**，校验的是"单个 checkpoint 的片段"而非"channel 完整数据流"。因为这两处现在都用 `Mode.LENIENT`（容忍首尾半条），
   单 cp 片段天然的首尾半条不会再误报为断言级，只是仍不是理论上最完整的校验对象。**未修正**：`ChannelStateCheckpointWriter`
   按 checkpoint 生命周期建实例（`checkpointId` 是实例字段），结构上看不到同一个 channel 跨多个 checkpoint 的数据，要做到
   "聚合到 channel 完整数据流"需要在更外层（跨 writer 实例）维护累积器，改动会超出这个类本身、触及调用方生命周期管理，本轮
   未做，留作后续单独评估。
2. **output 侧恢复读（`ResultSubpartitionRecoveredStateHandler.recover`）没有任何 `ChannelStateInvariant` 调用**，恢复链在
   output 方向缺一段观测。未修正：不在本轮 §6 规则修正范围内（§6 规则修正聚焦"上下游容忍度区分"和"日志分级"，不要求补齐新的
   观测点）。
3. **真正可靠的信号仍是 `STRIDE-IRREGULAR`**（`shape().summary()` 里的 `*** STRIDE-IRREGULAR ***`）：完整数据中间某条 record
   的 stride 突变，不受首尾半条影响、不受 mode 影响，是当前实现里唯一在任何 mode 下都直接判真损坏的判据。读日志优先只信这个
   + 非 tolerated 的 `CORRUPT-RECORD-AT`。

### 本轮已修正项（不再是待办）

- ~~`shape()` 没有区分上下游、没有容忍首尾半条~~ → 新增 `ChannelStateInvariant.Mode {STRICT, LENIENT}`，`shape(bytes, mode)`
  在 `LENIENT` 下用 `skipToFirstParseableRecord` 跳过开头悬挂字节、在结尾遇到不足一条完整 record 时判定为 `tailTolerated`
  而非 `firstCorruptRecordAt`；中间 record 的 STRIDE-IRREGULAR 在两种 mode 下都判真损坏。
- ~~`writeOutput` 没有任何校验~~ → `ChannelStateCheckpointWriter.writeOutput`/`completeOutput` 补上累积 + `Mode.LENIENT` flush。
- ~~日志不分级，真损坏可能被首尾半条淹没~~ → `logViolationIfAny` 统一按 `shape.toleratedEdgeOnly` 分流：真损坏打
  `[CS-INV-ASSERT]`（`LOG.warn`），仅因 `LENIENT` 容忍的首尾半条打 `[CS-INV-TOLERATED]`（`LOG.info`）。`[CS-INV-SNAP-ASSERT]`
  标签废弃，SNAP 违规现在也走这条统一分级路径。
- ~~五个校验阶段用散落字符串命名（`SNAP`/`WRITE`/`RECOVER`/`REWRITE`/`RECV`），且聚合 key 缺 input/output 结构化维度~~ →
  新增 `ChannelStateInvariant.Layer` 枚举（`SNAPSHOT`/`CHECKPOINT_WRITE`/`RECOVER_READ`/`RECOVER_REWRITE`/`CHANNEL_RECEIVE`，
  与 HL §6.1.5 一一对应）和 `Direction` 枚举（`INPUT`/`OUTPUT`），`key()`/`flush()` 全部改为接收这两个类型化参数，取代旧字符串。

---

## 4. 编译与测试命令

### 4.1 编译（只改 `flink-runtime` 生产代码后）

重新 build 并安装到本地 `~/.m2`：

```bash
cd flink-runtime
../mvnw -T 20 clean install -U -Pfast -DskipTests \
  -Dmaven.javadoc.skip=true -Drat.skip=true -Dcheckstyle.skip=true -Denforcer.skip=true
```

> 约 5 分钟以上。`repro/repro.sh` 假设生产模块已 install 到 `~/.m2`，它只重编 `flink-tests` 的 test-classes。
> 若编译报错且是插桩导致，先修插桩再重编。

### 4.2 跑相关单测（Java 11）

channel-state 写/读相关单测（示例）：

```bash
./mvnw -pl flink-runtime -P java11-target -P java11 \
  -Dtest=ChannelStateCheckpointWriterTest,ChannelStateChunkReaderTest test
```

复现 ITCase（一次性，不用 repro.sh 时）：

```bash
./mvnw -pl flink-tests -P java11-target -P java11 \
  -Dtest='UnalignedCheckpointRescaleITCase#shouldRescaleUnalignedCheckpoint' test
```

> 全局 test 极慢（>30min），只跑与改动相关的 test。测试须由 `flink-test-runner` sub agent 运行。
