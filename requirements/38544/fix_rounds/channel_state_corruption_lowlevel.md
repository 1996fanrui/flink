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
| `append(String key, ByteBuffer buffer)` | `:86` | 把 buffer 的可读字节**拷贝**进 `key` 对应的累积器；用 `duplicate()`，**不动** reader index / refcount |
| `flush(String key, String taskAndChannel, String layer)` | `:104` | 取出并移除 `key` 的累积字节，对**拼接后的完整字节流**跑 `shape()`，打 `[CS-INV] layer=<L>`；`!valid` 再打 `[CS-INV-ASSERT] layer=<L>` |
| `key(String task, String channel, String layer)` | `:134` | 组 accumulator key = `task \| channel \| layer` |
| `label(String task, String channel)` | `:139` | 组人类可读标签 = `task=<..> ch=<..>` |
| `validateSnapshot(String taskAndChannel, long barrierId, List<Buffer> buffers)` | `:151` | 快照专用：**自己 retain 一份**副本、拼接、`shape()`，打 `[CS-INV-SNAP]`；`!valid` 打 `[CS-INV-SNAP-ASSERT]`；`finally` 里 recycle 自己的副本，不影响 caller 的 buffer |
| `shape(byte[] bytes)` | `:200` | 核心判据，见下 |

### 日志标签（当前实现）

| 标签 | 出处 | 含义 |
|---|---|---|
| `[CS-INV]` | `flush` | 一个 key 的完整拼接数据形状（每次 flush 一行） |
| `[CS-INV-ASSERT]` | `flush` | 该完整数据 `shape().valid==false`（被判 INVALID） |
| `[CS-INV-SNAP]` | `validateSnapshot` | 快照（一个 barrier 收集到的 buffer 集）的形状 |
| `[CS-INV-SNAP-ASSERT]` | `validateSnapshot` | 快照被判 INVALID |
| `[CS-INV-REC]` | `flush`（仅 `LOG_RECORDS=true`）| 逐条 record（at/len/tag） |

> 注意：HL 旧稿提到的 `[CS-INV-CORRUPT]` / `[CS-INV-REC]` 逐 buffer 抛错瞬间日志、以及 `filter.IN/OUT`、`readChunk.IN`、
> `recover.INJECT` 这类 per-buffer 阶段标签，**当前分支代码里并不存在**。当前实现走的是"完整数据累积 + flush 校验"路线，
> 标签只有上面这几个。读日志时以本表为准。

### `shape()` 的判据（约 `:200`–`:226`）

对一段拼接好的 `byte[]`：
1. `findHeaderOffsets`：扫出所有 `AB CD EA FC` 出现的偏移。
2. 相邻偏移差 = stride；只要有两个 stride 不相等 → `strideIrregular=true`，`summary()` 里打 `*** STRIDE-IRREGULAR ***`。
3. `walkFraming`：从 `pos=0` 起按 `[4B big-endian length][payload]` 逐条走；每条要求 `length>0 && pos+4+length<=end` 且
   `payload[0]` 落在 `[MIN_TAG,MAX_TAG]`；第一处违规记 `firstCorruptRecordAt` 并停（不 resync），dump 前后各 16 字节 hex。
4. `valid = !strideIrregular && headerCount>0 && firstCorruptRecordAt<0`。
5. `summary()`：`headerCount==0` 打 `NO-HEADER`；否则打 `headers=N firstHeaderAt=.. strides=[..]`（+ 可能的 STRIDE-IRREGULAR）
   + `parsedRecords=..` + 可能的 `*** CORRUPT-RECORD-AT=.. before=[hex] after=[hex] ***`。

**判据缺陷见 §3，务必先读再据此下结论。**

---

## 2. 当前已加的校验点（读代码核实，落在 commit d4fa328 / bfd05a8 / 1075f95）

所有 call site 都先 `ChannelStateInvariant.isEnabled()` 再动作。累积型（`append`+`flush`）和快照型（`validateSnapshot`）两类。

### 2.1 接收层 RECV（恢复迁入物理 channel）

| 类（相对路径） | 方法 | 约行 | key / label | layer |
|---|---|---|---|---|
| `.../io/network/partition/consumer/LocalInputChannel.java` | 构造器里 `initialRecoveredBuffers` 迁移循环 | append `:135`，flush `:152`（循环 `:132`–`:156`）| key=`(taskLabel, channelInfo+" kind=Local", "RECV")`；task=`stateWriter.taskLabel()` | `RECV` |
| `.../io/network/partition/consumer/RemoteInputChannel.java` | 构造器里 `initialRecoveredBuffers` 迁移循环 | append `:178`，flush `:194`（循环 `:169`–`:198`）| key=`(taskLabel, channelInfo+" kind=Remote", "RECV")` | `RECV` |

- 只对 `buffer.isBuffer()`（跳过 event）累积；用 `getNioBufferReadable()`，不动 reader index。
- **key 按 `taskLabel`（=物理 channel 所在 task）分组**，一个物理 channel 的迁入 buffer 全部拼在一起后 flush 一次 → 这正是"物理 channel 接收侧完整数据"。

### 2.2 快照层 SNAP（正常运行时 checkpoint 采样在途数据）

| 类 | 方法 | 约行 | 说明 |
|---|---|---|---|
| `LocalInputChannel.java` | `checkpointStarted(CheckpointBarrier)`（`:163`）| `validateSnapshot` 调用 `:173` | 收集 `toBeConsumedBuffers` 里的 in-flight buffer，`validateSnapshot` 在锁外直接跑（Local 无并发锁问题）|
| `RemoteInputChannel.java` | `checkpointStarted(CheckpointBarrier)`（`:729`）| `validateSnapshot` 调用 `:761` | **锁外校验**：在 `synchronized (receivedBuffers)`（`:731`）内对 `getInflightBuffersUnsafe` 结果**逐个 `retainBuffer()` 存进 `invariantSnapshot`**（`:753`–`:756`），出锁后（`:760`）再 `validateSnapshot`，跑完 `:766`–`:768` 逐个 `recycleBuffer()`。这样校验的拷贝/日志不占锁、也不碰交给 writer 的那批 buffer 的 refcount。|

- SNAP 标签是 `[CS-INV-SNAP]`，**不走 `flush`/`key`**（`validateSnapshot` 内部自建拼接）；其分组粒度=**单个 barrier 的一次快照**（参数 `barrierId`），即"这一个 checkpoint 里这个 channel 采到的 in-flight 片段"，**不是** channel 完整数据流。

### 2.3 checkpoint 写入层 WRITE

| 类 | 方法 | 约行 | key / 分组 | layer |
|---|---|---|---|---|
| `.../checkpoint/channel/ChannelStateCheckpointWriter.java` | `writeInput(...)`（`:145`）| append `:154`；flush 在 `completeInput(...)`（`:219`）里 `:230` | `invariantWriteKey`（`:174`）= key(`jobVertexID-subtaskIndex-cp<checkpointId>`, `info.toString()`, `"WRITE"`) | `WRITE` |
| 同上 | `writeOutput(...)`（`:180`）| **当前无插桩** | —— | —— |

- **重要事实（与 HL 旧稿不符，以代码为准）**：当前只有 **`writeInput`（input / 下游语义）有 WRITE 校验**；
  `writeOutput`（output / 上游语义）**没有任何 `ChannelStateInvariant` 调用**。所以"WRITE 层区分上下游"目前**未落地**（见 §3 待修正项）。
- **WRITE 累积 key 带 `-cp<checkpointId>`** → 按**单个 checkpoint** 分片：flush 出来的是"这一个 checkpoint 里这个 input channel 写了哪些字节"，
  **不是** channel 完整数据流（这是误报源之一，见 §3）。flush 在 `completeInput` 里遍历该 pending result 的所有 input channel 逐个 flush。

### 2.4 恢复读 RECOVER / 重写 REWRITE

| 类 | 方法 | 约行 | key / 分组 | layer |
|---|---|---|---|---|
| `.../checkpoint/channel/RecoveredChannelStateHandler.java`（内部类 `InputChannelRecoveredStateHandler`）| `recover(InputChannelInfo, int, BufferWithContext)`（`:109`）| append `:122` | `invariantRecoverKey`（`:151`）= key(`recovery-<identityHashCode(this)>`, `channelInfo`, `"RECOVER"`) | `RECOVER` |
| 同上 | `recoverWithFiltering(...)`（`:166`）| append `:185`（对 `filterAndRewrite` 返回的每个 filtered buffer）| `invariantRewriteKey`（`:161`）= key(`recovery-<identityHashCode(this)>`, `channelInfo`, `"REWRITE"`) | `REWRITE` |
| 同上 | `close()`（`:205`）| flush `:212`（RECOVER）+ `:213`（REWRITE）| 遍历 `channelsSeenForInvariantCheck` 逐 channel flush | —— |

- **RECOVER/REWRITE key 用 `recovery-<identityHashCode(this)>`**，即**每个 recovery pass（一次 `readInputData` 一个 handler 实例）一组**，
  在 `close()` 时对该 pass 见过的每个 channel 一次性 flush → 这**是** channel 完整数据流粒度（与 WRITE 的 per-cp 分片不同，注意区分）。
- RECOVER 累积的是恢复读回的原始字节（`recover` 里注入前）；REWRITE 累积的是 `filterAndRewrite` 重写后、即将 `onRecoveredStateBuffer` 注入物理 channel 的字节。
- **output 侧恢复无插桩**：同文件里 output 的 `RecoveredChannelStateHandler.recover(ResultSubpartitionInfo, ...)`（`:289`）**没有** `ChannelStateInvariant` 调用。

### 2.5 各层 key 分组一览（重点：分组粒度决定校验对象）

| layer | 分组维度 | 校验对象 | 是否 = channel 完整数据流 |
|---|---|---|---|
| `RECV` | `taskLabel + channel` | 物理 channel 迁入的全部 buffer | 是 |
| `SNAP` | 单个 `barrierId` | 一次 checkpoint 采到的 in-flight 片段 | **否**（单 cp 片段） |
| `WRITE` | `jobVertexID-subtask-cp<N> + channel` | 单个 checkpoint 写入的 input 字节 | **否**（单 cp 片段） |
| `RECOVER` | `recovery-<handler实例> + channel` | 一次 recovery pass 读回的字节 | 是 |
| `REWRITE` | `recovery-<handler实例> + channel` | 一次 recovery pass 重写后的字节 | 是 |

---

## 3. 当前实现的已知缺陷（供后续修正，非已完成）

基于排查结论，当前 `ChannelStateInvariant` 与 call site 有以下缺陷，会产生大量**假阳性 ASSERT**，读日志时必须知道：

1. **`shape()` 要求"从第 0 字节整段自包含"，没有区分上下游、没有容忍首尾半条。**
   `walkFraming` 从 `pos=0` 硬走 framing、`headerCount>0` 才算 valid。对**上游 output / 快照 in-flight 片段**（业务上本就可能首尾半条），
   会把 `NO-HEADER`、中间 record 起头（第一条不是从 record 边界开始）、尾部截断统统判成 `INVALID`。这与 HL §6.2 的通用规则冲突。

2. **`WRITE`/`SNAP` 累积器按"单个 checkpoint"分片（`SNAP` 按 `barrierId`，`WRITE` 按 `-cp<N>`），校验的是"单个 checkpoint 的片段"而非"channel 完整数据流"。**
   单 cp 片段天然可能首尾半条 → 又一个假阳性来源。要校验"完整数据不容忍半条"，聚合粒度必须是 channel 完整数据流（参考 `RECV`/`RECOVER`/`REWRITE` 的分组）。

3. **真正可靠的信号是 `STRIDE-IRREGULAR`**（`shape().summary()` 里的 `*** STRIDE-IRREGULAR ***`）：完整数据中间某条 record 的 stride 突变，
   不受首尾半条影响，是当前实现里唯一穿透噪声的判据。读日志优先只信这个 + 中间 record 的 `CORRUPT-RECORD-AT`。

### 待修正项（按 HL §6 通用规则的正确改法，尚未落地）

- **下游 / 恢复链（`RECV` / `RECOVER` / `REWRITE`，以及 WRITE 的 input 语义）**：不容忍半条，违规打断言级。
- **上游 checkpoint-output（`writeOutput`，当前缺失）**：需补上 output 侧校验，且**容忍首尾半条**，只校验中间 record 等间距连续，
  首尾半条打普通/观测级、不算 ASSERT。
- **日志分级**：断言级（真损坏）与观测级（上游可容忍半条）用不同标签/级别，避免真损坏被淹没。
- **聚合粒度**：`WRITE`/`SNAP` 改为按 channel 完整数据流聚合（而非单 cp 分片），或明确把单 cp 片段当"可首尾半条"处理。

> 以上是**待修正项**，当前代码尚未实现。修复本身另起流程，不在插桩范围。

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
