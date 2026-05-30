# Data Corruption After Read-Path Refactor — 排查计划（基于硬特征 + 插桩）

> 大重构（commit 范围 `8f4a7f36..HEAD`，读路径整体重写）后，rescale 路径重新出现下游反序列化数据破坏。
> 本文档不再罗列泛泛的猜测；聚焦**已知硬特征**与**已被验证有效的定位方法论**，给出可执行的排查计划。
> 尚未定论；不要据此直接改代码。

## 0. 观测现象与硬特征（事实）

复现率极低：跑几十次测试（每次 50 个 job，累计上千 job）才 2-3 个 job 出问题。

观测到 **4 次失败**，均在最新代码、`UnalignedCheckpointRescaleITCase.shouldRescaleUnalignedCheckpoint`（rescale/downscale）上，表现为两类异常：

- `Stream corrupted. Cannot find the header ...`（`UnalignedCheckpointTestBase.checkHeader`）
- `Can't get next record for channel ...` → `EOFException`（`StreamElementSerializer.deserialize` → `readByte` 读不到字节）

**测试数据结构（事实）**：record 是单个 8 字节 `long`（`UnalignedCheckpointTestBase`：`HEADER = 0xABCDEAFC << 32`，高 4 字节固定 header `ABCDEAFC`，低 4 字节递增 value）。filter 重写时按 `[4 字节 length 前缀][record 字节]` 写入。

**4 次失败的现场（事实，来自 `log/`）**：

| log | corrupt value | 按字节展开 | EOF channel |
|---|---|---|---|
| `20260530_210829` | `cdeafc00090c6500` | `CD EA FC 00 09 0C 65 00` | — |
| `20260530_211559` | `cdeafc00084d5500` | `CD EA FC 00 08 4D 55 00` | gate0/ch0, ch1, ch3 |
| `20260530_211837` | `cdeafc000559d000` | `CD EA FC 00 05 59 D0 00` | — |
| `20260530_214530` | （仅 EOF，无 value） | — | gate0/ch1 |
| （首次报告样本） | `cdeafc000559d000` | 同 211837 | gate0/ch1 (keyby) |

每次失败还伴随 `IllegalArgumentException: Checkpoint needs at least one vertex that commits the checkpoint`（JM 端，疑似 corruption 导致 task FAILED → graph 重启的 cascade，非独立 bug）。

**硬特征（关键，4 次完全一致，决定排查方向）**：
3 个有 value 的样本**全部以 `CD EA FC 00` 开头**。正确 record 8 字节 = `AB CD EA FC | VV VV VV VV`（4 字节 header + 4 字节 value）。读出来变成 `CD EA FC 00 | xx xx xx 00`——即读取窗口**整体向后挪了 1 字节**：丢掉了最高字节 `AB`，从这条 record 的第 2 字节（`CD`）开始当第 1 字节读起，尾部多吸入后面 1 字节。

→ **corruption 是「从某一点起，字节流整体错位 1 字节」，且方向固定（少 1 字节 / 窗口后移 1）**，4 次稳定一致。说明在出错 record **之前**，数据流某处**少了 1 个字节**（或前一条被多读了 1 字节，吃掉了这条的 `AB`）。这是极强的约束——不是大段错位、不是乱序、不是随机。这排除了一批假设（见 §2），并把范围收窄到「**某一步精确地多/少 1 字节**」。

## 1. 已被验证有效的定位方法论（来自本 issue 之前的排查，可复用）

之前几轮疑难（hang、`Missing RecoveryCheckpointBarrier`、`read buffer is null`）的根因**都不是靠读代码猜出来的，而是靠运行时插桩在复现瞬间抓现场**：

- `hang_evidence.md` §5：在 `RecoveredBufferQueue.offer/finish` 入口加临时 `LOG.info`，仅本次重现用。
- `recovery_in_recovery_flag_unification.md` §1：`[HANG-DIAG]` 日志**直接定位**抛错瞬间的 channel 状态（`allDelivered=true, buffers=[#0:EVENT(...)]`），看到不变式被破坏的现场。
- `read_buffer_null_credit_fix.md` §1：heap dump + 运行时日志实测确认「池空、非泄漏」。

**结论**：对千分之二的低概率问题，读代码只能列嫌疑、抓不到那 0.2% 的现场。必须插桩 + 跑到复现 + 从日志还原字节来源链。符合 CLAUDE.md「未确认根因前不改代码；假设必须实验验证」。

## 2. 硬特征排除的假设（±1 字节平移 ⇒ 不是这些）

- **批次顺序错（recovery 数据 vs live data 谁先）**：即使顺序乱，每条 record 字节完整、length 前缀完整，deserializer 逐条解析不会错位。要 ±1 平移必须是**单条 record 内部边界被破坏**或**流里凭空多/少 1 字节**。→ 排除「recovery/live 交错」类。
- **整段 position 算错（读错 segment / offset 偏很多）**：会错很多字节、或读到完全无关内容，不是稳定的 ±1。→ 大段错位类降权。
- **channel 串味（A 的字节送到 B）**：会让整条 record 进错 deserializer，但每条仍完整，不产生 ±1 平移。→ 降权。

→ 真正要找的是：**数据链路上某一步，对某段连续字节多写/少写、或多读/少读了 1 个字节（length 前缀算错、短 buffer 边界、event 字节混入 data 流等）。**

## 3. 基于「±1 字节」的精确嫌疑点（待插桩验证，非定论）

按「哪一步可能差 1 字节」逐一列出，标注插桩点：

### 嫌疑 A0 ⭐ — `GateFilterHandler` 的共享可变状态在 channel 间复用（最契合「多 channel 同时 ±1」）
**位置**：`ChannelStateFilteringHandler.GateFilterHandler`（:277-292）。
- 一个 gate 一个 `GateFilterHandler`；该 handler 的 `outputSerializer`（`DataOutputSerializer`）、`lengthBuffer[4]`、`deserializationDelegate` 被**该 gate 下所有 channel、所有 record 复用**（filter 按 old channel 分 `VirtualChannel`，但 re-serialize 用的是 gate 级共享的这几个对象）。
- **为何契合**：`20260530_211559` 一次 3 个 channel（ch0/ch1/ch3）同时 EOF —— 指向一个**所有 channel 共享**的组件出错，而非单 channel 偶发。±1 字节恰好能由共享 `outputSerializer`/`lengthBuffer` 在某次 channel 切换或 buffer-full 边界未干净复位产生。
- **怀疑**：`outputSerializer.clear()` 是否每条 record 前都调用且彻底；`lengthBuffer` 写入与读取是否始终 4 字节对齐；跨 channel 切换（`requestBufferBlocking` 触发 flush）时，`serializeElement` 正在进行的「length 已写、data 未写完」状态是否可能跨 flush 边界错位 1 字节。
- **插桩**：在 `serializeElement` 记录 `(newChannelInfo, recordLength, outputSerializer.length())`，在 `writeDataToBuffer` 每次换 buffer 记录 `(已写 offset, remaining, 目标 buffer 切换前后 size)`。

### 嫌疑 A — filter 重写 record 时 length 前缀与 record 字节的写入边界
**位置**：`ChannelStateFilteringHandler.serializeElement` / `writeDataToBuffer`（:359-436）。
- `serializeElement` 先 `writeLengthToBuffer(recordLength)`（4 字节）再写 `serializedData[0..recordLength]`。
- `writeDataToBuffer` 在 buffer 满时 `requestBufferBlocking` 换 buffer 继续写。
- **怀疑**：`recordLength = outputSerializer.length()` 与实际写出的 `serializedData` 字节数是否始终一致；buffer-full 边界换 buffer 时 `offset/remaining` 推进是否可能差 1。若 length 前缀写的值与后面实际字节数差 1，下游按 length 读就整体平移。
- **插桩**：在 `serializeElement` 出口记录 `(newChannelInfo, recordLength, 实际写入字节累计)`；在 `flush` 记录 `(currentChannel, payload.remaining())`。

### 嫌疑 B — `SpillFile.append` 写入字节数 vs Entry 记录的 length
**位置**：`SpillFile.append`（:164-194）。
- `length = payload.remaining()`，`offsetBeforeWrite = active.currentEnd`，写完 `currentEnd += length`，Entry 记 `(offsetBeforeWrite, length)`。
- **怀疑**：write 循环 `while (written < length)` 是否在某种 short write 下把 payload position 推过头/不足，导致下一条 entry 的 `offsetBeforeWrite` 偏 1。
- **插桩**：append 出口记录 `(channelInfo, offsetBeforeWrite, length, active.currentEnd, segmentIndex)`。

### 嫌疑 C — `SpillFileReader` 顺序读：`readFully` 推进的 position 与下一 entry offset 不符
**位置**：`SpillFileReader.peek` / `ensureActiveChannelFor` / `readFully`（:131-284）。
- 只在 `activeChannel==null` 时 `position(e.offset)`，之后靠 `read(view)` 顺序前进；假设「entry 物理连续」。
- **怀疑**：若某 entry 读完后文件 position ≠ 下一 entry 的 `e.offset`（哪怕差 1），后续全错位。低概率竞争不易解释确定性读，但**可在插桩里直接验证 `activeChannel.position()` 是否 == 下一个 e.offset**。
- **插桩**：`peek` 里 `read` 前后记录 `(segmentIndex, entryCursor, e.offset, e.length, activeChannel.position() before/after)`，断言 `before == e.offset`。

### 嫌疑 D — 一个 event / 短 buffer 的字节混入了某 channel 的 data deserializer 流
**位置**：消费侧 `AbstractStreamTaskNetworkInput.processBuffer`（:295-304）+ 各 channel `getNextBuffer` 的 DataType 判定。
- deserializer 按 channel 累积；只有 `isBuffer()==true` 才喂。
- **怀疑**：recovery 期间是否存在某个 buffer，其 DataType 被判成 data（isBuffer=true）但内容其实含 event/控制字节，或一个本应整段的 data buffer 被切了 1 字节。±1 也可能来自「某 buffer size 比真实数据多/少 1」。
- **插桩**：在 `processBuffer` 记录 `(channelInfo, buffer.getDataType(), buffer.getSize(), sequenceNumber)`；在出错 channel 的 deserializer 喂入处 dump 前若干字节。

## 4. 执行计划

1. **加临时插桩**（仅诊断、本次重现用，事后删除）：覆盖嫌疑 A/B/C/D 的打点，每条都带 `channelInfo` + 关键 offset/length/size，便于按出错 channel `gateIdx/inputChannelIdx` 过滤。
2. **跑复现**：用现有 loop 脚本反复跑 `UnalignedCheckpointRescaleITCase`，直到命中（千分之二，需多跑）。
3. **抓现场**：复现后，从日志按出错 channel 还原「该 channel 的 entry 序列 / 字节累计 / 读出 position」，定位是哪一步开始 ±1。
4. 锁定根因后再改代码 + 补**逐字节比对**的回归测试。

## 5. 测试盲区（事实，需补）

`SpillFileReaderConcurrencyTest` 用 8 字节 payload、只 decode 前 4 字节做 id 去重，**从不逐字节比对 entry 内容**。读路径若产生 ±1 字节错位，该测试无法发现——这解释了 bug 为何能通过现有单测进入主干。回归测试必须逐字节比对。
