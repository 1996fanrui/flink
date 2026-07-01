# Round 1 Findings — 三层完整数据校验：定位到 WRITE 层

## 判定：CONCLUSIVE（层级），WRITE 内部机制仍 INSUFFICIENT → 需 Round 2

数据在 **① 写入层（WRITE，`[Channel state writer]` 线程，`ChannelStateCheckpointWriter`）就已损坏**，是最早、压倒性多数的损坏来源。RECOVER / REWRITE 层不是首发源。

## 本轮插桩

commit `d4fa328`：新建 `ChannelStateInvariant`，对每个 (task, input channel) 的**所有 buffer 拼接后的完整字节流**做 record-framing 校验（length 合法 / tag∈[0,6] / header `AB CD EA FC` 按 stride=21 出现），校验失败只打 `[CS-INV-ASSERT]` 不抛异常。三层：WRITE / RECOVER / REWRITE。

## 复现现场

`FAIL_w8_1.log`（16 worker，PASS=8:FAIL=1；备份 `repro/results-R1-20260701_153511`）。
失败类型 = **数据丢失断言**（非 Corrupt stream）：`[NUM_OUTPUTS = NUM_INPUTS] expected: 387503 but was: 386928`（断言口径丢 575；attempt 内 `numLostValues=253109`）。

## 证据链

1. **分层与时序**：707 条 `[CS-INV-ASSERT]` = WRITE **704** / RECOVER **3** / REWRITE **0**。最早一条在 `FAIL_w8_1.log:3841`，layer=WRITE，`ch=InputChannelInfo{gateIdx=0, inputChannelIdx=15}`。RECOVER 首条在 line 19139，远晚于 WRITE → 先坏的是 WRITE，RECOVER 的少数几条是 WRITE 已损坏数据在恢复端的下游表现。
2. **损坏形态 = 末尾 record 边界截断**（决定性）：`CORRUPT-RECORD-AT == parsedRecords×21` 占 **539/540**。即每条 record 逐条解析都完好、stride 恒 21，损坏一律发生在**最后一条 record 处、因流尾部字节不足而截断**。
   - line 3840 现场：`bytes=500 headers=24 parsedRecords=23 CORRUPT-RECORD-AT=483(=23×21)`；第 24 条 record 声明 length=`00 00 00 11`=17，但流里只剩 13 字节 → 尾部被切。
   - 344 条 NO-HEADER 型对应 `bytes=1/10/…/15`（均 < 一条完整 record 21 字节）→ 同一"尾部截断"现象的极端版。
3. **非误报**：11425 条 healthy WRITE 样本 firstHeaderAt 恒 13、parsedRecords 与 headers 一致、无 corrupt → 校验器语义自洽。
4. **与丢数据关联**：WRITE 层 ASSERT 覆盖 405 个不同 (task, channel)、横跨多个 checkpoint 窗口，与丢失量级/广度一致 → 写 checkpoint 时截断丢字节 → 恢复后 record 缺失 → NUM_OUTPUTS < NUM_INPUTS。

## healthy→corrupt 转变点

WRITE 层内部：同一 channel 完整数据流，前 N 条 record 健康、第 N+1 条（末尾）record 在写入时被截断。无更早层，WRITE 首次变坏。

## 关键代码观察（供 Round 2）

- `ChannelStateCheckpointWriter.writeInput`（`:145`）：校验器 `append` 抓的是 `buffer.getNioBufferReadable()`（append 时刻可读字节，`:153-157`）；实际落盘是 `write()`→`serializer.writeData(dataStream, buffer)`（`:210`）。
- 既然**累积器里的数据（append 时刻各 buffer 拼接）就已末尾截断**，问题指向 **`writeInput` 收到的 buffer 本身末尾已少字节**（或 append/落盘读少了），而非 `write()` 落盘环节本身。

## Round 2 方向（收窄到 WRITE 内部，尚未定位是哪一步截断）

1. 在 `writeInput` 记录传入 buffer 的 `readableBytes()` / readerIndex / 长度，对比实际 append 与 `serializer.writeData` 写入的字节数——确认是"最后一个 buffer 短"还是"少 append 了一个 buffer"。
2. 查 flush 触发时机是否与 checkpoint barrier / buffer 回收（recycle）竞争——截断恒在末尾 record，高度指向"最后一个 buffer 在写完前被 recycle / 可读字节被提前截断"。
3. 对比 `Buffer.readableBytes()` 与实际序列化写入长度，区分 buffer duplicate/readerIndex 处理错误 vs 长度计算错误。
