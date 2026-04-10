# Design Review Issues — FLINK-38544

## Issue 1: Checkpoint Support Required

**Problem**: Filtering only happens when checkpointing-during-recovery is enabled. So checkpoint WILL trigger while disk has unreplayed spill data. Current design has no checkpoint support.

**Impact**: Spill data not included in checkpoint → data loss on failover during recovery.

**Key constraint**: Checkpoint snapshot requires data segmented into individual buffers (buffer-sized chunks), not arbitrary-sized entries. Each snapshot entry must be bounded by buffer size.

**Action needed**:
- Add checkpoint snapshot support to OutputWriter
- When checkpoint triggers, iterate unreplayed SpillEntry queue, read buffer-sized chunks from disk, write to checkpoint storage
- Reference old design's approach: `readNextTo(OutputStream, int)` for streaming disk data directly to checkpoint storage without loading into Network Buffer
- Update user_requirements.md, acceptance_test.md, commit_plan.md

**Open questions**:
- Does OutputWriter provide the checkpoint iterator, or a separate component?
- How to handle concurrent checkpoint read + replay write on the same file? (read cursor for checkpoint vs read cursor for replay)

---

## Issue 2: SpillEntry Granularity — ✅ 已解决

**Problem**: SpillEntry length is ambiguous. One channel may produce 100KB+ of data. A single SpillEntry with length=100KB cannot be replayed into one 32KB Network Buffer.

**Resolution**: SpillEntry 与 Network Buffer 1:1 对应。最大 length = memorySegmentSize（来自 Flink 配置，非硬编码 32KB）。多次 write() 累积到同一个活跃 SpillEntry，满或 channel 变更时密封。已在 design.md、data_flow.md、commit_plan.md、user_requirements.md、acceptance_test.md 中同步更新。

---

## Issue 3: OutputWriter Constructor Parameters

**Problem**: Constructor takes single `RecoveredInputChannel`, but OutputWriter is per-gate and needs:
- Buffer request from pool (any channel works, shared pool)
- Deliver replayed data to the **correct target channel** (old→new channel mapping)

**Resolution**: Constructor should receive:
- `InputGate` (or channel array) for buffer requests and delivery routing
- Channel mapping (`InflightDataRescalingDescriptor` or equivalent) for old→new channel resolution
- `String[] spillDirs`, `int gateIndex`
- Buffer size from config

**Action needed**:
- commit_plan.md: update OutputWriter constructor signature

---

## Issue 4: OutputWriter.write() Interface

**Problem**: `write(byte[], offset, length)` is wrong. The write interface should include channel info for delivery routing.

**Resolution**: `write(byte[] data, int offset, int length, InputChannelInfo channelInfo)`. OutputWriter uses channelInfo to:
- Auto-detect channel change → flush current backend
- Tag SpillEntry with channelInfo for correct replay delivery
- Resolve target channel via mapping when delivering to InputChannel

**Action needed**:
- commit_plan.md: update write() signature
- Already partially correct in commit_plan.md (has oldSubtaskIndex/oldChannelIndex), but should use InputChannelInfo or equivalent

---

## Issue 5: filterAndRewrite Interface Bridging

**Problem**: Current `filterAndRewrite` internally uses `writeDataToBuffer(byte[], offset, length, Buffer, List<Buffer>, BufferSupplier)`. Changing to OutputWriter means:
- `serializeElement` produces `byte[]` via `outputSerializer.getSharedBuffer()` — this can be written directly to OutputWriter
- `writeDataToBuffer` buffer management logic (full → request new) is replaced by OutputWriter's writeToBackend
- The length prefix (4 bytes) also needs to go through OutputWriter

**Resolution**: Replace `BufferSupplier` + `writeDataToBuffer` with OutputWriter. The `serializeElement` method writes length prefix + record bytes directly to `writer.write()`. OutputWriter handles buffer/file switching internally.

**Action needed**:
- commit_plan.md Commit 6: detail how `writeDataToBuffer` is replaced
- Verify `serializeElement` can work with OutputWriter without other changes

---

## Issue 6: Minor Corrections

**6a**: Design Principle 3 — "32KB chunks" should be "buffer-sized chunks (configured, not hardcoded 32KB)"

**6b**: user_requirements.md `需求偏离` says "无" — REQ-NHLB deviates from original requirement (original had no Heap Buffer concept). Should document the deviation.

**Action needed**: Fix in respective documents.
