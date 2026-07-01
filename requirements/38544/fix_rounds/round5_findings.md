# Round 5 — Design question settled: boundary (off-by-N) vs unsound-splice

## Question

For one physical channel, downstream recovery feeds a per-channel spanning record
deserializer, in delivery order: **segment A** (the downstream's own input-channel-state)
then **segment B** (the upstream's unsent OUTPUT buffers, re-typed as input by the
output-to-downstream distribution). B corrupts. Decide:

- **(boundary)** B is the logical continuation of A, corruption is a byte-level seam
  off-by-N → fix = align the seam; OR
- **(unsound-splice)** B is not a splice-contiguous continuation of A (its leading partial
  record's head is not in A), so record-deserializing B on A's deserializer is
  fundamentally unsound → fix = do not record-deserialize the re-typed output buffers.

## Verdict: **unsound-splice.**

The seam data proves **A ends exactly on a record boundary (no spanning remainder) while
B begins in the middle of a record.** There is therefore no byte-level "seam" to align:
B's leading partial record has no head in A, and no head in the deserializer's spanning
state (A left it empty). Feeding B through A's spanning deserializer is unsound
regardless of alignment. This refutes (boundary).

---

## 1. Counter sanity — per-channel record counters ARE non-monotonic; counter continuity is meaningless

The `values` field is the low 4 bytes (`CCCC`) after each `AB CD EA FC` marker — the
source-assigned value carried by each record. The job keys/rebalances source values across
channels, so a single physical channel sees a **scattered subset**, not a contiguous run.

Proof from HEALTHY (non-failing) channels in the same log — values are non-monotonic even
**within a single buffer**:

- `drain.OUT ch=InputChannelInfo{inputChannelIdx=0}` (round5_FAIL.log:66699, healthy):
  `values=[first: 121590,122010,122430 ... last: 112927,113347,113767] n=195` — the LAST
  value (112927) is **smaller** than the FIRST (121590). Monotonic streams cannot do this.
- Healthy `inputChannelIdx=29` segments (round5_FAIL.log:75121, 75528, 77909, 79458):
  e.g. `values=[first: 55661,56101,119540 ... last: 55821]`,
  `values=[first: 46741,105120,69140 ... last: 38037,55401]` — values jump up and down
  arbitrarily within one segment.

**Conclusion: counter continuity CANNOT be used as a contiguity signal.** The
non-monotonic `CCCC` counters in the `[CS-INV-SEAM]` line (A last `134291,210060,207716`,
B first `119534,100919,215926`) prove nothing about whether A and B are contiguous — that
is exactly what a healthy channel looks like too. **The only valid contiguity signal is the
record-framing seam: `prevTrailingPartial` (does A leave a spanning remainder?) and
`curFirstHeaderAt` (does B start on a frame boundary?).**

## 2. Seam interpretation — A clean, B mid-record

Frame layout (instrumentation doc, round5): each record is `[4B length=0x11=17][17B payload]`,
stride 21, with marker `AB CD EA FC` at `frameStart+10`. A **record-aligned** buffer that
starts at a frame boundary shows `firstHeaderAt=13` (the drainer's 3-byte segment preamble +
the first frame's `4B len` at +3, marker at +13). `firstHeaderAt=2` means the buffer's first
frame boundary is not at the start — the first ~11 bytes are the **tail of a prior record**.

Decisive line, failing `InputChannelInfo{gateIdx=0, inputChannelIdx=29}`, task
`failing-map (12/20)#1` (round5_FAIL.log:107766, `[CS-INV-SEAM]`):

- **Segment A** — `len=252 firstHeaderAt=13`, n=12, delivered first (drain.OUT
  round5_FAIL.log:107762). `prevTrailingPartial=0`. `prevTail` ends
  `... AB CD EA FC 00 03 2B 64` = a **complete** `[len][marker][value]` record. **A ends
  exactly on a record boundary and leaves NO partial record in the deserializer's spanning
  state.**
- **Segment B** — `len=1018 firstHeaderAt=2`, n=49, delivered second (drain.OUT
  round5_FAIL.log:107764). `curHead = D2 EE | AB CD EA FC 00 01 D2 EE | 00 00 00 11 ...`:
  the first 2 bytes `D2 EE` are the **tail of a record** (the low 2 bytes of a value whose
  marker/head are NOT in B), then a marker at offset 2, then the next clean `00 00 00 11`
  length at offset 8. **B begins mid-record.**
- Result: `[CS-INV-CORRUPT] recordsOkInThisBuffer=0` (round5_FAIL.log:107762) — the
  deserializer chokes on B's very first bytes.

B's orphaned leading ~11 bytes are the **tail of a record whose head was already
sent/consumed by the downstream and thus was never snapshotted into any recovered segment.**
B is the producer's *unsent output* in-flight buffer; the record that straddled the
subpartition-buffer boundary had its head in an earlier output buffer that was already
transmitted (or in the producer's serializer), so it is not present in the snapshot. The
first buffer of a snapshotted ResultSubpartition is allowed to start mid-record by design —
in normal operation the network stack re-spans it against the buffer the consumer received
live. There is no head for it in A, because A is a completely different byte stream (the
downstream's own input-channel-state), not the producer's output prefix.

## 3. Code confirmation — how B reaches A's deserializer, and that no boundary/seed is carried

- **Re-typing (write→read identity collision).**
  `TaskStateAssignment.distributeOutputBufferToDownstream`
  (`flink-runtime/src/main/java/org/apache/flink/runtime/checkpoint/TaskStateAssignment.java:600-636`):
  a `ResultSubpartitionStateHandle` (upstream output) is wrapped into an
  `InputChannelStateHandle` with
  `inputChannelInfo = new InputChannelInfo(gateIdxResultPartition, oldUpstreamSubtaskIndex)`
  (line 621-622) and `subtaskIndex = oldDownstreamSubtaskIndex` (line 626). So the upstream's
  output for subpartition *s* is presented to the downstream as input-channel-state on
  channel `oldUpstreamSubtaskIndex`. It carries **only** delegate + offsets + size — **no
  record-boundary / first-record-offset / spanning-seed metadata** (line 624-630).

- **Two reads into ONE handler / ONE per-channel spanning deserializer.**
  `SequentialChannelStateReaderImpl.readInputData`
  (`.../checkpoint/channel/SequentialChannelStateReaderImpl.java:88-100`) calls `read(...)`
  **twice against the same `stateHandler`**: first
  `ChannelStateHelper::extractUnmergedInputHandles` (segment A, genuine input-channel-state),
  then `OperatorSubtaskState::getUpstreamOutputBufferState` (segment B, the re-typed output).
  Both funnel through `readSequentially` → `ChannelStateChunkReader.readChunk`
  (`SequentialChannelStateReaderImpl.java:255-296`) → `stateHandler.recover(...)`. Nothing in
  this path establishes or records a record boundary; `readChunk` just copies opaque bytes
  into network buffers by length prefix (line 266-275). A and B for one physical channel key
  to the same mapped `InputChannelInfo`, so they are spilled into one per-channel segment
  stream and consumed by one deserializer.

- **Drain preserves opaque byte order, no framing.**
  `FetchedChannelStateDrainer.drainSegment`
  (`.../checkpoint/channel/FetchedChannelStateDrainer.java:122-159`) fills fixed-capacity
  buffers from `seg.bodyStream()` purely by byte count and delivers them via
  `ch.onRecoveredStateBuffer(buf)`. No record awareness.

- **Consumer: one spanning deserializer per `InputChannelInfo`.**
  `AbstractStreamTaskNetworkInput`
  (`.../streaming/runtime/io/AbstractStreamTaskNetworkInput.java`): `getActiveSerializer`
  (line 385-386) returns `recordDeserializers.get(channelInfo)` — a single
  `SpillingAdaptiveSpanningRecordDeserializer` per `InputChannelInfo` (field line 75). It
  carries partial-record spanning state across buffers, so A's trailing state (here: empty)
  and then B are fed to the same instance. The failure throws from `emitNext`'s
  `catch (IOException)` around `getNextRecord`.

**Nowhere is a record boundary or spanning seed established for the re-typed output buffers,
and a ResultSubpartition's first snapshotted in-flight buffer is allowed to start mid-record
by design.** Hence B on A's deserializer is fundamentally unsound.

## 4. Reconciliation with Round 4 and the common invariant

Round 4 (a different repro instance, `inputChannelIdx=20`, task `failing-map (4/21)#0`) saw
**A end mid-record** (A's final frame incomplete) and B also start mid-record — there, A's
dangling partial got glued onto B's tail (`18 + 8 ≠ 21`). Round 5 shows **A end clean**
(`prevTrailingPartial=0`) and B still start mid-record (`curFirstHeaderAt=2`).

These are the two faces of the same defect. The two possibilities differ only in whether A
happened to leave a remainder:

- **Round 4:** A's remainder (wrong stream) + B's head-less tail → mis-frame.
- **Round 5:** A leaves no remainder, so the deserializer starts B expecting a fresh
  frame at B's byte 0 — but B's byte 0 is 11 bytes into a record → mis-frame on B's first
  bytes.

**Common invariant: B (re-typed upstream output) begins mid-record and its head is not
reliably present in A or in the deserializer's spanning state.** Whether A ends clean or
dirty, splicing B onto A's per-channel spanning deserializer is unsound. This is *not* an
off-by-N seam that any fixed byte alignment could repair — in Round 5 there is no seam
remainder at all, yet B still cannot be parsed. **(boundary) is definitively refuted.**

## 5. Fix

**Do not record-deserialize the re-typed upstream-output buffers on the shared per-channel
deserializer.** Restore the pre-38542 invariant: upstream unsent output in-flight buffers
must be replayed as **raw `Buffer`s** into the input channel, so the live consumer
deserializer re-spans them against buffers it received on the wire — never mixed with the
distinct input-channel-state stream in the filter/spill/consume record pipeline.

Precise change — sever the second read pass from the record-deserializing input pipeline in
`SequentialChannelStateReaderImpl.readInputData`
(`.../checkpoint/channel/SequentialChannelStateReaderImpl.java:88-100`):

- Keep the first read (genuine input-channel-state, `extractUnmergedInputHandles`) exactly
  as-is — those segments are record-aligned by construction (drain.OUT shows `firstHeaderAt=13`
  for all legitimate input-state segments in this log).
- Remove the second `read(...)` over `getUpstreamOutputBufferState` from this
  per-channel-deserializer path. Route the re-typed upstream-output handles instead down a
  **raw in-flight-buffer replay** (the same mechanism output recovery uses,
  `ResultSubpartition.addRecovered` / raw `RecoveredInputChannel` buffer injection), so those
  bytes enter the channel as opaque network buffers, not as a second segment on the shared
  spanning deserializer.

This makes each physical channel's spanning deserializer see exactly one contiguous,
record-aligned stream (its own input-channel-state), and the head-less output buffers get
re-spanned by the normal runtime path that is designed to tolerate a mid-record start.

**Fallback (only if the two streams must share a channel):** give the re-typed output stream
a distinct deserializer identity (so it never collides with input-channel-state spanning
state) **and** carry a first-record-offset / "starts mid-record, drop leading orphan
partial" marker so its head-less first buffer is not misread. Both are required together;
alignment metadata alone does not help Round 5's case (A leaves no remainder, yet B is still
head-less), and a distinct identity alone still leaves B unparseable from byte 0.
