# Round 4 — Verification: is the corrupt buffer's "head" spliced into the SAME deserializer?

## Scope

Focused re-check of ONE claim from Round 3: that the corrupt buffer is *the first buffer its
deserializer sees, with no spanning predecessor*. A reviewer objected that recovery reads both
input-channel-state and upstream-output-state in order into the same deserializer, so spanning
*should* splice a boundary-spanning record and not corrupt.

**Result: the reviewer is right that the two states go to the same deserializer, and Round 3's
"fresh deserializer / no spanning predecessor" statement is FACTUALLY WRONG. But this does not make
the bug disappear — it reclassifies it. The predecessor buffer that *is* present belongs to an
UNRELATED byte stream, and it is precisely the (wrong) spanning splice that produces the
corruption.** Class **B** (implementation bug: two unrelated streams collide on one deserializer key),
with the true corruption site relocated to the DOWNSTREAM CONSUMER, not the filter path.

Failing channel: `InputChannelInfo{gateIdx=0, inputChannelIdx=20}`, recovering task
`failing-map (4/21)#0` (old subtask index 3), recovery job `efc0ff85`, source snapshot job `656565d3`.

---

## 0. First correction: the corruption is NOT thrown in the filter/VirtualChannel path

Round 3 (and the task brief) place the failure in
`ChannelStateFilteringHandler.GateFilterHandler.filterAndRewrite` (keyed by
`SubtaskConnectionDescriptor`). In **round3_FAIL.log that is not where it throws.**

- All 6 `[CS-INV-CORRUPT]` lines in round3 log `ch=InputChannelInfo{...}` (not
  `SubtaskConnectionDescriptor{...}`) and run on the **main task thread** `[failing-map (4/21)#0]`,
  not on a `channel-state-unspilling-...` thread. (round3_FAIL.log:128196, 128257, 147663, 147724,
  147762, 149094.)
- `corruptionSite(...)` with an `InputChannelInfo` argument is only called from
  `AbstractStreamTaskNetworkInput.emitNext` at
  `flink-runtime/.../streaming/runtime/io/AbstractStreamTaskNetworkInput.java:179`, inside the
  `catch (IOException)` around `currentRecordDeserializer.getNextRecord(...)` (line 173). The
  `ch=` printed is `lastChannel` (an `InputChannelInfo`), set in `processBuffer` at line 324.
- Contrast round2_FAIL.log, where the CORRUPT *did* fire in the filter path
  (`ch=SubtaskConnectionDescriptor{...}` on `channel-state-unspilling-...` threads:
  round2_FAIL.log:81224, 87471, 108014, 109538).

So between round2 and round3 the failure **moved downstream**: from the recovery-time filter
deserializer (per `SubtaskConnectionDescriptor`) to the **consumer-time per-channel deserializer**
`recordDeserializers.get(InputChannelInfo)`
(`AbstractStreamTaskNetworkInput.java:75, 353-355`). This deserializer consumes the recovered
buffers that recovery spilled and the input gate replayed.

---

## 1. Full ordered buffer sequence into the failing consumer deserializer (InputChannelInfo{20})

The consumer deserializer for `inputChannelIdx=20` is fed by the recovered spill segments that the
drainer delivers via `RecoverableInputChannel.onRecoveredStateBuffer`
(`FetchedChannelStateDrainer.drainSegment`, FetchedChannelStateDrainer.java:122-159). For channel 20
on this task, in delivery order (`drain.OUT` lines, round3_FAIL.log):

| # | line | source | len | firstHeaderAt | headers | ends on boundary? |
|---|------|--------|-----|---------------|---------|-------------------|
| 1 | 122767 | **Segment A** | 2142 | 13 | 102 | **NO — ends mid-record** |
| 2 | 123829 | **Segment B** (corrupt) | 2693 | 18 | 128 | (starts mid-record) |

**The corrupt buffer (B) is NOT the first buffer the channel-20 deserializer sees.** Segment A is
delivered first. This directly refutes Round 3 §2/§4.4 ("a fresh `SpillingAdaptiveSpanningRecordDeserializer`
(no spanning seed) parses from byte 0").

Record framing (validated against B's hex dump at round3_FAIL.log:128198): each record is
`[4-byte length = 0x11 = 17][17-byte payload]`, stride 21, and the `AB CD EA FC` marker sits at
`frameStart + 10`.

- **Segment A** (`firstHeaderAt=13` ⇒ first full frame at offset 3; 102 markers, last at 2134): the
  final frame would span 2124..2144 but the buffer is only 2142 bytes long ⇒ **A ends ~18 bytes into
  an incomplete trailing record.** A therefore leaves a *partial record* in the deserializer's
  spanning state.
- **Segment B** (`firstHeaderAt=18` ⇒ 8 leading bytes `FC 00 00 FC 55 00 00 00` then a clean
  `00 00 00 11` length at offset 8): B begins with the *tail* of some record and expects a spanning
  predecessor to supply that record's head.

So a preceding buffer exists (A), and it ended mid-record (leftover expected). The deserializer will
try to complete A's dangling partial using B's leading bytes.

---

## 2. Do input-state and upstream-output-state for this channel map to the SAME deserializer?

**Yes — and that is the defect.** The two segments are two *different origin classes* that
collide onto the same key.

Write side, source job `656565d3` (`buildHandle` / `ckptWrite`):

- **Segment A** — genuine INPUT channel state (round3_FAIL.log:39281, 40341):
  ```
  ckptWrite.MEM@off65619 map=INPUT ch=InputChannelInfo{gateIdx=0, inputChannelIdx=20} len=2142 firstHeaderAt=13
  buildHandle InputChannelStateHandle subtask=3 info=InputChannelInfo{inputChannelIdx=20} offsets=[65619]   (file 8c38df16)
  ```
  = "input buffers that old-subtask-3 had received on its input channel 20."
- **Segment B** — UPSTREAM OUTPUT state, re-typed (round3_FAIL.log:96319, E2/E3 of round3_evidence):
  ```
  buildHandle ResultSubpartitionStateHandle subtask=20 info=ResultSubpartitionInfo{partitionIdx=0, subPartitionIdx=3}
             offsets=[72561,73589,74317,75046]   (file 5d48e233)
  ```
  = "output buffers that old-subtask-20 had produced for subpartition 3."

`TaskStateAssignment.distributeOutputBufferToDownstream`
(`flink-runtime/.../checkpoint/TaskStateAssignment.java:600-636`) re-types B into an
`InputChannelStateHandle` with
`InputChannelInfo(gateIdx, inputChannelIdx = oldUpstreamSubtaskIndex = 20)` and
`subtaskIndex = subPartitionIdx = 3`. On the read side both handles therefore surface as
`InputChannelInfo{inputChannelIdx=20}, oldSubtask=3` (round3_FAIL.log:120462 vs 120501).

- **Filter path key** = `SubtaskConnectionDescriptor(oldSubtaskIndex=3, oldChannelIndex=20)`
  (`RecoveredChannelStateHandler.recover` → `ChannelStateFilteringHandler.filterAndRewrite`
  → `GateFilterHandler.filterAndRewrite`, ChannelStateFilteringHandler.java, key built as
  `new SubtaskConnectionDescriptor(oldSubtaskIndex, oldChannelIndex)`). A and B produce the **same
  (3,20) key** → same VirtualChannel deserializer during recovery.
- **Spill segment key** = the *mapped new* `InputChannelInfo`
  (`RecoveredChannelStateHandler.segmentSerializerFor(getMappedChannels(channelInfo).getChannelInfo())`,
  RecoveredChannelStateHandler.java:556, 318-335). A and B both map to new channel 20.
- **Consumer key** = `InputChannelInfo{inputChannelIdx=20}` in `recordDeserializers`
  (`AbstractStreamTaskNetworkInput.getActiveSerializer`, line 353-355). Same deserializer.

So at *every* stage — recovery VirtualChannel, spill segment, and consumer deserializer — the
input-channel-state bytes (A) and the upstream-output bytes (B) for this physical channel land on
**one shared deserializer**, spliced by spanning. The reviewer's model of the plumbing is correct.

(Note the two segments are sealed independently — `switchChannelIfNeeded` seals A when other
channels are processed between the two read passes: spillSeal at round3_FAIL.log:120483 for A,
120526 for B — but sealing only bounds the on-disk segment; on the consume side the *single*
per-channel deserializer still carries spanning state across the two delivered buffers.)

---

## 3. Is the head present at all?

There **is** a predecessor buffer (Segment A) and it **does** leave a partial-record head in the
deserializer. But it is the **wrong head**: A is the tail of old-subtask-3's *input* stream on
channel 20; B is the head-less start of old-subtask-20's *output* stream for subpartition 3. These
are two independent byte streams that were never one contiguous record sequence. B's own genuine
head (the record whose tail is B's leading 8 bytes) was **never snapshotted** — it lived in an
earlier, already-transmitted output buffer of the producer (round3 §2, confirmed by
`firstHeaderAt=18` on the first output chunk, ckptWrite.MEM@off72561 in round3_evidence E3).

So: A's dangling partial (18 bytes of a 21-byte input record) is prepended to B's 8-byte output-tail
fragment. `18 + 8 = 26 ≠ 21`, and A's partial needs only 3 more bytes while B's clean framing does
not resume until offset 8 — the two do not reconcile. The deserializer consumes A's length prefix,
then reads B's bytes as that record's remaining payload + next length, mis-frames immediately, and
throws on the very first record of B: `recordsOkInThisBuffer=0` (round3_FAIL.log:128196).

---

## 4. Classification

**Class B — implementation bug (key/stream collision), corruption realized at the consumer
deserializer.** Precisely:

- It is NOT (A) "head never recovered / pure design gap with a fresh deserializer" as Round 3 stated:
  a predecessor buffer is present and *is* fed to the same deserializer first. Round 3's specific
  mechanism ("fresh deserializer parses B from byte 0") is **wrong**.
- It is NOT (C) "correct head spliced, so prior root cause wrong and nothing corrupts": the splice
  happens but joins two unrelated streams, so it genuinely corrupts.
- It IS (B): input-channel-state (A) and re-typed upstream-output-state (B) for the same physical
  channel are routed to the **same** spanning deserializer (VirtualChannel `(3,20)`, spill segment
  new-channel-20, and consumer `InputChannelInfo{20}`), where A's trailing partial record is
  illegitimately spliced onto B, which itself legitimately starts mid-record with no snapshotted head.

**Two independent faults compound:**
1. **Stream collision (the mismatch site).** `TaskStateAssignment.distributeOutputBufferToDownstream`
   (TaskStateAssignment.java:621-630) maps upstream-output subpartition state onto the same
   `InputChannelInfo`/`SubtaskConnectionDescriptor` identity as real input-channel state, so two
   unrelated record streams share one spanning deserializer. Because a `SpillingAdaptive...`
   deserializer carries partial-record state across buffers, the tail of stream A gets glued to the
   start of stream B.
2. **Head-less output start (the design gap that makes the collision fatal).** B (upstream output
   in-flight) intrinsically begins mid-record and its head is not in the snapshot, so B can never be
   safely fed to a record-framing deserializer as if it were a fresh, aligned start — and certainly
   not one already holding leftover bytes from a different stream.

## 5. Precise fix / next step

The two recovered streams must NOT be multiplexed onto one spanning record deserializer:

- **Preferred (matches the pre-FLINK-38542 invariant):** do not record-deserialize the re-typed
  upstream-output buffers at all. Replay them as raw in-flight `Buffer`s into the input channel
  (as the original output-recovery path did via `ResultSubpartition.addRecovered`), so the *real*
  consumer deserializer re-spans them against buffers it received live — never mixing them with the
  distinct input-channel-state stream in a single filter/spill/consume deserializer. Concretely,
  keep upstream-output recovery on a raw-buffer path rather than routing it through
  `SequentialChannelStateReaderImpl.readInputData`'s second `read(...)` →
  `ChannelStateFilteringHandler` → spill → consumer-deserializer pipeline.
- **If they must share a channel:** give the two streams distinct deserializer identities (do not let
  input-channel-state and re-typed upstream-output-state collide on the same
  `SubtaskConnectionDescriptor`/`InputChannelInfo` spanning deserializer), *and* carry a
  first-record-offset / "starts mid-record, drop leading partial" marker for the output stream so its
  head-less first buffer is not misread. Both are required — fixing only the alignment marker still
  leaves A's tail spliced into B; fixing only the collision still leaves B head-less.

**Bottom line: this is a real bug (class B).** The reviewer's premise — that head and tail reach the
same deserializer in order — is correct, and that is exactly why it breaks: the "head" that reaches
the deserializer is the dangling tail of a *different* stream (input-channel-state), not the head of
B's record, so spanning splices two unrelated streams and corrupts on B's first record. Round 3's
conclusion is corrected: the failure is at the consumer deserializer
(`AbstractStreamTaskNetworkInput.java:179`), the corrupt buffer is the *second* buffer on the
channel (not the first), and a predecessor buffer is present but belongs to the wrong stream.
