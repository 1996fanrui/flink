# Round 3 — Root-cause analysis: channel-state recovery corruption

## Verdict: **CONCLUSIVE**

**One-sentence root cause:** The FLINK-38542 "recover upstream output buffers on the downstream task"
feature re-types a producer's in-flight `ResultSubpartitionStateHandle` (output buffers, which legitimately
begin mid-record) into an `InputChannelStateHandle` and feeds those bytes through the downstream input
**record deserializer**, but the first recovered buffer of a subpartition starts mid-record with no spanning
predecessor and no partial-record marker, so the deserializer reads a data byte as a length/tag and corrupts
the stream (`recordsOkInThisBuffer=0`).

**Fix site:** `TaskStateAssignment.distributeOutputBufferToDownstream`
(`flink-runtime/src/main/java/org/apache/flink/runtime/checkpoint/TaskStateAssignment.java:600-637`) — the
output→input handle conversion — together with the read/deserialize path it feeds
(`SequentialChannelStateReaderImpl.readInputData` second `read(...)` at line 95-99 →
`ChannelStateFilteringHandler` / `SpillingAdaptiveSpanningRecordDeserializer`). The conversion drops the
record-boundary context that the original output-recovery path never needed (because it replayed raw
buffers), so the receiving record deserializer has no way to know byte 0 is mid-record.

---

## 1. Resolution of the pivotal "by-design vs. cross-wire" question

The captured tuple is **NOT a metadata cross-wire**. It is the legitimate, intentional
`getUpstreamOutputBufferState` path (option 2 in the brief). Proof:

- `OperatorSubtaskState` has a **third** channel-state collection,
  `upstreamOutputBufferState`, declared as `StateObjectCollection<InputChannelStateHandle>`
  (`OperatorSubtaskState.java:93`, getter line 200). `readInputData` deliberately issues a **second** read
  over it (`SequentialChannelStateReaderImpl.java:95-99`,
  `OperatorSubtaskState::getUpstreamOutputBufferState`).
- That collection is populated by `TaskStateAssignment.distributeOutputBufferToDownstream`
  (`TaskStateAssignment.java:600-636`). It takes a producer `ResultSubpartitionStateHandle` and **wraps its
  delegate + offsets + size into a new `InputChannelStateHandle`**:
  ```java
  int oldUpstreamSubtaskIndex   = stateHandle.getSubtaskIndex();          // 20
  int oldDownstreamSubtaskIndex = info.getSubPartitionIdx();              // 3
  InputChannelInfo inputChannelInfo =
      new InputChannelInfo(gateIdxResultPartition, oldUpstreamSubtaskIndex);   // idx=20
  InputChannelStateHandle upstreamOutputBufferHandle =
      new InputChannelStateHandle(
          oldDownstreamSubtaskIndex,          // subtaskIndex (oldSubtask) = 3
          inputChannelInfo,                   // InputChannelInfo{gateIdx, inputChannelIdx=20}
          stateHandle.getDelegate(),          // SAME file 5d48e233
          stateHandle.getOffsets(),           // SAME offsets [72561,73589,74317,75046]
          stateHandle.getStateSize());
  ```
- This **exactly reproduces** the logged handle (evidence E1 vs E2): `inputChannelIdx=20` = upstream subtask
  20, `oldSubtask=3` = subPartitionIdx 3, same file `5d48e233`, same offset list. And it explains why **every**
  `readSequentially.handle` in recovery is typed `InputChannelStateHandle` even though the bytes were written
  as a `ResultSubpartitionStateHandle` (E2).
- `[CS-INV-ASSERT]` count = 0 (E8): the write side never put a `ResultSubpartitionInfo` key into the input
  offsets map. The offsets `[72561,...]` are *genuinely* output-subpartition-3 offsets; the input handle that
  carries them was **constructed on purpose** by the rescale assignment.

So Round 2's "offset cross-wiring: an input handle references a file offset that holds output bytes" was the
right *observation* but the wrong *mechanism*: the input handle holds output offsets **by design**, not by a
serialization/assembly mixup. The corruption is the **misalignment** the brief's option 2 predicted.

This feature is recent on this branch:
`686c00f8e3b [FLINK-38542][checkpoint] Recover output buffers of upstream task on downstream task side directly`.

## 2. Why the buffer arrives mid-record with no predecessor

A subpartition's in-flight output buffers are a **byte stream that begins wherever the previous (already
transmitted, not-snapshotted) buffer left off** — they are intrinsically *not* record-aligned at their first
snapshotted buffer. The producing job's snapshot of subpartition 3 / subtask 20 starts at off72561 with
`firstHeaderAt=18` (E3): the head of that first record lived in an earlier buffer that was already sent
downstream before the checkpoint barrier and is **not in the snapshot**.

- The four chunks `[72561,73589,74317,75046]` belong to **one** producer subtask's (20) one subpartition (3),
  so they correctly span among **themselves** (they share one virtual channel after conversion). But the
  **first** chunk's leading partial record has no head anywhere in this handle.
- On recovery these four are read in order and concatenated into a single 2693-byte buffer for virtual
  channel `(oldSubtask=3, oldChannel=20)` (E4), and the per-virtual-channel
  `SpillingAdaptiveSpanningRecordDeserializer` (fresh, no spanning seed) parses from byte 0. Byte 0 is 18 bytes
  into a record → it reads a data byte as a 4-byte length/tag → `recordsOkInThisBuffer=0`, corrupt (E5).

`firstHeaderAt != 0` is normal for *mid-stream* input buffers because the input path always has a spanning
predecessor carrying the head. Here there is **none**, because this is the **first** buffer of the
subpartition and the head was never checkpointed.

## 3. Why the original output-recovery path did NOT corrupt — the asymmetry

The pre-existing output-recovery path **never record-deserializes** the recovered output buffers. In
`ResultSubpartitionRecoveredStateHandler.recover`
(`RecoveredChannelStateHandler.java:627-656`) the recovered buffer is re-injected as a **raw network
`BufferConsumer`** into the `ResultSubpartition`, prefixed by a `SubtaskConnectionDescriptor` event:
```java
checkpointedResultPartition.addRecovered(
        mappedSubpartition.getSubPartitionIdx(),
        EventSerializer.toBufferConsumer(channelSelector, false));
checkpointedResultPartition.addRecovered(
        mappedSubpartition.getSubPartitionIdx(), bufferConsumer.copy());
```
Raw replay preserves the original buffer boundaries, and the *real* downstream consumer re-spans records with
its own deserializer (which already holds the head from normal pre-barrier delivery). A leading partial
record is therefore harmless.

The new downstream path breaks this invariant: instead of raw replay, the same output bytes flow into the
**input recovery deserializer** via `SpillingWithFilteringHandler.recover` →
`ChannelStateFilteringHandler.GateFilterHandler.filterAndRewrite`
(`RecoveredChannelStateHandler.java:542-561`; `ChannelStateFilteringHandler` `setNextBuffer` +
`getNextRecord` loop). That deserializer assumes a record-aligned start and has no marker telling it the first
buffer is mid-record, so it cannot skip the leading partial record.

`InputChannelStateHandle` has no field to carry a partial-record / first-record-offset hint, so the
conversion at `TaskStateAssignment.java:624-630` discards the only information that could have let the
receiver re-align.

## 4. Mechanism (proof chain)

1. Producing job `656565d3` snapshots subpartition-3 in-flight output of subtask 20 → 4 buffers at offsets
   `[72561,73589,74317,75046]`, first buffer mid-record (`firstHeaderAt=18`) because its head was already
   transmitted and not snapshotted (E3). Built as `ResultSubpartitionStateHandle subPartitionIdx=3 subtask=20`
   (E2). No key cross-wire (E8).
2. On rescale/restart, `TaskStateAssignment.distributeOutputBufferToDownstream`
   (`TaskStateAssignment.java:600-636`) re-types that output handle into
   `InputChannelStateHandle{InputChannelInfo{gateIdx=0, inputChannelIdx=20}, oldSubtask=3,
   delegate=5d48e233, offsets=[72561,...]}` and stores it in the downstream subtask's
   `upstreamOutputBufferState` (E1).
3. Recovery job `efc0ff85` reads it via the second `read(...)` in `readInputData`
   (`SequentialChannelStateReaderImpl.java:95-99`); `readSequentially`/`extractOffsetsSorted`/`readChunk`
   delivers the 4 chunks to virtual channel `(oldSubtask=3, oldChannel=20)` (E4).
4. `SpillingWithFilteringHandler.recover` → `filterAndRewrite` runs the concatenated 2693-byte buffer through
   a fresh `SpillingAdaptiveSpanningRecordDeserializer` starting at byte 0. Byte 0 is 18 bytes into a record;
   no spanning head exists → reads a data byte as a tag → `recordsOkInThisBuffer=0`, corrupt (E5).
5. Reproduced on retry #1 (E6) and on a second independent channel idx=23 (E7).

## 5. Fix direction (for the implementer; not part of the verdict)

The defect is architectural to FLINK-38542: an output in-flight buffer stream is being fed into a record
deserializer that requires a record-aligned start. Options, in rough order of soundness:

- **Preferred:** route the upstream-output buffers through a raw-replay path analogous to the original output
  recovery (re-inject as buffers and let the real input record deserializer re-span), instead of
  record-deserializing them in the filter/spill path. I.e. the downstream should treat recovered upstream
  output as opaque in-flight buffers, not as a freshly-framed input record stream.
- **Or** carry the first-buffer partial-record offset from the producer snapshot through the conversion at
  `TaskStateAssignment.java:624-630` (extend `InputChannelStateHandle` / the recovery path with a
  "skip leading N bytes / first record starts at offset X" marker) so the receiving deserializer can re-align
  before parsing.
- **Or** snapshot the subpartition's in-flight output starting at a record boundary (carry the spanning head),
  so the first recovered buffer is always record-aligned.

The single line that *creates* the unrecoverable handle is `TaskStateAssignment.java:624-630`
(`new InputChannelStateHandle(... stateHandle.getOffsets() ...)`); the line that *cannot cope* with it is the
deserialize loop in `ChannelStateFilteringHandler` reached via `SequentialChannelStateReaderImpl.java:95-99`.

## 6. What changed vs. Round 2

Round 2 reached CONCLUSIVE on the *symptom* (output bytes delivered to an input virtual channel, breaking
framing) but attributed it to an accidental "offset cross-wire in the shared file namespace." Round 3's
`buildHandle` instrumentation + the `upstreamOutputBufferState` code path show the delivery is **intentional**
(FLINK-38542), so the true defect is the **misalignment** of feeding unaligned output buffers into the aligned
input deserializer — pinned to `TaskStateAssignment.distributeOutputBufferToDownstream` and the input
deserialize path it feeds. The residual gap Round 2 flagged ("name the single line vs. handle assembly") is
now closed: it is handle assembly at `TaskStateAssignment.java:624-630`, and it is by design, not a bug in the
write-side offset bookkeeping (which is clean — `[CS-INV-ASSERT]`=0).
