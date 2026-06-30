# Round 2 — Root-cause analysis: channel-state corruption (filter path, tag -22)

## Verdict: **CONCLUSIVE**

The log exhibits the decisive healthy→corrupt transition with a complete proof chain. The failing
input channel during recovery is fed **exactly one** buffer, and that buffer is bytes that were
written to the checkpoint file as **OUTPUT (ResultSubpartition) state**, not input-channel state.
Input and output channel state share **one physical checkpoint file with one offset namespace**, and
the recovered InputChannelStateHandle's offset for the failing channel points into the output region.
The buffer therefore begins mid-record (`firstHeaderAt=2`) with no spanning predecessor, so the
recovery deserializer reads a data byte as a tag → `Corrupt stream, found tag: -22`.

This is **not** a spanning/ordering bug inside the deserializer, and **not** the spill/drain read-back
(this round's failure is on the recovery *filter* path, upstream of any spill). It is an **offset
cross-wiring**: an input-channel handle references a file offset that holds output-subpartition bytes.

---

## 1. Job topology in the single MiniCluster JVM

Three job attempts, identified by the leading bracket hash:

| Job hash | Role | Evidence |
|---|---|---|
| `fe2e9068…` | writes only (initial chk, parallelism 20) | 22736 `ckptWrite`, 0 `readChunk` |
| `940a5f3d…` | **recovers fe2e9068 AND re-checkpoints** | 4506 `readChunk` + 10929 `ckptWrite` |
| `b60737eb…` | recovers 940a5f3d (corruption fires here) | 8326 `readChunk`, 0 `ckptWrite` |

The rescale is upscale 20→21 (downscale0/failing-map 21 instances) — channel-state buffers are
re-distributed across the new channel count.

All four `[CS-INV-CORRUPT]` lines, **0** `STRIDE-IRREGULAR`, **0** `[CS-INV-ASSERT]` in the whole log.
The two distinct failures (each with a `#0` and a `#1` retry) are:

| Descriptor (oldSubtask, oldChannel) | failing task | read chunk | shape |
|---|---|---|---|
| `{6,29}` | `failing-map (7/20)#0` (`…e6fd_6_0`) | `readChunk.IN@off48512 idx=29` | `len=556 headers=27 firstHeaderAt=2` |
| `{4,8}` | `failing-map (5/20)#0` (`…e6fd_4_0`) | `readChunk.IN@off51649 idx=8` | `len=451 headers=22 firstHeaderAt=2` |

## 2. Addressing: descriptor ↔ InputChannelInfo

In `ChannelStateFilteringHandler.GateFilterHandler.filterAndRewrite` (line 280-281) the virtual
channel key is `new SubtaskConnectionDescriptor(oldSubtaskIndex, oldChannelIndex)`. The driver is
`SpillingWithFilteringHandler.recover` (RecoveredChannelStateHandler.java:551-556), which calls
`filterAndRewrite(channelInfo.getGateIdx(), oldSubtaskIndex, channelInfo.getInputChannelIdx(), …)`.
So `SubtaskConnectionDescriptor{inputSubtaskIndex=I, outputSubtaskIndex=C}` ⇒
`oldSubtaskIndex=I, oldChannelIndex = channelInfo.inputChannelIdx = C`.

Confirmed by adjacency in the log: the `readChunk.IN` immediately preceding the corrupt `filter.IN
{6,29}` is `readChunk.IN@off48512 ch=InputChannelInfo{gateIdx=0, inputChannelIdx=29}` (round2 line
81220 → 81222). So the failing channelInfo is **`{gateIdx=0, inputChannelIdx=29}`**, oldSubtask=6.

> Correction to the prior capture's note: the matching channel is `inputChannelIdx=29`, **not** 6.
> The "6" in `_6_0` / `inputSubtaskIndex=6` is the *old subtask index*, not the channel index.
> `inputChannelIdx=6` never appears on the failing thread.

## 3. Chunk sequence delivered to the failing virtual channel (delivery order)

Restricting to the **exact** failing attempt
`645d4f7b…_b8c789ec3a44294cb45da029ffe0e6fd_6_0` (read job `b60737`), the readChunk for
`InputChannelInfo{idx=29}` occurs **exactly once**:

```
81220  readChunk.IN@off48512 idx=29 len=556 headers=27 firstHeaderAt=2 strides=[21×26]
81222  filter.IN {6,29}        len=556 headers=27 firstHeaderAt=2 strides=[21×26]
81224  [CS-INV-CORRUPT] {6,29} recordsOkInThisBuffer=0 …  Corrupt stream, found tag: -22
```

So the corrupt buffer is the **FIRST and ONLY** buffer ever delivered to virtual channel `{6,29}`
for this task. It starts mid-record (`firstHeaderAt=2`): the value header `AB CD EA FC` sits at
internal offset 13 of a record, so a buffer whose first header is at offset 2 begins 11 bytes into a
record. The 11 leading bytes `A5 33 AB CD EA FC 00 00 A5 33 00` are the **tail of a record whose head
was never delivered** (hex at round2 line 81230). With no spanning predecessor, the deserializer
reads `A5 33` as length+tag → `tag: -22` (`0xEA`). `recordsOkInThisBuffer=0` confirms it failed on the
very first read.

The strides inside the buffer are all 21 — the buffer is internally well-framed; only its **start**
is mid-record and its **head** is missing.

(Earlier confusion: a `grep '_6_0)-thread-1'` substring also matches `(7/21)#0 (ee8…)` and
`downscale0 (7/21)#0`. The full attempt id must be matched. With the full id, the failing task gets a
single idx=29 chunk.)

## 4. Where the `firstHeaderAt=2` bytes were born — the smoking gun

The bytes at file offset 48512 were written by job `940a5f3d` on the `[Channel state writer]` thread
via the **MEM output path** (`ChannelStateCheckpointWriter.writeOutput` → `write`, line 248):

```
68709  ckptWrite.MEM@off48512 ch=ResultSubpartitionInfo{partitionIdx=0, subPartitionIdx=6}
       len=556 headers=27 firstHeaderAt=2
```

i.e. off48512 is an **OUTPUT (ResultSubpartition) in-flight buffer**, which legitimately starts
mid-record. **Off48512 was never written as input idx=29** (grep for an
`InputChannelInfo … @off48512` write in `940a5f3d` returns 0). Yet the recovery of
`failing-map (7/20)#0` reads its input channel idx=29 from exactly off48512.

Identical structure for the second failure: off51649 was written as
`ResultSubpartitionInfo{subPartitionIdx=4} len=451 headers=22 firstHeaderAt=2` (line 68630), and
`failing-map (5/20)#0` reads input idx=8 from off51649 (line 87469) → corrupt `{4,8}`.

## 5. Input and output state share ONE file / ONE offset namespace

`ChannelStateCheckpointWriter.write` records every buffer's offset as `checkpointStream.getPos()`
into the same `dataStream` regardless of whether it is input (`writeInput`) or output
(`writeOutput`). Proof from the log: within a single ~1 KB offset window in job `940a5f3d`, input and
output writes **interleave** in the same offset namespace:

```
@off48039 InputChannelInfo{inputChannelIdx=4}
@off48055 ResultSubpartitionInfo{subPartitionIdx=5}
@off48065 ResultSubpartitionInfo{subPartitionIdx=6}
@off48100 InputChannelInfo{inputChannelIdx=3}
@off48233 InputChannelInfo{inputChannelIdx=20}
@off48245 ResultSubpartitionInfo{subPartitionIdx=6}
@off48450 InputChannelInfo{inputChannelIdx=19}
@off48512 ResultSubpartitionInfo{subPartitionIdx=6}   ← read back as input idx=29
```

So offset 48512 is unambiguous within this file: it is OUTPUT subpartition 6. The recovered
**InputChannelStateHandle** for `failing-map (7/20)#0`'s gate-0 channel-29 carries offset 48512,
which points into output-state bytes. That is the defect.

## 6. Mechanism (proof chain)

1. Job `940a5f3d` recovers from `fe2e9068` and takes a new checkpoint, writing input + output channel
   state interleaved into one file (§5). Offset 48512 holds an output buffer (subpartition 6) that
   begins mid-record (`firstHeaderAt=2`) — normal for an unaligned-checkpoint in-flight buffer.
2. The InputChannelStateHandle persisted for that subtask's gate-0/channel-29 ends up with an offset
   list that includes **48512** — an offset belonging to **output** state (or to a different channel),
   not the channel-29 input record stream.
3. On recovery in `b60737`, `SequentialChannelStateReaderImpl.readSequentially` →
   `extractOffsetsSorted` → `ChannelStateChunkReader.readChunk` seeks to 48512 and reads the bytes
   there as channel-29 input (round2 line 81220), then `SpillingWithFilteringHandler.recover` feeds
   them to virtual channel `{6,29}` (`filterAndRewrite`, line 81222).
4. Those bytes are an output buffer starting 11 bytes into a record. It is the first buffer for that
   virtual channel, so the deserializer has no spanning head → reads `A5 33` as length/tag →
   `Corrupt stream, found tag: -22` (line 81224).

The exact same chain reproduces for `{4,8}` at off51649 (output subpartition 4) and on both `#1`
retries (lines 107953/108014 and 109536/109538). Two independent channels, identical signature.

## 7. What is proven vs. the residual gap

**Proven by the log + code:**
- The failing virtual channel receives exactly one buffer and it begins mid-record with no
  predecessor (lines 81220-81224; 87469-87471).
- The bytes at that read offset were written as OUTPUT subpartition state, not input (lines 68709 /
  68630), and were never written as the corresponding input channel.
- Input and output state occupy one shared offset namespace in the producing job (§5), so the offset
  unambiguously addresses output bytes.

**Residual gap (does not change the verdict):** the instrumentation logs the byte *offset* but not the
*StreamStateHandle identity* of the file each chunk is read from, nor the handle metadata
(`StateContentMetaInfo.offsets`) as it is recorded on the write side. The conclusion that the
input-handle offset is cross-wired to the output region rests on (a) the shared offset namespace and
(b) the exact byte-shape identity (`len/headers/firstHeaderAt = 556/27/2` and `451/22/2`) between the
output write and the input read at the same offset. Coincidence is excluded by the shape identity and
by the second independent reproduction. To name the *single line* that records the wrong offset (vs.
the metadata being mis-assembled at handle-build time), one more capture is warranted — see §8.

## 8. Recommended confirming instrumentation (to pin the exact write-side line)

Add, on the **write** side of `940a5f3d`, a log of the handle/offset bookkeeping so the cross-wire is
visible at the moment it is recorded:

- In `ChannelStateCheckpointWriter.write` (line 248-255), additionally log the *kind* of `key`
  (Input vs ResultSubpartition) together with `offset` and `size`, and the identity of the
  `StateContentMetaInfo` map being updated (`getInputChannelOffsets` vs `getResultSubpartitionOffsets`).
  Assert that an `InputChannelInfo` key is only ever added to the input-offsets map and a
  `ResultSubpartitionInfo` only to the output-offsets map (the current code already routes by caller,
  so the suspect is the *handle assembly* that turns these maps into `AbstractChannelStateHandle`s).
- In the handle-building path (where `StateContentMetaInfo` becomes `InputChannelStateHandle` /
  `ResultSubpartitionStateHandle`), log each produced handle's `info` + full `offsets` list. Then in
  `SequentialChannelStateReaderImpl.extractOffsets` (line 195-202) log `handle.getInfo()` +
  `handle.getDelegate()` (file identity) + each offset. A single FAIL log will then show the
  `InputChannelStateHandle{gateIdx=0, inputChannelIdx=29}` literally carrying offset 48512 that the
  write side recorded under `ResultSubpartitionInfo{subPartitionIdx=6}` — naming the exact mixing
  point (most likely an offset-map keyed by a type that collides Input vs Output, or a handle built
  from the wrong offsets collection during the recover-and-rewrite of job `940a5f3d`).

This is a confirmation refinement; the corruption *mechanism* (output bytes delivered to an input
virtual channel via a cross-wired offset, breaking record framing) is already CONCLUSIVE.
