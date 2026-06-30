# Round 3 — Decisive evidence (verbatim)

Log: `requirements/38544/fix_rounds/round3_FAIL.log` (151008 lines).
Failing channel: `InputChannelInfo{gateIdx=0, inputChannelIdx=20}`, recovering task `failing-map (4/21)#0`,
recovery job `efc0ff85`, producing job `656565d3`. `[CS-INV-ASSERT]` count in whole log = **0**.
All `readSequentially.handle` lines in the recovery = **11869**, every one typed `InputChannelStateHandle`
(the reader never sees a `ResultSubpartitionStateHandle`).

## E1 — The recovered handle (read side, recovery job efc0ff85)

Line 120501:
```
[CS-INV] readSequentially.handle InputChannelStateHandle info=InputChannelInfo{gateIdx=0, inputChannelIdx=20}
  oldSubtask=3 delegate#392e6382{Segment File State: file:/.../656565d3.../5d48e233-9064-4794-a35c-0ad1d3fa1cd4
  [Starting Position: 713, 115820 bytes]} offsets=[72561, 73589, 74317, 75046]
```
An `InputChannelStateHandle` (input!) with `inputChannelIdx=20`, `oldSubtask=3`, backing file `5d48e233`,
offsets `[72561, 73589, 74317, 75046]`.

## E2 — The handle was BUILT as OUTPUT (write side, producing job 656565d3)

The ONLY `buildHandle` line carrying offsets `[72561, ...]` (line 96319):
```
[656565d3...] [Channel state writer] [CS-INV] buildHandle ResultSubpartitionStateHandle subtask=20
  info=ResultSubpartitionInfo{partitionIdx=0, subPartitionIdx=3}
  delegate#7280459e{Segment File State: file:/.../656565d3.../5d48e233-9064-4794-a35c-0ad1d3fa1cd4
  [Starting Position: 713, 115820 bytes]} offsets=[72561, 73589, 74317, 75046]
```
Same file `5d48e233`, same offset list, built as **ResultSubpartitionStateHandle subPartitionIdx=3, subtask=20**.

Mapping is exactly the FLINK-38542 conversion in `TaskStateAssignment.distributeOutputBufferToDownstream`:
`inputChannelIdx (20) = oldUpstreamSubtaskIndex (=output handle subtask 20)`,
`oldSubtask (3) = subPartitionIdx (3)`.

## E3 — The four written OUTPUT chunks (write side)

```
ckptWrite.MEM@off72561 map=OUTPUT ch=ResultSubpartitionInfo{partitionIdx=0, subPartitionIdx=3} len=1024 headers=48 firstHeaderAt=18
ckptWrite.MEM@off73589 map=OUTPUT ch=ResultSubpartitionInfo{partitionIdx=0, subPartitionIdx=3} len=724  headers=35 firstHeaderAt=2
ckptWrite.MEM@off74317 map=OUTPUT ch=ResultSubpartitionInfo{partitionIdx=0, subPartitionIdx=3} len=725  headers=34 firstHeaderAt=13
ckptWrite.MEM@off75046 map=OUTPUT ch=ResultSubpartitionInfo{partitionIdx=0, subPartitionIdx=3} len=220  headers=11 firstHeaderAt=2
```
Sums: 1024+724+725+220 = **2693**; 48+35+34+11 = **128**. First chunk starts mid-record (`firstHeaderAt=18`).

## E4 — The four chunks read back as INPUT idx=20 (read side, lines 120520-120524)

```
readChunk.IN@off72561 ... ch=InputChannelInfo{gateIdx=0, inputChannelIdx=20} len=1024 headers=48 firstHeaderAt=18
readChunk.IN@off73589 ... ch=InputChannelInfo{gateIdx=0, inputChannelIdx=20} len=724  headers=35 firstHeaderAt=2
readChunk.IN@off74317 ... ch=InputChannelInfo{gateIdx=0, inputChannelIdx=20} len=725  headers=34 firstHeaderAt=13
readChunk.IN@off75046 ... ch=InputChannelInfo{gateIdx=0, inputChannelIdx=20} len=220  headers=11 firstHeaderAt=2
```
Backing file `5d48e233` — identical to E1/E2. Same four offsets, same shapes.

## E5 — The corruption (line 128196)

```
[failing-map (4/21)#0] ERROR [CS-INV-CORRUPT] ch=InputChannelInfo{gateIdx=0, inputChannelIdx=20}
  recordsOkInThisBuffer=0 len=2693 headers=128 firstHeaderAt=18 strides=[21,...]
```
`len=2693 / headers=128` = the concatenation of the four OUTPUT chunks. `recordsOkInThisBuffer=0` →
the deserializer failed on the very first read because the buffer begins mid-record (`firstHeaderAt=18`)
with no spanning predecessor.

## E6 — Retry #1 reproduces identically (lines 138376 / 138395-138399 / 147724)

Same handle (`inputChannelIdx=20 oldSubtask=3 offsets=[72561,73589,74317,75046]`), same 4 reads, same
`CS-INV-CORRUPT recordsOkInThisBuffer=0 len=2693 headers=128 firstHeaderAt=18`.

## E7 — Second independent channel idx=23

```
readSequentially.handle InputChannelStateHandle info=InputChannelInfo{gateIdx=0, inputChannelIdx=23}
  oldSubtask=5 delegate#...{.../7da9aa88... [Starting Position: 0, 11050 bytes]} offsets=[3817]
CS-INV-CORRUPT ch=InputChannelInfo{gateIdx=0, inputChannelIdx=23} recordsOkInThisBuffer=0 len=1895 headers=90 firstHeaderAt=18
```
Same signature class (`inputChannelIdx = old upstream subtask`, `oldSubtask = subPartitionIdx`,
first buffer mid-record, `recordsOkInThisBuffer=0`). A second instance of the same conversion path.

## E8 — There is no input/output cross-wire at write time

`grep -c [CS-INV-ASSERT]` over the whole log = **0**. The write side routes `InputChannelInfo` keys only to
the input offsets map and `ResultSubpartitionInfo` keys only to the output offsets map
(`ChannelStateCheckpointWriter.write` → `ChannelStateInvariant.assertKeyKind`, always-on). So the offsets
`[72561,...]` are *legitimately* OUTPUT-subpartition-3 offsets; the input handle that carries them was
**deliberately constructed** from the output handle by the rescale assignment, not produced by a metadata mixup.
