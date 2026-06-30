# Round 2 — Verbatim decisive evidence

Source: `requirements/38544/fix_rounds/round2_FAIL.log` (35 MB). Line numbers are `grep -n` file lines.

## Whole-file counts
- `[CS-INV-CORRUPT]` lines: **4** (two failures `#0` + their `#1` retries).
- `STRIDE-IRREGULAR`: **0**.
- `[CS-INV-ASSERT]`: **0**.
- Jobs: `fe2e9068…` (write-only), `940a5f3d…` (read+write), `b60737eb…` (read-only; corruption fires here).

---

## Failure A — virtual channel {6,29}, failing-map (7/20)#0

Read chunk delivered to the failing input channel (the only idx=29 chunk for this attempt):

```
81220  53986 [b60737eb…] [channel-state-unspilling-failing-map (7/20)#0 (645d4f7b…_b8c789ec3a44294cb45da029ffe0e6fd_6_0)-thread-1] INFO
       [CS-INV] readChunk.IN@off48512 ch=InputChannelInfo{gateIdx=0, inputChannelIdx=29}
       len=556 headers=27 firstHeaderAt=2 strides=[21×26]
```

Fed to the virtual channel and fails immediately:

```
81222  [CS-INV] filter.IN ch=SubtaskConnectionDescriptor{inputSubtaskIndex=6, outputSubtaskIndex=29}
       len=556 headers=27 firstHeaderAt=2 strides=[21×26]
81224  ERROR [CS-INV-CORRUPT] ch=SubtaskConnectionDescriptor{inputSubtaskIndex=6, outputSubtaskIndex=29}
       recordsOkInThisBuffer=0 len=556 headers=27 firstHeaderAt=2 strides=[21×26]
       Corrupt stream, found tag: -22
hex (first 16B):  A5 33 AB CD EA FC 00 00 A5 33 00 00 00 11 00 00
```

`firstHeaderAt=2` ⇒ buffer begins 11 bytes into a record; `A5 33` (the missing record's tail) is read
as length/tag → `-22` (`0xEA`). `recordsOkInThisBuffer=0` ⇒ first buffer of the channel, no spanning
predecessor.

Origin of these bytes on the WRITE side (job 940a5f3d), at the SAME offset, written as **OUTPUT**:

```
68709  33779 [940a5f3d…] [Channel state writer] INFO
       [CS-INV] ckptWrite.MEM@off48512 ch=ResultSubpartitionInfo{partitionIdx=0, subPartitionIdx=6}
       len=556 headers=27 firstHeaderAt=2
```

Off48512 was **never** written as an input channel in 940a5f3d (grep count = 0).

---

## Failure B — virtual channel {4,8}, failing-map (5/20)#0 (independent reproduction)

```
87469  59269 [b60737eb…] [channel-state-unspilling-failing-map (5/20)#0 (645d4f7b…_b8c789ec3a44294cb45da029ffe0e6fd_4_0)-thread-1] INFO
       [CS-INV] readChunk.IN@off51649 ch=InputChannelInfo{gateIdx=0, inputChannelIdx=8}
       len=451 headers=22 firstHeaderAt=2 strides=[21×21]
87471  ERROR [CS-INV-CORRUPT] ch=SubtaskConnectionDescriptor{inputSubtaskIndex=4, outputSubtaskIndex=8}
       recordsOkInThisBuffer=0 len=451 headers=22 firstHeaderAt=2 strides=[21×21]
hex (first 16B):  94 D0 AB CD EA FC 00 01 94 D0 00 00 00 11 00 00
```

Origin on the WRITE side (job 940a5f3d), same offset, written as **OUTPUT**:

```
68630  33778 [940a5f3d…] [Channel state writer] INFO
       [CS-INV] ckptWrite.MEM@off51649 ch=ResultSubpartitionInfo{partitionIdx=0, subPartitionIdx=4}
       len=451 headers=22 firstHeaderAt=2
```

---

## Shared input/output offset namespace in job 940a5f3d (proves one file)

Interleaved input and output writes in one ~1 KB offset window (`grep ckptWrite` for off48xxx):

```
@off48039 InputChannelInfo{inputChannelIdx=4}
@off48055 ResultSubpartitionInfo{subPartitionIdx=5}
@off48065 ResultSubpartitionInfo{subPartitionIdx=6}
@off48100 InputChannelInfo{inputChannelIdx=3}
@off48233 InputChannelInfo{inputChannelIdx=20}
@off48254 InputChannelInfo{inputChannelIdx=18}
@off48450 InputChannelInfo{inputChannelIdx=19}
@off48512 ResultSubpartitionInfo{subPartitionIdx=6}   ← read back in b60737 as input idx=29
```

Input-channel state and ResultSubpartition (output) state are written to the same checkpoint stream
with one `getPos()` offset namespace, so offset 48512 unambiguously addresses output bytes.

---

## Code anchors (file:line)

- Virtual-channel key: `ChannelStateFilteringHandler.java:280-281`
  `new SubtaskConnectionDescriptor(oldSubtaskIndex, oldChannelIndex)`; corruption capture at 314-322.
- Recover → filter driver: `RecoveredChannelStateHandler.java:551-556`
  (`SpillingWithFilteringHandler.recover` passes `channelInfo.getInputChannelIdx()` as oldChannelIndex).
- Read/seek by offset: `SequentialChannelStateReaderImpl.java:140-157` (`readSequentially`),
  `188-193` (`extractOffsetsSorted`, sort by file offset), `227-266` (`ChannelStateChunkReader.readChunk`).
- Write/offset record: `ChannelStateCheckpointWriter.java:147-164` (`writeInput`), `219-236`
  (`writeOutput`), `238-258` (`write` → `ChannelStateInvariant.stage("ckptWrite.MEM@off"+offset…)`
  and `offsets.withDataAdded(offset, size)`).
