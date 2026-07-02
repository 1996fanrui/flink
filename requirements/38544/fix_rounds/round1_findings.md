# Round 1 findings — channel-state corruption (branch `38544-spilling-v2/20260702-01-check-data-corruption`)

Analyzed logs: `repro/results/FAIL_w6_1.log`, `repro/results/FAIL_w11_2.log` (backup in
`repro/results-round1-instrumented-*`). Independent verification per HL doc §5/§6; verdict standard: strict.

## VERDICT: CONCLUSIVE

The corruption is pinned, with exact byte accounting in both logs, to **one step**: the **downstream
input-channel state capture at checkpoint time, while the channel is still in recovery mode**
(the SNAPSHOT/(a)-collection + persist-window layer that feeds `CHECKPOINT_WRITE(input)`).
Live network bytes that the upstream had already sent before the barrier are **not persisted** for
in-recovery channels; the upstream's `CHECKPOINT_WRITE(output)` correctly excludes those same bytes
as "already sent". The checkpoint's input/output fragment pair is therefore **mutually inconsistent**
(a hole of ≥11–16 bytes at the pass-1/pass-2 seam). Every stage downstream of the write is proven
byte-faithful: `RECOVER_READ` = exact concatenation of the two fragments (byte sums match to the byte),
and `RECOVER_REWRITE` / `CHANNEL_RECEIVE` carry identical bytes. The first *validator* to fire is
`RECOVER_READ` (per-fragment validators structurally cannot see a missing tail — LL doc §3.1/§3.3),
but the cross-fragment arithmetic shows the healthy→corrupt transition happened at capture/write time,
not read time.

Caveats honored: no presupposed root cause used; value-order (non-monotonic per channel, HL §8.8) was
*not* used as a corruption signal — only record framing (stride/firstHeaderAt/CORRUPT-RECORD-AT).

---

## 1. Corrupt channels and failure surface

### FAIL_w6_1 (jobs: fb04d8d4 → d937a969 → 96c8941a)
- Final failure in job3 `96c8941a`, attempt #1: `java.io.IOException: Can't get next record for channel
  InputChannelInfo{gateIdx=0, inputChannelIdx=29}` / `Corrupt stream, found tag: -22` (L84603/L84615),
  thrown at task consumption time (recovery itself passed — no filtering deserialization on this path).
- Corrupt channel: **failing-map subtask 4, gate 0, channel 29** (upstream = `rescale0` subtask 29,
  subpartition 4). Vertices: failing-map = `b8c789ec…e6fd`, rescale0 = `7b4c6fe4…a96e`.

### FAIL_w11_2 (jobs: 78f1fd28 → e0dbc057 → e41ec78f)
- Failures in job3 `e41ec78f`, **during recovery** (both attempts, deterministic — both restored the same
  `Savepoint 7` = job2 `chk-7`, L51269/L61689): `Corrupt stream, found tag: -15/-22/65` thrown from
  `VirtualChannel.getNextRecord` inside `SpillingWithFilteringHandler.recover → filterAndRewrite`
  (first at L58846; rescale 21→20 / 31→30 makes the filter deserialize records during recovery, so the
  same defect explodes earlier than in w6_1).
- Corrupt channels (real, STRIDE-IRREGULAR): **(ch5, oldSubtask=11), (ch29, oldSubtask=8),
  (ch7, oldSubtask=6)**; plus a fourth instance **(ch15, oldSubtask=6)** manifesting as a head-orphan
  (fha=18, CORRUPT-RECORD-AT=0 — also real under STRICT recovery rules).

---

## 2. Per-stage chain (time order)

### FAIL_w6_1 — channel (failing-map sub4, ch29)

Job3 attempt #0 restored from job2's `chk-8` ("Savepoint 8", L61995). Restore of this channel was **clean**:

- L67545 `[CS-INV] … layer=RECOVER_READ … old=InputChannelInfo{gateIdx=0, inputChannelIdx=29} oldSubtask=4
  bytes=3339 headers=159 firstHeaderAt=13 strides=[21…]` (159 whole records; REWRITE identical)
- L68613 `[CS-INV] … layer=CHANNEL_RECEIVE … bytes=3339 … parsedRecords=159` (ts 52999 — delivery done)

Checkpoint 9 triggered @55208 (L70079), completed L71559; failing-map designed failure L71823; attempt #1
restored from **job3's own chk-9** (L78772). Stages for cp9:

| stage | line | key facts |
|---|---|---|
| SNAPSHOT | L70779 | `[CS-INV-SNAP] failing-map (5/20)#0 ch29 kind=Remote cp=9 numBuffers=3 bytes=1512 headers=72 firstHeaderAt=13` — the (a) collection = 72 whole records = unconsumed tail of the 159 recovered records. **No live bytes.** |
| CHECKPOINT_WRITE (input) | L71433 | `task=b8c789ec…-4-cp9 ch=…Idx=29 … bytes=1512 headers=72 firstHeaderAt=13` — **write == snapshot, byte-for-byte (1512 = 72×21, ends on a record boundary). No ASSERT.** Zero (b) spill-replay bytes, zero (c) maybePersist bytes. |
| CHECKPOINT_WRITE (output, upstream) | L71243 | `task=7b4c6fe4…-29-cp9 ch=ResultSubpartitionInfo{partitionIdx=0, subPartitionIdx=4} mode=LENIENT bytes=1795 headers=86 firstHeaderAt=2` — 1795 = **10-byte head partial + 85 whole records**. fha=2 ⇒ the fragment starts 11 bytes into a record ⇒ upstream accounts **11 bytes as already sent downstream**. Tolerated at write (legitimate for output). |
| RECOVER_READ (att #1) | L82968/82969 | `bytes=3307 headers=158 … strides=[21×71, **10**, 21×85] *** STRIDE-IRREGULAR *** parsedRecords=72 *** CORRUPT-RECORD-AT=1512 before=[00 00 00 00 00 01 D5 07 AB CD EA FC 00 01 D5 07] after=[C0 0E AB CD EA FC 00 01 C0 0E 00 00 00 11 00 00]` — **3307 = 1512 + 1795 exactly; break exactly at the pass-1/pass-2 seam; headers 158 = 72+86.** |
| RECOVER_REWRITE | L82999/83000 | identical (bytes=3307, CORRUPT-RECORD-AT=1512, same hex) — rewrite added/lost nothing. |
| CHANNEL_RECEIVE | L83718/83719 | identical — spill round-trip + drain re-chunking added/lost nothing. |
| task consume | L84603+ | `Can't get next record for channel …Idx=29` / `found tag: -22` (0xEA = header byte 3 — deserializer window landed in a header, consistent with the seam misalignment). |

**Healthy sibling (same checkpoint, same upstream task) — proves the stitch model and exonerates the reader
and the upstream accounting:** failing-map **sub8** ch29: input fragment L71521 `bytes=342` = 16 whole records
+ **6-byte trailing partial** (cra=336); upstream output `…-29-cp9 sub8` L71247 `bytes=15 headers=1 fha=7` =
**15-byte head partial**. 6 + 15 = 21 — the pair stitches perfectly, and sub8 recovered without any assert at
attempt #1. Sub8's live bytes were persisted (its channel had finished consuming its recovered data before
barrier 9 — no SNAP(a) line exists for it); sub4's were not (72 recovered records still unconsumed at barrier).

### FAIL_w11_2 — three channels, written by job2 `e0dbc057` at cp7 (~3.8 s after job2's restore, i.e. checkpoint-during-recovery), read by job3

Job2's own recovery from job1 `chk-5` was clean: **0** RECOVER_READ/REWRITE/RECEIVE asserts in job2.

| channel (old) | SNAPSHOT (job2, cp7) | CHECKPOINT_WRITE input | CHECKPOINT_WRITE output (upstream) | RECOVER_READ (job3, att#0 = att#1 identical) |
|---|---|---|---|---|
| ch5 / oldSub=11 | L39557: nbuf=1 **bytes=1178 fha=15** (2-byte head orphan) | L40933: `…-11-cp7` **bytes=1197 fha=13** = 57 whole records. 1197 = **19-byte deserializer prefix + 1178** (head healed: 19+2=21) | L40753: `rescale0-5-cp7 sub11` **bytes=1501 fha=2** = 10-byte head partial + 71 records | L59391/59392 (att1: L68772): `bytes… headers=75 … stride 10 @ seam, CORRUPT-RECORD-AT=1197` **= input length exactly**; hex `before=[…00 02 D5 62 AB CD EA FC 00 02 D5 62] after=[AE D5 AB CD EA FC 00 02 AE D5 00 00 00 11…]` |
| ch29 / oldSub=8 | L39547: nbuf=1 **bytes=2016 fha=13** | L40978: `…-8-cp7` **bytes=2016 fha=13** = 96 whole (**write == snapshot**) | L40918: `rescale0-29-cp7 sub8` **bytes=1900 fha=2** = 10 + 90×21 | L59552/59553 (att1: L70167): `CORRUPT-RECORD-AT=2016` **= input length**; `before=[…00 00 F7 A4 …] after=[F6 CC AB CD EA FC 00 01 F6 CC 00 00 00 11…]` |
| ch7 / oldSub=6 | L39537: nbuf=1 **bytes=667 fha=8** (5-byte-consumed head) | L40983: `…-6-cp7` **bytes=672 fha=13** = 32 whole (= 5-byte prefix + 667, head healed) | L40705: `rescale0-7-cp7 sub6` **bytes=934 fha=2** = 10 + 44×21 | L59716/59717 (att1: L68130): `bytes=1606 = 672+934 exactly, headers=77=32+45, CORRUPT-RECORD-AT=672` **= input length**; `before=[…00 01 22 98 …] after=[08 6E AB CD EA FC 00 02 08 6E 00 00 00 11…]` |
| ch15 / oldSub=6 | *(no SNAP line — (a) empty)* | **no input fragment written at all** | L40811: `rescale0-15-cp7 sub6` **bytes=173 fha=18** = **5-byte head orphan** + 8 records ⇒ upstream accounts **16 bytes sent** | L59735 (att1: L68150): `headers=8 fha=18 parsed=0 CORRUPT-RECORD-AT=0` — stream **is** the bare output fragment; the 16 sent bytes exist nowhere. |

The prefix-healing observed on ch5 (+19) and ch7 (+5) proves the writer *does* prepend the deserializer's
unconsumed spanning prefix (head partials heal at write). What is missing in every corrupt instance is the
**live tail** — the bytes the upstream had already sent before the barrier.

No earlier stage shows real corruption in either log: zero STRIDE-IRREGULAR at CHECKPOINT_WRITE anywhere
(all 541/1153 write ASSERTs are head/tail edge partials at per-cp fragment granularity, the known §3.1 noise;
verified none has a mid-stream break — headers ≈ parsedRecords, cra ∈ {0, parsed×21}); zero SNAPSHOT ASSERTs.
The two `parsedRecords=1`-style write asserts in w11_2 job1 (L13580/L13598, failing-map's own output edge at
cp4) are on a different edge, at a checkpoint that was never restored (job2 restored cp5), and job2's recovery
read everything clean — not in the corrupt lineage.

## 3. Byte-level decode of the break (all five instances, same signature)

Record layout: `[4B len=0x11][1B tag][8B ts][AB CD EA FC][4B value]` = 21 bytes, header at record offset 13
(ts == value in this test).

At the seam offset (== input-fragment length in every case), the stream is:
`…[complete record N of input fragment][10-byte orphan: 2 ts bytes + AB CD EA FC + value][healthy 00 00 00 11 …]`
⇒ exactly the **first 11 bytes of one record are missing** (len 4 + tag 1 + ts 6); the single stride of 10
(= 21−11) and firstHeaderAt=2 on the output fragments are the same fact seen from three angles. The ch15
instance is the same defect with a 16-byte hole (output fha=18 ⇔ 5-byte orphan).

Value analysis across the seams (value = 4 bytes after `AB CD EA FC`):

| log / channel | last value before seam | orphan record value | jump |
|---|---|---|---|
| w6_1 ch29/sub4 | 0x0001D507 = 120071 | 0x0001C00E = 114702 | −5369 (backward) |
| w11_2 ch5/sub11 | 0x0002D562 = 185698 | 0x0002AED5 = 175829 | −9869 (backward) |
| w11_2 ch29/sub8 | 0x0000F7A4 = 63396 | 0x0001F6CC = 128716 | +65320 (forward) |
| w11_2 ch7/sub6 | 0x00012298 = 74392 | 0x0002086E = 133230 | +58838 (forward) |

Jumps go both directions ⇒ per-channel value order is non-monotonic (rescale/keyBy mixing; HL §8.8), so the
values carry no corruption signal of their own — the defect is purely a **framing hole at the seam between
the two data sources** (pass 1 = the checkpoint's input state; pass 2 = upstream output state redistributed
as input, LL §2.4). There is **no overlap and no byte mutation**: byte sums are exact on every hop.

Note on loss size: the framing only exposes the *partial-record* part of the hole. If whole live records were
also sent pre-barrier and dropped, they vanish silently (stride stays 21). The observed 11/16-byte holes are a
lower bound; the same defect can produce pure record loss without any "Corrupt stream" (matching this repro's
other failure mode, the NUM_OUTPUTS/NUM_INPUTS assertion).

## 4. Cross-validation

Same first-corrupt stage in both logs (RECOVER_READ as first *validator* hit; capture/write as first *factual*
divergence), same seam signature (break exactly at input-fragment length; output fragment fha=2/18; missing
record head; single short stride), five independent instances across two logs and two different writer jobs
(job3-cp9 in w6_1; job2-cp7 in w11_2), both checkpoints taken while the affected downstream channels still
held unconsumed recovered data (nonempty (a) collections; w11_2 cp7 fired ~3.8 s into job2's recovery).
All corrupt channels are `kind=Remote`.

## 5. Implicated code step (named only as far as the evidence carries)

The logs prove: for a channel with `inRecovery` semantics at `checkpointStarted`, the persisted input state
consists of **only** `[deserializer prefix] + [(a) pre-RecoveryCheckpointBarrier recovered buffers]`, and
**zero live network bytes**, even though the upstream's own output capture proves live bytes had been sent
pre-barrier (FIFO ⇒ they arrive before the channel's real barrier and must be persisted per UC semantics).

Read-only code inspection matches this exactly —
`flink-runtime/src/main/java/org/apache/flink/runtime/io/network/partition/consumer/RemoteInputChannel.java`,
`checkpointStarted(CheckpointBarrier)` (≈L912–952):

```java
channelStatePersister.startPersisting(barrier.getId(), toPersist);
if (inRecovery) {
    // Recovered inflight buffers are collected in one shot and the upstream sends no
    // data during recovery, so close the persist window immediately ...
    channelStatePersister.stopPersisting(barrier.getId());
}
```

The eager `stopPersisting` closes the persist window at checkpoint start, so
`channelStatePersister.maybePersist(buffer)` in `onBuffer` (≈L844) — the (c) path that is supposed to catch
live buffers arriving between checkpoint start and the channel's real barrier — becomes a no-op. The code
comment's premise ("the upstream sends no data during recovery"; likewise `collectPreRecoveryBarrier`'s
javadoc "upstream has no credit … no live data buffers") is **falsified by the logs**: in w6_1 the upstream
had been sending for ~2.3 s (recovered delivery for the channel finished at ts 52999; barrier at ts 55245)
while the channel still counted as in-recovery because 72 recovered records were unconsumed. The healthy
sibling (sub8, not in recovery at the barrier) shows the same (c) path persisting live bytes correctly and
stitching 6+15=21 with the upstream fragment.

Bug locus, stated precisely: **between the runtime channel ground truth and stage
SNAPSHOT/CHECKPOINT_WRITE(input)** — the in-recovery branch of `RemoteInputChannel.checkpointStarted`
(and the persist-window lifecycle around `ChannelStatePersister`) drops all live pre-barrier data for
channels that still hold recovered data (or, ch15 variant, whose recovery sentinel is still pending with an
empty (a)). Stages CHECKPOINT_WRITE→RECOVER_READ→RECOVER_REWRITE→CHANNEL_RECEIVE are byte-faithful
(exonerated by exact sums). `LocalInputChannel.checkpointStarted` has a different window lifecycle
(no eager stop); all observed corrupt instances are Remote, so no claim is made about the Local path.

Not established by this round (does not affect the verdict; relevant for the fix): whether, at the instant of
`checkpointStarted`, the live bytes were already queued in `receivedBuffers` after the sentinel (and were
skipped by `collectPreRecoveryBarrier` stopping at the sentinel) or arrived afterwards via `onBuffer` (and
were skipped because the window was closed). Both paths funnel into the same missing-(c) outcome; a one-line
counter of "live data buffers behind the sentinel at collection time" would distinguish them if desired.
