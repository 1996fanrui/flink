# Round 1 — Root-cause analysis: channel-state byte misalignment (idx=30)

## Verdict: **INSUFFICIENT** (break is bounded to an uninstrumented region, not yet pinpointed)

The captured log proves that **every instrumented channel-state stage for the failing channel
`InputChannelInfo{gateIdx=0, inputChannelIdx=30}` is byte-perfect (stride 21, zero
`STRIDE-IRREGULAR`)**, while the consumer that reads the recovered bytes
(`AbstractStreamTaskNetworkInput.emitNext`) fails with `Corrupt stream, found tag: -22`.
Because the corruption is NOT visible at any instrumented stage, it is introduced in the
**uninstrumented gap between the last instrumented stage (`spillSeal.body` write / `spillRead.header`
read-back) and the consumer's network input** — i.e. inside the spill read-back **body delivery**
(`FetchedChannelStateReaderImpl` body stream + `FetchedChannelStateDrainer.drainSegment` →
`onRecoveredStateBuffer`) and/or the consumer-side input buffer assembly. The log does not contain
the healthy→corrupt transition, so I will **not** declare CONCLUSIVE.

---

## 1. Environment / phase reconstruction

- The whole FAIL log is a single MiniCluster JVM. The leading number on each line is a relative
  timestamp (ms). It is **monotonic across all three test jobs**, so phases are ordered by timestamp,
  not by a JVM reset.
- `[CS-INV]` stages present in the log (whole file): `ckptWrite.MEM`, `filter.IN`, `readChunk.IN`,
  `spillRead.header`, `spillSeal.body`. **`filter.OUT` is gated off (0 lines)** — the filter rewrite
  OUTPUT bytes are never captured. **0 `[CS-INV-CORRUPT]`, 0 `[CS-INV-ASSERT]`, 0 `STRIDE-IRREGULAR`
  in the entire 120k-line log.**
- Failing task: `failing-map (4/21)#1`, attempt id `..._3_1` (new subtask index 3). Recovery work for
  this task runs on thread `channel-state-unspilling-failing-map (4/21)#1 (..._3_1)-thread-1`.
- Failure: line **118311** (and recurrences 118336, 118399, 120111, 120178, 120555, 120647). Stack is
  the **consumer** path, not the recovery filter path:
  `AbstractStreamTaskNetworkInput.emitNext:159` → `SpillingAdaptiveSpanningRecordDeserializer.getNextRecord`
  → `StreamElementSerializer.deserialize:222` → `Corrupt stream, found tag: -22`. `-22 = 0xEA` = the
  3rd header byte of `AB CD EA FC`, i.e. the read window has slid into a header region.

### Three test phases (data-flow), per the instrumentation design

```
Job1 (chk-5)      : in-flight memory → checkpoint file        (ckptWrite.MEM)
Job2 (recover 5)  : readChunk → filter/spill → drain          (readChunk.IN, filter.IN, spillSeal.body)
       (write 7)  : ckptWrite.MEM (+ SPILL for undrained tail) → checkpoint file
Job3 (recover 7)  : readChunk → spill → drain → ★consumer fails★
```

## 2. idx=30 stage chain (failing subtask thread `_3_1`)

The failing recovery thread emits **`readChunk.IN`, `spillRead.header`, `spillSeal.body`** for idx=30
— and **no `filter.IN`** (filter.IN is keyed by `SubtaskConnectionDescriptor` and is only logged on
threads driving the `SpillingWithFilteringHandler`; the failing thread's idx=30 data is sealed
verbatim into spill segments, then read back and drained to the consumer).

| Stage | What it captures | idx=30 result (failing thread `_3_1`) |
|---|---|---|
| `readChunk.IN@off…` | raw chunk read from chk-7 file, after `serializer.readData`, before `recover()` | **HEALTHY** — 416 strides, all 21, 0 irregular (lines 113913, 116929, 117429, 117431, 117460, 117462, 117463). `firstHeaderAt` varies (12/11/2/7/12) only because 4096-byte chunks start mid-record — stride inside each chunk is still 21. |
| `spillSeal.body` | re-sealed spill segment body just before it is written to the spill file | **HEALTHY** — 4 segments for idx=30 from `_3_1`, 419 strides, all 21, 0 irregular. (Whole-log idx=30: 5403 strides, all 21.) NO-HEADER entries are tiny `len=4`/`len=16` event records (events carry no `AB CD EA FC`) — benign. |
| `spillRead.header` | the 12-byte segment header on read-back (gate/ch/bufLen/readOff) | logs an **empty body** (`new byte[0]`), so it is always `len=0 NO-HEADER` by construction — it does **not** capture the read-back body. For `_3_1` idx=30: bufLen=42/1/5837/2971 at readOff=921/184/9067/22623. |

Whole-log idx=30 stage totals: `ckptWrite.MEM`=176, `readChunk.IN`=211, `spillSeal.body`=119,
`spillRead.header`=119. NO-HEADER lines: 119 are `spillRead.header` len=0 (by design), the rest are
small event/spanning fragments (len 1/4/6/11/16). **No NO-HEADER chunk for idx=30 carries
header-bearing record data that lost alignment, and no idx=30 line anywhere is STRIDE-IRREGULAR.**

## 3. Correlating the corrupt consumer read to recovered data

The consumer failure at line 118311 reads idx=30 bytes that were recovered in Job3. Every instrumented
stage on the path to that consumer is healthy:

- raw file read (`readChunk.IN`) — healthy
- re-spilled body written (`spillSeal.body`) — healthy
- segment header read back (`spillRead.header`) — header fields sane (bufLen ≥ 0)

The corruption therefore appears **only at the consumer**, downstream of the last instrumented byte
capture. There is **no instrumentation** on:

1. the spill read-back **body** delivered to the consumer
   (`FetchedChannelStateReaderImpl.firstSegment`/`followingSegment` → `BoundedSegmentStream`
   `bodyStream()`), and
2. the drainer's re-chunking + delivery
   (`FetchedChannelStateDrainer.drainSegment` → `fill` → `ch.onRecoveredStateBuffer(buf)`), and
3. the consumer's network input buffer (`AbstractStreamTaskNetworkInput.emitNext`).

So the healthy→corrupt transition happens inside that gap. (`grep` confirms 0 `ChannelStateInvariant`
call sites in `FetchedChannelStateDrainer.java` and `AbstractStreamTaskNetworkInput.java`.)

## 4. Strongest mechanistic suspect (NOT proven by the log)

`FetchedChannelStateReaderImpl.firstSegment()` (file:lines 123–154) handles snapshot resume of a
**partially-drained** segment. chk-7 (Job2) is the first checkpoint that captures channel-state mid-drain,
so a segment can be partially delivered before the barrier. On Job3 read-back the code computes
`deliveredPrefix = current.deliveredBodyBytes()`, rewinds to the segment header, reads it, then
**skips `deliveredPrefix` body bytes** and hands out `bufferLength - deliveredPrefix`:

```
int deliveredPrefix = (int) current.deliveredBodyBytes();   // line 127
current.rewindToSegmentStart();                              // line 128
SegmentHeader header = readHeaderAtCurrent();                // line 133
skipBody(deliveredPrefix);                                   // line 150
currentBody = new BoundedSegmentStream(header.bufferLength - deliveredPrefix, deliveredPrefix); // 151
```

If `deliveredPrefix` is off by N bytes (e.g. it accounts for body-only vs. header-inclusive offsets, or
mixes the MEM-delivered prefix with the SPILL remainder at the MEM/SPILL seam), the body handed to the
consumer starts N bytes off a record boundary → the deserializer reads a header byte (`0xEA`) as a tag →
`-22`. This is consistent with the symptom (header byte read as tag) and with chk-7 being the only
checkpoint mixing MEM + SPILL. **But the log captures neither `deliveredPrefix` nor the body bytes
actually handed out, so this is a hypothesis, not proof.** `spillRead.header` shows non-trivial
`readOff` (921/184/9067/22623) and a `bufLen=1` segment, both compatible with a resume/seam, but they do
not pin the break.

## 5. Why not CONCLUSIVE

The rule is: only CONCLUSIVE if the log contains the healthy→corrupt transition for idx=30. It does not.
Every instrumented stage is healthy; the corrupt bytes are first observed at an uninstrumented consumer
read. "The resume/skip code looks wrong" is not proof. → **INSUFFICIENT.**

---

## 6. Next-round instrumentation spec (add exactly these, then the break is pinpointed)

The gap to close is **read-back body → drain → consumer**. Add three captures so the next reproduction
shows which transition breaks alignment:

### A. Spill read-back BODY (the missing twin of `spillSeal.body`)
- **File:** `flink-runtime/.../checkpoint/channel/FetchedChannelStateReaderImpl.java`
- **Where:** in `firstSegment()` (after line 151) and `followingSegment()` (after line 166), capture the
  body that will be delivered. Because `BoundedSegmentStream` is consumed lazily by the drainer, the
  clean approach is to read the whole remaining body into a `byte[]` for the diagnostic (buffer it and
  re-serve it), then:
  ```
  ChannelStateInvariant.stage("spillRead.body deliveredPrefix=" + deliveredPrefix
        + " bufLen=" + header.bufferLength + " readOff=" + current.readOffset,
        header.channelInfo, body, 0, body.length);
  ```
- **Assert:** the delivered body for a record-bearing channel must be stride-21 from offset 0 when
  `deliveredPrefix == 0`; when `deliveredPrefix > 0` the FIRST header must land at
  `(21 - (deliveredPrefix % 21)) % 21` (i.e. the resume must keep records aligned). Use
  `ChannelStateInvariant.assertEq("spillRead.resumeAlign", header.channelInfo, "firstHeaderOffset",
  expectedFirstHeader, actualFirstHeader)` so it fail-fasts at the exact segment.

### B. Drainer per-buffer delivery to the consumer
- **File:** `flink-runtime/.../checkpoint/channel/FetchedChannelStateDrainer.java`
- **Where:** in `drainSegment`, immediately before each `ch.onRecoveredStateBuffer(buf)` (lines 132 and
  145), dump the buffer that will reach the consumer:
  ```
  ChannelStateInvariant.stage("drain.OUT ch=" + ch.getChannelInfo(), ch.getChannelInfo(),
        buf.getNioBufferReadable());
  ```
- **Why:** this is the last point before the consumer. If `spillRead.body` (A) is healthy but
  `drain.OUT` is irregular, the bug is the re-chunking/`fill` in the drainer; if `drain.OUT` is already
  irregular and matches A, the bug is upstream in the resume skip (A); if both A and B are healthy, the
  bug is on the consumer input-assembly side (C).

### C. Consumer input (confirm where bytes finally break)
- **File:** `flink-runtime/.../streaming/runtime/io/AbstractStreamTaskNetworkInput.java`
- **Where:** in `emitNext`, just before handing the buffer's bytes to
  `SpillingAdaptiveSpanningRecordDeserializer` (around line 159), capture the buffer for the recovered
  channel:
  ```
  ChannelStateInvariant.stage("consumer.IN ch=" + channelInfo, channelInfo, dataBufferBytes);
  ```
  and ungate `filter.OUT` (or rely on A/B) so all four points (`spillSeal.body` → `spillRead.body` →
  `drain.OUT` → `consumer.IN`) form a contiguous chain with no gap.

With A+B+C in place, the next FAIL log will show the exact stage where stride-21 becomes irregular,
plus (via the assert in A) the precise `deliveredPrefix`/offset arithmetic that is wrong — turning this
into a CONCLUSIVE pin.
