# Round 1 — verbatim evidence (trimmed)

Source log: `requirements/38544/fix_rounds/round1_FAIL.log` (120,682 lines).
Failing channel: `InputChannelInfo{gateIdx=0, inputChannelIdx=30}`.
Failing task: `failing-map (4/21)#1`, attempt id `..._b8c789ec...._3_1`.

Long `strides=[21,21,...]` lists are trimmed for readability; the full lists were verified
programmatically (see "Stride verification" at the bottom).

---

## A. The failure (consumer path) — line 118310–118327

```
118310  java.io.IOException: Can't get next record for channel InputChannelInfo{gateIdx=0, inputChannelIdx=30}
            at AbstractStreamTaskNetworkInput.emitNext(AbstractStreamTaskNetworkInput.java:162)
            at StreamOneInputProcessor.processInput(StreamOneInputProcessor.java:81)
            ...
118311  Caused by: java.io.IOException: Corrupt stream, found tag: -22
            at StreamElementSerializer.deserialize(StreamElementSerializer.java:222)
            at StreamElementSerializer.deserialize(StreamElementSerializer.java:44)
            at NonReusingDeserializationDelegate.read(NonReusingDeserializationDelegate.java:53)
            at NonSpanningWrapper.readInto(NonSpanningWrapper.java:337)
            at SpillingAdaptiveSpanningRecordDeserializer.readNonSpanningRecord(...:130)
            at SpillingAdaptiveSpanningRecordDeserializer.readNextRecord(...:105)
            at SpillingAdaptiveSpanningRecordDeserializer.getNextRecord(...:95)
            at AbstractStreamTaskNetworkInput.emitNext(AbstractStreamTaskNetworkInput.java:159)
            ... 10 more
```

`-22 = 0xEA` = the 3rd byte of the record header `AB CD EA FC` → the deserializer read a header byte
as a StreamElement tag → byte stream is misaligned. This is the **consumer** path
(`AbstractStreamTaskNetworkInput`), i.e. downstream of channel-state recovery, NOT the
`ChannelStateFilteringHandler`/`VirtualChannel` recovery-filter path.

Recurrences: lines 118311, 118336, 118399, 120111, 120178, 120555, 120647.

---

## B. idx=30 instrumented stages for the failing operator `failing-map (4/21)#1 (..._3_1)` — ALL HEALTHY

### `readChunk.IN` (raw chunk read from chk-7 file during Job3 recovery)
```
line 116929  readChunk.IN@off25996  len=1     headers=0   (NO-HEADER — 1-byte event/spanning fragment)
line 117429  readChunk.IN@off34663  len=4096  headers=195 firstHeaderAt=12  strides all 21
line 117431  readChunk.IN@off38763  len=1741  headers=83  firstHeaderAt=11  strides all 21
line 117460  readChunk.IN@off13813  len=1024  headers=49  firstHeaderAt=2   strides all 21
line 117462  readChunk.IN@off14841  len=1024  headers=49  firstHeaderAt=7   strides all 21
line 117463  readChunk.IN@off15869  len=923   headers=44  firstHeaderAt=12  strides all 21
```
(`firstHeaderAt` differs because fixed 4096-byte chunks start mid-record; stride inside each chunk = 21.)

### `spillSeal.body` (re-spilled segment body just before write to spill file)
```
line 116952  spillSeal.body  len=1     headers=0   (NO-HEADER — 1-byte event)
line 117433  spillSeal.body  len=5837  headers=278 firstHeaderAt=12  strides all 21
line 117465  spillSeal.body  len=2971  headers=142 firstHeaderAt=2   strides all 21
```

### `spillRead.header` (12-byte segment header on read-back; body NOT captured here)
```
line 117939  spillRead.header gate=0 ch=30 bufLen=1     readOff=184
line 118006  spillRead.header gate=0 ch=30 bufLen=5837  readOff=9067
line 118034  spillRead.header gate=0 ch=30 bufLen=2971  readOff=22623
```
(The site logs an empty body `new byte[0]`, so it is always `len=0 NO-HEADER` and reveals nothing about
the read-back body alignment — see `FetchedChannelStateReaderImpl.readHeaderAtCurrent` lines 224–247.)

Note: `filter.IN` is absent for this operator's idx=30 — its data is sealed verbatim into spill segments
and drained, not routed through the rescale filter rewrite. `filter.OUT` is gated off everywhere.

---

## C. Global counts proving no instrumented stage ever sees corruption

```
[CS-INV-CORRUPT] total in log : 0
[CS-INV-ASSERT]  total in log : 0
STRIDE-IRREGULAR total in log : 0
filter.OUT lines              : 0   (output of filter rewrite never captured)
```

Distinct `[CS-INV]` stages present: `ckptWrite.MEM`, `filter.IN`, `readChunk.IN`, `spillRead.header`,
`spillSeal.body`. (No `drain.*`, no `consumer.IN` — the path from spill-read-back body → drain →
consumer is uninstrumented.)

---

## D. Stride verification (programmatic, full lists)

idx=30 across the whole log and for the failing thread, every captured stride equals 21:

```
# all readChunk.IN for failing subtask threads, idx=30:   416 strides, all == 21, 0 irregular
# all spillSeal.body for failing subtask threads, idx=30: 419 strides, all == 21, 0 irregular
# all spillSeal.body for idx=30 (whole log):              5403 strides, all == 21, 0 irregular
# idx=30 STRIDE-IRREGULAR (whole log):                    0
```

The only idx=30 NO-HEADER chunks are `spillRead.header` (len=0, by design) and small event records
(len 1/4/6/11/16, which carry no `AB CD EA FC` header) — none are misaligned record data.

---

## E. Uninstrumented gap — code references for the suspect region

- `FetchedChannelStateReaderImpl.firstSegment()` lines 123–154 — partial-commit resume:
  `deliveredPrefix = current.deliveredBodyBytes()` (127) → `skipBody(deliveredPrefix)` (150) →
  `new BoundedSegmentStream(header.bufferLength - deliveredPrefix, deliveredPrefix)` (151). The body
  bytes handed out here are NOT captured by any `[CS-INV]` stage.
- `FetchedChannelStateDrainer.drainSegment()` lines 122–151 — fills fixed-capacity buffers from the
  segment body stream and delivers via `ch.onRecoveredStateBuffer(buf)` (132, 145). NOT instrumented.
- `AbstractStreamTaskNetworkInput.emitNext` (the consumer that throws) — NOT instrumented.
