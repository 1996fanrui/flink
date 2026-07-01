# Round 5 — Verbatim decisive evidence

Log: `requirements/38544/fix_rounds/round5_FAIL.log` (~60MB). Failing channel
`InputChannelInfo{gateIdx=0, inputChannelIdx=29}`, recovering task `failing-map (12/20)#1`.
Line numbers are the `awk NR` line in the file.

## E1 — The corruption and the seam (decisive)

The two lines are back-to-back at the failure point. A (own input-channel-state) was
delivered first, B (re-typed upstream output) second, on the same channel-29 deserializer.

`[CS-INV-CORRUPT]` (round5_FAIL.log:167962):
```
[CS-INV-CORRUPT] ch=InputChannelInfo{gateIdx=0, inputChannelIdx=29} recordsOkInThisBuffer=0 len=1018 headers=49 firstHeaderAt=2 strides=[21,21,...(48)...,21]
```

`[CS-INV-SEAM]` (round5_FAIL.log:168000):
```
[CS-INV-SEAM] ch=InputChannelInfo{gateIdx=0, inputChannelIdx=29}
  prevTrailingPartial=0
  prevTail=03 34 8C 00 00 00 11 00 00 00 00 00 00 03 2B 64 AB CD EA FC 00 03 2B 64
  prevLastValues=[values=[first: 108288,132291,208860 ... last: 134291,210060,207716] n=12]
  curFirstHeaderAt=2
  curHead=D2 EE AB CD EA FC 00 01 D2 EE 00 00 00 11 00 00 00 00 00 00 01 8A 37 AB
  curFirstValues=[values=[first: 119534,100919,215926 ... last: 124868,123202,127719] n=49]
```

Reading:
- `prevTrailingPartial=0` and `prevTail` ends `... AB CD EA FC 00 03 2B 64` = a complete
  `[marker][value]` → **A ended exactly on a record boundary, no spanning remainder.**
- `curHead = D2 EE | AB CD EA FC ... | 00 00 00 11 ...` → the first 2 bytes `D2 EE` are the
  tail of a record whose head is not in B; marker at offset 2; next clean length prefix
  `00 00 00 11` at offset 8 → **B begins mid-record.** `firstHeaderAt=2`.
- No seam remainder + B starts mid-record ⇒ nothing to "align"; B is head-less.

## E2 — A and B delivery order into channel 29 (drain.OUT), same task instance

Segment A (round5_FAIL.log:107762, thread `channel-state-unspilling-failing-map (12/20)#1`):
```
drain.OUT ch=InputChannelInfo{gateIdx=0, inputChannelIdx=29} len=252 headers=12 firstHeaderAt=13
  strides=[21,21,21,21,21,21,21,21,21,21,21]
  values=[first: 108288,132291,208860 ... last: 134291,210060,207716] n=12
```
Segment B (round5_FAIL.log:107764, same thread, delivered immediately after A):
```
drain.OUT ch=InputChannelInfo{gateIdx=0, inputChannelIdx=29} len=1018 headers=49 firstHeaderAt=2
  strides=[21,...(48)...,21]
  values=[first: 119534,100919,215926 ... last: 124868,123202,127719] n=49
```
A: `firstHeaderAt=13` (record-aligned). B: `firstHeaderAt=2` (mid-record). These are the same
byte values echoed by the `[CS-INV-SEAM]` line's prevLastValues / curFirstValues.

## E3 — Counter-sanity: HEALTHY channels' drain.OUT values are non-monotonic (scattered)

Counter continuity is meaningless — a healthy channel's `values` jump around within one
buffer, including LAST < FIRST.

Healthy `inputChannelIdx=0` (round5_FAIL.log:66699) — LAST value < FIRST value in one buffer:
```
drain.OUT ch=InputChannelInfo{gateIdx=0, inputChannelIdx=0} len=4096 headers=195 firstHeaderAt=12
  values=[first: 121590,122010,122430 ... last: 112927,113347,113767] n=195
```
`121590 (first) > 112927 (last)` — non-monotonic within a single healthy buffer.

Healthy `inputChannelIdx=0` (round5_FAIL.log:66690):
```
drain.OUT ch=InputChannelInfo{gateIdx=0, inputChannelIdx=0} len=4096 headers=195 firstHeaderAt=13
  values=[first: 144352,144772,83814 ... last: 77289,92898,93318] n=195
```
`144352 → 144772 → 83814` — jumps down inside the first three values.

Healthy `inputChannelIdx=29` (the SAME channel index as the failing one, on other tasks):
```
round5_FAIL.log:75121  len=84  values=[first: 55661,56101,119540 ... last: 55821] n=4
round5_FAIL.log:75528  len=105 values=[first: 46741,105120,69140 ... last: 38037,55401] n=5
round5_FAIL.log:76297  len=84  values=[first: 152526,71480,105080 ... last: 46761] n=4
round5_FAIL.log:77909  len=126 values=[first: 100887,101460,53593 ... last: 80703,70454,70614] n=6
```
All scattered/non-monotonic. Confirms the `CCCC` counters carry no contiguity information;
only `firstHeaderAt` / `prevTrailingPartial` (record framing) are valid contiguity signals.

## E4 — Legitimate input-state segments are record-aligned; mid-record starts are the minority

Distribution of `firstHeaderAt` over all `drain.OUT` (5783 lines):
```
   5336  firstHeaderAt=13     <- record-aligned (segment preamble + first frame)
     84  firstHeaderAt=12
     61  firstHeaderAt=11
     ...
      6  firstHeaderAt=2      <- mid-record (the failing B is one of these)
      ...
```
The overwhelming majority start record-aligned (`firstHeaderAt=13`); the low-value starts
(2,3,4,...) are the head-less re-typed-output segments. All genuine input-state segments for
channel 29 (E2 A, and healthy-channel-29 rows in E3) show `firstHeaderAt=13`.

## E5 — Code: re-typing collides output onto input identity, no boundary metadata

`flink-runtime/.../checkpoint/TaskStateAssignment.java:600-636`
```
621  InputChannelInfo inputChannelInfo =
622      new InputChannelInfo(gateIdxResultPartition, oldUpstreamSubtaskIndex);
624  InputChannelStateHandle upstreamOutputBufferHandle =
625      new InputChannelStateHandle(
626          oldDownstreamSubtaskIndex,
627          inputChannelInfo,
628          stateHandle.getDelegate(),
629          stateHandle.getOffsets(),
630          stateHandle.getStateSize());
```
Upstream OUTPUT is re-typed as InputChannelStateHandle on `inputChannelIdx = oldUpstreamSubtaskIndex`.
Only delegate/offsets/size are carried — no record-boundary / first-record-offset / seed.

## E6 — Code: two reads into one handler; per-channel deserializer

`flink-runtime/.../checkpoint/channel/SequentialChannelStateReaderImpl.java:88-100`
```
88   try (stateHandler) {
90       read(stateHandler,
92           groupByDelegate(streamSubtaskStates(),
93               ChannelStateHelper::extractUnmergedInputHandles));   // segment A (input-channel-state)
95       read(stateHandler,
97           groupByDelegate(streamSubtaskStates(),
98               OperatorSubtaskState::getUpstreamOutputBufferState));  // segment B (re-typed output)
```
Both reads target the same `stateHandler`; A and B for one physical channel key to the same
mapped `InputChannelInfo` and are consumed by one spanning deserializer.

`flink-runtime/.../streaming/runtime/io/AbstractStreamTaskNetworkInput.java`
```
75   protected final Map<InputChannelInfo, R> recordDeserializers;
385  protected R getActiveSerializer(InputChannelInfo channelInfo) {
386      return recordDeserializers.get(channelInfo);
```
One `SpillingAdaptiveSpanningRecordDeserializer` per `InputChannelInfo`, carrying spanning
state across the A→B delivery.
