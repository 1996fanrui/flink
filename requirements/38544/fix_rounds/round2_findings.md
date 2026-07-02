# Round 2 findings — fix validation for b3e9702 (branch `38544-spilling-v2/20260702-01-check-data-corruption`)

Independent verification of the fix
`b3e9702 [FLINK-38544][checkpoint] Keep remote persist window open when checkpoint starts during recovery`
(removes the premature `channelStatePersister.stopPersisting` in the in-recovery branch of
`RemoteInputChannel.checkpointStarted`; mirrors the earlier Local-side fix `201a5f`).
Round-1 root cause (CONCLUSIVE, see `round1_findings.md`): with the eager `stopPersisting`, live
pre-barrier network bytes arriving on an in-recovery channel were dropped from the checkpoint's
input state, leaving a framing hole exactly at the input/upstream-output seam on restore
(`W == S`, upstream head-orphan unmatched, `STRIDE-IRREGULAR` at `RECOVER_READ`, `Corrupt stream`).

Verifier did not write the fix; all statements below are re-derived from the retained logs and the
current code. Analysis scripts: session scratchpad (`verdict.py` and predecessors), regex-parsing
every `[CS-INV*]` line and joining SNAPSHOT / CHANNEL_RECEIVE / CHECKPOINT_WRITE / RECOVER_READ per
channel instance.

## VERDICT: FIX-CONFIRMED

- The risky scenario (checkpoint starts while channels are still in recovery) occurred, provably,
  thousands of times across the retained logs, on both Local and Remote channels — including on the
  checkpoint that was actually restored (cp7) in all 4 verify runs.
- Post-fix, in-recovery channels **do** persist live pre-barrier bytes: 159 provably-in-recovery
  channels wrote `CHECKPOINT_WRITE(input) > SNAPSHOT` with the surplus byte-decomposable as live
  records (157 Local + **2 Remote — the exact round-1 corrupt channel class (`failing-map` ←
  `7b4c6fe4`), both with a mid-record seam that stitches `p+q=21` against the upstream fragment**,
  see §3.3).
- Every restore validates STRICT-clean: **0 `STRIDE-IRREGULAR`, 0 recovery-chain
  `[CS-INV-ASSERT]`, 0 `Corrupt stream`** across 71 contention runs (monitored live) + 5 retained
  fully-analyzed runs; all exact-matchable `RECOVER_READ` streams equal `W(input) + U(upstream
  output)` to the byte, including 3 576 read-instances of channels proven in-recovery at write time.
- Honest scope limits are in §6 (most notably: the single Remote live-byte instance is verified at
  the write layer, its checkpoint was not itself restored; prime.log alone would have been
  insufficient — the risky live-byte scenario is absent there).

## 1. Run statistics

| campaign | runs | result | corruption signals |
|---|---|---|---|
| `repro/repro.sh 12 60 300` (12 workers, CPU contention) | **71** | PASS=71 FAIL=0 INFRA=0 | live monitor on every run's log: 0 `STRIDE-IRREGULAR`, 0 recovery-chain `[CS-INV-ASSERT]` (logs of passing runs deleted by repro.sh; only `prime.log` retained) |
| `repro/results/prime.log` (online prime, no contention) | 1 | PASS (22.16 s) | 0 / 0 / 0 (`Corrupt stream`=0) |
| `repro/verify_run{1..4}.log` — **4 extra runs executed by this verifier**, sequential, same single narrowed parameter (`upscale PIPELINE 20→21`) | 4 | all PASS (21.7–22.9 s, `Tests run: 1, Failures: 0, Errors: 0`) | 0 / 0 / 0 each |

Pre-fix baselines on this branch: failure rate ~5–10 %/run under contention; round-1 instrumented
loop hit 2 FAIL in 18 runs (~11 %). Probability of 71 clean contention runs if the bug were
unfixed: (1−0.05)^71 ≈ 2.6 % down to (1−0.11)^71 ≈ 0.02 %.

Noise-class asserts (expected, per lowlevel doc §3.1/§3.3): per run ~18 400–19 800
`[CS-INV-ASSERT] layer=CHECKPOINT_WRITE` (single-cp fragment edge partials; **0** of them
stride-irregular; head-partial=0, tail-partial≈13 960 of 27 161 input fragments in prime) and 1–10
`layer=SNAPSHOT` asserts (LENIENT parser desync on head-partial snapshots: strides all constant 21,
`parsedRecords=1`, `CORRUPT-RECORD-AT` at a non-record offset; hex decodes to healthy records).

## 2. How "in recovery at checkpointStarted" was proven from logs (no in-recovery marker is logged)

- **Local rule**: `LocalInputChannel.checkpointStarted` non-recovery branch sets
  `toPersist = Collections.emptyList()` and `validateSnapshot` is only called on non-empty
  collections ⇒ **every `[CS-INV-SNAP] ... kind=Local` line is an in-recovery (a)-collection.**
- **Remote rule `S==R_recv`**: a Remote SNAP whose `bytes/headers/firstHeaderAt` exactly equal the
  channel's `CHANNEL_RECEIVE` flush (same execution attempt) is the *entire recovered stream still
  unconsumed in `receivedBuffers`* at the barrier ⇒ in-recovery. (During Remote recovery upstream
  data is credit-gated until `onRecoveredStateConsumed`; a live-queue snapshot byte-, record- and
  offset-identical to a multi-KB recovered stream is not otherwise possible.) Delivery finished ⇒
  spill-replay (b) = 0 and deserializer prefix = 0 ⇒ **`W − S` = live (c) bytes exactly.**

Proven in-recovery (a)-collections per retained run (Local + Remote(S==R_recv)); job2 = writer of
the restored cp7:

| run | job2 cp6 | job2 cp7 (restored!) | job2 cp8 | job3 cps | total | with live bytes (W>S+prefix) |
|---|---|---|---|---|---|---|
| prime | 522L+51R | — (none) | — | 11L+2R @cp8; 1R each @cp12/13/19 | 589 | 22 (all Local) |
| run1 | 513L+49R | **361L+27R** | 68L+3R | 243L+53R @cp8; **1R @cp11**/13/15 | 1 321 | 43 (42 Local + **1 Remote**) |
| run2 | 505L+39R | **364L+29R** | 152L+10R | 55L+5R @cp8; 1R @cp11/12/14 | 1 162 | 38 (all Local) |
| run3 | 488L+11R | **350L+6R** | 145L | 24L @cp8; **1R @cp15** | 1 025 | 33 (32 Local + **1 Remote**) |
| run4 | 530L+9R | **395L+4R** | 165L | 150L+4R @cp8; 1R @cp14/16 | 1 259 | 23 (all Local) |

## 3. Byte accounting — the fix's effect observed directly

### 3.1 Local exemplar, full write→restore chain (run1; restored twice)

Channel: `upscale0` subtask 8 (`(9/42)#0`), gate 0, ch 19, kind=Local; job2 `e6f400a1`, cp7 —
the checkpoint job3 `ec02c841` restored in both attempts ("Restoring job … from Savepoint 7").

```
L45257  ts 7211 [CS-INV]      ... ch=...Idx=19} kind=Local layer=CHANNEL_RECEIVE bytes=3717 headers=177 fha=13   <- recovered stream, delivery done
L62889  ts 8515 [CS-INV-SNAP] ... ch=...Idx=19} kind=Local layer=SNAPSHOT cp=7 numBuffers=1 bytes=1722 headers=82 fha=13   <- (a): unconsumed tail (82 of 177 records)
L62963  ts 8515 [CS-INV] task=7559af08...-8-cp7 ch=...Idx=19} layer=CHECKPOINT_WRITE mode=STRICT bytes=3192 headers=152 fha=13
L65084          [CS-INV] task=25a6d73f(rebalance0)...-19-cp7 ch=ResultSubpartitionInfo{...,subPartitionIdx=8} mode=LENIENT bytes=1218 headers=58 fha=13
L81408  att#0 / L103394 att#1  [CS-INV] ... old=...Idx=19} oldSubtask=8 layer=RECOVER_READ mode=STRICT bytes=4410 headers=210 fha=13 parsedRecords=210
```

Accounting: **S=1722 (82 rec) → W=3192 (152 rec) ⇒ live = 1470 bytes = 70 whole records** persisted
through the open window after checkpoint start and before the barrier (channel provably in recovery
at start: Local rule; delivery flush L45257 precedes SNAP). Restore (both attempts):
**R = 4410 = 3192 + 1218 exactly**, headers 210 = 152 + 58, stride constant 21, `parsedRecords=210`,
no assert. Pre-fix semantics (window closed at start) would have dropped those 70 records — the
silent-record-loss failure mode (NUM_OUTPUTS ≠ NUM_INPUTS). 886/882/824/984 read-instances of
in-recovery-written channels validated like this in runs 1–4 (all `R=W+U` exact, `irr=false`).

### 3.2 Round-1 comparison — same channel class, pre-fix vs post-fix

| | round-1 `FAIL_w6_1` (pre-fix, Remote failing-map sub4 ch29, in recovery at cp9) | post-fix golden instance (run3, Remote failing-map sub18 ch8, in recovery at cp15) |
|---|---|---|
| SNAPSHOT (a) | 1512 B = 72 whole records | 1176 B = 56 whole records (== full recovered stream, S==R_recv) |
| CHECKPOINT_WRITE(input) | **1512 B — identical to SNAP, zero live bytes** | **1534 B = 1176 + 358 live** (= 17 whole records + 1-byte record head) |
| upstream output fragment | 1795 B, fha=2 ⇒ 11 bytes "already sent" — **existed nowhere** | 566 B, fha=12 ⇒ 20-byte head orphan — **exactly the 20 bytes completing W's 1-byte tail: 1+20=21 STITCH-OK** |
| restore | `RECOVER_READ ... strides=[21×71, 10, 21×85] *** STRIDE-IRREGULAR *** CORRUPT-RECORD-AT=1512` → `Corrupt stream, found tag: -22` | cp15 not restored (job finished); write-layer fragments byte-consistent; every restored checkpoint in all runs read back clean |

A second Remote instance exists in run1 (`verify_run1.log`, job3 `ec02c841` cp11, `failing-map`
sub 6 g0ch1, rule S==R_recv): **S=483 (23 rec) → W=2364, live = 1881 B = 89 whole records +
12-byte partial (Wtail=12)**; upstream `7b4c6fe4` fragment 2991 B, fha=1 ⇒ 9-byte head orphan;
**stitch 12+9=21 OK** (SNAP L114702, W L121141). Also write-layer verified (cp11 not restored).

### 3.3 Golden Remote instance (the exact fixed code path carrying live bytes), run3 `verify_run3.log`

Channel `failing-map (19/21)#1` subtask 18, gate 0, ch 8, **kind=Remote**, upstream vertex
`7b4c6fe4` — the same downstream-vertex/upstream-vertex pair (`b8c789ec` ← `7b4c6fe4`) as every
round-1 corrupt instance. Delivery finished ts 10524; barrier cp15 arrived ts 18007 with the full
backlog still unconsumed (7.5 s consumption lag — the w6_1 scenario shape):

```
L111957 ts 10524 [CS-INV]      ... ch=...Idx=8} kind=Remote layer=CHANNEL_RECEIVE bytes=1176 headers=56 fha=13
L151334 ts 18007 [CS-INV-SNAP] task=failing-map (19/21)#1 ... ch=...Idx=8} kind=Remote layer=SNAPSHOT cp=15 numBuffers=3 bytes=1176 headers=56 fha=13
L157853 ts 19074 [CS-INV] task=b8c789ec...-18-cp15 ch=...Idx=8} layer=CHECKPOINT_WRITE mode=STRICT bytes=1534 headers=73 fha=13 ... parsedRecords=73
L157432 ts 19067 [CS-INV] task=7b4c6fe4...-8-cp15 ch=ResultSubpartitionInfo{partitionIdx=0, subPartitionIdx=18} mode=LENIENT bytes=566 headers=27 fha=12
```

Decode: S == R_recv = 1176 = 56×21 (proof of in-recovery). W = 1534 = 1176 + **358 live bytes =
17×21 + 1**: headers 73 = 56 + 17, W ends 1 byte into a record. Upstream fragment starts with the
complementary **20-byte orphan (fha=12 ⇒ q=20; p+q = 1+20 = 21)** and 26 more whole records.
The persist window stayed open ~1 s after checkpoint start (SNAP ts 18007 → input complete ts
19074) — with the pre-fix eager `stopPersisting`, all 358 bytes would have been dropped and a
restore of cp15 would break at offset 1176 with the round-1 signature. This is
`RemoteInputChannel.checkpointStarted`'s in-recovery branch + `maybePersist` (c-path) working
end-to-end at the write layer.

## 4. Restore-side validation (all retained runs)

- Zero `STRIDE-IRREGULAR`, zero `RECOVER_READ`/`RECOVER_REWRITE`/`CHANNEL_RECEIVE`
  `[CS-INV-ASSERT]`, zero `Corrupt stream` in prime + runs 1–4 (and in the 71-run monitor).
- Exact `R = W + U` byte matching over all RECOVER_READ lines (heuristic join by
  vertex/oldSubtask/gate/channel/cp): e.g. run1 8 001 reads, 7 116 exact-matched (845 input-only /
  3 018 output-only fragments, 885 unmatched = empty-fragment or ambiguous joins, not failures —
  every read line itself is STRICT-valid); seam census run1: 3 417 mid-record (`p+q=21`) +
  4 274 whole-record. The restored cp7 in prime carried 650 mid-record live seams — every one read
  back STRICT-clean twice. Attempt #0 and #1 reads are byte-identical in all runs.
- Job2's restore of job1-cp5 validates the same way in every run (writer never in recovery in job1).

## 5. Interpretation

The five-stage pipeline (HL §6.1.5) post-fix shows: SNAPSHOT (a) batches intact; CHECKPOINT_WRITE =
prefix + (a) + spill-replay (b) + live (c) with (c) now non-empty for in-recovery channels (159
instances); recovery chain byte-faithful (already exonerated in round 1 and re-confirmed by zero
asserts and exact byte sums here). The round-1 defect — (c) forced empty for in-recovery Remote
channels — is gone, and no new defect (double-persist of recovered buffers, stride break, byte
mutation) appeared anywhere: no test failed, and record counts balance (NUM_OUTPUTS assertion
passes in all 76 runs).

## 6. Residual risk / not exercised

1. **The two golden Remote live-byte instances (§3.3) are write-layer-verified only** — their
   checkpoints (job3 cp11 / cp15) were never restored, because the test only restores job2-cp7 and
   (on internal restart) the latest completed checkpoint. The end-to-end "Remote in-recovery
   live-byte checkpoint → restore" replay of w6_1 was not observed in any *retained* log.
   Mitigation: round 1 proved every stage downstream of CHECKPOINT_WRITE byte-faithful (exact
   sums), the seam arithmetic of §3.3 is byte-exact at the write layer in both instances, and the
   *Local* equivalent of the full chain was restored and validated 3 576 times.
2. **prime.log alone is a false-negative trap**: without CPU contention the no-contention runs'
   in-recovery Remote channels mostly see the barrier before consuming (window closes at
   barrier-stash; `dW=0`, all seams whole-record) — pre-fix and post-fix bytes would be identical
   there. The scenario-bearing evidence came from the 4 extra runs (jobs 2's cp7 in-recovery in
   runs 1–4, live bytes in all runs, Remote golden instance in run 3). The 71 contention runs very
   likely exercised the Remote path heavily (round-1 hit rate ≥11 %/run), but their logs were
   deleted on pass — that leg of the evidence is statistical (§1), not observational.
3. Remaining asymmetries: `kind=Local` provides most live-byte instances (Local fix `201a5f`,
   validated at scale); Remote instances are rare without contention (2 across 5 retained runs).
   If stronger direct evidence is ever wanted: rerun the contention loop with repro.sh patched to
   retain the first N passing logs, or add a one-line INFO log in the in-recovery branch.
4. Instrumentation (`ChannelStateInvariant`, temporary diagnostics) is still in place and must be
   removed before merging; the SNAPSHOT-assert parser desync (§1) and per-cp-fragment STRICT noise
   (LL §3.1) remain known cosmetic limitations of the diagnostics, not of the fix.
5. Scenarios not exercised by this test at all (unchanged from round 1): sort-merge shuffle
   (`FullyFilledBuffer`) paths, multi-gate tasks, checkpoint abort/subsume while the (now longer-
   lived) persist window is open — `checkpointStopped` still closes the window on abort by code
   inspection, but no abort occurred in the retained logs.

## Appendix: retained artifacts

- `repro/results/prime.log` (+ `.pass` = 71 P), `repro/verify_run{1..4}.log` (4 verifier-executed
  runs, ~120 MB each; gitignored).
- Round-1 pre-fix failure logs: `repro/results-round1-instrumented-20260702_131231/FAIL_w6_1.log`,
  `FAIL_w11_2.log`.
- Analysis scripts in the session scratchpad (`verdict.py`): parse `[CS-INV*]` lines, apply the §2
  proof rules, join S/W/U/R per channel, seam census.
