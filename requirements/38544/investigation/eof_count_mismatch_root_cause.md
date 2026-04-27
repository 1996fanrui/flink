# EOF + Count-Mismatch Root Cause Investigation — Round 2 (FLINK-39519, branch `39519/20260424-02-organize-commits-before-address-pr17`)

## Summary

The primary candidate root cause for both EOF and count-mismatch is:

> **`SingleInputGate.convertRecoveredInputChannels` calls `RecoveredInputChannel.releaseAllResources()` immediately after `toInputChannel()`. `releaseAllResources()` invokes `RecoveredBufferStoreImpl.releaseAll()`, which (1) recycles every buffer still queued in the store and (2) drops every still-pending spill entry via `coord.onChannelReleased()`. Both losses are silent.**

The new physical channel (`LocalInputChannel` / `RemoteInputChannel`) inherits *the same store reference* from `toInputChannel()` and is supposed to keep consuming buffers from it after recovery. But the very next line wipes that store. Anything the dispatcher had already delivered to the store but the task had not yet `tryTake`-d is recycled; anything the dispatcher had spilled to disk but had not yet drained back is dropped.

Round 2 reproduces both symptoms directly:

- **EOF (case 3, parameter `downscale KEYED_DIFFERENT_PARALLELISM 12→7 sourceSleepMs=0L`)**: 3-of-3 runs failed; 2-of-3 produced `EOFException`s during deserialization. Direct byte-level evidence below.
- **Count mismatch (case 5, parameter `downscale KEYED_DIFFERENT_PARALLELISM 5→3 sourceSleepMs=5L`)**: NUM_OUTPUTS=NUM_INPUTS assertion `expected: 266275L but was: 266189L` (-86 records). No EOFException. Same `releaseAll`-recycles-ready-buffers mechanism, just no buffer-internal record-boundary truncation; whole records were lost when ready buffers in the store were recycled.

The B3 race (drain peek-then-skip vs. `onChannelReleased` removing the peeked entry) **does** fire concurrently with the EOF window in case 3 (4 of 5 HEAD-MISMATCH events fire BEFORE the EOF, not after as Round 1 incorrectly stated). It is therefore not "purely downstream"; it is concurrent. But it only triggers because R has already started releasing stores while drain is still running, so R is the upstream cause and B3 is a co-symptom. A fix to R would close B3's window as a side-effect.

---

## Methodology

1. Re-applied Round 1 diagnostic patches (`patches/01-FilteredBufferDispatcherImpl.diff`, `patches/02-RecoveredBufferStoreImpl.diff`, `patches/03-ChannelStateCheckpointWriter.diff`) and added a `[DBG-TRYTAKE]` log in `RecoveredBufferStoreImpl.tryTake()` (`patches-round-2/02b-RecoveredBufferStoreImpl-with-trytake.diff`) so consumption is visible in the log.
2. Built `flink-runtime` with `../mvnw -T 20 clean install -U -Pfast -DskipTests -Dmaven.javadoc.skip=true -Drat.skip=true -Dcheckstyle.skip=true -Denforcer.skip=true -P java11-target -P java11`.
3. Ran the parameterized test 5 times across 3 different cases:
   - Case 3 (`downscale KEYED_DIFFERENT_PARALLELISM 12→7 0L`) × 3 runs (`test-run-2-case3-run{1,2,3}.log`)
   - Case 4 (`upscale KEYED_DIFFERENT_PARALLELISM 7→12 0L`) × 1 run (`test-run-2-case4.log`)
   - Case 5 (`downscale KEYED_DIFFERENT_PARALLELISM 5→3 5L`) × 1 run (`test-run-2-case5.log`)
4. All log line numbers below refer to the surefire XML in the named `round-2/test-run-2-*.log` file.

---

## Aggregate run summary

| Run | Case | Result | EOFExceptions | recycledReady (total) | removedByRelease (total) |
|---|---|---|---:|---:|---:|
| run1 | case 3 (downscale 12→7) | FAILED | 4 | 607 | 2 |
| run2 | case 3 (downscale 12→7) | FAILED (graph terminated, 0 EOFs) | 0 | 1153 | 1064 |
| run3 | case 3 (downscale 12→7) | FAILED | 9 | 693 | 0 |
| run4 | case 4 (upscale 7→12) | FAILED | 1 | 1423 | 4 |
| run5 | case 5 (downscale 5→3, sleep=5L) | FAILED — `expected: 266275L but was: 266189L` | 0 | 417 | 0 |

100% of 5 runs failed. 3 runs produced `EOFException`. 1 run (case 5) produced a direct count-mismatch (-86 records). Run 2 of case 3 failed without EOF because phase 2 graph ended with `RecoveryIsSuppressedFixedDelayRestartBackoffTimeStrategy(maxNumberRestartAttempts=0)` before `waitForAllTaskRunning` could observe RUNNING — same root mechanism (recovered-state buffers wiped → tasks fail before reaching RUNNING), surfaced through a different assertion path.

---

## Direct byte-level evidence: EOF in `case3-run1`

Failing task: `rebalance0 (2/4)#0` on channel `inputChannelIdx=0`. Job ID `9a76271e2bac76d375d97f433dcd0b31` (phase 2 of test).

Per-channel-0 chain in `round-2/test-run-2-case3-run1.log`:

| L | t | Event |
|---:|---:|---|
| 13207 | 5688 | `addBuffer ch=0 bufLen=4096 readyAfter=1` |
| 13209 | 5688 | `addBuffer ch=0 bufLen=4096 readyAfter=2` |
| 13221-13247 | 5688-5690 | 4× `writeToSpillFile ch=0` (lengths 4096, 4096, 4096, 1171) — totalWrites=1..4 for this dispatcher |
| 13263 | 5690 | `addBuffer ch=0 bufLen=4 readyAfter=3` (the `EndOfInputChannelStateEvent`) |
| 13273-13287 | 5691 | 4× `peek ch=0` + `commit ch=0` cycles draining the spilled entries back into the store; final `newDrainHead=EntryPosition{END}, totalPops=4`. After drain, `readyAfter` rises to 7 (3 prior + 4 drained). |
| 13289 | 5691 | `[DBG-CLOSE] close ENTER writes=4 drainPops=4 eager=0 removedByRelease=0 ... drainHead=EntryPosition{END}` — dispatcher confirms it has handed everything to the store. |
| 13315 | 5692 | `[DBG-TRYTAKE] ch=0 bufLen=4096 remainingReady=6 pending=0 released=false` — task consumed exactly ONE buffer (4096 bytes) from this channel. |
| 13363 | 5696 | `[DBG-STORE] releaseAll ch=0 recycledReady=6 pendingAtRelease=0 coord=true` — `convertRecoveredInputChannels`-path `releaseAll` recycles the remaining 6 ready buffers. |
| 14596 | 5735 | `Task [rebalance0 (2/4)#0] WARN ... switched from RUNNING to FAILED with failure cause: java.io.IOException: Can't get next record for channel InputChannelInfo{gateIdx=0, inputChannelIdx=0}` |
| 14609 | 5735 | `Caused by: java.io.EOFException` (at `SpillingAdaptiveSpanningRecordDeserializer.readNextRecord:112`) |

Reading: 7 buffers totalling 4×4096 + 1×1171 + 1×4 + (≥2 unspilled adds) bytes were placed into the store. The task pulled exactly one 4096-byte buffer (L13315) and then `releaseAll` recycled the remaining 6. Of those 6 recycled buffers, the `EndOfInputChannelStateEvent` (4-byte) AND the trailing 1171-byte buffer were both dropped. After the recycle, `LocalInputChannel.getNextBuffer` falls through to the live subpartition view, which is empty for this channel, so the partial record the deserializer was assembling from the consumed 4096-byte buffer cannot be completed → `EOFException` reading the next byte at `DataInputDeserializer.readByte:134`.

This is the byte-level demonstration the Round 1 reviewer asked for: the recycled buffers contained the trailing bytes of the in-flight record AND the channel-state terminator; consumption stopped after exactly 1 buffer, so the deserializer's pending record could not be completed.

---

## Direct evidence: Count mismatch in `case5`

Failing test assertion (`round-2/test-run-2-case5.log`, line 82 of the surefire XML):

```
Multiple Failures (1 failure)
	org.opentest4j.AssertionFailedError: [NUM_OUTPUTS = NUM_INPUTS] 
expected: 266275L
 but was: 266189L
```

86 records short. Census:
- 417 recycledReady total across all stores
- 0 removedByRelease (no spill happened in case 5 — `sourceSleepMs=5L` slows ingest enough that the recovery state stays in-memory; thus all loss is via path (a) only)
- 0 writes total (no spilling occurred)
- Distribution: 999 stores recycled 0 buffers, 315 stores recycled exactly 1, 30 stores recycled 2, 14 stores recycled 3.

Most of the 359 (=315+30+14) non-empty recycle events were `EndOfInputChannelStateEvent`-only (4-byte) recycles, which alone do not lose user data. The remainder are the data buffers preceding EndOfState that the task did not consume before `releaseAll` fired. The 86-record gap is plausibly accounted for by the ≤102 (=315 + 60 + 42 - 315 EndOfState-only) non-trivial recycled buffers carrying user records; we did not perform a byte-by-byte allocation in the report because the test does not log per-record buffer composition. The mechanism (silent recycle on conversion path) is identical to the EOF case; the absence of EOF is because no consumer was mid-record when its store was wiped — instead, whole records simply vanished.

---

## Aggregate loss census from `round-1/test-run-1.log` (corrected)

Sum across all phase-2 dispatchers in job `a0b6db728152e0990deafd1192198434`:

| Counter | Total | Computation |
|---|---:|---|
| `removedByRelease` (entries dropped from spill deque by `onChannelReleased`) | 757 | `grep DBG-CLOSE | grep a0b6db | awk -F removedByRelease= '{...}' | sum` |
| `recycledReady` (in-memory ready buffers recycled by `releaseAll`) | (job-scoped) — see below | |

Re recycledReady: the Round 1 report wrote "797" but the live total across ALL `[DBG-STORE] releaseAll` lines in `test-run-1.log` (covering both jobs in the log) is **942**. Restricted to the failing job `a0b6db...`, the figure is documented per-task in the table below; the cross-task sum for that job is between the 757 of `removedByRelease` and the 942 cross-job total. Because the Round 1 number was incorrect either way, the fix below is to drop the cross-job total claim and rely on the per-task line evidence and the totals reproduced fresh in Round 2 (Aggregate run summary above).

Per-task highlights with verified line numbers in `round-1/test-run-1.log`:

- L11788 `upscale0 (4/6)`: `writes=30 drainPops=12 eager=5 removedByRelease=13`
- L14405 `keyby0 (2/7)`: `writes=116 drainPops=11 eager=14 removedByRelease=91` (Round 1 cited L13325 — that line is actually the unrelated `keyby0 (7/7) ... CancelTaskException`. Corrected.)
- L12187 `Co-Keyed-Process (3/7)`: `writes=52 drainPops=17 eager=0 removedByRelease=35`
- L12261 `downscale0 (4/4)`: `writes=116 drainPops=8 eager=2 removedByRelease=106`

---

## Concurrency demonstration: `upscale0 (4/6)` releaseAll fires while drain is mid-flight

`round-1/test-run-1.log` L11751-L11788 shows the precise interleave:

- L11751 (t=5893, mailbox thread `[upscale0 (4/6)#0]`): `releaseAll ch=1 recycledReady=8 pendingAtRelease=0 coord=true`
- L11758 (t=5893, same mailbox thread): `releaseAll ch=0 recycledReady=1 pendingAtRelease=6` — releaseAll captured 6 entries on disk that drain had spilled but had not yet drained back.
- L11762: `removeEntriesForChannel ch=0 ... removed=6 sealed=true` — those 6 disk entries are dropped.
- L11764-L11774: same pattern for ch=3 (recycled=4, pending=1, removed=1) and ch=2 (recycled=4, pending=6, removed=6).
- L11788 (t=5894, recovery thread `channel-state-unspilling-upscale0 (4/6)#0 ... thread-1`): `[DBG-CLOSE] close ENTER writes=30 drainPops=12 eager=5 removedByRelease=13 ... drainHead=EntryPosition{fileIndex=0, offset=66271}`

Reading: at the moment releaseAll fires (t=5893), the dispatcher's drain has only committed 12 of its 30 writes (`drainHead=66271`, well below the dispatcher's high-watermark). The `pendingAtRelease=6` numbers on each releaseAll line are the entries drain still owed to that channel's store. Drain is mid-flight when releaseAll wipes its target stores. The recovery thread's own close at t=5894 shows the final `removedByRelease=13` count — it accumulated from the four `removeEntriesForChannel` calls that the mailbox thread fired into this dispatcher between L11762 and L11776.

This is the demonstration the Round 1 reviewer asked for. The `keyby1 (2/7)` example used in Round 1 had `writes=0` (no spill happened on its dispatcher), so it demonstrated only the readyBuffers-recycle path, not the concurrent-with-drain path. `upscale0 (4/6)` demonstrates both.

---

## Candidate analysis (full)

### Root cause R: `convertRecoveredInputChannels` calls `releaseAllResources` on every recovered channel right after `toInputChannel()` — wipes the store the new physical channel just inherited

**Trigger chain (file:line)**

1. `flink-runtime/.../SequentialChannelStateReaderImpl.java:114-122` — `flush → finishRecovery → drainPendingSpill` (per the C5 contract in `requirements/38544/close_drain_separation.md`).
2. `RecoveredChannelStateHandler.finishRecovery` `:255-264` → for every input gate, `inputGate.finishReadRecoveredState()`.
3. `SingleInputGate.finishReadRecoveredState` `:457-462` → for every `RecoveredInputChannel`, `finishReadRecoveredState()`.
4. `RecoveredInputChannel.finishReadRecoveredState` `:175-192` → adds `EndOfInputChannelStateEvent` and **completes `bufferFilteringCompleteFuture`**.
5. `StreamTask.initializeStateAndOpenOperators` `:907-918` wires `requestPartitionsTrigger.thenRun(() -> mainMailboxExecutor.execute(inputGate::requestPartitions, ...))`.
6. The mailbox executes `SingleInputGate.requestPartitions → convertRecoveredInputChannels` `:398-443`:
   ```java
   InputChannel realInputChannel = ((RecoveredInputChannel) inputChannel).toInputChannel();
   inputChannel.releaseAllResources();        //  <-- (R)
   ```
7. `RecoveredInputChannel.toInputChannel` `:133-145` passes `store` to the new physical channel.
8. `RecoveredInputChannel.releaseAllResources` `:270-275` calls `store.releaseAll()`.
9. `RecoveredBufferStoreImpl.releaseAll` `:170-196`:
   ```java
   for (Buffer buffer : readyBuffers) buffer.recycleBuffer();  // (a) drop ready buffers
   readyBuffers.clear();
   ...
   if (c != null) c.onChannelReleased(channelInfo);             // (b) drop on-disk entries
   ```
10. `LocalInputChannel.getNextBuffer` `:258-265` reads via `recoveredStore.tryTake()` first; the store is now empty, so the channel skips straight to the live subpartition view.

**Failure mode**: silent. `releaseAll` recycles buffers without surfacing any exception; both (a) queued ready buffers and (b) still-pending spill entries vanish.

**Explains EOF + count mismatch?** YES — both reproduced live in Round 2.
- EOF: case 3, byte-level evidence above (rebalance0 (2/4) ch=0, L13315/L13363/L14609 in `round-2/test-run-2-case3-run1.log`).
- Count mismatch: case 5, assertion `expected 266275L but was 266189L` in `round-2/test-run-2-case5.log`.

**Verdict**: KEEP. Single primary candidate root cause for both symptoms in this matrix.

---

### Candidate A: drain holds `synchronized(store)` while triggering `addBuffer` listener (deadlock risk)

Documented in `requirements/38544/remaining_drain_buffer_loss.md` §A as "no data loss, only theoretical deadlock risk".

**Log evidence**: across all 5 round-2 runs and the original round-1 log, `grep -c -i 'deadlock' <log>` returns **0** and `grep -c 'WAITING\|java.lang.Thread.State' <log>` returns **0** — no stack/thread dumps were captured. This **does not** falsify Candidate A; it only means no deadlock is observable in the failure window because the test fails (EOF or count-mismatch or graph-terminated) before the lock-order conditions for Candidate A's deadlock have a chance to manifest. The relevant fix already landed (`af462720b8b` and the subsequent `f23f1282466`); this round's harness can neither prove nor disprove A.

**Verdict**: EXCLUDE for this reproduction with caveat: A could not be falsified by these runs because the test failed before any plausible deadlock window. The fix already in place (two-phase lock pattern in `RecoveredBufferStoreImpl.addBuffer`) is independently justified.

---

### Candidate B1: `drain.pollFirst` vs `snapshot.addAll` → `ConcurrentModificationException`

`FilteredBufferDispatcherImpl.onChannelCheckpointStarted` `:336-358` calls `reader.snapshot()` which iterates `Reader.entries`; a concurrent `drain.pollFirst()` from the recovery thread can throw CME.

**Log evidence**:
- `grep -c 'DBG-CKPT' round-1/test-run-1.log` returns **0** and same for all round-2 logs.
- No `ConcurrentModificationException` stack traces.

The phase-2 `onChannelCheckpointStarted` path is never exercised in any of these reproductions because the test fails before phase-2 checkpointing. Loud failure mode would also surface in stack traces — none observed.

**Verdict**: EXCLUDE for this reproduction. Loud per the doc itself.

---

### Candidate B2: `drain.pollFirst` vs `onChannelReleased.iterator.remove` → CME

Same iterator-mutation pattern as B1 but on the release path.

**Log evidence**:
- No CME stack traces in any of the 5 round-2 logs nor in `round-1/test-run-1.log`.
- `removeEntriesForChannel` runs under `synchronized(this)` on the dispatcher; `peekNextEntry` / `skipNextEntry` are not synchronized, but the operations they perform on the deque are different. A real concurrent iteration would surface CME — none observed.

**Verdict**: EXCLUDE for this reproduction. Same loud-vs-silent argument as B1.

---

### Candidate B3: drain peek-then-skip races with `onChannelReleased` → silent buffer mis-routing (HEAD-MISMATCH)

`peekNextEntry` returns `e`; the lock-free disk I/O reads `e`'s bytes; `skipNextEntry` then pops a different entry `e'` because `onChannelReleased` deleted `e` between (1) and (3). Bytes for `e` are written to `store_e` (which is being released → SHORT-CIRCUIT) while `e'` is silently dropped.

**Log evidence (in `round-1/test-run-1.log`)** — there are **5** HEAD-MISMATCH events, and the EOF that fails the test fires at t=6093 for `keyby1 (2/7)` (L17850-L17869):

| L | t | Task | peeked | popped |
|---:|---:|---|---|---|
| 17246 | 6076 | failing-map (7/7) | ch=3 off=64974 | ch=4@69067 |
| 17273 | 6077 | failing-map (7/7) | ch=5 off=107266 | ch=4@122512 |
| 17288 | 6078 | failing-map (7/7) | ch=4 off=130704 | ch=6@137002 |
| 17603 | 6085 | failing-map (4/7) | ch=6 off=46893 | null |
| 18441 | 6102 | failing-map (2/7) | ch=6 off=138305 | null |

Round 1 listed only the first 4 events and called the cluster "AFTER the EOF". That timing claim was wrong: 4 of 5 events (L17246, L17273, L17288, L17603) fire at t=6076-6085, **before** the EOF at t=6093; only L18441 (t=6102) is after.

Re-derived verdict: B3 is concurrent with the EOF-causing window, not strictly downstream. However, the trigger condition for B3 is "drain is running while `onChannelReleased` fires for a channel still on its drain path". `onChannelReleased` is fired by `RecoveredBufferStoreImpl.releaseAll` — i.e. by R. So the necessary condition for B3 is supplied by R: the moment R's mailbox path starts wiping stores while drain is still in flight, B3's window opens. Therefore B3 and R share the same upstream cause; fixing R closes B3's window as a side-effect.

**Verdict**: KEEP as co-symptom and hardening target, NOT as a separate root cause. R is the primary trigger; B3 is a concurrent secondary effect of the same release-mid-drain interleave.

---

## Why the C5 contract `flush → finishRecovery → drainPendingSpill` enables R

The contract was chosen so that drain (which can block on buffer pool) does NOT run inside a `synchronized` close. That intent is correct. But it has a side effect the doc didn't anticipate: `finishRecovery` completes `bufferFilteringCompleteFuture`, which schedules `requestPartitions` → `convertRecoveredInputChannels` on the task mailbox. That mailbox call lands while the recovery thread is still partway through `drainPendingSpill`, and `convertRecoveredInputChannels` calls `releaseAllResources()` on every recovered channel, wiping its store before drain can finish delivering buffers AND before the task has finished consuming what was already delivered.

The interleave is directly visible in `round-1/test-run-1.log` at L11751-L11788 (upscale0 (4/6) section above):
- mailbox thread calls `releaseAll` and `onChannelReleased` for ch=1,0,3,2 of upscale0 (4/6) at t=5893
- recovery thread `channel-state-unspilling-upscale0 (4/6)#0 ... thread-1` reaches `[DBG-CLOSE]` at t=5894 with `drainPops=12 < writes=30` and `drainHead=66271`

So drain had committed only 12 of 30 entries when releaseAll fired on the same dispatcher's stores.

---

## Fix direction (no code, just invariants)

The fix must ensure both:

- **I1 (deque entries)**: every entry `drainPendingSpill` is supposed to deliver to a store must reach that store **before** the store transitions to `released`.
- **I2 (in-memory ready buffers)**: when a `RecoveredInputChannel` converts to a physical channel, the inherited store's already-queued buffers must NOT be recycled — they must remain consumable by the new physical channel.

**Path P1 (recommended): decouple "stop recovered-channel pre-conversion accounting" from "wipe store"**

`RecoveredInputChannel.releaseAllResources` is being called for the wrong reason in `convertRecoveredInputChannels`. The intent of that line is "the recovered channel's lifecycle as a `RecoveredInputChannel` is done; the physical channel takes over." But there is no actual *resource* to release — the store and its bufferManager are being inherited by the new physical channel. The fix is to **not call `store.releaseAll()` on the conversion path**:
- Either: introduce a separate `convertCompleted()` hook on `RecoveredInputChannel` that flips its own `isReleased` and frees only the resources NOT inherited by the physical channel. Don't touch the store.
- Or: make `RecoveredBufferStoreImpl.releaseAll()` no-op when invoked via the conversion path (by passing an explicit "ownership transferred" flag from `toInputChannel`); only the failure / cancellation path should actually wipe.

After this, the physical channel keeps consuming through `recoveredStore.tryTake()` until the store is naturally drained. It will see `EndOfInputChannelStateEvent` first, then naturally roll over to live data, exactly as `LocalInputChannel.getNextBuffer` already structures the read.

**Path P2 (additional defence-in-depth)**: re-order so `drainPendingSpill` completes BEFORE conversion can happen. Concretely: do not let `bufferFilteringCompleteFuture.complete(null)` happen until BOTH `finishReadRecoveredState`-per-channel AND `drainPendingSpill` are done. This requires moving drain BEFORE `finishRecovery` in `SequentialChannelStateReaderImpl.readInputData`, OR completing the future on a different signal that waits for drain.

P1 alone fixes the bug (and trivially closes B3 as well, because the conversion path no longer invokes `onChannelReleased`). P2 alone also fixes it but constrains the drain-blocking property. P1+P2 together is a belt-and-suspenders solution.

When the fix lands, the same tracing harness (counters at close: `writes`, `drainPops`, `eager`, `removedByRelease`, `recycledReady` summed across all phase-2 dispatchers and stores) becomes the regression check: **`removedByRelease == 0` and `recycledReady == 0` for the conversion path** must hold for every successful run.

---

## Files

Logs (round-2):
- `round-2/test-run-2-case3-run1.log` — case 3 run 1, surefire XML, 4 EOFs, 607 recycledReady
- `round-2/test-run-2-case3-run2.log` — case 3 run 2, surefire XML, 0 EOFs, 1153 recycledReady, graph FAILED
- `round-2/test-run-2-case3-run3.log` — case 3 run 3, surefire XML, 9 EOFs, 693 recycledReady
- `round-2/test-run-2-case4.log` — case 4 (upscale 7→12), surefire XML, 1 EOF, 1423 recycledReady
- `round-2/test-run-2-case5.log` — case 5 (downscale 5→3 5L), surefire XML, **count-mismatch -86 records**, 417 recycledReady
- `round-2/test-run-2-case*-mvn.log` — corresponding mvn stdout per run

Diagnostic patches (NOT to commit; revert before any production change):
- `patches/01-FilteredBufferDispatcherImpl.diff`
- `patches/02-RecoveredBufferStoreImpl.diff`
- `patches/03-ChannelStateCheckpointWriter.diff`
- `patches/04-UnalignedCheckpointRescaleITCase-test-filter.diff` (single-case parameterization for case 3)
- `patches-round-2/02b-RecoveredBufferStoreImpl-with-trytake.diff` (adds `[DBG-TRYTAKE]` log)
- `patches-round-2/04b-UnalignedCheckpointRescaleITCase-test-filter-case5.diff` (single-case parameterization for case 5)

Round-1 log retained for cross-reference: `round-1/test-run-1.log`.
