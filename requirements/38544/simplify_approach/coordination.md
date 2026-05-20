# Cross-thread cooperation: lock principles and the checkpoint protocol

> **Follow-up (2026-05-24):** later rounds of fixes (see [`../fix_rounds/`](../fix_rounds/))
> superseded parts of this doc. In particular: §7.4 of
> [`end_of_input_event_missing_fix.md`](../fix_rounds/end_of_input_event_missing_fix.md)'s
> conditional wake is now replaced by the per-channel `upstreamReady` future described in
> [`recovery_in_recovery_flag_unification.md §9`](../fix_rounds/recovery_in_recovery_flag_unification.md);
> the Step 1 / Step 2 `isInRecovery` predicate is enforced per channel
> ([`missing_recovery_checkpoint_barrier_fix.md`](../fix_rounds/missing_recovery_checkpoint_barrier_fix.md)).
> Lock principles and the 3-step skeleton below remain valid; the §3.2 dispatcher diagram does not
> yet show the new `upstreamReady` await before each `onRecoveredStateBuffer` / sentinel push.

> Scope: the cooperation mechanism between `channelIOExecutor` (the async thread, described in [`unspiller.md`](./unspiller.md)) and the task thread (mailbox; the consumer side is described in [`input_channel.md`](./input_channel.md)). This doc is the contract that the other two docs both adhere to.

## 1. The lock

There is exactly one lock in this design: a private `Object lock` field on `SpillFileReader`, taken via plain `synchronized (lock)` blocks. It is deliberately not the implicit `this` monitor of `SpillFileReader`, so the lock is a named, grep-able, `@GuardedBy`-annotated field.

### What the lock guards

| Resource | Why it is guarded by this lock |
|---|---|
| Each `RecoverableInputChannel`'s `recoveredBuffers` write path during recovery (delegated through `onRecoveredStateBuffer`) | Recovery delivery and the task-thread checkpoint barrier insertion must observe a single cut per channel; that cut is deterministic only if all writes funnel through one lock. `finishReadRecoveredState` at end-of-drain is NOT covered here — see §1 "End-of-drain exception" below. |
| `SpillFileReader.currentSegmentIndex` and `SpillFileReader.currentOffset` | Their advance must be observed as one atomic action together with the matching channel add-buffer; otherwise the task thread snapshot can see a half-applied entry. |
| The Step 1 barrier-insertion sequence (snapshot disk + add a `RecoveryCheckpointBarrier` to every channel) | The task thread must take the disk cut and insert all per-channel barriers in one atomic interval, so that the recovered-data set is disjoint between "before barrier" and "after barrier". |

The lock does NOT guard: any upstream-side state (`receivedBuffers` on `RemoteInputChannel`, `subpartitionView` on `LocalInputChannel`, `toBeConsumedBuffers`, `hasPendingPriorityEvent`). Those keep their master semantics and their existing per-channel locks.

### Two strong principles

**Principle 1.** Every recovery-side mutation on a `RecoverableInputChannel` happens inside `synchronized (SpillFileReader.lock)`. Targets (no exceptions):

- drain calling `ch.onRecoveredStateBuffer(buf)` to deliver a recovered buffer;
- task thread inserting `RecoveryCheckpointBarrier` into each channel at Step 1 (also performed via `ch.onRecoveredStateBuffer(barrier)` — the barrier is just a sentinel `Buffer`).

**End-of-drain exception.** `drain` calling `ch.finishReadRecoveredState()` after the last entry is delivered runs **outside** `SpillFileReader.lock`. At this point no more buffers will be added, so the (queue, offset) atomicity that this principle protects does not apply; the flag is published through the channel's internal monitor that `finishReadRecoveredState` already takes. See [`unspiller.md`](./unspiller.md) §4 step (D).

**Principle 2.** Advancing `currentSegmentIndex` / `currentOffset` happens in the **same** `synchronized (SpillFileReader.lock)` block as the matching `ch.onRecoveredStateBuffer(buf)`. They are inseparable; if the lock is split, the task thread can observe a half-applied state and either drop or double-count an entry.

### What each thread does while holding the lock

| Thread | Frequency | Body of the critical section |
|---|---|---|
| `channelIOExecutor` (drain phase) | High — once per spill entry | **Exactly two actions, both pure in-memory** (see [`unspiller.md`](./unspiller.md) §4 step (C)): (1) `ch.onRecoveredStateBuffer(buf)`; (2) `seg.pollNextEntry()` + update `(currentSegmentIndex, currentOffset)`. Microsecond scale. |
| `channelIOExecutor` (drain finish) | Once at end of drain | `ch.finishReadRecoveredState()` on every channel — **runs outside the lock** (see "End-of-drain exception" above and [`unspiller.md`](./unspiller.md) §4 step (D)). |
| task thread | Exactly once at the moment a checkpoint fires | (1) snapshot every `SpillFileSegment` and capture `(currentSegmentIndex, currentOffset)` as the `DiskSnapshot.startPos`; (2) call `ch.onRecoveredStateBuffer(new RecoveryCheckpointBarrier())` on every channel. Body is fully in-memory; the disk read for Step 3 happens **after** the lock is released, on the writer thread. |

### What happens outside the lock

Drain's slow steps are deliberately kept outside the lock so the critical section stays microsecond-scale and task-thread Step 1 never waits on I/O:

- `ch.requestBufferBlocking()` parks inside `BufferManager.requestBufferBlocking` on the channel's own `bufferQueue` (Java `Object.wait/notifyAll`, woken by `BufferPool`'s `BufferListener` callback). SpillFileReader never sees the parking primitive directly.
- `seg.readBytesAt(...)` runs on the buf in hand (local to this drain iteration; not yet visible to anyone else).

### Lock order

There are exactly **two** locks involved in any drain or checkpoint path:

| Lock | Scope |
|---|---|
| `SpillFileReader.lock` | outer; held by drain (per entry) and by task thread (Step 1 only) |
| channel-internal queue monitor | inner; `synchronized(receivedBuffers)` on `RemoteInputChannel` (reused from master — it now guards both `receivedBuffers` and the new `recoveredBuffers`); `synchronized(recoveredBuffers)` on `LocalInputChannel` (Local has no `receivedBuffers` field on master, so the new field's identity serves as its own monitor) |

Global lock order:

```
SpillFileReader.lock → channel-internal queue monitor
```

All paths obey this order:

| Path | Holds |
|---|---|
| drain delivery (`SpillFileReader.drain` → `onRecoveredStateBuffer`) | `SpillFileReader.lock` → channel monitor |
| Step 1 barrier insert (`snapshotAndInsertBarriers` → `onRecoveredStateBuffer`) | `SpillFileReader.lock` → channel monitor |
| Step 2 in-memory snapshot | channel monitor only |
| `getNextBuffer()` (consumer) | channel monitor only |
| network `onBuffer` (Remote) | channel monitor only (this *is* `receivedBuffers`) |

No third lock is introduced, no path takes the locks in reverse → no cycle, no deadlock.

## 2. Cross-thread interface surface

All three Java interfaces are declared in full in [`overview.md`](./overview.md#6-cross-thread-java-interfaces) §6. This section is a per-method **lock-and-purpose quick reference** for reasoning about coordination semantics; it does not re-declare the interfaces.

### 2.1 `RecoveryCheckpointTrigger` — task thread → unspilling thread

Implemented by `SpillFileReader`; task thread holds the reference typed as the interface. Declaration: [`overview.md`](./overview.md#61-recoverycheckpointtrigger--task-thread--unspilling-thread) §6.1.

| Method | Lock precondition | Purpose |
|---|---|---|
| `snapshotAndInsertBarriers()` | Caller MUST NOT hold `SpillFileReader.lock`. Method takes the lock itself. | Atomically (1) snapshot every `SpillFileSegment` + capture `(currentSegmentIndex, currentOffset)` as `DiskSnapshot.startPos`; (2) call `onRecoveredStateBuffer(new RecoveryCheckpointBarrier())` on every channel. Returns the `DiskSnapshot` for Step 3. |

### 2.2 `RecoverableInputChannel` — unspilling thread → physical channels

Implemented by `RecoveredInputChannel`, `LocalInputChannel`, `RemoteInputChannel`. Drain holds references typed as the interface. Declaration: [`overview.md`](./overview.md#62-recoverableinputchannel--unspilling-thread--physical-channels) §6.2.

| Method | Lock precondition | Purpose |
|---|---|---|
| `onRecoveredStateBuffer(Buffer buffer)` | Caller MUST hold `SpillFileReader.lock`. | Append `buffer` to the channel's `recoveredBuffers`. Used for both real recovered data and the `RecoveryCheckpointBarrier` sentinel (channel impl does not distinguish). |
| `finishReadRecoveredState()` | Caller does NOT need to hold `SpillFileReader.lock` (end-of-drain exception — see §1). | Flip `allRecoveredBuffersDelivered` to true. The channel completes `stateConsumedFuture` once both this flag is set and `recoveredBuffers` has been fully consumed. |

### 2.3 `BufferRequester` — unspilling thread → buffer pool

A two-method interface; lives in the same package as `SpillFileReader` so the cross-package access to `RecoveredInputChannel.releaseAllResources()` is hidden inside the single implementation `RecoveredChannelBufferRequester`. Declaration: [`overview.md`](./overview.md#63-bufferrequester--unspilling-thread--buffer-pool) §6.3.

| Method | Lock precondition | Purpose |
|---|---|---|
| `requestBufferBlocking(InputChannelInfo)` | Caller MUST NOT hold `SpillFileReader.lock`. | Block until a buffer is available from the source channel's pool. Implementation delegates to `RecoveredInputChannel.requestBufferBlocking()` (master existing, with the heap fallback removed). Internally parks on the per-channel `BufferManager.bufferQueue` (`Object.wait`), woken by `BufferPool`'s `BufferListener` callback. |
| `releaseExclusiveBuffers()` | Called once at end of drain, single-threaded (no lock contention). | Release the exclusive buffers held by every source channel served by this requester. Implementation iterates the source channels and calls `RecoveredInputChannel.releaseAllResources()` — master existing method, access modifier promoted from package-private to public. |

## 3. The checkpoint 3-step protocol

### 3.1 Trigger point — the two `Alternating*` UC entry points

In master, `CheckpointableInput.checkpointStarted` is reached from exactly two call sites:

- `AlternatingWaitingForFirstBarrierUnaligned.barrierReceived` — the checkpoint started as UC from the outset;
- `AlternatingCollectingBarriers.barrierReceived` — aligned barrier in flight, but the alignment has now timed out and is being switched to UC.

These two **are the UC entry points** in master's barrier handler. Recovery-during-checkpoint is a UC-only feature, so plugging the 3-step in via these two entries covers every relevant case — nothing else triggers UC handling.

We add **one** call to a shared task-level dispatcher (see §3.2) at the top of each of these two methods. No further hook points are required.

### 3.2 Shared task-level dispatcher

The same body runs for both `Alternating*` callers — one helper on `ChannelState`:

```java
// ChannelState.onCheckpointStartedForAllInputs (called from both Alternating* classes)
public void onCheckpointStartedForAllInputs(CheckpointBarrier barrier,
                                             ChannelStateWriter writer) throws ... {
    // (1) Task-level Step 1 — snapshot disk + insert RecoveryCheckpointBarrier into each
    //     channel's recoveredBuffers. RecoveryCheckpointTrigger is a no-op impl when the
    //     feature is off or recovery has fully completed; returns an empty DiskSnapshot
    //     and inserts no barriers. Outer code does not branch on the feature flag.
    DiskSnapshot snap = recoveryCheckpointTrigger.snapshotAndInsertBarriers();

    // (2) Master-existing per-gate iteration. Internally each channel's
    //     checkpointStarted picks ONE of two mutually exclusive branches based
    //     on its own (allRecoveredBuffersDelivered, recoveredBuffers) state:
    //       - in recovery  → walk recoveredBuffers up to RecoveryCheckpointBarrier
    //       - not recovery → master's existing receivedBuffers persistence
    //     See §3.3 for the channel-internal body; no outer Step 2 loop.
    for (CheckpointableInput input : inputs) {
        input.checkpointStarted(barrier);
    }

    // (3) Task-level Step 3 — hand the disk slice to the writer.
    writer.addInputDataFromSpill(barrier.getId(), snap);
}
```

Two `Alternating*` callers reduce to one line each:

```java
channelState.onCheckpointStartedForAllInputs(unalignedBarrier, channelStateWriter);
controller.triggerGlobalCheckpoint(unalignedBarrier);
```

Key properties:

- **No `if (filter-on)` at this layer.** Step 1 and Step 3 always run; they collapse to no-op when there's nothing to do (feature off, or recovery fully completed on every channel) via:
  - `RecoveryCheckpointTrigger.snapshotAndInsertBarriers()` no-ops (empty `DiskSnapshot`, no barrier inserts) when the spill file is empty AND `allRecoveredBuffersDelivered` is true on every channel;
  - `ChannelStateWriter.addInputDataFromSpill(empty)` no-ops on the writer side.
- **No outer Step 2 loop.** Step 2 is **embedded inside each `channel.checkpointStarted(barrier)`** (master's existing per-channel method, now extended — see §3.3). The dispatcher only iterates gates per master; gates iterate channels per master; each channel picks one of two mutually exclusive branches based on its own state.
- **Task-level once per checkpoint.** `recoveryCheckpointTrigger.snapshotAndInsertBarriers()` is called exactly once, regardless of gate count. Master per-gate iteration covers all channels exactly once.

Sequence view of the dispatcher running on the task thread:

```mermaid
sequenceDiagram
    autonumber
    participant T as task thread (mailbox)
    participant CS as ChannelState
    participant U as SpillFileReader
    participant CIO as channelIOExecutor
    participant Ch as RecoverableInputChannel (×N)
    participant W as ChannelStateWriter
    Note over CIO: drain holds SpillFileReader.lock briefly per entry
    T->>CS: onCheckpointStartedForAllInputs(barrier)
    activate CS
    Note over CS: Step 1
    CS->>U: snap = snapshotAndInsertBarriers()
    activate U
    Note over U: enter synchronized(SpillFileReader.lock)<br/>(CIO blocked outside next critical section)
    loop per RecoverableInputChannel
      U->>Ch: onRecoveredStateBuffer(RecoveryCheckpointBarrier)
    end
    Note over U: exit synchronized(SpillFileReader.lock)
    deactivate U
    Note over CIO: drain resumes;<br/>new deliveries all land after the barrier
    Note over CS: master-existing per-gate iteration<br/>(Step 2 embedded inside each channel.checkpointStarted)
    loop per CheckpointableInput → per channel
      CS->>Ch: checkpointStarted(barrier)
      Note right of Ch: if inRecovery → walk recoveredBuffers up to barrier<br/>else → master receivedBuffers persistence<br/>(mutually exclusive — never both)
      Ch->>W: addInputData(retained pre-barrier buffers)
    end
    Note over CS: Step 3
    CS->>W: addInputDataFromSpill(snap)
    Note right of W: writer async demux<br/>by entry.channelInfo
    deactivate CS
```

### 3.3 Inside `channel.checkpointStarted` — Step 2 embedded

Both `RemoteInputChannel.checkpointStarted` and `LocalInputChannel.checkpointStarted` extend their master-existing bodies with **one mutually exclusive branch** — recovery and non-recovery never both run. Master's existing receivedBuffers persistence runs **only** when not in recovery; the new recoveredBuffers walk runs **only** when in recovery. The temporal mutual-exclusion invariant (§3.4 defensive assert) makes this branch correctness-preserving.

```java
// channel.checkpointStarted (RemoteInputChannel / LocalInputChannel)
public void checkpointStarted(CheckpointBarrier barrier) throws CheckpointException {
    synchronized (channelMonitor()) {
        boolean inRecovery = !allRecoveredBuffersDelivered || !recoveredBuffers.isEmpty();
        if (inRecovery) {
            // Defensive: during recovery, upstream must not have sent live data
            // buffers into receivedBuffers (only priority events / control buffers).
            assert receivedBuffersHasNoLiveDataBuffer()
                : "live upstream data observed in receivedBuffers during recovery";

            // Walk recoveredBuffers up to the RecoveryCheckpointBarrier sentinel
            // inserted by Step 1, retain pre-barrier buffers, hand them to the
            // channel state writer. Drop the barrier sentinel itself.
            List<Buffer> retained = new ArrayList<>();
            Iterator<Buffer> it = recoveredBuffers.iterator();
            while (it.hasNext()) {
                Buffer b = it.next();
                if (b instanceof RecoveryCheckpointBarrier
                        && ((RecoveryCheckpointBarrier) b).getCheckpointId() == barrier.getId()) {
                    it.remove(); break;
                }
                retained.add(b.retainBuffer());        // retain — task still consumes from queue
            }
            channelStateWriter.addInputData(
                barrier.getId(), channelInfo, SEQUENCE_NUMBER_RESTORED,
                CloseableIterator.fromList(retained, Buffer::recycleBuffer));
        } else {
            // Master existing — channelStatePersister.startPersisting + maybePersist
            // setup; persists receivedBuffers content (upstream live data).
            // Untouched from master.
            <master existing body>
        }
    }
}
```

Key points:

- **Mutually exclusive branches.** During recovery, only the new branch runs; master's body is skipped (it has nothing to do — `receivedBuffers` has no live data, only priority events handled separately by the barrier-arrival mechanism that triggered this checkpoint). Outside recovery, only master's body runs — `recoveredBuffers` is empty, no walk needed.
- **Must find the cpId-matched barrier.** End-of-queue without a match is a bug — throw, don't silently return an empty retained list.
- **Recovery-phase predicate uses BOTH fields** — `!allRecoveredBuffersDelivered || !recoveredBuffers.isEmpty()`. This catches every moment the channel still has work to do recovery-side, including the boundary cases where the producer hasn't yet started delivering (`recoveredBuffers` empty but flag false) and where the producer finished but the consumer hasn't drained (flag true but `recoveredBuffers` non-empty). It is the **negation** of the `stateConsumedFuture` completion predicate ([`input_channel.md`](./input_channel.md) §3.7).
- **`receivedBuffersHasNoLiveDataBuffer()`** — channel-internal helper. On `RemoteInputChannel`: iterate `receivedBuffers` and verify every buffer has `!Buffer.isBuffer()` (priority events / control buffers only, no data). On `LocalInputChannel`: trivially `true` (no `receivedBuffers` field; live data arrives via `subpartitionView` which is consulted only after `recoveredBuffers` is empty per §3.6).
- **`channelMonitor()`** — same monitor used by `onRecoveredStateBuffer` (Remote: `synchronized(receivedBuffers)`, Local: `synchronized(recoveredBuffers)`). See §1 lock order.

### 3.4 Step 1 and Step 3 details

**Step 1** — `snapshotAndInsertBarriers()` internal behavior in [`unspiller.md`](./unspiller.md) §3: inside `synchronized(SpillFileReader.lock)`, take a `DiskSnapshot` and call `ch.onRecoveredStateBuffer(RecoveryCheckpointBarrier)` on every channel in `allChannels`. After releasing the lock, `channelIOExecutor` resumes drain; subsequent add-buffers land after the barrier; subsequent `currentOffset` advances past `snap.startPos` (so Step 3's iterator skips already-delivered entries).

**Step 3** — new `ChannelStateWriter` method:

```java
void addInputDataFromSpill(long checkpointId, CloseableIterator<DiskSnapshot.Chunk> chunks);
```

Async writer thread demuxes by `chunk.channelInfo` into each channel's checkpoint output stream. Empty `DiskSnapshot` is a no-op at the writer side.

### 3.5 Ordering

- Step 1 runs first — the disk snap + barrier inserts must precede everything else.
- Step 2 (inside each `channel.checkpointStarted`) and Step 3 (`addInputDataFromSpill`) both follow Step 1; there is no ordering dependency between them. The §3.2 pseudocode runs the per-gate loop (containing Step 2) before the explicit Step 3 call, which is the recommended linear form.

## 4. The `RecoveryCheckpointBarrier` sentinel

```java
public final class RecoveryCheckpointBarrier implements Buffer {
    /** cpId of the triggering checkpoint; Step 2 matches on this. */
    long getCheckpointId();
}
```

Constraints:

- Only the task thread `add`s it into `recoveredBuffers` in Step 1 (via `onRecoveredStateBuffer`);
- Only the task thread recognizes and `remove`s it in Step 2;
- The operator layer never sees it, because Step 2 always completes before the channel's next task consumption loop (same mailbox tick);
- At the implementation level, this can be a marker field on an existing `Buffer` subclass or a brand-new sentinel type; the final encoding form will be decided during landing, but **the semantics will not change**.

## 5. Correctness proof

Suppose the task thread finishes Step 1 at some moment T. Prove this checkpoint is complete and contains no duplicates:

- **Complete**: at moment T, all unconsumed recovery data falls into two parts —
  - the portion already drained into some channel but not yet consumed by the task → before the barrier in that channel's `recoveredBuffers` → captured by Step 2;
  - the portion still on disk (in entry granularity, `entryPos >= snap.startPos`) → captured by Step 3.

- **No duplicates**: inside `synchronized (lock)` at moment T, `currentOffset` and each channel's barrier position are observed at the same time; Principle 2 guarantees that "advance disk offset" and "channel add-buffer" are the same atomic action, so the two positions are a snapshot of the same physical instant — it is impossible for some entry to be before `currentOffset` (i.e. "already delivered") and at the same time after the barrier (i.e. "not yet delivered").

- **drain resuming does not contaminate this checkpoint**: Principle 1 guarantees that before the lock is released, `channelIOExecutor` cannot enter any channel's `recoveredBuffers`; after the lock is released, its next add-buffer is guaranteed to happen-after the already-inserted barrier, so all new deliveries land after the barrier.

## 6. Relationship to the FLINK-39519 class of races

On master, listener switching on `RecoveredInputChannel` (the channel reference changes after `stateConsumedFuture` triggers conversion) once caused a stale-enqueue race. Under this design:

- conversion completes **before** drain starts (filter → conversion → drain is strictly serial; see [`overview.md`](./overview.md) §2);
- `SpillFileReader.allChannels` is captured with physical channel references at construction time and is never switched again during drain;
- there is no listener-switching window; no possibility of stale-enqueue.

## 7. Defensive invariants — fail loud, not silent

Where the design guarantees "X is reachable / present / acquirable at this point", the implementation must throw on the negative — never silent skip or default. Concrete sites:

- `channel.checkpointStarted` in-recovery branch: must find the cpId-matched `RecoveryCheckpointBarrier` (§3.3).
- `SpillFileReader.drain`: must find the physical channel via `channelByInfo.get(entry.channelInfo)`.
- `ChannelStateFilteringHandler.filterAndRewrite`: must find the `VirtualChannel` for `(oldSubtask, oldChannel)`.
- `RecoveredChannelStateHandler.recover`: `getMappedChannels` must not return null.
- Filter-path buffer pool exhaustion: must park, never fall back to heap allocation.
- Every cross-thread future (`bufferFilteringCompleteFuture`, `conversionDoneFutures`, `drainHandoff`, `stateConsumedFuture`): must be completed (success or exceptional) on every reachable terminal path.
- Every `SpillFile.acquire()` must pair with a `release()`; every drain exit must call `finishReadRecoveredState()` for every channel.
