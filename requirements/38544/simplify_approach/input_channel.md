# InputChannel-side changes

> Scope: when `checkpointingDuringRecoveryEnabled=true` + filter is on, this doc covers the entry through which the drain phase delivers a recovered buffer into a physical `InputChannel` and how that channel coordinates consumption with concurrently-arriving upstream data. When the feature is off, master is not touched.

## 1. Design principles

- Drain uses the **same two-method vocabulary** for every channel kind (recovered / local / remote). The channel implementation hides the rest.
- The `notifyChannelNonEmpty / queueChannel / inputChannelsWithData` wake-up chain on master is **unchanged**; recovery delivery and upstream delivery both go through it.
- Recovery data and upstream data must be consumed **in order** (all recovery data before any normal upstream data), with one explicit exception: priority events on the upstream side (UC barriers, etc.) may pass through during recovery — this is exactly what `checkpointingDuringRecoveryEnabled` exists for.
- Every recovery-data add to `recoveredQueue` happens inside the drainer's `lock` (see [`coordination.md`](./coordination.md) principle 1; end-of-drain `finishRecoveredBufferDelivery` is the exception).

## 2. Why this requires channel-side changes

Master `RecoveredInputChannel` keeps a single FIFO `receivedBuffers` and puts every recovered buffer in **before** `requestSubpartition()` is called, so order is automatic. In this design drain runs **after** conversion (so the physical channel is in place when checkpoint barriers arrive) — by then upstream has already been told to send, and upstream data may race drain into the same channel. A "channel side untouched" assumption is therefore impossible: something on the channel must separate recovery delivery from upstream delivery and enforce ordering. Rejected alternatives that try to avoid touching the channel are listed in §5.

Second, in-recovery checkpoints require a new per-channel method to snapshot `recoveredQueue` up to a cpId-matched `RecoveryCheckpointBarrier` — absent on master.

## 3. Final design

### 3.1 The `RecoverableInputChannel` interface

The new methods are extracted into a Java interface declared in [`overview.md`](./overview.md#62-recoverableinputchannel--unspilling-thread--physical-channels) §6.2, implemented by every channel kind that participates in recovery delivery: `RecoveredInputChannel`, `LocalInputChannel`, `RemoteInputChannel`. Drain holds channel references typed as `RecoverableInputChannel` (via `SpillFileDrainer.channelByInfo: Map<InputChannelInfo, RecoverableInputChannel>`); it never casts down to a specific channel class. Method names are taken verbatim from master's existing `RecoveredInputChannel` API so drain has a uniform vocabulary across all three implementations.

The interface contains the delivery side (`onRecoveredStateBuffer`, `finishRecoveredBufferDelivery`) plus buffer allocation (`requestRecoveryBufferBlocking()`). Each channel kind implements `requestRecoveryBufferBlocking()` against its own per-channel `BufferManager`, so the recovered buffer is owned by the physical channel that will eventually recycle it; a `LocalInputChannel` constructed without recovery support has no such pool and rejects the call.

### 3.2 New fields on each physical channel

| Field | Single, well-defined purpose |
|---|---|
| `recoveredQueue: RecoveredBufferQueue` (new) | FIFO of buffers delivered by drain. The field exists only for recovery; once `allDelivered=true` and the queue has drained, it stays empty for the rest of the task's lifetime. |
| `allDelivered: boolean` (inside `recoveredQueue`) | Starts false; flipped to true exactly once by `finishRecoveredBufferDelivery()`. Specifically means **"the spiller / drain producer has finished adding recovered buffers into this channel"** — it does NOT mean "the consumer has finished consuming them". Full recovery completion is `allDelivered == true && recoveredQueue.isEmpty()`. |

### 3.3 Locking `recoveredQueue` — reuse, don't add a new lock

`recoveredQueue` has two writers (drain on `channelIOExecutor`, task thread at Step 1) and one reader (task thread, both for Step 2 snapshot and for normal `getNextBuffer` consumption), so it MUST be guarded. The design **reuses an existing monitor** in both channel kinds rather than introducing a third lock object:

| Channel | Channel-internal monitor for `recoveredQueue` |
|---|---|
| `RemoteInputChannel` | Reuse master's existing `synchronized(receivedBuffers)`. The same monitor now also guards `recoveredQueue` — no new lock object on `RemoteInputChannel`. |
| `LocalInputChannel` | Use `synchronized(recoveredQueue)` —— Local has no `receivedBuffers` field on master, so `recoveredQueue`'s own identity serves as its monitor. |

Global lock order:

```
drainer.lock → channel-internal queue monitor
```

`onRecoveredStateBuffer` enters the channel monitor while the caller already holds the drainer's `lock`; `finishRecoveredBufferDelivery` enters the channel monitor without the drainer's `lock` (end-of-drain exception, see [`coordination.md`](./coordination.md) §1); `getNextBuffer` and Step 2's snapshot walk hold only the channel monitor; network `onBuffer` (Remote) still holds only `receivedBuffers` (= the same channel monitor). No path takes the locks in reverse — no cycle, no deadlock. See [`coordination.md`](./coordination.md#lock-order) §1 "Lock order" for the full per-path table.

### 3.4 The two semantically distinct queues on `LocalInputChannel`

`LocalInputChannel` ends up with two `Deque`s. They are **never mixed** — each holds exactly one kind of buffer:

| Deque | Holds | Producer | Consumer | Lifetime |
|---|---|---|---|---|
| `recoveredQueue` (NEW, this design) | Buffers delivered by drain during recovery | `channelIOExecutor` drain (cross-thread) | task thread | Recovery only; dead afterwards |
| `toBeConsumedBuffers` (master existing, kept) | `FullyFilledBuffer` partial-buffer splits returned by `subpartitionView.getNextBuffer()` | task thread (re-entrant inside `getNextBuffer`) | task thread (same call frame) | Any time during normal operation |

On `RemoteInputChannel` the picture is simpler: `recoveredQueue` (new) holds drain output; master's existing `receivedBuffers` holds upstream traffic from `onBuffer`. Nothing else changes.

### 3.5 Decouple FLINK-39018's recovery wiring from `toBeConsumedBuffers`

The recovery feature itself is **not** being removed — it is still required. The point of this section is that three FLINK-39018 commits wired recovery on top of `toBeConsumedBuffers` because that was the only buffer-holder available at the time. With `recoveredQueue` now in place, each piece of recovery wiring is **moved** off `toBeConsumedBuffers` and onto the new field (or, for the checkpoint case, onto the 3-step protocol). The recovery responsibility itself is preserved on every row below; only the field it operates on changes.

| FLINK-39018 site | Currently (recovery coupled to `toBeConsumedBuffers`) | After decoupling (recovery moved onto `recoveredQueue`) |
|---|---|---|
| Constructor param `ArrayDeque<Buffer> initialRecoveredBuffers` + body migrating it into `toBeConsumedBuffers` (commit `d1914c63c95`) | Receives migrated buffers from `RecoveredInputChannel.toInputChannel()` and stashes them in `toBeConsumedBuffers` | Migration still happens, but expressed via the new uniform method path: `RecoveredInputChannel.toInputChannel()` calls `onRecoveredStateBuffer` for each buffer and then `finishRecoveredBufferDelivery()`, so migrated buffers land in `recoveredQueue` — same shape drain uses. The dedicated constructor parameter is dropped because it is no longer needed; the migration is expressed through the new methods. |
| `getNextBuffer` early branch `if (!toBeConsumedBuffers.isEmpty()) return getNextRecoveredBuffer()` (commit `cebc174ad5f`) | Routes `toBeConsumedBuffers` consumption through a recovery-aware path (because the queue also holds recovered buffers) | `toBeConsumedBuffers` branch returns to its FullyFilledBuffer-splits-only form (`return getBufferAndAvailability(toBeConsumedBuffers.removeFirst())`). A new branch on `recoveredQueue` (§3.6) serves recovery. |
| `getNextRecoveredBuffer()` method body (commit `cebc174ad5f`) | Priority-event interleaving against `toBeConsumedBuffers`-held recovered buffers | The interleaving logic is preserved; it moves into the new recovery branch and operates on `recoveredQueue` + `hasPendingPriorityEvent` instead. The standalone method is removed because there is no longer a coupling to interleave against. |
| `hasPendingPriorityEvent` field (commit `cebc174ad5f`) | Set by `notifyPriorityEvent`, consulted by `getNextRecoveredBuffer` | Unchanged. The new recovery branch reads it for the same purpose. |
| `checkpointStarted` scanning `toBeConsumedBuffers` for inflight buffers (commit `3aef0932ded`) | Persists `toBeConsumedBuffers` contents (which included recovered buffers) on a checkpoint barrier | Recovery-time persistence is **not lost** — it moves to the 3-step protocol in [`coordination.md`](./coordination.md), which snapshots `recoveredQueue` together with the on-disk slice. `checkpointStarted` returns to its pre-39018 shape (`startPersisting(barrier.getId(), Collections.emptyList())`) because `toBeConsumedBuffers` no longer carries any recovery data. |

Net effect: every recovery responsibility previously living on `toBeConsumedBuffers` continues to exist, just rewired onto `recoveredQueue` (or, for the checkpoint case, onto the 3-step protocol). `toBeConsumedBuffers` is restored to its FullyFilledBuffer-splits-only role.

### 3.6 Consumer logic — `getNextBuffer()`

Uses the same `inRecovery` predicate as §3.8 (single source of truth: this channel still has recovery-side work iff producer hasn't finished OR queue isn't drained yet):

```
inRecovery = !allDelivered || !recoveredQueue.isEmpty();
if (inRecovery):
    if (hasPendingPriorityEvent)        return <pull priority event from upstream>;
    if (!recoveredQueue.isEmpty())      return recoveredQueue.poll();
    return empty;                                                                       // block ordinary upstream
else:
    return <master path>;                                                                // toBeConsumedBuffers splits → subpartitionView (Local) ; receivedBuffers (Remote)
```

`<pull priority event from upstream>` reuses the master-existing priority path: for `RemoteInputChannel`, head-of-`receivedBuffers` priority position (via `addPriorityBuffer`); for `LocalInputChannel`, pull the priority entry out of `subpartitionView` (logic that used to live in `getNextRecoveredBuffer`). This is the only path by which upstream data leaves the channel during recovery — it lets unaligned checkpoint barriers fire while drain is still producing.

### 3.7 `stateConsumedFuture`

Completed by the channel itself when both hold:
- `allDelivered == true`;
- `recoveredQueue` is empty.

Whichever transition makes both true is the trigger — either `finishRecoveredBufferDelivery()` runs when `recoveredQueue` is already empty, or the consumer polls the last entry off `recoveredQueue` after the flag is already set. No EOICS sentinel is inserted into the queue, and no external "bootstrap" / "task close" path is allowed to complete the future — any such fallback would mask a missing `finishRecoveredBufferDelivery` upstream.

### 3.8 Extending `checkpointStarted` to snapshot `recoveredQueue`

`channel.checkpointStarted(barrier)` is master's per-channel snapshot entry, reached from `IndexedInputGate.checkpointStarted` iterating channels. The cpId-bounded scan can be expressed either **(a) inlined into the existing `checkpointStarted`** (one method, two mutually exclusive branches — shown in the pseudocode below) **or (b) extracted into a sibling method** such as `snapshotDuringRecovery(long cpId)` / `checkpointStartedInRecoveryPhase` called by the dispatcher when the channel is in recovery. Both forms are protocol-equivalent; the dispatcher in [`coordination.md`](./coordination.md#32-shared-task-level-dispatcher) §3.2 only iterates gates per master and each channel picks one branch based on its own state. The doc shows form (a); the final choice is local to the channel implementation and can be made during coding when the actual diff size is visible.

Extension shape (pseudocode; same on Local & Remote):

```java
public void checkpointStarted(CheckpointBarrier barrier) throws CheckpointException {
    synchronized (channelMonitor()) {
        // Recovery-phase predicate uses BOTH fields. The four boundary cases:
        //   - drain hasn't produced yet:         !allDelivered, recoveredQueue empty   → in recovery
        //   - drain producing:                   !allDelivered, recoveredQueue non-empty → in recovery
        //   - drain done, consumer behind:        allDelivered, recoveredQueue non-empty → in recovery
        //   - recovery fully done:                allDelivered, recoveredQueue empty   → NOT in recovery
        // This predicate is the NEGATION of the stateConsumedFuture completion
        // condition (§3.7) — same source of truth, opposite phase.
        boolean inRecovery = !allDelivered || !recoveredQueue.isEmpty();

        if (inRecovery) {
            // Defensive: during recovery, upstream must not have sent live data buffers.
            assert receivedBuffersHasNoLiveDataBuffer()
                : "live upstream data observed in receivedBuffers during recovery";

            // Walk recoveredQueue up to the RecoveryCheckpointBarrier sentinel inserted
            // by Step 1; retain pre-barrier buffers and hand them to the channel state writer.
            List<Buffer> retained = new ArrayList<>();
            Iterator<Buffer> it = recoveredQueue.iterator();
            while (it.hasNext()) {
                Buffer b = it.next();
                if (b instanceof RecoveryCheckpointBarrier
                        && ((RecoveryCheckpointBarrier) b).getCheckpointId() == barrier.getId()) {
                    it.remove(); break;
                }
                retained.add(b.retainBuffer());           // retain — task still consumes from queue
            }
            channelStateWriter.addInputData(
                barrier.getId(), channelInfo, SEQUENCE_NUMBER_RESTORED,
                CloseableIterator.fromList(retained, Buffer::recycleBuffer));
        } else {
            // Master existing — channelStatePersister.startPersisting + maybePersist setup,
            // persists receivedBuffers content (upstream live data). Untouched from master.
            <master existing body>
        }
    }
}
```

Notes:

- **Mutually exclusive branches.** The two branches are **never both executed**. During recovery, only the new branch runs; outside recovery, only master's body runs. The temporal mutual-exclusion invariant (recovery-side state and upstream live data never coexist) makes this branch correctness-preserving — in either branch, the "other side"'s state is empty by construction.
- **`receivedBuffersHasNoLiveDataBuffer()`** is a channel-internal helper. Remote: iterate `receivedBuffers` and verify every buffer has `!Buffer.isBuffer()` (only priority events / control buffers, no data). Local: trivially `true` (no `receivedBuffers` field on master; live data is pulled via `subpartitionView` only after `recoveredQueue` is empty per §3.6).
- **`channelMonitor()`** is the same monitor used by `onRecoveredStateBuffer` and `getNextBuffer`: `synchronized(receivedBuffers)` on Remote, `synchronized(recoveredQueue)` on Local (§3.3).

### 3.9 Why we did not reuse existing master upstream entries

- `RemoteInputChannel.onBuffer` also runs sequence-number validation, priority branching, `channelStatePersister` bookkeeping, and `onSenderBacklog` — all network-protocol-specific side effects that misfire on drain output. Bypassing them in-place is larger surgery than adding a separate method.
- `LocalInputChannel` has no push entry on master at all (pull-based via `subpartitionView`); something new must be added regardless.

## 4. Invariants

- `getNextBuffer` external callers (`InputGate.pollNext`, `StreamTaskNetworkInput`, etc.) keep the same signature and contract.
- `notifyChannelNonEmpty / queueChannel / inputChannelsWithData` is unchanged; both upstream add (master path) and `recoveredQueue` add (new `onRecoveredStateBuffer`) call it.
- The priority-event chain on the upstream side (`addPriorityBuffer / firstPriorityEvent`) is unchanged.
- `allDelivered` transitions false → true exactly once per recovery, via the channel's internal monitor (end-of-drain exception, see [`coordination.md`](./coordination.md) §1).
- `toBeConsumedBuffers` carries only `FullyFilledBuffer` splits — no recovery data. Its access remains single-threaded (task thread re-entrant inside `getNextBuffer`); no lock is needed for it.
- The existing `onRecoveredStateBuffer` / `finishRecoveredBufferDelivery` on `RecoveredInputChannel` are untouched and continue to serve the filter-off path.

## 5. Rejected alternatives

Kept short so a future reader can see why each was dropped without revisiting the discussion.

- **Defer `requestSubpartition()` until drain completes.** Violates the non-negotiable invariant in [`overview.md`](./overview.md) §1 — upstream subscription must be issued early enough for checkpoints to fire during recovery.
- **Single FIFO `receivedBuffers` + a consumption-gate flag.** Upstream data still races into the same queue ahead of drain output; gating consumption cannot retroactively fix the in-queue order.
- **Reuse `RemoteInputChannel.onBuffer` for drain delivery.** The method also runs sequence-number / persister / backlog bookkeeping that does not apply to drain output; bypassing those in-place is larger surgery than adding a separate method.
- **Reuse `LocalInputChannel.toBeConsumedBuffers` for recovery delivery** (FLINK-39018's current approach). The two flows have disjoint producers, lifetimes, and synchronization requirements; sharing one Deque forces the FullyFilledBuffer hot path to acquire a lock it does not need and conflates two distinct invariants. See §3.4 for the precise revert points.
- **Wrap a `ResultSubpartitionView`** so drain is hidden behind the existing pull path. Pushes the change off the channel but intrudes on a more central abstraction with broader blast radius than the two new methods.
