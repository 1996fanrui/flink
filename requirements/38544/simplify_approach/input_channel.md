# InputChannel-side changes

> Scope: when `checkpointingDuringRecoveryEnabled=true` + filter is on, this doc covers the entry through which the drain phase delivers a recovered buffer into a physical `InputChannel` and how that channel coordinates consumption with concurrently-arriving upstream data. When the feature is off, master is not touched.

## 1. Design principles

- Drain uses the **same two-method vocabulary** for every channel kind (recovered / local / remote). The channel implementation hides the rest.
- The `notifyChannelNonEmpty / queueChannel / inputChannelsWithData` wake-up chain on master is **unchanged**; recovery delivery and upstream delivery both go through it.
- Recovery data and upstream data must be consumed **in order** (all recovery data before any normal upstream data), with one explicit exception: priority events on the upstream side (UC barriers, etc.) may pass through during recovery — this is exactly what `checkpointingDuringRecoveryEnabled` exists for.
- Every channel write during recovery happens inside `Unspiller.lock` (see [`coordination.md`](./coordination.md) principle 1).

## 2. Why this requires channel-side changes

Master `RecoveredInputChannel` keeps a single FIFO `receivedBuffers` and puts every recovered buffer in **before** `requestSubpartition()` is called, so order is automatic. In this design drain runs **after** conversion (so the physical channel is in place when checkpoint barriers arrive) — by then upstream has already been told to send, and upstream data may race drain into the same channel. A "channel side untouched" assumption is therefore impossible: something on the channel must separate recovery delivery from upstream delivery and enforce ordering. Rejected alternatives that try to avoid touching the channel are listed in §5.

## 3. Final design

### 3.1 The `RecoverableInputChannel` interface

The two new methods are extracted into a Java interface declared in [`overview.md`](./overview.md#62-recoverableinputchannel--unspilling-thread--physical-channels) §6.2, implemented by every channel kind that participates in recovery delivery: `RecoveredInputChannel`, `LocalInputChannel`, `RemoteInputChannel`. Drain holds channel references typed as `RecoverableInputChannel` (via `Unspiller.channelByInfo: Map<InputChannelInfo, RecoverableInputChannel>`); it never casts down to a specific channel class. Method names are taken verbatim from master's existing `RecoveredInputChannel` API so drain has a uniform vocabulary across all three implementations.

The interface intentionally contains only the delivery side (`onRecoveredStateBuffer`, `finishReadRecoveredState`). Buffer allocation lives behind a separate contract (`BufferRequester`, [`overview.md`](./overview.md#63-bufferrequester--unspilling-thread--buffer-pool) §6.3); this keeps `LocalInputChannel`, which has no per-channel buffer pool on master, from being forced to invent one.

### 3.2 New fields on each physical channel

| Field | Single, well-defined purpose |
|---|---|
| `recoveredBuffers: Deque<Buffer>` (new) | FIFO of buffers delivered by drain. The field exists only for recovery; once `recoveredStateFinished=true` and the queue has drained, it stays empty for the rest of the task's lifetime. |
| `recoveredStateFinished: boolean` (new) | Starts false; flipped to true exactly once by `finishReadRecoveredState()`. The single source of truth for "drain is no longer producing into this channel". |

### 3.3 Locking `recoveredBuffers` — reuse, don't add a new lock

`recoveredBuffers` has two writers (drain on `channelIOExecutor`, task thread at Step 1) and one reader (task thread, both for Step 2 snapshot and for normal `getNextBuffer` consumption), so it MUST be guarded. The design **reuses an existing monitor** in both channel kinds rather than introducing a third lock object:

| Channel | Channel-internal monitor for `recoveredBuffers` |
|---|---|
| `RemoteInputChannel` | Reuse master's existing `synchronized(receivedBuffers)`. The same monitor now also guards `recoveredBuffers` — no new lock object on `RemoteInputChannel`. |
| `LocalInputChannel` | Use `synchronized(recoveredBuffers)` —— Local has no `receivedBuffers` field on master, so the new field's own identity serves as its monitor. |

Global lock order:

```
Unspiller.lock → channel-internal queue monitor
```

`onRecoveredStateBuffer` and `finishReadRecoveredState` enter the channel monitor while the caller already holds `Unspiller.lock`; `getNextBuffer` and Step 2's snapshot walk hold only the channel monitor; network `onBuffer` (Remote) still holds only `receivedBuffers` (= the same channel monitor). No path takes the locks in reverse — no cycle, no deadlock. See [`coordination.md`](./coordination.md#lock-order) §1 "Lock order" for the full per-path table.

### 3.4 The two semantically distinct queues on `LocalInputChannel`

`LocalInputChannel` ends up with two `Deque`s. They are **never mixed** — each holds exactly one kind of buffer:

| Deque | Holds | Producer | Consumer | Lifetime |
|---|---|---|---|---|
| `recoveredBuffers` (NEW, this design) | Buffers delivered by drain during recovery | `channelIOExecutor` drain (cross-thread) | task thread | Recovery only; dead afterwards |
| `toBeConsumedBuffers` (master existing, kept) | `FullyFilledBuffer` partial-buffer splits returned by `subpartitionView.getNextBuffer()` | task thread (re-entrant inside `getNextBuffer`) | task thread (same call frame) | Any time during normal operation |

On `RemoteInputChannel` the picture is simpler: `recoveredBuffers` (new) holds drain output; master's existing `receivedBuffers` holds upstream traffic from `onBuffer`. Nothing else changes.

### 3.5 Decouple FLINK-39018's recovery wiring from `toBeConsumedBuffers`

The recovery feature itself is **not** being removed — it is still required. The point of this section is that three FLINK-39018 commits wired recovery on top of `toBeConsumedBuffers` because that was the only buffer-holder available at the time. With `recoveredBuffers` now in place, each piece of recovery wiring is **moved** off `toBeConsumedBuffers` and onto the new field (or, for the checkpoint case, onto the 3-step protocol). The recovery responsibility itself is preserved on every row below; only the field it operates on changes.

| FLINK-39018 site | Currently (recovery coupled to `toBeConsumedBuffers`) | After decoupling (recovery moved onto `recoveredBuffers`) |
|---|---|---|
| Constructor param `ArrayDeque<Buffer> initialRecoveredBuffers` + body migrating it into `toBeConsumedBuffers` (commit `d1914c63c95`) | Receives migrated buffers from `RecoveredInputChannel.toInputChannel()` and stashes them in `toBeConsumedBuffers` | Migration still happens, but expressed via the new uniform method path: `RecoveredInputChannel.toInputChannel()` calls `onRecoveredStateBuffer` for each buffer and then `finishReadRecoveredState()`, so migrated buffers land in `recoveredBuffers` — same shape drain uses. The dedicated constructor parameter is dropped because it is no longer needed; the migration is expressed through the new methods. |
| `getNextBuffer` early branch `if (!toBeConsumedBuffers.isEmpty()) return getNextRecoveredBuffer()` (commit `cebc174ad5f`) | Routes `toBeConsumedBuffers` consumption through a recovery-aware path (because the queue also holds recovered buffers) | `toBeConsumedBuffers` branch returns to its FullyFilledBuffer-splits-only form (`return getBufferAndAvailability(toBeConsumedBuffers.removeFirst())`). A new branch on `recoveredBuffers` (§3.5) serves recovery. |
| `getNextRecoveredBuffer()` method body (commit `cebc174ad5f`) | Priority-event interleaving against `toBeConsumedBuffers`-held recovered buffers | The interleaving logic is preserved; it moves into the new recovery branch and operates on `recoveredBuffers` + `hasPendingPriorityEvent` instead. The standalone method is removed because there is no longer a coupling to interleave against. |
| `hasPendingPriorityEvent` field (commit `cebc174ad5f`) | Set by `notifyPriorityEvent`, consulted by `getNextRecoveredBuffer` | Unchanged. The new recovery branch reads it for the same purpose. |
| `checkpointStarted` scanning `toBeConsumedBuffers` for inflight buffers (commit `3aef0932ded`) | Persists `toBeConsumedBuffers` contents (which included recovered buffers) on a checkpoint barrier | Recovery-time persistence is **not lost** — it moves to the 3-step protocol in [`coordination.md`](./coordination.md), which snapshots `recoveredBuffers` together with the on-disk slice. `checkpointStarted` returns to its pre-39018 shape (`startPersisting(barrier.getId(), Collections.emptyList())`) because `toBeConsumedBuffers` no longer carries any recovery data. |

Net effect: every recovery responsibility previously living on `toBeConsumedBuffers` continues to exist, just rewired onto `recoveredBuffers` (or, for the checkpoint case, onto the 3-step protocol). `toBeConsumedBuffers` is restored to its FullyFilledBuffer-splits-only role.

### 3.6 Consumer logic — `getNextBuffer()`

```
if (!recoveredStateFinished):
    // Recovery in progress: serve recoveredBuffers; let only priority events through from upstream.
    if (hasPendingPriorityEvent)            return <pull priority event from upstream>;
    if (!recoveredBuffers.isEmpty())        return recoveredBuffers.poll();
    return empty;                                                                       // block ordinary upstream
else:
    // Recovery done: drain any leftover recoveredBuffers, then resume master behavior.
    if (!recoveredBuffers.isEmpty())        return recoveredBuffers.poll();
    return <master path>;                                                                // toBeConsumedBuffers splits → subpartitionView (Local) ; receivedBuffers (Remote)
```

`<pull priority event from upstream>` reuses the master-existing priority path: for `RemoteInputChannel`, it is the `addPriorityBuffer` / head-of-`receivedBuffers` priority position; for `LocalInputChannel`, it pulls the priority entry out of `subpartitionView` (the logic that used to live in `getNextRecoveredBuffer`). This is the only path by which upstream data may leave the channel during recovery — it is what lets unaligned checkpoint barriers from upstream fire while drain is still producing.

### 3.7 `stateConsumedFuture`

Completed by the channel itself when both hold:
- `recoveredStateFinished == true`;
- `recoveredBuffers` is empty.

Whichever transition makes both true is the trigger — either `finishReadRecoveredState()` runs when `recoveredBuffers` is already empty, or the consumer polls the last entry off `recoveredBuffers` after the flag is already set. No EOICS sentinel is inserted into the queue.

### 3.8 Why we did not reuse existing master upstream entries

- `RemoteInputChannel.onBuffer` also runs sequence-number validation, priority branching, `channelStatePersister` bookkeeping, and `onSenderBacklog` — all network-protocol-specific side effects that misfire on drain output. Bypassing them in-place is larger surgery than adding a separate method.
- `LocalInputChannel` has no push entry on master at all (pull-based via `subpartitionView`); something new must be added regardless.

## 4. Invariants

- `getNextBuffer` external callers (`InputGate.pollNext`, `StreamTaskNetworkInput`, etc.) keep the same signature and contract.
- `notifyChannelNonEmpty / queueChannel / inputChannelsWithData` is unchanged; both upstream add (master path) and `recoveredBuffers` add (new `onRecoveredStateBuffer`) call it.
- The priority-event chain on the upstream side (`addPriorityBuffer / firstPriorityEvent`) is unchanged.
- `recoveredStateFinished` transitions false → true exactly once per recovery, inside `Unspiller.lock`.
- `toBeConsumedBuffers` carries only `FullyFilledBuffer` splits — no recovery data. Its access remains single-threaded (task thread re-entrant inside `getNextBuffer`); no lock is needed for it.
- The existing `onRecoveredStateBuffer` / `finishReadRecoveredState` on `RecoveredInputChannel` are untouched and continue to serve the filter-off path.

## 5. Rejected alternatives

Kept short so a future reader can see why each was dropped without revisiting the discussion.

- **Defer `requestSubpartition()` until drain completes.** Violates the non-negotiable invariant in [`overview.md`](./overview.md) §1 — upstream subscription must be issued early enough for checkpoints to fire during recovery.
- **Single FIFO `receivedBuffers` + a consumption-gate flag.** Upstream data still races into the same queue ahead of drain output; gating consumption cannot retroactively fix the in-queue order.
- **Reuse `RemoteInputChannel.onBuffer` for drain delivery.** The method also runs sequence-number / persister / backlog bookkeeping that does not apply to drain output; bypassing those in-place is larger surgery than adding a separate method.
- **Reuse `LocalInputChannel.toBeConsumedBuffers` for recovery delivery** (FLINK-39018's current approach). The two flows have disjoint producers, lifetimes, and synchronization requirements; sharing one Deque forces the FullyFilledBuffer hot path to acquire a lock it does not need and conflates two distinct invariants. See §3.4 for the precise revert points.
- **Wrap a `ResultSubpartitionView`** so drain is hidden behind the existing pull path. Pushes the change off the channel but intrudes on a more central abstraction with broader blast radius than the two new methods.
