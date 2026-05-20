# Task Breakdown

All tasks are based on **community master**, not the current branch. Current branch is being abandoned; the 7 code commits on it (FLINK-39520 ~ FLINK-39524 + `015f4a172e1` + `b97a5de9a54`) are **not carried over** — they implement the deprecated `RecoveredBufferStore` / `FilteredBufferDispatcher` / `OutputWriter` architecture and have no salvageable parts (`b97a5de9a54`'s heap-fallback removal is half-relevant but cleanly redone in Task 2b).

## Dependency graph

```
Task 0 (Refactor, pure, no behavior change)
  │
  ▼
Task 1 (Interfaces skeleton)
  │
  ├──► Task 2a (InputChannel side, parallel with 2b)
  │
  └──► Task 2b (Unspiller side, parallel with 2a)
              │
              ▼
        Task 3 (Tests)
```

- Task 0 and Task 1 have no hard dependency, but Task 0 first gives a clean baseline.
- Task 2a / 2b are **fully parallel** after Task 1 — each compiles against the Task 1 interface skeleton, they never touch each other's files. Either can merge first.
- Task 3 runs after both 2a and 2b have landed.
- There is no separate "integration" task — each side wires its own end of the system as part of its own PR.

## Task 0 — `LocalInputChannel` refactor: split `recoveredBuffers` out of `toBeConsumedBuffers`

**Pure refactor, behavior unchanged.** Master's FLINK-39018 commits coupled recovery buffers into `toBeConsumedBuffers`; we decouple them.

- Add new field `recoveredBuffers: Deque<...>`
- Constructor param `initialRecoveredBuffers` populates the new field (no longer `toBeConsumedBuffers`)
- `getNextBuffer` drains `recoveredBuffers` first, then falls through
- `checkpointStarted` scans `recoveredBuffers` for inflight (no longer `toBeConsumedBuffers`)
- `toBeConsumedBuffers` reverts to its pre-39018 single-purpose role (`FullyFilledBuffer` splits only)

Single commit. Independent of FLINK-38544 functionally; mergeable on its own.

## Task 1 — Interface skeleton

**Signatures and javadoc only, zero implementation.**

- `RecoveryCheckpointTrigger` (1 method)
- `RecoverableInputChannel` (2 methods)
- `BufferRequester` (2 methods)
- Type skeletons: `DiskSnapshot` (with inner `Chunk`), `RecoveryCheckpointBarrier` sentinel
- `ChannelStateWriter.addInputDataFromSpill(...)` signature (default empty impl)
- Promote `RecoveredInputChannel.releaseAllResources()` from package-private to `public`

After merge, both Task 2a and Task 2b can develop against these contracts.

## Task 2a — InputChannel side (parallel with 2b)

**Depends on Task 0 + Task 1.** All channel-side / task-thread-side wiring.

- Add `allRecoveredBuffersDelivered` flag on Local + Remote
- Implement `RecoverableInputChannel` on `RecoveredInputChannel`, `LocalInputChannel`, `RemoteInputChannel`
- Channel-internal locking: reuse `synchronized(receivedBuffers)` on Remote, `synchronized(recoveredBuffers)` on Local
- Extend `getNextBuffer`: `inRecovery` branch + priority-event exception
- Extend `checkpointStarted`: mutually exclusive if/else (Step 2 embedded)
- `stateConsumedFuture` fires when `(flag == true && recoveredBuffers.isEmpty())`
- New helper `ChannelState.onCheckpointStartedForAllInputs(barrier, writer)`
- Modify `AlternatingWaitingForFirstBarrierUnaligned` + `AlternatingCollectingBarriers` to call the helper (wires the 3-step trigger chain)
- Unit tests

## Task 2b — Unspiller side (parallel with 2a)

**Depends on Task 1.** All unspilling-thread-side / writer-side wiring.

- `SpillFile` + `SpillFileSegment` + `Entry`
- `FilteredBufferWriter` (prefilter + postfilter buffers, writes to spill file)
- `Unspiller` class (constructor, drain loop, `snapshotAndInsertBarriers`, `close`)
- `RecoveredChannelBufferRequester` implements `BufferRequester`
- `DiskSnapshot` full implementation + iterator that skips `entryPos < startPos`
- `ChannelStateWriter.addInputDataFromSpill(...)` body (async writer demux)
- Rewire `RecoveredChannelStateHandler.recover`'s filter branch: destination from `channel.onRecoveredStateBuffer` to `filteredBufferWriter.write`
- Wire `Unspiller` lifecycle (filter complete → conversion → mailbox submits drain task → close on completion)
- **Remove the heap fallback in `RecoveredInputChannel.requestBufferBlocking`** — the final FLINK-38544 goal action
- Unit tests

## Task 3 — System tests

Depends on 2a + 2b both merged.

- Run master's existing `UnalignedCheckpointRescaleITCase`
- New ITCase: UC fired during recovery (exercises the 3-step protocol)
- New ITCase: rescale + filter + large record (verifies OOM fix)
