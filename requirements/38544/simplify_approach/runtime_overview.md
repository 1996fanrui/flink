# Runtime Overview

> Target state after Tradeoff 1 = B and Tradeoff 6 = B. Current implementation is not depicted.

## Legend

- Solid arrow → main data flow
- Dashed arrow ⤳ wake-up / notification
- Colors: 🟦 unspilling thread ｜ 🟩 task thread ｜ 🟨 ChannelStateWriter executor ｜ ⬜ shared object

## The Diagram

```mermaid
flowchart TB
    classDef unspill fill:#cfe2ff,stroke:#3d6ad6,color:#000
    classDef task    fill:#d4edda,stroke:#2d8a4a,color:#000
    classDef csw     fill:#fff3cd,stroke:#b8860b,color:#000
    classDef shared  fill:#f0f0f0,stroke:#666,color:#000
    classDef sync    fill:#fde6cf,stroke:#cc6600,color:#000

    U["🟦 Unspilling Thread<br/>filter → flush → drain → close"]:::unspill
    T["🟩 Task Thread (mailbox)<br/>conversion / consume / checkpoint"]:::task
    CSW["🟨 ChannelStateWriter executor"]:::csw

    Disp["FilteredBufferDispatcher (per task)<br/>cache + spill-file ref<br/>single-threaded, no lock"]:::shared
    Store["RecoveredBufferStore (per channel)<br/>readyBuffers + pendingDiskRefs<br/>🔒 store monitor"]:::shared
    IC["InputChannel (per channel)<br/>Local / Remote<br/>holds store ref"]:::shared
    Spill[("Spill File (per task, shared)")]:::shared
    Gate["SingleInputGate<br/>(community master, unchanged)"]:::shared
    Pool["BufferPool<br/>(community master, unchanged)"]:::shared
    ConvFut["conversionDoneFuture"]:::sync

    %% filter phase
    U -- "bytes" --> Disp
    Disp -- "P1: write buffer" --> Store
    Disp -- "P2: append bytes" --> Spill
    Disp -- "P2: register DiskRef" --> Store

    %% conversion barrier between filter and drain
    T -- "do conversion, then complete" --> ConvFut
    ConvFut -. "await before drain" .-> U

    %% drain phase
    U -- "pop DiskRef + read disk + addBuffer" --> Store
    U -- "positional read" --> Spill
    U -. "request buffer (may park)" .-> Pool
    Pool -. "buffer available" .-> U

    %% wake task on new data
    Store -. "data available" .-> IC
    IC -. "notify" .-> Gate
    Gate -. "wake" .-> T

    %% task consume + recycle
    T -- "getNextBuffer (via gate)" --> IC
    IC -- "tryTake" --> Store
    T -. "recycle" .-> Pool
    Pool -. "segment available, wake" .-> U

    %% checkpoint
    T -- "checkpointStarted" --> IC
    IC -- "store.checkpoint(writer)" --> Store
    Store -- "atomic snapshot ready + pending<br/>open InputStream per DiskRef" --> Spill
    Store -- "addInputData<br/>(buffer iter + InputStreams)" --> CSW
```

## Threads

| Thread | Lifecycle | What it does |
|---|---|---|
| 🟦 unspilling-`<task>` | Single-thread executor, lives from task setup until recovery completes | filter → flush → await `conversionDoneFuture` → drain → close. Exclusive owner of dispatcher state and spill-file writes. |
| 🟩 task main thread (mailbox) | Whole task lifetime | Triggers conversion, consumes buffers from store, recycles them, handles checkpoint barriers. |
| 🟨 ChannelStateWriter executor | Single thread, async | Consumes the `addInputData` queue and writes checkpoint output. Does not touch dispatcher or store. |

## Objects

| Object | Scope | Key state |
|---|---|---|
| Dispatcher | per task | byte cache, spill-file reference, readers list. All fields owned by the unspilling thread; no lock. |
| Spill File | per task, shared across channels | `FileChannel` + `Path`, append-only by unspilling, positional read by everyone. |
| Store | per channel | `readyBuffers`, `pendingDiskRefs`, `dataAvailableListener` — all guarded by the store's intrinsic monitor (the only extra lock this branch adds). |
| SingleInputGate | per gate | Untouched community-master behaviour (`inputChannelsWithData` monitor, `channels[]`, etc.). |
| BufferPool | per task | Untouched community-master behaviour. |

## Phases at a Glance

1. **Filter** — unspilling thread feeds bytes through the dispatcher. The dispatcher chooses **P1** (network buffer → `store.readyBuffers`) when the pool has capacity, otherwise **P2** (spill bytes to disk + register `DiskRef` in `store.pendingDiskRefs`). Task thread does nothing yet (Tradeoff 1 = B).
2. **Conversion** — once filter finishes, task thread does the standard `RecoveredInputChannel → Local/Remote` swap inside the gate's `inputChannelsWithData` monitor and completes `conversionDoneFuture`.
3. **Drain** — unspilling thread, blocked on `conversionDoneFuture` until conversion is done, then loops over channels round-robin: pop next `DiskRef`, request a buffer (may park), positional read disk → buffer, `store.addBuffer`.
4. **Consume** — task thread reads buffers via `gate → store.tryTake`. Each `buffer.recycle` returns a segment to `BufferPool`, which wakes the unspilling thread blocked on `requestBufferBlocking`.
5. **Checkpoint** — see the dedicated diagram below.

## Checkpoint: Atomic Snapshot of Ready + Pending

The invariant that makes this design correct: **at checkpoint time the store monitor is taken once, and both `readyBuffers` (network inflight) and `pendingDiskRefs` (disk inflight) are snapshotted inside the same critical section.** A two-pass scheme would let drain move an entry from pending to ready in between, losing it from both snapshots.

```mermaid
sequenceDiagram
    autonumber
    participant U as 🟦 unspilling (drain)
    participant T as 🟩 task
    participant Store as Store (channel C)
    participant Spill as Spill File
    participant CSW as 🟨 CSW executor

    Note over U,Store: drain keeps moving DiskRefs from pending to ready

    T->>T: checkpoint barrier arrives on channel C
    T->>T: channel.checkpointStarted(barrier)

    rect rgb(255, 200, 200)
        Note over T,Store: 🔒 critical section under store monitor
        T->>Store: synchronized(store) {
        T->>Store: retain every buffer in readyBuffers (network inflight)
        T->>Store: shallow-copy pendingDiskRefs (disk inflight)
        T->>Store: }
    end

    Note over T,Store: After releasing the monitor, drain may resume —<br/>but the task already holds an atomic snapshot.

    T-->>CSW: addInputData(checkpointId, info, retained buffer iterator)
    loop for each DiskRef
        T->>Spill: Files.newInputStream(path) + skip(offset)
        T-->>CSW: addInputData(checkpointId, info, InputStream, length)
    end
    CSW->>CSW: drain its queue, write checkpoint output sequentially
```

### Why the snapshot must be atomic

Drain's atomic step inside the store monitor is `pendingDiskRefs.pollFirst()` + `readyBuffers.add(buffer)`. Therefore at any instant a given `DiskRef e` is in **exactly one** of `pendingDiskRefs` or `readyBuffers` — never neither, never both. Snapshotting both queues under one monitor lock captures `e` exactly once.

If checkpoint instead locked twice (first for ready, then for pending), drain could slip in between the two locks, pop `e` from pending, and add the buffer to ready **after** the ready snapshot was already taken. `e` would then be absent from both snapshots and lost from the checkpoint.

### Channels are independent

Each channel snapshots its own store on its own thread path, with no cross-channel coordination. Dispatcher is not involved. The price is that the per-channel disk reads (`InputStream` per `DiskRef`) end up as random I/O on the shared spill file — accepted under Tradeoff 6 = B because seek cost is negligible on SSD/NVMe.

## Locks Introduced by This Branch (Summary)

Only one: the `RecoveredBufferStore` intrinsic monitor.

Everything else (gate's `inputChannelsWithData` monitor, channel-side `receivedBuffers` monitors, `BufferPool` internals, `requestLock`) is community-master behaviour and is reused unchanged.

Non-lock coordination: `conversionDoneFuture` (a `CompletableFuture`) to serialise filter → conversion → drain.
