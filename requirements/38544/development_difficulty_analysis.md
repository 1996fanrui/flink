# FLINK-38544 — Why the Spilling Change Is Bigger Than V1

Shortly, the code change of spilling logic is even bigger than checkpointing during recovery V1:

1. The checkpoint trigger had to change — from per-InputChannel snapshots to a single shared pass triggered by the last channel — to preserve cross-channel ordering and avoid random IO on the shared spill file.
2. Unifying the three data sources (recovered-network / recovered-disk / live) behind the InputChannel forced an async loader + state store — which requires priority events to stay responsive through the store and every new buffer to coordinate FIFO with the async drain thread.

Most of the effort lands in `FilteredBufferDispatcherImpl` and `FilteredSpillFile`. The dispatcher hosts the register-mode wait-set for #1 and the dynamic "spill idle" + priority coordination for #2; the spill file makes the single shared pass coexist with the async drain. Neither class has a direct predecessor in V1, so this part is largely new implementation.

## V1 baseline (for comparison)

```mermaid
flowchart LR
    Pool[Network Buffer Pool] -- "buffer" --> Filter[filterAndRewrite]
    Filter -- "filled buffer, enqueue" --> IC[InputChannel]
    IC --> Task[Task thread]

    style Pool fill:#e8f5e9,stroke:#43a047,color:#212121
    style Filter fill:#fff9c4,stroke:#fbc02d,color:#212121
    style IC fill:#c8e6c9,stroke:#388e3c,color:#1b5e20
    style Task fill:#eeeeee,stroke:#757575,color:#212121
```

- `filterAndRewrite` requests a buffer from the Network Buffer Pool
- Writes filtered bytes into the buffer
- Hands the buffer directly to the InputChannel
- No dispatcher, no spill file, no intermediate store

## V2 component overview

```mermaid
flowchart LR
    Filter[filterAndRewrite] --> Disp[FilteredBufferDispatcherImpl]
    Disp -- "spill (P2)" --> Spill[(FilteredSpillFile)]
    Spill -. "async drain" .-> Disp
    Disp -- "buffer (P1 / P3)" --> Store[RecoveredBufferStore]
    Store --> IC["InputChannel<br/>(Local / Remote / Recovered)"]
    IC --> Task[Task thread]
    IC -. "CP trigger (barrier)" .-> Store
    Store -. "onChannelCheckpointStarted" .-> Disp
    Store -. "ready buffers" .-> CSW[ChannelStateWriter]
    Disp -. "single-pass snapshot" .-> CSW

    style Filter fill:#fff9c4,stroke:#fbc02d,color:#212121
    style Disp fill:#e8f5e9,stroke:#43a047,color:#212121
    style Spill fill:#fce4ec,stroke:#d81b60,color:#212121
    style Store fill:#bbdefb,stroke:#1976d2,color:#0d47a1
    style IC fill:#c8e6c9,stroke:#388e3c,color:#1b5e20
    style Task fill:#eeeeee,stroke:#757575,color:#212121
    style CSW fill:#fff3e0,stroke:#f57c00,color:#212121

    linkStyle 0,1,2,3,4,5 stroke:#1976d2,stroke-width:2px
    linkStyle 6,7,8,9 stroke:#c62828,stroke-width:2px
```

- Blue lines = data consumption (replay path)
- Red lines = checkpoint (trigger propagation + snapshot write)

## Why V2 is hard — three new constraints (none applied in V1)

```mermaid
flowchart LR
    Filter[filterAndRewrite] --> Disp[FilteredBufferDispatcherImpl]
    Disp --> Spill[(FilteredSpillFile)]
    Spill -. drain .-> Disp
    Disp --> Store[RecoveredBufferStore]
    Store --> IC[InputChannel]
    IC --> Task[Task thread]

    C1["① Sequential I/O<br/>shared spill file<br/>must be read one pass"]:::challenge
    C2["② Checkpoint during recovery<br/>register-mode wait-set,<br/>last channel triggers snapshot"]:::challenge
    C3["③ High-priority events<br/>must stay responsive<br/>through the store"]:::challenge

    C1 -.-> Spill
    C2 -.-> Disp
    C3 -.-> IC

    classDef challenge fill:#ffccbc,stroke:#d84315,color:#bf360c,stroke-width:2px
    style Filter fill:#fff9c4,stroke:#fbc02d,color:#212121
    style Disp fill:#e8f5e9,stroke:#43a047,color:#212121
    style Spill fill:#fce4ec,stroke:#d81b60,color:#212121
    style Store fill:#bbdefb,stroke:#1976d2,color:#0d47a1
    style IC fill:#c8e6c9,stroke:#388e3c,color:#1b5e20
    style Task fill:#eeeeee,stroke:#757575,color:#212121
```

Each red box is a requirement V1 didn't have:

- ① → shared cross-channel spill file + register-mode checkpoint trigger (last channel fires the snapshot)
- ② → wait-set + single-pass snapshot coexisting with async drain
- ③ → dispatcher keeps priority events responsive while routing every buffer through the store

Behind that simplicity:

- Interfaces of `FilteredBufferDispatcherImpl` / `FilteredSpillFile` reshaped 10+ times as invariants landed
- Credit-gating `releaseHeldCredit` → deleted; upstream-no-replay invariant makes it unnecessary
- Source-buffer `Semaphore(5)` → 1 reused buffer; serial `readChunk` structurally guarantees at-most-one
- `onChannelCheckpointStarted` set-remove → 3-case state machine (late / current / new checkpointId)
