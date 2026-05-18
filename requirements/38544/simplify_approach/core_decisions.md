# Core Design Decisions

## Core Goal

**A checkpoint must be able to fire as soon as filtering completes, even when the spill file still holds unreplayed data on disk.**

---

## Q1: Does the task thread consume buffers while filtering is still in progress?

**No — task consumption starts only after filtering completes.**
- Filter is fast: deserialize → filter → re-serialize, no user logic; deferring consumption does not delay checkpoint
- Removes one class of producer/consumer concurrency from filter phase

## Q2: How does an InputChannel know about data still sitting on disk?

**Per-channel `RecoveredBufferStore` holding ready buffers + pending `DiskRef` queue.**
- Real channels (Local / Remote) delegate `getNextBuffer` + `checkpointStarted` + `release` to the store
- Recovered channel only owns the store reference during filter and hands it off at conversion — no consumption path → no extra lock contention

## Q3: Is the spill file shared across all channels in a task, or one per channel?

**One shared file per task, rotated at 64 MB.**
- Per-channel layout → thousands of fds
- Cleanup flexibility gain not worth the complexity

## Q4: How many threads load data from disk back into memory?

**A single loader thread, owned by the recovery handler.**
- Sequential read of shared file > parallel random reads
- Spill volume bounded by network buffer pool size → no parallelism payoff

## Q5: Do we keep the `RecoveredInputChannel` abstraction, or eliminate it?

**Keep it, unchanged from community master.**
- Removal requires reworking `SingleInputGate` partition setup + rescale — unrelated to disk-spilling goal
- Hard to maintain when checkpoint-during-recovery is disabled

## Q6: Should we drop the dispatcher lock and its phase-2 coordination state?

**Yes.**
- Disk entries move into per-channel `store.pendingDiskRefs` → each channel snapshots ready + pending atomically under store monitor
- Dispatcher becomes single-threaded on the filter thread, no lock needed
- Downside: random I/O at checkpoint time

## Q7: Can we also drop the store's borrowed gate lock?

**Probably yes — needs a POC.**
- `filter → conversion → drain` ordering makes listener field final before drain starts → FLINK-39519 stale-listener race gone
- POC must enforce drain-after-conversion + pass `UnalignedCheckpointRescaleITCase`
