# Spill pipeline: `SpillFileWriter` / `SpillFileReader` (async-thread side)

> **Follow-up (2026-05-24):** the runnable on `channelIOExecutor` is now a **single submit**
> (filter → wait `drainHandoff` future → drain → close), not "filter then submit a second
> drain task" — see [`overview.md §2`](./overview.md) updated diagram and
> [`../fix_rounds/recovery_in_recovery_flag_unification.md`](../fix_rounds/recovery_in_recovery_flag_unification.md).
> Additionally, before each `ch.onRecoveredStateBuffer(buf)` / `ch.finishRecoveredBufferDelivery()`
> the channel internally awaits its per-channel `upstreamReady` future (handled by the
> channel itself; the drain loop sees no API change), so any buffer / sentinel reaching
> `recoveredQueue` is delivered only after `requestSubpartitions` has truly published the
> upstream handle.

> Scope: all new behavior that runs on the existing master `channelIOExecutor` when filter is on. When the feature is off, this thread follows the master path unchanged.

## 1. Responsibilities

Covers the two periods of recovery, serially, all on the single-thread `channelIOExecutor`:

- **filter phase**: read from state handle → filter → write to disk (replaces master's heap fallback).
- **drain phase**: sequentially read from disk → request a buffer from the physical channel → deliver into the channel.

## 2. Internal components

| Component | Role |
|---|---|
| `FilteredBufferWriter` | Accumulation before writing to disk during filter. One `prefilterBuffer` per task (used by filter to read source data) + one `postfilterBuffer` (accumulates filter output); when the latter is full, flush it to `SpillFile`. **Replaces the heap-fallback branch inside master's `RecoveredInputChannel.requestBufferBlocking`**, eliminating heap growth at the source. **Single-channel-per-entry invariant**: each entry carries one channel's bytes; flush triggers are (i) channel switch in `beginChannel` and (ii) buffer full — no third trigger. |
| `SpillFile` | The actual on-disk object. Multi-segment: rotates once a single file exceeds 64 MB; each entry carries `(channelInfo, offset, length)` metadata in an in-memory `entries` queue. `snapshot()` can clone the entries (independent of the frozen state), used for checkpoint snapshotting. |
| `SpillFileWriter` | Phase 1 (filter) façade. Owns `SpillFile` during filter; appends `(channelInfo, length, payload)` records; closes after filter completes and hands `SpillFile` to a `SpillFileReader`. Task thread does not hold a reference. |
| `SpillFileReader` main body | Phase 2 (drain) façade. drain loop + the global `Object lock` (used via `synchronized(lock)`) + advancing `(currentSegmentIndex, currentOffset)`. Implements `RecoveryCheckpointTrigger`; the task thread holds the reference. |

## 2a. SpillFile format and lifecycle

**On-disk format.** Filter writes are single-threaded append (`channelIOExecutor` is the only writer); each record is `(channelInfo, length, payload)`. A file rotates into a new segment once it exceeds the configured size (64 MB default); within a segment, writes are sequential.

**Read patterns.** Drain reads the segment list in order. Each in-recovery checkpoint's Step 3 reader (one per cpId) opens an independent reader over the same SpillFile starting at `snap.startPos`. Every reader is **per-stream sequential** (no seek-backward); multiple concurrent readers (drain + N cpId readers) share the file via independent file handles, so file-level IO is interleaved but not random.

**DiskSnapshot fields**: `segmentList` (immutable snapshot of segments at the Step 1 lock-held moment) + `startPos: (segmentIndex, offset)`. End position is implicit — `segmentList`'s last segment's current end, well-defined because filter completes before drain starts and never extends the file again.

**File lifecycle (ref counter).**

| Holder | Acquired at | Released at |
|---|---|---|
| producer (filter pipeline) | when the filter pipeline constructs the `SpillFile` (single producer side) | when the drain reader has taken its own grant (handoff transfer); see "producer hand-off" note below |
| drain | `SpillFileReader` construction | drain loop exits (after §4 step (D)) |
| each in-recovery cpId Step 3 reader | inside `snapshotAndInsertBarriers`, holding the lock, when the DiskSnapshot is built | callback chained onto `ChannelStateWriter.getAndRemoveWriteResult(cpId).getInputChannelStateHandles()` |

**Producer hand-off (mandatory).** Producer holds the initial grant until the drain reader (constructed later on the task thread) has taken its own grant, then releases. The writer cannot itself hold the grant — writer release would run on the I/O thread *before* reader construction, drop refCount to zero, and let segments be deleted in that gap. Every failure path (filter / conversion / reader build throws) must still release the producer grant or segments leak.

**`SpillFile.close()` is forced cleanup, not a release.** Reserved for shutdown / abort / tearDown — production filter→drain goes through `release()`.

Counter reaches zero → SpillFile deletes all segments. Task abort drives the same path: `ChannelStateWriter.abort(cpId, cause, cleanup)` exceptionally completes the per-cpId future, the callback still fires, the ref is released.

## 2b. The `SpillFileWriter` class (phase 1)

```java
public final class SpillFileWriter implements Closeable {
    private final SpillFile spillFile;
    private final FilteredBufferWriter accumulator;   // one pre + one post buffer

    public SpillFileWriter(SpillFile spillFile, FilteredBufferWriter accumulator);

    /** Append one record. Called by filter on channelIOExecutor (single writer). */
    public void write(InputChannelInfo channelInfo, Buffer data) throws IOException;

    /** Flushes the accumulator's remaining post-filter buffer to `SpillFile` and closes
     *  for write. After this returns, the `SpillFile` is final and is handed to a
     *  `SpillFileReader` for phase 2. */
    @Override public void close() throws IOException;
}
```

Single-phase: no cross-thread access (the task thread does not see this class), no lock, no checkpoint interface.

## 3. The `SpillFileReader` class (phase 2)

All three cross-thread Java interfaces (`RecoveryCheckpointTrigger`, `RecoverableInputChannel`, `BufferRequester`) plus the `RecoveredChannelBufferRequester` implementation are declared in [`overview.md`](./overview.md#6-cross-thread-java-interfaces) §6 and are not redeclared here. `SpillFileReader implements RecoveryCheckpointTrigger, Closeable`; the task thread holds the reference typed as the interface.

```java
public final class SpillFileReader implements RecoveryCheckpointTrigger, Closeable {
    private final SpillFile spillFile;
    private final List<RecoverableInputChannel> allChannels;                       // full channel set of this task
    private final Map<InputChannelInfo, RecoverableInputChannel> channelByInfo;    // derived from allChannels
    private final BufferRequester bufferRequester;                                  // buffer allocation + release
    /** Global lock. Dedicated `Object` field (not `synchronized(this)`) — named,
     *  GuardedBy-annotated, grep-able. Guards: (a) channel `recoveredBuffers` writes
     *  via onRecoveredStateBuffer; (b) drain progress fields below; (c) Step 1
     *  barrier insertion. End-of-drain `finishReadRecoveredState` is NOT guarded
     *  here — see §4 step (D). */
    private final Object lock = new Object();

    // drain progress, guarded by `lock`
    @GuardedBy("lock") private int  currentSegmentIndex;
    @GuardedBy("lock") private long currentOffset;

    public SpillFileReader(SpillFile spillFile,
                     List<RecoverableInputChannel> allChannels,
                     BufferRequester bufferRequester);

    /** Sequentially drains every spill segment. Called by channelIOExecutor after
     *  conversion completes. Buffer allocation + disk read run outside the lock;
     *  the lock is taken only for the two in-memory actions per entry (deliver +
     *  advance offset). */
    public void drain() throws IOException, InterruptedException {
        for (SpillFileSegment seg : spillFile.segments()) {
            while ((Entry e = seg.peekNextEntry()) != null) {
                RecoverableInputChannel ch = channelByInfo.get(e.channelInfo);

                // (A) Outside the lock: allocate a buffer via BufferRequester.
                //     Internally delegates to RecoveredInputChannel.requestBufferBlocking,
                //     which parks on BufferManager.bufferQueue (Object.wait/notifyAll).
                Buffer buf = bufferRequester.requestBufferBlocking(e.channelInfo);

                // (B) Still outside the lock: synchronous disk read into buf.
                seg.readBytesAt(e.offset, e.length, buf.getMemorySegment().asByteArray());

                // (C) Critical section — two in-memory actions, strongly coupled:
                //       1. deliver buf into the channel's recoveredBuffers
                //       2. advance drain progress
                synchronized (lock) {
                    ch.onRecoveredStateBuffer(buf);
                    seg.pollNextEntry();
                    currentSegmentIndex = seg.segmentIndex;
                    currentOffset = e.offset + e.length;
                }
            }
            seg.close();
        }
        // (D) Drain done. No more buffers will be added, so the (queue, offset) atomicity
        //     that Principle 1 protects does not apply here — runs outside the lock. The
        //     flag is published via the channel's internal monitor that
        //     finishReadRecoveredState already takes.
        for (RecoverableInputChannel ch : allChannels) ch.finishReadRecoveredState();
    }

    @Override public DiskSnapshot snapshotAndInsertBarriers();   // see overview §6.1

    /** Releases the BufferRequester (which in turn releases every source channel's
     *  exclusive buffers via RecoveredInputChannel.releaseAllResources), then closes
     *  the spill file. */
    @Override public void close() throws IOException {
        bufferRequester.releaseExclusiveBuffers();
        spillFile.close();
    }
}
```

The task thread holds the reference typed as `RecoveryCheckpointTrigger`, not as `SpillFileReader`. `drain()` and the constructor are within-thread / setup-time, not part of the cross-thread interface.

`DiskSnapshot` exposed to `ChannelStateWriter`:

```java
public final class DiskSnapshot implements CloseableIterator<DiskSnapshot.Chunk> {
    // Internals: List<SpillFileSegment snapshot copies> + (currentSegmentIndex, currentOffset)
    // Iteration skips entries with entryPos < startPos (those already entered a channel)
    public static final class Chunk { InputChannelInfo channelInfo; byte[] data; int length; }
}
```

## 3a. `InputChannelInfo` direction: write NEW, look up NEW

Filter writes `SpillFile` entries keyed by **NEW** (post-rescale, `mappedChannel.getChannelInfo()`) channelInfo — drain looks up physical channels by NEW too. Filter-internal addressing (`filterAndRewrite(gateIdx, oldSubtaskIndex, oldChannelIndex, ...)`) continues to use OLD, because the virtual channel registry is keyed by `(oldSubtask, oldChannel)`. Non-rescale runs have OLD == NEW so the distinction is invisible; rescale paths are where the wrong direction silently mis-routes drain output to the wrong physical channel.

## 4. Internal invariants

Steps (A)/(B)/(C)/(D) refer to the labelled lines inside `drain()` (§3).

- **(A)** Buffer allocation goes through `bufferRequester.requestBufferBlocking(channelInfo)`. The implementation delegates to `RecoveredInputChannel.requestBufferBlocking()`, which parks on `BufferManager.bufferQueue` (Java `Object.wait/notifyAll`, woken by `BufferPool`'s `BufferListener`). This park MUST happen outside `SpillFileReader.lock`; otherwise buffer-pool jitter would in turn block checkpoint Step 1.
- **(B)** Disk read (`seg.readBytesAt`) runs outside `SpillFileReader.lock`. The buf is local to this iteration and not yet visible to any other thread, so reading it concurrently with the task thread's snapshot is safe.
- **(C)** The two-statement critical section is **the only place** `currentSegmentIndex` / `currentOffset` are mutated and **the only place** drain calls `ch.onRecoveredStateBuffer(...)`. The two actions must stay coupled — this is the second strong principle in [`coordination.md`](./coordination.md); separating them would create a window where the snapshot sees a half-applied entry (either "already in channel but offset not advanced" or "offset advanced but not yet in channel").
- **(D)** After the full segment set is iterated, drain calls `finishReadRecoveredState()` on every channel **outside `SpillFileReader.lock`**. At this point no more buffers will be added, so the (queue, offset) atomicity that Principle 1 protects does not apply; the flag is published through the channel's internal monitor that `finishReadRecoveredState` already takes. The channel itself completes `stateConsumedFuture` once both its `allRecoveredBuffersDelivered` flag is true and its `recoveredBuffers` field is empty (see [`input_channel.md`](./input_channel.md) §3.7). No EOICS sentinel buffer is inserted into the queue.

## 5. Reuse / change boundary against master

Reused:

- `channelIOExecutor` itself (master's existing single-thread executor).
- `ChannelStateFilteringHandler.filterAndRewrite` (master's existing filter implementation).
- `RecoveredChannelStateHandler.recover` overall shape (filter branch destination changes from `channel.onRecoveredStateBuffer` to `filteredBufferWriter.write`).
- `RecoveredInputChannel.requestBufferBlocking()` (master existing) — drain reaches it via `BufferRequester.requestBufferBlocking(channelInfo)` → `RecoveredChannelBufferRequester` → the per-channel `RecoveredInputChannel` instance. Internally parks on `BufferManager.bufferQueue`.
- `RecoveredInputChannel.releaseAllResources()` (master existing) — drain reaches it via `BufferRequester.releaseExclusiveBuffers()` at end of drain.
- The two future hand-off points `bufferFilteringCompleteFuture` / `stateConsumedFuture`.

Changed:

- Remove the heap fallback in `RecoveredInputChannel.requestBufferBlocking` (the `MemorySegmentFactory.allocateUnpooledSegment` block at lines 354-360) — the OOM path FLINK-38544 exists to fix.
- Promote `RecoveredInputChannel.releaseAllResources()` from package-private to `public` (no behavior change, just visibility) so `RecoveredChannelBufferRequester` in `o.a.f.runtime.checkpoint.channel` can call it.
- Add new classes: `FilteredBufferWriter`, `SpillFile`, `SpillFileWriter`, `SpillFileReader`, `DiskSnapshot`, `RecoveredChannelBufferRequester`.
- Add three new interfaces (declared in [`overview.md`](./overview.md#6-cross-thread-java-interfaces) §6): `RecoveryCheckpointTrigger`, `RecoverableInputChannel`, `BufferRequester`.
- Switch the filter-phase `bufferSupplier` from `channel::requestBufferBlocking` to the reusable `prefilterBuffer` source.
