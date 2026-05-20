# Unspiller component (async-thread side)

> Scope: all new behavior that runs on the existing master `channelIOExecutor` when filter is on. When the feature is off, this thread follows the master path unchanged.

## 1. Responsibilities

Covers the two periods of recovery, serially, all on the single-thread `channelIOExecutor`:

- **filter phase**: read from state handle → filter → write to disk (replaces master's heap fallback).
- **drain phase**: sequentially read from disk → request a buffer from the physical channel → deliver into the channel.

## 2. Internal components

| Component | Role |
|---|---|
| `FilteredBufferWriter` | Accumulation before writing to disk during filter. One `prefilterBuffer` per task (used by filter to read source data) + one `postfilterBuffer` (accumulates filter output); when the latter is full, flush it to `SpillFile`. **Replaces the heap-fallback branch inside master's `RecoveredInputChannel.requestBufferBlocking`**, eliminating heap growth at the source. |
| `SpillFile` | The actual on-disk object. Multi-segment: rotates once a single file exceeds 64 MB; each entry carries `(channelInfo, offset, length)` metadata in an in-memory `entries` queue. `snapshot()` can clone the entries (independent of the frozen state), used for checkpoint snapshotting. |
| `Unspiller` main body | drain loop + the global `Object lock` (used via `synchronized(lock)`) + advancing `(currentSegmentIndex, currentOffset)`. This is the object that exposes the public interface to the task thread. |

## 3. The `Unspiller` class

All three cross-thread Java interfaces (`RecoveryCheckpointTrigger`, `RecoverableInputChannel`, `BufferRequester`) plus the `RecoveredChannelBufferRequester` implementation are declared in [`overview.md`](./overview.md#6-cross-thread-java-interfaces) §6 and are not redeclared here. `Unspiller implements RecoveryCheckpointTrigger, Closeable`; the task thread holds the reference typed as the interface.

```java
public final class Unspiller implements RecoveryCheckpointTrigger, Closeable {
    private final SpillFile spillFile;
    private final List<RecoverableInputChannel> allChannels;                       // full channel set of this task
    private final Map<InputChannelInfo, RecoverableInputChannel> channelByInfo;    // derived from allChannels
    private final BufferRequester bufferRequester;                                  // buffer allocation + release
    /** Global lock. Dedicated `Object` field (not `synchronized(this)`) — named,
     *  GuardedBy-annotated, grep-able. Guards: (a) channel `recoveredBuffers` writes
     *  via onRecoveredStateBuffer / finishReadRecoveredState; (b) drain progress
     *  fields below; (c) Step 1 barrier insertion. */
    private final Object lock = new Object();

    // drain progress, guarded by `lock`
    @GuardedBy("lock") private int  currentSegmentIndex;
    @GuardedBy("lock") private long currentOffset;

    public Unspiller(SpillFile spillFile,
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
        // (D) Drain done. Flip allRecoveredBuffersDelivered on every channel inside the lock.
        synchronized (lock) {
            for (RecoverableInputChannel ch : allChannels) ch.finishReadRecoveredState();
        }
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

The task thread holds the reference typed as `RecoveryCheckpointTrigger`, not as `Unspiller`. `drain()` and the constructor are within-thread / setup-time, not part of the cross-thread interface.

`DiskSnapshot` exposed to `ChannelStateWriter`:

```java
public final class DiskSnapshot implements CloseableIterator<DiskSnapshot.Chunk> {
    // Internals: List<SpillFileSegment snapshot copies> + (currentSegmentIndex, currentOffset)
    // Iteration skips entries with entryPos < startPos (those already entered a channel)
    public static final class Chunk { InputChannelInfo channelInfo; byte[] data; int length; }
}
```

## 4. Internal invariants

Steps (A)/(B)/(C)/(D) refer to the labelled lines inside `drain()` (§3).

- **(A)** Buffer allocation goes through `bufferRequester.requestBufferBlocking(channelInfo)`. The implementation delegates to `RecoveredInputChannel.requestBufferBlocking()`, which parks on `BufferManager.bufferQueue` (Java `Object.wait/notifyAll`, woken by `BufferPool`'s `BufferListener`). This park MUST happen outside `Unspiller.lock`; otherwise buffer-pool jitter would in turn block checkpoint Step 1.
- **(B)** Disk read (`seg.readBytesAt`) runs outside `Unspiller.lock`. The buf is local to this iteration and not yet visible to any other thread, so reading it concurrently with the task thread's snapshot is safe.
- **(C)** The two-statement critical section is **the only place** `currentSegmentIndex` / `currentOffset` are mutated and **the only place** drain calls `ch.onRecoveredStateBuffer(...)`. The two actions must stay coupled — this is the second strong principle in [`coordination.md`](./coordination.md); separating them would create a window where the snapshot sees a half-applied entry (either "already in channel but offset not advanced" or "offset advanced but not yet in channel").
- **(D)** After the full segment set is iterated, drain takes the lock one more time and calls `finishReadRecoveredState()` on every channel. The channel itself completes `stateConsumedFuture` once both its `allRecoveredBuffersDelivered` flag is true and its `recoveredBuffers` field is empty (see [`input_channel.md`](./input_channel.md) §3.7). No EOICS sentinel buffer is inserted into the queue.

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
- Add new classes: `FilteredBufferWriter`, `SpillFile`, `Unspiller`, `DiskSnapshot`, `RecoveredChannelBufferRequester`.
- Add three new interfaces (declared in [`overview.md`](./overview.md#6-cross-thread-java-interfaces) §6): `RecoveryCheckpointTrigger`, `RecoverableInputChannel`, `BufferRequester`.
- Switch the filter-phase `bufferSupplier` from `channel::requestBufferBlocking` to the reusable `prefilterBuffer` source.
