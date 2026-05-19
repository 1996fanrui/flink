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

## 3. Public class and interfaces

### 3.1 The `RecoveryCheckpointTrigger` interface (task-thread-facing)

The single method the task thread calls on the unspilling side is extracted into a Java interface, so the task thread depends on the contract instead of the concrete `Unspiller` class.

```java
public interface RecoveryCheckpointTrigger {

    /** Atomic — implementations MUST take Unspiller.lock internally and then:
     *    (1) snapshot every SpillFileSegment + capture (currentSegmentIndex,
     *        currentOffset) as DiskSnapshot.startPos;
     *    (2) call onRecoveredStateBuffer(new RecoveryCheckpointBarrier()) on
     *        every channel in the task's channel set.
     *
     *  Returns the DiskSnapshot for the caller to feed into
     *  ChannelStateWriter.addInputDataFromSpill at Step 3.
     *
     *  Caller (task thread) MUST NOT hold Unspiller.lock — the method takes
     *  the lock itself. */
    DiskSnapshot snapshotAndInsertBarriers();
}
```

### 3.2 The `Unspiller` class

```java
public final class Unspiller implements RecoveryCheckpointTrigger, Closeable {
    private final SpillFile spillFile;
    private final List<RecoverableInputChannel> allChannels;                       // full channel set of this task
    private final Map<InputChannelInfo, RecoverableInputChannel> channelByInfo;    // derived from allChannels
    /** The global lock. A dedicated `Object` field (NOT `synchronized(this)` or any
     *  other implicit Java monitor) so it is named, GuardedBy-annotated, and easy
     *  to grep. Locking is via plain `synchronized(lock)` blocks. The lock guards:
     *    (a) each channel's recoveredBuffers writes during recovery, performed via
     *        RecoverableInputChannel.onRecoveredStateBuffer / finishReadRecoveredState;
     *    (b) the drain progress fields currentSegmentIndex / currentOffset below;
     *    (c) the per-checkpoint Step 1 barrier-insertion sequence run by the task thread. */
    private final Object lock = new Object();

    // drain progress, guarded by `lock`
    @GuardedBy("lock") private int  currentSegmentIndex;
    @GuardedBy("lock") private long currentOffset;

    /** allChannels is the full set of RecoverableInputChannels for this task
     *  (stable during recovery). drain routes via channelByInfo by channelInfo;
     *  checkpoint Step 1 iterates allChannels directly to insert a barrier into
     *  each channel — the caller does not need to pass them in again. */
    public Unspiller(SpillFile spillFile, List<RecoverableInputChannel> allChannels);

    /** Sequentially drains every spill segment to its target channel.
     *  Called by channelIOExecutor after conversion completes.
     *
     *  One iteration per spill entry. Buffer allocation and disk read both run
     *  outside the lock so the critical section stays microsecond-scale; the
     *  lock is taken only for the two strictly in-memory actions: deliver
     *  buf to the channel, and advance drain offset. */
    public void drain() throws IOException, InterruptedException {
        for (SpillFileSegment seg : spillFile.segments()) {
            while ((Entry e = seg.peekNextEntry()) != null) {
                RecoverableInputChannel ch = channelByInfo.get(e.channelInfo);

                // (A) Outside the lock: park inside BufferManager.requestBufferBlocking
                //     on the channel's bufferQueue monitor until a buffer is recycled
                //     back to the pool (Object.wait/notifyAll via BufferListener).
                Buffer buf = ch.requestBufferBlocking();

                // (B) Still outside the lock: synchronous disk read into buf.
                //     This is the slowest step per iteration; keeping it outside
                //     the lock means task-thread Step 1 never waits on disk I/O.
                seg.readBytesAt(e.offset, e.length, buf.getMemorySegment().asByteArray());

                // (C) Critical section — exactly two actions, both pure in-memory:
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
        // (D) Drain done. Flip recoveredStateFinished on every channel inside the lock.
        synchronized (lock) {
            for (RecoverableInputChannel ch : allChannels) ch.finishReadRecoveredState();
        }
    }

    /** RecoveryCheckpointTrigger — see §3.1. */
    @Override
    public DiskSnapshot snapshotAndInsertBarriers();
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

Steps (A)/(B)/(C)/(D) refer to the labelled lines inside `drain()` (§3.2).

- **(A)** Buffer-allocation parking is inside `BufferManager.requestBufferBlocking`, on the channel's own `bufferQueue` (Java `Object.wait/notifyAll`, woken by `BufferPool`'s `BufferListener` callback). This park MUST happen outside `Unspiller.lock`; otherwise buffer-pool jitter would in turn block checkpoint Step 1.
- **(B)** Disk read (`seg.readBytesAt`) runs outside `Unspiller.lock`. The buf is local to this iteration and not yet visible to any other thread, so reading it concurrently with the task thread's snapshot is safe.
- **(C)** The two-statement critical section is **the only place** `currentSegmentIndex` / `currentOffset` are mutated and **the only place** drain calls `ch.onRecoveredStateBuffer(...)`. The two actions must stay coupled — this is the second strong principle in [`coordination.md`](./coordination.md); separating them would create a window where the snapshot sees a half-applied entry (either "already in channel but offset not advanced" or "offset advanced but not yet in channel").
- **(D)** After the full segment set is iterated, drain takes the lock one more time and calls `finishReadRecoveredState()` on every channel. The channel itself completes `stateConsumedFuture` once both its `recoveredStateFinished` flag is true and its `recoveredBuffers` field is empty (see [`input_channel.md`](./input_channel.md) §3.7). No EOICS sentinel buffer is inserted into the queue.

## 5. Reuse / change boundary against master

Reused:

- `channelIOExecutor` itself (the existing master single-thread executor).
- `ChannelStateFilteringHandler.filterAndRewrite` (the existing master filter implementation).
- The overall shape of `RecoveredChannelStateHandler.recover` (the destination of the filter branch changes from "channel.onRecoveredStateBuffer" to "filteredBufferWriter.write").
- The `BufferManager.requestBufferBlocking` parking mechanism (`Object.wait` on the channel's own `bufferQueue`, woken by `BufferPool`'s `BufferListener` callback). Drain reaches this via `RecoverableInputChannel.requestBufferBlocking`.
- The two future hand-off points `RecoveredInputChannel.bufferFilteringCompleteFuture` / `stateConsumedFuture`.

Changed:

- Remove the heap fallback in `RecoveredInputChannel.requestBufferBlocking` (the `MemorySegmentFactory.allocateUnpooledSegment` block at lines 354-360) — this is exactly the OOM path the whole project is set up to solve.
- Add four new classes: `FilteredBufferWriter` / `SpillFile` / `Unspiller` / `DiskSnapshot`.
- Switch the filter-phase `bufferSupplier` from `channel::requestBufferBlocking` to the reusable `prefilterBuffer` source.
