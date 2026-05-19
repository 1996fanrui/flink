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
| `Unspiller` main body | drain loop + monitor + advancing `(currentSegmentIndex, currentOffset)`. This is the object that exposes the public interface to the task thread. |

## 3. Public class and interface

```java
public final class Unspiller implements Closeable {
    private final SpillFile spillFile;
    private final List<InputChannel> allChannels;                       // full channel set of this task
    private final Map<InputChannelInfo, InputChannel> channelByInfo;    // derived from allChannels
    private final Object monitor = new Object();                        // the global lock

    // drain progress, written inside the monitor
    private int  currentSegmentIndex;
    private long currentOffset;

    /** allChannels is the full set of InputChannels for this task (stable during recovery).
     *  drain routes via channelByInfo by channelInfo; checkpoint Step 1 iterates
     *  allChannels directly to insert a barrier into each channel — the caller does
     *  not need to pass them in again. */
    public Unspiller(SpillFile spillFile, List<InputChannel> allChannels);

    /** Sequentially drains every spill segment to its target channel.
     *  Called by channelIOExecutor after conversion completes. */
    public void drain() throws IOException, InterruptedException;

    /** Step 1 of the checkpoint protocol. See coordination.md. */
    public DiskSnapshot snapshotAndInsertBarriers();
}
```

`DiskSnapshot` exposed to `ChannelStateWriter`:

```java
public final class DiskSnapshot implements CloseableIterator<DiskSnapshot.Chunk> {
    // Internals: List<SpillFileSegment snapshot copies> + (currentSegmentIndex, currentOffset)
    // Iteration skips entries with entryPos < startPos (those already entered a channel)
    public static final class Chunk { InputChannelInfo channelInfo; byte[] data; int length; }
}
```

## 4. drain loop shape

One iteration per entry; the buffer request parks outside the monitor, while I/O + delivery + advancing the offset are completed atomically inside the monitor.

```
drain() {
  for (SpillFileSegment seg : spillFile.segments()) {
    while ((Entry e = seg.peekNextEntry()) != null) {
      InputChannel ch = channelByInfo.get(e.channelInfo);

      // (A) parks on LocalBufferPool.getAvailableFuture — outside the monitor
      Buffer buf = ch.requestBufferBlocking();

      // (B) short critical section: I/O + delivery + offset advance; the three are bound together in the same lock
      synchronized (monitor) {
        seg.readBytesAt(e.offset, e.length, buf.getMemorySegment().asByteArray());
        ch.<add-buffer entry>(buf);            // determined by the final choice in input_channel.md
        seg.pollNextEntry();
        currentSegmentIndex = seg.segmentIndex;
        currentOffset = e.offset + e.length;
      }
    }
    seg.close();
  }
}
```

## 5. Internal invariants

- (A) Buffer-allocation parking must happen outside the monitor; otherwise buffer-pool blocking would in turn block checkpoint Step 1.
- (B) **No parking-class operation** is performed inside the critical section (disk read is synchronous I/O, which is acceptable).
- (B) **The three actions are tightly coupled in the same critical section**: (disk read → deliver into channel → advance offset). This is the concrete manifestation of the second strong principle in [`coordination.md`](./coordination.md); none can be omitted, otherwise the checkpoint snapshot would have an inconsistency window of "the on-disk entry has been drained but the offset has not advanced" or vice versa.
- After the full segment set is iterated, drain returns and `channelIOExecutor` immediately delivers an `EndOfInputChannelStateEvent` into each physical channel (also inside the monitor), completing `stateConsumedFuture`.

## 6. Reuse / change boundary against master

Reused:

- `channelIOExecutor` itself (the existing master single-thread executor).
- `ChannelStateFilteringHandler.filterAndRewrite` (the existing master filter implementation).
- The overall shape of `RecoveredChannelStateHandler.recover` (the destination of the filter branch changes from "channel.onRecoveredStateBuffer" to "filteredBufferWriter.write").
- The `getAvailableFuture` parking mechanism of `LocalBufferPool.requestMemorySegmentBlocking`.
- The two future hand-off points `RecoveredInputChannel.bufferFilteringCompleteFuture` / `stateConsumedFuture`.

Changed:

- Remove the heap fallback in `RecoveredInputChannel.requestBufferBlocking` (the `MemorySegmentFactory.allocateUnpooledSegment` block at lines 354-360) — this is exactly the OOM path the whole project is set up to solve.
- Add four new classes: `FilteredBufferWriter` / `SpillFile` / `Unspiller` / `DiskSnapshot`.
- Switch the filter-phase `bufferSupplier` from `channel::requestBufferBlocking` to the reusable `prefilterBuffer` source.
