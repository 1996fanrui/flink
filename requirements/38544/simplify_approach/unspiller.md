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
| `SpillFile` | The actual on-disk object. Multi-segment: rotates once a single file exceeds 64 MB. Structurally a `List<SpillFileSegment>`; each segment owns its own `List<Entry>` (`(channelInfo, offsetInSegment, length)`). No flat top-level entry list. Construction takes `maxEntryLength`. |
| `SpillFileWriter` | Phase 1 (filter) façade. Owns `SpillFile` during filter; appends `(channelInfo, length, payload)` records; closes after filter completes and hands `SpillFile` to phase 2. Task thread does not hold a reference. |
| `SpillFileReader` | Phase 2 forward iterator. Lock-free, single reusable byte buffer, single open segment `FileChannel`; `peek + advance + snapshot` API. Each segment file is opened at most once per reader. See §3.2. |
| `SpillFileDrainer` | Phase 2 façade held by the task thread. Implements `RecoveryCheckpointTrigger`; exposes `drain()` to `channelIOExecutor`. Holds the root `SpillFileReader`, the channel set, and the lock that serialises drain cursor advance with checkpoint snapshot derivation. See §3.3. |

## 2a. SpillFile format and lifecycle

**On-disk format.** Filter writes are single-threaded append (`channelIOExecutor` is the only writer); each record is `(channelInfo, length, payload)`. A file rotates into a new segment once it exceeds the configured size (64 MB default); within a segment, writes are sequential.

**Read patterns.** The drainer holds a single root `SpillFileReader` that walks the segment list in order. Each in-recovery checkpoint derives an independent sub-`SpillFileReader` via `rootReader.snapshot()`, starting at the root's current cursor. Every reader is **per-stream sequential** (forward-only, never seeks back, opens each segment file at most once); multiple concurrent readers (root + N sub-readers) share the file via independent `FileChannel` handles, so file-level IO is interleaved but not random.

**File lifecycle (ref counter).** Every `SpillFileReader` instance holds exactly one ref-count grant, acquired at construction and released at `close()`.

| Holder | Acquired at | Released at |
|---|---|---|
| producer (filter pipeline) | when the filter pipeline constructs the `SpillFile` (single producer side) | when the root reader has taken its own grant (handoff transfer); see "producer hand-off" note below |
| root reader (drain) | `spillFile.reader()` in `SpillFileDrainer` ctor | drainer's `close()` |
| per-checkpoint sub-reader | `rootReader.snapshot()` inside `snapshotAndInsertBarriers`, holding the drainer's lock | callback chained onto `ChannelStateWriter.getAndRemoveWriteResult(cpId).getInputChannelStateHandles()` |

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

## 3. The reader stack (phase 2)

Phase 2 consists of two classes:

- **`SpillFileReader`** — pure forward iterator over a `SpillFile`'s entries. Lock-free,
  checkpoint-unaware. API: `peek + advance + snapshot + close`. Owns a single reusable
  byte buffer and a single open segment `FileChannel` at any instant.
- **`SpillFileDrainer`** — the only class the rest of the system holds. Implements
  `RecoveryCheckpointTrigger`, exposes `drain()` to the `channelIOExecutor`. Holds the
  root `SpillFileReader`, the `allChannels` list, and the lock that serialises drain
  cursor advance with checkpoint-trigger snapshot derivation.

### 3.1 Lock principle

Snapshot is the cause, lock is the effect. The lock exists because the root reader is
shared between the drain thread and the task-thread checkpoint trigger: the trigger
needs to derive a sub-reader from a stable cursor, so root cursor advance must be
serialised with `rootReader.snapshot()`. Sub-readers are single-consumer (the
checkpoint write path) and need no lock.

The lock is owned by `SpillFileDrainer`. `SpillFileReader` is intrinsically lock-free;
the contract is that callers sharing a reader across threads (only the drainer does)
take the drainer's lock at the right boundaries.

### 3.2 `SpillFileReader`

#### Internal data model

`SpillFile` is structurally a `List<SpillFileSegment>`; each segment owns
`List<Entry>` recording `(channelInfo, offsetInSegment, length)`. There is no flat
top-level entry list; an entry belongs to exactly one segment by construction. (See
§2a for the on-disk format.)

#### Cursor state

```
segmentCursor   : index into spillFile.segments()              (range: [0, segments.size()])
entryCursor     : index into segments[segmentCursor].entries() (range: [0, segment.entries.size()])
activeChannel   : FileChannel for segments[segmentCursor], lazily opened on first read
reusable        : byte[maxEntryLength], allocated at construction
cachedChunk     : @Nullable Chunk wrapping reusable; non-null iff peek() succeeded and
                  advance() has not been called since
refGrant        : 1 ref-count grant on spillFile, acquired in ctor, released in close()
```

Invariants:

- `activeChannel != null` ⇒ it is the `FileChannel` of `segments[segmentCursor]` and is
  positioned at the byte after the most-recently-read entry (or `0` if no entry has
  been read in this segment yet).
- When `advance()` crosses a segment boundary (`entryCursor` reaches end of current
  segment), `activeChannel` is closed and nulled before `segmentCursor` increments.
  The next `peek()` opens the new segment's channel.
- A segment file is opened **at most once** per reader instance — the cursor only moves
  forward, never re-enters a closed segment.
- `peek()` is idempotent: if `cachedChunk != null` it is returned immediately; otherwise
  the bytes at the current entry are read into `reusable`, `cachedChunk` is set, and
  returned. Both `peek` calls and the disk read happen **outside any caller lock**.
- `advance()` invalidates `cachedChunk` and bumps `entryCursor`; if it falls off the
  end of the current segment, it closes `activeChannel` and bumps `segmentCursor`.
  Pure in-memory updates; safe inside a caller lock.

#### API

```java
public final class SpillFileReader implements Closeable {

    public static final class Chunk {
        public final InputChannelInfo channelInfo;
        public final byte[] data;     // aliases reusable; valid only until advance()
        public final int length;
    }

    /** Returns the next entry as a {@code Chunk}, or {@code null} if exhausted.
     *  Reads disk into the reusable buffer on first call; idempotent until
     *  {@link #advance()}. Caller must NOT hold any cross-thread lock. */
    @Nullable
    public Chunk peek() throws IOException;

    /** Advances past the previously peeked entry, invalidating the cache. Pure
     *  in-memory update. Must be called by the drain loop inside the drainer's lock,
     *  paired with {@code ch.onRecoveredStateBuffer(buf)}. Sub-readers may call it
     *  without a lock (single consumer). */
    public void advance();

    /** Derives an independent sub-reader covering entries from the current cursor to
     *  end-of-file. The sub-reader gets its own reusable buffer, its own ref-count
     *  grant, and starts with an empty cache and no open channel. Must be called
     *  inside the drainer's lock when invoked on the root reader, so the cursor is
     *  not in a half-advanced state. */
    public SpillFileReader snapshot();

    /** Releases this reader's ref-count grant on the SpillFile and closes the active
     *  segment channel (if any). Idempotent. */
    @Override
    public void close() throws IOException;

    /** Single-consumer convenience wrapper: {@code hasNext() == peek() != null},
     *  {@code next() == peek() + advance()}. Used by sub-readers handed to
     *  {@link ChannelStateWriter#addInputDataFromSpill}. */
    public CloseableIterator<Chunk> asIterator();
}
```

### 3.3 `SpillFileDrainer`

```java
public final class SpillFileDrainer implements RecoveryCheckpointTrigger, Closeable {
    private final SpillFile spillFile;
    private final SpillFileReader rootReader;
    private final List<RecoverableInputChannel> allChannels;
    private final Map<InputChannelInfo, RecoverableInputChannel> channelByInfo;

    /** Guards: (a) channel {@code recoveredBuffers} writes via
     *  {@code onRecoveredStateBuffer}; (b) root reader cursor advance; (c) root
     *  reader snapshot derivation; (d) per-channel barrier insertion in Step 1.
     *  End-of-drain {@code finishRecoveredBufferDelivery} is NOT guarded — see (D). */
    private final Object lock = new Object();

    public SpillFileDrainer(SpillFile spillFile, List<RecoverableInputChannel> allChannels);

    /** Sequentially drains every entry. Called by {@code channelIOExecutor} after
     *  channel conversion completes. */
    public void drain() throws IOException, InterruptedException {
        Chunk c;
        while ((c = rootReader.peek()) != null) {              // (B) outside lock — disk read
            RecoverableInputChannel ch = channelByInfo.get(c.channelInfo);
            Buffer buf = ch.requestRecoveryBufferBlocking();   // (A) outside lock — may park
            copy c.data[0..c.length) into buf;                 // outside lock — local memcpy
            synchronized (lock) {                              // (C) two coupled in-memory actions
                ch.onRecoveredStateBuffer(buf);
                rootReader.advance();
            }
        }
        // (D) Drain done. No more buffers will be added; outside lock.
        for (RecoverableInputChannel ch : allChannels) ch.finishRecoveredBufferDelivery();
    }

    @Override
    public CloseableIterator<Chunk> snapshotAndInsertBarriers(long cpId) {
        synchronized (lock) {
            SpillFileReader sub = rootReader.snapshot();
            for (RecoverableInputChannel ch : allChannels) {
                if (ch.isInRecovery()) {
                    ch.onRecoveredStateBuffer(
                            toBuffer(new RecoveryCheckpointBarrier(cpId)));
                }
            }
            if (sub.peek() == null) {
                sub.close();
                return CloseableIterator.empty();
            }
            return sub.asIterator();
        }
    }

    /** Closes the root reader (releasing its ref-count grant) and any per-channel
     *  resources owned at the drainer level. */
    @Override
    public void close() throws IOException;
}
```

### 3.4 Lifecycle

- `SpillFile` constructed with `maxEntryLength` (= recover network buffer size, fixed at
  task startup) — every reader / sub-reader allocates its own `byte[maxEntryLength]`.
- `spillFile.reader()` is called once by the drainer's ctor; that one root reader is
  the only reader the drain thread ever holds.
- `SpillFileReader` ctor calls `spillFile.acquire()`; `close()` calls `spillFile.release()`.
  `snapshot()` constructs a sub-reader that takes its own grant. Ref-count
  bookkeeping never leaves the reader; external callers do not touch
  `acquire/release`.

### 3.5 Downstream surface

- `ChannelStateWriter.addInputDataFromSpill(long cpId, CloseableIterator<SpillFileReader.Chunk> chunks)`
  consumes the sub-reader via `sub.asIterator()` — single thread, no lock.
- `RecoveryCheckpointTrigger.snapshotAndInsertBarriers(long cpId)` returns
  `CloseableIterator<SpillFileReader.Chunk>`. No `DiskSnapshot` class exists.

## 3a. `InputChannelInfo` direction: write NEW, look up NEW

Filter writes `SpillFile` entries keyed by **NEW** (post-rescale, `mappedChannel.getChannelInfo()`) channelInfo — drain looks up physical channels by NEW too. Filter-internal addressing (`filterAndRewrite(gateIdx, oldSubtaskIndex, oldChannelIndex, ...)`) continues to use OLD, because the virtual channel registry is keyed by `(oldSubtask, oldChannel)`. Non-rescale runs have OLD == NEW so the distinction is invisible; rescale paths are where the wrong direction silently mis-routes drain output to the wrong physical channel.

## 4. Internal invariants

Steps (A)/(B)/(C)/(D) refer to the labelled lines inside `SpillFileDrainer.drain()` (§3.3).

- **(A)** Buffer allocation goes through `ch.requestRecoveryBufferBlocking()` on the physical channel, which parks on its `BufferManager.bufferQueue` (Java `Object.wait/notifyAll`, woken by `BufferPool`'s `BufferListener`). This park MUST happen outside the drainer's lock; otherwise buffer-pool jitter would block checkpoint Step 1.
- **(B)** Disk read happens inside `rootReader.peek()` — the reader sequentially reads the entry's bytes into its own reusable buffer outside the drainer's lock. The reusable buffer is reader-private and not yet visible to any other thread, so the read can proceed concurrently with the task thread's `snapshotAndInsertBarriers`.
- **(C)** The two-statement critical section is **the only place** `rootReader`'s entry cursor is advanced (`rootReader.advance()`) and **the only place** drain calls `ch.onRecoveredStateBuffer(...)`. The two actions must stay coupled — this is the second strong principle in [`coordination.md`](./coordination.md); separating them would create a window where a concurrent `rootReader.snapshot()` sees a half-applied entry (either "already in channel but cursor not advanced" or "cursor advanced but not yet in channel").
- **(D)** After the root reader is exhausted, drain calls `finishRecoveredBufferDelivery()` on every channel **outside the drainer's lock**. At this point no more buffers will be added, so the (queue, cursor) atomicity that Principle 1 protects does not apply; the flag is published through the channel's internal monitor that `finishRecoveredBufferDelivery` already takes. The channel itself completes `stateConsumedFuture` once both its `allRecoveredBuffersDelivered` flag is true and its `recoveredBuffers` field is empty (see [`input_channel.md`](./input_channel.md) §3.7). No EOICS sentinel buffer is inserted into the queue.

## 5. Reuse / change boundary against master

Reused:

- `channelIOExecutor` itself (master's existing single-thread executor).
- `ChannelStateFilteringHandler.filterAndRewrite` (master's existing filter implementation).
- `RecoveredChannelStateHandler.recover` overall shape (filter branch destination changes from `channel.onRecoveredStateBuffer` to `filteredBufferWriter.write`).
- The two future hand-off points `bufferFilteringCompleteFuture` / `stateConsumedFuture`.

Changed:

- Remove the heap fallback in `RecoveredInputChannel.requestBufferBlocking` (the `MemorySegmentFactory.allocateUnpooledSegment` block at lines 354-360) — the OOM path FLINK-38544 exists to fix.
- Buffer ownership during drain shifts to the **physical** channel: drain calls `ch.requestRecoveryBufferBlocking()` on the post-conversion `LocalInputChannel` / `RemoteInputChannel`, which allocates from the physical channel's own `BufferManager`. No `BufferRequester` indirection; `RecoveredChannelBufferRequester` does not exist.
- Add new classes: `FilteredBufferWriter`, `SpillFile`, `SpillFileWriter`, `SpillFileReader`, `SpillFileDrainer`.
- Add two new interfaces (declared in [`overview.md`](./overview.md#6-cross-thread-java-interfaces) §6): `RecoveryCheckpointTrigger`, `RecoverableInputChannel`. (`BufferRequester` is not introduced.)
- Switch the filter-phase `bufferSupplier` from `channel::requestBufferBlocking` to the reusable `prefilterBuffer` source.
