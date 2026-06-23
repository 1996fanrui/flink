# Consistent snapshot: how both sides stay consistent

> Scope: when a checkpoint fires *during recovery*, the in-flight recovery data lives in two
> places — partly already drained into channel in-memory queues, partly still on disk. This doc
> explains how the drain side and the snapshot side together produce a single cut of that data
> that is **complete (nothing lost)** and **duplicate-free (nothing counted twice)**.
>
> This is the code-level companion to [`coordination.md`](./coordination.md) §3–§5. Real class
> names (the design docs use the older `SpillFile*` names):
>
> | Design-doc name | Real class |
> |---|---|
> | `SpillFileDrainer` | `FetchedChannelStateDrainer` |
> | `SpillFileReader` | `FetchedChannelStateReader` / `FetchedChannelStateReaderImpl` |
> | `DiskSnapshot` | `FetchedChannelStateSnapshot` |
> | `SpillFile` | `FetchedChannelState` |

## 1. The two places recovery data lives

At any instant during recovery, an unconsumed recovery record is in exactly one of two places:

- **In memory** — already drained off disk into some channel's `recoveredQueue`, waiting for the
  task to consume it.
- **On disk** — still in the spill file, not yet drained.

A checkpoint must capture **both** parts, with no record falling through the gap between them and
no record captured by both. That is the whole consistency problem.

The cut is produced atomically by `FetchedChannelStateDrainer.snapshotAndInsertBarriers(cpId)`:

- **Memory side** — insert a `RecoveryCheckpointBarrier` sentinel into every in-recovery channel's
  queue. Everything *before* the barrier is "in this checkpoint"; Step 2
  (`channel.checkpointStarted`) walks up to the barrier and hands those buffers to the channel
  state writer.
- **Disk side** — derive an independent reader from the drainer's committed position
  (`rootReader.snapshot().reader()`). Everything from that position onward is "in this
  checkpoint"; Step 3 (`addInputDataFromSpill`) replays it into the checkpoint stream.

Both happen inside the **same** `synchronized (lock)` block, so the memory barrier positions and
the disk committed position are a snapshot of the **same physical instant**.

## 2. The drainer manages two offsets — `current` vs `committed`

`FetchedChannelStateReaderImpl` holds **two** `Position`s and nothing else shadows them. This split
is the heart of the consistency guarantee.

| Position | Meaning | Who advances it | Inside the lock? |
|---|---|---|---|
| `current` | **Live read position** — exactly where the open file stream physically sits. | header reads (`nextSegment`), the consumer reading the body, first-positioning `skipBody` — all via `advanceReadOffset` | **No** — physical IO runs outside the lock |
| `committed` | **Delivered boundary** — how far data has actually been handed to a channel queue. | **only** `SpillSegment.commit()`, via `current.copyAsDelivered(committed, …)` | **Yes** — commit runs inside the drainer lock |

A `Position` is three values that fully locate a resume point: `fileIndex`,
`segmentStartOffset` (byte offset of the current segment's header), and `readOffset`.

**The key consequence — `snapshot()` captures `committed`, never `current`:**

```java
// FetchedChannelStateReaderImpl.snapshot()
return new FetchedChannelStateSnapshot(channelState, committed.copy());
```

So even though the reader's `current` may have physically run ahead — into the middle of a segment
body, because the consumer reads the body outside the lock — the snapshot still lands exactly on
the **delivered** boundary. The physical read position is irrelevant to the cut; only the
delivered boundary is.

### What this means for your question

- **Reading a header (`nextSegment`) advances `current`, not `committed`.** It does not move the
  snapshot.
- **The consumer reading body bytes advances `current`, not `committed`.** It does not move the
  snapshot.
- **Only `commit()` advances `committed`** — and therefore only `commit()` moves where a future
  snapshot resumes from.

So: yes — `getNextSegment` and body reads do not affect the snapshot offset; only `commit` does.
The precise statement is that **`snapshot()` is anchored on `committed`, the delivered boundary —
not on `current`, the physical read position.**

## 3. Why "deliver + commit" must be one atomic action

The drain critical section does exactly two pure in-memory actions per chunk
(`FetchedChannelStateDrainer.drainSegment`):

```java
synchronized (lock) {
    ch.onRecoveredStateBuffer(buf);   // buffer enters the channel's in-memory queue
    seg.commit();                     // committed advances to this delivered point
}
```

The slow work — the disk read that fills `buf` (`fill`) and the buffer allocation
(`requestRecoveryBufferBlocking`) — is deliberately kept **outside** the lock, so the critical
section stays microsecond-scale and the task thread's snapshot never waits on IO.

Because "buffer entered the queue" and "committed advanced" are the **same** atomic action, a
record can never be simultaneously *before* `committed` (i.e. "already delivered, so on disk it's
past the snapshot start") and *after* the in-memory barrier (i.e. "not yet delivered, captured by
the disk side"). That impossible overlap is exactly what would cause a duplicate, and the atomicity
rules it out.

`commit()` records **delivered body bytes** (`deliveredFromSegmentHead()` = skipped prefix + bytes
read so far), not "physically read up to here". A partial tail buffer that is delivered before the
segment is fully read still commits to the exact delivered point.

## 4. The ongoing-segment skip — resuming mid-body

A checkpoint can fire while a segment is **partially delivered**: some of its body already went out
in earlier buffers (so `committed` sits mid-body), but the rest is still on disk. The snapshot
reader must hand out **only the not-yet-delivered remainder** of that ongoing segment — not the
whole segment (would duplicate the delivered prefix), not nothing (would lose the remainder).

This is the **one and only place** in `FetchedChannelStateReaderImpl` that skips bytes — the first
`nextSegment()` call (`firstSegment()`); every later call does zero skipping:

1. `committed.readOffset` may sit mid-body. Compute the already-delivered prefix:
   `deliveredPrefix = current.deliveredBodyBytes()` (= `readOffset − segmentStartOffset −
   SEGMENT_HEADER_BYTES`, clamped at 0).
2. `rewindToSegmentStart()` — rewind the read offset back to the segment **header**, because the
   snapshot still needs the whole-segment header (channel id, full body length) even though it will
   only emit the tail.
3. Read the header. Then branch on the prefix:
   - `deliveredPrefix == bufferLength` → this segment was *fully* delivered before the snapshot;
     skip its whole body and move on to the next segment (`followingSegment()`).
   - `deliveredPrefix < bufferLength` → **skip exactly `deliveredPrefix` body bytes** and hand out a
     `BoundedSegmentStream` over the remaining `bufferLength − deliveredPrefix` bytes.

`BoundedSegmentStream` carries `alreadyDelivered = deliveredPrefix` so that if *this* (snapshot)
reader later commits, it records the boundary measured from the segment head, consistent with the
root reader. The bounded view reaches EOF exactly at the segment end — it never reads into the next
segment.

After this first positioning, every subsequent `nextSegment()` is steady-state: the previous body
was read to its end, so the stream already sits on the next header — no skip, no seek. A
`checkState` at `nextSegment()` entry enforces "previous body fully consumed before advancing".

## 5. Lifecycle: the disk slice is an independent reference

`snapshot()` returns a `FetchedChannelStateSnapshot` that, in its constructor, takes one
independent lifecycle grant on the underlying `FetchedChannelState` (`channelState.acquire()`). The
reader opened from it (`reader()`, callable exactly once — fail-loud on a second call) releases
that grant on `close()`. So the snapshot's disk slice survives independently of the drain reader:
the spill file's segments are deleted only when every reference (drain reader + every per-cpId
snapshot reader) has released.

If the drain already finished, the root reader is closed and there is nothing left on disk to
snapshot; `snapshotAndInsertBarriers` returns `FetchedChannelStateReader.emptyReader()` after still
inserting barriers, so the caller's normal flow handles it uniformly. The `drainFinished` flag is
read **inside the lock**, atomic with barrier insertion, so there is no window where drain closes
the root reader between the barrier insert and the snapshot derive.

## 6. The cut is complete and duplicate-free

Let T be the instant the task thread holds the lock inside `snapshotAndInsertBarriers`.

- **Complete.** Every unconsumed recovery record at T is either:
  - already delivered into a channel queue → it is *before* the barrier just inserted → captured by
    the memory side (Step 2); or
  - still on disk at/after `committed` → captured by the disk side (Step 3, via the snapshot
    reader, with the ongoing segment's delivered prefix skipped per §4).
- **Duplicate-free.** Inside the lock, the barrier positions and `committed` are observed at the
  same instant; "buffer entered queue" and "committed advanced" are one atomic action (§3), so no
  record is both before `committed` and after a barrier.
- **Drain resuming does not contaminate the cut.** Until the lock is released, the drain thread
  cannot enter any queue; its next delivery after release is guaranteed to land *after* the
  inserted barrier.
