# Design Discussion — FLINK-38544 Spilling

## Core Principle (Agreed)

**The ONLY goal of this branch**: replace heap buffer with disk when Network Buffer Pool is insufficient. ALL other features (checkpoint, priority events, channel conversion, task consumption) must remain exactly the same.

Master branch: `requestBufferBlocking()` → pool exhausted → allocate unlimited heap buffer → OOM risk.
This branch: `requestBufferBlocking()` → pool exhausted → spill to disk → bounded memory.

The spill file is logically equivalent to a heap buffer — same data, different storage medium.

## Current Master Branch Flow

```
Recovery thread:
  ChannelStateChunkReader.readChunk()
    → stateHandler.getBuffer()
      → channel.requestBufferBlocking()     ← returns heap buffer if pool exhausted
    → stateHandler.recover()
      → recoverWithFiltering()
        → filterAndRewrite(..., channel::requestBufferBlocking)  ← also returns heap buffer
        → returns List<Buffer>              ← heap buffers with filtered data
        → channel.onRecoveredStateBuffer(buffer)  ← into receivedBuffers
  → finishReadRecoveredState()
    → bufferFilteringCompleteFuture.complete()

Task thread (concurrent in checkpoint-during-recovery mode):
  RecoveredInputChannel.getNextBuffer()
    → receivedBuffers.poll()                ← consumes heap buffers
  → eventually: toInputChannel()
    → remainingBuffers from receivedBuffers → LocalInputChannel.toBeConsumedBuffers

Checkpoint (after channel conversion):
  LocalInputChannel.checkpointStarted()
    → snapshot toBeConsumedBuffers           ← includes former heap buffers
```

Everything is a Buffer in memory. Checkpoint naturally covers all data.

## The Challenge with Disk Replacement

When we replace heap buffer with disk:
1. `filterAndRewrite` needs to write output somewhere when no buffer available → OutputWriter (routes to buffer or disk)
2. Disk data needs to be consumed by Task thread → needs a Buffer (MemorySegment is final)
3. Disk data needs to be checkpoint-able → needs to be snapshot

## Key Constraint: Why Heap Fallback Exists

The heap fallback prevents deadlock. Without it:
1. Recovery thread calls `requestBufferBlocking()` for output buffer
2. All pool buffers are in `receivedBuffers` (not yet consumed)
3. Task thread hasn't started yet (early recovery phase)
4. Recovery thread blocks forever → deadlock

Heap buffer bypasses the pool, breaking the deadlock. Disk spilling serves the same purpose — it provides an output path that doesn't consume pool buffers.

## Open Question: Disk Data Consumption & Checkpoint

### Consumption
RecoveredInputChannel.getNextBuffer() currently returns from `receivedBuffers` only. With disk data, it needs a second source. Two options:

**Option A**: Modify RecoveredInputChannel.getNextBuffer() to also load from disk when receivedBuffers is empty but disk data exists. Request a network buffer (non-blocking), load chunk from disk, return. This is contained within RecoveredInputChannel — no changes to Local/RemoteInputChannel.

**Option B**: Blocking drain within RecoveredInputChannel lifecycle — load all disk data back to receivedBuffers before channel conversion. Simple but delays checkpoint.

### Checkpoint
RecoveredInputChannel.checkpointStarted() currently throws `CHECKPOINT_DECLINED_TASK_NOT_READY`. With disk data, it needs to snapshot receivedBuffers + disk data.

**Priority event concern**: In unaligned checkpoint, checkpoint barrier is a priority event. During recovery, if some channels are already converted (LocalInputChannel) and others are still RecoveredInputChannel, the InputGate needs all channels to handle the barrier. RecoveredInputChannel must support checkpointStarted() for this to work.

**However**: RecoveredInputChannel's data is recovered state (no live upstream, no in-stream barriers). The priority event handling in RecoveredInputChannel may be simpler than LocalInputChannel because there's no subpartitionView to coordinate with. The checkpoint just needs to snapshot all unconsumed data (receivedBuffers + disk). Need deeper investigation on what exactly RecoveredInputChannel needs for priority events vs what LocalInputChannel needs.

## TODO

- [ ] Investigate: what does RecoveredInputChannel.checkpointStarted() actually need for unaligned checkpoint? Is it just snapshotting inflight buffers, or does it need full priority event handling?
- [ ] Investigate: can RecoveredInputChannel support checkpoint without the full priority event logic that LocalInputChannel has?
- [ ] Design the exact modification to RecoveredInputChannel for disk data consumption + checkpoint
