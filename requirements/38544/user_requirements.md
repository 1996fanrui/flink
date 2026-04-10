# User Requirements — FLINK-38544 Spilling

## 需求偏离

| 需求编号 | 原因 | 替代方案 |
|---------|------|---------|
| REQ-NHLB | 原始需求未涉及 Heap Buffer 概念，Heap Buffer 是为解决 Source Buffer 和 Filtered Buffer 竞争 Network Buffer Pool 导致死锁的新设计 | Source Buffer 使用 Heap 分配（max 5 per gate），Filtered Buffer 通过 OutputWriter 管理 Network Buffer 或 spill to disk |

## Fundamental Principle

**This branch's ONLY goal**: replace heap buffer with disk when Network Buffer Pool is insufficient. Master branch uses unlimited heap buffer fallback in `requestBufferBlocking()`, risking OOM. This branch replaces that heap fallback with disk spilling to bound memory usage. **All other features — checkpoint, priority events, channel conversion, task consumption, barrier handling — must remain exactly the same.**

A spill file is logically equivalent to a heap buffer: same data, different storage medium. Anywhere a heap buffer works today, disk data must work identically.

## Core Architecture

OutputWriter and RecoveredBufferStore are the core components. They decouple filtering from InputChannel:

- **filterAndRewrite** writes bytes to `OutputWriter.write(bytes, gateIdx, channelInfo)`. It does not care about buffer allocation, disk spilling, or delivery to InputChannel. It only produces filtered bytes.
- **OutputWriter** manages buffer allocation and disk spilling internally. When a buffer is full or disk data is replayed, it delivers ready buffers to the target channel's RecoveredBufferStore. It does not know about InputChannel consumption or checkpoint.
- **RecoveredBufferStore** provides ready buffers to InputChannel via `tryTake()` and `checkpoint()`. It does not know about OutputWriter, disk files, or filtering. It only holds ready-to-consume buffers.

See data_flow.md for the detailed data flow diagram.

## Requirements

### REQ-NHLB Memory Isolation

Source Buffer (pre-filter) uses Heap memory, isolated from Network Buffer Pool. This eliminates deadlock where Source Buffer and Filtered Buffer compete for the same pool.

### REQ-QY68 Source Buffer Concurrency Control

Gate processes Virtual Channels sequentially (one at a time). Max 5 Heap Buffers per gate (~160KB). Prevents unbounded heap growth.

### REQ-8HRS Three Data Paths

Filtered data routed through OutputWriter with three paths:
- **P1**: Network Buffer available, no disk data → write to buffer → target channel's RecoveredBufferStore
- **P2**: No Network Buffer available → write to file on disk
- **P3**: Network Buffer available, disk has unreplayed data → replay oldest disk data to target channel's RecoveredBufferStore (FIFO ordering)

P3 drains eagerly: loop until no buffer available or disk empty.

### REQ-0EG7 OutputWriter Abstraction

filterAndRewrite writes to a unified OutputWriter interface. Filter logic does not know whether the backend is a Network Buffer or a File. Upper layer decides backend per request.

### REQ-WRTR Backend Downgrade Only

Within one writeToBackend call, backend can only downgrade (buffer → file), never upgrade. Downgrading creates disk data, which forces file path on subsequent checks. Upgrade opportunity is at the next write() call via P3 drain.

### REQ-BFSD Disk Pure Byte Stream

Spill files store raw bytes only. No metadata (record boundaries, channel context, DataType, etc.) on disk. All metadata lives in in-memory `Queue<SpillEntry>` with gateIndex, channelInfo, offset, and length per entry. Each SpillEntry corresponds to one memory-segment-sized chunk (`taskmanager.memory.segment-size`, default 32KB).

### REQ-SFMG Single File Per Task

All channels across all gates within a task share a single spill file. Data appended sequentially (FIFO). File rotation at 64MB. Old file deleted after all its entries replayed. No per-channel or per-gate files. This aligns with the per-task filtering thread — one thread, one OutputWriter, one spill file.

### REQ-CRSR Cursor-Based Disk Tracking

"Disk has data" means unreplayed data exists, tracked by a cursor (queue non-empty), not physical file existence. Once all entries replayed, subsequent writes can go to Network Buffer (pure memory path).

### REQ-CHDL Channel Change Detection

OutputWriter auto-detects channel change by comparing channelInfo with previous call. On change, flush current backend before writing new data. No separate notifyChannelChange() call needed.

### REQ-BYPS Byte-Position Switching

OutputWriter can switch between buffer and file at any byte position. A record may span across buffer and file. Task Thread's SpanningWrapper handles cross-buffer record reassembly transparently.

### REQ-RPLY Disk Replay Mechanism

Replay reads memory-segment-sized chunks (`taskmanager.memory.segment-size`, default 32KB) from spill file into Network Buffer, delivered to target channel's RecoveredBufferStore. No record boundary awareness needed. SpanningWrapper on consumer side reassembles spanning records.

### REQ-DRIN Drain Mechanism

Two drain phases:
1. **P3 eager drain** (during filtering): on each write(), OutputWriter eagerly replays disk data when buffer available, delivering to target channel's RecoveredBufferStore.
2. **Blocking drain** (after filtering): recovery thread runs blocking drain loop — `requestBufferBlocking()` → load from disk → deliver to target channel's RecoveredBufferStore — until disk empty. This loop runs concurrently with Task thread consumption. Channel conversion does NOT wait for drain to complete.

OutputWriter.close() only flushes current backend and stops accepting writes. It does NOT block on drain completion.

### REQ-SPDR Spill Directory from IOManager

Spill files use directories from `IOManager.getSpillingDirectoriesPaths()`, same as SpanningWrapper. No fallback to `java.io.tmpdir`. Invalid directories throw IOException.

### REQ-NPBY Non-Filtering Unaffected

When unaligned checkpoint recovery is disabled or parallelism unchanged (NO_RESCALE), the original channel state recovery path is used. No Heap Buffer, no OutputWriter, no changes.

### REQ-MNIV Minimal Code Invasion

All new logic (OutputWriter, SpillFile I/O, RecoveredBufferStore, drain loop) lives in new classes. Existing InputChannel classes (LocalInputChannel, RemoteInputChannel) only add a RecoveredBufferStore field and a `getNextBuffer()` branch to consume from the store before their existing data sources. No spill/disk details leak into InputChannel code.

### REQ-JD2C Resource Safety

- write/close on a closed OutputWriter must throw IllegalStateException
- SpillFileWriter.close() must use try-finally to guarantee file handle release
- OutputWriter.close() is idempotent: repeated calls do not throw
- Spill files cleaned up when all entries replayed and store released. Abnormal exit relies on TM's FileChannelManagerImpl shutdown hook

### REQ-KM7C Checkpoint Snapshot Support

When checkpoint triggers during recovery with unreplayed spill data, all unreplayed disk data must be included in the checkpoint snapshot. Disk data is logically equivalent to in-memory buffers and must be treated identically by checkpoint. This ensures no data loss on failover during recovery.

### REQ-G4KW Disk Data Consumption by InputChannel

After channel conversion (RecoveredInputChannel → LocalInputChannel/RemoteInputChannel), disk data must be consumable by the converted InputChannel via RecoveredBufferStore. RecoveredInputChannel cannot perform checkpoint (it lacks checkpoint protocol support — barrier handling, ChannelStatePersister, unaligned checkpoint). Therefore, channel conversion must happen even when disk data exists, and the converted InputChannel must be able to consume remaining disk data alongside its existing checkpoint and priority event handling.

### REQ-TXGD Existing Checkpoint Protocol Compatibility

Disk data consumption and checkpoint snapshotting must be compatible with the existing checkpoint protocol in LocalInputChannel/RemoteInputChannel, including: unaligned checkpoint barrier handling, priority event processing, ChannelStatePersister integration, and inflight buffer collection. No reimplementation of checkpoint protocol in RecoveredInputChannel.

### REQ-7388 RecoveredBufferStore Abstraction

Extract recovered buffer management into a standalone `RecoveredBufferStore` class, one per channel. This class encapsulates both in-memory buffers and disk-backed spill data behind a unified interface. Both RecoveredInputChannel (during recovery) and the final InputChannel (Local/Remote, after conversion) consume data through this store. The store hides all disk-to-buffer loading details from InputChannel.

**Architecture:**

```
OutputWriter (per-task)
  ├── Spill file (per-task, all gates/channels share)
  ├── SpillEntry queue [{gateIdx, channelInfo, offset, len}, ...]
  └── Drain loop: dequeue entry → requestBufferBlocking() → load from disk
        → dispatch to target channel's RecoveredBufferStore

RecoveredBufferStore (per-channel)
  └── ready queue [buffer, buffer, ...]
  └── used by: RecoveredInputChannel (during recovery)
             → then LocalInputChannel or RemoteInputChannel (after conversion)
```

**Lifecycle:**
1. Created per-channel during recovery. OutputWriter holds references to all stores.
2. OutputWriter populates stores: P1 → buffer directly into target store's ready queue; P2 → disk (shared spill file)
3. RecoveredInputChannel consumes via `store.tryTake()` during recovery phase
4. Filtering completes → `finishReadRecoveredState()` → channel conversion. Store reference transfers from RecoveredInputChannel to LocalInputChannel/RemoteInputChannel
5. Blocking drain loop (recovery thread) continues after filtering: loads disk data into buffers, dispatches to correct channel's store based on SpillEntry's (gateIndex, channelInfo)
6. Store reports completion when all data consumed and drain loop finished
7. InputChannel decides when to stop using the store (separate from store's own state)

**Interface** (verified against LocalInputChannel.getNextRecoveredBuffer, checkpointStarted, releaseAllResources, getBuffersInUseCount):

Consumption:
- `tryTake()` → Buffer or null — non-blocking, returns next ready buffer from internal queue. Returns null if no ready buffer (disk data may still be loading by drain loop)
- `peekNextDataType()` → Buffer.DataType — data type of the next available buffer without consuming. Returns `NONE` if empty

State:
- `isEmpty()` → boolean — no ready buffers AND no pending disk data. InputChannel uses this to decide whether to fall through to its next data source
- `isComplete()` → boolean — all data consumed AND drain loop finished (no more data will ever be added). InputChannel uses this to decide when to drop the store reference
- `size()` → int — number of ready buffers (for `getBuffersInUseCount`)

Checkpoint:
- `checkpoint(ChannelStateWriter, checkpointId, channelInfo)` — snapshot all remaining data: retain ready buffers + stream disk data directly to checkpoint storage without consuming Network Buffers

Resource cleanup:
- `releaseAll()` — recycle all ready buffers, close spill files, stop drain loop

Note: priority event handling (hasPendingPriorityEvent, fetching from subpartitionView) stays in InputChannel — the store is not involved. InputChannel handles priority events before calling `store.tryTake()`.

**Impact on existing code:**

The following commits introduced buffer migration and recovered buffer handling in physical channels. With RecoveredBufferStore, these changes are **replaced** — physical channels no longer receive recovered buffers in their existing queues. Instead, they hold a `RecoveredBufferStore` reference and consume via `store.tryTake()`.

LocalInputChannel:
- `toBeConsumedBuffers` no longer receives recovered buffers. It is retained **only** for `FullyFilledBuffer` splits (normal data path, unrelated to recovery).
- Constructor: remove `ArrayDeque<Buffer> initialRecoveredBuffers` parameter and buffer migration logic. Add `RecoveredBufferStore store` field instead.
- `getNextBuffer()`: check `store` first (recovered data), then `toBeConsumedBuffers` (FullyFilledBuffer splits), then `subpartitionView` (normal data).
- `getNextRecoveredBuffer()`: data source changes from `toBeConsumedBuffers` to `store.tryTake()`. Priority event handling (`hasPendingPriorityEvent`) stays in InputChannel, unchanged.
- `checkpointStarted()`: replace `toBeConsumedBuffers` iteration with `store.checkpoint(writer, checkpointId, channelInfo)`. Store handles both ready buffers and disk data snapshot internally.
- `getBuffersInUseCount()` / `unsynchronizedGetNumberOfQueuedBuffers()`: add `store.size()` to the count.
- `releaseAllResources()`: call `store.releaseAll()`. No longer need to recycle recovered buffers from `toBeConsumedBuffers`.

RemoteInputChannel:
- `receivedBuffers` no longer receives recovered buffers. It is retained **only** for live network data.
- Constructor: remove `ArrayDeque<Buffer> initialRecoveredBuffers` parameter and buffer migration logic. Add `RecoveredBufferStore store` field instead.
- `getNextBuffer()`: check `store` first (recovered data), then `receivedBuffers` (network data).
- `checkReadability()` hack is removable — `receivedBuffers` only contains network data which requires `partitionRequestClient` initialization.
- `checkpointStarted()`: call `store.checkpoint()` for recovered data, then `getInflightBuffersUnsafe()` for network data (existing logic unchanged). `RecoveryMetadata` append logic stays in RemoteInputChannel, not in store.
- `getBuffersInUseCount()` / `unsynchronizedGetNumberOfQueuedBuffers()`: add `store.size()` to the count.
- `releaseAllResources()`: call `store.releaseAll()`.

RecoveredInputChannel:
- `receivedBuffers` (ArrayDeque) replaced by store. `onRecoveredStateBuffer()` replaced by `OutputWriter.write()` → store.
- `toInputChannel()`: no longer extracts `remainingBuffers`. Instead, passes `store` reference to the new physical channel.
- `requestBufferBlocking()` heap fallback removed — OutputWriter handles buffer/disk allocation internally.

**RemoteInputChannel compatibility verified**: SequenceBuffer wrapping is not needed — store returns Buffer, InputChannel wraps into BufferAndAvailability directly. Subpartition routing via RecoveryMetadata stays in RemoteInputChannel.checkpointStarted(), not in store. Priority element handling (convertToPriorityEvent, PrioritizedDeque) only operates on network data in receivedBuffers, never on store data. No additional store methods required.

### REQ-T5AJ Read Robustness

- Partial read (fewer bytes than expected) must throw IOException, not return silently
- File truncation/corruption detected and reported as IOException
