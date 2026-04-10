# User Requirements — FLINK-38544 Spilling

## 需求偏离

| 需求编号 | 原因 | 替代方案 |
|---------|------|---------|
| REQ-NHLB | 原始需求未涉及 Heap Buffer 概念，Heap Buffer 是为解决 Source Buffer 和 Filtered Buffer 竞争 Network Buffer Pool 导致死锁的新设计 | Source Buffer 使用 Heap 分配（max 5 per gate），Filtered Buffer 通过 OutputWriter 管理 Network Buffer 或 spill to disk |

## Fundamental Principle

**This branch's ONLY goal**: replace heap buffer with disk when Network Buffer Pool is insufficient. Master branch uses unlimited heap buffer fallback in `requestBufferBlocking()`, risking OOM. This branch replaces that heap fallback with disk spilling to bound memory usage. **All other features — checkpoint, priority events, channel conversion, task consumption, barrier handling — must remain exactly the same.**

A spill file is logically equivalent to a heap buffer: same data, different storage medium. Anywhere a heap buffer works today, disk data must work identically.

## Requirements

### REQ-NHLB Memory Isolation

Source Buffer (pre-filter) uses Heap memory, isolated from Network Buffer Pool. This eliminates deadlock where Source Buffer and Filtered Buffer compete for the same pool.

### REQ-QY68 Source Buffer Concurrency Control

Gate processes Virtual Channels sequentially (one at a time). Max 5 Heap Buffers per gate (~160KB). Prevents unbounded heap growth.

### REQ-8HRS Three Data Paths

Filtered data routed through OutputWriter with three paths:
- **P1**: Network Buffer available, no disk data → write to buffer → InputChannel
- **P2**: No Network Buffer available → write to file on disk
- **P3**: Network Buffer available, disk has unreplayed data → replay oldest disk data to InputChannel (FIFO ordering)

P3 drains eagerly: loop until no buffer available or disk empty.

### REQ-0EG7 OutputWriter Abstraction

filterAndRewrite writes to a unified OutputWriter interface. Filter logic does not know whether the backend is a Network Buffer or a File. Upper layer decides backend per request.

### REQ-WRTR Backend Downgrade Only

Within one writeToBackend call, backend can only downgrade (buffer → file), never upgrade. Downgrading creates disk data, which forces file path on subsequent checks. Upgrade opportunity is at the next write() call via P3 drain.

### REQ-BFSD Disk Pure Byte Stream

Spill files store raw bytes only. No metadata (record boundaries, channel context, DataType, etc.) on disk. All metadata lives in in-memory `Queue<SpillEntry>` with channelInfo, offset, and length per entry. Each SpillEntry corresponds to one buffer-sized chunk.

### REQ-SFMG Single File Per Gate

All channels within a gate share a single spill file. Data appended sequentially (FIFO). File rotation at 64MB. Old file deleted after all its entries replayed. No per-channel files — avoid large number of small files.

### REQ-CRSR Cursor-Based Disk Tracking

"Disk has data" means unreplayed data exists, tracked by a cursor (queue non-empty), not physical file existence. Once all entries replayed, subsequent writes can go to Network Buffer (pure memory path).

### REQ-CHDL Channel Change Detection

OutputWriter auto-detects channel change by comparing channelInfo with previous call. On change, flush current backend before writing new data. No separate notifyChannelChange() call needed.

### REQ-BYPS Byte-Position Switching

OutputWriter can switch between buffer and file at any byte position. A record may span across buffer and file. Task Thread's SpanningWrapper handles cross-buffer record reassembly transparently.

### REQ-RPLY Disk Replay Mechanism

Replay reads buffer-sized chunks (from InputGate config, typically 32KB) from spill file into Network Buffer, delivered to InputChannel. No record boundary awareness needed. SpanningWrapper on consumer side reassembles spanning records.

### REQ-DRIN Close Drain

OutputWriter.close() flushes current backend, then blocking-drains all remaining disk data: loop requestBufferBlocking() → replay → InputChannel, until disk empty.

### REQ-SPDR Spill Directory from IOManager

Spill files use directories from `IOManager.getSpillingDirectoriesPaths()`, same as SpanningWrapper. No fallback to `java.io.tmpdir`. Invalid directories throw IOException.

### REQ-NPBY Non-Filtering Unaffected

When unaligned checkpoint recovery is disabled or parallelism unchanged (NO_RESCALE), the original channel state recovery path is used. No Heap Buffer, no OutputWriter, no changes.

### REQ-MNIV Minimal Code Invasion

All new logic (OutputWriter, SpillFile I/O, P3 replay, file offset management) lives in new classes. Existing files only call `writer.write()`. No internal details leak into existing code.

### REQ-JD2C Resource Safety

- write/close on a closed OutputWriter must throw IllegalStateException
- SpillFileWriter.close() must use try-finally to guarantee file handle release
- OutputWriter.close() is idempotent: repeated calls do not throw
- Spill files cleaned up on close. Abnormal exit relies on TM's FileChannelManagerImpl shutdown hook

### REQ-KM7C Checkpoint Snapshot Support

When checkpoint triggers during recovery with unreplayed spill data, all unreplayed disk data must be included in the checkpoint snapshot. Disk data is logically equivalent to in-memory buffers and must be treated identically by checkpoint. This ensures no data loss on failover during recovery.

### REQ-G4KW Disk Data Consumption by InputChannel

After channel conversion (RecoveredInputChannel → LocalInputChannel/RemoteInputChannel), disk data must be consumable by the converted InputChannel. RecoveredInputChannel cannot perform checkpoint (it lacks checkpoint protocol support — barrier handling, ChannelStatePersister, unaligned checkpoint). Therefore, channel conversion must happen even when disk data exists, and the converted InputChannel must be able to consume remaining disk data alongside its existing checkpoint and priority event handling.

### REQ-TXGD Existing Checkpoint Protocol Compatibility

Disk data consumption and checkpoint snapshotting must be compatible with the existing checkpoint protocol in LocalInputChannel/RemoteInputChannel, including: unaligned checkpoint barrier handling, priority event processing, ChannelStatePersister integration, and inflight buffer collection. No reimplementation of checkpoint protocol in RecoveredInputChannel.

### REQ-7388 RecoveredBufferStore Abstraction

Extract recovered buffer management into a standalone `RecoveredBufferStore` class. This class encapsulates both in-memory buffers and disk-backed spill data behind a unified interface. Both RecoveredInputChannel (during recovery) and the final InputChannel (Local/Remote, after conversion) consume data through this store. The store hides all disk-to-buffer loading details from InputChannel.

**Lifecycle:**
1. Created during recovery, populated by OutputWriter (P1 → in-memory buffer, P2 → disk)
2. RecoveredInputChannel consumes via store during recovery phase
3. On channel conversion, store reference transfers from RecoveredInputChannel to LocalInputChannel/RemoteInputChannel
4. Blocking drain loop (recovery thread) continuously loads disk data into ready buffers: `requestBufferBlocking()` → load from disk → ready for consumption
5. Store reports completion when all data (in-memory + disk) is consumed
6. InputChannel decides when to stop using the store (separate from store's own state)

**Interface** (verified against LocalInputChannel.getNextRecoveredBuffer, checkpointStarted, releaseAllResources, getBuffersInUseCount):

Consumption:
- `tryTake()` → Buffer or null — non-blocking, returns next ready buffer from internal queue. Returns null if no ready buffer (disk data may still be loading). InputChannel calls this where it previously called `toBeConsumedBuffers.removeFirst()`
- `peekNextDataType()` → Buffer.DataType — data type of the next available buffer without consuming. Used by InputChannel to construct `BufferAndAvailability.nextDataType`. Returns `NONE` if empty

State:
- `isEmpty()` → boolean — no ready buffers AND no pending disk data. InputChannel uses this to decide whether to fall through to subpartitionView/receivedBuffers
- `isComplete()` → boolean — all data consumed AND blocking drain loop finished (no more data will ever be added). InputChannel uses this to decide when to drop the store reference
- `size()` → int — number of ready buffers (for `getBuffersInUseCount`)

Checkpoint:
- `checkpoint(ChannelStateWriter, checkpointId, channelInfo)` — snapshot all remaining data: retain ready buffers + stream disk data directly to checkpoint storage without consuming Network Buffers

Resource cleanup:
- `releaseAll()` — recycle all ready buffers, close spill files, stop drain loop

Note: priority event handling (hasPendingPriorityEvent, fetching from subpartitionView) stays in InputChannel — the store is not involved. InputChannel handles priority events before calling `store.tryTake()`.

**Impact on existing code:**
- `toBeConsumedBuffers` in LocalInputChannel no longer receives recovered buffers (keeps FullyFilledBuffer splits only)
- `receivedBuffers` in RemoteInputChannel no longer receives recovered buffers (keeps live network data only, `checkReadability()` hack removable)
- `receivedBuffers` in RecoveredInputChannel replaced by store

**TODO**: Verify interface completeness against RemoteInputChannel recovered buffer usage patterns (SequenceBuffer wrapping, subpartition routing, priority element handling). The interface above is validated against LocalInputChannel but RemoteInputChannel may require additional methods.

### REQ-T5AJ Read Robustness

- Partial read (fewer bytes than expected) must throw IOException, not return silently
- File truncation/corruption detected and reported as IOException
