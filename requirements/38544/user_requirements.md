# User Requirements — FLINK-38544 Spilling

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

Spill files store raw bytes only. No metadata (record boundaries, channel context, DataType, etc.) on disk. All metadata lives in in-memory `Queue<SpillEntry>` with channelInfo, offset, and length per entry.

### REQ-SFMG Single File Per Gate

All channels within a gate share a single spill file. Data appended sequentially (FIFO). File rotation at 64MB. Old file deleted after all its entries replayed. No per-channel files — avoid large number of small files.

### REQ-CRSR Cursor-Based Disk Tracking

"Disk has data" means unreplayed data exists, tracked by a cursor (queue non-empty), not physical file existence. Once all entries replayed, subsequent writes can go to Network Buffer (pure memory path).

### REQ-CHDL Channel Change Detection

OutputWriter auto-detects channel change by comparing channelInfo with previous call. On change, flush current backend before writing new data. No separate notifyChannelChange() call needed.

### REQ-BYPS Byte-Position Switching

OutputWriter can switch between buffer and file at any byte position. A record may span across buffer and file. Task Thread's SpanningWrapper handles cross-buffer record reassembly transparently.

### REQ-RPLY Disk Replay Mechanism

Replay reads fixed-size chunks (32KB) from spill file into Network Buffer, delivered to InputChannel. No record boundary awareness needed. SpanningWrapper on consumer side reassembles spanning records.

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

### REQ-T5AJ Read Robustness

- Partial read (fewer bytes than expected) must throw IOException, not return silently
- File truncation/corruption detected and reported as IOException
