# Acceptance Tests — FLINK-38544 Spilling

## Status

| ID | Summary | Requirement | Status |
|----|---------|-------------|--------|
| AT-2W3J | P1: buffer available, no disk data → InputChannel | REQ-8HRS | Pending |
| AT-GE7G | P2: no buffer → write to file | REQ-8HRS | Pending |
| AT-SX5O | P3: replay oldest disk data to InputChannel | REQ-8HRS | Pending |
| AT-QUBL | P3 priority over P1 (FIFO ordering) | REQ-8HRS,REQ-CRSR | Pending |
| AT-P3DL | P3 eager drain (loop until no buffer or disk empty) | REQ-8HRS | Pending |
| AT-36DP | Source Buffer uses Heap, does not compete with Pool | REQ-NHLB | Pending |
| AT-41PK | Heap Buffer max 5 per gate | REQ-QY68 | Pending |
| AT-UE7O | Gate processes channels sequentially | REQ-QY68 | Pending |
| AT-DWGD | Backend downgrade only (buffer → file), no upgrade | REQ-WRTR | Pending |
| AT-BYPS | Record spans across buffer and file correctly | REQ-BYPS | Pending |
| AT-CHDL | Channel change auto-detected, flush on change | REQ-CHDL | Pending |
| AT-SFMG | Single spill file per gate, all channels share | REQ-SFMG | Pending |
| AT-5097 | File rotation at 64MB | REQ-SFMG | Pending |
| AT-CRSR | Disk has data = unreplayed entries, cursor-based | REQ-CRSR | Pending |
| AT-DRIN | close() blocking drain until disk empty | REQ-DRIN | Pending |
| AT-CLID | close() idempotent | REQ-JD2C | Pending |
| AT-CLFL | close() cleans up all spill files | REQ-JD2C | Pending |
| AT-CWRT | write after close throws IllegalStateException | REQ-JD2C | Pending |
| AT-HW4P | Truncated file throws IOException | REQ-T5AJ | Pending |
| AT-C3MK | Spill dirs from IOManager, no java.io.tmpdir fallback | REQ-SPDR | Pending |
| AT-U7Q2 | Non-filtering scenario unchanged | REQ-NPBY | Pending |
| AT-LN5V | Large data: multiple file rotations, full replay correct | REQ-SFMG,REQ-8HRS | Pending |
| AT-UFNZ | UnalignedCheckpointRescaleITCase integration test | REQ-NHLB,REQ-8HRS,REQ-NPBY | Pending |

## Test Details

### AT-2W3J P1 Memory Path

Network Buffer Pool has buffer, no disk data → filtered data written to Network Buffer → InputChannel. No spill file created.

### AT-GE7G P2 Spill Path

Network Buffer Pool exhausted → filtered data written to spill file on disk. Disk cursor indicates unreplayed data.

### AT-SX5O P3 Replay Path

Disk has unreplayed data, Network Buffer available → replay oldest 32KB chunk from disk to InputChannel. Data content matches original.

### AT-QUBL P3 FIFO Ordering

When disk has data, new filtered data must go to disk (not directly to InputChannel). Replay order matches write order across all channels.

### AT-P3DL P3 Eager Drain

On each write(), P3 replay loops until no buffer available or disk empty. Not just one entry per write.

### AT-36DP Source Buffer Heap Isolation

getBuffer() in filtering mode returns Heap Buffer (allocateUnpooledSegment). Network Buffer Pool available count unchanged.

### AT-41PK Heap Buffer Limit

Exceeding 5 Heap Buffers per gate throws IllegalStateException.

### AT-UE7O Sequential Channel Processing

Only one channel's data processed at a time within a gate. No concurrent Source Buffer holding.

### AT-DWGD Backend Downgrade Only

Within one writeToBackend call: start with buffer, buffer full → request fails → downgrade to file. Once on file, stays on file for remainder of call.

### AT-BYPS Cross-Backend Record Spanning

Write a record that starts in a Network Buffer and ends in a file (buffer full mid-record, no new buffer available). After replay, Task Thread deserializes the record correctly via SpanningWrapper.

### AT-CHDL Channel Change Detection

Write data for channel A, then channel B. Verify current backend is flushed between channel transitions. SpillEntry queue has correct channelInfo per entry.

### AT-SFMG Single File Per Gate

Multiple channels write to OutputWriter. Verify only one spill file created (not one per channel). All entries in same file with correct offsets.

### AT-5097 File Rotation

Write more than 64MB of data. Verify multiple spill files created. All data replayed correctly across files. Old files deleted after all entries replayed.

### AT-CRSR Cursor-Based Tracking

Spill data, replay partially. hasDiskData() returns true. Replay all remaining. hasDiskData() returns false. Subsequent writes can use Network Buffer (no forced file path).

### AT-DRIN Close Drain

After all S3 data consumed, call close(). All remaining disk data drained to InputChannel via blocking buffer requests. Disk empty after close.

### AT-CLID Close Idempotent

Calling close() twice does not throw.

### AT-CLFL Close Cleanup

After close(), all spill files deleted from disk.

### AT-CWRT Write After Close

write() after close() throws IllegalStateException.

### AT-HW4P Truncated File

Truncate a spill file mid-entry. Replay throws IOException with expected vs actual byte count.

### AT-C3MK Spill Directory Source

OutputWriter constructor accepts String[] from IOManager. No fallback to java.io.tmpdir. Empty array throws IOException.

### AT-U7Q2 Non-Filtering Unchanged

unalignedDuringRecoveryEnabled=false or NO_RESCALE: no OutputWriter created, no Heap Buffer allocated, original path used.

### AT-LN5V Large Data Multi-Rotation

Write enough data to trigger 3+ file rotations. All data replayed in FIFO order. Content matches. All files cleaned up.

### AT-UFNZ Integration Test

UnalignedCheckpointRescaleITCase passes with all rescale scenarios.
