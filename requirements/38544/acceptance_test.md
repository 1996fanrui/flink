# Acceptance Tests — FLINK-38544 Spilling

## Status

| 编号 | 测试内容概要 | 需求ID列表 | 状态 | 测试执行方 | 备注 |
|------|------------|-----------|------|-----------|------|
| AT-2W3J | P1: buffer available, no disk data → InputChannel | REQ-8HRS | 待测试 | 代码自动化 | |
| AT-GE7G | P2: no buffer → write to file | REQ-8HRS | 待测试 | 代码自动化 | |
| AT-SX5O | P3: replay oldest disk data to InputChannel | REQ-8HRS | 待测试 | 代码自动化 | |
| AT-QUBL | P3 priority over P1 (FIFO ordering) | REQ-8HRS,REQ-CRSR | 待测试 | 代码自动化 | |
| AT-P3DL | P3 eager drain (loop until no buffer or disk empty) | REQ-8HRS | 待测试 | 代码自动化 | |
| AT-36DP | Source Buffer uses Heap, does not compete with Pool | REQ-NHLB | 待测试 | 代码自动化 | |
| AT-41PK | Heap Buffer max 5 per gate | REQ-QY68 | 待测试 | 代码自动化 | |
| AT-UE7O | Gate processes channels sequentially | REQ-QY68 | 待测试 | 代码自动化 | |
| AT-DWGD | Backend downgrade only (buffer → file), no upgrade | REQ-WRTR | 待测试 | 代码自动化 | |
| AT-BYPS | Record spans across buffer and file correctly | REQ-BYPS | 待测试 | 代码自动化 | |
| AT-CHDL | Channel change auto-detected, flush on change | REQ-CHDL | 待测试 | 代码自动化 | |
| AT-SFMG | Single spill file per gate, all channels share | REQ-SFMG | 待测试 | 代码自动化 | |
| AT-5097 | File rotation at 64MB | REQ-SFMG | 待测试 | 代码自动化 | |
| AT-CRSR | Disk has data = unreplayed entries, cursor-based | REQ-CRSR | 待测试 | 代码自动化 | |
| AT-DRIN | close() blocking drain until disk empty | REQ-DRIN | 待测试 | 代码自动化 | |
| AT-CLID | close() idempotent | REQ-JD2C | 待测试 | 代码自动化 | |
| AT-CLFL | close() cleans up all spill files | REQ-JD2C | 待测试 | 代码自动化 | |
| AT-CWRT | write after close throws IllegalStateException | REQ-JD2C | 待测试 | 代码自动化 | |
| AT-HY10 | SpillFileWriter.close() try-finally guarantees file handle release | REQ-JD2C | 待测试 | 代码自动化 | |
| AT-HW4P | Truncated file throws IOException | REQ-T5AJ | 待测试 | 代码自动化 | |
| AT-C3MK | Spill dirs from IOManager, no java.io.tmpdir fallback | REQ-SPDR | 待测试 | 代码自动化 | |
| AT-U7Q2 | Non-filtering scenario unchanged | REQ-NPBY | 待测试 | 代码自动化 | |
| AT-LN5V | Large data: multiple file rotations, full replay correct | REQ-SFMG,REQ-8HRS | 待测试 | 代码自动化 | |
| AT-9632 | OutputWriter abstraction: filter writes to unified interface | REQ-0EG7 | 待测试 | 代码自动化 | |
| AT-7OWS | Spill file stores raw bytes only, no metadata on disk | REQ-BFSD,REQ-RPLY | 待测试 | 代码自动化 | |
| AT-CTTS | Checkpoint snapshot of unreplayed disk data | REQ-KM7C | 待测试 | 代码自动化 | |
| AT-N3YQ | Concurrent checkpoint snapshot and replay | REQ-KM7C | 待测试 | 代码自动化 | |
| AT-HQB4 | SpillEntry granularity equals buffer size | REQ-BFSD,REQ-RPLY | 待测试 | 代码自动化 | |
| AT-1KTC | Minimal code invasion: new logic in new classes | REQ-MNIV | 待测试 | Agent 执行 | |
| AT-UFNZ | UnalignedCheckpointRescaleITCase integration test | REQ-NHLB,REQ-8HRS,REQ-NPBY | 待测试 | 代码自动化 | |

## Test Details

### [L1-测试] AT-2W3J P1 Memory Path

Network Buffer Pool has buffer, no disk data → filtered data written to Network Buffer → InputChannel. No spill file created.

**命令**: `./mvnw test -pl flink-runtime -Dtest=OutputWriterTest#testP1MemoryPath -P java11-target -P java11`
**断言**: test pass, exit code 0

### [L1-测试] AT-GE7G P2 Spill Path

Network Buffer Pool exhausted → filtered data written to spill file on disk. Disk cursor indicates unreplayed data.

**命令**: `./mvnw test -pl flink-runtime -Dtest=OutputWriterTest#testP2SpillPath -P java11-target -P java11`
**断言**: test pass, exit code 0

### [L1-测试] AT-SX5O P3 Replay Path

Disk has unreplayed data, Network Buffer available → replay oldest 32KB chunk from disk to InputChannel. Data content matches original.

**命令**: `./mvnw test -pl flink-runtime -Dtest=OutputWriterTest#testP3ReplayPath -P java11-target -P java11`
**断言**: test pass, exit code 0

### [L1-测试] AT-QUBL P3 FIFO Ordering

When disk has data, new filtered data must go to disk (not directly to InputChannel). Replay order matches write order across all channels.

**命令**: `./mvnw test -pl flink-runtime -Dtest=OutputWriterTest#testP3FIFOOrdering -P java11-target -P java11`
**断言**: test pass, exit code 0

### [L1-测试] AT-P3DL P3 Eager Drain

On each write(), P3 replay loops until no buffer available or disk empty. Not just one entry per write.

**命令**: `./mvnw test -pl flink-runtime -Dtest=OutputWriterTest#testP3EagerDrain -P java11-target -P java11`
**断言**: test pass, exit code 0

### [L1-测试] AT-36DP Source Buffer Heap Isolation

getBuffer() in filtering mode returns Heap Buffer (allocateUnpooledSegment). Network Buffer Pool available count unchanged.

**命令**: `./mvnw test -pl flink-runtime -Dtest=RecoveredInputChannelTest#testHeapBufferIsolation -P java11-target -P java11`
**断言**: test pass, exit code 0

### [L1-测试] AT-41PK Heap Buffer Limit

Exceeding 5 Heap Buffers per gate blocks or throws.

**命令**: `./mvnw test -pl flink-runtime -Dtest=RecoveredChannelStateHandlerTest#testHeapBufferLimit -P java11-target -P java11`
**断言**: test pass, exit code 0

### [L1-测试] AT-UE7O Sequential Channel Processing

Only one channel's data processed at a time within a gate. No concurrent Source Buffer holding.

**命令**: `./mvnw test -pl flink-runtime -Dtest=RecoveredChannelStateHandlerTest#testSequentialChannelProcessing -P java11-target -P java11`
**断言**: test pass, exit code 0

### [L1-测试] AT-DWGD Backend Downgrade Only

Within one writeToBackend call: start with buffer, buffer full → request fails → downgrade to file. Once on file, stays on file for remainder of call.

**命令**: `./mvnw test -pl flink-runtime -Dtest=OutputWriterTest#testBackendDowngradeOnly -P java11-target -P java11`
**断言**: test pass, exit code 0

### [L1-测试] AT-BYPS Cross-Backend Record Spanning

Write a record that starts in a Network Buffer and ends in a file (buffer full mid-record, no new buffer available). After replay, Task Thread deserializes the record correctly via SpanningWrapper.

**命令**: `./mvnw test -pl flink-runtime -Dtest=OutputWriterTest#testCrossBackendRecordSpanning -P java11-target -P java11`
**断言**: test pass, exit code 0

### [L1-测试] AT-CHDL Channel Change Detection

Write data for channel A, then channel B. Verify current backend is flushed between channel transitions. SpillEntry queue has correct channelInfo per entry.

**命令**: `./mvnw test -pl flink-runtime -Dtest=OutputWriterTest#testChannelChangeDetection -P java11-target -P java11`
**断言**: test pass, exit code 0

### [L1-测试] AT-SFMG Single File Per Gate

Multiple channels write to OutputWriter. Verify only one spill file created (not one per channel). All entries in same file with correct offsets.

**命令**: `./mvnw test -pl flink-runtime -Dtest=OutputWriterTest#testSingleFilePerGate -P java11-target -P java11`
**断言**: test pass, exit code 0

### [L1-测试] AT-5097 File Rotation

Write more than 64MB of data. Verify multiple spill files created. All data replayed correctly across files. Old files deleted after all entries replayed.

**命令**: `./mvnw test -pl flink-runtime -Dtest=SpillFileTest#testFileRotation -P java11-target -P java11`
**断言**: test pass, exit code 0

### [L1-测试] AT-CRSR Cursor-Based Tracking

Spill data, replay partially. hasDiskData() returns true. Replay all remaining. hasDiskData() returns false. Subsequent writes can use Network Buffer (no forced file path).

**命令**: `./mvnw test -pl flink-runtime -Dtest=OutputWriterTest#testCursorBasedTracking -P java11-target -P java11`
**断言**: test pass, exit code 0

### [L1-测试] AT-DRIN Close Drain

After all S3 data consumed, call close(). All remaining disk data drained to InputChannel via blocking buffer requests. Disk empty after close.

**命令**: `./mvnw test -pl flink-runtime -Dtest=OutputWriterTest#testCloseDrain -P java11-target -P java11`
**断言**: test pass, exit code 0

### [L1-测试] AT-CLID Close Idempotent

Calling close() twice does not throw.

**命令**: `./mvnw test -pl flink-runtime -Dtest=OutputWriterTest#testCloseIdempotent -P java11-target -P java11`
**断言**: test pass, exit code 0

### [L1-测试] AT-CLFL Close Cleanup

After close(), all spill files deleted from disk.

**命令**: `./mvnw test -pl flink-runtime -Dtest=OutputWriterTest#testCloseCleanup -P java11-target -P java11`
**断言**: test pass, exit code 0

### [L1-测试] AT-CWRT Write After Close

write() after close() throws IllegalStateException.

**命令**: `./mvnw test -pl flink-runtime -Dtest=OutputWriterTest#testWriteAfterClose -P java11-target -P java11`
**断言**: test pass, exit code 0

### [L1-测试] AT-HY10 SpillFileWriter Try-Finally

SpillFileWriter.close() uses try-finally to guarantee file handle release even when IOException occurs during close.

**命令**: `./mvnw test -pl flink-runtime -Dtest=SpillFileTest#testCloseReleasesFileHandle -P java11-target -P java11`
**断言**: test pass, exit code 0

### [L1-测试] AT-HW4P Truncated File

Truncate a spill file mid-entry. Replay throws IOException with expected vs actual byte count.

**命令**: `./mvnw test -pl flink-runtime -Dtest=SpillFileTest#testTruncatedFileThrows -P java11-target -P java11`
**断言**: test pass, exit code 0

### [L1-测试] AT-C3MK Spill Directory Source

OutputWriter constructor accepts String[] from IOManager. No fallback to java.io.tmpdir. Empty array throws IOException.

**命令**: `./mvnw test -pl flink-runtime -Dtest=OutputWriterTest#testSpillDirectorySource -P java11-target -P java11`
**断言**: test pass, exit code 0

### [L1-测试] AT-U7Q2 Non-Filtering Unchanged

unalignedDuringRecoveryEnabled=false or NO_RESCALE: no OutputWriter created, no Heap Buffer allocated, original path used.

**命令**: `./mvnw test -pl flink-runtime -Dtest=RecoveredChannelStateHandlerTest#testNonFilteringUnchanged -P java11-target -P java11`
**断言**: test pass, exit code 0

### [L1-测试] AT-LN5V Large Data Multi-Rotation

Write enough data to trigger 3+ file rotations. All data replayed in FIFO order. Content matches. All files cleaned up.

**命令**: `./mvnw test -pl flink-runtime -Dtest=OutputWriterTest#testLargeDataMultiRotation -P java11-target -P java11`
**断言**: test pass, exit code 0

### [L1-测试] AT-9632 OutputWriter Abstraction

filterAndRewrite writes to a unified OutputWriter interface. Filter logic does not know whether the backend is a Network Buffer or a File. Verify OutputWriter.write() accepts raw bytes and channelInfo, internally routes to buffer or file.

**命令**: `./mvnw test -pl flink-runtime -Dtest=OutputWriterTest#testUnifiedWriteInterface -P java11-target -P java11`
**断言**: test pass, exit code 0

### [L1-测试] AT-7OWS Disk Pure Byte Stream

Spill files store raw bytes only. No metadata (record boundaries, channel context, DataType, etc.) on disk. All metadata lives in in-memory Queue<SpillEntry>. Replay reads 32KB chunks from spill file into Network Buffer, no record boundary awareness.

**命令**: `./mvnw test -pl flink-runtime -Dtest=SpillFileTest#testPureByteStream -P java11-target -P java11`
**断言**: test pass, exit code 0

### [L2-Agent] AT-1KTC Minimal Code Invasion

All new logic (OutputWriter, SpillFileWriter, SpillFileReader, SpillEntry) lives in new classes. Existing files only call writer.write(). No internal details leak into existing code.

**采集命令**: `grep -rn "SpillFile\|SpillEntry" --include="*.java" flink-runtime/src/main/java/org/apache/flink/runtime/checkpoint/channel/ | grep -v -E "(OutputWriter|SpillFile|SpillEntry)\.java:"`
**判定**: grep 结果应为空。若有输出，每一行必须仅为 import 语句，不允许出现方法调用、字段声明或类型引用
**客观证据**: grep 命令输出

### [L1-测试] AT-UFNZ Integration Test

UnalignedCheckpointRescaleITCase passes with all rescale scenarios.

**命令**: `./mvnw test -pl flink-tests -Dtest=UnalignedCheckpointRescaleITCase -P java11-target -P java11`
**断言**: test pass, exit code 0
