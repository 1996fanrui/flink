/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.checkpoint.channel;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.memory.DataOutputSerializer;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Writes recovered channel-state data to spill files in a segment-per-channel format.
 *
 * <p>One instance is created per recovery and discarded after {@link #close()}. Segment boundaries
 * are self-described in disk segment headers; no in-memory segment locator table is maintained.
 *
 * <h3>Disk format</h3>
 *
 * <pre>
 * [ 4B BE int: gate idx      ]   segment header: written once per channel segment
 * [ 4B BE int: channel idx   ]
 * [ 4B BE int: buffer length ]   segment body byte count (backfilled at segment seal)
 *   [ 4B BE int: record length ]  repeated for every record in this segment
 *   [ N bytes: serialized record ]
 * [ 4B BE int: gate idx      ]   next segment header (channel switch or post-rotation)
 * ...
 * </pre>
 *
 * <h3>Segment buffering and bufferLength backfill</h3>
 *
 * <p>The segment body byte count is only known after the entire segment is written, so each segment
 * is first accumulated in an in-memory {@link DataOutputSerializer}. The 12-byte header is written
 * at segment open with a zero placeholder for {@code bufferLength}; at seal time {@link
 * DataOutputSerializer#writeIntUnsafe} backfills the actual body length at offset 8, then the
 * entire buffer is flushed to the file stream in one shot.
 *
 * <h3>Segment and file boundaries</h3>
 *
 * <p>A segment is one uninterrupted stream of records for a single channel. A segment switch
 * happens when a different channel is addressed. File rotation happens only after a segment is
 * fully sealed (never mid-segment or mid-record), so each segment's bytes fit entirely within one
 * file, and {@code body()} in the reader never crosses a file boundary.
 *
 * <p>Single-writer and intentionally unsynchronized; callers must serialize via the channel IO
 * executor.
 */
@Internal
public class FetchedChannelStateWriter implements Closeable {

    /** Byte offset of the {@code bufferLength} field within a segment's header. */
    static final int BUFFER_LENGTH_HEADER_OFFSET = 2 * Integer.BYTES;

    /** Total size of the segment header in bytes: gateIdx + channelIdx + bufferLength. */
    static final int SEGMENT_HEADER_BYTES = 3 * Integer.BYTES;

    private static final int STREAM_BUFFER_SIZE = 64 * 1024;

    private final FetchedChannelState channelState;
    private final Path baseDir;
    private final long maxFileSizeBytes;

    /** Output stream to the current spill file. Null before the first segment is opened. */
    private BufferedOutputStream currentStream;

    /**
     * In-memory accumulation buffer for the current segment. The 12-byte header occupies offsets
     * [0, 12); the body follows from offset 12 onward. At segment seal time, {@code
     * writeIntUnsafe(bodyLength, BUFFER_LENGTH_HEADER_OFFSET)} backfills the body length before
     * flushing to the file stream.
     */
    private DataOutputSerializer segmentBuffer;

    /**
     * Channel whose segment is currently open. Null means no segment is in progress (beginning or
     * right after a channel seal).
     */
    private InputChannelInfo currentChannel;

    /**
     * Total bytes flushed to the current file so far (includes sealed segments only). Used to
     * decide when to rotate after sealing a segment.
     */
    private long runningLength = 0L;

    private boolean closed = false;

    public FetchedChannelStateWriter(
            FetchedChannelState channelState, Path baseDir, long maxFileSizeBytes) {
        checkArgument(maxFileSizeBytes > 0, "maxFileSizeBytes must be positive");
        this.channelState = checkNotNull(channelState);
        this.baseDir = checkNotNull(baseDir);
        this.maxFileSizeBytes = maxFileSizeBytes;
        // Initial capacity covers a typical small segment; grows as needed.
        this.segmentBuffer = new DataOutputSerializer(256);
    }

    public FetchedChannelStateWriter(FetchedChannelState channelState, Path baseDir) {
        this(channelState, baseDir, FetchedChannelState.DEFAULT_SEGMENT_SIZE_BYTES);
    }

    // -------------------------------------------------------------------------------------------
    // Write API
    // -------------------------------------------------------------------------------------------

    /**
     * Writes a single serialized record for the given channel using the filtering path.
     *
     * <p>Writes a 4-byte length prefix followed by the record bytes into the current segment
     * buffer. A new segment is opened if this is the first write or if the channel has changed.
     *
     * @param channelInfo the target channel for this record
     * @param serializedRecord the serialized record bytes
     * @param recordLength number of valid bytes in {@code serializedRecord}
     */
    public void writeRecord(InputChannelInfo channelInfo, byte[] serializedRecord, int recordLength)
            throws IOException {
        checkNotNull(channelInfo);
        checkArgument(recordLength >= 0, "recordLength must be non-negative");
        checkState(!closed, "Writer is closed");

        switchChannelIfNeeded(channelInfo);

        segmentBuffer.writeInt(recordLength);
        segmentBuffer.write(serializedRecord, 0, recordLength);
    }

    /**
     * Writes raw bytes for the given channel using the pass-through path.
     *
     * <p>The bytes are written verbatim (without additional framing) into the current segment
     * buffer. A new segment is opened if this is the first write or if the channel has changed.
     * Consecutive pass-through calls for the same channel are merged into a single segment.
     *
     * @param channelInfo the target channel
     * @param data the raw bytes to write
     * @param offset start offset in {@code data}
     * @param length number of bytes to write
     */
    public void writePassThrough(InputChannelInfo channelInfo, byte[] data, int offset, int length)
            throws IOException {
        checkNotNull(channelInfo);
        checkArgument(length >= 0, "length must be non-negative");
        checkState(!closed, "Writer is closed");

        switchChannelIfNeeded(channelInfo);

        segmentBuffer.write(data, offset, length);
    }

    // -------------------------------------------------------------------------------------------
    // Internal helpers
    // -------------------------------------------------------------------------------------------

    private void switchChannelIfNeeded(InputChannelInfo channelInfo) throws IOException {
        if (currentChannel != null && currentChannel.equals(channelInfo)) {
            // Same channel: keep accumulating into the current segment.
            return;
        }
        if (currentChannel != null) {
            // Different channel: seal the open segment before starting a new one.
            sealCurrentSegment();
        }
        openSegmentForChannel(channelInfo);
    }

    private void openSegmentForChannel(InputChannelInfo channelInfo) throws IOException {
        ensureFileOpen();
        segmentBuffer.clear();
        // Write segment header: gateIdx (4B) + channelIdx (4B) + bufferLength placeholder (4B).
        // bufferLength is backfilled at seal time via writeIntUnsafe at
        // BUFFER_LENGTH_HEADER_OFFSET.
        segmentBuffer.writeInt(channelInfo.getGateIdx());
        segmentBuffer.writeInt(channelInfo.getInputChannelIdx());
        segmentBuffer.writeInt(0); // placeholder; backfilled in sealCurrentSegment()
        currentChannel = channelInfo;
    }

    /**
     * Seals the current segment: backfills the body length into the header, flushes the entire
     * segment buffer to the file stream, then checks whether file rotation is due.
     */
    private void sealCurrentSegment() throws IOException {
        if (currentChannel == null) {
            return;
        }
        int totalBytes = segmentBuffer.length();
        int bodyBytes = totalBytes - SEGMENT_HEADER_BYTES;
        // Backfill bufferLength field at fixed offset 8 in the segment header.
        // Math.toIntExact guards against the unlikely case of a single segment > 2 GB.
        segmentBuffer.writeIntUnsafe(Math.toIntExact(bodyBytes), BUFFER_LENGTH_HEADER_OFFSET);

        // Flush the complete segment (header + body) to the file stream in one shot.
        byte[] raw = segmentBuffer.getSharedBuffer();
        currentStream.write(raw, 0, totalBytes);
        runningLength += totalBytes;

        currentChannel = null;

        // File rotation is checked only after a complete segment is sealed, never mid-segment.
        if (runningLength >= maxFileSizeBytes) {
            rotateFile();
        }
    }

    private void ensureFileOpen() throws IOException {
        if (currentStream != null) {
            return;
        }
        openNewFile();
    }

    private void openNewFile() throws IOException {
        Files.createDirectories(baseDir);
        int index = channelState.files().size();
        Path filePath = baseDir.resolve("spill-segment-" + index + ".bin");
        FileOutputStream fos = new FileOutputStream(filePath.toFile());
        currentStream = new BufferedOutputStream(fos, STREAM_BUFFER_SIZE);
        channelState.addFile(filePath);
        runningLength = 0L;
    }

    private void rotateFile() throws IOException {
        currentStream.flush();
        currentStream.close();
        currentStream = null;
        runningLength = 0L;
        // The next write will open a new file via ensureFileOpen().
    }

    // -------------------------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------------------------

    /**
     * Seals the current open segment (if any), flushes, and closes the current file stream.
     *
     * <p>After this call the associated {@link FetchedChannelState}'s file list is complete and
     * ready for reading.
     */
    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        if (currentChannel != null) {
            sealCurrentSegment();
        }
        if (currentStream != null) {
            currentStream.flush();
            currentStream.close();
            currentStream = null;
        }
    }

    public FetchedChannelState getChannelState() {
        return channelState;
    }
}
