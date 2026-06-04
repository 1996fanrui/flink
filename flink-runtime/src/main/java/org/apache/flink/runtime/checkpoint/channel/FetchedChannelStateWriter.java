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
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

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
 * <p>One instance is created per recovery and discarded after {@link #close()}. It maintains an
 * open {@link BufferedOutputStream} to the current spill file and writes directly to it without
 * using network buffers.
 *
 * <h3>Disk format</h3>
 *
 * <pre>
 * [ 4B BE int: gate idx     ]   segment header, written once per channel segment
 * [ 4B BE int: channel idx  ]
 *   [ 4B BE int: record length ]  repeated for every record in this segment
 *   [ N bytes: serialized record ]
 * [ 4B BE int: gate idx     ]   next segment header (new channel or post-rotation)
 * ...
 * </pre>
 *
 * <h3>Segment and file boundaries</h3>
 *
 * <p>A segment is one uninterrupted stream of records for a single channel. A segment switch
 * happens when a different channel is addressed. File rotation happens only after a segment is
 * fully sealed (never mid-segment or mid-record), so each segment's bytes fit entirely within one
 * file. This keeps the segment locator {@code (fileIndex, offset, length)} unambiguous and avoids
 * cross-file reads.
 *
 * <p>Single-writer and intentionally unsynchronized; callers must serialize via the channel IO
 * executor.
 */
@Internal
public class FetchedChannelStateWriter implements Closeable {

    private static final int BUFFER_SIZE = 64 * 1024;

    private final FetchedChannelState channelState;

    private final Path baseDir;

    private final long maxFileSizeBytes;

    /** Output stream to the current spill file. Null before the first segment is opened. */
    private BufferedOutputStream currentStream;

    private DataOutputViewStreamWrapper dataView;

    /**
     * Index of the currently open file in {@link FetchedChannelState#files()}, or {@code -1} if no
     * file is open yet.
     */
    private int currentFileIndex = -1;

    /**
     * Total bytes written to the current file so far. Used to decide when to rotate. Rotation
     * happens only after a complete segment is sealed.
     */
    private long runningLength = 0L;

    /**
     * Channel whose segment is currently open. Null means no segment is in progress (beginning or
     * right after a channel seal).
     */
    private InputChannelInfo currentChannel;

    /**
     * Byte offset within the current file where the body of the current segment starts. Excludes
     * the 8-byte segment header (gateIdx + channelIdx).
     */
    private long currentSegmentBodyOffset;

    /**
     * Number of body bytes written for the current open segment. Used to build the {@link
     * FetchedSegment} entry when the segment is sealed.
     */
    private long currentSegmentBodyLength;

    private boolean closed = false;

    public FetchedChannelStateWriter(
            FetchedChannelState channelState, Path baseDir, long maxFileSizeBytes) {
        checkArgument(maxFileSizeBytes > 0, "maxFileSizeBytes must be positive");
        this.channelState = checkNotNull(channelState);
        this.baseDir = checkNotNull(baseDir);
        this.maxFileSizeBytes = maxFileSizeBytes;
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
     * <p>The caller is responsible for serializing the record into {@code serializedRecord} and
     * providing its length. This method writes the 4-byte length prefix followed by the record
     * bytes directly to the underlying stream. A segment header is emitted if this is the first
     * write for this channel or if the channel has changed from the previous call.
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

        dataView.writeInt(recordLength);
        dataView.write(serializedRecord, 0, recordLength);
        currentSegmentBodyLength += Integer.BYTES + recordLength;
        runningLength += Integer.BYTES + recordLength;
    }

    /**
     * Writes a raw byte array for the given channel using the pass-through path.
     *
     * <p>The bytes are written verbatim (without any framing). A segment header is emitted if this
     * is the first write for this channel or the channel has changed.
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

        dataView.write(data, offset, length);
        currentSegmentBodyLength += length;
        runningLength += length;
    }

    /**
     * Returns the underlying {@link DataOutputViewStreamWrapper} for direct serialization.
     *
     * <p>Used by the filtering handler to serialize directly into the stream. The caller must call
     * {@link #notifyBytesWritten} after writing to keep internal length tracking consistent.
     * Channel switching and segment header emission are handled by {@link
     * #prepareForChannel(InputChannelInfo)} which must be called before using the view.
     */
    DataOutputViewStreamWrapper getDataView() {
        return dataView;
    }

    /**
     * Ensures the writer is ready to accept data for {@code channelInfo}: opens a file if none is
     * open, emits a segment header on channel switch, and returns the current data view.
     *
     * <p>Must be called before any direct writes via {@link #getDataView()}.
     */
    DataOutputViewStreamWrapper prepareForChannel(InputChannelInfo channelInfo) throws IOException {
        checkNotNull(channelInfo);
        checkState(!closed, "Writer is closed");
        switchChannelIfNeeded(channelInfo);
        return dataView;
    }

    /**
     * Records that {@code bytes} bytes have been written directly through {@link #getDataView()}.
     *
     * <p>Must be called after each direct-view write to keep {@link #currentSegmentBodyLength} and
     * {@link #runningLength} consistent.
     */
    void notifyBytesWritten(long bytes) {
        currentSegmentBodyLength += bytes;
        runningLength += bytes;
    }

    // -------------------------------------------------------------------------------------------
    // Internal helpers
    // -------------------------------------------------------------------------------------------

    private void switchChannelIfNeeded(InputChannelInfo channelInfo) throws IOException {
        if (currentChannel != null && currentChannel.equals(channelInfo)) {
            // Same channel: no switch needed.
            return;
        }
        if (currentChannel != null) {
            // Seal the current segment before switching.
            sealCurrentSegment();
        }
        openSegmentForChannel(channelInfo);
    }

    private void openSegmentForChannel(InputChannelInfo channelInfo) throws IOException {
        ensureFileOpen();

        // Write segment header: gateIdx (4B) + channelIdx (4B).
        dataView.writeInt(channelInfo.getGateIdx());
        dataView.writeInt(channelInfo.getInputChannelIdx());
        runningLength += 2 * Integer.BYTES;

        // The body starts immediately after the 8-byte header.
        currentSegmentBodyOffset = runningLength;
        currentSegmentBodyLength = 0L;
        currentChannel = channelInfo;
    }

    /**
     * Seals the current segment by appending its locator to the channel-state table. Then checks if
     * file rotation is due (only after a complete segment, never mid-segment).
     */
    private void sealCurrentSegment() throws IOException {
        if (currentChannel == null) {
            return;
        }
        // Body offset within the file is the start of body bytes, not the header start.
        // The header is 8B; body starts at (start-of-header + 8B).
        long headerStart = currentSegmentBodyOffset - 2 * Integer.BYTES;
        long bodyOffsetInFile = headerStart + 2 * Integer.BYTES;

        channelState.appendSegment(
                new FetchedSegment(
                        currentChannel,
                        currentFileIndex,
                        bodyOffsetInFile,
                        currentSegmentBodyLength));
        currentChannel = null;

        // Rotation is only checked after a complete segment is sealed (never mid-segment).
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
        currentStream = new BufferedOutputStream(fos, BUFFER_SIZE);
        dataView = new DataOutputViewStreamWrapper(currentStream);
        currentFileIndex = channelState.addFile(filePath);
        runningLength = 0L;
    }

    private void rotateFile() throws IOException {
        // Flush and close the current stream before opening a new file.
        currentStream.flush();
        currentStream.close();
        currentStream = null;
        dataView = null;
        currentFileIndex = -1;
        runningLength = 0L;
        // Next write will open a new file via ensureFileOpen().
    }

    // -------------------------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------------------------

    /**
     * Seals the current open segment (if any), flushes, and closes the current file stream.
     *
     * <p>After this call the associated {@link FetchedChannelState}'s segment and file lists are
     * complete and ready for reading.
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
            dataView = null;
        }
    }

    public FetchedChannelState getChannelState() {
        return channelState;
    }
}
