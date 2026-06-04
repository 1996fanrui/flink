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
import org.apache.flink.util.CloseableIterator;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.NoSuchElementException;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * Forward reader over a {@link FetchedChannelState}'s spill files.
 *
 * <p>Iterates per-channel segments following the in-memory segment locator table built by the
 * writer. Each segment's bytes are read from disk on demand; no segment body data is held in memory
 * at any time. The reader owns one lifecycle grant ({@link FetchedChannelState#acquire()}) and
 * releases it on {@link #close()}.
 *
 * <p>The root reader is driven by the drain thread. Snapshot readers (created via {@link
 * #snapshot()}) are independent, single-consumer, and own their own lifecycle grant.
 *
 * <p>Thread-safety: the root reader's {@link #snapshot()} and the drain cursor advancement via
 * {@link FetchedSegmentCursor#commitConsumed()} must be called under the drainer lock. Disk reads
 * happen outside that lock.
 */
@Internal
public final class FetchedChannelStateReader implements Closeable {

    private final FetchedChannelState channelState;
    private final List<Path> files;
    private final List<FetchedSegment> segments;

    /**
     * Index of the segment currently being iterated (or about to be iterated). Advances to the next
     * segment when the current cursor's body is fully consumed.
     */
    private int segmentCursor;

    /**
     * Byte offset within the body of the current segment that has been committed (delivered under
     * lock). Starts at 0 for each segment; advances by the number of committed bytes on each {@link
     * FetchedSegmentCursor#commitConsumed()} call. Used by {@link #snapshot()} to derive the
     * correct start position within a partially drained segment.
     */
    private long committedBytesInSegment;

    @Nullable private FileChannel activeFileChannel;
    private int activeFileIndex = -1;

    private boolean closed;

    private FetchedChannelStateReader(
            FetchedChannelState channelState, int segmentCursor, long committedBytesInSegment) {
        this.channelState = channelState;
        this.files = channelState.files();
        this.segments = channelState.segments();
        this.segmentCursor = segmentCursor;
        this.committedBytesInSegment = committedBytesInSegment;
        channelState.acquire();
    }

    /**
     * Opens a root reader covering all segments from the beginning.
     *
     * @param state a sealed (writer-closed) {@link FetchedChannelState}
     * @return a new root reader; caller must {@link #close()} it when done
     */
    public static FetchedChannelStateReader openRoot(FetchedChannelState state) {
        return new FetchedChannelStateReader(state, 0, 0L);
    }

    /**
     * Returns an iterator over all remaining segments, starting from the current cursor position.
     * Each {@link FetchedSegmentCursor} exposes the channel info, a bounded body stream, and the
     * commit primitive.
     *
     * <p>The returned iterator is single-use and must be closed when done.
     */
    public CloseableIterator<FetchedSegmentCursor> segments() {
        checkState(!closed, "FetchedChannelStateReader is closed");
        return new SegmentIterator();
    }

    /**
     * Derives an independent reader starting from the current drain position. The snapshot reader
     * owns its own {@link FetchedChannelState} lifecycle grant and has an independent file channel.
     *
     * <p>Must be called under the drainer lock so that {@link #segmentCursor} and {@link
     * #committedBytesInSegment} reflect the latest committed state.
     *
     * @return a new independent reader; caller must {@link #close()} it when done
     */
    public FetchedChannelStateReader snapshot() {
        checkState(!closed, "FetchedChannelStateReader is closed");
        return new FetchedChannelStateReader(channelState, segmentCursor, committedBytesInSegment);
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        try {
            closeActiveChannel();
        } finally {
            channelState.release();
        }
    }

    // -------------------------------------------------------------------------------------------
    // Internal helpers
    // -------------------------------------------------------------------------------------------

    /**
     * Opens (or reuses) the {@link FileChannel} for the given file index. Closes the currently open
     * channel first if a different file is needed. File transitions are transparent to callers.
     */
    private FileChannel channelFor(int fileIndex) throws IOException {
        if (activeFileIndex != fileIndex) {
            closeActiveChannel();
            Path path = files.get(fileIndex);
            activeFileChannel = FileChannel.open(path, StandardOpenOption.READ);
            activeFileIndex = fileIndex;
        }
        return activeFileChannel;
    }

    private void closeActiveChannel() throws IOException {
        if (activeFileChannel != null) {
            activeFileChannel.close();
            activeFileChannel = null;
            activeFileIndex = -1;
        }
    }

    // -------------------------------------------------------------------------------------------
    // SegmentIterator
    // -------------------------------------------------------------------------------------------

    private final class SegmentIterator implements CloseableIterator<FetchedSegmentCursor> {

        /**
         * Index into {@code segments} for the next cursor to return. Initialized from the outer
         * reader's {@code segmentCursor} so snapshots start at the correct position.
         */
        private int iterPos = segmentCursor;

        /**
         * Offset within the current segment's body from which reading should start. For the first
         * segment of a snapshot reader, this equals {@code committedBytesInSegment}. For all
         * subsequent segments it is 0.
         */
        private long startOffsetInSegment = committedBytesInSegment;

        /** Whether the body of the cursor at {@code iterPos - 1} has been fully consumed. */
        private boolean currentBodyConsumed = true;

        private boolean iterClosed;

        @Override
        public boolean hasNext() {
            checkState(!iterClosed, "SegmentIterator is closed");
            // A segment is "remaining" if there are still segments beyond the current position,
            // or if the current segment has bytes left after the start offset.
            if (iterPos >= segments.size()) {
                return false;
            }
            FetchedSegment seg = segments.get(iterPos);
            return startOffsetInSegment < seg.length;
        }

        @Override
        public FetchedSegmentCursor next() throws RuntimeException {
            checkState(!iterClosed, "SegmentIterator is closed");
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            FetchedSegment seg = segments.get(iterPos);
            long bodyOffset = seg.offset + startOffsetInSegment;
            long remainingLength = seg.length - startOffsetInSegment;

            FileChannel fc;
            try {
                fc = channelFor(seg.fileIndex);
            } catch (IOException e) {
                throw new RuntimeException("Failed to open spill file for segment " + seg, e);
            }

            BoundedSegmentStream bodyStream =
                    new BoundedSegmentStream(fc, bodyOffset, remainingLength, seg);

            int myIterPos = iterPos;
            long myStartOffset = startOffsetInSegment;
            currentBodyConsumed = false;

            // After producing this cursor, advance to the next segment (start offset resets to 0).
            iterPos++;
            startOffsetInSegment = 0L;

            return new FetchedSegmentCursor() {
                @Override
                public InputChannelInfo channelInfo() {
                    return seg.channelInfo;
                }

                @Override
                public InputStream body() {
                    return bodyStream;
                }

                @Override
                public long length() {
                    return remainingLength;
                }

                @Override
                public void commitConsumed() {
                    // committedBytesInSegment in the outer reader tracks how many bytes of the
                    // segment at segmentCursor have been durably delivered. Once a segment is fully
                    // committed, advance segmentCursor and reset the byte counter.
                    long consumed = myStartOffset + bodyStream.bytesRead();
                    if (consumed >= seg.length) {
                        // Entire segment consumed; move the outer cursor to the next segment.
                        segmentCursor = myIterPos + 1;
                        committedBytesInSegment = 0L;
                    } else {
                        segmentCursor = myIterPos;
                        committedBytesInSegment = consumed;
                    }
                    currentBodyConsumed = true;
                }
            };
        }

        @Override
        public void close() {
            iterClosed = true;
        }
    }

    // -------------------------------------------------------------------------------------------
    // BoundedSegmentStream
    // -------------------------------------------------------------------------------------------

    /**
     * An {@link InputStream} bounded to a specific byte range {@code [offset, offset + length)} in
     * a {@link FileChannel}. Reads return EOF after {@code length} bytes. If the underlying file
     * ends before {@code length} bytes are available, an {@link EOFException} is thrown
     * (fail-loud).
     */
    private static final class BoundedSegmentStream extends InputStream {

        private final FileChannel fc;
        private final long startOffset;
        private final long length;
        private final FetchedSegment seg;

        private long position;
        private long bytesRead;

        /** 8 KB scratch buffer for bulk reads. */
        private static final int SCRATCH_SIZE = 8 * 1024;

        private final InputStream channelStream;

        BoundedSegmentStream(FileChannel fc, long startOffset, long length, FetchedSegment seg)
                throws RuntimeException {
            this.fc = fc;
            this.startOffset = startOffset;
            this.length = length;
            this.seg = seg;
            this.position = 0;
            this.bytesRead = 0;
            // Position the channel at the start of this segment body slice.
            try {
                fc.position(startOffset);
            } catch (IOException e) {
                throw new RuntimeException(
                        "Failed to seek to offset " + startOffset + " in segment " + seg, e);
            }
            this.channelStream = Channels.newInputStream(fc);
        }

        @Override
        public int read() throws IOException {
            if (position >= length) {
                return -1;
            }
            int b = channelStream.read();
            if (b < 0) {
                throw new EOFException(
                        "Unexpected EOF in segment "
                                + seg
                                + " after "
                                + bytesRead
                                + "/"
                                + length
                                + " bytes");
            }
            position++;
            bytesRead++;
            return b;
        }

        @Override
        public int read(byte[] buf, int off, int len) throws IOException {
            if (position >= length) {
                return -1;
            }
            int toRead = (int) Math.min(len, length - position);
            int n = channelStream.read(buf, off, toRead);
            if (n < 0) {
                throw new EOFException(
                        "Unexpected EOF in segment "
                                + seg
                                + " after "
                                + bytesRead
                                + "/"
                                + length
                                + " bytes");
            }
            position += n;
            bytesRead += n;
            return n;
        }

        /** Returns the number of bytes read from this stream so far. */
        long bytesRead() {
            return bytesRead;
        }

        @Override
        public void close() {
            // Do not close the underlying FileChannel; it is managed by the outer reader.
        }
    }
}
