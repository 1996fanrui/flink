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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.NoSuchElementException;

import static org.apache.flink.runtime.checkpoint.channel.FetchedChannelStateWriter.SEGMENT_HEADER_BYTES;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Forward reader over a {@link FetchedChannelState}'s spill files.
 *
 * <p>Iterates per-channel segments by scanning the files sequentially. Each segment header ([4B
 * gateIdx][4B channelIdx][4B bufferLength]) is read from disk on demand; no segment body data or
 * in-memory segment locator table is held. The reader owns one lifecycle grant ({@link
 * FetchedChannelState#acquire()}) and releases it on {@link #close()}.
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

    /**
     * Index of the file currently being (or about to be) read. Advances when a file is exhausted.
     */
    private int fileIndex;

    /**
     * Byte offset within the current file for the next read. Points to the start of the next
     * segment header (or past EOF when the file is exhausted). Updated after every segment seal.
     */
    private long fileOffset;

    /**
     * Number of bytes within the body of the current segment that have been committed (delivered
     * under lock). Starts at 0 for each new segment; advances on each {@link
     * FetchedSegmentCursor#commitConsumed()} call. Used by {@link #snapshot()} to derive the
     * correct start position within a partially drained segment.
     */
    private long committedBytesInSegment;

    @Nullable private FileChannel activeFileChannel;
    private int activeFileIndex = -1;

    private boolean closed;

    private FetchedChannelStateReader(
            FetchedChannelState channelState,
            int fileIndex,
            long fileOffset,
            long committedBytesInSegment) {
        this.channelState = channelState;
        this.files = channelState.files();
        this.fileIndex = fileIndex;
        this.fileOffset = fileOffset;
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
        return new FetchedChannelStateReader(state, 0, 0L, 0L);
    }

    /**
     * Returns an iterator over all remaining segments, starting from the current cursor position.
     * Each {@link FetchedSegmentCursor} exposes the channel info, a bounded body stream, and the
     * commit primitive.
     *
     * <p>Segments are discovered by reading 12-byte headers sequentially from disk; no in-memory
     * segment locator table is used. The returned iterator is single-use and must be closed when
     * done.
     */
    public CloseableIterator<FetchedSegmentCursor> segments() {
        checkState(!closed, "FetchedChannelStateReader is closed");
        return new SegmentIterator();
    }

    /**
     * Derives an independent reader starting from the current drain position. The snapshot reader
     * owns its own {@link FetchedChannelState} lifecycle grant and has an independent file channel.
     *
     * <p>Must be called under the drainer lock so that {@link #fileIndex}, {@link #fileOffset}, and
     * {@link #committedBytesInSegment} reflect the latest committed state.
     *
     * @return a new independent reader; caller must {@link #close()} it when done
     */
    public FetchedChannelStateReader snapshot() {
        checkState(!closed, "FetchedChannelStateReader is closed");
        return new FetchedChannelStateReader(
                channelState, fileIndex, fileOffset, committedBytesInSegment);
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
     * channel first if a different file is needed.
     */
    private FileChannel channelFor(int fileIdx) throws IOException {
        if (activeFileIndex != fileIdx) {
            closeActiveChannel();
            Path path = files.get(fileIdx);
            activeFileChannel = FileChannel.open(path, StandardOpenOption.READ);
            activeFileIndex = fileIdx;
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

    /**
     * Iterates per-channel segments by reading 12-byte headers sequentially from the spill files.
     *
     * <p>State machine: the iterator tracks which file and offset the next segment header lives at.
     * When a file is exhausted it advances to the next file. Snapshot readers start from a
     * partially-consumed segment: the first segment's body is trimmed by {@code
     * committedBytesInSegment}.
     */
    private final class SegmentIterator implements CloseableIterator<FetchedSegmentCursor> {

        /** File index for the next segment header to read. */
        private int iterFileIndex = fileIndex;

        /** Byte offset in {@code iterFileIndex} for the next segment header. */
        private long iterFileOffset = fileOffset;

        /**
         * For the first segment (snapshot resume), this many bytes have already been committed and
         * should be skipped; for all subsequent segments it is 0.
         */
        private long startOffsetInSegment = committedBytesInSegment;

        /**
         * Header parsed ahead of time: populated by {@link #tryReadNextHeader()} and consumed by
         * {@link #next()}. Null when we have not yet peeked the next header.
         */
        @Nullable private ParsedHeader peekedHeader;

        /** Whether we have already tried reading the next header (and possibly found EOF). */
        private boolean headerPeeked;

        private boolean iterClosed;

        @Override
        public boolean hasNext() {
            checkState(!iterClosed, "SegmentIterator is closed");
            if (!headerPeeked) {
                peekedHeader = tryReadNextHeader();
                headerPeeked = true;
            }
            if (peekedHeader == null) {
                return false;
            }
            // Skip segments where the committed offset equals the body length (fully consumed).
            return startOffsetInSegment < peekedHeader.bufferLength;
        }

        @Override
        public FetchedSegmentCursor next() {
            checkState(!iterClosed, "SegmentIterator is closed");
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            ParsedHeader header = peekedHeader;
            peekedHeader = null;
            headerPeeked = false;

            long myFileIndex = header.fileIndex;
            long bodyStartInFile = header.bodyStartOffset;
            long remainingLength = header.bufferLength - startOffsetInSegment;
            long bodyReadStart = bodyStartInFile + startOffsetInSegment;

            FileChannel fc;
            try {
                fc = channelFor((int) myFileIndex);
            } catch (IOException e) {
                throw new RuntimeException(
                        "Failed to open spill file " + myFileIndex + " at offset " + bodyReadStart,
                        e);
            }

            BoundedSegmentStream bodyStream =
                    new BoundedSegmentStream(fc, bodyReadStart, remainingLength, myFileIndex);

            // After this segment's body, the next header begins immediately after the full body.
            long nextOffset = bodyStartInFile + header.bufferLength;
            long nextFileIdx = myFileIndex;

            // Reset snapshot start offset; only the first next() call may use it.
            startOffsetInSegment = 0L;

            InputChannelInfo channelInfo = new InputChannelInfo(header.gateIdx, header.channelIdx);
            long fullSegmentLength = header.bufferLength;
            long myStartOffsetInSegment = header.bufferLength - remainingLength;

            return new FetchedSegmentCursor() {
                @Override
                public InputChannelInfo channelInfo() {
                    return channelInfo;
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
                    // current segment have been durably delivered. Once the segment is fully
                    // committed, advance the file cursor past this segment.
                    long consumed = myStartOffsetInSegment + bodyStream.bytesRead();
                    if (consumed >= fullSegmentLength) {
                        // Whole segment consumed: advance outer cursor past this segment's end.
                        fileIndex = (int) nextFileIdx;
                        fileOffset = nextOffset;
                        committedBytesInSegment = 0L;
                        // Also advance the iterator so the next hasNext() reads the right header.
                        iterFileIndex = (int) nextFileIdx;
                        iterFileOffset = nextOffset;
                    } else {
                        // Partial consume: outer cursor stays at this segment's header start but
                        // committedBytesInSegment records how far into the body we've delivered.
                        fileIndex = (int) myFileIndex;
                        fileOffset = bodyStartInFile - SEGMENT_HEADER_BYTES;
                        committedBytesInSegment = consumed;
                    }
                }
            };
        }

        @Override
        public void close() {
            iterClosed = true;
        }

        /**
         * Attempts to read the next 12-byte segment header from the current iterator position.
         * Advances to the next file when the current file is exhausted. Returns {@code null} when
         * all files have been read. Throws {@link IOException} wrapped in a {@link
         * RuntimeException} on partial header reads (truncated file).
         */
        @Nullable
        private ParsedHeader tryReadNextHeader() {
            while (iterFileIndex < files.size()) {
                try {
                    FileChannel fc = channelFor(iterFileIndex);
                    long fileSize = fc.size();
                    if (iterFileOffset >= fileSize) {
                        // This file is exhausted; move to the next one.
                        iterFileIndex++;
                        iterFileOffset = 0L;
                        continue;
                    }
                    // Read 12-byte header: [gateIdx][channelIdx][bufferLength]
                    ByteBuffer headerBuf = ByteBuffer.allocate(SEGMENT_HEADER_BYTES);
                    headerBuf.order(ByteOrder.BIG_ENDIAN);
                    fc.position(iterFileOffset);
                    int bytesRead = 0;
                    while (bytesRead < SEGMENT_HEADER_BYTES) {
                        int n = fc.read(headerBuf);
                        if (n < 0) {
                            break;
                        }
                        bytesRead += n;
                    }
                    if (bytesRead < SEGMENT_HEADER_BYTES) {
                        throw new RuntimeException(
                                new EOFException(
                                        "Truncated segment header in file "
                                                + files.get(iterFileIndex)
                                                + " at offset "
                                                + iterFileOffset
                                                + ": expected "
                                                + SEGMENT_HEADER_BYTES
                                                + " bytes, got "
                                                + bytesRead));
                    }
                    headerBuf.flip();
                    int gateIdx = headerBuf.getInt();
                    int channelIdx = headerBuf.getInt();
                    int bufferLength = headerBuf.getInt();

                    long bodyStartOffset = iterFileOffset + SEGMENT_HEADER_BYTES;
                    ParsedHeader header =
                            new ParsedHeader(
                                    iterFileIndex,
                                    iterFileOffset,
                                    bodyStartOffset,
                                    gateIdx,
                                    channelIdx,
                                    bufferLength);

                    // Advance iterator cursor past this segment for the next call.
                    iterFileOffset = bodyStartOffset + bufferLength;
                    return header;
                } catch (IOException e) {
                    throw new RuntimeException("Failed to read segment header", e);
                }
            }
            return null;
        }
    }

    // -------------------------------------------------------------------------------------------
    // ParsedHeader
    // -------------------------------------------------------------------------------------------

    private static final class ParsedHeader {
        final int fileIndex;

        /** Byte offset of the header start within the file. */
        final long headerOffset;

        /** Byte offset of the first body byte within the file (headerOffset + 12). */
        final long bodyStartOffset;

        final int gateIdx;
        final int channelIdx;
        final long bufferLength;

        ParsedHeader(
                int fileIndex,
                long headerOffset,
                long bodyStartOffset,
                int gateIdx,
                int channelIdx,
                long bufferLength) {
            this.fileIndex = fileIndex;
            this.headerOffset = headerOffset;
            this.bodyStartOffset = bodyStartOffset;
            this.gateIdx = gateIdx;
            this.channelIdx = channelIdx;
            this.bufferLength = bufferLength;
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
        private final long fileIndex;

        private long position;
        private long bytesRead;

        private final InputStream channelStream;

        BoundedSegmentStream(FileChannel fc, long startOffset, long length, long fileIndex) {
            this.fc = fc;
            this.startOffset = startOffset;
            this.length = length;
            this.fileIndex = fileIndex;
            this.position = 0;
            this.bytesRead = 0;
            try {
                fc.position(startOffset);
            } catch (IOException e) {
                throw new RuntimeException(
                        "Failed to seek to offset " + startOffset + " in file index " + fileIndex,
                        e);
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
                        "Unexpected EOF in segment body (file index="
                                + fileIndex
                                + ", startOffset="
                                + startOffset
                                + ") after "
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
                        "Unexpected EOF in segment body (file index="
                                + fileIndex
                                + ", startOffset="
                                + startOffset
                                + ") after "
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
