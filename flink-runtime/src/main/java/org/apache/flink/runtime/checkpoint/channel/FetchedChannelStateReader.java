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
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.List;
import java.util.NoSuchElementException;

import static org.apache.flink.runtime.checkpoint.channel.AbstractSpillingHandler.SEGMENT_HEADER_BYTES;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Forward reader over a {@link FetchedChannelState}'s spill files.
 *
 * <p>Reading is strictly sequential by design: a reader is positioned once at construction (offset
 * 0 for the root reader, or the current drain position for a {@link #snapshot()}), then consumes
 * forward only. It never seeks backward and never re-positions mid-iteration. Multiple files are
 * read in order: when one file is exhausted, the next is opened and read from its start. This
 * mirrors the only two access patterns: the drain thread reads the root reader front to back, and
 * each checkpoint derives a fresh snapshot reader that resumes from the drain position and reads
 * forward.
 *
 * <p>Because access is sequential, the body of one segment must be fully consumed before the next
 * segment is read; skipping ahead or rewinding is a contract violation and fails loud.
 *
 * <p>The reader owns one lifecycle grant ({@link FetchedChannelState#acquire()}) and releases it on
 * {@link #close()}.
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
     * File index of the segment at the current drain cursor. Together with {@link #fileOffset} and
     * {@link #committedBytesInSegment} this captures exactly where a {@link #snapshot()} resumes.
     */
    private int fileIndex;

    /** Byte offset within {@link #fileIndex} of the current segment's header. */
    private long fileOffset;

    /**
     * Number of bytes within the body of the current segment that have been committed (delivered
     * under lock). Starts at 0 for each new segment; advances on each {@link
     * FetchedSegmentCursor#commitConsumed()} call. Used to resume a {@link #snapshot()} in the
     * middle of a partially drained segment.
     */
    private long committedBytesInSegment;

    private boolean closed;

    FetchedChannelStateReader(
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
     * Returns an iterator over all remaining segments, starting from the current cursor position.
     * Each {@link FetchedSegmentCursor} exposes the channel info, a bounded body stream, and the
     * commit primitive.
     *
     * <p>The iterator reads files sequentially from the cursor position forward and is single-use;
     * it must be closed when done.
     */
    public CloseableIterator<FetchedSegmentCursor> segments() {
        checkState(!closed, "FetchedChannelStateReader is closed");
        return new SegmentIterator();
    }

    /**
     * Derives an independent reader starting from the current drain position. The snapshot reader
     * owns its own {@link FetchedChannelState} lifecycle grant and its own sequential stream.
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
        channelState.release();
    }

    // -------------------------------------------------------------------------------------------
    // SegmentIterator
    // -------------------------------------------------------------------------------------------

    /**
     * Iterates per-channel segments over a single forward stream that spans the spill files in
     * order. The stream is opened once, positioned at the reader's cursor, and consumed strictly
     * forward: each segment's 12-byte header is read, then its body is exposed as a bounded view of
     * the same stream. Advancing requires the previous body to be fully consumed and committed.
     */
    private final class SegmentIterator implements CloseableIterator<FetchedSegmentCursor> {

        /** Single forward stream spanning files [fileIndex .. last], opened at the cursor. */
        private final SequentialSpillStream stream;

        /**
         * For the first segment (snapshot resume), this many body bytes were already committed and
         * must be skipped; 0 for every subsequent segment.
         */
        private int pendingStartOffsetInSegment;

        /** Bounded body view of the current segment; must be exhausted before advancing. */
        @Nullable private BoundedSegmentStream currentBody;

        private boolean iterClosed;

        SegmentIterator() {
            this.stream =
                    new SequentialSpillStream(
                            files, fileIndex, fileOffset + committedBytesInSegment);
            // committedBytesInSegment is folded into the stream position above so reads start at
            // the
            // first undelivered byte; it is also the in-segment skip for the resumed first segment.
            this.pendingStartOffsetInSegment = (int) committedBytesInSegment;
        }

        @Override
        public boolean hasNext() {
            checkState(!iterClosed, "SegmentIterator is closed");
            ensurePreviousBodyConsumed();
            return stream.hasRemaining();
        }

        @Override
        public FetchedSegmentCursor next() {
            checkState(!iterClosed, "SegmentIterator is closed");
            ensurePreviousBodyConsumed();
            if (!stream.hasRemaining()) {
                throw new NoSuchElementException();
            }

            ParsedHeader header;
            try {
                header = stream.readHeader();
            } catch (IOException e) {
                throw new RuntimeException("Failed to read segment header", e);
            }

            // Resume skip applies only to the first segment of a snapshot reader.
            int skip = pendingStartOffsetInSegment;
            pendingStartOffsetInSegment = 0;

            int remainingLength = header.bufferLength - skip;
            checkState(
                    remainingLength >= 0,
                    "Resume offset %s exceeds segment length %s",
                    skip,
                    header.bufferLength);

            currentBody = new BoundedSegmentStream(stream, remainingLength);

            InputChannelInfo channelInfo = new InputChannelInfo(header.gateIdx, header.channelIdx);
            int fullSegmentLength = header.bufferLength;
            long headerStartInFile = header.headerOffset;
            int headerFileIndex = header.fileIndex;
            int alreadyConsumedInSegment = skip;
            BoundedSegmentStream body = currentBody;

            return new FetchedSegmentCursor() {
                @Override
                public InputChannelInfo channelInfo() {
                    return channelInfo;
                }

                @Override
                public InputStream body() {
                    return body;
                }

                @Override
                public int length() {
                    return remainingLength;
                }

                @Override
                public void commitConsumed() {
                    int consumed = alreadyConsumedInSegment + body.bytesRead();
                    if (consumed >= fullSegmentLength) {
                        // Whole segment consumed: cursor moves to the next segment's header.
                        fileIndex = header.nextFileIndex;
                        fileOffset = header.nextHeaderOffset;
                        committedBytesInSegment = 0L;
                    } else {
                        // Partial consume: cursor stays at this segment's header, recording how far
                        // into the body we have durably delivered.
                        fileIndex = headerFileIndex;
                        fileOffset = headerStartInFile;
                        committedBytesInSegment = consumed;
                    }
                }
            };
        }

        /**
         * Enforces the sequential contract: the previously returned segment body must be fully read
         * before the iterator can peek or advance. Skipping ahead with an unconsumed body is a
         * caller bug.
         */
        private void ensurePreviousBodyConsumed() {
            if (currentBody != null) {
                checkState(
                        currentBody.remaining() == 0,
                        "Previous segment body not fully consumed before advancing: %s bytes left",
                        currentBody.remaining());
                currentBody = null;
            }
        }

        @Override
        public void close() {
            iterClosed = true;
            try {
                stream.close();
            } catch (IOException e) {
                throw new RuntimeException("Failed to close spill stream", e);
            }
        }
    }

    // -------------------------------------------------------------------------------------------
    // ParsedHeader
    // -------------------------------------------------------------------------------------------

    private static final class ParsedHeader {
        /** File index this header was read from. */
        final int fileIndex;

        /** Byte offset of the header start within {@link #fileIndex}. */
        final long headerOffset;

        final int gateIdx;
        final int channelIdx;

        /** Segment body length in bytes; persisted on disk as a 4-byte int. */
        final int bufferLength;

        /** File index where the next segment header starts. */
        final int nextFileIndex;

        /** Byte offset of the next segment header within {@link #nextFileIndex}. */
        final long nextHeaderOffset;

        ParsedHeader(
                int fileIndex,
                long headerOffset,
                int gateIdx,
                int channelIdx,
                int bufferLength,
                int nextFileIndex,
                long nextHeaderOffset) {
            this.fileIndex = fileIndex;
            this.headerOffset = headerOffset;
            this.gateIdx = gateIdx;
            this.channelIdx = channelIdx;
            this.bufferLength = bufferLength;
            this.nextFileIndex = nextFileIndex;
            this.nextHeaderOffset = nextHeaderOffset;
        }
    }

    // -------------------------------------------------------------------------------------------
    // SequentialSpillStream
    // -------------------------------------------------------------------------------------------

    /**
     * A single forward {@link InputStream} over the spill files {@code [startFileIndex .. last]},
     * read in order. One file is open at a time; when it is exhausted the next file is opened and
     * read from its start. The stream never seeks backward after the initial positioning.
     *
     * <p>It tracks, for each byte produced, which file and in-file offset it came from, so the
     * reader can record exact resume coordinates without re-seeking. Header reads and body reads
     * both pull from this same stream.
     */
    private static final class SequentialSpillStream implements Closeable {

        private final List<Path> files;

        /** File index currently open and being read. */
        private int currentFileIndex;

        /** Next read offset within {@link #currentFileIndex}. */
        private long offsetInFile;

        /** Open stream over {@link #currentFileIndex}, or {@code null} before the first read. */
        @Nullable private InputStream fileStream;

        /** Size of the file currently open. */
        private long currentFileSize;

        SequentialSpillStream(List<Path> files, int startFileIndex, long startOffsetInFile) {
            this.files = files;
            this.currentFileIndex = startFileIndex;
            this.offsetInFile = startOffsetInFile;
        }

        /**
         * Returns true if more segment data remains, advancing past exhausted files as needed.
         * After this returns true, {@link #currentFileIndex}/{@link #offsetInFile} point at
         * readable data.
         */
        boolean hasRemaining() {
            try {
                while (currentFileIndex < files.size()) {
                    ensureOpen();
                    if (offsetInFile < currentFileSize) {
                        return true;
                    }
                    advanceToNextFile();
                }
                return false;
            } catch (IOException e) {
                throw new RuntimeException("Failed to scan spill files", e);
            }
        }

        /** Reads one 12-byte segment header from the current position. */
        ParsedHeader readHeader() throws IOException {
            int headerFileIndex = currentFileIndex;
            long headerOffset = offsetInFile;

            byte[] headerBytes = new byte[SEGMENT_HEADER_BYTES];
            readFully(headerBytes);
            DataInputStream h = new DataInputStream(new java.io.ByteArrayInputStream(headerBytes));
            int gateIdx = h.readInt();
            int channelIdx = h.readInt();
            int bufferLength = h.readInt();
            checkState(bufferLength >= 0, "negative segment length: %s", bufferLength);

            // The body occupies the next bufferLength bytes within the same file. The next header
            // begins right after the body.
            return new ParsedHeader(
                    headerFileIndex,
                    headerOffset,
                    gateIdx,
                    channelIdx,
                    bufferLength,
                    currentFileIndex,
                    offsetInFile + bufferLength);
        }

        /**
         * Reads up to {@code len} body bytes into {@code buf}. A segment body never crosses a file
         * boundary, so this reads only from the current file. Returns -1 only when the bounded body
         * view has signalled exhaustion; an unexpected mid-body EOF throws.
         */
        int readBody(byte[] buf, int off, int len) throws IOException {
            ensureOpen();
            int n = fileStream.read(buf, off, len);
            if (n > 0) {
                offsetInFile += n;
            }
            return n;
        }

        private void readFully(byte[] buf) throws IOException {
            ensureOpen();
            int read = 0;
            while (read < buf.length) {
                int n = fileStream.read(buf, read, buf.length - read);
                if (n < 0) {
                    throw new EOFException(
                            "Truncated segment header in file "
                                    + files.get(currentFileIndex)
                                    + " at offset "
                                    + offsetInFile
                                    + ": expected "
                                    + buf.length
                                    + " bytes, got "
                                    + read);
                }
                read += n;
                offsetInFile += n;
            }
        }

        private void ensureOpen() throws IOException {
            if (fileStream == null) {
                Path path = files.get(currentFileIndex);
                currentFileSize = java.nio.file.Files.size(path);
                InputStream in = java.nio.file.Files.newInputStream(path);
                long skipped = 0;
                while (skipped < offsetInFile) {
                    long s = in.skip(offsetInFile - skipped);
                    if (s <= 0) {
                        // skip can return 0 near EOF; read-and-discard as a fallback.
                        if (in.read() < 0) {
                            in.close();
                            throw new EOFException(
                                    "Cannot position to offset "
                                            + offsetInFile
                                            + " in spill file "
                                            + path);
                        }
                        skipped++;
                    } else {
                        skipped += s;
                    }
                }
                fileStream = in;
            }
        }

        private void advanceToNextFile() throws IOException {
            if (fileStream != null) {
                fileStream.close();
                fileStream = null;
            }
            currentFileIndex++;
            offsetInFile = 0L;
        }

        @Override
        public void close() throws IOException {
            if (fileStream != null) {
                fileStream.close();
                fileStream = null;
            }
        }
    }

    // -------------------------------------------------------------------------------------------
    // BoundedSegmentStream
    // -------------------------------------------------------------------------------------------

    /**
     * A forward-only view over the next {@code length} body bytes of a {@link
     * SequentialSpillStream}. Reads return EOF after {@code length} bytes. If the underlying stream
     * ends before {@code length} bytes are available, an {@link EOFException} is thrown
     * (fail-loud). Closing this view does not close the underlying stream; it is owned by the
     * iterator.
     */
    private static final class BoundedSegmentStream extends InputStream {

        private final SequentialSpillStream stream;
        private final int length;

        private int position;

        BoundedSegmentStream(SequentialSpillStream stream, int length) {
            this.stream = stream;
            this.length = length;
        }

        /** Number of body bytes read from this view so far. */
        int bytesRead() {
            return position;
        }

        /** Number of body bytes not yet read from this view. */
        int remaining() {
            return length - position;
        }

        @Override
        public int read() throws IOException {
            byte[] one = new byte[1];
            int n = read(one, 0, 1);
            return n < 0 ? -1 : (one[0] & 0xFF);
        }

        @Override
        public int read(byte[] buf, int off, int len) throws IOException {
            if (position >= length) {
                return -1;
            }
            int toRead = Math.min(len, length - position);
            int n = stream.readBody(buf, off, toRead);
            if (n < 0) {
                throw new EOFException(
                        "Unexpected EOF in segment body after "
                                + position
                                + "/"
                                + length
                                + " bytes");
            }
            position += n;
            return n;
        }

        @Override
        public void close() {
            // Do not close the underlying stream; it is owned by the iterator.
        }
    }
}
