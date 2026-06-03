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
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.NoSuchElementException;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Forward reader over {@link SpillFile} entries.
 *
 * <p>The root reader is shared by the drain thread and checkpoint trigger: disk reads happen in
 * {@link #peek()}, while {@link #advance()} must be paired with channel delivery under the drainer
 * lock. Snapshot readers are independent and single-consumer.
 */
@Internal
public final class SpillFileReader implements Closeable {

    /** A recovered-state entry. {@link #data} is valid until the next {@link #advance()}. */
    public static final class Chunk {
        public final InputChannelInfo channelInfo;
        public final byte[] data;
        public final int length;

        Chunk(InputChannelInfo channelInfo, byte[] data, int length) {
            this.channelInfo = channelInfo;
            this.data = data;
            this.length = length;
        }
    }

    private final SpillFile spillFile;
    private final List<SpillFile.SpillFileSegment> segments;
    private final byte[] reusable;
    private final ByteBuffer headerBuffer = ByteBuffer.allocate(SpillFile.HEADER_BYTES);

    private int segmentCursor;

    /** Byte offset of the next record within the segment at {@code segmentCursor}. */
    private long byteOffset;

    @Nullable private FileChannel activeChannel;

    /**
     * Cached chunk for the record at {@code (segmentCursor, byteOffset)}, non-null iff a {@link
     * #peek()} succeeded and {@link #advance()} has not been called since. Holds the only live
     * metadata copy at a time, so iteration cost stays constant regardless of record count.
     */
    @Nullable private Chunk cachedChunk;

    private boolean closed;

    private SpillFileReader(
            SpillFile spillFile,
            List<SpillFile.SpillFileSegment> segments,
            int segmentCursor,
            long byteOffset) {
        this.spillFile = checkNotNull(spillFile);
        this.segments = checkNotNull(segments);
        this.reusable = new byte[spillFile.maxEntryLength()];
        this.segmentCursor = segmentCursor;
        this.byteOffset = byteOffset;
        this.spillFile.acquire();
    }

    static SpillFileReader openRoot(SpillFile spillFile) {
        return new SpillFileReader(spillFile, spillFile.segments(), 0, 0L);
    }

    /** Reads and caches the next record. Must be called outside cross-thread locks. */
    @Nullable
    public Chunk peek() throws IOException {
        checkState(!closed, "SpillFileReader is closed");
        if (cachedChunk != null) {
            return cachedChunk;
        }
        advanceToNextSegmentIfCurrentIsExhausted();
        if (segmentCursor >= segments.size()) {
            return null;
        }
        SpillFile.SpillFileSegment seg = segments.get(segmentCursor);
        ensureActiveChannelFor(seg, byteOffset);
        cachedChunk = readRecord(seg);
        return cachedChunk;
    }

    /** Advances past the cached record. Root readers must call this under the drainer lock. */
    public void advance() {
        checkState(!closed, "SpillFileReader is closed");
        checkState(cachedChunk != null, "advance() called without a preceding successful peek()");
        byteOffset += SpillFile.HEADER_BYTES + cachedChunk.length;
        cachedChunk = null;
    }

    /**
     * Derives an independent reader covering records not yet advanced past. Root readers must call
     * this under the drainer lock.
     */
    public SpillFileReader snapshot() {
        checkState(!closed, "SpillFileReader is closed");
        return new SpillFileReader(spillFile, segments, segmentCursor, byteOffset);
    }

    /** Single-consumer iterator wrapper for snapshot readers. */
    public CloseableIterator<Chunk> asIterator() {
        return new CloseableIterator<Chunk>() {
            @Override
            public boolean hasNext() {
                try {
                    return peek() != null;
                } catch (IOException ioe) {
                    throw new RuntimeException(ioe);
                }
            }

            @Override
            public Chunk next() {
                Chunk c;
                try {
                    c = peek();
                } catch (IOException ioe) {
                    throw new RuntimeException(ioe);
                }
                if (c == null) {
                    throw new NoSuchElementException();
                }
                advance();
                return c;
            }

            @Override
            public void close() throws Exception {
                SpillFileReader.this.close();
            }
        };
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        cachedChunk = null;
        try {
            closeActiveChannel();
        } finally {
            spillFile.release();
        }
    }

    private void advanceToNextSegmentIfCurrentIsExhausted() throws IOException {
        if (segmentCursor >= segments.size()) {
            return;
        }

        SpillFile.SpillFileSegment currentSegment = segments.get(segmentCursor);
        checkState(currentSegment.currentEnd > 0, "Spill segment %s is empty", currentSegment.path);
        checkState(
                byteOffset <= currentSegment.currentEnd,
                "Byte offset %s is past the segment end %s in spill segment %s",
                byteOffset,
                currentSegment.currentEnd,
                currentSegment.path);

        if (byteOffset < currentSegment.currentEnd) {
            return;
        }

        closeActiveChannel();
        segmentCursor++;
        byteOffset = 0L;

        if (segmentCursor >= segments.size()) {
            return;
        }

        SpillFile.SpillFileSegment nextSegment = segments.get(segmentCursor);
        checkState(nextSegment.currentEnd > 0, "Spill segment %s is empty", nextSegment.path);
    }

    private void ensureActiveChannelFor(SpillFile.SpillFileSegment seg, long offset)
            throws IOException {
        if (activeChannel == null) {
            activeChannel = FileChannel.open(seg.path, StandardOpenOption.READ);
            activeChannel.position(offset);
        }
    }

    /** Reads the inline header then the payload for the record at the current position. */
    private Chunk readRecord(SpillFile.SpillFileSegment seg) throws IOException {
        headerBuffer.clear();
        readFully(headerBuffer, SpillFile.HEADER_BYTES);
        headerBuffer.flip();
        int gateIdx = headerBuffer.getInt();
        int channelIdx = headerBuffer.getInt();
        int length = headerBuffer.getInt();
        checkState(
                length >= 0 && length <= reusable.length,
                "Decoded record length %s out of bounds [0, %s] in spill segment %s",
                length,
                reusable.length,
                seg.path);
        readFully(ByteBuffer.wrap(reusable, 0, length), length);
        return new Chunk(new InputChannelInfo(gateIdx, channelIdx), reusable, length);
    }

    private void readFully(ByteBuffer view, int length) throws IOException {
        int totalRead = 0;
        while (totalRead < length) {
            int n = activeChannel.read(view);
            if (n < 0) {
                throw new IOException(
                        "Unexpected EOF reading segment "
                                + segments.get(segmentCursor).path
                                + " after "
                                + totalRead
                                + "/"
                                + length
                                + " bytes");
            }
            totalRead += n;
        }
    }

    private void closeActiveChannel() throws IOException {
        if (activeChannel != null) {
            activeChannel.close();
            activeChannel = null;
        }
    }
}
