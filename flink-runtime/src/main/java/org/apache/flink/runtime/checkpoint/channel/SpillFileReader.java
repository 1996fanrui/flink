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
 * Forward iterator over a {@link SpillFile}'s entries. Two access patterns share the same
 * underlying state:
 *
 * <ul>
 *   <li><b>Drain (root reader, shared across threads):</b> caller uses {@link #peek()} + {@link
 *       #advance()}. Disk read inside {@code peek()} runs outside any caller lock; the {@code
 *       advance()} call is paired with delivery and must happen under the drainer's lock.
 *   <li><b>Per-checkpoint sub-reader (single consumer):</b> derived via {@link #snapshot()} from a
 *       root reader, consumed via {@link #asIterator()} without any lock.
 * </ul>
 *
 * <p>Invariants:
 *
 * <ul>
 *   <li>Each segment file is opened at most once per reader instance. The cursor only moves
 *       forward; when {@link #advance()} crosses a segment boundary, the previous {@link
 *       FileChannel} is closed before the next segment is opened.
 *   <li>One reusable {@code byte[maxEntryLength]} per reader, allocated at construction. {@link
 *       Chunk#data} aliases this buffer; the bytes are valid only until the next {@link
 *       #advance()}.
 *   <li>Each reader instance holds exactly one {@code SpillFile} ref-count grant — acquired in the
 *       constructor, released in {@link #close()}. {@link #snapshot()} constructs a sub-reader that
 *       takes its own grant.
 * </ul>
 */
@Internal
public final class SpillFileReader implements Closeable {

    /**
     * A single recovered-state entry exposed to the caller. {@link #data} aliases the reader's
     * reusable buffer — valid only until the next {@link SpillFileReader#advance()}.
     */
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

    /** Index into {@code segments}; range [0, segments.size()]. */
    private int segmentCursor;

    /** Index into {@code segments[segmentCursor].entries()}. */
    private int entryCursor;

    /** Open FileChannel for {@code segments[segmentCursor]}, or {@code null}. */
    @Nullable private FileChannel activeChannel;

    /**
     * Cached chunk for the entry at {@code (segmentCursor, entryCursor)}, non-null iff a {@link
     * #peek()} succeeded and {@link #advance()} has not been called since.
     */
    @Nullable private Chunk cachedChunk;

    private boolean closed;

    /**
     * Constructs a reader covering all segments / entries of {@code spillFile} starting at position
     * {@code (segmentCursor, entryCursor)}. Takes one ref-count grant on the spill file; paired
     * with {@link #close()}.
     */
    private SpillFileReader(
            SpillFile spillFile,
            List<SpillFile.SpillFileSegment> segments,
            int segmentCursor,
            int entryCursor) {
        this.spillFile = checkNotNull(spillFile);
        this.segments = checkNotNull(segments);
        this.reusable = new byte[spillFile.maxEntryLength()];
        this.segmentCursor = segmentCursor;
        this.entryCursor = entryCursor;
        this.spillFile.acquire();
    }

    /** Opens a root reader positioned at the start of the spill file. */
    static SpillFileReader openRoot(SpillFile spillFile) {
        return new SpillFileReader(spillFile, spillFile.segments(), 0, 0);
    }

    /**
     * Returns the next entry as a {@link Chunk}, or {@code null} if exhausted. The bytes are read
     * into the reusable buffer on first call and cached until {@link #advance()} invalidates them.
     *
     * <p>Caller must NOT hold any cross-thread lock when invoking this — disk I/O may block.
     */
    @Nullable
    public Chunk peek() throws IOException {
        checkState(!closed, "SpillFileReader is closed");
        if (cachedChunk != null) {
            return cachedChunk;
        }
        skipExhaustedSegments();
        if (segmentCursor >= segments.size()) {
            return null;
        }
        SpillFile.SpillFileSegment seg = segments.get(segmentCursor);
        SpillFile.Entry e = seg.entries().get(entryCursor);
        ensureActiveChannelFor(seg, e.offset);
        readFully(e.length);
        cachedChunk = new Chunk(e.channelInfo, reusable, e.length);
        return cachedChunk;
    }

    /**
     * Advances past the entry returned by the most recent {@link #peek()}, invalidating its cache.
     * Pure in-memory update — safe to call inside a caller lock; for a root reader shared with the
     * drainer's checkpoint trigger, MUST be called under the drainer's lock, paired with delivery.
     */
    public void advance() {
        checkState(!closed, "SpillFileReader is closed");
        checkState(cachedChunk != null, "advance() called without a preceding successful peek()");
        cachedChunk = null;
        entryCursor++;
        // skipExhaustedSegments is deferred to the next peek(); advance() stays in-memory only and
        // does NOT close the active FileChannel — that happens lazily when peek() needs to open a
        // new segment.
    }

    /**
     * Derives an independent sub-reader covering entries the root reader has not yet delivered.
     * Must be called inside the drainer's lock when invoked on the root reader.
     *
     * <p>If a {@link #peek()} is in flight (entry has been read into the reusable buffer but {@link
     * #advance()} has not been called), the peeked entry is still part of the snapshot. Only {@link
     * #advance()} marks an entry as delivered by moving the cursor; {@code peek()} is just an
     * internal read-ahead cache.
     */
    public SpillFileReader snapshot() {
        checkState(!closed, "SpillFileReader is closed");
        return new SpillFileReader(spillFile, segments, segmentCursor, entryCursor);
    }

    /**
     * Single-consumer convenience wrapper: {@code hasNext() == peek() != null} and {@code next() ==
     * peek() + advance()}. Safe only when this reader is NOT shared with the drainer (i.e. only
     * sub-readers).
     */
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

    /**
     * Skips empty segments at the current cursor. Empty segments are unusual but possible (e.g. a
     * segment file created lazily that received no entries before rotation). Closes any active
     * channel before moving on.
     */
    private void skipExhaustedSegments() throws IOException {
        while (segmentCursor < segments.size()
                && entryCursor >= segments.get(segmentCursor).entries().size()) {
            closeActiveChannel();
            segmentCursor++;
            entryCursor = 0;
        }
    }

    private void ensureActiveChannelFor(SpillFile.SpillFileSegment seg, long offset)
            throws IOException {
        if (activeChannel == null) {
            activeChannel = FileChannel.open(seg.path, StandardOpenOption.READ);
            activeChannel.position(offset);
        }
        // Reads within a segment are strictly forward; FileChannel.position advances by each read
        // so no explicit seek is needed beyond the initial open.
    }

    private void readFully(int length) throws IOException {
        ByteBuffer view = ByteBuffer.wrap(reusable, 0, length);
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
