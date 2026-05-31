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

    private int segmentCursor;

    private int entryCursor;

    @Nullable private FileChannel activeChannel;

    /**
     * Cached chunk for the entry at {@code (segmentCursor, entryCursor)}, non-null iff a {@link
     * #peek()} succeeded and {@link #advance()} has not been called since.
     */
    @Nullable private Chunk cachedChunk;

    private boolean closed;

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

    static SpillFileReader openRoot(SpillFile spillFile) {
        return new SpillFileReader(spillFile, spillFile.segments(), 0, 0);
    }

    /** Reads and caches the next entry. Must be called outside cross-thread locks. */
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
        SpillFile.Entry e = seg.entries().get(entryCursor);
        ensureActiveChannelFor(seg, e.offset);
        readFully(e.length);
        cachedChunk = new Chunk(e.channelInfo, reusable, e.length);
        return cachedChunk;
    }

    /** Advances past the cached entry. Root readers must call this under the drainer lock. */
    public void advance() {
        checkState(!closed, "SpillFileReader is closed");
        checkState(cachedChunk != null, "advance() called without a preceding successful peek()");
        cachedChunk = null;
        entryCursor++;
    }

    /**
     * Derives an independent reader covering entries not yet advanced past. Root readers must call
     * this under the drainer lock.
     */
    public SpillFileReader snapshot() {
        checkState(!closed, "SpillFileReader is closed");
        return new SpillFileReader(spillFile, segments, segmentCursor, entryCursor);
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
        int entryCount = currentSegment.entries().size();
        checkState(entryCount > 0, "Spill segment %s has no entries", currentSegment.path);
        checkState(
                entryCursor <= entryCount,
                "Entry cursor %s is past the entry count %s in spill segment %s",
                entryCursor,
                entryCount,
                currentSegment.path);

        if (entryCursor < entryCount) {
            return;
        }

        closeActiveChannel();
        segmentCursor++;
        entryCursor = 0;

        if (segmentCursor >= segments.size()) {
            return;
        }

        SpillFile.SpillFileSegment nextSegment = segments.get(segmentCursor);
        checkState(
                !nextSegment.entries().isEmpty(),
                "Spill segment %s has no entries",
                nextSegment.path);
    }

    private void ensureActiveChannelFor(SpillFile.SpillFileSegment seg, long offset)
            throws IOException {
        if (activeChannel == null) {
            activeChannel = FileChannel.open(seg.path, StandardOpenOption.READ);
            activeChannel.position(offset);
        }
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
