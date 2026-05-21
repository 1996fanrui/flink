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
import org.apache.flink.annotation.VisibleForTesting;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An append-only, segmented on-disk store for recovered channel-state buffers produced by the
 * filter phase. Written by a single thread (the {@code channelIOExecutor}); records the metadata
 * for every appended payload in an in-memory {@link Entry} queue so that the drain phase (Phase 4)
 * can replay the payloads in order.
 *
 * <p>Segments rotate once a single file would exceed the configured segment size. The default size
 * caps each segment file at 64 MiB, balancing OS-level I/O scheduling against per-segment metadata
 * overhead.
 *
 * <p>Single-writer invariant — all mutating methods assume the caller is the one and only writer.
 * No internal locking is performed; correctness relies on the {@code channelIOExecutor}'s
 * single-thread guarantee.
 */
@Internal
public final class SpillFile implements Closeable {

    /**
     * Maximum size of a single on-disk segment file before rotating to a new one. Aligned with the
     * segment-size convention used elsewhere in the channel-state IO path.
     */
    public static final long DEFAULT_SEGMENT_SIZE_BYTES = 64L * 1024 * 1024;

    /**
     * One on-disk segment of a {@link SpillFile}. Holds the segment index, file path, an opened
     * {@link FileChannel} for append-only writes, and a running byte counter.
     */
    static final class SpillFileSegment implements Closeable {
        final int segmentIndex;
        final Path path;
        final FileChannel channel;
        // Number of bytes written so far in this segment. Updated after every append.
        long currentEnd;

        SpillFileSegment(int segmentIndex, Path path, FileChannel channel) {
            this.segmentIndex = segmentIndex;
            this.path = path;
            this.channel = channel;
            this.currentEnd = 0L;
        }

        @Override
        public void close() throws IOException {
            // Closing an already-closed FileChannel is a no-op, which keeps SpillFile.close
            // idempotent without per-segment bookkeeping.
            channel.close();
        }
    }

    /**
     * A single record persisted in a {@link SpillFile}: identifies the channel, segment, and byte
     * range of the payload. The drain phase replays the file in {@code entries} order.
     */
    static final class Entry {
        final InputChannelInfo channelInfo;
        final int segmentIndex;
        final long offset;
        final int length;

        Entry(InputChannelInfo channelInfo, int segmentIndex, long offset, int length) {
            this.channelInfo = channelInfo;
            this.segmentIndex = segmentIndex;
            this.offset = offset;
            this.length = length;
        }
    }

    private final Path baseDir;
    private final long segmentSizeBytes;
    private final List<SpillFileSegment> segments = new ArrayList<>();
    private final Deque<Entry> entries = new ArrayDeque<>();
    private boolean closed = false;

    public SpillFile(Path baseDir, long segmentSizeBytes) {
        checkArgument(
                segmentSizeBytes > 0, "segmentSizeBytes must be positive: %s", segmentSizeBytes);
        this.baseDir = checkNotNull(baseDir);
        this.segmentSizeBytes = segmentSizeBytes;
    }

    public SpillFile(Path baseDir) {
        this(baseDir, DEFAULT_SEGMENT_SIZE_BYTES);
    }

    /**
     * Append a single payload for {@code channelInfo}. The payload is written from {@code
     * payload.position()} to {@code payload.limit()}; on return the payload's position is advanced
     * past the written bytes (standard {@link FileChannel#write(ByteBuffer)} semantics).
     *
     * <p>If writing the payload would push the active segment past {@link #segmentSizeBytes}, a new
     * segment is created first. A single payload is never split across segments — the recovered
     * buffer size is bounded by a single network buffer (well below 64 MiB), so segment rotation
     * happens cleanly between records.
     *
     * @throws IllegalStateException if {@link #close()} has been called.
     */
    public void append(InputChannelInfo channelInfo, ByteBuffer payload) throws IOException {
        if (closed) {
            throw new IllegalStateException(
                    "Cannot append to a closed SpillFile (baseDir=" + baseDir + ").");
        }
        checkNotNull(channelInfo);
        checkNotNull(payload);

        int length = payload.remaining();
        if (length == 0) {
            // Empty payloads produce no on-disk effect and no entry — keeping entry semantics
            // strictly "one entry == one non-empty record".
            return;
        }

        SpillFileSegment active = activeSegmentFor(length);
        long offsetBeforeWrite = active.currentEnd;

        int written = 0;
        while (written < length) {
            int n = active.channel.write(payload);
            // FileChannel.write never returns negative for a regular file; guard defensively.
            if (n <= 0) {
                throw new IOException(
                        "FileChannel.write returned " + n + " on segment " + active.path);
            }
            written += n;
        }
        active.currentEnd = offsetBeforeWrite + length;
        entries.add(new Entry(channelInfo, active.segmentIndex, offsetBeforeWrite, length));
    }

    /**
     * Returns the segment to write the next payload into, rotating to a fresh segment when adding
     * {@code payloadLength} bytes would exceed {@link #segmentSizeBytes}. Allocates the first
     * segment lazily on the first call.
     */
    private SpillFileSegment activeSegmentFor(int payloadLength) throws IOException {
        if (segments.isEmpty()) {
            return openNewSegment();
        }
        SpillFileSegment current = segments.get(segments.size() - 1);
        if (current.currentEnd + payloadLength > segmentSizeBytes) {
            return openNewSegment();
        }
        return current;
    }

    private SpillFileSegment openNewSegment() throws IOException {
        Files.createDirectories(baseDir);
        int index = segments.size();
        Path segmentPath = baseDir.resolve("spill-segment-" + index + ".bin");
        FileChannel channel =
                FileChannel.open(
                        segmentPath,
                        StandardOpenOption.CREATE_NEW,
                        StandardOpenOption.WRITE,
                        StandardOpenOption.READ);
        SpillFileSegment seg = new SpillFileSegment(index, segmentPath, channel);
        segments.add(seg);
        return seg;
    }

    /**
     * Closes every segment file. Idempotent — repeated calls are no-ops, so the closure ordering
     * inside higher-level facades ({@code SpillFileWriter}) can safely call this even after the
     * accumulator has already closed it.
     */
    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        IOException firstError = null;
        for (SpillFileSegment seg : segments) {
            try {
                seg.close();
            } catch (IOException e) {
                if (firstError == null) {
                    firstError = e;
                } else {
                    firstError.addSuppressed(e);
                }
            }
        }
        if (firstError != null) {
            throw firstError;
        }
    }

    @VisibleForTesting
    public boolean isClosed() {
        return closed;
    }

    /**
     * Returns an unmodifiable view of the entry queue. Package-private so callers in this package
     * (Phase 4 drain, tests) can inspect the disk layout without touching internal state.
     */
    List<Entry> entries() {
        return Collections.unmodifiableList(new ArrayList<>(entries));
    }

    /** Returns an unmodifiable snapshot of the current segment list. */
    List<SpillFileSegment> segments() {
        return Collections.unmodifiableList(segments);
    }

    /**
     * Reads {@code length} bytes from {@code segmentIndex} starting at {@code offset} into a fresh
     * byte array. Exists primarily so tests and Phase 4 drain code can verify on-disk content
     * without re-implementing the segment lookup. Reads the open file channel directly via a
     * position-based read so concurrent appends (single-writer guarantee aside) cannot affect the
     * channel's logical position.
     */
    byte[] readBytes(int segmentIndex, long offset, int length) throws IOException {
        checkArgument(
                segmentIndex >= 0 && segmentIndex < segments.size(),
                "segmentIndex out of range: %s",
                segmentIndex);
        checkArgument(length >= 0, "length must be non-negative: %s", length);

        SpillFileSegment seg = segments.get(segmentIndex);
        ByteBuffer buf = ByteBuffer.allocate(length);
        int totalRead = 0;
        while (totalRead < length) {
            int n = seg.channel.read(buf, offset + totalRead);
            if (n < 0) {
                throw new IOException(
                        "Unexpected EOF reading segment "
                                + seg.path
                                + " at offset "
                                + (offset + totalRead));
            }
            totalRead += n;
        }
        return buf.array();
    }
}
