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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An append-only, segmented on-disk store for recovered channel-state buffers produced by the
 * filter phase. Written by a single thread (the {@code channelIOExecutor}); each segment owns its
 * own {@link Entry} list so that "entry belongs to segment" is a structural fact, not a runtime
 * check.
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
     * One on-disk segment of a {@link SpillFile}. Holds the segment index, file path, the
     * append-side {@link FileChannel}, a running byte counter, and the entry list for records that
     * landed in this segment. The entry list is the structural owner of entry-to-segment mapping —
     * no separate top-level entry queue exists.
     */
    static final class SpillFileSegment implements Closeable {
        final int segmentIndex;
        final Path path;
        final FileChannel channel;
        // Number of bytes written so far in this segment. Updated after every append.
        long currentEnd;

        // Entries that landed in this segment, in append order. Populated by SpillFile.append on
        // the single-writer (filter) thread.
        private final List<Entry> entries = new ArrayList<>();

        SpillFileSegment(int segmentIndex, Path path, FileChannel channel) {
            this.segmentIndex = segmentIndex;
            this.path = path;
            this.channel = channel;
            this.currentEnd = 0L;
        }

        /** Read-only view of the entries belonging to this segment. */
        List<Entry> entries() {
            return Collections.unmodifiableList(entries);
        }

        @Override
        public void close() throws IOException {
            // Closing an already-closed FileChannel is a no-op, which keeps SpillFile.close
            // idempotent without per-segment bookkeeping.
            channel.close();
        }
    }

    /**
     * A single record persisted in a {@link SpillFile}: identifies the channel and the byte range
     * within its owning segment. The owning segment is determined structurally by which {@link
     * SpillFileSegment#entries} list the entry appears in.
     */
    static final class Entry {
        final InputChannelInfo channelInfo;
        final long offset;
        final int length;

        Entry(InputChannelInfo channelInfo, long offset, int length) {
            this.channelInfo = channelInfo;
            this.offset = offset;
            this.length = length;
        }
    }

    private final Path baseDir;
    private final long segmentSizeBytes;
    private final int maxEntryLength;
    private final List<SpillFileSegment> segments = new ArrayList<>();
    private boolean closed = false;

    /**
     * Number of live consumers that still need the on-disk segments: each live {@code
     * SpillFileReader} instance holds exactly one grant (acquired in its constructor, released in
     * its {@code close()}). Incremented by {@link #acquire()} and decremented by {@link
     * #release()}. The actual segment deletion is gated by {@link #cleanedUp} so it runs at most
     * once even when {@code release} and {@link #close()} race.
     */
    private final AtomicInteger refCount = new AtomicInteger(0);

    /**
     * Latches true the first time a cleanup path wins the CAS, making segment deletion idempotent
     * across the {@code release-to-zero} path and the forced {@link #close()} path (which the
     * shutdown / test harness needs even if some references are still outstanding).
     */
    private final AtomicBoolean cleanedUp = new AtomicBoolean(false);

    public SpillFile(Path baseDir, long segmentSizeBytes, int maxEntryLength) {
        checkArgument(
                segmentSizeBytes > 0, "segmentSizeBytes must be positive: %s", segmentSizeBytes);
        checkArgument(
                maxEntryLength >= 0, "maxEntryLength must be non-negative: %s", maxEntryLength);
        this.baseDir = checkNotNull(baseDir);
        this.segmentSizeBytes = segmentSizeBytes;
        this.maxEntryLength = maxEntryLength;
    }

    public SpillFile(Path baseDir, int maxEntryLength) {
        this(baseDir, DEFAULT_SEGMENT_SIZE_BYTES, maxEntryLength);
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
        active.entries.add(new Entry(channelInfo, offsetBeforeWrite, length));
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
     * Increments the reference count. Each {@code SpillFileReader} instance holds exactly one grant
     * (acquired in its constructor, released in {@link Closeable#close()}). Pairs with {@link
     * #release()}.
     */
    public void acquire() {
        refCount.incrementAndGet();
    }

    /**
     * Decrements the reference count. When the count reaches zero, attempts the one-shot segment
     * deletion guarded by {@link #cleanedUp}. The CAS makes the deletion idempotent: concurrent
     * {@code release()} callers that all observe zero, plus a racing forced {@link #close()}, all
     * agree on a single cleanup. Once cleanup runs, {@link #closed} flips too so any further {@link
     * #append} attempts fail loudly rather than write to deleted files.
     */
    public void release() throws IOException {
        if (refCount.decrementAndGet() == 0) {
            if (cleanedUp.compareAndSet(false, true)) {
                closed = true;
                deleteAllSegments();
            }
        }
    }

    /**
     * Forced cleanup entry retained for tests and task-manager shutdown — callers may need to
     * remove segments even if some readers are still outstanding (e.g. the checkpoint they belong
     * to was aborted before the writer future fired). Shares the {@link #cleanedUp} CAS with {@link
     * #release()} so the actual deletion runs at most once.
     */
    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        if (cleanedUp.compareAndSet(false, true)) {
            deleteAllSegments();
        }
    }

    /**
     * Closes every segment {@link FileChannel} and removes the underlying segment file from disk.
     * Always invoked through the {@link #cleanedUp} CAS so it runs at most once.
     */
    private void deleteAllSegments() throws IOException {
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
            try {
                Files.deleteIfExists(seg.path);
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

    /** Per-entry max byte length; SpillFileReader allocates {@code byte[maxEntryLength]}. */
    int maxEntryLength() {
        return maxEntryLength;
    }

    /** Returns an unmodifiable snapshot of the current segment list. */
    List<SpillFileSegment> segments() {
        return Collections.unmodifiableList(segments);
    }

    /**
     * Flattens all segments' entries into a single list in append order. Test-only helper for
     * asserting metadata; the production read path goes through {@link SpillFileReader}.
     */
    @VisibleForTesting
    public List<Entry> entries() {
        List<Entry> out = new ArrayList<>();
        for (SpillFileSegment seg : segments) {
            out.addAll(seg.entries);
        }
        return Collections.unmodifiableList(out);
    }

    /**
     * Opens a root {@link SpillFileReader} positioned at the start of the file. Called once by the
     * {@code SpillFileDrainer}'s constructor; the resulting reader is shared between the drain
     * thread and the task-thread checkpoint trigger.
     */
    public SpillFileReader reader() {
        return SpillFileReader.openRoot(this);
    }
}
