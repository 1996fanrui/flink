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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An append-only, segmented on-disk store for recovered channel-state buffers produced by the
 * filter phase. Written by a single thread (the {@code channelIOExecutor}); records the metadata
 * for every appended payload in an in-memory {@link Entry} queue so that the drain can replay the
 * payloads in order.
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
     *
     * <p>Drain ({@code SpillFileReader}) maintains a private peek/poll cursor over the entries
     * belonging to this segment so that read progress can be advanced atomically with the channel
     * delivery inside {@code SpillFileReader.lock}. Each {@link #readBytesAt} call opens an
     * independent read-only {@link FileChannel} so that the drain and any concurrent in-recovery
     * checkpoint readers never share file position state.
     */
    static final class SpillFileSegment implements Closeable {
        final int segmentIndex;
        final Path path;
        final FileChannel channel;
        // Number of bytes written so far in this segment. Updated after every append.
        long currentEnd;

        // Drain-side cursor over the entries that belong to this segment. Populated by the
        // enclosing SpillFile after filter completes; consumed by drain. Not used by readers
        // opened over a SpillFile.Snapshot — those iterate via their own cursor.
        private final Deque<Entry> drainEntries = new ArrayDeque<>();

        SpillFileSegment(int segmentIndex, Path path, FileChannel channel) {
            this.segmentIndex = segmentIndex;
            this.path = path;
            this.channel = channel;
            this.currentEnd = 0L;
        }

        /** Returns the next entry to drain without removing it, or {@code null} when empty. */
        Entry peekNextEntry() {
            return drainEntries.peek();
        }

        /** Removes and returns the next entry to drain, or {@code null} when empty. */
        Entry pollNextEntry() {
            return drainEntries.poll();
        }

        /**
         * Reads {@code length} bytes from {@code offset} into {@code dest} using an independently
         * opened read-only {@link FileChannel}. A fresh handle per call avoids sharing position
         * state with the segment's append-side handle and with any other concurrent reader (drain
         * and the per-checkpoint readers can call this in parallel).
         */
        void readBytesAt(long offset, int length, byte[] dest) throws IOException {
            checkArgument(length >= 0, "length must be non-negative: %s", length);
            checkArgument(
                    dest.length >= length, "dest buffer too small: %s < %s", dest.length, length);
            try (FileChannel reader = FileChannel.open(path, StandardOpenOption.READ)) {
                ByteBuffer view = ByteBuffer.wrap(dest, 0, length);
                int totalRead = 0;
                while (totalRead < length) {
                    int n = reader.read(view, offset + totalRead);
                    if (n < 0) {
                        throw new IOException(
                                "Unexpected EOF reading segment "
                                        + path
                                        + " at offset "
                                        + (offset + totalRead));
                    }
                    totalRead += n;
                }
            }
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

    /**
     * Number of live consumers that still need the on-disk segments: the drain reader, plus one per
     * in-flight {@link DiskSnapshot} produced by a recovery-time checkpoint. Incremented by {@link
     * #acquire()} and decremented by {@link #release()}. The actual segment deletion is gated by
     * {@link #cleanedUp} so it runs at most once even when {@code release} and {@link #close()}
     * race.
     */
    private final AtomicInteger refCount = new AtomicInteger(0);

    /**
     * Latches true the first time a cleanup path wins the CAS, making segment deletion idempotent
     * across the {@code release-to-zero} path and the forced {@link #close()} path (which the
     * shutdown / test harness needs even if some references are still outstanding).
     */
    private final AtomicBoolean cleanedUp = new AtomicBoolean(false);

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
        Entry entry = new Entry(channelInfo, active.segmentIndex, offsetBeforeWrite, length);
        entries.add(entry);
        // Mirror into the per-segment drain queue so SpillFileReader can peek/poll without
        // re-grouping. Filter is single-writer; appending here observes the same single-writer
        // invariant.
        active.drainEntries.add(entry);
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
     * Increments the reference count. Held by the {@link SpillFileReader} (one grant) and by each
     * in-flight {@link DiskSnapshot} produced for a recovery-time checkpoint. Pairs with {@link
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
     * remove segments even if some {@link DiskSnapshot} references are still outstanding (e.g. the
     * checkpoint they belong to was aborted before the writer future fired). Shares the {@link
     * #cleanedUp} CAS with {@link #release()} so the actual deletion runs at most once.
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

    /**
     * Returns an unmodifiable view of the entry queue. Package-private so callers in this package
     * (drain, tests) can inspect the disk layout without touching internal state.
     */
    List<Entry> entries() {
        return Collections.unmodifiableList(new ArrayList<>(entries));
    }

    /** Returns an unmodifiable snapshot of the current segment list. */
    List<SpillFileSegment> segments() {
        return Collections.unmodifiableList(segments);
    }

    /**
     * Returns an immutable point-in-time view of the file's metadata: a shallow copy of the segment
     * list plus a defensive copy of the entry queue. Both lists are exposed as unmodifiable. Filter
     * is the single writer of {@link #entries} and {@link SpillFileSegment#currentEnd}; once filter
     * has completed (the only legitimate moment to call this method), neither changes, so the
     * shallow segment copy is a stable read-only view of the on-disk layout.
     *
     * <p>Callers are expected to be on the task thread inside {@code SpillFileReader.lock}. The
     * method itself does no synchronisation — atomicity with the drain progress fields is the
     * caller's responsibility.
     */
    public Snapshot snapshot() {
        return new Snapshot(new ArrayList<>(segments), new ArrayList<>(entries));
    }

    /**
     * Immutable view of a {@link SpillFile} produced by {@link #snapshot()}. Used by the
     * per-checkpoint readers ({@link DiskSnapshot}) to iterate the disk slice without touching the
     * live writer state. The lists are unmodifiable; segments themselves remain shared with the
     * underlying {@link SpillFile} since they are no longer mutated after filter completes.
     */
    public static final class Snapshot {
        private final List<SpillFileSegment> segments;
        private final List<Entry> entries;

        Snapshot(List<SpillFileSegment> segments, List<Entry> entries) {
            this.segments = Collections.unmodifiableList(segments);
            this.entries = Collections.unmodifiableList(entries);
        }

        /** Returns the segments captured at snapshot time, in segment-index order. */
        public List<SpillFileSegment> getSegments() {
            return segments;
        }

        /** Returns the entries captured at snapshot time, in append order. */
        public List<Entry> getEntries() {
            return entries;
        }
    }

    /**
     * Reads {@code length} bytes from {@code segmentIndex} starting at {@code offset} into a fresh
     * byte array. Exists primarily so tests and drain code can verify on-disk content without
     * re-implementing the segment lookup. Reads the open file channel directly via a position-based
     * read so concurrent appends (single-writer guarantee aside) cannot affect the channel's
     * logical position.
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
