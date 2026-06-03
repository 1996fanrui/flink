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
 * Append-only, segmented storage for recovered channel-state buffers.
 *
 * <p>A segment is one physical spill file. An entry is one buffer payload appended to a segment.
 *
 * <p>Mutations are single-writer and intentionally unsynchronized; callers must serialize them via
 * the channel IO executor.
 */
@Internal
public final class SpillFile implements Closeable {

    public static final long DEFAULT_SEGMENT_SIZE_BYTES = 64L * 1024 * 1024;

    /**
     * Fixed per-record header preceding each payload on disk: {@code gateIdx (int) | channelIdx
     * (int) | length (int)}. Metadata lives inline in the file rather than on the JVM heap, so a
     * recovery with millions of buffers no longer keeps a per-buffer entry object alive.
     */
    static final int HEADER_BYTES = 3 * Integer.BYTES;

    static final class SpillFileSegment implements Closeable {
        final int segmentIndex;
        final Path path;
        final FileChannel channel;

        /**
         * Bytes written so far: the rotation cursor for writers and the read boundary for readers.
         */
        long currentEnd;

        SpillFileSegment(int segmentIndex, Path path, FileChannel channel) {
            this.segmentIndex = segmentIndex;
            this.path = path;
            this.channel = channel;
            this.currentEnd = 0L;
        }

        @Override
        public void close() throws IOException {
            channel.close();
        }
    }

    private final Path baseDir;
    private final long segmentSizeBytes;
    private final int maxEntryLength;
    private final List<SpillFileSegment> segments = new ArrayList<>();

    /** Reused across appends; safe because mutations are single-writer (see class doc). */
    private final ByteBuffer headerBuffer = ByteBuffer.allocate(HEADER_BYTES);

    private boolean closed = false;

    private final AtomicInteger refCount = new AtomicInteger(0);

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

    /** Appends one non-empty payload without splitting it across segments. */
    public void append(InputChannelInfo channelInfo, ByteBuffer payload) throws IOException {
        if (closed) {
            throw new IllegalStateException(
                    "Cannot append to a closed SpillFile (baseDir=" + baseDir + ").");
        }
        checkNotNull(channelInfo);
        checkNotNull(payload);

        int length = payload.remaining();
        if (length == 0) {
            return;
        }

        SpillFileSegment active = activeSegmentFor(length);

        headerBuffer.clear();
        headerBuffer.putInt(channelInfo.getGateIdx());
        headerBuffer.putInt(channelInfo.getInputChannelIdx());
        headerBuffer.putInt(length);
        headerBuffer.flip();

        writeFully(active, headerBuffer);
        writeFully(active, payload);
        active.currentEnd += HEADER_BYTES + length;
    }

    private void writeFully(SpillFileSegment segment, ByteBuffer src) throws IOException {
        while (src.hasRemaining()) {
            int n = segment.channel.write(src);
            if (n <= 0) {
                throw new IOException(
                        "FileChannel.write returned " + n + " on segment " + segment.path);
            }
        }
    }

    private SpillFileSegment activeSegmentFor(int payloadLength) throws IOException {
        if (segments.isEmpty()) {
            return openNewSegment();
        }
        SpillFileSegment current = segments.get(segments.size() - 1);
        if (current.currentEnd + HEADER_BYTES + payloadLength > segmentSizeBytes) {
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

    /** Acquires a lifecycle grant for a reader or handoff owner. */
    public void acquire() {
        refCount.incrementAndGet();
    }

    /** Releases a lifecycle grant; the last release removes all segment files. */
    public void release() throws IOException {
        if (refCount.decrementAndGet() == 0) {
            if (cleanedUp.compareAndSet(false, true)) {
                closed = true;
                deleteAllSegments();
            }
        }
    }

    /** Forces cleanup, even when lifecycle grants are still outstanding. */
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

    int maxEntryLength() {
        return maxEntryLength;
    }

    List<SpillFileSegment> segments() {
        return Collections.unmodifiableList(segments);
    }

    public SpillFileReader reader() {
        return SpillFileReader.openRoot(this);
    }
}
