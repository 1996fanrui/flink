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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Sealed container for recovered channel-state data written to spill files.
 *
 * <p>Holds a list of file paths (in write order) and an in-memory segment locator table. Each
 * {@link FetchedSegment} entry records the {@code (channelInfo, fileIndex, offset, length)} needed
 * to read one per-channel segment from disk on demand, without materializing the segment body in
 * memory.
 *
 * <p>The segment locator table is built by the writer ({@link FetchedChannelStateWriter}) during
 * writing and becomes read-only once the writer is closed. The file list grows as the writer
 * rotates to new files (one rotation per 64 MB soft limit), and is also sealed on writer close.
 *
 * <p>File lifecycle is managed by {@link #acquire()} / {@link #release()} reference counting. Files
 * are deleted only when the last lifecycle grant is released (i.e. when both the drain reader and
 * all snapshot readers have finished). This invariant is preserved from the previous
 * implementation; only the deletion target changes from segment {@link
 * java.nio.channels.FileChannel} objects to plain {@link Path} entries.
 *
 * <p>Mutations (file and segment list appends) are single-writer and intentionally unsynchronized;
 * callers must serialize them via the channel IO executor.
 */
@Internal
public final class FetchedChannelState implements Closeable {

    /**
     * Soft upper bound on a single spill file's size in bytes. File rotation is checked only after
     * a segment is fully sealed, so a file may exceed this limit when a single channel segment is
     * larger than 64 MB.
     */
    public static final long DEFAULT_SEGMENT_SIZE_BYTES = 64L * 1024 * 1024;

    /** Ordered list of spill file paths, one entry per physical file. Read-only after sealing. */
    private final List<Path> files = new ArrayList<>();

    /**
     * Ordered segment locator table. One entry per channel switch. Read-only after sealing.
     * Quantity = number of channel switches, far fewer than the total record count.
     */
    private final List<FetchedSegment> segmentLocators = new ArrayList<>();

    // close() and release() may be called from different threads; volatile ensures visibility.
    private volatile boolean closed = false;

    private final AtomicInteger refCount = new AtomicInteger(0);

    private final AtomicBoolean cleanedUp = new AtomicBoolean(false);

    // -------------------------------------------------------------------------------------------
    // Write-phase API (called by FetchedChannelStateWriter, single-writer)
    // -------------------------------------------------------------------------------------------

    /**
     * Registers a new spill file path. Called by the writer when it opens a new file.
     *
     * @return the index of the newly added file (used to construct {@link FetchedSegment} entries).
     */
    int addFile(Path filePath) {
        checkNotNull(filePath);
        int index = files.size();
        files.add(filePath);
        return index;
    }

    /**
     * Appends a sealed segment locator entry to the in-memory table.
     *
     * <p>Called by the writer each time a channel segment is fully written (channel switch, file
     * rotation, or writer close).
     */
    void appendSegment(FetchedSegment segment) {
        checkNotNull(segment);
        segmentLocators.add(segment);
    }

    // -------------------------------------------------------------------------------------------
    // Read-phase API (called by the reader after the writer is sealed)
    // -------------------------------------------------------------------------------------------

    /**
     * Opens a root reader covering all segments from the beginning. The returned reader holds one
     * lifecycle grant and must be closed when done.
     */
    public FetchedChannelStateReader reader() {
        return FetchedChannelStateReader.openRoot(this);
    }

    /** Returns the ordered list of spill file paths. Read-only view. */
    public List<Path> files() {
        return Collections.unmodifiableList(files);
    }

    /**
     * Returns the segment locator table built during writing. Each entry records the location of
     * one per-channel segment on disk. Read-only view.
     */
    public List<FetchedSegment> segments() {
        return Collections.unmodifiableList(segmentLocators);
    }

    // -------------------------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------------------------

    /** Acquires a lifecycle grant for a reader or handoff owner. */
    public void acquire() {
        refCount.incrementAndGet();
    }

    /**
     * Releases a lifecycle grant. When the last grant is released (refCount reaches zero), all
     * spill files are deleted. This preserves the invariant that files exist for the lifetime of
     * all readers (drain + snapshot) and are cleaned up exactly once when the last reader finishes.
     */
    public void release() throws IOException {
        if (refCount.decrementAndGet() == 0) {
            if (cleanedUp.compareAndSet(false, true)) {
                closed = true;
                deleteAllFiles();
            }
        }
    }

    /** Forces cleanup even when lifecycle grants are still outstanding. */
    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        if (cleanedUp.compareAndSet(false, true)) {
            deleteAllFiles();
        }
    }

    private void deleteAllFiles() throws IOException {
        IOException firstError = null;
        for (Path file : files) {
            try {
                Files.deleteIfExists(file);
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
}
