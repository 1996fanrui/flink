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

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.function.ThrowingConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Manages spilling and replaying of filtered buffers during channel state recovery.
 *
 * <p>When the Network Buffer Pool is exhausted during recovery, filtered buffers are spilled to
 * disk. When pool space becomes available, data is replayed from disk in FIFO order to preserve
 * record ordering.
 *
 * <p>All entry metadata is maintained in memory. The spill files on disk contain only raw buffer
 * bytes.
 *
 * <p>Spill files are distributed across multiple directories in a round-robin fashion to spread I/O
 * load.
 */
class SpillingBufferManager implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(SpillingBufferManager.class);

    /** Default maximum spill file size before creating a new file (64MB). */
    static final long DEFAULT_MAX_SPILL_FILE_SIZE = 64 * 1024 * 1024;

    private final String[] spillDirs;
    private final String attemptId;
    private final int gateIndex;
    private final long maxSpillFileSize;

    /** Round-robin index for selecting the next spill directory. */
    private int dirIndex = 0;

    /** FIFO queue of spill files. */
    private final Queue<SpillFile> spillFiles = new ArrayDeque<>();

    /** Tracks alive checkpoint iterators for force-close on manager close. */
    private final Set<CheckpointSpillIterator> aliveIterators = new HashSet<>();

    /** Current writer, null if not actively spilling. */
    @Nullable private SpillFileWriter currentWriter;

    /** The SpillFile being written to by the currentWriter. */
    @Nullable private SpillFile currentSpillFile;

    /** Sequence number for spill file naming. */
    private int spillSequence = 0;

    private boolean closed = false;

    SpillingBufferManager(String[] spillDirs, String attemptId, int gateIndex) {
        this(spillDirs, attemptId, gateIndex, DEFAULT_MAX_SPILL_FILE_SIZE);
    }

    SpillingBufferManager(
            String[] spillDirs, String attemptId, int gateIndex, long maxSpillFileSize) {
        checkArgument(spillDirs.length > 0, "At least one spill directory must be provided");
        this.spillDirs = spillDirs;
        this.attemptId = attemptId;
        this.gateIndex = gateIndex;
        this.maxSpillFileSize = maxSpillFileSize;
    }

    /**
     * Spills a filtered buffer's data to disk. The caller's buffer is NOT recycled by this method.
     */
    void spillBuffer(Buffer buffer, int oldSubtaskIndex, int oldChannelIndex) throws IOException {
        checkState(!closed, "SpillingBufferManager is closed");
        if (currentWriter == null || currentWriter.getBytesWritten() >= maxSpillFileSize) {
            rotateWriter();
        }
        currentWriter.writeBuffer(buffer);
        currentSpillFile.metadata.add(
                new EntryMetadata(
                        buffer.readableBytes(),
                        buffer.getDataType(),
                        oldSubtaskIndex,
                        oldChannelIndex));
    }

    /**
     * Replays the next entry from disk into the given Network Buffer.
     *
     * <p>If an exception occurs during read, the SpillFileReader internally seeks back so the entry
     * can be retried on the next call.
     *
     * <p>When this method returns null, the caller-provided {@code networkBuffer} has NOT been
     * consumed and the caller is responsible for recycling it.
     *
     * @param networkBuffer pre-allocated Network Buffer to read data into
     * @return a ReplayResult containing the filled buffer and channel context, or null if no more
     *     disk data
     */
    @Nullable
    ReplayResult replayToBuffer(Buffer networkBuffer) throws IOException {
        checkState(!closed, "SpillingBufferManager is closed");

        // Finalize current writer to make all spilled data available
        finalizeCurrentWriter();

        while (!spillFiles.isEmpty()) {
            SpillFile spillFile = spillFiles.peek();
            if (spillFile.reader == null) {
                spillFile.reader = new SpillFileReader(spillFile.file);
            }

            EntryMetadata meta = spillFile.metadata.peek();
            boolean success = spillFile.reader.readNextTo(networkBuffer, meta.dataLength);
            if (success) {
                spillFile.metadata.poll();
                spillFile.bytesConsumed += meta.dataLength;
                networkBuffer.setDataType(meta.dataType);

                // If no more entries in this file, clean up
                if (spillFile.metadata.isEmpty()) {
                    closeAndCleanupSpillFile(spillFile);
                    spillFiles.poll();
                }

                return new ReplayResult(networkBuffer, meta.oldSubtaskIndex, meta.oldChannelIndex);
            }

            // Metadata indicates remaining entries but file returned EOF -- data corruption
            closeAndCleanupSpillFile(spillFile);
            spillFiles.poll();
            throw new IOException(
                    "Spill file ended unexpectedly: metadata indicates remaining entries but file has no more data");
        }
        return null;
    }

    /** Returns true if there is spilled data that has not yet been replayed. */
    boolean hasDiskData() {
        checkState(!closed, "SpillingBufferManager is closed");
        return !spillFiles.isEmpty() || currentWriter != null;
    }

    /**
     * Creates an iterator over unread spill data starting from the current replay position. Callers
     * must close the iterator to release file references.
     */
    CloseableIterator<CheckpointEntry> createCheckpointIterator() throws IOException {
        checkState(!closed, "SpillingBufferManager is closed");
        finalizeCurrentWriter();

        // Snapshot current files with their remaining metadata and consumed byte offsets
        Queue<CheckpointFileEntry> snapshot = new ArrayDeque<>();
        for (SpillFile sf : spillFiles) {
            sf.refCount.incrementAndGet();
            // Snapshot the remaining metadata as a new queue
            Queue<EntryMetadata> metadataSnapshot = new ArrayDeque<>(sf.metadata);
            snapshot.add(new CheckpointFileEntry(sf, metadataSnapshot, sf.bytesConsumed));
        }

        CheckpointSpillIterator iterator = new CheckpointSpillIterator(snapshot);
        aliveIterators.add(iterator);
        return iterator;
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;

        // Force close all alive iterators
        for (CheckpointSpillIterator iter : new ArrayList<>(aliveIterators)) {
            try {
                iter.close();
            } catch (Exception e) {
                LOG.warn("Failed to close checkpoint iterator", e);
            }
        }
        aliveIterators.clear();

        // currentSpillFile is not in spillFiles queue (not yet finalized), handle separately
        if (currentWriter != null) {
            File writerFile = currentWriter.getFile();
            try {
                currentWriter.close();
            } catch (IOException e) {
                LOG.warn("Failed to close spill writer", e);
            } finally {
                writerFile.delete();
            }
            currentWriter = null;
            currentSpillFile = null;
        }

        // Force cleanup all spill files
        while (!spillFiles.isEmpty()) {
            SpillFile sf = spillFiles.poll();
            try {
                sf.close();
            } catch (IOException e) {
                LOG.warn("Failed to close spill file reader", e);
            } finally {
                sf.file.delete();
            }
        }
    }

    private void rotateWriter() throws IOException {
        finalizeCurrentWriter();

        String dir = spillDirs[dirIndex % spillDirs.length];
        dirIndex++;

        File dirFile = new File(dir);
        if (!dirFile.mkdirs() && !dirFile.isDirectory()) {
            throw new IOException("Failed to create spill directory: " + dir);
        }

        File spillFile =
                new File(
                        dir,
                        String.format(
                                "channel-state-spill-%s-%d-%d.tmp",
                                attemptId, gateIndex, spillSequence++));
        currentWriter = new SpillFileWriter(spillFile);
        currentSpillFile = new SpillFile(spillFile);
    }

    private void finalizeCurrentWriter() throws IOException {
        if (currentWriter != null) {
            currentWriter.close();
            spillFiles.add(currentSpillFile);
            currentWriter = null;
            currentSpillFile = null;
        }
    }

    private void closeAndCleanupSpillFile(SpillFile spillFile) throws IOException {
        spillFile.close();
        if (spillFile.refCount.get() <= 0) {
            spillFile.file.delete();
        }
    }

    /** In-memory metadata for a single spilled entry. */
    static class EntryMetadata {
        final int dataLength;
        final Buffer.DataType dataType;
        final int oldSubtaskIndex;
        final int oldChannelIndex;

        EntryMetadata(
                int dataLength,
                Buffer.DataType dataType,
                int oldSubtaskIndex,
                int oldChannelIndex) {
            this.dataLength = dataLength;
            this.dataType = dataType;
            this.oldSubtaskIndex = oldSubtaskIndex;
            this.oldChannelIndex = oldChannelIndex;
        }
    }

    /** Holds a SpillFile together with a metadata snapshot for checkpoint iteration. */
    private static class CheckpointFileEntry {
        final SpillFile spillFile;
        final Queue<EntryMetadata> metadataSnapshot;

        /** Byte offset in the file to skip already-consumed entries. */
        final long startOffset;

        CheckpointFileEntry(
                SpillFile spillFile, Queue<EntryMetadata> metadataSnapshot, long startOffset) {
            this.spillFile = spillFile;
            this.metadataSnapshot = metadataSnapshot;
            this.startOffset = startOffset;
        }
    }

    /**
     * Entry returned by the checkpoint iterator. Provides metadata and a {@link #writeTo} method to
     * stream data directly from the spill file to an OutputStream without any intermediate buffer.
     *
     * <p><b>Contract:</b> {@link #writeTo(OutputStream)} must be called exactly once, before the
     * next {@code iterator.next()} call. The underlying SpillFileReader is sequential, so calling
     * writeTo advances the reader past this entry's bytes. Skipping writeTo or calling it twice
     * corrupts the reader position for subsequent entries.
     */
    static class CheckpointEntry {
        final int oldSubtaskIndex;
        final int oldChannelIndex;
        final Buffer.DataType dataType;
        final int dataLength;

        private final ThrowingConsumer<OutputStream, IOException> dataWriter;
        private boolean consumed = false;

        CheckpointEntry(
                int oldSubtaskIndex,
                int oldChannelIndex,
                Buffer.DataType dataType,
                int dataLength,
                ThrowingConsumer<OutputStream, IOException> dataWriter) {
            this.oldSubtaskIndex = oldSubtaskIndex;
            this.oldChannelIndex = oldChannelIndex;
            this.dataType = dataType;
            this.dataLength = dataLength;
            this.dataWriter = dataWriter;
        }

        /** Writes the entry's data to the output stream. Must be called exactly once. */
        void writeTo(OutputStream out) throws IOException {
            checkState(!consumed, "CheckpointEntry.writeTo() has already been called");
            consumed = true;
            dataWriter.accept(out);
        }
    }

    /**
     * Iterator that reads spill files for checkpoint snapshotting, returning {@link
     * CheckpointEntry} with channel context and lazy data access. Each entry's data is NOT read at
     * iteration time; instead, the caller must invoke {@link CheckpointEntry#writeTo(OutputStream)}
     * before calling {@code next()} again. On close, decrements ref counts on remaining
     * (unconsumed) spill files.
     *
     * <p>Non-static inner class to access {@code aliveIterators} for lifecycle tracking.
     */
    private class CheckpointSpillIterator implements CloseableIterator<CheckpointEntry> {
        private final Queue<CheckpointFileEntry> entries;
        @Nullable private SpillFileReader currentReader;
        @Nullable private CheckpointEntry nextEntry;
        private boolean iteratorClosed = false;

        CheckpointSpillIterator(Queue<CheckpointFileEntry> entries) {
            this.entries = entries;
        }

        @Override
        public boolean hasNext() {
            if (iteratorClosed) {
                return false;
            }
            if (nextEntry != null) {
                return true;
            }
            try {
                nextEntry = readNextEntry();
                return nextEntry != null;
            } catch (IOException e) {
                throw new RuntimeException("Failed to read spill file for checkpoint", e);
            }
        }

        @Override
        public CheckpointEntry next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            CheckpointEntry result = nextEntry;
            nextEntry = null;
            return result;
        }

        @Nullable
        private CheckpointEntry readNextEntry() throws IOException {
            while (!entries.isEmpty()) {
                CheckpointFileEntry cfe = entries.peek();

                if (currentReader == null) {
                    currentReader = new SpillFileReader(cfe.spillFile.file);
                    if (cfe.startOffset > 0) {
                        currentReader.skipBytes(cfe.startOffset);
                    }
                }

                if (!cfe.metadataSnapshot.isEmpty()) {
                    EntryMetadata meta = cfe.metadataSnapshot.poll();
                    final SpillFileReader reader = currentReader;
                    final int length = meta.dataLength;
                    return new CheckpointEntry(
                            meta.oldSubtaskIndex,
                            meta.oldChannelIndex,
                            meta.dataType,
                            meta.dataLength,
                            out -> reader.readNextTo(out, length));
                }

                // Current file exhausted, move to next
                currentReader.close();
                currentReader = null;
                cfe.spillFile.refCount.decrementAndGet();
                entries.poll();
            }
            return null;
        }

        @Override
        public void close() throws Exception {
            if (iteratorClosed) {
                return;
            }
            iteratorClosed = true;

            if (currentReader != null) {
                currentReader.close();
                currentReader = null;
                if (!entries.isEmpty()) {
                    entries.poll().spillFile.refCount.decrementAndGet();
                }
            }
            for (CheckpointFileEntry cfe : entries) {
                cfe.spillFile.refCount.decrementAndGet();
            }
            entries.clear();

            nextEntry = null;

            aliveIterators.remove(this);
        }
    }

    /** Tracks a spill file, its reader state, metadata queue, and checkpoint reference count. */
    static class SpillFile {
        final File file;
        final Queue<EntryMetadata> metadata = new ArrayDeque<>();

        /**
         * Accessed from both recovery and checkpoint threads. TOCTOU in {@link
         * #closeAndCleanupSpillFile} is benign: worst case is delayed file deletion until {@link
         * #close()}.
         */
        final AtomicInteger refCount = new AtomicInteger(0);

        /** Total bytes consumed by replay, used to calculate checkpoint start offset. */
        long bytesConsumed = 0;

        @Nullable SpillFileReader reader;

        SpillFile(File file) {
            this.file = file;
        }

        void close() throws IOException {
            if (reader != null) {
                reader.close();
                reader = null;
            }
        }
    }

    /** Result of replaying a buffer from disk, including the channel context for delivery. */
    static class ReplayResult {
        final Buffer buffer;
        final int oldSubtaskIndex;
        final int oldChannelIndex;

        ReplayResult(Buffer buffer, int oldSubtaskIndex, int oldChannelIndex) {
            this.buffer = buffer;
            this.oldSubtaskIndex = oldSubtaskIndex;
            this.oldChannelIndex = oldChannelIndex;
        }
    }
}
