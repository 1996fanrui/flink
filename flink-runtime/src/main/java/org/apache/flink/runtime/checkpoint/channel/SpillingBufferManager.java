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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Manages spilling and replaying of filtered buffers during channel state recovery.
 *
 * <p>When the Network Buffer Pool is exhausted during recovery, filtered buffers are spilled to
 * disk. When pool space becomes available, data is replayed from disk in FIFO order to preserve
 * record ordering.
 *
 * <p>Each spill file entry includes channel context (oldSubtaskIndex, oldChannelIndex) so that
 * replayed data can be delivered to the correct channel during Phase 2 disk drain.
 */
class SpillingBufferManager implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(SpillingBufferManager.class);

    /** Default maximum spill file size before creating a new file (64MB). */
    static final long DEFAULT_MAX_SPILL_FILE_SIZE = 64 * 1024 * 1024;

    private final String spillDir;
    private final String attemptId;
    private final int gateIndex;
    private final long maxSpillFileSize;

    /** FIFO queue of spill files. */
    private final Queue<SpillFile> spillFiles = new ArrayDeque<>();

    /** Current writer, null if not actively spilling. */
    @Nullable private SpillFileWriter currentWriter;

    /** Sequence number for spill file naming. */
    private int spillSequence = 0;

    private boolean closed = false;

    SpillingBufferManager(String spillDir, String attemptId, int gateIndex) {
        this(spillDir, attemptId, gateIndex, DEFAULT_MAX_SPILL_FILE_SIZE);
    }

    SpillingBufferManager(String spillDir, String attemptId, int gateIndex, long maxSpillFileSize) {
        this.spillDir = spillDir;
        this.attemptId = attemptId;
        this.gateIndex = gateIndex;
        this.maxSpillFileSize = maxSpillFileSize;

        cleanupOldAttemptFiles();
    }

    /**
     * Spills a filtered buffer's data to disk with its channel context. A new spill file is created
     * if the current one exceeds the size limit. The caller's buffer is NOT recycled by this
     * method.
     */
    void spillBuffer(Buffer buffer, int oldSubtaskIndex, int oldChannelIndex) throws IOException {
        if (currentWriter == null || currentWriter.getBytesWritten() >= maxSpillFileSize) {
            rotateWriter();
        }
        currentWriter.writeBuffer(buffer, oldSubtaskIndex, oldChannelIndex);
    }

    /**
     * Replays the next entry from disk into the given Network Buffer.
     *
     * @param networkBuffer pre-allocated Network Buffer to copy data into
     * @return a ReplayResult containing the filled buffer and channel context, or null if no more
     *     disk data
     */
    @Nullable
    ReplayResult replayToBuffer(Buffer networkBuffer) throws IOException {
        // Finalize the current writer to ensure all spilled data is available for replay
        if (currentWriter != null) {
            finalizeCurrentWriter();
        }

        while (!spillFiles.isEmpty()) {
            SpillFile spillFile = spillFiles.peek();
            if (spillFile.reader == null) {
                spillFile.reader = new SpillFileReader(spillFile.file);
            }

            SpillFileReader.SpillEntry entry = spillFile.reader.readNext();
            if (entry != null) {
                try {
                    // Copy disk data to the network buffer
                    copyBufferData(entry.buffer, networkBuffer);
                    networkBuffer.setDataType(entry.buffer.getDataType());

                    // Eagerly clean up if no more data remains in this file
                    if (!spillFile.reader.hasRemaining()) {
                        spillFile.close();
                        if (spillFile.refCount.get() <= 0) {
                            spillFile.file.delete();
                        }
                        spillFiles.poll();
                    }

                    return new ReplayResult(
                            networkBuffer, entry.oldSubtaskIndex, entry.oldChannelIndex);
                } finally {
                    entry.buffer.recycleBuffer();
                }
            }

            // Current file exhausted (empty file or read returned null), clean up
            spillFile.close();
            if (spillFile.refCount.get() <= 0) {
                spillFile.file.delete();
            }
            spillFiles.poll();
        }
        return null;
    }

    /**
     * Returns true if there is spilled data on disk that has not yet been replayed. Finalizes the
     * current writer if open to ensure all data is accounted for.
     */
    boolean hasDiskData() {
        if (currentWriter != null) {
            try {
                finalizeCurrentWriter();
            } catch (IOException e) {
                LOG.warn("Failed to finalize current spill writer", e);
            }
        }
        return !spillFiles.isEmpty();
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;

        // Close current writer and delete its file
        if (currentWriter != null) {
            File writerFile = currentWriter.getFile();
            try {
                currentWriter.close();
            } catch (IOException e) {
                LOG.warn("Failed to close spill writer", e);
            }
            if (writerFile.exists()) {
                writerFile.delete();
            }
            currentWriter = null;
        }

        // Force cleanup all spill files
        while (!spillFiles.isEmpty()) {
            SpillFile sf = spillFiles.poll();
            try {
                sf.close();
            } catch (IOException e) {
                LOG.warn("Failed to close spill file reader", e);
            }
            if (sf.file.exists()) {
                sf.file.delete();
            }
        }
    }

    private void rotateWriter() throws IOException {
        finalizeCurrentWriter();

        File dir = new File(spillDir);
        if (!dir.exists()) {
            dir.mkdirs();
        }

        File spillFile =
                new File(
                        spillDir,
                        String.format(
                                "channel-state-spill-%s-%d-%d.tmp",
                                attemptId, gateIndex, spillSequence++));
        currentWriter = new SpillFileWriter(spillFile);
    }

    private void finalizeCurrentWriter() throws IOException {
        if (currentWriter != null) {
            currentWriter.close();
            spillFiles.add(new SpillFile(currentWriter.getFile()));
            currentWriter = null;
        }
    }

    private void cleanupOldAttemptFiles() {
        File dir = new File(spillDir);
        if (!dir.exists() || !dir.isDirectory()) {
            return;
        }
        File[] oldFiles =
                dir.listFiles(
                        (d, name) ->
                                name.startsWith("channel-state-spill-")
                                        && name.endsWith(".tmp")
                                        && !name.contains("-" + attemptId + "-"));
        if (oldFiles != null) {
            for (File f : oldFiles) {
                if (f.delete()) {
                    LOG.debug("Cleaned up old spill file: {}", f);
                }
            }
        }
    }

    private static void copyBufferData(Buffer source, Buffer target) {
        int dataLength = source.readableBytes();
        source.getMemorySegment()
                .copyTo(
                        source.getMemorySegmentOffset() + source.getReaderIndex(),
                        target.getMemorySegment(),
                        target.getMemorySegmentOffset(),
                        dataLength);
        target.setSize(dataLength);
    }

    /**
     * Creates an iterator for Checkpoint to read all unloaded spill data. The iterator holds
     * references on the snapshotted spill files to prevent file deletion during iteration. Callers
     * must close the iterator to release the references.
     */
    CloseableIterator<Buffer> createCheckpointIterator() throws IOException {
        // Finalize current writer so all spilled data is available
        if (currentWriter != null) {
            finalizeCurrentWriter();
        }

        // Snapshot current files and increase ref counts
        Queue<SpillFile> snapshot = new ArrayDeque<>();
        for (SpillFile sf : spillFiles) {
            sf.refCount.incrementAndGet();
            snapshot.add(sf);
        }

        return new CheckpointSpillIterator(snapshot);
    }

    /**
     * Iterator that reads spill files for checkpoint snapshotting. Each returned buffer is an
     * independent copy allocated with {@link
     * org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler} and must be recycled by the
     * caller. On close, decrements ref counts on remaining (unconsumed) spill files.
     */
    private static class CheckpointSpillIterator implements CloseableIterator<Buffer> {
        private final Queue<SpillFile> files;
        @Nullable private SpillFileReader currentReader;
        @Nullable private Buffer nextBuffer;

        CheckpointSpillIterator(Queue<SpillFile> files) {
            this.files = files;
        }

        @Override
        public boolean hasNext() {
            if (nextBuffer != null) {
                return true;
            }
            try {
                nextBuffer = readNextBuffer();
                return nextBuffer != null;
            } catch (IOException e) {
                throw new RuntimeException("Failed to read spill file for checkpoint", e);
            }
        }

        @Override
        public Buffer next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            Buffer result = nextBuffer;
            nextBuffer = null;
            return result;
        }

        @Nullable
        private Buffer readNextBuffer() throws IOException {
            while (true) {
                if (currentReader != null) {
                    SpillFileReader.SpillEntry entry = currentReader.readNext();
                    if (entry != null) {
                        // For checkpoint, we only need the buffer data.
                        // Channel context is not needed since checkpoint stores channel info
                        // separately.
                        return entry.buffer;
                    }
                    currentReader.close();
                    currentReader = null;
                }

                if (files.isEmpty()) {
                    return null;
                }

                SpillFile sf = files.poll();
                sf.refCount.decrementAndGet();
                currentReader = new SpillFileReader(sf.file);
            }
        }

        @Override
        public void close() throws Exception {
            if (currentReader != null) {
                currentReader.close();
                currentReader = null;
            }
            // Release ref counts for remaining unconsumed files
            for (SpillFile sf : files) {
                sf.refCount.decrementAndGet();
            }
            files.clear();

            if (nextBuffer != null) {
                nextBuffer.recycleBuffer();
                nextBuffer = null;
            }
        }
    }

    /** Tracks a spill file, its reader state, and checkpoint reference count. */
    static class SpillFile {
        final File file;
        final AtomicInteger refCount = new AtomicInteger(0);
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
