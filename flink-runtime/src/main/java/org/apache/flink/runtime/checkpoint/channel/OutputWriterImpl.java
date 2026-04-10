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
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.consumer.RecoveredBufferStoreImpl;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.function.Supplier;

/**
 * Implementation of {@link OutputWriter} that manages three data paths:
 *
 * <ul>
 *   <li><b>P1 (buffer)</b>: Data is written directly to a network buffer and delivered to the
 *       target store.
 *   <li><b>P2 (spill to disk)</b>: When no buffer is available, data is written to a spill file and
 *       tracked as a {@link SpillEntry}.
 *   <li><b>P3 (replay from disk)</b>: When buffers become available later, spilled entries are
 *       eagerly drained from disk into buffers and delivered to stores.
 * </ul>
 *
 * <p>On {@link #close()}, all remaining spill entries are drained using a blocking buffer supplier,
 * spill files are deleted, and all stores are marked complete.
 */
@Internal
public class OutputWriterImpl implements OutputWriter {

    /** Blocking supplier that may wait for a buffer to become available. */
    @FunctionalInterface
    public interface BlockingSupplier<T> {
        T get() throws InterruptedException, IOException;
    }

    private final Map<InputChannelInfo, RecoveredBufferStoreImpl> storesByChannel;
    private final String[] spillDirs;
    private final int memorySegmentSize;
    private final Supplier<Buffer> bufferSupplier;
    private final BlockingSupplier<Buffer> blockingBufferSupplier;

    // Active buffer state
    private Buffer activeBuffer;
    private InputChannelInfo activeChannelInfo;
    private int activeBufferPosition;

    // Active spill entry accumulation state
    private long activeSpillEntryStartOffset;
    private int activeSpillEntryLength;
    private InputChannelInfo activeSpillChannelInfo;
    private SpillFileReader activeSpillFileReader;

    // Spill infrastructure
    private SpillFileWriter spillFileWriter;
    private final Queue<SpillEntry> spillEntryQueue;
    private final List<SpillFileReader> allSpillFileReaders;
    private int lastKnownFileCount;

    // Lifecycle flags
    private boolean flushed;
    private boolean closed;

    /**
     * Creates a new OutputWriterImpl.
     *
     * @param storesByChannel per-channel stores for delivering recovered buffers
     * @param spillDirs directories for spill files
     * @param memorySegmentSize the size of a memory segment / network buffer
     * @param bufferSupplier non-blocking supplier, returns null when exhausted
     * @param blockingBufferSupplier blocking supplier used during close() drain
     * @throws IOException if spillDirs is empty
     */
    public OutputWriterImpl(
            Map<InputChannelInfo, RecoveredBufferStoreImpl> storesByChannel,
            String[] spillDirs,
            int memorySegmentSize,
            Supplier<Buffer> bufferSupplier,
            BlockingSupplier<Buffer> blockingBufferSupplier)
            throws IOException {
        if (spillDirs.length == 0) {
            throw new IOException("Spill directories must not be empty");
        }
        this.storesByChannel = storesByChannel;
        this.spillDirs = spillDirs;
        this.memorySegmentSize = memorySegmentSize;
        this.bufferSupplier = bufferSupplier;
        this.blockingBufferSupplier = blockingBufferSupplier;
        this.spillEntryQueue = new ArrayDeque<>();
        this.allSpillFileReaders = new ArrayList<>();
        this.lastKnownFileCount = 0;
        this.activeSpillEntryLength = 0;
    }

    @Override
    public void write(byte[] data, int length, InputChannelInfo channelInfo)
            throws IOException, InterruptedException {
        if (flushed || closed) {
            throw new IllegalStateException("Cannot write after " + (closed ? "close" : "flush"));
        }

        // Channel change detection: flush active buffer and seal active spill entry
        if (activeChannelInfo != null && !activeChannelInfo.equals(channelInfo)) {
            flushActiveBuffer();
            sealActiveSpillEntry();
        }
        activeChannelInfo = channelInfo;

        // P3 eager drain: replay spilled entries while non-blocking buffers are available
        eagerDrain();

        // Write data to the backend (buffer or file)
        writeToBackend(data, 0, length, channelInfo);
    }

    @Override
    public void flush() throws IOException {
        if (flushed || closed) {
            return;
        }

        // Flush active buffer (partial)
        flushActiveBuffer();
        // Seal any active spill entry
        sealActiveSpillEntry();

        flushed = true;
    }

    @Override
    public void close() throws IOException, InterruptedException {
        if (closed) {
            return;
        }
        closed = true;

        // Flush active buffer and seal spill entry if not already flushed
        if (!flushed) {
            flushActiveBuffer();
            sealActiveSpillEntry();
            flushed = true;
        }

        // Blocking drain: drain all spill entries using blocking buffer supplier
        while (!spillEntryQueue.isEmpty()) {
            Buffer buffer = blockingBufferSupplier.get();
            SpillEntry entry = spillEntryQueue.poll();
            loadEntryIntoBuffer(entry, buffer);
            RecoveredBufferStoreImpl store = storesByChannel.get(entry.getChannelInfo());
            store.addBuffer(buffer);
            store.removePendingSpillEntry(entry);
        }

        // Cleanup spill infrastructure
        if (spillFileWriter != null) {
            spillFileWriter.close();
            spillFileWriter.deleteAllFiles();
        }
        for (SpillFileReader reader : allSpillFileReaders) {
            reader.close();
        }

        // Mark all stores as complete
        for (RecoveredBufferStoreImpl store : storesByChannel.values()) {
            store.markComplete();
        }
    }

    /**
     * Writes bytes into the current backend, handling buffer fill, spill transitions, and the
     * downgrade-only rule (once a write call falls to file, it stays on file).
     */
    private void writeToBackend(byte[] data, int offset, int length, InputChannelInfo channelInfo)
            throws IOException {
        boolean downgradedToFile = false;
        int remaining = length;
        int pos = offset;

        while (remaining > 0) {
            if (!downgradedToFile && activeBuffer != null) {
                // Write into active buffer
                int space = memorySegmentSize - activeBufferPosition;
                int toWrite = Math.min(space, remaining);
                activeBuffer.getMemorySegment().put(activeBufferPosition, data, pos, toWrite);
                activeBufferPosition += toWrite;
                pos += toWrite;
                remaining -= toWrite;

                // Buffer full — deliver to store
                if (activeBufferPosition >= memorySegmentSize) {
                    activeBuffer.setSize(memorySegmentSize);
                    storesByChannel.get(channelInfo).addBuffer(activeBuffer);
                    activeBuffer = null;
                    activeBufferPosition = 0;
                }
            } else if (!downgradedToFile && activeBuffer == null) {
                // Try to acquire a new buffer
                if (!spillEntryQueue.isEmpty()) {
                    // Disk has data — must stay on file to preserve ordering
                    downgradedToFile = true;
                } else {
                    Buffer newBuffer = bufferSupplier.get();
                    if (newBuffer != null) {
                        // P1: got a buffer
                        activeBuffer = newBuffer;
                        activeBufferPosition = 0;
                        // Loop back to write into this buffer
                    } else {
                        // No buffer available — downgrade to file
                        downgradedToFile = true;
                    }
                }
            }

            if (downgradedToFile && remaining > 0) {
                // P2: write to spill file
                int toWrite = Math.min(remaining, memorySegmentSize - activeSpillEntryLength);
                long fileOffset = writeToSpillFile(data, pos, toWrite);

                // Start a new spill entry if needed
                if (activeSpillEntryLength == 0) {
                    activeSpillEntryStartOffset = fileOffset;
                    activeSpillChannelInfo = channelInfo;
                    activeSpillFileReader = getCurrentSpillFileReader();
                }
                activeSpillEntryLength += toWrite;
                pos += toWrite;
                remaining -= toWrite;

                // Seal entry when it reaches memorySegmentSize (1:1 buffer alignment)
                if (activeSpillEntryLength >= memorySegmentSize) {
                    sealActiveSpillEntry();
                }
            }
        }
    }

    /** Eagerly drains spill entries while non-blocking buffers are available. */
    private void eagerDrain() throws IOException {
        while (!spillEntryQueue.isEmpty()) {
            Buffer buffer = bufferSupplier.get();
            if (buffer == null) {
                break;
            }
            SpillEntry entry = spillEntryQueue.poll();
            loadEntryIntoBuffer(entry, buffer);
            RecoveredBufferStoreImpl store = storesByChannel.get(entry.getChannelInfo());
            store.addBuffer(buffer);
            store.removePendingSpillEntry(entry);
        }
    }

    /** Loads a spill entry's data from disk into a buffer. */
    private void loadEntryIntoBuffer(SpillEntry entry, Buffer buffer) throws IOException {
        byte[] tmp = new byte[entry.getLength()];
        entry.getFileReader().read(entry.getOffset(), tmp, entry.getLength());
        buffer.getMemorySegment().put(0, tmp, 0, entry.getLength());
        buffer.setSize(entry.getLength());
    }

    /** Flushes the active buffer (if any) to the target store. */
    private void flushActiveBuffer() {
        if (activeBuffer != null && activeBufferPosition > 0) {
            activeBuffer.setSize(activeBufferPosition);
            storesByChannel.get(activeChannelInfo).addBuffer(activeBuffer);
        } else if (activeBuffer != null) {
            // Buffer allocated but nothing written — recycle it
            activeBuffer.recycleBuffer();
        }
        activeBuffer = null;
        activeBufferPosition = 0;
    }

    /** Seals the currently accumulating spill entry (if any) and adds it to the queue and store. */
    private void sealActiveSpillEntry() {
        if (activeSpillEntryLength > 0 && activeSpillChannelInfo != null) {
            SpillEntry entry =
                    new SpillEntry(
                            activeSpillChannelInfo,
                            activeSpillFileReader,
                            activeSpillEntryStartOffset,
                            activeSpillEntryLength);
            spillEntryQueue.add(entry);
            storesByChannel.get(activeSpillChannelInfo).addPendingSpillEntry(entry);
            activeSpillEntryLength = 0;
            activeSpillChannelInfo = null;
            activeSpillFileReader = null;
        }
    }

    /** Writes data to the spill file, creating the SpillFileWriter lazily if needed. */
    private long writeToSpillFile(byte[] data, int offset, int length) throws IOException {
        if (spillFileWriter == null) {
            spillFileWriter = new SpillFileWriter(spillDirs, memorySegmentSize);
            lastKnownFileCount = 0;
        }
        return spillFileWriter.write(data, offset, length);
    }

    /**
     * Returns the SpillFileReader for the current spill file. Tracks file rotations and creates new
     * readers as needed.
     */
    private SpillFileReader getCurrentSpillFileReader() throws IOException {
        int currentFileCount = spillFileWriter.getAllFiles().size();
        if (currentFileCount > lastKnownFileCount) {
            // New file was created (initial or rotation) — create a reader for it
            SpillFileReader reader = spillFileWriter.getCurrentFileReader();
            allSpillFileReaders.add(reader);
            lastKnownFileCount = currentFileCount;
            return reader;
        }
        // Return the most recent reader
        return allSpillFileReaders.get(allSpillFileReaders.size() - 1);
    }
}
