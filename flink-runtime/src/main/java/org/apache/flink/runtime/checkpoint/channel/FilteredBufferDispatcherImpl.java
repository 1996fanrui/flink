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
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.function.Function;

/**
 * Implementation of {@link FilteredBufferDispatcher} that manages three data paths:
 *
 * <ul>
 *   <li><b>P1 (buffer)</b>: Data is written directly to a network buffer and delivered to the
 *       target store.
 *   <li><b>P2 (spill to disk)</b>: When no buffer is available, data is written to a spill file and
 *       tracked as a {@link FilteredSpillFile.Entry}.
 *   <li><b>P3 (replay from disk)</b>: When buffers become available later, spilled entries are
 *       eagerly drained from disk into buffers and delivered to stores.
 * </ul>
 *
 * <p>On {@link #close()}, all remaining spill entries are drained using a blocking buffer supplier,
 * spill files are deleted, and all stores are marked complete.
 */
@Internal
public class FilteredBufferDispatcherImpl implements FilteredBufferDispatcher {

    /**
     * Function variant that may block waiting for a resource to become available for the given
     * channel.
     */
    @FunctionalInterface
    interface BlockingFunction<K, V> {
        V apply(K key) throws InterruptedException, IOException;
    }

    /**
     * Per-channel stores used by this dispatcher. Typed as the concrete {@link
     * RecoveredBufferStoreImpl} rather than {@link
     * org.apache.flink.runtime.io.network.partition.consumer.RecoveredBufferStore} because the
     * producer-side methods (addBuffer, markComplete, incrementPending, decrementPending) are
     * intentionally not part of the public interface — they are only called by
     * FilteredBufferDispatcher, which is the sole producer of buffers for the stores.
     */
    private final Map<InputChannelInfo, RecoveredBufferStoreImpl> storesByChannel;

    private final ChannelStateWriter channelStateWriter;
    private final String[] spillDirs;
    private final int memorySegmentSize;
    private final Function<InputChannelInfo, Buffer> bufferSupplier;
    private final BlockingFunction<InputChannelInfo, Buffer> blockingBufferSupplier;

    // Active buffer state
    private Buffer activeBuffer;
    private InputChannelInfo activeChannelInfo;
    private int activeBufferPosition;

    // Active spill entry accumulation state
    private long activeSpillEntryStartOffset;
    private int activeSpillEntryLength;
    private InputChannelInfo activeSpillChannelInfo;

    // Spill infrastructure
    private FilteredSpillFile.Writer spillFileWriter;
    private final Queue<FilteredSpillFile.Entry> spillEntryQueue;
    private final Queue<FilteredSpillFile.Reader> spillEntryReaderQueue;
    private final List<FilteredSpillFile.Reader> allSpillFileReaders;
    private int lastKnownFileCount;
    private byte[] drainBuffer;

    // Checkpoint wait-set state machine (Task thread via onChannelCheckpointStarted)
    private long currentCheckpointId = -1L;
    private Set<InputChannelInfo> waitSet;

    // Lifecycle flags
    private boolean flushed;
    private boolean closed;

    /**
     * Creates a new FilteredBufferDispatcherImpl.
     *
     * @param storesByChannel per-channel stores for delivering recovered buffers
     * @param channelStateWriter writer used during phase2 to stream spill entries to checkpoint
     *     storage without allocating network buffers
     * @param spillDirs directories for spill files
     * @param memorySegmentSize the size of a memory segment / network buffer
     * @param bufferSupplier non-blocking per-channel supplier; returns null when the channel's pool
     *     is exhausted
     * @param blockingBufferSupplier blocking per-channel supplier used during close() drain
     * @throws IOException if spillDirs is empty
     */
    public FilteredBufferDispatcherImpl(
            Map<InputChannelInfo, RecoveredBufferStoreImpl> storesByChannel,
            ChannelStateWriter channelStateWriter,
            String[] spillDirs,
            int memorySegmentSize,
            Function<InputChannelInfo, Buffer> bufferSupplier,
            BlockingFunction<InputChannelInfo, Buffer> blockingBufferSupplier)
            throws IOException {
        if (spillDirs.length == 0) {
            throw new IOException("Spill directories must not be empty");
        }
        this.storesByChannel = storesByChannel;
        this.channelStateWriter = channelStateWriter;
        this.spillDirs = spillDirs;
        this.memorySegmentSize = memorySegmentSize;
        this.bufferSupplier = bufferSupplier;
        this.blockingBufferSupplier = blockingBufferSupplier;
        this.spillEntryQueue = new ArrayDeque<>();
        this.spillEntryReaderQueue = new ArrayDeque<>();
        this.allSpillFileReaders = new ArrayList<>();
        this.lastKnownFileCount = 0;
        this.activeSpillEntryLength = 0;
        this.drainBuffer = null;

        // Register this dispatcher as the checkpoint listener on every per-channel store.
        // EMPTY store's setCheckpointListener is a no-op, so this is safe for all store types.
        for (RecoveredBufferStoreImpl store : storesByChannel.values()) {
            store.setCheckpointListener(this::onChannelCheckpointStarted);
        }
    }

    @Override
    public synchronized void write(byte[] data, int length, InputChannelInfo channelInfo)
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
    public synchronized void flush() throws IOException {
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
    public synchronized void close() throws IOException, InterruptedException {
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

        // Blocking drain: drain all spill entries using blocking buffer supplier keyed by channel
        while (!spillEntryQueue.isEmpty()) {
            FilteredSpillFile.Entry entry = spillEntryQueue.peek();
            Buffer buffer = blockingBufferSupplier.apply(entry.getChannelInfo());
            spillEntryQueue.poll();
            FilteredSpillFile.Reader reader = spillEntryReaderQueue.poll();
            loadEntryIntoBuffer(entry, reader, buffer);
            RecoveredBufferStoreImpl store =
                    Preconditions.checkNotNull(
                            storesByChannel.get(entry.getChannelInfo()),
                            "No store for channel %s",
                            entry.getChannelInfo());
            store.addBuffer(buffer);
            store.decrementPending();
        }

        // Cleanup spill infrastructure
        if (spillFileWriter != null) {
            spillFileWriter.close();
            spillFileWriter.deleteAllFiles();
        }
        for (FilteredSpillFile.Reader reader : allSpillFileReaders) {
            reader.close();
        }

        // Mark all stores as complete
        for (RecoveredBufferStoreImpl store : storesByChannel.values()) {
            store.markComplete();
        }
    }

    /**
     * Called by each per-channel store (via {@link
     * org.apache.flink.runtime.io.network.partition.consumer.ChannelCheckpointStartedListener})
     * when that channel's ready buffers have been snapshotted into the ChannelStateWriter.
     *
     * <p>On the first callback for a given checkpointId, the wait-set is built by scanning {@code
     * spillEntryQueue} for channels with pending spill entries. Subsequent callbacks for the same
     * checkpoint remove their channel from the wait-set. When the wait-set becomes empty, all
     * channels with disk data have reported in and {@link #drainSpillEntriesToCheckpoint} is
     * triggered to write the remaining spill entries via the streaming InputStream overload.
     *
     * <p>Called from the Task thread; synchronized on {@code this} to be mutually exclusive with
     * the Recovery thread's {@link #write} / {@link #flush} / {@link #close}.
     */
    public synchronized void onChannelCheckpointStarted(
            long checkpointId, InputChannelInfo channelInfo) {
        if (checkpointId != currentCheckpointId) {
            // New checkpoint: rebuild the wait-set by scanning the current spillEntryQueue.
            // Only channels that have at least one pending spill entry need to be waited for.
            currentCheckpointId = checkpointId;
            waitSet = new HashSet<>();
            for (FilteredSpillFile.Entry entry : spillEntryQueue) {
                waitSet.add(entry.getChannelInfo());
            }
        }
        waitSet.remove(channelInfo);
        if (waitSet.isEmpty()) {
            drainSpillEntriesToCheckpoint(checkpointId);
        }
    }

    /**
     * Performs a single sequential pass over {@code spillEntryQueue}, streaming each spill entry
     * directly to checkpoint storage via {@link ChannelStateWriter#addInputData(long,
     * InputChannelInfo, int, InputStream, int)}.
     *
     * <p>Entries belonging to the same physical spill file are streamed through a single {@link
     * FilteredSpillFile.Reader} opened once per file. When the reader changes (indicating a file
     * rotation boundary), a new sequential stream is opened starting at the first entry's offset in
     * the new file. Reusing one stream per file avoids the overhead of creating a new bounded
     * InputStream object for every individual entry.
     *
     * <p>The "snapshot + drain" is merged: each entry is polled from the queue as it is written to
     * the checkpoint, ensuring that the same entry is not double-written if {@link #close()} runs
     * concurrently on the Recovery thread. Because this method runs under {@code
     * synchronized(this)} and {@link #close()}'s drain loop also holds the same lock, the two are
     * mutually exclusive.
     *
     * <p>After phase2, the queue is empty. The {@link #close()} drain loop sees an empty queue and
     * exits immediately (no double-write). Each channel store's pending count is decremented so
     * that {@link
     * org.apache.flink.runtime.io.network.partition.consumer.RecoveredBufferStore#isEmpty()}
     * becomes true once phase2 completes, allowing credit release.
     *
     * <p>Must be called under {@code synchronized(this)}.
     */
    private void drainSpillEntriesToCheckpoint(long checkpointId) {
        // One sequential stream per physical spill file.
        // ChannelStateWriter's executor is single-threaded and processes requests FIFO, so
        // sharing one InputStream across consecutive addInputData calls for the same file is safe:
        // each call reads exactly dataLength bytes and advances the stream position before the
        // next call begins.
        FilteredSpillFile.Reader currentReader = null;
        InputStream currentStream = null;

        while (!spillEntryQueue.isEmpty()) {
            FilteredSpillFile.Entry entry = spillEntryQueue.poll();
            FilteredSpillFile.Reader reader = spillEntryReaderQueue.poll();

            if (reader != currentReader) {
                // File rotation boundary or first entry: open a new stream at this entry's offset.
                currentReader = reader;
                currentStream = reader.openSequentialStream(entry.getOffset());
            }

            channelStateWriter.addInputData(
                    checkpointId,
                    entry.getChannelInfo(),
                    ChannelStateWriter.SEQUENCE_NUMBER_RESTORED,
                    currentStream,
                    entry.getLength());
            // Decrement pending so that store.isEmpty() returns true once phase2 completes,
            // allowing RemoteInputChannel to release held credit to the upstream.
            RecoveredBufferStoreImpl store =
                    Preconditions.checkNotNull(
                            storesByChannel.get(entry.getChannelInfo()),
                            "No store for channel %s",
                            entry.getChannelInfo());
            store.decrementPending();
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
                    Preconditions.checkNotNull(
                                    storesByChannel.get(channelInfo),
                                    "No store for channel %s",
                                    channelInfo)
                            .addBuffer(activeBuffer);
                    activeBuffer = null;
                    activeBufferPosition = 0;
                }
            } else if (!downgradedToFile && activeBuffer == null) {
                // Try to acquire a new buffer
                if (!spillEntryQueue.isEmpty()) {
                    // Disk has data — must stay on file to preserve ordering
                    downgradedToFile = true;
                } else {
                    Buffer newBuffer = bufferSupplier.apply(channelInfo);
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
            FilteredSpillFile.Entry entry = spillEntryQueue.peek();
            Buffer buffer = bufferSupplier.apply(entry.getChannelInfo());
            if (buffer == null) {
                break;
            }
            spillEntryQueue.poll();
            FilteredSpillFile.Reader reader = spillEntryReaderQueue.poll();
            loadEntryIntoBuffer(entry, reader, buffer);
            RecoveredBufferStoreImpl store =
                    Preconditions.checkNotNull(
                            storesByChannel.get(entry.getChannelInfo()),
                            "No store for channel %s",
                            entry.getChannelInfo());
            store.addBuffer(buffer);
            store.decrementPending();
        }
    }

    /** Loads a spill entry's data from disk into a buffer. */
    private void loadEntryIntoBuffer(
            FilteredSpillFile.Entry entry, FilteredSpillFile.Reader reader, Buffer buffer)
            throws IOException {
        if (drainBuffer == null || drainBuffer.length < entry.getLength()) {
            drainBuffer = new byte[memorySegmentSize];
        }
        reader.read(entry.getOffset(), drainBuffer, entry.getLength());
        buffer.getMemorySegment().put(0, drainBuffer, 0, entry.getLength());
        buffer.setSize(entry.getLength());
    }

    /** Flushes the active buffer (if any) to the target store. */
    private void flushActiveBuffer() {
        if (activeBuffer != null && activeBufferPosition > 0) {
            activeBuffer.setSize(activeBufferPosition);
            Preconditions.checkNotNull(
                            storesByChannel.get(activeChannelInfo),
                            "No store for channel %s",
                            activeChannelInfo)
                    .addBuffer(activeBuffer);
        } else if (activeBuffer != null) {
            // Buffer allocated but nothing written — recycle it
            activeBuffer.recycleBuffer();
        }
        activeBuffer = null;
        activeBufferPosition = 0;
    }

    /** Seals the currently accumulating spill entry (if any) and adds it to the queue and store. */
    private void sealActiveSpillEntry() throws IOException {
        if (activeSpillEntryLength > 0 && activeSpillChannelInfo != null) {
            FilteredSpillFile.Entry entry =
                    new FilteredSpillFile.Entry(
                            activeSpillChannelInfo,
                            activeSpillEntryStartOffset,
                            activeSpillEntryLength);
            spillEntryQueue.add(entry);
            spillEntryReaderQueue.add(getCurrentSpillFileReader());
            Preconditions.checkNotNull(
                            storesByChannel.get(activeSpillChannelInfo),
                            "No store for channel %s",
                            activeSpillChannelInfo)
                    .incrementPending();
            activeSpillEntryLength = 0;
            activeSpillChannelInfo = null;
        }
    }

    /** Writes data to the spill file, creating the FilteredSpillFile.Writer lazily if needed. */
    private long writeToSpillFile(byte[] data, int offset, int length) throws IOException {
        if (spillFileWriter == null) {
            spillFileWriter = new FilteredSpillFile.Writer(spillDirs);
            lastKnownFileCount = 0;
        }
        return spillFileWriter.write(data, offset, length);
    }

    /**
     * Returns the FilteredSpillFile.Reader for the current spill file. Tracks file rotations and
     * creates new readers as needed.
     */
    private FilteredSpillFile.Reader getCurrentSpillFileReader() throws IOException {
        int currentFileCount = spillFileWriter.getAllFiles().size();
        if (currentFileCount > lastKnownFileCount) {
            // New file was created (initial or rotation) — create a reader for it
            FilteredSpillFile.Reader reader = spillFileWriter.getCurrentFileReader();
            allSpillFileReaders.add(reader);
            lastKnownFileCount = currentFileCount;
            return reader;
        }
        // Return the most recent reader
        return allSpillFileReaders.get(allSpillFileReaders.size() - 1);
    }
}
