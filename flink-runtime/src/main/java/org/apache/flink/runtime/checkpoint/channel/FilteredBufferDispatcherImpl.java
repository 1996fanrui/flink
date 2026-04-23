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
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Function;

import static org.apache.flink.util.IOUtils.closeQuietly;

/**
 * Implementation of {@link FilteredBufferDispatcher} that manages three data paths:
 *
 * <ul>
 *   <li><b>P1 (buffer)</b>: Data is written directly to a network buffer and delivered to the
 *       target store.
 *   <li><b>P2 (spill to disk)</b>: When no buffer is available, data is written to a spill file via
 *       {@link FilteredSpillFile.Writer#writeEntry}.
 *   <li><b>P3 (eager drain)</b>: When buffers become available later, spilled entries are eagerly
 *       replayed from disk into buffers and delivered to stores.
 * </ul>
 *
 * <p>A byte[] memory cache accumulates payload bytes for the active channel. On channel change or
 * cache full, {@link #flushCache()} is invoked: if the spill writer is idle and a buffer is
 * available the cached bytes go directly to a network buffer (P1); otherwise they are written to
 * the spill file (P2). On {@link #close()}, all remaining spill entries are drained using a
 * blocking buffer supplier, spill files are deleted, and all stores are marked complete.
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

    // Memory cache state: accumulates bytes for the active channel before committing to P1 or P2
    private final byte[] cache;
    private int cachePosition;
    private InputChannelInfo cacheChannel;

    // Spill infrastructure (P2/P3 paths)
    private FilteredSpillFile.Writer spillFileWriter;

    // Checkpoint wait-set state machine
    private long currentCheckpointId = -1L;
    private Set<InputChannelInfo> waitSet;

    // Lifecycle flags
    private boolean flushed;
    private boolean closed;

    /**
     * Creates a new FilteredBufferDispatcherImpl.
     *
     * @param storesByChannel per-channel stores for delivering recovered buffers
     * @param channelStateWriter writer used during phase2 to stream spill chunks to checkpoint
     *     storage without allocating network buffers
     * @param spillDirs directories for spill files
     * @param memorySegmentSize the size of a memory segment / network buffer
     * @param bufferSupplier non-blocking per-channel supplier; returns null when the pool is
     *     exhausted
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
        this.cache = new byte[memorySegmentSize];
        this.cachePosition = 0;

        // Register this dispatcher as the checkpoint listener on every per-channel store.
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

        // P3: eagerly replay any pending spill entries while non-blocking buffers are available
        eagerDrain();

        // Channel change: flush cached bytes for the previous channel before accumulating new data
        if (cacheChannel != null && !cacheChannel.equals(channelInfo) && cachePosition > 0) {
            flushCache();
        }
        cacheChannel = channelInfo;

        // Copy bytes into cache; flush whenever the cache fills up
        int pos = 0;
        while (pos < length) {
            int space = memorySegmentSize - cachePosition;
            int toCopy = Math.min(space, length - pos);
            System.arraycopy(data, pos, cache, cachePosition, toCopy);
            cachePosition += toCopy;
            pos += toCopy;

            if (cachePosition == memorySegmentSize) {
                flushCache();
                cacheChannel = channelInfo;
            }
        }
    }

    @Override
    public synchronized void flush() throws IOException {
        if (flushed || closed) {
            return;
        }
        flushCache();
        if (spillFileWriter != null) {
            spillFileWriter.finish();
        }
        flushed = true;
    }

    @Override
    public synchronized void close() throws IOException, InterruptedException {
        if (closed) {
            return;
        }
        closed = true;

        if (!flushed) {
            flushCache();
            if (spillFileWriter != null) {
                spillFileWriter.finish();
            }
            flushed = true;
        }

        // Drain remaining spill entries into buffers using the blocking supplier
        if (spillFileWriter != null) {
            drainSpillThroughBuffers();
        }

        // Cleanup spill infrastructure
        if (spillFileWriter != null) {
            spillFileWriter.close();
            spillFileWriter.deleteAllFiles();
        }

        // Mark all stores as complete
        for (RecoveredBufferStoreImpl store : storesByChannel.values()) {
            store.markComplete();
        }
    }

    /**
     * Called by each per-channel store when that channel's ready buffers have been snapshotted into
     * the ChannelStateWriter.
     *
     * <p>On the first callback for a given checkpointId, the wait-set is built by scanning all
     * sealed Readers for channels with pending spill entries. Subsequent callbacks remove their
     * channel from the wait-set. When the wait-set becomes empty, all channels with disk data have
     * reported in and {@link #drainSpillEntriesToCheckpoint} is triggered.
     *
     * <p>Called from the Task thread; synchronized on {@code this} to be mutually exclusive with
     * the Recovery thread's {@link #write} / {@link #flush} / {@link #close}.
     */
    public synchronized void onChannelCheckpointStarted(
            long checkpointId, InputChannelInfo channelInfo) {
        if (checkpointId != currentCheckpointId) {
            // New checkpoint: rebuild the wait-set from channels with pending spill entries
            currentCheckpointId = checkpointId;
            waitSet = new HashSet<>();
            if (spillFileWriter != null) {
                for (FilteredSpillFile.Reader reader : spillFileWriter.getReaders()) {
                    if (reader.isSealed()) {
                        waitSet.addAll(reader.getPendingChannels());
                    }
                }
            }
        }
        waitSet.remove(channelInfo);
        if (waitSet.isEmpty()) {
            drainSpillEntriesToCheckpoint(checkpointId);
        }
    }

    /**
     * Drains all sealed spill Readers to checkpoint storage via {@link
     * ChannelStateWriter#addInputDataFromSpill}. Creates snapshot Readers for each sealed Reader,
     * wraps them in a {@link DrainChunkIterator}, and submits the iterator to the
     * ChannelStateWriter. The iterator is responsible for closing the snapshot Readers.
     */
    private void drainSpillEntriesToCheckpoint(long checkpointId) {
        if (spillFileWriter == null) {
            return;
        }
        List<FilteredSpillFile.Reader> snapshots = new ArrayList<>();
        try {
            for (FilteredSpillFile.Reader reader : spillFileWriter.getReaders()) {
                if (reader.isSealed() && reader.hasEntries()) {
                    snapshots.add(reader.snapshot());
                    // Decrement pending for each entry being handed off to checkpoint
                    for (InputChannelInfo ch : reader.getPendingChannels()) {
                        RecoveredBufferStoreImpl store =
                                Preconditions.checkNotNull(
                                        storesByChannel.get(ch), "No store for channel %s", ch);
                        store.decrementPending();
                    }
                    // Drain the original reader so close() drain sees empty entries
                    drainReaderEntries(reader);
                }
            }
        } catch (IOException e) {
            for (FilteredSpillFile.Reader snap : snapshots) {
                try {
                    snap.close();
                } catch (IOException ignored) {
                }
            }
            throw new RuntimeException("Failed to snapshot spill readers for checkpoint", e);
        }
        if (!snapshots.isEmpty()) {
            channelStateWriter.addInputDataFromSpill(
                    checkpointId, new DrainChunkIterator(snapshots));
        }
    }

    /**
     * Drains all remaining spill entries into network buffers using the blocking buffer supplier.
     * Only entries that have not been consumed by phase2 (drainSpillEntriesToCheckpoint) remain.
     */
    private void drainSpillThroughBuffers() throws IOException, InterruptedException {
        for (FilteredSpillFile.Reader reader : spillFileWriter.getReaders()) {
            while (reader.hasEntries()) {
                InputChannelInfo ch = reader.peekNextChannel();
                Buffer buffer = blockingBufferSupplier.apply(ch);
                FilteredSpillFile.Chunk chunk = reader.readNext();
                if (chunk == null) {
                    break;
                }
                Preconditions.checkState(
                        buffer.getMaxCapacity() >= chunk.getLength(),
                        "Buffer capacity %s is smaller than chunk length %s",
                        buffer.getMaxCapacity(),
                        chunk.getLength());
                buffer.getMemorySegment().put(0, chunk.getData(), 0, chunk.getLength());
                buffer.setSize(chunk.getLength());
                RecoveredBufferStoreImpl store =
                        Preconditions.checkNotNull(
                                storesByChannel.get(ch), "No store for channel %s", ch);
                store.addBuffer(buffer);
                store.decrementPending();
            }
        }
    }

    /**
     * Flushes the cache for the current channel via P1 (buffer) or P2 (spill). After calling, the
     * cache is empty and cacheChannel is null.
     *
     * <p>P1 is chosen when the spill writer is idle (no pending disk entries) AND a non-blocking
     * buffer can be obtained. Otherwise the bytes are spilled (P2). This ensures FIFO ordering:
     * once any data has been spilled, all subsequent data must also spill to preserve order.
     */
    private void flushCache() throws IOException {
        if (cachePosition == 0) {
            cacheChannel = null;
            return;
        }

        InputChannelInfo channelInfo = cacheChannel;
        int bytesToFlush = cachePosition;
        cachePosition = 0;
        cacheChannel = null;

        // P1: spill writer idle and a buffer is available — write directly to network buffer
        if (isSpillIdle()) {
            Buffer buffer = bufferSupplier.apply(channelInfo);
            if (buffer != null) {
                Preconditions.checkState(
                        buffer.getMaxCapacity() >= bytesToFlush,
                        "Buffer capacity %s is smaller than cache size %s",
                        buffer.getMaxCapacity(),
                        bytesToFlush);
                buffer.getMemorySegment().put(0, cache, 0, bytesToFlush);
                buffer.setSize(bytesToFlush);
                RecoveredBufferStoreImpl store =
                        Preconditions.checkNotNull(
                                storesByChannel.get(channelInfo),
                                "No store for channel %s",
                                channelInfo);
                store.addBuffer(buffer);
                return;
            }
        }

        // P2: spill writer not idle or no buffer available — write to spill file
        writeToSpillFile(cache, 0, bytesToFlush, channelInfo);
    }

    /** Writes bytes to the spill file, creating the Writer lazily if needed. */
    private void writeToSpillFile(byte[] data, int offset, int length, InputChannelInfo ci)
            throws IOException {
        if (spillFileWriter == null) {
            spillFileWriter = new FilteredSpillFile.Writer(spillDirs, memorySegmentSize);
        }
        spillFileWriter.writeEntry(data, offset, length, ci);
        // Increment pending count so store.isEmpty() correctly reflects outstanding data
        RecoveredBufferStoreImpl store =
                Preconditions.checkNotNull(storesByChannel.get(ci), "No store for channel %s", ci);
        store.incrementPending();
    }

    /** Eagerly replays spill entries while non-blocking buffers are available. */
    private void eagerDrain() throws IOException {
        if (spillFileWriter == null) {
            return;
        }
        for (FilteredSpillFile.Reader reader : spillFileWriter.getReaders()) {
            while (reader.hasEntries()) {
                InputChannelInfo ch = reader.peekNextChannel();
                Buffer buffer = bufferSupplier.apply(ch);
                if (buffer == null) {
                    return;
                }
                FilteredSpillFile.Chunk chunk = reader.readNext();
                if (chunk == null) {
                    buffer.recycleBuffer();
                    return;
                }
                Preconditions.checkState(
                        buffer.getMaxCapacity() >= chunk.getLength(),
                        "Buffer capacity %s is smaller than chunk length %s",
                        buffer.getMaxCapacity(),
                        chunk.getLength());
                buffer.getMemorySegment().put(0, chunk.getData(), 0, chunk.getLength());
                buffer.setSize(chunk.getLength());
                RecoveredBufferStoreImpl store =
                        Preconditions.checkNotNull(
                                storesByChannel.get(ch), "No store for channel %s", ch);
                store.addBuffer(buffer);
                store.decrementPending();
            }
        }
    }

    /** Returns true if no spill entries have been written yet (P1 is safe). */
    private boolean isSpillIdle() {
        return spillFileWriter == null || spillFileWriter.isIdle();
    }

    /**
     * Drains all remaining entries from a Reader without loading them, so that the Reader reports
     * no pending entries after phase2 snapshot.
     */
    private void drainReaderEntries(FilteredSpillFile.Reader reader) throws IOException {
        while (reader.hasEntries()) {
            reader.readNext();
        }
    }

    // -------------------------------------------------------------------------
    // DrainChunkIterator — CloseableIterator over snapshot Readers
    // -------------------------------------------------------------------------

    /**
     * Iterates over chunks from a sequence of snapshot {@link FilteredSpillFile.Reader}s. Each
     * Reader is drained in order; once exhausted it is popped and closed immediately. {@link
     * #close()} closes whatever Readers remain (i.e. those not yet consumed by the iterator).
     */
    private static final class DrainChunkIterator
            implements CloseableIterator<FilteredSpillFile.Chunk> {

        private final Deque<FilteredSpillFile.Reader> remaining;

        DrainChunkIterator(List<FilteredSpillFile.Reader> snapshots) {
            this.remaining = new ArrayDeque<>(snapshots);
        }

        @Override
        public boolean hasNext() {
            while (!remaining.isEmpty() && !remaining.peekFirst().hasEntries()) {
                closeQuietly(remaining.pollFirst());
            }
            return !remaining.isEmpty();
        }

        @Override
        public FilteredSpillFile.Chunk next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            try {
                return remaining.peekFirst().readNext();
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to read spill chunk", e);
            }
        }

        @Override
        public void close() {
            while (!remaining.isEmpty()) {
                closeQuietly(remaining.pollFirst());
            }
        }
    }
}
