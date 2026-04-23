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

import static org.apache.flink.util.IOUtils.closeQuietly;

/**
 * Implementation of {@link FilteredBufferDispatcher} that manages three data paths:
 *
 * <ul>
 *   <li><b>P1 (buffer)</b>: Data is written directly to a network buffer and delivered to the
 *       target store.
 *   <li><b>P2 (spill to disk)</b>: When no buffer is available, data is written to a spill file via
 *       {@link FilteredSpillFile#writeEntry}.
 *   <li><b>P3 (eager drain)</b>: When buffers become available later, spilled entries are eagerly
 *       replayed from disk into buffers and delivered to stores.
 * </ul>
 *
 * <p>A byte[] memory cache accumulates payload bytes for the active channel. On channel change or
 * cache full, {@link #flushCache()} is invoked: if the spill writer is idle and a buffer is
 * available the cached bytes go directly to a network buffer (P1); otherwise they are written to
 * the spill file (P2). On {@link #close()}, all remaining spill entries are drained via {@link
 * BufferRequester#requestBufferBlocking(InputChannelInfo)}, spill files are deleted, and all stores
 * are marked complete.
 */
@Internal
public class FilteredBufferDispatcherImpl implements FilteredBufferDispatcher {

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
    private final BufferRequester bufferRequester;

    // Memory cache state: accumulates bytes for the active channel before committing to P1 or P2
    private final byte[] cache;
    private int cachePosition;
    private InputChannelInfo cacheChannel;

    // Spill infrastructure (P2/P3 paths)
    private FilteredSpillFile spillFile;

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
     * @param bufferRequester per-channel buffer source. The non-blocking variant is used for the
     *     fast path (P1) and eager replay (P3); the blocking variant is used for the close() drain
     * @throws IOException if spillDirs is empty
     */
    public FilteredBufferDispatcherImpl(
            Map<InputChannelInfo, RecoveredBufferStoreImpl> storesByChannel,
            ChannelStateWriter channelStateWriter,
            String[] spillDirs,
            int memorySegmentSize,
            BufferRequester bufferRequester)
            throws IOException {
        if (spillDirs.length == 0) {
            throw new IOException("Spill directories must not be empty");
        }
        this.storesByChannel = storesByChannel;
        this.channelStateWriter = channelStateWriter;
        this.spillDirs = spillDirs;
        this.memorySegmentSize = memorySegmentSize;
        this.bufferRequester = bufferRequester;
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
        if (spillFile != null) {
            spillFile.finish();
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
            if (spillFile != null) {
                spillFile.finish();
            }
            flushed = true;
        }

        // Drain remaining spill entries into buffers via the blocking requester
        if (spillFile != null) {
            drainSpillThroughBuffers();
        }

        // Cleanup spill infrastructure (close() also deletes all spill files)
        if (spillFile != null) {
            spillFile.close();
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
            // New checkpoint: rebuild the wait-set from channels with pending spill entries.
            // Invariant: checkpoint only starts after recovery ends (writer.finish()); all
            // Readers must already be sealed at this point.
            currentCheckpointId = checkpointId;
            waitSet = new HashSet<>();
            if (spillFile != null) {
                for (FilteredSpillFile.Reader reader : spillFile.getReaders()) {
                    Preconditions.checkState(
                            reader.isSealed(),
                            "Reader must be sealed when checkpoint starts; writer.finish() "
                                    + "must be called before checkpoint trigger.");
                    waitSet.addAll(reader.getPendingChannels());
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
        if (spillFile == null) {
            return;
        }
        List<FilteredSpillFile.Reader> snapshots = new ArrayList<>();
        try {
            for (FilteredSpillFile.Reader reader : spillFile.getReaders()) {
                // Invariant: checkpoint only runs after recovery ends (writer.finish()); every
                // Reader is sealed at this point.
                Preconditions.checkState(
                        reader.isSealed(),
                        "Reader must be sealed when draining spill entries to checkpoint; "
                                + "writer.finish() must be called before checkpoint trigger.");
                if (!reader.hasEntries()) {
                    continue;
                }
                snapshots.add(reader.snapshot());
                // Drain the original reader so close() drain sees empty entries, and decrement
                // the pending count once per entry (mirror of incrementPending in writeToSpillFile;
                // must be per-entry to stay symmetric — getPendingChannels() would undercount when
                // the same channel has multiple entries in one reader).
                while (reader.hasEntries()) {
                    InputChannelInfo ch = reader.peekNextChannel();
                    reader.readNext();
                    RecoveredBufferStoreImpl store =
                            Preconditions.checkNotNull(
                                    storesByChannel.get(ch), "No store for channel %s", ch);
                    store.decrementPending();
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
     * Drains all remaining spill entries into network buffers using the blocking buffer requester.
     * Only entries that have not been consumed by phase2 (drainSpillEntriesToCheckpoint) remain.
     */
    private void drainSpillThroughBuffers() throws IOException, InterruptedException {
        for (FilteredSpillFile.Reader reader : spillFile.getReaders()) {
            while (reader.hasEntries()) {
                InputChannelInfo ch = reader.peekNextChannel();
                Buffer buffer = bufferRequester.requestBufferBlocking(ch);
                FilteredSpillFile.Chunk chunk = reader.readNext();
                writeChunkToBuffer(buffer, chunk.getData(), chunk.getLength());
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
            Buffer buffer = bufferRequester.requestBuffer(channelInfo);
            if (buffer != null) {
                writeChunkToBuffer(buffer, cache, bytesToFlush);
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
        writeToSpillFile(cache, bytesToFlush, channelInfo);
    }

    /**
     * Copies {@code length} bytes from {@code data} into the given network buffer. Assumes the
     * buffer is freshly acquired (writerIndex == 0); after this call, {@code buffer.getSize() ==
     * length}.
     */
    private static void writeChunkToBuffer(Buffer buffer, byte[] data, int length) {
        Preconditions.checkState(
                buffer.getMaxCapacity() >= length,
                "Buffer capacity %s is smaller than chunk length %s",
                buffer.getMaxCapacity(),
                length);
        buffer.asByteBuf().writeBytes(data, 0, length);
    }

    /** Writes bytes to the spill file, creating the Writer lazily if needed. */
    private void writeToSpillFile(byte[] data, int length, InputChannelInfo channelInfo)
            throws IOException {
        if (spillFile == null) {
            spillFile = new FilteredSpillFile(spillDirs, memorySegmentSize);
        }
        spillFile.writeEntry(data, length, channelInfo);
        // Increment pending count so store.isEmpty() correctly reflects outstanding data
        RecoveredBufferStoreImpl store =
                Preconditions.checkNotNull(
                        storesByChannel.get(channelInfo),
                        "No store for channel %s",
                        channelInfo);
        store.incrementPending();
    }

    /** Eagerly replays spill entries while non-blocking buffers are available. */
    private void eagerDrain() throws IOException {
        if (spillFile == null) {
            return;
        }
        for (FilteredSpillFile.Reader reader : spillFile.getReaders()) {
            while (reader.hasEntries()) {
                InputChannelInfo ch = reader.peekNextChannel();
                Buffer buffer = bufferRequester.requestBuffer(ch);
                if (buffer == null) {
                    return;
                }
                FilteredSpillFile.Chunk chunk = reader.readNext();
                if (chunk == null) {
                    buffer.recycleBuffer();
                    return;
                }
                writeChunkToBuffer(buffer, chunk.getData(), chunk.getLength());
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
        return spillFile == null || spillFile.isIdle();
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
