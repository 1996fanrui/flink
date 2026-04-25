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

package org.apache.flink.runtime.io.network.partition.consumer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.checkpoint.channel.RecoveredBufferStoreCoordinator;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.util.CloseableIterator;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Thread-safe implementation of {@link RecoveredBufferStore} that manages per-channel recovered
 * buffers. Buffers are either ready (in-memory, available for consumption) or pending (on disk,
 * tracked by count only — the actual spill entries are owned by FilteredBufferDispatcher).
 *
 * <p>Thread safety: all methods that access {@code readyBuffers} or {@code pendingCount} are
 * synchronized on {@code this}. The {@code released} flag is volatile since it is only written
 * once. Coordinator notifications and the data-available listener are invoked <em>outside</em>
 * any store-level lock to prevent deadlocks with the coordinator's own synchronisation.
 *
 * <p>Public interface methods are called from the Task thread. Internal methods (addBuffer,
 * incrementPending, decrementPending) are called from the Recovery thread
 * (FilteredBufferDispatcher).
 */
@Internal
public class RecoveredBufferStoreImpl implements RecoveredBufferStore {

    private final InputChannelInfo channelInfo;

    @GuardedBy("this")
    private final ArrayDeque<Buffer> readyBuffers = new ArrayDeque<>();

    @GuardedBy("this")
    private int pendingCount = 0;

    private volatile boolean released = false;

    @GuardedBy("this")
    private DataAvailableListener dataAvailableListener;

    @GuardedBy("this")
    private RecoveredBufferStoreCoordinator coordinator;

    /**
     * Creates a store bound to a single input channel. The bound {@link InputChannelInfo} is used
     * when persisting ready buffers during checkpoint and when notifying the checkpoint listener.
     */
    public RecoveredBufferStoreImpl(InputChannelInfo channelInfo) {
        this.channelInfo = checkNotNull(channelInfo);
    }

    /** Returns the input channel this store is bound to. */
    public InputChannelInfo getChannelInfo() {
        return channelInfo;
    }

    // ---------------------------------------------------------------------------
    // Public interface methods (Task thread)
    // ---------------------------------------------------------------------------

    @Nullable
    @Override
    public synchronized Buffer tryTake() {
        return readyBuffers.poll();
    }

    @Override
    public synchronized Buffer.DataType peekNextDataType() {
        Buffer peeked = readyBuffers.peek();
        return peeked != null ? peeked.getDataType() : Buffer.DataType.NONE;
    }

    @Override
    public synchronized boolean isEmpty() {
        return readyBuffers.isEmpty() && pendingCount == 0;
    }

    @Override
    public synchronized int size() {
        return readyBuffers.size() + pendingCount;
    }

    /**
     * Checkpoints the ready buffers to the given ChannelStateWriter. Ready buffers are retained and
     * passed to the writer via CloseableIterator. After snapshotting, the registered {@link
     * RecoveredBufferStoreCoordinator} is notified <em>outside</em> the store lock so the
     * coordinator can safely acquire its own lock without risking a deadlock.
     *
     * <p>Pending spill entries on disk are checkpointed by the coordinator, which owns the spill
     * entries and file readers, triggered via
     * {@link RecoveredBufferStoreCoordinator#onChannelCheckpointStarted}.
     */
    @Override
    public void checkpoint(ChannelStateWriter writer, long checkpointId) throws IOException {
        // Step 1: snapshot ready buffers under lock; capture coordinator reference.
        RecoveredBufferStoreCoordinator c;
        synchronized (this) {
            if (!readyBuffers.isEmpty()) {
                List<Buffer> retained = new ArrayList<>(readyBuffers.size());
                for (Buffer buffer : readyBuffers) {
                    retained.add(buffer.retainBuffer());
                }
                writer.addInputData(
                        checkpointId,
                        channelInfo,
                        ChannelStateWriter.SEQUENCE_NUMBER_RESTORED,
                        CloseableIterator.fromList(retained, Buffer::recycleBuffer));
            }
            c = coordinator;
        }

        // Step 2: notify the coordinator outside the store lock to avoid deadlock with the
        // coordinator's own synchronisation.
        if (c != null) {
            c.onChannelCheckpointStarted(checkpointId, channelInfo);
        }
    }

    @Override
    public void releaseAll() {
        // Step 1: flip the released flag and recycle ready buffers under lock; capture the
        // coordinator reference for invocation outside the lock.
        RecoveredBufferStoreCoordinator c;
        synchronized (this) {
            released = true;
            for (Buffer buffer : readyBuffers) {
                buffer.recycleBuffer();
            }
            readyBuffers.clear();
            pendingCount = 0;
            c = coordinator;
        }

        // Step 2: notify the coordinator outside the store lock so the coordinator can safely
        // acquire its own lock to drop disk-resident spill entries for this channel.
        if (c != null) {
            c.onChannelReleased(channelInfo);
        }
    }

    // ---------------------------------------------------------------------------
    // Setters (interface methods)
    // ---------------------------------------------------------------------------

    @Override
    public synchronized void setCoordinator(RecoveredBufferStoreCoordinator coordinator) {
        this.coordinator = coordinator;
    }

    /**
     * {@inheritDoc}
     *
     * <p>The notification listener fires when a buffer is added to a previously empty ready queue,
     * waking up the Task thread waiting for data.
     */
    @Override
    public synchronized void setDataAvailableListener(DataAvailableListener listener) {
        this.dataAvailableListener = listener;
    }

    // ---------------------------------------------------------------------------
    // Internal methods (Recovery thread, called by FilteredBufferDispatcher)
    // ---------------------------------------------------------------------------

    /**
     * Adds a recovered buffer to the ready queue. If the queue was previously empty, the
     * notification listener is invoked to wake up the Task thread.
     */
    public synchronized void addBuffer(Buffer buffer) {
        if (released) {
            buffer.recycleBuffer();
            return;
        }
        boolean wasEmpty = readyBuffers.isEmpty();
        readyBuffers.add(buffer);
        if (wasEmpty && dataAvailableListener != null) {
            dataAvailableListener.onDataAvailable();
        }
    }

    /**
     * Increments the pending spill entry count. Called when FilteredBufferDispatcher spills data to
     * disk.
     */
    public synchronized void incrementPending() {
        pendingCount++;
    }

    /**
     * Decrements the pending spill entry count. Called when FilteredBufferDispatcher drains a spill
     * entry (into a buffer via P3/close path, or directly to checkpoint storage via phase-2 path).
     */
    public synchronized void decrementPending() {
        pendingCount--;
    }
}
