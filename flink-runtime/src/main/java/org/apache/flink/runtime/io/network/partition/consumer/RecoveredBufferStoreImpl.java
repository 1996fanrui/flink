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
import org.apache.flink.runtime.checkpoint.channel.SpillEntry;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.util.CloseableIterator;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;

/**
 * Thread-safe implementation of {@link RecoveredBufferStore} that manages per-channel recovered
 * buffers. Buffers can be either ready (in-memory, available for consumption) or pending (on disk as
 * spill entries, awaiting materialization).
 *
 * <p>Thread safety: all methods that access {@code readyBuffers} or {@code pendingSpillEntries} are
 * synchronized on {@code this}. The {@code complete} flag is volatile since it is only written once.
 *
 * <p>Public interface methods are called from the Task thread. Internal methods (addBuffer,
 * markComplete, etc.) are called from the Recovery thread (OutputWriter).
 */
@Internal
public class RecoveredBufferStoreImpl implements RecoveredBufferStore {

    @GuardedBy("this")
    private final ArrayDeque<Buffer> readyBuffers = new ArrayDeque<>();

    @GuardedBy("this")
    private final List<SpillEntry> pendingSpillEntries = new ArrayList<>();

    private volatile boolean complete = false;
    private volatile boolean released = false;
    private Runnable notificationCallback;

    // --- Public interface methods (Task thread) ---

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
        return readyBuffers.isEmpty() && pendingSpillEntries.isEmpty();
    }

    @Override
    public boolean isComplete() {
        // complete is volatile; check ready buffers under lock
        if (!complete) {
            return false;
        }
        synchronized (this) {
            return readyBuffers.isEmpty();
        }
    }

    @Override
    public synchronized int size() {
        return readyBuffers.size();
    }

    /**
     * Checkpoints the current store contents. Ready buffers are retained and passed to the writer
     * via CloseableIterator. Pending spill entries on disk will be checkpointed via the
     * ChannelStateWriter streaming overload added in a separate commit.
     */
    @Override
    public synchronized void checkpoint(
            ChannelStateWriter writer, long checkpointId, InputChannelInfo channelInfo)
            throws IOException {
        // Checkpoint ready buffers: retain each and pass to writer
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

        // Pending spill entries are checkpointed via ChannelStateWriter streaming overload,
        // which is added in a separate commit. Integration wires this in C6.
    }

    @Override
    public synchronized void releaseAll() {
        released = true;
        for (Buffer buffer : readyBuffers) {
            buffer.recycleBuffer();
        }
        readyBuffers.clear();
        pendingSpillEntries.clear();
    }

    // --- Internal methods (Recovery thread, called by OutputWriter) ---

    /**
     * Adds a recovered buffer to the ready queue. If the queue was previously empty, the
     * notification callback is invoked to wake up the Task thread.
     */
    public synchronized void addBuffer(Buffer buffer) {
        if (released) {
            buffer.recycleBuffer();
            return;
        }
        boolean wasEmpty = readyBuffers.isEmpty();
        readyBuffers.add(buffer);
        if (wasEmpty && notificationCallback != null) {
            notificationCallback.run();
        }
    }

    /** Marks this store as complete — no more buffers will be added by the recovery thread. */
    public void markComplete() {
        complete = true;
    }

    /**
     * Sets the callback invoked when a buffer is added to a previously empty store. Used to notify
     * the InputChannel that data is available.
     */
    public void setNotificationCallback(Runnable callback) {
        this.notificationCallback = callback;
    }

    /** Adds a pending spill entry referencing data on disk. */
    public synchronized void addPendingSpillEntry(SpillEntry entry) {
        pendingSpillEntries.add(entry);
    }

    /** Removes a pending spill entry (after its data has been materialized into a buffer). */
    public synchronized void removePendingSpillEntry(SpillEntry entry) {
        pendingSpillEntries.remove(entry);
    }
}
