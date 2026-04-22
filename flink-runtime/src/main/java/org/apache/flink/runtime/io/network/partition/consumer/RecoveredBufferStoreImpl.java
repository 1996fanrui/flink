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
 * buffers. Buffers are either ready (in-memory, available for consumption) or pending (on disk,
 * tracked by count only — the actual spill entries are owned by FilteredBufferDispatcher).
 *
 * <p>Thread safety: all methods that access {@code readyBuffers} or {@code pendingCount} are
 * synchronized on {@code this}. The {@code complete} flag is volatile since it is only written
 * once. Callbacks are invoked <em>outside</em> any store-level lock to prevent deadlocks with
 * FilteredBufferDispatcher's own synchronisation.
 *
 * <p>Public interface methods are called from the Task thread. Internal methods (addBuffer,
 * markComplete, etc.) are called from the Recovery thread (FilteredBufferDispatcher).
 */
@Internal
public class RecoveredBufferStoreImpl implements RecoveredBufferStore {

    @GuardedBy("this")
    private final ArrayDeque<Buffer> readyBuffers = new ArrayDeque<>();

    @GuardedBy("this")
    private int pendingCount = 0;

    private volatile boolean complete = false;
    private volatile boolean released = false;

    @GuardedBy("this")
    private Runnable notificationCallback;

    @GuardedBy("this")
    private ChannelCheckpointStartedListener checkpointListener;

    @GuardedBy("this")
    private Runnable onBecameEmptyCallback;

    /**
     * Tracks whether the onBecameEmpty callback has already fired for the current empty state.
     * Reset to false each time a buffer is added (store becomes non-empty again), so subsequent
     * transitions from non-empty to empty fire the callback again.
     */
    @GuardedBy("this")
    private boolean becameEmptyCallbackFired = false;

    // ---------------------------------------------------------------------------
    // Public interface methods (Task thread)
    // ---------------------------------------------------------------------------

    @Nullable
    @Override
    public Buffer tryTake() {
        // Capture the buffer and whether the store just became empty under the lock.
        // Then fire the onBecameEmpty callback outside the lock to avoid deadlock.
        Buffer buffer;
        Runnable cb = null;
        synchronized (this) {
            buffer = readyBuffers.poll();
            if (buffer != null
                    && readyBuffers.isEmpty()
                    && pendingCount == 0
                    && !becameEmptyCallbackFired) {
                becameEmptyCallbackFired = true;
                cb = onBecameEmptyCallback;
            }
        }
        if (cb != null) {
            cb.run();
        }
        return buffer;
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
     * Checkpoints the ready buffers to the given ChannelStateWriter. Ready buffers are retained and
     * passed to the writer via CloseableIterator. After snapshotting, the {@link
     * ChannelCheckpointStartedListener} is invoked <em>outside</em> the store lock so that
     * FilteredBufferDispatcher can safely acquire its own lock without risking a deadlock.
     *
     * <p>Pending spill entries on disk are checkpointed by FilteredBufferDispatcher, which owns the
     * spill entries and file readers, triggered via the ChannelCheckpointStartedListener.
     */
    @Override
    public void checkpoint(
            ChannelStateWriter writer, long checkpointId, InputChannelInfo channelInfo)
            throws IOException {
        // Step 1: snapshot ready buffers under lock; capture callback reference.
        ChannelCheckpointStartedListener cb;
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
            cb = checkpointListener;
        }

        // Step 2: fire callback outside lock to avoid deadlock with FilteredBufferDispatcher's
        // lock.
        if (cb != null) {
            cb.onChannelCheckpointStarted(checkpointId, channelInfo);
        }
    }

    @Override
    public synchronized void releaseAll() {
        released = true;
        for (Buffer buffer : readyBuffers) {
            buffer.recycleBuffer();
        }
        readyBuffers.clear();
        pendingCount = 0;
    }

    // ---------------------------------------------------------------------------
    // Callback setters (interface methods)
    // ---------------------------------------------------------------------------

    @Override
    public synchronized void setCheckpointListener(ChannelCheckpointStartedListener listener) {
        this.checkpointListener = listener;
    }

    @Override
    public synchronized void setOnBecameEmptyCallback(Runnable callback) {
        this.onBecameEmptyCallback = callback;
    }

    /**
     * {@inheritDoc}
     *
     * <p>The notification callback fires when a buffer is added to a previously empty ready queue,
     * waking up the Task thread waiting for data.
     */
    @Override
    public synchronized void setNotificationCallback(Runnable callback) {
        this.notificationCallback = callback;
    }

    // ---------------------------------------------------------------------------
    // Internal methods (Recovery thread, called by FilteredBufferDispatcher)
    // ---------------------------------------------------------------------------

    /**
     * Adds a recovered buffer to the ready queue. If the queue was previously empty, the
     * notification callback is invoked to wake up the Task thread. The becameEmpty flag is also
     * reset so a subsequent drain-to-empty transition fires the onBecameEmpty callback again.
     */
    public synchronized void addBuffer(Buffer buffer) {
        if (released) {
            buffer.recycleBuffer();
            return;
        }
        boolean wasEmpty = readyBuffers.isEmpty();
        readyBuffers.add(buffer);
        if (wasEmpty) {
            // Reset the flag: now that the store is non-empty again, the next time it
            // transitions to empty the callback should fire.
            becameEmptyCallbackFired = false;
            if (notificationCallback != null) {
                notificationCallback.run();
            }
        }
    }

    /**
     * Marks this store as complete — no more buffers will be added by the recovery thread. If the
     * store is already empty when markComplete is called and the onBecameEmpty callback has not yet
     * fired for this empty state, it is fired outside any lock.
     */
    public void markComplete() {
        Runnable cb = null;
        synchronized (this) {
            complete = true;
            // If already empty at the moment of completion and callback hasn't fired yet,
            // fire it now to ensure the empty transition is always signalled.
            if (readyBuffers.isEmpty() && pendingCount == 0 && !becameEmptyCallbackFired) {
                becameEmptyCallbackFired = true;
                cb = onBecameEmptyCallback;
            }
        }
        if (cb != null) {
            cb.run();
        }
    }

    /**
     * Increments the pending spill entry count. Called when FilteredBufferDispatcher spills data to
     * disk.
     *
     * <p>If the store was logically empty before this call (readyBuffers empty AND pendingCount was
     * zero), reset the {@code becameEmptyCallbackFired} flag so that the next empty transition will
     * fire the onBecameEmpty callback again.
     */
    public synchronized void incrementPending() {
        if (readyBuffers.isEmpty() && pendingCount == 0) {
            // Store is transitioning from empty to non-empty; reset the flag so the callback
            // fires again when the store next becomes empty.
            becameEmptyCallbackFired = false;
        }
        pendingCount++;
    }

    /**
     * Decrements the pending spill entry count. Called when FilteredBufferDispatcher drains a spill
     * entry (into a buffer via P3/close path, or directly to checkpoint storage via phase-2 path).
     *
     * <p>If this decrement causes the store to become empty (readyBuffers empty AND pendingCount
     * reaches zero) and the onBecameEmpty callback has not yet fired for this empty state, the
     * callback is fired outside the lock to prevent deadlocks.
     */
    public void decrementPending() {
        Runnable cb = null;
        synchronized (this) {
            pendingCount--;
            if (readyBuffers.isEmpty() && pendingCount == 0 && !becameEmptyCallbackFired) {
                becameEmptyCallbackFired = true;
                cb = onBecameEmptyCallback;
            }
        }
        if (cb != null) {
            cb.run();
        }
    }
}
