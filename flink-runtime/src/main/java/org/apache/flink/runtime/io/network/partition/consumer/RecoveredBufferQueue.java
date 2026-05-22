/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.io.network.partition.consumer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.checkpoint.channel.RecoveryCheckpointBarrier;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;

/**
 * Holds recovery buffers and recovery-completion state for a physical input channel during the
 * spill-recovery handover.
 *
 * <p>Encapsulates the buffer deque, the {@code allDelivered} flag, and the recovery-sequence
 * counter so that {@link LocalInputChannel} and {@link RemoteInputChannel} can share a single
 * implementation of the recovery state machine. Full recovery is reached when {@code allDelivered
 * && isEmpty()}.
 *
 * <p>All methods are <b>unsafe</b>: callers must hold the channel's monitor (Local: the queue
 * instance itself; Remote: the {@code receivedBuffers} monitor so recovery and live upstream queues
 * can be inspected atomically). The queue intentionally does not own a lock, allowing Remote to
 * inspect both queues under a single critical section.
 */
@Internal
class RecoveredBufferQueue {

    private final InputChannelInfo channelInfo;

    private final Deque<Buffer> buffers = new ArrayDeque<>();

    RecoveredBufferQueue(InputChannelInfo channelInfo, boolean initiallyDelivered) {
        this.channelInfo = channelInfo;
        this.allDelivered = initiallyDelivered;
    }

    /**
     * True once the drain/spill producer has finished pushing recovered buffers into this queue.
     * Producer-side completion only; the consumer may still have buffers queued. May be initialized
     * to {@code true} when the owning channel has no SpillFileReader-driven drain phase ahead of
     * it: in that case {@code isInRecovery} starts false and the channel goes straight to the
     * normal path, avoiding a {@code true → false} flip that could let a consumer race past an
     * in-flight {@code requestSubpartitions} retrigger and observe a null {@code subpartitionView}.
     */
    private boolean allDelivered;

    /**
     * Sequence-number counter for buffers emitted during recovery. Starts at {@link
     * Integer#MIN_VALUE} so that recovery sequence numbers cannot collide with live upstream
     * sequence numbers. Single-threaded (task thread only).
     */
    private int sequenceNumber = Integer.MIN_VALUE;

    /**
     * Appends a buffer to the queue.
     *
     * @return {@code true} if the queue transitioned from empty to non-empty as a result of this
     *     call (so the caller knows whether to issue the channel-available notification).
     */
    boolean offer(Buffer buffer) {
        // Strict monotonic invariant: push is only allowed while the channel is still in the
        // recovery phase. The transition state (allDelivered=true but buffers non-empty) still
        // counts as in-recovery — a RecoveryCheckpointBarrier may be pushed in that window and
        // the consumer keeps using the in-recovery branch until buffers drain. Once buffers go
        // empty and allDelivered=true (isInRecovery()=false), the channel has fully left recovery
        // and no caller is allowed to push back into the queue. Fail loud so any violator shows
        // up in the stack.
        Preconditions.checkState(
                isInRecovery(),
                "Push into RecoveredBufferQueue after recovery finished (channelInfo=%s, bufferType=%s)",
                channelInfo,
                buffer.getDataType());
        boolean wasEmpty = buffers.isEmpty();
        buffers.add(buffer);
        return wasEmpty;
    }

    /** Marks producer-side delivery as complete. Idempotent. */
    void finish() {
        allDelivered = true;
    }

    /** Recovery is still in progress unless the producer is done and the queue is drained. */
    boolean isInRecovery() {
        return !allDelivered || !buffers.isEmpty();
    }

    boolean isAllDelivered() {
        return allDelivered;
    }

    boolean isEmpty() {
        return buffers.isEmpty();
    }

    int size() {
        return buffers.size();
    }

    Buffer peek() {
        return buffers.peek();
    }

    Buffer poll() {
        return buffers.poll();
    }

    /**
     * Post-increment counter; returned value is the sequence number for the buffer just emitted.
     */
    int nextSequenceNumber() {
        return sequenceNumber++;
    }

    /**
     * Walks the queue up to the {@link RecoveryCheckpointBarrier} sentinel matching {@code
     * checkpointId}, retaining each pre-barrier data buffer for the channel-state writer and
     * removing the sentinel itself. Pre-barrier events are left in the queue for normal consumption
     * — the channel-state writer only accepts data buffers.
     *
     * @throws IOException if no sentinel matching {@code checkpointId} is found (the snapshot
     *     protocol guarantees one must be present whenever this method is invoked); retained
     *     buffers are released before throwing so the caller does not have to clean up.
     */
    List<Buffer> collectPreRecoveryBarrier(long checkpointId) throws IOException {
        List<Buffer> retained = new ArrayList<>();
        try {
            Iterator<Buffer> it = buffers.iterator();
            while (it.hasNext()) {
                Buffer b = it.next();
                if (isRecoveryCheckpointBarrier(b, checkpointId)) {
                    it.remove();
                    b.recycleBuffer();
                    return retained;
                }
                if (b.isBuffer()) {
                    retained.add(b.retainBuffer());
                }
            }
        } catch (IOException e) {
            releaseRetainedBuffers(retained);
            throw e;
        }
        releaseRetainedBuffers(retained);
        throw new IOException(
                "Missing RecoveryCheckpointBarrier for checkpoint "
                        + checkpointId
                        + " in recoveredBuffers for channel "
                        + channelInfo);
    }

    /** Recycles every queued buffer and clears the queue. */
    void releaseAll() {
        for (Buffer buffer : buffers) {
            buffer.recycleBuffer();
        }
        buffers.clear();
    }

    private static void releaseRetainedBuffers(List<Buffer> retained) {
        for (Buffer buffer : retained) {
            buffer.recycleBuffer();
        }
    }

    private static boolean isRecoveryCheckpointBarrier(Buffer b, long checkpointId)
            throws IOException {
        if (b.isBuffer()) {
            return false;
        }
        AbstractEvent event =
                EventSerializer.fromBuffer(b, RecoveryCheckpointBarrier.class.getClassLoader());
        b.setReaderIndex(0);
        return event instanceof RecoveryCheckpointBarrier
                && ((RecoveryCheckpointBarrier) event).getCheckpointId() == checkpointId;
    }
}
