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
 * Recovery buffer queue shared by local and remote input channels.
 *
 * <p>All methods are unsafe: callers must hold the owning channel's monitor. The queue does not own
 * a lock because remote channels must inspect recovered and live upstream buffers atomically.
 */
@Internal
class RecoveredBufferQueue {

    private final InputChannelInfo channelInfo;

    private final Deque<Buffer> buffers = new ArrayDeque<>();

    RecoveredBufferQueue(InputChannelInfo channelInfo, boolean initiallyDelivered) {
        this.channelInfo = channelInfo;
        this.allDelivered = initiallyDelivered;
    }

    private boolean allDelivered;

    private int sequenceNumber = Integer.MIN_VALUE;

    /**
     * Appends a buffer to the queue.
     *
     * @return {@code true} if the queue transitioned from empty to non-empty as a result of this
     *     call (so the caller knows whether to issue the channel-available notification).
     */
    boolean offer(Buffer buffer) {
        Preconditions.checkState(
                isInRecovery(),
                "Push into RecoveredBufferQueue after recovery finished (channelInfo=%s, bufferType=%s)",
                channelInfo,
                buffer.getDataType());
        boolean wasEmpty = buffers.isEmpty();
        buffers.add(buffer);
        return wasEmpty;
    }

    void finish() {
        Preconditions.checkState(
                !allDelivered,
                "finish() called on a RecoveredBufferQueue that is already done (channelInfo=%s)",
                channelInfo);
        allDelivered = true;
    }

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

    int nextSequenceNumber() {
        return sequenceNumber++;
    }

    /**
     * Walks the queue up to the {@link RecoveryCheckpointBarrier} sentinel matching {@code
     * checkpointId}, retaining each pre-barrier data buffer and removing the sentinel.
     *
     * @throws IOException if no sentinel matching {@code checkpointId} is found (the snapshot
     *     protocol guarantees one must be present whenever this method is invoked).
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
