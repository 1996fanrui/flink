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
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.io.network.buffer.Buffer;

import java.io.IOException;

/**
 * Implemented by physical input channels that can receive recovered buffers pushed by the spill
 * drain. The drain holds channel references typed as this interface and never casts down.
 */
@Internal
public interface RecoverableInputChannel {

    /**
     * Identifies the channel within its task. Surfacing this on the interface lets consumers
     * holding only the interface type build {@code InputChannelInfo}-keyed lookups without
     * downcasting to {@link InputChannel}.
     */
    InputChannelInfo getChannelInfo();

    /**
     * Appends a recovered buffer (or a {@code RecoveryCheckpointBarrier} sentinel) to this
     * channel's {@code recoveredBuffers} queue. If the channel has already been released, the
     * buffer is recycled silently. Wakes the consumer via the existing {@code
     * notifyChannelNonEmpty} chain if the queue was empty before this call.
     *
     * <p>Caller MUST hold {@code SpillFileReader.lock}. This ensures that advancing the spill
     * reader's internal offset and the corresponding channel add-buffer are atomic, so no entry can
     * be observed as belonging to both the memory snapshot and the disk snapshot.
     */
    void onRecoveredStateBuffer(Buffer buffer);

    /**
     * Signals that the producer has finished delivering recovered buffers into this channel. The
     * consumer may still have leftover buffers queued; the channel completes its state-consumed
     * future once this flag is set AND the queue is drained.
     *
     * <p>Implementations must first await the channel's upstream connection being published so
     * channels with no spill entries still observe the upstream-ready edge before being marked
     * delivered. Caller does NOT need to hold {@code SpillFileReader.lock}: no more buffers are
     * being added, so the (queue, offset) atomicity protected by the lock no longer applies.
     */
    void finishRecoveredBufferDelivery() throws IOException;

    /**
     * Inserts a {@code RecoveryCheckpointBarrier} for {@code checkpointId} into this channel's
     * recovery queue, but only if the channel is still in recovery. The in-recovery check and the
     * insert happen atomically under the channel's own queue monitor (the same monitor that guards
     * {@code onRecoveredStateBuffer} and the consumer's recovery branch), so a concurrent
     * end-of-drain {@code finishRecoveredBufferDelivery} cannot flip the channel out of recovery
     * between the decision and the insert. If the channel has already left recovery (or been
     * released), nothing is inserted and no barrier buffer is allocated.
     */
    void insertRecoveryCheckpointBarrierIfInRecovery(long checkpointId) throws IOException;

    /**
     * Blocks until a buffer is available from this channel's own buffer pool. Implementations must
     * first await the channel's upstream connection (Local {@code subpartitionView} / Remote {@code
     * partitionRequestClient}) being published before allocating, so the recovered buffer is
     * delivered only after the upstream is in place. Surfaces release as a {@link
     * java.util.concurrent.CompletionException} / {@link
     * java.util.concurrent.CancellationException} so the caller can terminate gracefully.
     *
     * <p>Must be invoked outside the {@code SpillFileReader.lock} critical section so the
     * checkpoint trigger can still take the lock while this call is parked.
     */
    Buffer requestRecoveryBufferBlocking() throws InterruptedException, IOException;
}
