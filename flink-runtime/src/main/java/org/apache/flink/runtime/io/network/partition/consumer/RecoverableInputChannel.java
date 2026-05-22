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
     * <p>Caller does NOT need to hold {@code SpillFileReader.lock}: no more buffers are being
     * added, so the (queue, offset) atomicity protected by the lock no longer applies.
     */
    void finishRecoveredBufferDelivery() throws IOException;

    /**
     * Reports whether the channel's recovery queue is still active: producer has not yet signalled
     * completion OR consumer has not yet drained every queued recovery buffer. Used to decide, per
     * channel, whether a {@code RecoveryCheckpointBarrier} should be inserted into the channel
     * queue.
     *
     * <p>Implementations must take the channel's own queue monitor (the same one that protects
     * {@code onRecoveredStateBuffer} and {@code getNextBuffer}'s recovery branch) so the value
     * cannot flip mid-decision.
     */
    boolean isInRecovery();

    /**
     * Blocks until the channel's upstream connection (Local {@code subpartitionView} / Remote
     * {@code partitionRequestClient}) is published. Surfaces release as a {@link
     * java.util.concurrent.CompletionException} / {@link
     * java.util.concurrent.CancellationException} so the caller can recycle in-flight buffers and
     * terminate gracefully.
     *
     * <p>Must be invoked off the task thread and outside the {@code SpillFileReader.lock} critical
     * section. The PartitionNotFoundException retrigger that completes {@code upstreamReady} is
     * itself a mailbox-scheduled mail, so awaiting from the task thread would deadlock against the
     * future being waited on.
     */
    void awaitUpstreamReady();
}
