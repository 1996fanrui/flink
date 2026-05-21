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
import org.apache.flink.runtime.io.network.buffer.Buffer;

import java.io.IOException;

/**
 * Implemented by {@code RecoveredInputChannel}, {@code LocalInputChannel}, and {@code
 * RemoteInputChannel}. The drain (SpillFileReader) holds channel references typed as this interface
 * and never casts down; method names mirror the existing {@code RecoveredInputChannel} API.
 */
@Internal
public interface RecoverableInputChannel {

    /**
     * Appends a recovered buffer (or a {@code RecoveryCheckpointBarrier} sentinel) to this
     * channel's {@code recoveredBuffers} queue. If the channel has already been released, the
     * buffer is recycled silently. Wakes the consumer via the existing {@code
     * notifyChannelNonEmpty} chain if the queue was empty before this call.
     *
     * <p>Caller (drain thread or task thread at Step 1) MUST hold {@code SpillFileReader.lock}.
     * This ensures that advancing {@code SpillFileReader}'s internal offset and the corresponding
     * channel add-buffer are atomic, so neither side can observe an entry that belongs to both the
     * memory snapshot and the disk snapshot simultaneously.
     *
     * @param buffer the recovered data buffer or the {@code RecoveryCheckpointBarrier} sentinel
     */
    void onRecoveredStateBuffer(Buffer buffer);

    /**
     * Signals that the spill/drain producer has finished adding recovered buffers into this
     * channel. Flips {@code allRecoveredBuffersDelivered} from {@code false} to {@code true}
     * exactly once (producer-side completion only). The consumer may still have leftover buffers
     * queued in {@code recoveredBuffers}. The channel completes {@code stateConsumedFuture} once
     * both this flag is {@code true} AND {@code recoveredBuffers} is empty.
     *
     * <p>End-of-drain exception: caller does NOT need to hold {@code SpillFileReader.lock}. At this
     * point no more buffers are being added, so the (queue, offset) atomicity that Principle 1
     * protects does not apply. The flag is published through the channel's own internal monitor.
     *
     * @throws IOException if an error occurs while finalising the channel state
     */
    void finishReadRecoveredState() throws IOException;
}
