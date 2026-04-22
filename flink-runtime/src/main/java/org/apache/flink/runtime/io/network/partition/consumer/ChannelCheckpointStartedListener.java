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

/**
 * Listener invoked when a per-channel checkpoint snapshot has started. Used by {@link
 * RecoveredBufferStoreImpl} to notify the OutputWriter that this channel's ready buffers have been
 * snapshotted, so the OutputWriter can update its wait-set and, when all channels are accounted
 * for, flush pending spill entries into the checkpoint.
 *
 * <p>The listener is always invoked outside any store-level lock to avoid deadlocks with the
 * OutputWriter's own synchronisation.
 */
@Internal
@FunctionalInterface
public interface ChannelCheckpointStartedListener {

    /**
     * Called after a channel's ready buffers have been snapshotted into the {@link
     * org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter}.
     *
     * @param checkpointId the ID of the checkpoint that just started for this channel
     * @param channelInfo the input channel that triggered the snapshot
     */
    void onChannelCheckpointStarted(long checkpointId, InputChannelInfo channelInfo);
}
