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

/**
 * Cross-channel coordinator notified by per-channel
 * {@link org.apache.flink.runtime.io.network.partition.consumer.RecoveredBufferStore} instances on
 * lifecycle events. Implementations centralise bookkeeping that spans multiple channels (such as
 * checkpoint wait-sets or shared on-disk spill state) and react to per-channel transitions.
 *
 * <p>All methods are invoked from the Task thread, <em>outside</em> the calling store's lock, so
 * implementations may freely acquire their own synchronisation without deadlock risk.
 */
@Internal
public interface RecoveredBufferStoreCoordinator {

    /**
     * Invoked from inside {@code RecoveredBufferStore#checkpoint} after the store has snapshotted
     * its ready buffers. Implementations use this to maintain a wait-set across channels and, when
     * all channels have reported in, drain pending spill entries into the checkpoint.
     */
    void onChannelCheckpointStarted(long checkpointId, InputChannelInfo channelInfo);

    /**
     * Invoked from inside {@code RecoveredBufferStore#releaseAll} so the coordinator can drop
     * disk-resident spill entries still associated with the released channel.
     */
    void onChannelReleased(InputChannelInfo channelInfo);
}
