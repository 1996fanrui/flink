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

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * Per-channel store for recovered buffers during unaligned checkpoint recovery. Buffers can be
 * either in-memory (ready for consumption) or on disk (pending spill entries). This interface
 * provides thread-safe access for consumption by the Task thread and population by the Recovery
 * thread.
 */
@Internal
public interface RecoveredBufferStore {

    /**
     * Takes the next buffer from the store. Returns null if no ready buffer is available.
     *
     * @return the next buffer, or null if no ready buffer available
     */
    @Nullable
    Buffer tryTake();

    /**
     * Peeks the data type of the next ready buffer without removing it.
     *
     * @return the data type of the next buffer, or {@link Buffer.DataType#NONE} if empty
     */
    Buffer.DataType peekNextDataType();

    /** Returns true if the ready buffer queue is empty and no pending spill entries exist. */
    boolean isEmpty();

    /**
     * Returns true if the store has been marked complete AND all ready buffers have been consumed.
     */
    boolean isComplete();

    /** Returns the number of ready buffers currently in the store. */
    int size();

    /**
     * Checkpoints the current store contents to the given ChannelStateWriter.
     *
     * @param writer the channel state writer to checkpoint to
     * @param checkpointId the checkpoint ID
     * @param channelInfo the input channel info for this store
     * @throws IOException if checkpointing fails
     */
    void checkpoint(ChannelStateWriter writer, long checkpointId, InputChannelInfo channelInfo)
            throws IOException;

    /** Releases all buffers held in this store and clears all state. */
    void releaseAll();
}
