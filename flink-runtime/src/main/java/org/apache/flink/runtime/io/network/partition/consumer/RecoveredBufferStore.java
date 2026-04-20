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
 *
 * <p>Use {@link #EMPTY} as a sentinel when no recovered data is present (non-filtering mode, or
 * after recovery has fully drained), rather than holding {@code null} references.
 */
@Internal
public interface RecoveredBufferStore {

    /**
     * Singleton no-op store used when there is no recovered data for a channel. All query methods
     * return their neutral/empty sentinel values; all mutating methods and callback setters are
     * no-ops.
     */
    RecoveredBufferStore EMPTY = new EmptyRecoveredBufferStore();

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
     * Checkpoints the current store contents to the given ChannelStateWriter. Implementations
     * should snapshot ready buffers first, then fire the {@link CheckpointCallback} (if one is
     * registered) <em>outside</em> any store-level lock to avoid deadlock with the OutputWriter.
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

    /**
     * Registers a callback that is invoked after this channel's ready buffers have been
     * snapshotted during a checkpoint. The callback is fired outside any store-level lock.
     *
     * <p>The typical recipient is OutputWriter, which uses the callback to maintain its per-channel
     * wait-set and flush pending spill entries once all channels have reported in.
     *
     * @param callback the callback to invoke; replaces any previously registered callback
     */
    void setCheckpointCallback(CheckpointCallback callback);

    /**
     * Registers a callback that is invoked once when the store transitions from non-empty to empty
     * (i.e., {@link #isEmpty()} first becomes {@code true}). Used by {@code RemoteInputChannel} to
     * release held credit back to the upstream partition when recovery data has been fully
     * consumed.
     *
     * @param callback the callback to invoke; replaces any previously registered callback
     */
    void setOnBecameEmptyCallback(Runnable callback);

    /**
     * Registers a callback that is invoked when a buffer is added to a previously empty ready
     * queue. Used to notify the InputChannel that data is available for consumption.
     *
     * @param callback the callback to invoke; replaces any previously registered callback
     */
    void setNotificationCallback(Runnable callback);
}
