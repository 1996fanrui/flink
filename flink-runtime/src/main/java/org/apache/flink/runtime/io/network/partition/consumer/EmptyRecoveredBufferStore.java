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

import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.io.network.buffer.Buffer;

import javax.annotation.Nullable;

/**
 * No-op implementation of {@link RecoveredBufferStore} used as a sentinel value for channels that
 * have no recovered data (non-filtering mode, or filtering mode after recovery has fully drained).
 *
 * <p>Exposed as {@link RecoveredBufferStore#EMPTY}. All methods either return their neutral/empty
 * sentinel value or do nothing. Callback setters silently discard the supplied callbacks.
 *
 * <p>Using this singleton eliminates {@code null} checks at every call site and makes the
 * "no store" case an explicit, named contract.
 */
class EmptyRecoveredBufferStore implements RecoveredBufferStore {

    @Nullable
    @Override
    public Buffer tryTake() {
        return null;
    }

    @Override
    public Buffer.DataType peekNextDataType() {
        return Buffer.DataType.NONE;
    }

    @Override
    public boolean isEmpty() {
        return true;
    }

    @Override
    public boolean isComplete() {
        return true;
    }

    @Override
    public int size() {
        return 0;
    }

    /** No-op: EMPTY store holds no data, nothing to checkpoint. */
    @Override
    public void checkpoint(
            ChannelStateWriter writer, long checkpointId, InputChannelInfo channelInfo) {
        // no-op
    }

    /** No-op: EMPTY store holds no buffers to release. */
    @Override
    public void releaseAll() {
        // no-op
    }

    /** No-op: EMPTY store never transitions to non-empty, so the callback would never fire. */
    @Override
    public void setCheckpointCallback(CheckpointCallback callback) {
        // no-op
    }

    /** No-op: EMPTY store is always empty, so the callback would never fire. */
    @Override
    public void setOnBecameEmptyCallback(Runnable callback) {
        // no-op
    }

    /** No-op: EMPTY store never receives buffers, so the notification would never fire. */
    @Override
    public void setNotificationCallback(Runnable callback) {
        // no-op
    }
}
