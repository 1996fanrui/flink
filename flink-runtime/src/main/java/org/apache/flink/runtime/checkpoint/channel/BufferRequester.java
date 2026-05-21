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
import org.apache.flink.runtime.io.network.buffer.Buffer;

import java.io.IOException;

/**
 * Funnels every buffer allocation that the drain (SpillFileReader) needs. Lives in the same package
 * as {@code SpillFileReader}, so {@code SpillFileReader} depends only on this interface; the
 * cross-package access to {@code RecoveredInputChannel}'s release primitive is encapsulated inside
 * the single implementation {@code RecoveredChannelBufferRequester}.
 */
@Internal
public interface BufferRequester {

    /**
     * Blocks until a buffer is available from the source channel's pool. Implementations are
     * expected to delegate to {@code RecoveredInputChannel.requestBufferBlocking()}, which parks on
     * the per-channel {@code BufferManager.bufferQueue} ({@code Object.wait / notifyAll}) and is
     * woken by the {@code BufferPool}'s {@code BufferListener} callback.
     *
     * <p>Caller MUST NOT hold {@code SpillFileReader.lock}. Parking inside the lock would stall the
     * checkpoint trigger whenever buffer-pool pressure causes a wait, because the task thread
     * cannot acquire the lock to insert its barrier.
     *
     * @param channelInfo identifies the channel whose buffer pool to allocate from
     * @return a pooled buffer ready for writing recovered data
     * @throws InterruptedException if the thread is interrupted while waiting for a buffer
     * @throws IOException if an I/O error occurs while requesting the buffer
     */
    Buffer requestBufferBlocking(InputChannelInfo channelInfo)
            throws InterruptedException, IOException;

    /**
     * Called once at end of drain. Releases the exclusive buffers held by every source channel
     * served by this requester. Implementations are expected to iterate the source channels and
     * call {@code RecoveredInputChannel.releaseAllResources()} on each.
     *
     * <p>Single-threaded — no lock required. Drain has finished and no concurrent producer is
     * active at this point.
     *
     * @throws IOException if releasing any channel's resources fails
     */
    void releaseExclusiveBuffers() throws IOException;
}
