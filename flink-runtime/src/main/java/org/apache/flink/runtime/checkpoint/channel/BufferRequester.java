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
 * Funnels every buffer allocation that the spill-file drain needs, isolating the drain from the
 * cross-package channel release primitive.
 */
@Internal
public interface BufferRequester {

    /**
     * Blocks until a buffer is available from the source channel's pool.
     *
     * <p>Caller MUST NOT hold {@code SpillFileReader.lock}: parking inside the lock would stall the
     * checkpoint trigger whenever buffer-pool pressure causes a wait, because the task thread
     * cannot acquire the lock to insert its barrier.
     */
    Buffer requestBufferBlocking(InputChannelInfo channelInfo)
            throws InterruptedException, IOException;

    /**
     * Releases the exclusive buffers held by every source channel served by this requester. Called
     * once at end of drain; single-threaded — no concurrent producer is active at this point.
     */
    void releaseExclusiveBuffers() throws IOException;
}
