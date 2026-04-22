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

import java.io.IOException;

/**
 * Dispatches filtered channel state data across multiple channels' {@link
 * org.apache.flink.runtime.io.network.partition.consumer.RecoveredBufferStore}s, managing the
 * buffer/disk backend transparently. Callers use {@link #write(byte[], int, InputChannelInfo)} to
 * push data for a target channel, and the implementation decides whether to use a network buffer
 * (P1), spill to disk (P2), or replay from disk (P3).
 */
@Internal
public interface FilteredBufferDispatcher extends AutoCloseable {

    /**
     * Writes data for the given channel.
     *
     * @param data the byte array containing the data
     * @param length the number of bytes to write from the data array
     * @param channelInfo the target input channel
     * @throws IOException if an I/O error occurs
     * @throws InterruptedException if the thread is interrupted while waiting for resources
     */
    void write(byte[] data, int length, InputChannelInfo channelInfo)
            throws IOException, InterruptedException;

    /**
     * Flushes any buffered data. After flush, no more writes are accepted.
     *
     * @throws IOException if an I/O error occurs
     */
    void flush() throws IOException;

    /**
     * Drains all spilled data to buffers, cleans up spill files, and marks all stores as complete.
     * Idempotent: calling close() multiple times has no additional effect.
     *
     * @throws IOException if an I/O error occurs
     * @throws InterruptedException if the thread is interrupted during the blocking drain
     */
    @Override
    void close() throws IOException, InterruptedException;
}
