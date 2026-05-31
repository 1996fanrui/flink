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
import org.apache.flink.runtime.checkpoint.channel.ChannelStateFilteringHandler.BufferSupplier;
import org.apache.flink.runtime.io.network.buffer.Buffer;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Accumulates filter output for one input channel at a time and flushes it to a {@link SpillFile}
 * on channel switch, buffer full, or close.
 *
 * <p>Single-writer and intentionally unsynchronized.
 */
@Internal
public final class SpillFileWriter implements Closeable, BufferSupplier {

    private final SpillFile spillFile;

    private final Buffer outputBuffer;

    private InputChannelInfo currentChannel;

    private boolean closed = false;

    public SpillFileWriter(SpillFile spillFile, Buffer outputBuffer) {
        this.spillFile = checkNotNull(spillFile);
        this.outputBuffer = checkNotNull(outputBuffer);
    }

    public SpillFile getSpillFile() {
        return spillFile;
    }

    @Override
    public Buffer requestBufferBlocking(InputChannelInfo channelInfo) throws IOException {
        checkNotNull(channelInfo);
        boolean channelSwitch = currentChannel != null && !currentChannel.equals(channelInfo);
        boolean bufferFull = outputBuffer.getSize() == outputBuffer.getMaxCapacity();
        if ((channelSwitch || bufferFull) && outputBuffer.getSize() > 0) {
            flush();
        }
        currentChannel = channelInfo;
        return outputBuffer;
    }

    public void flush() throws IOException {
        if (outputBuffer.getSize() == 0) {
            return;
        }
        checkState(currentChannel != null, "flush invoked with no currentChannel");
        ByteBuffer payload = outputBuffer.getNioBufferReadable();
        spillFile.append(currentChannel, payload);
        outputBuffer.setReaderIndex(0);
        outputBuffer.setSize(0);
        currentChannel = null;
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        flush();
    }
}
