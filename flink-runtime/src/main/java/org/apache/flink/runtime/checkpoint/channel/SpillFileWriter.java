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
 * Accumulates filter-phase output bytes for one input channel at a time and flushes to a {@link
 * SpillFile} on two triggers: (a) the input channel switches, or (b) the accumulator buffer fills
 * up. Both triggers are detected inside {@link #requestBufferBlocking(InputChannelInfo)}. {@link
 * #close()} flushes any residual bytes.
 *
 * <p>The writer implements {@link BufferSupplier}: filter callers pass {@code this} as the supplier
 * so that filter output bytes land directly in the accumulator — no intermediate buffer copy. The
 * accumulator's underlying {@link org.apache.flink.core.memory.MemorySegment} comes from a single
 * heap buffer that lives for the entire filter phase.
 *
 * <p>Single-writer invariant — every mutating method assumes the {@code channelIOExecutor} is the
 * sole caller. No internal synchronization is performed.
 */
@Internal
public final class SpillFileWriter implements Closeable, BufferSupplier {

    private final SpillFile spillFile;

    /**
     * The single accumulator buffer. Wraps a heap {@link
     * org.apache.flink.core.memory.MemorySegment} with a no-op recycler so the segment survives
     * intermediate {@code recycleBuffer()} calls from the filter; the owning handler frees the
     * segment in its {@code close()}.
     */
    private final Buffer outputBuffer;

    /**
     * The input channel that owns the bytes currently sitting in {@link #outputBuffer}. {@code
     * null} when the accumulator is empty.
     */
    private InputChannelInfo currentChannel;

    private boolean closed = false;

    public SpillFileWriter(SpillFile spillFile, Buffer outputBuffer) {
        this.spillFile = checkNotNull(spillFile);
        this.outputBuffer = checkNotNull(outputBuffer);
    }

    /** Returns the underlying {@link SpillFile} so the drain can read it post-close. */
    public SpillFile getSpillFile() {
        return spillFile;
    }

    /**
     * {@link BufferSupplier} entry. Returns the single accumulator buffer to the filter, tagged
     * with the destination channel. Flushes the accumulator first if either (a) the previous
     * channel was different or (b) the accumulator is already at capacity.
     */
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

    /** Flushes readable accumulator bytes to the spill file and resets the buffer. */
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

    /**
     * Flushes residual bytes. Does not touch the {@link SpillFile} lifecycle: the producer holds
     * the initial ref-count grant until handoff to the drain reader.
     */
    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        flush();
    }
}
