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
 * #close()} flushes any residual bytes before delegating to the spill file.
 *
 * <p>The accumulator implements {@link BufferSupplier}: filter callers pass {@code this} as the
 * supplier so that filter output bytes land directly in the accumulator — no intermediate buffer
 * copy. The accumulator's underlying {@link org.apache.flink.core.memory.MemorySegment} comes from
 * a single pool buffer that lives for the entire filter phase.
 *
 * <p>Single-writer invariant — every mutating method assumes the {@code channelIOExecutor} is the
 * sole caller. No internal synchronization is performed.
 */
@Internal
public final class FilteredBufferWriter implements Closeable, BufferSupplier {

    private final SpillFile spillFile;

    /**
     * The single accumulator buffer. Wraps a pool-backed {@link
     * org.apache.flink.core.memory.MemorySegment} with a no-op recycler so the segment survives
     * intermediate {@code recycleBuffer()} calls from the filter; the owning handler recycles the
     * pool buffer in its {@code close()} to return memory to the pool.
     */
    private final Buffer outputBuffer;

    /**
     * The input channel that owns the bytes currently sitting in {@link #outputBuffer}. {@code
     * null} when the accumulator is empty (after a flush or before the first {@link
     * #requestBufferBlocking}). Updated by {@link #requestBufferBlocking}; consumed and reset to
     * {@code null} by {@link #flush()}.
     */
    private InputChannelInfo currentChannel;

    private boolean closed = false;

    public FilteredBufferWriter(SpillFile spillFile, Buffer outputBuffer) {
        this.spillFile = checkNotNull(spillFile);
        this.outputBuffer = checkNotNull(outputBuffer);
    }

    /**
     * {@link BufferSupplier} entry. Returns the single accumulator buffer to the filter, tagged
     * with the destination channel. Flushes the accumulator first if either (a) the previous
     * channel was different — every {@link SpillFile} entry carries exactly one channel's bytes —
     * or (b) the accumulator is already at capacity, so the filter receives a buffer with writable
     * space. After any flush, {@code currentChannel} is set to the supplied {@code channelInfo}.
     *
     * <p>The returned buffer is the accumulator itself — the filter writes into it but does NOT own
     * it. The handler that constructed this writer (and holds the underlying pool buffer) is
     * responsible for recycling. Callers must not call {@code recycleBuffer()} on the returned
     * value.
     */
    @Override
    public Buffer requestBufferBlocking(InputChannelInfo channelInfo) throws IOException {
        checkNotNull(channelInfo);
        boolean channelSwitch = currentChannel != null && !currentChannel.equals(channelInfo);
        boolean bufferFull = outputBuffer.getSize() == outputBuffer.getMaxCapacity();
        if ((channelSwitch || bufferFull) && outputBuffer.getSize() > 0) {
            // flush() resets currentChannel to null; the assignment below re-tags the accumulator
            // with the caller-supplied channelInfo before returning.
            flush();
        }
        currentChannel = channelInfo;
        return outputBuffer;
    }

    /**
     * Flushes the accumulator's readable bytes to the spill file under {@link #currentChannel} and
     * resets the buffer. No-op when the accumulator is empty. Public so the owning handler can
     * force a residual flush at end-of-filter before {@link #close()} closes the spill file.
     */
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
     * Flushes any residual bytes. SpillFile lifecycle is not managed here — the owning {@link
     * SpillFileWriter} releases the writer's ref-count grant on its own close. Idempotent. Does not
     * recycle the pool buffer backing {@link #outputBuffer} — the owning handler holds that pool
     * buffer and returns it to the pool on its own close path.
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
