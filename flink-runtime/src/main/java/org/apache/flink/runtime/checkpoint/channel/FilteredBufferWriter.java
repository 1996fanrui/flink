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

/**
 * Accumulates filter-phase output bytes for one input channel at a time and flushes to a {@link
 * SpillFile} on two triggers: (a) the input channel switches, or (b) the accumulator buffer fills
 * up. {@link #close()} flushes any residual bytes before delegating to the spill file.
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
     * #beginChannel}). Set by {@link #beginChannel}; consumed and reset by {@link #flush()}.
     */
    private InputChannelInfo currentChannel;

    private boolean closed = false;

    public FilteredBufferWriter(SpillFile spillFile, Buffer outputBuffer) {
        this.spillFile = checkNotNull(spillFile);
        this.outputBuffer = checkNotNull(outputBuffer);
    }

    /**
     * Declares the input channel that will own the next batch of filter output bytes. Flushes the
     * accumulator first if the previous channel was different — every {@link SpillFile} entry
     * carries exactly one channel's bytes.
     */
    public void beginChannel(InputChannelInfo channelInfo) throws IOException {
        checkNotNull(channelInfo);
        if (currentChannel != null
                && !currentChannel.equals(channelInfo)
                && outputBuffer.getSize() > 0) {
            flush();
        }
        currentChannel = channelInfo;
    }

    /**
     * {@link BufferSupplier} entry. Returns the single accumulator buffer to the filter; if the
     * accumulator is already at capacity, flushes its bytes to the spill file first so the filter
     * receives a buffer with writable space. Callers must invoke {@link #beginChannel} first so the
     * flush has a channel tag.
     *
     * <p>The returned buffer is {@code retainBuffer()}-bumped so the filter's {@code
     * recycleBuffer()} after writing does not push the accumulator's refCount to zero.
     */
    @Override
    public Buffer requestBufferBlocking() throws IOException {
        assert currentChannel != null : "beginChannel must be called before requestBufferBlocking";
        if (outputBuffer.getSize() == outputBuffer.getMaxCapacity()) {
            InputChannelInfo saved = currentChannel;
            flush();
            currentChannel = saved;
        }
        return outputBuffer.retainBuffer();
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
        assert currentChannel != null : "flush invoked with no currentChannel";
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
