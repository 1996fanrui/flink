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

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Accumulates filter-phase output into a single reusable post-filter {@link Buffer} and flushes to
 * a {@link SpillFile} once the post-filter buffer fills up. Combined with a stable pre-filter
 * buffer instance, this bounds the filter-phase memory footprint to a constant.
 *
 * <p>Single-writer invariant — every mutating method (notably {@link #write}) assumes the {@code
 * channelIOExecutor} is the sole caller. No internal synchronization is performed.
 */
@Internal
public final class FilteredBufferWriter implements Closeable {

    /**
     * Hook used to obtain a fresh post-filter buffer once the active one has been flushed. The
     * filter-phase memory bound assumes this never falls back to an unbounded heap allocation; the
     * call may block until a pooled buffer becomes available.
     */
    public interface BufferPoolHook {
        /**
         * Returns the next post-filter buffer to accumulate into. Blocks until a buffer is
         * available; must not silently widen the memory budget by returning heap-allocated buffers.
         */
        Buffer requestPostfilterBuffer() throws InterruptedException, IOException;
    }

    private final SpillFile spillFile;
    private final Buffer prefilterBuffer;
    private final BufferPoolHook bufferPoolHook;

    // The active post-filter accumulator. Replaced (via the hook) on every flush — but the
    // previous instance is recycled before the new one starts receiving bytes, so the in-flight
    // count is still bounded by 1.
    private Buffer postfilterBuffer;

    // Channel that wrote the bytes currently sitting in postfilterBuffer. Used to stamp a residual
    // flush at close() time with the correct channel identity. Reset to null after every flush.
    private InputChannelInfo activeChannel;

    private boolean closed = false;

    public FilteredBufferWriter(
            SpillFile spillFile,
            Buffer prefilterBuffer,
            Buffer initialPostfilterBuffer,
            BufferPoolHook bufferPoolHook) {
        this.spillFile = checkNotNull(spillFile);
        this.prefilterBuffer = checkNotNull(prefilterBuffer);
        this.postfilterBuffer = checkNotNull(initialPostfilterBuffer);
        this.bufferPoolHook = checkNotNull(bufferPoolHook);
    }

    /**
     * Returns the stable pre-filter buffer instance. Callers (typically {@code
     * ChannelStateFilteringHandler.filterAndRewrite}'s buffer supplier) get the same reference on
     * every call so that filter output never expands the working-set buffer count.
     */
    public Buffer getPrefilterBuffer() {
        return prefilterBuffer;
    }

    /**
     * Appends {@code buf}'s readable bytes to the active post-filter buffer, flushing and rotating
     * whenever the accumulator fills up. A single {@code write} call can produce multiple {@link
     * SpillFile} entries when the source bytes span more than one post-filter buffer.
     *
     * <p>The caller retains ownership of {@code buf}; bytes are copied into the accumulator.
     *
     * @throws IllegalStateException if {@link #close()} has already been called.
     */
    public void write(InputChannelInfo channelInfo, Buffer buf)
            throws IOException, InterruptedException {
        if (closed) {
            throw new IllegalStateException("Cannot write to a closed FilteredBufferWriter.");
        }
        checkNotNull(channelInfo);
        checkNotNull(buf);

        int sourceOffset = buf.getReaderIndex();
        int remaining = buf.readableBytes();
        while (remaining > 0) {
            int free = freeCapacity(postfilterBuffer);
            if (free == 0) {
                // Active accumulator is already full from a previous write — flush before copying
                // any more bytes so we never reject input that would otherwise fit after rotation.
                rotatePostfilterBuffer();
                free = freeCapacity(postfilterBuffer);
            }
            int toCopy = Math.min(free, remaining);
            copyBytes(buf, sourceOffset, postfilterBuffer, toCopy);
            activeChannel = channelInfo;
            sourceOffset += toCopy;
            remaining -= toCopy;
            if (remaining > 0) {
                // More source bytes than the post-filter can hold: flush the now-full accumulator
                // and keep copying into a fresh one.
                rotatePostfilterBuffer();
            }
        }
    }

    /**
     * Flushes any remaining bytes in the active post-filter buffer, recycles it, and closes the
     * underlying {@link SpillFile}. Idempotent — repeated calls after the first are no-ops so the
     * facade {@code SpillFileWriter} can safely guarantee close ordering without double-close
     * concerns on the FileChannel layer.
     */
    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        try {
            flushActive();
        } finally {
            try {
                if (postfilterBuffer != null) {
                    postfilterBuffer.recycleBuffer();
                    postfilterBuffer = null;
                }
            } finally {
                spillFile.close();
            }
        }
    }

    // ---------------------------------------------------------------------------------------
    // Internal helpers
    // ---------------------------------------------------------------------------------------

    /** Flushes the active post-filter buffer to the spill file and obtains a fresh one. */
    private void rotatePostfilterBuffer() throws IOException, InterruptedException {
        flushActive();
        Buffer next = checkNotNull(bufferPoolHook.requestPostfilterBuffer());
        // Defensive reset — pool implementations vary on whether they reset indices for the
        // consumer. The accumulator must always start at size 0.
        next.setReaderIndex(0);
        next.setSize(0);
        postfilterBuffer = next;
    }

    /** Writes the active accumulator's readable bytes to the spill file and recycles it. */
    private void flushActive() throws IOException {
        if (postfilterBuffer == null || postfilterBuffer.readableBytes() == 0) {
            return;
        }
        ByteBuffer payload = postfilterBuffer.getNioBufferReadable();
        spillFile.append(activeChannel, payload);
        postfilterBuffer.setReaderIndex(0);
        postfilterBuffer.setSize(0);
        postfilterBuffer.recycleBuffer();
        postfilterBuffer = null;
        activeChannel = null;
    }

    private static int freeCapacity(Buffer buf) {
        return buf.getMaxCapacity() - buf.getSize();
    }

    private static void copyBytes(Buffer src, int srcOffset, Buffer dst, int length) {
        int dstWriteAt = dst.getMemorySegmentOffset() + dst.getSize();
        // src/dst memory-segment offsets are the absolute start of the buffer's data slice; the
        // logical reader index (passed as srcOffset) maps to absolute position by adding it.
        src.getMemorySegment()
                .copyTo(
                        src.getMemorySegmentOffset() + srcOffset,
                        dst.getMemorySegment(),
                        dstWriteAt,
                        length);
        dst.setSize(dst.getSize() + length);
    }
}
