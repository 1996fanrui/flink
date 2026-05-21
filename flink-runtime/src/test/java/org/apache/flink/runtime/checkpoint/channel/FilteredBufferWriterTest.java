/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.checkpoint.channel;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.memory.MemoryManager;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link FilteredBufferWriter}. */
class FilteredBufferWriterTest {

    @TempDir Path tempDir;

    private static final int BUF_SIZE = MemoryManager.DEFAULT_PAGE_SIZE;

    @Test
    void testWriteAccumulatesUntilPostfilterBufferFull() throws Exception {
        SpillFile spillFile = new SpillFile(tempDir);
        Buffer prefilter = newHeapBuffer(BUF_SIZE);
        Buffer postfilter = newHeapBuffer(BUF_SIZE);
        TrackingHook hook = new TrackingHook(BUF_SIZE);

        try (FilteredBufferWriter writer =
                new FilteredBufferWriter(spillFile, prefilter, postfilter, hook)) {
            InputChannelInfo c0 = new InputChannelInfo(0, 0);
            // Write 100 bytes — far from filling the buffer. No spill.
            writer.write(c0, payloadBuffer(100, (byte) 0x41));
            assertThat(spillFile.entries()).isEmpty();

            // Another 200 bytes — still under capacity, still no spill.
            writer.write(c0, payloadBuffer(200, (byte) 0x42));
            assertThat(spillFile.entries()).isEmpty();
        }

        // close() flushes residual content as one entry.
        assertThat(spillFile.entries()).hasSize(1);
        assertThat(spillFile.entries().get(0).length).isEqualTo(300);
        // Hook never asked for a fresh post-filter buffer (we never filled the active one).
        assertThat(hook.callCount).isEqualTo(0);
    }

    @Test
    void testWriteSpanningMultipleBuffersProducesMultipleEntries() throws Exception {
        SpillFile spillFile = new SpillFile(tempDir);
        Buffer prefilter = newHeapBuffer(BUF_SIZE);
        Buffer postfilter = newHeapBuffer(BUF_SIZE);
        TrackingHook hook = new TrackingHook(BUF_SIZE);

        try (FilteredBufferWriter writer =
                new FilteredBufferWriter(spillFile, prefilter, postfilter, hook)) {
            InputChannelInfo c0 = new InputChannelInfo(0, 0);
            // Write 2.5x BUF_SIZE worth of bytes in one call. Should rotate twice and leave the
            // tail in the active post-filter buffer.
            int total = BUF_SIZE * 2 + BUF_SIZE / 2;
            writer.write(c0, payloadBuffer(total, (byte) 0x55));

            // Two flushes happened during write — that's the segment count produced so far.
            List<SpillFile.Entry> entriesDuringWrite = spillFile.entries();
            assertThat(entriesDuringWrite).hasSize(2);
            assertThat(entriesDuringWrite.get(0).length).isEqualTo(BUF_SIZE);
            assertThat(entriesDuringWrite.get(1).length).isEqualTo(BUF_SIZE);
        }

        // close() flushes the residual half buffer as the third entry.
        List<SpillFile.Entry> entriesAfterClose = spillFile.entries();
        assertThat(entriesAfterClose).hasSize(3);
        assertThat(entriesAfterClose.get(2).length).isEqualTo(BUF_SIZE / 2);
        // The hook supplied two new post-filter buffers during the rotates.
        assertThat(hook.callCount).isEqualTo(2);
    }

    @Test
    void testPrefilterBufferIsStableInstance() throws Exception {
        SpillFile spillFile = new SpillFile(tempDir);
        Buffer prefilter = newHeapBuffer(BUF_SIZE);
        Buffer postfilter = newHeapBuffer(BUF_SIZE);
        TrackingHook hook = new TrackingHook(BUF_SIZE);

        try (FilteredBufferWriter writer =
                new FilteredBufferWriter(spillFile, prefilter, postfilter, hook)) {
            Buffer a = writer.getPrefilterBuffer();
            Buffer b = writer.getPrefilterBuffer();
            Buffer c = writer.getPrefilterBuffer();
            assertThat(a).isSameAs(prefilter);
            assertThat(b).isSameAs(prefilter);
            assertThat(c).isSameAs(prefilter);
        }
    }

    @Test
    void testCloseFlushesRemainingThenClosesSpillFile() throws Exception {
        SpillFile spillFile = new SpillFile(tempDir);
        Buffer prefilter = newHeapBuffer(BUF_SIZE);
        Buffer postfilter = newHeapBuffer(BUF_SIZE);
        TrackingHook hook = new TrackingHook(BUF_SIZE);

        FilteredBufferWriter writer =
                new FilteredBufferWriter(spillFile, prefilter, postfilter, hook);

        InputChannelInfo c0 = new InputChannelInfo(0, 0);
        writer.write(c0, payloadBuffer(7, (byte) 0x77));
        assertThat(spillFile.entries()).isEmpty();
        assertThat(spillFile.isClosed()).isFalse();

        writer.close();
        assertThat(spillFile.entries()).hasSize(1);
        assertThat(spillFile.entries().get(0).length).isEqualTo(7);
        assertThat(spillFile.isClosed()).isTrue();
    }

    @Test
    void testWriteAfterCloseThrows() throws Exception {
        SpillFile spillFile = new SpillFile(tempDir);
        Buffer prefilter = newHeapBuffer(BUF_SIZE);
        Buffer postfilter = newHeapBuffer(BUF_SIZE);
        TrackingHook hook = new TrackingHook(BUF_SIZE);

        FilteredBufferWriter writer =
                new FilteredBufferWriter(spillFile, prefilter, postfilter, hook);
        writer.close();
        assertThatThrownBy(
                        () ->
                                writer.write(
                                        new InputChannelInfo(0, 0), payloadBuffer(4, (byte) 0x33)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("closed");
    }

    @Test
    void testCloseIsIdempotent() throws Exception {
        SpillFile spillFile = new SpillFile(tempDir);
        Buffer prefilter = newHeapBuffer(BUF_SIZE);
        Buffer postfilter = newHeapBuffer(BUF_SIZE);
        TrackingHook hook = new TrackingHook(BUF_SIZE);

        FilteredBufferWriter writer =
                new FilteredBufferWriter(spillFile, prefilter, postfilter, hook);
        writer.write(new InputChannelInfo(0, 0), payloadBuffer(5, (byte) 0x99));
        writer.close();
        // Second close must not throw and must not produce extra entries.
        int entriesAfterFirstClose = spillFile.entries().size();
        writer.close();
        assertThat(spillFile.entries()).hasSize(entriesAfterFirstClose);
    }

    // -------------------------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------------------------

    private static Buffer newHeapBuffer(int size) {
        MemorySegment seg = MemorySegmentFactory.allocateUnpooledSegment(size);
        BufferRecycler resetOnly = MemorySegment::free;
        return new NetworkBuffer(seg, resetOnly);
    }

    /** Creates a Buffer pre-filled with {@code length} copies of {@code fill}. */
    private static Buffer payloadBuffer(int length, byte fill) {
        MemorySegment seg = MemorySegmentFactory.allocateUnpooledSegment(length);
        byte[] data = new byte[length];
        java.util.Arrays.fill(data, fill);
        seg.put(0, data);
        NetworkBuffer buf = new NetworkBuffer(seg, MemorySegment::free);
        buf.setSize(length);
        return buf;
    }

    /**
     * {@link FilteredBufferWriter.BufferPoolHook} that hands out heap-backed buffers and counts.
     */
    private static final class TrackingHook implements FilteredBufferWriter.BufferPoolHook {
        private final int bufferSize;
        private final List<Buffer> issued = new ArrayList<>();
        int callCount;

        TrackingHook(int bufferSize) {
            this.bufferSize = bufferSize;
        }

        @Override
        public Buffer requestPostfilterBuffer() throws IOException {
            callCount++;
            Buffer b = newHeapBuffer(bufferSize);
            issued.add(b);
            return b;
        }
    }
}
