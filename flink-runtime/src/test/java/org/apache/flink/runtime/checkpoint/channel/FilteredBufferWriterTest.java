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

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link FilteredBufferWriter}. */
class FilteredBufferWriterTest {

    @TempDir Path tempDir;

    private static final int BUF_SIZE = MemoryManager.DEFAULT_PAGE_SIZE;

    @Test
    void testChannelSwitchFlushesAndKeepsEntryPerChannel() throws Exception {
        // Bytes from different channels must never share a spill-file entry. The accumulator
        // flushes whenever requestBufferBlocking sees a different channel with pending bytes.
        SpillFile spillFile = new SpillFile(tempDir);
        Buffer accumulator = newHeapBuffer(BUF_SIZE);
        try (FilteredBufferWriter writer = new FilteredBufferWriter(spillFile, accumulator)) {
            InputChannelInfo c0 = new InputChannelInfo(0, 0);
            InputChannelInfo c1 = new InputChannelInfo(0, 1);

            writeBytes(writer.requestBufferBlocking(c0), 100, (byte) 0x41);
            // No flush yet — buffer not full, channel not changed.
            assertThat(spillFile.entries()).isEmpty();

            // Channel switch: requesting a buffer for c1 flushes channel 0's 100 bytes first.
            Buffer slotForC1 = writer.requestBufferBlocking(c1);
            assertThat(spillFile.entries()).hasSize(1);
            assertThat(spillFile.entries().get(0).channelInfo).isEqualTo(c0);
            assertThat(spillFile.entries().get(0).length).isEqualTo(100);

            writeBytes(slotForC1, 50, (byte) 0x42);
        }

        // close() flushes channel 1's 50 bytes as a separate entry.
        List<SpillFile.Entry> entries = spillFile.entries();
        assertThat(entries).hasSize(2);
        assertThat(entries.get(1).channelInfo)
                .as("second entry must carry channel-1's tag, not channel-0")
                .isEqualTo(new InputChannelInfo(0, 1));
        assertThat(entries.get(1).length).isEqualTo(50);
    }

    @Test
    void testBufferFullFlushesInsideRequest() throws Exception {
        SpillFile spillFile = new SpillFile(tempDir);
        Buffer accumulator = newHeapBuffer(BUF_SIZE);
        try (FilteredBufferWriter writer = new FilteredBufferWriter(spillFile, accumulator)) {
            InputChannelInfo c0 = new InputChannelInfo(0, 0);
            // Fill the accumulator exactly to capacity.
            writeBytes(writer.requestBufferBlocking(c0), BUF_SIZE, (byte) 0x55);
            // No flush yet — buffer is full but no further requestBufferBlocking has been called.
            assertThat(spillFile.entries()).isEmpty();

            // Next requestBufferBlocking detects size == capacity and flushes before returning.
            Buffer fresh = writer.requestBufferBlocking(c0);
            assertThat(spillFile.entries()).hasSize(1);
            assertThat(spillFile.entries().get(0).channelInfo).isEqualTo(c0);
            assertThat(spillFile.entries().get(0).length).isEqualTo(BUF_SIZE);
            assertThat(fresh.getSize()).isEqualTo(0);
        }
    }

    @Test
    void testCloseFlushesResidualBytes() throws Exception {
        SpillFile spillFile = new SpillFile(tempDir);
        Buffer accumulator = newHeapBuffer(BUF_SIZE);
        FilteredBufferWriter writer = new FilteredBufferWriter(spillFile, accumulator);

        InputChannelInfo c0 = new InputChannelInfo(0, 0);
        writeBytes(writer.requestBufferBlocking(c0), 7, (byte) 0x77);
        assertThat(spillFile.entries()).isEmpty();

        writer.close();
        assertThat(spillFile.entries()).hasSize(1);
        assertThat(spillFile.entries().get(0).length).isEqualTo(7);
        // SpillFile lifecycle is owned by SpillFileWriter (ref-count grant), not by the accumulator
        // — FilteredBufferWriter.close only flushes residual bytes.
    }

    @Test
    void testCloseIsIdempotent() throws Exception {
        SpillFile spillFile = new SpillFile(tempDir);
        Buffer accumulator = newHeapBuffer(BUF_SIZE);
        FilteredBufferWriter writer = new FilteredBufferWriter(spillFile, accumulator);

        writeBytes(writer.requestBufferBlocking(new InputChannelInfo(0, 0)), 5, (byte) 0x99);
        writer.close();
        int entriesAfterFirstClose = spillFile.entries().size();
        // Second close must not throw and must not produce extra entries.
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

    /** Appends {@code length} copies of {@code fill} to {@code buf}, advancing its size. */
    private static void writeBytes(Buffer buf, int length, byte fill) {
        int writeAt = buf.getMemorySegmentOffset() + buf.getSize();
        byte[] data = new byte[length];
        Arrays.fill(data, fill);
        buf.getMemorySegment().put(writeAt, data);
        buf.setSize(buf.getSize() + length);
    }
}
