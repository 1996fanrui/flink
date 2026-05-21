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
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.memory.MemoryManager;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link SpillFileWriter}. */
class SpillFileWriterTest {

    @TempDir Path tempDir;

    private static final int BUF_SIZE = MemoryManager.DEFAULT_PAGE_SIZE;

    @Test
    void testWriteDelegates() throws Exception {
        SpillFile spillFile = new SpillFile(tempDir);
        FilteredBufferWriter accumulator = newAccumulator(spillFile);
        try (SpillFileWriter writer = new SpillFileWriter(spillFile, accumulator)) {
            InputChannelInfo c0 = new InputChannelInfo(0, 0);
            writer.write(c0, payloadBuffer(8, (byte) 0x11));
            writer.write(c0, payloadBuffer(13, (byte) 0x22));
        }
        // After close, all bytes (8 + 13 = 21) end up as a single entry — the writer accumulated
        // them in one post-filter buffer and the close-time flush stamped a single record.
        assertThat(spillFile.entries()).hasSize(1);
        assertThat(spillFile.entries().get(0).length).isEqualTo(8 + 13);
        assertThat(spillFile.entries().get(0).channelInfo).isEqualTo(new InputChannelInfo(0, 0));
    }

    @Test
    void testCloseOrdering() throws Exception {
        // close() must call FilteredBufferWriter.close() BEFORE SpillFile.close(): the
        // accumulator flushes residual bytes, which requires the spill file's FileChannels to
        // still be open. The SpillFile close inside the facade after the accumulator returns is
        // an idempotent no-op, but its position is load-bearing for the invariant.
        //
        // Verification strategy: drive a write so that there is a non-zero residual, then close.
        // If the order were swapped (SpillFile.close before accumulator.close), the accumulator
        // would attempt to write to a closed FileChannel and throw — which is exactly the
        // failure mode this test exists to prevent.
        SpillFile spillFile = new SpillFile(tempDir);
        FilteredBufferWriter accumulator = newAccumulator(spillFile);
        SpillFileWriter writer = new SpillFileWriter(spillFile, accumulator);

        writer.write(new InputChannelInfo(0, 0), payloadBuffer(7, (byte) 0x33));
        assertThat(spillFile.entries()).isEmpty();

        writer.close();

        assertThat(spillFile.entries()).hasSize(1);
        assertThat(spillFile.entries().get(0).length).isEqualTo(7);
        assertThat(spillFile.isClosed()).isTrue();
    }

    @Test
    void testCloseIsIdempotent() throws Exception {
        SpillFile spillFile = new SpillFile(tempDir);
        FilteredBufferWriter accumulator = newAccumulator(spillFile);
        SpillFileWriter writer = new SpillFileWriter(spillFile, accumulator);

        writer.close();
        // Second close must not throw and must not produce extra entries.
        writer.close();
        writer.close();
        assertThat(spillFile.isClosed()).isTrue();
    }

    @Test
    void testGetSpillFileReturnsConstructorArg() {
        SpillFile spillFile = new SpillFile(tempDir);
        FilteredBufferWriter accumulator = newAccumulator(spillFile);
        SpillFileWriter writer = new SpillFileWriter(spillFile, accumulator);
        assertThat(writer.getSpillFile()).isSameAs(spillFile);
    }

    // -------------------------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------------------------

    private static FilteredBufferWriter newAccumulator(SpillFile spillFile) {
        return new FilteredBufferWriter(
                spillFile,
                newHeapBuffer(BUF_SIZE),
                newHeapBuffer(BUF_SIZE),
                () -> newHeapBuffer(BUF_SIZE));
    }

    private static Buffer newHeapBuffer(int size) {
        MemorySegment seg = MemorySegmentFactory.allocateUnpooledSegment(size);
        return new NetworkBuffer(seg, MemorySegment::free);
    }

    private static Buffer payloadBuffer(int length, byte fill) {
        MemorySegment seg = MemorySegmentFactory.allocateUnpooledSegment(length);
        byte[] data = new byte[length];
        java.util.Arrays.fill(data, fill);
        seg.put(0, data);
        NetworkBuffer buf = new NetworkBuffer(seg, MemorySegment::free);
        buf.setSize(length);
        return buf;
    }
}
