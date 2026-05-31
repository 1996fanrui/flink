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
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

class SpillFileWriterTest {

    @TempDir Path tempDir;

    private static final int BUF_SIZE = MemoryManager.DEFAULT_PAGE_SIZE;

    @Test
    void testChannelSwitchFlushesAndKeepsEntryPerChannel() throws Exception {
        SpillFile spillFile = new SpillFile(tempDir, 4096);
        try (SpillFileWriter writer = newWriter(spillFile)) {
            InputChannelInfo c0 = new InputChannelInfo(0, 0);
            InputChannelInfo c1 = new InputChannelInfo(0, 1);

            writeBytes(writer.requestBufferBlocking(c0), 100, (byte) 0x41);
            assertThat(spillFile.entries()).isEmpty();

            Buffer slotForC1 = writer.requestBufferBlocking(c1);
            assertThat(spillFile.entries()).hasSize(1);
            assertThat(spillFile.entries().get(0).channelInfo).isEqualTo(c0);
            assertThat(spillFile.entries().get(0).length).isEqualTo(100);

            writeBytes(slotForC1, 50, (byte) 0x42);
        }

        assertThat(spillFile.entries()).hasSize(2);
        assertThat(spillFile.entries().get(1).channelInfo).isEqualTo(new InputChannelInfo(0, 1));
        assertThat(spillFile.entries().get(1).length).isEqualTo(50);
    }

    @Test
    void testBufferFullFlushesInsideRequest() throws Exception {
        SpillFile spillFile = new SpillFile(tempDir, 4096);
        try (SpillFileWriter writer = newWriter(spillFile)) {
            InputChannelInfo c0 = new InputChannelInfo(0, 0);
            writeBytes(writer.requestBufferBlocking(c0), BUF_SIZE, (byte) 0x55);
            assertThat(spillFile.entries()).isEmpty();

            Buffer fresh = writer.requestBufferBlocking(c0);
            assertThat(spillFile.entries()).hasSize(1);
            assertThat(spillFile.entries().get(0).channelInfo).isEqualTo(c0);
            assertThat(spillFile.entries().get(0).length).isEqualTo(BUF_SIZE);
            assertThat(fresh.getSize()).isZero();
        }
    }

    @Test
    void testCloseFlushesResidualBytes() throws Exception {
        SpillFile spillFile = new SpillFile(tempDir, 4096);
        SpillFileWriter writer = newWriter(spillFile);

        Buffer slot = writer.requestBufferBlocking(new InputChannelInfo(0, 0));
        writeBytes(slot, 7, (byte) 0x33);
        assertThat(spillFile.entries()).isEmpty();

        writer.close();
        assertThat(spillFile.entries()).hasSize(1);
        assertThat(spillFile.entries().get(0).length).isEqualTo(7);
    }

    @Test
    void testCloseIsIdempotent() throws Exception {
        SpillFile spillFile = new SpillFile(tempDir, 4096);
        SpillFileWriter writer = newWriter(spillFile);

        writer.close();
        writer.close();
        writer.close();
        assertThat(spillFile.entries()).isEmpty();
    }

    @Test
    void testGetSpillFileReturnsConstructorArg() {
        SpillFile spillFile = new SpillFile(tempDir, 4096);
        SpillFileWriter writer = newWriter(spillFile);
        assertThat(writer.getSpillFile()).isSameAs(spillFile);
    }

    private static SpillFileWriter newWriter(SpillFile spillFile) {
        return new SpillFileWriter(spillFile, newHeapBuffer(BUF_SIZE));
    }

    private static Buffer newHeapBuffer(int size) {
        MemorySegment seg = MemorySegmentFactory.allocateUnpooledSegment(size);
        return new NetworkBuffer(seg, MemorySegment::free);
    }

    private static void writeBytes(Buffer buf, int length, byte fill) {
        int writeAt = buf.getMemorySegmentOffset() + buf.getSize();
        byte[] data = new byte[length];
        Arrays.fill(data, fill);
        buf.getMemorySegment().put(writeAt, data);
        buf.setSize(buf.getSize() + length);
    }
}
