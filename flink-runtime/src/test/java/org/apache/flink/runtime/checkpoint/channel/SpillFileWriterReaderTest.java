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

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link SpillFileWriter} and {@link SpillFileReader}. */
class SpillFileWriterReaderTest {

    @TempDir Path tempDir;

    @Test
    void testWriteAndReadSingleDataBuffer() throws IOException {
        File spillFile = new File(tempDir.toFile(), "test-single.tmp");

        byte[] data = {1, 2, 3, 4, 5};
        Buffer buffer = createDataBuffer(data);

        try (SpillFileWriter writer = new SpillFileWriter(spillFile)) {
            writer.writeBuffer(buffer, 0, 0);
        } finally {
            buffer.recycleBuffer();
        }

        try (SpillFileReader reader = new SpillFileReader(spillFile)) {
            assertThat(reader.hasRemaining()).isTrue();

            SpillFileReader.SpillEntry entry = reader.readNext();
            assertThat(entry).isNotNull();
            assertThat(entry.oldSubtaskIndex).isEqualTo(0);
            assertThat(entry.oldChannelIndex).isEqualTo(0);
            assertThat(entry.buffer.getDataType()).isEqualTo(Buffer.DataType.DATA_BUFFER);
            assertThat(entry.buffer.readableBytes()).isEqualTo(5);

            byte[] readData = new byte[5];
            entry.buffer.getMemorySegment().get(0, readData, 0, 5);
            assertThat(readData).isEqualTo(data);
            entry.buffer.recycleBuffer();

            assertThat(reader.readNext()).isNull();
        }
    }

    @Test
    void testWriteAndReadMultipleBuffersInOrder() throws IOException {
        File spillFile = new File(tempDir.toFile(), "test-multi.tmp");

        byte[] data1 = {10, 20, 30};
        byte[] data2 = {40, 50, 60, 70};
        byte[] data3 = {80};

        Buffer buf1 = createDataBuffer(data1);
        Buffer buf2 = createDataBuffer(data2);
        Buffer buf3 = createDataBuffer(data3);

        try (SpillFileWriter writer = new SpillFileWriter(spillFile)) {
            writer.writeBuffer(buf1, 1, 2);
            writer.writeBuffer(buf2, 3, 4);
            writer.writeBuffer(buf3, 5, 6);
        } finally {
            buf1.recycleBuffer();
            buf2.recycleBuffer();
            buf3.recycleBuffer();
        }

        try (SpillFileReader reader = new SpillFileReader(spillFile)) {
            // First entry
            SpillFileReader.SpillEntry entry1 = reader.readNext();
            assertThat(entry1).isNotNull();
            assertThat(entry1.oldSubtaskIndex).isEqualTo(1);
            assertThat(entry1.oldChannelIndex).isEqualTo(2);
            assertThat(entry1.buffer.readableBytes()).isEqualTo(3);
            byte[] read1 = new byte[3];
            entry1.buffer.getMemorySegment().get(0, read1, 0, 3);
            assertThat(read1).isEqualTo(data1);
            entry1.buffer.recycleBuffer();

            // Second entry
            SpillFileReader.SpillEntry entry2 = reader.readNext();
            assertThat(entry2).isNotNull();
            assertThat(entry2.oldSubtaskIndex).isEqualTo(3);
            assertThat(entry2.oldChannelIndex).isEqualTo(4);
            assertThat(entry2.buffer.readableBytes()).isEqualTo(4);
            byte[] read2 = new byte[4];
            entry2.buffer.getMemorySegment().get(0, read2, 0, 4);
            assertThat(read2).isEqualTo(data2);
            entry2.buffer.recycleBuffer();

            // Third entry
            SpillFileReader.SpillEntry entry3 = reader.readNext();
            assertThat(entry3).isNotNull();
            assertThat(entry3.oldSubtaskIndex).isEqualTo(5);
            assertThat(entry3.oldChannelIndex).isEqualTo(6);
            assertThat(entry3.buffer.readableBytes()).isEqualTo(1);
            entry3.buffer.recycleBuffer();

            // EOF
            assertThat(reader.readNext()).isNull();
            assertThat(reader.hasRemaining()).isFalse();
        }
    }

    @Test
    void testWriteEventBuffer() throws IOException {
        File spillFile = new File(tempDir.toFile(), "test-event.tmp");

        byte[] data = {1, 2, 3};
        Buffer eventBuffer = createEventBuffer(data);

        try (SpillFileWriter writer = new SpillFileWriter(spillFile)) {
            writer.writeBuffer(eventBuffer, 0, 0);
        } finally {
            eventBuffer.recycleBuffer();
        }

        try (SpillFileReader reader = new SpillFileReader(spillFile)) {
            SpillFileReader.SpillEntry entry = reader.readNext();
            assertThat(entry).isNotNull();
            assertThat(entry.buffer.getDataType()).isEqualTo(Buffer.DataType.EVENT_BUFFER);
            entry.buffer.recycleBuffer();
        }
    }

    @Test
    void testBytesWrittenTracking() throws IOException {
        File spillFile = new File(tempDir.toFile(), "test-tracking.tmp");

        byte[] data = new byte[100];

        Buffer buffer = createDataBuffer(data);

        try (SpillFileWriter writer = new SpillFileWriter(spillFile)) {
            assertThat(writer.getBytesWritten()).isEqualTo(0);

            writer.writeBuffer(buffer, 0, 0);
            // Per entry: 4 (subtask) + 4 (channel) + 4 (length) + 100 (data) + 1 (flag) = 113
            assertThat(writer.getBytesWritten()).isEqualTo(113);
        } finally {
            buffer.recycleBuffer();
        }
    }

    @Test
    void testEmptyFileReturnsNull() throws IOException {
        File spillFile = new File(tempDir.toFile(), "test-empty.tmp");
        spillFile.createNewFile();

        try (SpillFileReader reader = new SpillFileReader(spillFile)) {
            assertThat(reader.readNext()).isNull();
            assertThat(reader.hasRemaining()).isFalse();
        }
    }

    @Test
    void testMixedDataAndEventBuffers() throws IOException {
        File spillFile = new File(tempDir.toFile(), "test-mixed.tmp");

        Buffer dataBuf = createDataBuffer(new byte[] {1, 2});
        Buffer eventBuf = createEventBuffer(new byte[] {3, 4, 5});

        try (SpillFileWriter writer = new SpillFileWriter(spillFile)) {
            writer.writeBuffer(dataBuf, 0, 1);
            writer.writeBuffer(eventBuf, 2, 3);
        } finally {
            dataBuf.recycleBuffer();
            eventBuf.recycleBuffer();
        }

        try (SpillFileReader reader = new SpillFileReader(spillFile)) {
            SpillFileReader.SpillEntry entry1 = reader.readNext();
            assertThat(entry1.buffer.getDataType()).isEqualTo(Buffer.DataType.DATA_BUFFER);
            assertThat(entry1.oldSubtaskIndex).isEqualTo(0);
            assertThat(entry1.oldChannelIndex).isEqualTo(1);
            entry1.buffer.recycleBuffer();

            SpillFileReader.SpillEntry entry2 = reader.readNext();
            assertThat(entry2.buffer.getDataType()).isEqualTo(Buffer.DataType.EVENT_BUFFER);
            assertThat(entry2.oldSubtaskIndex).isEqualTo(2);
            assertThat(entry2.oldChannelIndex).isEqualTo(3);
            entry2.buffer.recycleBuffer();
        }
    }

    // --- Helper methods ---

    private static Buffer createDataBuffer(byte[] data) {
        MemorySegment segment = MemorySegmentFactory.allocateUnpooledSegment(data.length);
        segment.put(0, data, 0, data.length);
        return new NetworkBuffer(
                segment, FreeingBufferRecycler.INSTANCE, Buffer.DataType.DATA_BUFFER, data.length);
    }

    private static Buffer createEventBuffer(byte[] data) {
        MemorySegment segment = MemorySegmentFactory.allocateUnpooledSegment(data.length);
        segment.put(0, data, 0, data.length);
        return new NetworkBuffer(
                segment, FreeingBufferRecycler.INSTANCE, Buffer.DataType.EVENT_BUFFER, data.length);
    }
}
