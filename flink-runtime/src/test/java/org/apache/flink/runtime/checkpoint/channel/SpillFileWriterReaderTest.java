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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration tests for {@link SpillFileWriter} and {@link SpillFileReader} together. Verifies that
 * data written by the writer can be correctly read back by the reader.
 */
class SpillFileWriterReaderTest {

    @TempDir Path tempDir;

    @Test
    void testWriteAndReadSingleBuffer() throws IOException {
        File spillFile = new File(tempDir.toFile(), "test-single.tmp");

        byte[] data = {1, 2, 3, 4, 5};
        Buffer buffer = createDataBuffer(data);

        try (SpillFileWriter writer = new SpillFileWriter(spillFile)) {
            writer.writeBuffer(buffer);
        } finally {
            buffer.recycleBuffer();
        }

        try (SpillFileReader reader = new SpillFileReader(spillFile)) {
            assertThat(reader.hasRemaining()).isTrue();

            Buffer target = createEmptyBuffer(1024);
            boolean success = reader.readNextTo(target, 5);
            assertThat(success).isTrue();
            assertThat(target.readableBytes()).isEqualTo(5);

            byte[] readData = new byte[5];
            target.getMemorySegment().get(0, readData, 0, 5);
            assertThat(readData).isEqualTo(data);
            target.recycleBuffer();

            assertThat(reader.hasRemaining()).isFalse();
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
            writer.writeBuffer(buf1);
            writer.writeBuffer(buf2);
            writer.writeBuffer(buf3);
        } finally {
            buf1.recycleBuffer();
            buf2.recycleBuffer();
            buf3.recycleBuffer();
        }

        try (SpillFileReader reader = new SpillFileReader(spillFile)) {
            assertReadEquals(reader, data1);
            assertReadEquals(reader, data2);
            assertReadEquals(reader, data3);
            assertThat(reader.hasRemaining()).isFalse();
        }
    }

    @Test
    void testBytesWrittenTracking() throws IOException {
        File spillFile = new File(tempDir.toFile(), "test-tracking.tmp");

        byte[] data = new byte[100];
        Buffer buffer = createDataBuffer(data);

        try (SpillFileWriter writer = new SpillFileWriter(spillFile)) {
            assertThat(writer.getBytesWritten()).isEqualTo(0);
            writer.writeBuffer(buffer);
            assertThat(writer.getBytesWritten()).isEqualTo(100);
        } finally {
            buffer.recycleBuffer();
        }
    }

    @Test
    void testEmptyFileReturnsEof() throws IOException {
        File spillFile = new File(tempDir.toFile(), "test-empty.tmp");
        spillFile.createNewFile();

        try (SpillFileReader reader = new SpillFileReader(spillFile)) {
            Buffer target = createEmptyBuffer(1024);
            assertThat(reader.readNextTo(target, 5)).isFalse();
            target.recycleBuffer();
            assertThat(reader.hasRemaining()).isFalse();
        }
    }

    @Test
    void testReadToOutputStream() throws IOException {
        File spillFile = new File(tempDir.toFile(), "test-outputstream.tmp");

        byte[] data1 = {1, 2};
        byte[] data2 = {3, 4, 5};
        Buffer buf1 = createDataBuffer(data1);
        Buffer buf2 = createDataBuffer(data2);

        try (SpillFileWriter writer = new SpillFileWriter(spillFile)) {
            writer.writeBuffer(buf1);
            writer.writeBuffer(buf2);
        } finally {
            buf1.recycleBuffer();
            buf2.recycleBuffer();
        }

        try (SpillFileReader reader = new SpillFileReader(spillFile)) {
            ByteArrayOutputStream baos1 = new ByteArrayOutputStream();
            reader.readNextTo(baos1, 2);
            assertThat(baos1.toByteArray()).isEqualTo(data1);

            ByteArrayOutputStream baos2 = new ByteArrayOutputStream();
            reader.readNextTo(baos2, 3);
            assertThat(baos2.toByteArray()).isEqualTo(data2);
        }
    }

    @Test
    void testLargeBufferRoundTrip() throws IOException {
        File spillFile = new File(tempDir.toFile(), "test-large.tmp");
        int numEntries = 100;
        byte[][] allData = new byte[numEntries][];

        try (SpillFileWriter writer = new SpillFileWriter(spillFile)) {
            for (int i = 0; i < numEntries; i++) {
                allData[i] = new byte[] {(byte) i, (byte) (i + 1), (byte) (i + 2)};
                Buffer buf = createDataBuffer(allData[i]);
                writer.writeBuffer(buf);
                buf.recycleBuffer();
            }
        }

        try (SpillFileReader reader = new SpillFileReader(spillFile)) {
            for (int i = 0; i < numEntries; i++) {
                Buffer target = createEmptyBuffer(1024);
                assertThat(reader.readNextTo(target, 3)).isTrue();
                byte[] readData = new byte[3];
                target.getMemorySegment().get(0, readData, 0, 3);
                assertThat(readData).isEqualTo(allData[i]);
                target.recycleBuffer();
            }
            assertThat(reader.hasRemaining()).isFalse();
        }
    }

    @Test
    void testTruncatedFileThrowsIOException() throws IOException {
        File spillFile = new File(tempDir.toFile(), "test-truncated.tmp");
        byte[] data = {1, 2, 3, 4, 5};
        Buffer buffer = createDataBuffer(data);
        try (SpillFileWriter writer = new SpillFileWriter(spillFile)) {
            writer.writeBuffer(buffer);
        } finally {
            buffer.recycleBuffer();
        }

        // Truncate file to 3 bytes (out of 5)
        try (java.io.RandomAccessFile raf = new java.io.RandomAccessFile(spillFile, "rw")) {
            raf.setLength(3);
        }

        // readNextTo(Buffer, int) should throw on partial read
        try (SpillFileReader reader = new SpillFileReader(spillFile)) {
            Buffer target = createEmptyBuffer(1024);
            assertThatThrownBy(() -> reader.readNextTo(target, 5))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("expected");
            target.recycleBuffer();
        }

        // readNextTo(OutputStream, int) should also throw
        try (SpillFileReader reader = new SpillFileReader(spillFile)) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            assertThatThrownBy(() -> reader.readNextTo(baos, 5))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("expected");
        }
    }

    // --- Helper methods ---

    private static void assertReadEquals(SpillFileReader reader, byte[] expected)
            throws IOException {
        Buffer target = createEmptyBuffer(1024);
        assertThat(reader.readNextTo(target, expected.length)).isTrue();
        byte[] readData = new byte[expected.length];
        target.getMemorySegment().get(0, readData, 0, expected.length);
        assertThat(readData).isEqualTo(expected);
        target.recycleBuffer();
    }

    private static Buffer createDataBuffer(byte[] data) {
        MemorySegment segment = MemorySegmentFactory.allocateUnpooledSegment(data.length);
        segment.put(0, data, 0, data.length);
        return new NetworkBuffer(
                segment, FreeingBufferRecycler.INSTANCE, Buffer.DataType.DATA_BUFFER, data.length);
    }

    private static Buffer createEmptyBuffer(int capacity) {
        MemorySegment segment = MemorySegmentFactory.allocateUnpooledSegment(capacity);
        return new NetworkBuffer(
                segment, FreeingBufferRecycler.INSTANCE, Buffer.DataType.DATA_BUFFER, 0);
    }
}
