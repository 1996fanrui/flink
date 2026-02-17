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

package org.apache.flink.runtime.io.network.buffer;

import org.apache.flink.core.memory.MemorySegment;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link LazyFileBuffer}. */
class LazyFileBufferTest {

    @TempDir Path tempDir;

    @Test
    void testWriteAndLazyLoad() throws IOException {
        File file = tempDir.resolve("buffer.bin").toFile();
        int capacity = 1024;
        byte[] testData = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};

        LazyFileBuffer buffer = new LazyFileBuffer(file, capacity);

        // Write data to the buffer
        int bytesWritten = buffer.writeBytes(new ByteArrayInputStream(testData), testData.length);
        assertThat(bytesWritten).isEqualTo(testData.length);

        // Finish writing
        buffer.finishWriting();

        // Verify the file exists after writing
        assertThat(file.exists()).isTrue();

        // Verify size is set correctly
        assertThat(buffer.getSize()).isEqualTo(testData.length);

        // Lazy load: getMemorySegment() should load data from file into memory
        MemorySegment segment = buffer.getMemorySegment();
        assertThat(segment).isNotNull();

        // Verify the loaded data matches what was written
        byte[] loadedData = new byte[testData.length];
        segment.get(0, loadedData, 0, testData.length);
        assertThat(loadedData).isEqualTo(testData);

        // Cleanup
        buffer.recycleBuffer();
    }

    @Test
    void testRecycleDeletesFileAndReleasesSegment() throws IOException {
        File file = tempDir.resolve("buffer_recycle.bin").toFile();
        int capacity = 256;
        byte[] testData = new byte[] {1, 2, 3, 4, 5};

        LazyFileBuffer buffer = new LazyFileBuffer(file, capacity);

        // Write and finish
        buffer.writeBytes(new ByteArrayInputStream(testData), testData.length);
        buffer.finishWriting();

        // Load into memory to create the segment
        MemorySegment segment = buffer.getMemorySegment();
        assertThat(segment).isNotNull();
        assertThat(file.exists()).isTrue();

        // Recycle should delete the file and release the segment
        buffer.recycleBuffer();

        // Verify the file is deleted
        assertThat(file.exists()).isFalse();

        // Verify the buffer is recycled
        assertThat(buffer.isRecycled()).isTrue();
    }

    @Test
    void testRetainAndRecycle() throws IOException {
        File file = tempDir.resolve("buffer_retain.bin").toFile();
        int capacity = 256;
        byte[] testData = new byte[] {10, 20, 30};

        LazyFileBuffer buffer = new LazyFileBuffer(file, capacity);

        // Write and finish
        buffer.writeBytes(new ByteArrayInputStream(testData), testData.length);
        buffer.finishWriting();

        // Load into memory
        buffer.getMemorySegment();

        // Initial reference count should be 1
        assertThat(buffer.refCnt()).isEqualTo(1);

        // Retain increases reference count
        Buffer retained = buffer.retainBuffer();
        assertThat(retained).isSameAs(buffer);
        assertThat(buffer.refCnt()).isEqualTo(2);

        // First recycle decreases reference count but doesn't free resources
        buffer.recycleBuffer();
        assertThat(buffer.refCnt()).isEqualTo(1);
        assertThat(buffer.isRecycled()).isFalse();
        assertThat(file.exists()).isTrue();

        // Second recycle should free resources
        buffer.recycleBuffer();
        assertThat(buffer.refCnt()).isZero();
        assertThat(buffer.isRecycled()).isTrue();
        assertThat(file.exists()).isFalse();
    }

    @Test
    void testMultipleReads() throws IOException {
        File file = tempDir.resolve("buffer_multi_read.bin").toFile();
        int capacity = 512;
        byte[] testData = new byte[] {100, (byte) 200, 50, 75, 25};

        LazyFileBuffer buffer = new LazyFileBuffer(file, capacity);

        // Write and finish
        buffer.writeBytes(new ByteArrayInputStream(testData), testData.length);
        buffer.finishWriting();

        // First read
        MemorySegment segment1 = buffer.getMemorySegment();
        byte[] firstRead = new byte[testData.length];
        segment1.get(0, firstRead, 0, testData.length);
        assertThat(firstRead).isEqualTo(testData);

        // Second read should return the same segment (data already in memory)
        MemorySegment segment2 = buffer.getMemorySegment();
        assertThat(segment2).isSameAs(segment1);

        byte[] secondRead = new byte[testData.length];
        segment2.get(0, secondRead, 0, testData.length);
        assertThat(secondRead).isEqualTo(testData);

        // Third read via getNioBufferReadable
        buffer.setReaderIndex(0);
        java.nio.ByteBuffer nioBuffer = buffer.getNioBufferReadable();
        byte[] thirdRead = new byte[testData.length];
        nioBuffer.get(thirdRead);
        assertThat(thirdRead).isEqualTo(testData);

        // Cleanup
        buffer.recycleBuffer();
    }

    @Test
    void testEmptyBuffer() throws IOException {
        File file = tempDir.resolve("buffer_empty.bin").toFile();
        int capacity = 128;

        LazyFileBuffer buffer = new LazyFileBuffer(file, capacity);

        // Finish writing without writing any data
        buffer.finishWriting();

        // Size should be 0
        assertThat(buffer.getSize()).isZero();

        // getMemorySegment should still work
        MemorySegment segment = buffer.getMemorySegment();
        assertThat(segment).isNotNull();

        // readableBytes should be 0
        assertThat(buffer.readableBytes()).isZero();

        // Cleanup
        buffer.recycleBuffer();
    }

    @Test
    void testBufferProperties() throws IOException {
        File file = tempDir.resolve("buffer_props.bin").toFile();
        int capacity = 256;
        byte[] testData = new byte[] {1, 2, 3, 4};

        LazyFileBuffer buffer = new LazyFileBuffer(file, capacity);

        // Test isBuffer default
        assertThat(buffer.isBuffer()).isTrue();

        // Test data type
        assertThat(buffer.getDataType()).isEqualTo(Buffer.DataType.DATA_BUFFER);

        // Test setDataType
        buffer.setDataType(Buffer.DataType.EVENT_BUFFER);
        assertThat(buffer.getDataType()).isEqualTo(Buffer.DataType.EVENT_BUFFER);
        assertThat(buffer.isBuffer()).isFalse();

        // Write and finish
        buffer.writeBytes(new ByteArrayInputStream(testData), testData.length);
        buffer.finishWriting();

        // Load segment
        buffer.getMemorySegment();

        // Test getMaxCapacity
        assertThat(buffer.getMaxCapacity()).isEqualTo(capacity);

        // Test reader/writer indices
        assertThat(buffer.getReaderIndex()).isZero();
        assertThat(buffer.getSize()).isEqualTo(testData.length);
        assertThat(buffer.readableBytes()).isEqualTo(testData.length);

        buffer.setReaderIndex(2);
        assertThat(buffer.getReaderIndex()).isEqualTo(2);
        assertThat(buffer.readableBytes()).isEqualTo(testData.length - 2);

        // Cleanup
        buffer.recycleBuffer();
    }

    @Test
    void testRecycleBeforeLoad() throws IOException {
        File file = tempDir.resolve("buffer_recycle_before_load.bin").toFile();
        int capacity = 256;
        byte[] testData = new byte[] {1, 2, 3};

        LazyFileBuffer buffer = new LazyFileBuffer(file, capacity);

        // Write and finish
        buffer.writeBytes(new ByteArrayInputStream(testData), testData.length);
        buffer.finishWriting();

        assertThat(file.exists()).isTrue();

        // Recycle without loading into memory
        buffer.recycleBuffer();

        // File should be deleted even without loading
        assertThat(file.exists()).isFalse();
        assertThat(buffer.isRecycled()).isTrue();
    }

    @Test
    void testWriteMultipleChunks() throws IOException {
        File file = tempDir.resolve("buffer_chunks.bin").toFile();
        int capacity = 1024;
        byte[] chunk1 = new byte[] {1, 2, 3, 4, 5};
        byte[] chunk2 = new byte[] {6, 7, 8, 9, 10};

        LazyFileBuffer buffer = new LazyFileBuffer(file, capacity);

        // Write first chunk
        int bytesWritten1 = buffer.writeBytes(new ByteArrayInputStream(chunk1), chunk1.length);
        assertThat(bytesWritten1).isEqualTo(chunk1.length);

        // Write second chunk
        int bytesWritten2 = buffer.writeBytes(new ByteArrayInputStream(chunk2), chunk2.length);
        assertThat(bytesWritten2).isEqualTo(chunk2.length);

        // Finish writing
        buffer.finishWriting();

        // Verify total size
        assertThat(buffer.getSize()).isEqualTo(chunk1.length + chunk2.length);

        // Load and verify data
        MemorySegment segment = buffer.getMemorySegment();
        byte[] loadedData = new byte[chunk1.length + chunk2.length];
        segment.get(0, loadedData, 0, loadedData.length);

        // Verify both chunks are present
        byte[] expectedData = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        assertThat(loadedData).isEqualTo(expectedData);

        // Cleanup
        buffer.recycleBuffer();
    }

    @Test
    void testCompression() throws IOException {
        File file = tempDir.resolve("buffer_compress.bin").toFile();
        int capacity = 256;
        byte[] testData = new byte[] {1, 2, 3};

        LazyFileBuffer buffer = new LazyFileBuffer(file, capacity);
        buffer.writeBytes(new ByteArrayInputStream(testData), testData.length);
        buffer.finishWriting();

        // Default should be not compressed
        assertThat(buffer.isCompressed()).isFalse();

        // Set compressed
        buffer.setCompressed(true);
        assertThat(buffer.isCompressed()).isTrue();

        // Cleanup
        buffer.recycleBuffer();
    }
}
