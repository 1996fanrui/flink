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
import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link SpillingBufferManager}. */
class SpillingBufferManagerTest {

    @TempDir Path tempDir;

    @Test
    void testSpillAndHasDiskData() throws Exception {
        try (SpillingBufferManager manager = createManager()) {
            assertThat(manager.hasDiskData()).isFalse();

            Buffer buffer = createDataBuffer(new byte[] {1, 2, 3});
            manager.spillBuffer(buffer, 0, 0);
            buffer.recycleBuffer();

            assertThat(manager.hasDiskData()).isTrue();
        }
    }

    @Test
    void testSpillAndReplay() throws Exception {
        try (SpillingBufferManager manager = createManager()) {
            // Spill a buffer
            byte[] data = {10, 20, 30, 40, 50};
            Buffer spillBuf = createDataBuffer(data);
            manager.spillBuffer(spillBuf, 1, 2);
            spillBuf.recycleBuffer();

            assertThat(manager.hasDiskData()).isTrue();

            // Replay into a network buffer
            Buffer networkBuffer = createEmptyBuffer(1024);
            SpillingBufferManager.ReplayResult result = manager.replayToBuffer(networkBuffer);

            assertThat(result).isNotNull();
            assertThat(result.oldSubtaskIndex).isEqualTo(1);
            assertThat(result.oldChannelIndex).isEqualTo(2);
            assertThat(result.buffer).isSameAs(networkBuffer);
            assertThat(result.buffer.readableBytes()).isEqualTo(5);
            assertThat(result.buffer.getDataType()).isEqualTo(Buffer.DataType.DATA_BUFFER);

            byte[] readData = new byte[5];
            result.buffer.getMemorySegment().get(0, readData, 0, 5);
            assertThat(readData).isEqualTo(data);
            result.buffer.recycleBuffer();

            // No more disk data
            assertThat(manager.hasDiskData()).isFalse();
        }
    }

    @Test
    void testMultipleSpillAndReplayInOrder() throws Exception {
        try (SpillingBufferManager manager = createManager()) {
            // Spill three buffers
            Buffer buf1 = createDataBuffer(new byte[] {1});
            Buffer buf2 = createDataBuffer(new byte[] {2});
            Buffer buf3 = createDataBuffer(new byte[] {3});

            manager.spillBuffer(buf1, 0, 0);
            manager.spillBuffer(buf2, 1, 1);
            manager.spillBuffer(buf3, 2, 2);
            buf1.recycleBuffer();
            buf2.recycleBuffer();
            buf3.recycleBuffer();

            // Replay in FIFO order
            Buffer net1 = createEmptyBuffer(1024);
            SpillingBufferManager.ReplayResult r1 = manager.replayToBuffer(net1);
            assertThat(r1).isNotNull();
            assertThat(r1.oldSubtaskIndex).isEqualTo(0);
            assertThat(r1.buffer.getMemorySegment().get(0)).isEqualTo((byte) 1);
            r1.buffer.recycleBuffer();

            Buffer net2 = createEmptyBuffer(1024);
            SpillingBufferManager.ReplayResult r2 = manager.replayToBuffer(net2);
            assertThat(r2).isNotNull();
            assertThat(r2.oldSubtaskIndex).isEqualTo(1);
            assertThat(r2.buffer.getMemorySegment().get(0)).isEqualTo((byte) 2);
            r2.buffer.recycleBuffer();

            Buffer net3 = createEmptyBuffer(1024);
            SpillingBufferManager.ReplayResult r3 = manager.replayToBuffer(net3);
            assertThat(r3).isNotNull();
            assertThat(r3.oldSubtaskIndex).isEqualTo(2);
            assertThat(r3.buffer.getMemorySegment().get(0)).isEqualTo((byte) 3);
            r3.buffer.recycleBuffer();

            // No more
            assertThat(manager.hasDiskData()).isFalse();
        }
    }

    @Test
    void testReplayReturnsNullWhenNoDiskData() throws Exception {
        try (SpillingBufferManager manager = createManager()) {
            Buffer networkBuffer = createEmptyBuffer(1024);
            SpillingBufferManager.ReplayResult result = manager.replayToBuffer(networkBuffer);
            assertThat(result).isNull();
            networkBuffer.recycleBuffer();
        }
    }

    @Test
    void testCloseIdempotency() throws Exception {
        SpillingBufferManager manager = createManager();

        Buffer buffer = createDataBuffer(new byte[] {1, 2, 3});
        manager.spillBuffer(buffer, 0, 0);
        buffer.recycleBuffer();

        // Close twice should not throw
        manager.close();
        manager.close();
    }

    @Test
    void testCloseDeletesSpillFiles() throws Exception {
        SpillingBufferManager manager = createManager();

        Buffer buffer = createDataBuffer(new byte[] {1, 2, 3});
        manager.spillBuffer(buffer, 0, 0);
        buffer.recycleBuffer();

        // Verify spill files exist
        File[] spillFiles = findSpillFiles();
        assertThat(spillFiles.length).isGreaterThan(0);

        manager.close();

        // Verify spill files are deleted
        spillFiles = findSpillFiles();
        assertThat(spillFiles.length).isEqualTo(0);
    }

    @Test
    void testSpillFileRotationOnSizeLimit() throws Exception {
        // Use a very small file size limit to test rotation
        try (SpillingBufferManager manager =
                new SpillingBufferManager(tempDir.toString(), "test-attempt", 0, 100)) {

            // Write buffers until rotation occurs. Each entry overhead is 13 bytes (4+4+4+1)
            // plus data. With a 100-byte limit, after ~2 entries of 50 bytes we should rotate.
            byte[] largeData = new byte[50];
            for (int i = 0; i < 4; i++) {
                Buffer buf = createDataBuffer(largeData);
                manager.spillBuffer(buf, 0, i);
                buf.recycleBuffer();
            }

            // Should have created multiple spill files
            File[] spillFiles = findSpillFiles();
            assertThat(spillFiles.length).isGreaterThan(1);
        }
    }

    @Test
    void testOldAttemptFileCleanup() throws Exception {
        // Create files from a "previous" attempt
        File oldFile = new File(tempDir.toFile(), "channel-state-spill-old-attempt-0-0.tmp");
        oldFile.createNewFile();
        assertThat(oldFile.exists()).isTrue();

        // Create manager with new attempt ID - should clean up old files
        try (SpillingBufferManager manager =
                new SpillingBufferManager(tempDir.toString(), "new-attempt", 0)) {
            assertThat(oldFile.exists()).isFalse();
        }
    }

    @Test
    void testEventBufferTypePreserved() throws Exception {
        try (SpillingBufferManager manager = createManager()) {
            Buffer eventBuf = createEventBuffer(new byte[] {7, 8, 9});
            manager.spillBuffer(eventBuf, 0, 0);
            eventBuf.recycleBuffer();

            Buffer networkBuffer = createEmptyBuffer(1024);
            SpillingBufferManager.ReplayResult result = manager.replayToBuffer(networkBuffer);
            assertThat(result).isNotNull();
            assertThat(result.buffer.getDataType()).isEqualTo(Buffer.DataType.EVENT_BUFFER);
            result.buffer.recycleBuffer();
        }
    }

    @Test
    void testCheckpointWithDiskData() throws Exception {
        try (SpillingBufferManager manager = createManager()) {
            // Spill two buffers with different channel contexts
            byte[] data1 = {10, 20, 30};
            byte[] data2 = {40, 50};
            Buffer buf1 = createDataBuffer(data1);
            Buffer buf2 = createDataBuffer(data2);
            manager.spillBuffer(buf1, 0, 1);
            manager.spillBuffer(buf2, 2, 3);
            buf1.recycleBuffer();
            buf2.recycleBuffer();

            // Create checkpoint iterator - should return both buffers
            try (CloseableIterator<Buffer> iter = manager.createCheckpointIterator()) {
                assertThat(iter.hasNext()).isTrue();
                Buffer ckBuf1 = iter.next();
                assertThat(ckBuf1.readableBytes()).isEqualTo(3);
                byte[] read1 = new byte[3];
                ckBuf1.getMemorySegment()
                        .get(ckBuf1.getMemorySegmentOffset(), read1, 0, read1.length);
                assertThat(read1).isEqualTo(data1);
                ckBuf1.recycleBuffer();

                assertThat(iter.hasNext()).isTrue();
                Buffer ckBuf2 = iter.next();
                assertThat(ckBuf2.readableBytes()).isEqualTo(2);
                byte[] read2 = new byte[2];
                ckBuf2.getMemorySegment()
                        .get(ckBuf2.getMemorySegmentOffset(), read2, 0, read2.length);
                assertThat(read2).isEqualTo(data2);
                ckBuf2.recycleBuffer();

                assertThat(iter.hasNext()).isFalse();
            }
        }
    }

    @Test
    void testCheckpointAfterFullReplay() throws Exception {
        try (SpillingBufferManager manager = createManager()) {
            // Spill and replay all data
            Buffer buf = createDataBuffer(new byte[] {1, 2, 3});
            manager.spillBuffer(buf, 0, 0);
            buf.recycleBuffer();

            Buffer networkBuf = createEmptyBuffer(1024);
            SpillingBufferManager.ReplayResult result = manager.replayToBuffer(networkBuf);
            assertThat(result).isNotNull();
            result.buffer.recycleBuffer();

            assertThat(manager.hasDiskData()).isFalse();

            // Checkpoint iterator should be empty since all data was replayed
            try (CloseableIterator<Buffer> iter = manager.createCheckpointIterator()) {
                assertThat(iter.hasNext()).isFalse();
            }
        }
    }

    @Test
    void testCheckpointIteratorRefCounting() throws Exception {
        try (SpillingBufferManager manager = createManager()) {
            // Spill data
            Buffer buf = createDataBuffer(new byte[] {1, 2, 3});
            manager.spillBuffer(buf, 0, 0);
            buf.recycleBuffer();

            // Create checkpoint iterator (holds ref count on spill files)
            CloseableIterator<Buffer> iter = manager.createCheckpointIterator();

            // Replay all data - file should NOT be deleted because iterator holds a ref
            Buffer networkBuf = createEmptyBuffer(1024);
            SpillingBufferManager.ReplayResult result = manager.replayToBuffer(networkBuf);
            assertThat(result).isNotNull();
            result.buffer.recycleBuffer();

            // Spill files should still exist on disk (ref held by iterator)
            File[] spillFilesOnDisk = findSpillFiles();
            assertThat(spillFilesOnDisk.length).isGreaterThan(0);

            // Close iterator (releases ref counts)
            iter.close();

            // Now the checkpoint iterator's data was consumed by close, but the file
            // should still exist until next replay attempt deletes it or manager closes.
            // The key point: the file was NOT deleted during replay because of the ref count.
        }
    }

    // --- Helper methods ---

    private SpillingBufferManager createManager() {
        return new SpillingBufferManager(tempDir.toString(), "test-attempt", 0);
    }

    private File[] findSpillFiles() {
        File dir = tempDir.toFile();
        File[] files =
                dir.listFiles(
                        (d, name) ->
                                name.startsWith("channel-state-spill-") && name.endsWith(".tmp"));
        return files != null ? files : new File[0];
    }

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

    private static Buffer createEmptyBuffer(int capacity) {
        MemorySegment segment = MemorySegmentFactory.allocateUnpooledSegment(capacity);
        return new NetworkBuffer(
                segment, FreeingBufferRecycler.INSTANCE, Buffer.DataType.DATA_BUFFER, 0);
    }
}
