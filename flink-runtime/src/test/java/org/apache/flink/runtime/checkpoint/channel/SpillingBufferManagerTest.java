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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
        // Use a very small file size limit to test rotation.
        // With metadata in memory, each entry on disk is just the raw bytes (no header overhead).
        // With 50-byte data and 100-byte file limit, rotation should occur after ~2 entries.
        try (SpillingBufferManager manager =
                new SpillingBufferManager(
                        new String[] {tempDir.toString()}, "test-attempt", 0, 100)) {

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
    void testDoesNotCleanupFilesFromPreviousAttempts() throws Exception {
        // Create files from a "previous" attempt
        File oldFile = new File(tempDir.toFile(), "channel-state-spill-old-attempt-0-0.tmp");
        oldFile.createNewFile();
        assertThat(oldFile.exists()).isTrue();

        // Create manager with new attempt ID - should NOT clean up old files
        try (SpillingBufferManager manager =
                new SpillingBufferManager(new String[] {tempDir.toString()}, "new-attempt", 0)) {
            assertThat(oldFile.exists()).isTrue();
        }
    }

    @Test
    void testEventBufferTypePreserved() throws Exception {
        try (SpillingBufferManager manager = createManager()) {
            Buffer eventBuf =
                    createBufferWithType(new byte[] {7, 8, 9}, Buffer.DataType.EVENT_BUFFER);
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

            // Create checkpoint iterator - should return CheckpointEntry with channel context
            try (CloseableIterator<SpillingBufferManager.CheckpointEntry> iter =
                    manager.createCheckpointIterator()) {
                assertThat(iter.hasNext()).isTrue();
                SpillingBufferManager.CheckpointEntry entry1 = iter.next();
                assertThat(entry1.oldSubtaskIndex).isEqualTo(0);
                assertThat(entry1.oldChannelIndex).isEqualTo(1);
                assertThat(entry1.dataType).isEqualTo(Buffer.DataType.DATA_BUFFER);
                ByteArrayOutputStream baos1 = new ByteArrayOutputStream();
                entry1.writeTo(baos1);
                assertThat(baos1.toByteArray()).isEqualTo(data1);

                assertThat(iter.hasNext()).isTrue();
                SpillingBufferManager.CheckpointEntry entry2 = iter.next();
                assertThat(entry2.oldSubtaskIndex).isEqualTo(2);
                assertThat(entry2.oldChannelIndex).isEqualTo(3);
                assertThat(entry2.dataType).isEqualTo(Buffer.DataType.DATA_BUFFER);
                ByteArrayOutputStream baos2 = new ByteArrayOutputStream();
                entry2.writeTo(baos2);
                assertThat(baos2.toByteArray()).isEqualTo(data2);

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
            try (CloseableIterator<SpillingBufferManager.CheckpointEntry> iter =
                    manager.createCheckpointIterator()) {
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
            CloseableIterator<SpillingBufferManager.CheckpointEntry> iter =
                    manager.createCheckpointIterator();

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
        }
    }

    @Test
    void testNoDiskDataWhenNoBuffersSpilled() throws Exception {
        SpillingBufferManager manager =
                new SpillingBufferManager(new String[] {tempDir.toString()}, "test-attempt", 0);
        assertThat(manager.hasDiskData()).isFalse();
        manager.close();
    }

    @Test
    void testReplayAllSpilledBuffersInFifoOrder() throws Exception {
        try (SpillingBufferManager manager = createManager()) {
            for (int i = 0; i < 5; i++) {
                Buffer buf = createDataBuffer(new byte[] {(byte) i});
                manager.spillBuffer(buf, 0, 0);
                buf.recycleBuffer();
            }

            assertThat(manager.hasDiskData()).isTrue();

            int replayedCount = 0;
            while (manager.hasDiskData()) {
                Buffer networkBuf = createEmptyBuffer(1024);
                SpillingBufferManager.ReplayResult result = manager.replayToBuffer(networkBuf);
                if (result != null) {
                    assertThat(result.buffer.getMemorySegment().get(0))
                            .isEqualTo((byte) replayedCount);
                    result.buffer.recycleBuffer();
                    replayedCount++;
                } else {
                    networkBuf.recycleBuffer();
                    break;
                }
            }

            assertThat(replayedCount).isEqualTo(5);
            assertThat(manager.hasDiskData()).isFalse();
        }
    }

    @Test
    void testMultipleSpillDirectories() throws Exception {
        Path dir1 = tempDir.resolve("dir1");
        Path dir2 = tempDir.resolve("dir2");
        Files.createDirectories(dir1);
        Files.createDirectories(dir2);

        // Use very small file size to force rotation across directories
        try (SpillingBufferManager manager =
                new SpillingBufferManager(
                        new String[] {dir1.toString(), dir2.toString()}, "test-attempt", 0, 50)) {

            for (int i = 0; i < 6; i++) {
                Buffer buf = createDataBuffer(new byte[30]);
                manager.spillBuffer(buf, 0, i);
                buf.recycleBuffer();
            }

            // Files should be distributed across both directories
            File[] files1 =
                    dir1.toFile().listFiles((d, name) -> name.startsWith("channel-state-spill-"));
            File[] files2 =
                    dir2.toFile().listFiles((d, name) -> name.startsWith("channel-state-spill-"));
            assertThat(files1).isNotNull();
            assertThat(files2).isNotNull();
            assertThat(files1.length).isGreaterThan(0);
            assertThat(files2.length).isGreaterThan(0);
        }
    }

    @Test
    void testOperationsAfterCloseThrow() throws Exception {
        SpillingBufferManager manager = createManager();
        manager.close();

        Buffer buf = createDataBuffer(new byte[] {1});
        assertThatThrownBy(() -> manager.spillBuffer(buf, 0, 0))
                .isInstanceOf(IllegalStateException.class);
        buf.recycleBuffer();

        Buffer netBuf = createEmptyBuffer(1024);
        assertThatThrownBy(() -> manager.replayToBuffer(netBuf))
                .isInstanceOf(IllegalStateException.class);
        netBuf.recycleBuffer();

        assertThatThrownBy(() -> manager.createCheckpointIterator())
                .isInstanceOf(IllegalStateException.class);

        assertThatThrownBy(() -> manager.hasDiskData()).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testHasDiskDataPureQuery() throws Exception {
        try (SpillingBufferManager manager = createManager()) {
            Buffer buf = createDataBuffer(new byte[] {1, 2, 3});
            manager.spillBuffer(buf, 0, 0);
            buf.recycleBuffer();

            assertThat(manager.hasDiskData()).isTrue();

            assertThat(manager.hasDiskData()).isTrue();
            assertThat(manager.hasDiskData()).isTrue();

            Buffer buf2 = createDataBuffer(new byte[] {4, 5});
            manager.spillBuffer(buf2, 0, 0);
            buf2.recycleBuffer();
        }
    }

    @Test
    void testReplayExceptionDoesNotSkipEntry() throws Exception {
        try (SpillingBufferManager manager = createManager()) {
            // Spill a 100-byte buffer
            byte[] data = new byte[100];
            for (int i = 0; i < 100; i++) {
                data[i] = (byte) i;
            }
            Buffer buf = createDataBuffer(data);
            manager.spillBuffer(buf, 1, 2);
            buf.recycleBuffer();

            // Try replay with too-small buffer - MemorySegment.wrap will throw
            Buffer tinyBuf = createEmptyBuffer(10);
            assertThatThrownBy(() -> manager.replayToBuffer(tinyBuf))
                    .isInstanceOf(IOException.class);
            tinyBuf.recycleBuffer();

            // Retry with adequate buffer - should get the SAME entry (not skipped)
            Buffer adequateBuf = createEmptyBuffer(1024);
            SpillingBufferManager.ReplayResult result = manager.replayToBuffer(adequateBuf);
            assertThat(result).isNotNull();
            assertThat(result.oldSubtaskIndex).isEqualTo(1);
            assertThat(result.oldChannelIndex).isEqualTo(2);
            assertThat(result.buffer.readableBytes()).isEqualTo(100);
            result.buffer.recycleBuffer();
        }
    }

    @Test
    void testCheckpointIteratorStartsFromReplayPosition() throws Exception {
        try (SpillingBufferManager manager = createManager()) {
            // Spill 3 buffers
            for (int i = 0; i < 3; i++) {
                Buffer buf = createDataBuffer(new byte[] {(byte) (i + 1)});
                manager.spillBuffer(buf, 0, i);
                buf.recycleBuffer();
            }

            // Replay 1 entry
            Buffer netBuf = createEmptyBuffer(1024);
            SpillingBufferManager.ReplayResult r = manager.replayToBuffer(netBuf);
            assertThat(r).isNotNull();
            assertThat(r.buffer.getMemorySegment().get(0)).isEqualTo((byte) 1);
            r.buffer.recycleBuffer();

            // Create checkpoint iterator - should only return remaining 2 entries
            try (CloseableIterator<SpillingBufferManager.CheckpointEntry> iter =
                    manager.createCheckpointIterator()) {
                assertThat(iter.hasNext()).isTrue();
                SpillingBufferManager.CheckpointEntry e1 = iter.next();
                ByteArrayOutputStream baos1 = new ByteArrayOutputStream();
                e1.writeTo(baos1);
                assertThat(baos1.toByteArray()).isEqualTo(new byte[] {2});

                assertThat(iter.hasNext()).isTrue();
                SpillingBufferManager.CheckpointEntry e2 = iter.next();
                ByteArrayOutputStream baos2 = new ByteArrayOutputStream();
                e2.writeTo(baos2);
                assertThat(baos2.toByteArray()).isEqualTo(new byte[] {3});

                assertThat(iter.hasNext()).isFalse();
            }
        }
    }

    @Test
    void testReplayDeletesFileWithoutIteratorRef() throws Exception {
        try (SpillingBufferManager manager = createManager()) {
            Buffer buf = createDataBuffer(new byte[] {1});
            manager.spillBuffer(buf, 0, 0);
            buf.recycleBuffer();

            // No iterator ref - replay should delete file
            Buffer netBuf = createEmptyBuffer(1024);
            SpillingBufferManager.ReplayResult result = manager.replayToBuffer(netBuf);
            assertThat(result).isNotNull();
            result.buffer.recycleBuffer();

            // Spill files should be deleted (no iterator holding refs)
            File[] spillFilesOnDisk = findSpillFiles();
            assertThat(spillFilesOnDisk.length).isEqualTo(0);
        }
    }

    @Test
    void testCloseForceClosesAliveIterators() throws Exception {
        SpillingBufferManager manager = createManager();
        Buffer buf = createDataBuffer(new byte[] {1, 2, 3});
        manager.spillBuffer(buf, 0, 0);
        buf.recycleBuffer();

        // Create iterator but don't close it
        CloseableIterator<SpillingBufferManager.CheckpointEntry> iter =
                manager.createCheckpointIterator();
        assertThat(iter.hasNext()).isTrue();

        // Close manager - should force close the iterator
        manager.close();

        // Iterator should now be closed (hasNext returns false)
        assertThat(iter.hasNext()).isFalse();

        // All spill files should be deleted
        File[] spillFilesOnDisk = findSpillFiles();
        assertThat(spillFilesOnDisk.length).isEqualTo(0);
    }

    @Test
    void testLargeDataMultiRotationReplay() throws Exception {
        // Use small file size limit to force many rotations
        try (SpillingBufferManager manager =
                new SpillingBufferManager(
                        new String[] {tempDir.toString()}, "test-attempt", 0, 200)) {

            int numBuffers = 50;
            byte[][] allData = new byte[numBuffers][];

            for (int i = 0; i < numBuffers; i++) {
                allData[i] = new byte[] {(byte) i, (byte) (i + 1), (byte) (i + 2)};
                Buffer buf = createDataBuffer(allData[i]);
                manager.spillBuffer(buf, 0, i);
                buf.recycleBuffer();
            }

            // Replay all and verify FIFO order
            for (int i = 0; i < numBuffers; i++) {
                Buffer netBuf = createEmptyBuffer(1024);
                SpillingBufferManager.ReplayResult result = manager.replayToBuffer(netBuf);
                assertThat(result).isNotNull();
                assertThat(result.oldChannelIndex).isEqualTo(i);
                byte[] readData = new byte[3];
                result.buffer.getMemorySegment().get(0, readData, 0, 3);
                assertThat(readData).isEqualTo(allData[i]);
                result.buffer.recycleBuffer();
            }

            assertThat(manager.hasDiskData()).isFalse();

            // All files cleaned up
            File[] spillFilesOnDisk = findSpillFiles();
            assertThat(spillFilesOnDisk.length).isEqualTo(0);
        }
    }

    @Test
    void testAllDataTypesPreserved() throws Exception {
        try (SpillingBufferManager manager = createManager()) {
            Buffer.DataType[] types = Buffer.DataType.values();
            for (int i = 0; i < types.length; i++) {
                if (types[i] == Buffer.DataType.NONE) continue;
                Buffer buf = createBufferWithType(new byte[] {(byte) i}, types[i]);
                manager.spillBuffer(buf, 0, i);
                buf.recycleBuffer();
            }

            for (int i = 0; i < types.length; i++) {
                if (types[i] == Buffer.DataType.NONE) continue;
                Buffer netBuf = createEmptyBuffer(1024);
                SpillingBufferManager.ReplayResult result = manager.replayToBuffer(netBuf);
                assertThat(result).isNotNull();
                assertThat(result.buffer.getDataType()).isEqualTo(types[i]);
                result.buffer.recycleBuffer();
            }
            assertThat(manager.hasDiskData()).isFalse();
        }
    }

    // --- Helper methods ---

    private SpillingBufferManager createManager() {
        return new SpillingBufferManager(new String[] {tempDir.toString()}, "test-attempt", 0);
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

    private static Buffer createBufferWithType(byte[] data, Buffer.DataType dataType) {
        MemorySegment segment = MemorySegmentFactory.allocateUnpooledSegment(data.length);
        segment.put(0, data, 0, data.length);
        return new NetworkBuffer(segment, FreeingBufferRecycler.INSTANCE, dataType, data.length);
    }

    private static Buffer createEmptyBuffer(int capacity) {
        MemorySegment segment = MemorySegmentFactory.allocateUnpooledSegment(capacity);
        return new NetworkBuffer(
                segment, FreeingBufferRecycler.INSTANCE, Buffer.DataType.DATA_BUFFER, 0);
    }
}
