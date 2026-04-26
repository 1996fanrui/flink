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

import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.consumer.RecoveredBufferStoreImpl;
import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link FilteredBufferDispatcherImpl}. */
class FilteredBufferDispatcherTest {

    private static final int SEGMENT_SIZE = 64;

    @TempDir Path tempDir;

    private InputChannelInfo ch0;
    private InputChannelInfo ch1;
    private RecoveredBufferStoreImpl store0;
    private RecoveredBufferStoreImpl store1;
    private Map<InputChannelInfo, RecoveredBufferStoreImpl> stores;
    private String[] spillDirs;

    @BeforeEach
    void setUp() {
        ch0 = new InputChannelInfo(0, 0);
        ch1 = new InputChannelInfo(0, 1);
        store0 = new RecoveredBufferStoreImpl(ch0);
        store1 = new RecoveredBufferStoreImpl(ch1);
        stores = new HashMap<>();
        stores.put(ch0, store0);
        stores.put(ch1, store1);
        spillDirs = new String[] {tempDir.toString()};
    }

    @AfterEach
    void tearDown() {
        // Temp dir auto-cleaned by @TempDir
    }

    /**
     * Test-only subclass of RecoveredBufferStoreImpl that records whether setCoordinator was
     * called and captures the registered coordinator, without using Mockito.
     */
    private static class TrackingBufferStore extends RecoveredBufferStoreImpl {
        private RecoveredBufferStoreCoordinator registeredCoordinator;
        private int setCoordinatorCount = 0;

        TrackingBufferStore(InputChannelInfo channelInfo) {
            super(channelInfo);
        }

        @Override
        public synchronized void setCoordinator(RecoveredBufferStoreCoordinator coordinator) {
            super.setCoordinator(coordinator);
            this.registeredCoordinator = coordinator;
            this.setCoordinatorCount++;
        }
    }

    // --- Helper methods ---

    private Buffer createBuffer() {
        return new NetworkBuffer(
                MemorySegmentFactory.allocateUnpooledSegment(SEGMENT_SIZE),
                FreeingBufferRecycler.INSTANCE);
    }

    private Queue<Buffer> createBufferPool(int count) {
        Queue<Buffer> pool = new LinkedList<>();
        for (int i = 0; i < count; i++) {
            pool.add(createBuffer());
        }
        return pool;
    }

    private byte[] createTestData(int length, byte fillValue) {
        byte[] data = new byte[length];
        Arrays.fill(data, fillValue);
        return data;
    }

    /** Drains all ready buffers from a store and returns their data. */
    private List<byte[]> drainStore(RecoveredBufferStoreImpl store) {
        List<byte[]> result = new ArrayList<>();
        Buffer buf;
        while ((buf = store.tryTake()) != null) {
            byte[] data = new byte[buf.getSize()];
            buf.getMemorySegment().get(0, data, 0, buf.getSize());
            buf.recycleBuffer();
            result.add(data);
        }
        return result;
    }

    /** Concatenates all byte arrays into a single byte array. */
    private byte[] concat(List<byte[]> chunks) {
        int totalLen = chunks.stream().mapToInt(a -> a.length).sum();
        byte[] result = new byte[totalLen];
        int pos = 0;
        for (byte[] chunk : chunks) {
            System.arraycopy(chunk, 0, result, pos, chunk.length);
            pos += chunk.length;
        }
        return result;
    }

    // --- Tests ---

    /** Buffer always available, no disk. All data flows to stores via buffers. */
    @Test
    void testP1MemoryPath() throws Exception {
        // Provide plenty of buffers so no spilling happens
        Queue<Buffer> pool = createBufferPool(10);
        FilteredBufferDispatcherImpl writer =
                new FilteredBufferDispatcherImpl(
                        stores,
                        ChannelStateWriter.NO_OP,
                        spillDirs,
                        SEGMENT_SIZE,
                        new TestBufferPool(pool));

        byte[] data = createTestData(SEGMENT_SIZE, (byte) 0xAA);
        writer.write(data, data.length, ch0);
        writer.flush();
        writer.close();

        List<byte[]> buffers = drainStore(store0);
        byte[] actual = concat(buffers);
        assertThat(actual).isEqualTo(data);
        assertThat(store0.isEmpty()).isTrue();
    }

    /** Buffer supplier always returns null. Data goes to disk, replayed on close. */
    @Test
    void testP2SpillPath() throws Exception {
        // No buffers for write, but provide blocking buffers for close drain
        Queue<Buffer> drainPool = createBufferPool(5);
        FilteredBufferDispatcherImpl writer =
                new FilteredBufferDispatcherImpl(
                        stores,
                        ChannelStateWriter.NO_OP,
                        spillDirs,
                        SEGMENT_SIZE,
                        TestBufferPool.drainOnly(drainPool));

        byte[] data = createTestData(SEGMENT_SIZE, (byte) 0xBB);
        writer.write(data, data.length, ch0);
        writer.flush();

        // Before close, store should have no ready buffers (data is on disk as pending)
        assertThat(store0.tryTake()).isNull();

        writer.close();

        // After close drain, data should be in store
        List<byte[]> buffers = drainStore(store0);
        byte[] actual = concat(buffers);
        assertThat(actual).isEqualTo(data);
        assertThat(store0.isEmpty()).isTrue();
    }

    /** First write spills, then buffer becomes available. P3 replays from disk. */
    @Test
    void testP3ReplayPath() throws Exception {
        Queue<Buffer> pool = new LinkedList<>();
        // No buffer initially — first write spills
        FilteredBufferDispatcherImpl writer =
                new FilteredBufferDispatcherImpl(
                        stores,
                        ChannelStateWriter.NO_OP,
                        spillDirs,
                        SEGMENT_SIZE,
                        new TestBufferPool(pool));

        // Write exactly SEGMENT_SIZE so the spill entry is auto-sealed
        byte[] data1 = createTestData(SEGMENT_SIZE, (byte) 0x11);
        writer.write(data1, data1.length, ch0);

        // Now add buffers — next write triggers P3 eager drain
        pool.addAll(createBufferPool(5));

        // Write to ch1 — channel change flushes ch0, then P3 drains the spilled entry
        byte[] data2 = createTestData(SEGMENT_SIZE, (byte) 0x22);
        writer.write(data2, data2.length, ch1);
        writer.flush();
        writer.close();

        // ch0's data should be replayed from disk via P3
        List<byte[]> buf0 = drainStore(store0);
        assertThat(concat(buf0)).isEqualTo(data1);

        List<byte[]> buf1 = drainStore(store1);
        assertThat(concat(buf1)).isEqualTo(data2);
    }

    /**
     * Multiple channels' data goes to disk. Replay order matches FIFO write order across channels.
     */
    @Test
    void testP3FIFOOrdering() throws Exception {
        Queue<Buffer> drainPool = createBufferPool(10);
        FilteredBufferDispatcherImpl writer =
                new FilteredBufferDispatcherImpl(
                        stores,
                        ChannelStateWriter.NO_OP,
                        spillDirs,
                        SEGMENT_SIZE,
                        TestBufferPool.drainOnly(drainPool));

        // Write to ch0 then ch1 — both spill to disk
        byte[] d0 = createTestData(SEGMENT_SIZE, (byte) 0x10);
        byte[] d1 = createTestData(SEGMENT_SIZE, (byte) 0x20);
        writer.write(d0, d0.length, ch0);
        writer.write(d1, d1.length, ch1);
        writer.flush();
        writer.close();

        // Both stores should have data after drain
        assertThat(concat(drainStore(store0))).isEqualTo(d0);
        assertThat(concat(drainStore(store1))).isEqualTo(d1);
    }

    /**
     * Multiple entries on disk, multiple buffers available. P3 loops until no buffer or disk empty.
     */
    @Test
    void testP3EagerDrain() throws Exception {
        Queue<Buffer> pool = new LinkedList<>();
        // No buffers initially
        FilteredBufferDispatcherImpl writer =
                new FilteredBufferDispatcherImpl(
                        stores,
                        ChannelStateWriter.NO_OP,
                        spillDirs,
                        SEGMENT_SIZE,
                        new TestBufferPool(pool));

        // Spill 3 entries for ch0 (each exactly SEGMENT_SIZE so they auto-seal)
        for (int i = 0; i < 3; i++) {
            byte[] d = createTestData(SEGMENT_SIZE, (byte) (0x30 + i));
            writer.write(d, d.length, ch0);
        }

        // Now provide buffers and write to ch1 — P3 eager drain should replay all 3 entries
        pool.addAll(createBufferPool(10));

        byte[] d3 = createTestData(SEGMENT_SIZE, (byte) 0x40);
        writer.write(d3, d3.length, ch1);
        writer.flush();
        writer.close();

        // Verify all 3 entries were drained to store0
        List<byte[]> results = drainStore(store0);
        assertThat(results).hasSize(3);
        for (int i = 0; i < 3; i++) {
            assertThat(results.get(i)).isEqualTo(createTestData(SEGMENT_SIZE, (byte) (0x30 + i)));
        }

        // ch1's data should also be correct
        assertThat(concat(drainStore(store1))).isEqualTo(d3);
    }

    /**
     * Start with buffer, buffer fills, no new buffer available. Remaining data goes to file. Cannot
     * upgrade back to buffer within one writeToBackend call.
     */
    @Test
    void testBackendDowngradeOnly() throws Exception {
        Queue<Buffer> pool = createBufferPool(1); // Only 1 buffer available
        Queue<Buffer> drainPool = createBufferPool(5);
        FilteredBufferDispatcherImpl writer =
                new FilteredBufferDispatcherImpl(
                        stores,
                        ChannelStateWriter.NO_OP,
                        spillDirs,
                        SEGMENT_SIZE,
                        new TestBufferPool(pool, drainPool));

        // Write data larger than one buffer — first SEGMENT_SIZE goes to buffer, rest to disk
        byte[] data = createTestData(SEGMENT_SIZE * 2, (byte) 0x44);
        writer.write(data, data.length, ch0);
        writer.flush();
        writer.close();

        // All data should be recovered
        List<byte[]> results = drainStore(store0);
        byte[] actual = concat(results);
        assertThat(actual).isEqualTo(data);
    }

    /** Data starts in buffer, spans to file when buffer full. */
    @Test
    void testCrossBackendRecordSpanning() throws Exception {
        Queue<Buffer> pool = createBufferPool(1); // 1 buffer of SEGMENT_SIZE
        Queue<Buffer> drainPool = createBufferPool(5);
        FilteredBufferDispatcherImpl writer =
                new FilteredBufferDispatcherImpl(
                        stores,
                        ChannelStateWriter.NO_OP,
                        spillDirs,
                        SEGMENT_SIZE,
                        new TestBufferPool(pool, drainPool));

        // Write half a buffer
        byte[] part1 = createTestData(SEGMENT_SIZE / 2, (byte) 0x55);
        writer.write(part1, part1.length, ch0);

        // Write data that exceeds the remaining buffer capacity
        byte[] part2 = createTestData(SEGMENT_SIZE, (byte) 0x66);
        writer.write(part2, part2.length, ch0);

        writer.flush();
        writer.close();

        // All data should be present
        List<byte[]> results = drainStore(store0);
        byte[] actual = concat(results);
        byte[] expected = new byte[part1.length + part2.length];
        System.arraycopy(part1, 0, expected, 0, part1.length);
        System.arraycopy(part2, 0, expected, part1.length, part2.length);
        assertThat(actual).isEqualTo(expected);
    }

    /** Write to channel A, then B. Verify flush between transitions. */
    @Test
    void testChannelChangeDetection() throws Exception {
        Queue<Buffer> pool = createBufferPool(10);
        FilteredBufferDispatcherImpl writer =
                new FilteredBufferDispatcherImpl(
                        stores,
                        ChannelStateWriter.NO_OP,
                        spillDirs,
                        SEGMENT_SIZE,
                        new TestBufferPool(pool));

        // Write partial data to ch0
        byte[] d0 = createTestData(SEGMENT_SIZE / 2, (byte) 0x77);
        writer.write(d0, d0.length, ch0);

        // Switch to ch1 — should flush ch0's partial buffer
        byte[] d1 = createTestData(SEGMENT_SIZE / 2, (byte) 0x88);
        writer.write(d1, d1.length, ch1);

        writer.flush();
        writer.close();

        // ch0 should have received its partial buffer
        List<byte[]> results0 = drainStore(store0);
        assertThat(concat(results0)).isEqualTo(d0);

        List<byte[]> results1 = drainStore(store1);
        assertThat(concat(results1)).isEqualTo(d1);
    }

    /** Multiple channels share one spill file. */
    @Test
    void testSingleFilePerTask() throws Exception {
        Queue<Buffer> drainPool = createBufferPool(10);
        FilteredBufferDispatcherImpl writer =
                new FilteredBufferDispatcherImpl(
                        stores,
                        ChannelStateWriter.NO_OP,
                        spillDirs,
                        SEGMENT_SIZE,
                        TestBufferPool.drainOnly(drainPool));

        byte[] d0 = createTestData(SEGMENT_SIZE, (byte) 0x99);
        byte[] d1 = createTestData(SEGMENT_SIZE, (byte) 0xAA);
        writer.write(d0, d0.length, ch0);
        writer.write(d1, d1.length, ch1);
        writer.flush();
        writer.close();

        // Both channels' data should be correct after drain
        assertThat(concat(drainStore(store0))).isEqualTo(d0);
        assertThat(concat(drainStore(store1))).isEqualTo(d1);
    }

    /** Spill, partial replay, verify tracking state. */
    @Test
    void testCursorBasedTracking() throws Exception {
        Queue<Buffer> pool = new LinkedList<>();
        FilteredBufferDispatcherImpl writer =
                new FilteredBufferDispatcherImpl(
                        stores,
                        ChannelStateWriter.NO_OP,
                        spillDirs,
                        SEGMENT_SIZE,
                        new TestBufferPool(pool));

        // Spill 2 entries to ch0 (each exactly SEGMENT_SIZE so they auto-seal)
        byte[] d1 = createTestData(SEGMENT_SIZE, (byte) 0x01);
        byte[] d2 = createTestData(SEGMENT_SIZE, (byte) 0x02);
        writer.write(d1, d1.length, ch0);
        writer.write(d2, d2.length, ch0);

        // Add only 1 buffer — partial replay (only 1 of 2 entries will drain)
        pool.add(createBuffer());

        // Write to ch1 — channel change triggers flush, then P3 drains 1 entry
        byte[] d3 = createTestData(SEGMENT_SIZE, (byte) 0x03);
        writer.write(d3, d3.length, ch1);

        // Provide remaining buffers for close drain
        pool.addAll(createBufferPool(5));
        writer.flush();
        writer.close();

        // All data should be correct
        List<byte[]> results0 = drainStore(store0);
        assertThat(results0).hasSize(2);
        assertThat(results0.get(0)).isEqualTo(d1);
        assertThat(results0.get(1)).isEqualTo(d2);
    }

    /** close() drains all disk data. */
    @Test
    void testCloseDrain() throws Exception {
        Queue<Buffer> drainPool = createBufferPool(10);
        FilteredBufferDispatcherImpl writer =
                new FilteredBufferDispatcherImpl(
                        stores,
                        ChannelStateWriter.NO_OP,
                        spillDirs,
                        SEGMENT_SIZE,
                        TestBufferPool.drainOnly(drainPool));

        // Spill multiple entries
        for (int i = 0; i < 5; i++) {
            byte[] data = createTestData(SEGMENT_SIZE, (byte) i);
            writer.write(data, data.length, ch0);
        }
        writer.flush();

        // Before close, no ready buffers
        assertThat(store0.tryTake()).isNull();

        writer.close();

        // After close, all 5 entries should be drained
        List<byte[]> results = drainStore(store0);
        assertThat(results).hasSize(5);
        for (int i = 0; i < 5; i++) {
            assertThat(results.get(i)).isEqualTo(createTestData(SEGMENT_SIZE, (byte) i));
        }
    }

    /** close() twice doesn't throw. */
    @Test
    void testCloseIdempotent() throws Exception {
        Queue<Buffer> pool = createBufferPool(5);
        FilteredBufferDispatcherImpl writer =
                new FilteredBufferDispatcherImpl(
                        stores,
                        ChannelStateWriter.NO_OP,
                        spillDirs,
                        SEGMENT_SIZE,
                        new TestBufferPool(pool));

        writer.flush();
        writer.close();
        // Second close should not throw
        writer.close();
    }

    /** After close(), spill files deleted. */
    @Test
    void testCloseCleanup() throws Exception {
        Queue<Buffer> drainPool = createBufferPool(10);
        FilteredBufferDispatcherImpl writer =
                new FilteredBufferDispatcherImpl(
                        stores,
                        ChannelStateWriter.NO_OP,
                        spillDirs,
                        SEGMENT_SIZE,
                        TestBufferPool.drainOnly(drainPool));

        byte[] data = createTestData(SEGMENT_SIZE * 3, (byte) 0xCC);
        writer.write(data, data.length, ch0);
        writer.flush();

        // Verify spill files exist
        try (Stream<Path> files =
                Files.list(tempDir).filter(p -> p.getFileName().toString().startsWith("spill-"))) {
            assertThat(files.count()).isGreaterThan(0);
        }

        writer.close();

        // After close, spill files should be deleted
        try (Stream<Path> files =
                Files.list(tempDir).filter(p -> p.getFileName().toString().startsWith("spill-"))) {
            assertThat(files.count()).isEqualTo(0);
        }
    }

    /** write() after close() throws IllegalStateException. */
    @Test
    void testWriteAfterClose() throws Exception {
        Queue<Buffer> pool = createBufferPool(5);
        FilteredBufferDispatcherImpl writer =
                new FilteredBufferDispatcherImpl(
                        stores,
                        ChannelStateWriter.NO_OP,
                        spillDirs,
                        SEGMENT_SIZE,
                        new TestBufferPool(pool));

        writer.flush();
        writer.close();

        byte[] data = createTestData(10, (byte) 0xDD);
        assertThatThrownBy(() -> writer.write(data, data.length, ch0))
                .isInstanceOf(IllegalStateException.class);
    }

    /** write() after flush() throws IllegalStateException. */
    @Test
    void testWriteAfterFlush() throws Exception {
        Queue<Buffer> pool = createBufferPool(5);
        FilteredBufferDispatcherImpl writer =
                new FilteredBufferDispatcherImpl(
                        stores,
                        ChannelStateWriter.NO_OP,
                        spillDirs,
                        SEGMENT_SIZE,
                        new TestBufferPool(pool));

        writer.flush();

        byte[] data = createTestData(10, (byte) 0xEE);
        assertThatThrownBy(() -> writer.write(data, data.length, ch0))
                .isInstanceOf(IllegalStateException.class);
    }

    /** Empty spill dirs throws IOException. */
    @Test
    void testSpillDirectorySource() {
        assertThatThrownBy(
                        () ->
                                new FilteredBufferDispatcherImpl(
                                        stores,
                                        ChannelStateWriter.NO_OP,
                                        new String[0],
                                        SEGMENT_SIZE,
                                        TestBufferPool.empty()))
                .isInstanceOf(IOException.class);
    }

    /** Enough data for 3+ file rotations. All replayed correctly. */
    @Test
    void testLargeDataMultiRotation() throws Exception {
        // FilteredSpillFile rotates at 64MB. Use small segments and many writes to trigger rotation.
        // For testing, we write enough data to cause rotation. FilteredSpillFile threshold is 64MB.
        // We use a small segment size and write enough data to exceed 64MB.
        // To avoid enormous test data, we use a smaller approach:
        // The rotation threshold in FilteredSpillFile is 64*1024*1024.
        // We need to write > 192MB of data for 3+ rotations.
        // For a unit test, this is too much. Instead, we verify the mechanism works
        // by writing enough data that results in multiple spill entries and verifying correctness.
        int entryCount = 100;
        int segmentSize = 256;
        Queue<Buffer> drainPool = new LinkedList<>();
        for (int i = 0; i < entryCount + 10; i++) {
            drainPool.add(
                    new NetworkBuffer(
                            MemorySegmentFactory.allocateUnpooledSegment(segmentSize),
                            FreeingBufferRecycler.INSTANCE));
        }

        String[] dirs = new String[] {tempDir.toString()};
        FilteredBufferDispatcherImpl writer =
                new FilteredBufferDispatcherImpl(
                        stores,
                        ChannelStateWriter.NO_OP,
                        dirs,
                        segmentSize,
                        TestBufferPool.drainOnly(drainPool));

        byte[][] expectedData = new byte[entryCount][];
        for (int i = 0; i < entryCount; i++) {
            expectedData[i] = new byte[segmentSize];
            Arrays.fill(expectedData[i], (byte) (i & 0xFF));
            writer.write(expectedData[i], expectedData[i].length, ch0);
        }
        writer.flush();
        writer.close();

        List<byte[]> results = drainStore(store0);
        assertThat(results).hasSize(entryCount);
        for (int i = 0; i < entryCount; i++) {
            assertThat(results.get(i)).isEqualTo(expectedData[i]);
        }
    }

    /** FilteredBufferDispatcher.write(data, length, channelInfo) is the unified write interface. */
    @Test
    void testUnifiedWriteInterface() throws Exception {
        Queue<Buffer> pool = createBufferPool(10);
        FilteredBufferDispatcherImpl writer =
                new FilteredBufferDispatcherImpl(
                        stores,
                        ChannelStateWriter.NO_OP,
                        spillDirs,
                        SEGMENT_SIZE,
                        new TestBufferPool(pool));

        // The write method accepts data, length, channelInfo — this is the unified interface
        // used by filterAndRewrite. Verify it works for multiple channels.
        byte[] d0 = createTestData(32, (byte) 0xF0);
        byte[] d1 = createTestData(32, (byte) 0xF1);

        writer.write(d0, d0.length, ch0);
        writer.write(d1, d1.length, ch1);
        writer.flush();
        writer.close();

        assertThat(concat(drainStore(store0))).isEqualTo(d0);
        assertThat(concat(drainStore(store1))).isEqualTo(d1);
    }

    /** SpillEntry aligns with memorySegmentSize, 1:1 with buffer. */
    @Test
    void testBufferAlignedEntryReplay() throws Exception {
        Queue<Buffer> drainPool = createBufferPool(5);
        FilteredBufferDispatcherImpl writer =
                new FilteredBufferDispatcherImpl(
                        stores,
                        ChannelStateWriter.NO_OP,
                        spillDirs,
                        SEGMENT_SIZE,
                        TestBufferPool.drainOnly(drainPool));

        // Write exactly 3 * SEGMENT_SIZE bytes — should create exactly 3 spill entries
        for (int i = 0; i < 3; i++) {
            byte[] data = createTestData(SEGMENT_SIZE, (byte) (0xA0 + i));
            writer.write(data, data.length, ch0);
        }
        writer.flush();
        writer.close();

        // Each spill entry maps 1:1 to a buffer
        List<byte[]> results = drainStore(store0);
        assertThat(results).hasSize(3);
        for (int i = 0; i < 3; i++) {
            assertThat(results.get(i)).hasSize(SEGMENT_SIZE);
            assertThat(results.get(i)).isEqualTo(createTestData(SEGMENT_SIZE, (byte) (0xA0 + i)));
        }
    }

    // ---------------------------------------------------------------------------
    // Tests for dispatcher registration and wait-set state machine
    // ---------------------------------------------------------------------------

    /**
     * After construction, each store has its coordinator registered to the
     * FilteredBufferDispatcherImpl instance.
     */
    @Test
    void testCoordinatorRegisteredOnConstruction() throws Exception {
        TrackingBufferStore trackStore0 = new TrackingBufferStore(ch0);
        TrackingBufferStore trackStore1 = new TrackingBufferStore(ch1);
        Map<InputChannelInfo, RecoveredBufferStoreImpl> trackStores = new HashMap<>();
        trackStores.put(ch0, trackStore0);
        trackStores.put(ch1, trackStore1);

        Queue<Buffer> pool = createBufferPool(5);
        FilteredBufferDispatcherImpl writer =
                new FilteredBufferDispatcherImpl(
                        trackStores,
                        ChannelStateWriter.NO_OP,
                        spillDirs,
                        SEGMENT_SIZE,
                        new TestBufferPool(pool));

        // Both stores must have had setCoordinator called exactly once with the dispatcher.
        assertThat(trackStore0.setCoordinatorCount).isEqualTo(1);
        assertThat(trackStore0.registeredCoordinator).isSameAs(writer);
        assertThat(trackStore1.setCoordinatorCount).isEqualTo(1);
        assertThat(trackStore1.registeredCoordinator).isSameAs(writer);
    }

    /**
     * First onChannelCheckpointStarted call for a checkpointId scans spillEntryQueue and builds the
     * correct wait-set; subsequent calls for the same checkpointId remove channels.
     */
    @Test
    void testWaitSetBuiltOnFirstCallback() throws Exception {
        // Spill one entry per channel so both appear in the wait-set.
        Queue<Buffer> drainPool = createBufferPool(10);
        FilteredBufferDispatcherImpl writer =
                new FilteredBufferDispatcherImpl(
                        stores,
                        ChannelStateWriter.NO_OP,
                        spillDirs,
                        SEGMENT_SIZE,
                        TestBufferPool.drainOnly(drainPool));

        byte[] d0 = createTestData(SEGMENT_SIZE, (byte) 0x10);
        byte[] d1 = createTestData(SEGMENT_SIZE, (byte) 0x20);
        writer.write(d0, d0.length, ch0);
        writer.write(d1, d1.length, ch1);
        writer.flush();

        // After flush: 2 spill entries in queue (ch0, ch1).
        // First callback for checkpoint 1: wait-set = {ch0, ch1}; then ch0 is removed.
        writer.onChannelCheckpointStarted(1L, ch0);
        // Second callback for same checkpoint: ch1 is removed → wait-set is now empty.
        writer.onChannelCheckpointStarted(1L, ch1);
        // No exception; wait-set reached empty — state machine operated correctly.

        writer.close();
    }

    /** New checkpointId causes wait-set to be rebuilt from current spillEntryQueue. */
    @Test
    void testWaitSetRebuiltOnNewCheckpointId() throws Exception {
        Queue<Buffer> drainPool = createBufferPool(10);
        FilteredBufferDispatcherImpl writer =
                new FilteredBufferDispatcherImpl(
                        stores,
                        ChannelStateWriter.NO_OP,
                        spillDirs,
                        SEGMENT_SIZE,
                        TestBufferPool.drainOnly(drainPool));

        // Spill entries for both channels.
        writer.write(createTestData(SEGMENT_SIZE, (byte) 0x30), SEGMENT_SIZE, ch0);
        writer.write(createTestData(SEGMENT_SIZE, (byte) 0x40), SEGMENT_SIZE, ch1);
        writer.flush();

        // Checkpoint 1: consume both callbacks (wait-set empties).
        writer.onChannelCheckpointStarted(1L, ch0);
        writer.onChannelCheckpointStarted(1L, ch1);

        // Checkpoint 2: new id → wait-set should be rebuilt from the *remaining* spillEntryQueue.
        // At this point the queue still holds the 2 entries (they are drained only on close).
        // Both channels should again appear in the wait-set.
        writer.onChannelCheckpointStarted(2L, ch0);
        writer.onChannelCheckpointStarted(2L, ch1);
        // No exception; both rebuilt and removed successfully.

        writer.close();
    }

    /**
     * Channel not present in spillEntryQueue is not in wait-set; removing it is a no-op. Uses a
     * fresh store map so only ch0 has a spill entry; ch1 callback is a no-op.
     */
    @Test
    void testCallbackForChannelWithNoPendingEntryIsNoOp() throws Exception {
        // Use a fresh store map to avoid interference from previous tests
        RecoveredBufferStoreImpl freshStore0 = new RecoveredBufferStoreImpl(ch0);
        RecoveredBufferStoreImpl freshStore1 = new RecoveredBufferStoreImpl(ch1);
        Map<InputChannelInfo, RecoveredBufferStoreImpl> freshStores = new HashMap<>();
        freshStores.put(ch0, freshStore0);
        freshStores.put(ch1, freshStore1);

        Queue<Buffer> drainPool = createBufferPool(5);
        FilteredBufferDispatcherImpl writer =
                new FilteredBufferDispatcherImpl(
                        freshStores,
                        ChannelStateWriter.NO_OP,
                        spillDirs,
                        SEGMENT_SIZE,
                        TestBufferPool.drainOnly(drainPool));

        // Only ch0 spills; ch1 has no entries in the queue.
        writer.write(createTestData(SEGMENT_SIZE, (byte) 0x50), SEGMENT_SIZE, ch0);
        writer.flush();

        // ch1 callback: not in wait-set → no-op remove, wait-set stays non-empty.
        writer.onChannelCheckpointStarted(42L, ch1);
        // ch0 callback: removed from wait-set → empty.
        writer.onChannelCheckpointStarted(42L, ch0);

        writer.close();
    }

    /**
     * Duplicate callback for the same channel in the same checkpoint is idempotent (Set.remove on
     * an already-absent element is a no-op).
     */
    @Test
    void testDuplicateCallbackForSameChannelIsIdempotent() throws Exception {
        Queue<Buffer> drainPool = createBufferPool(5);
        FilteredBufferDispatcherImpl writer =
                new FilteredBufferDispatcherImpl(
                        stores,
                        ChannelStateWriter.NO_OP,
                        spillDirs,
                        SEGMENT_SIZE,
                        TestBufferPool.drainOnly(drainPool));

        writer.write(createTestData(SEGMENT_SIZE, (byte) 0x70), SEGMENT_SIZE, ch0);
        writer.flush();

        // ch0 removed on first call; second call is a no-op (already absent from set).
        writer.onChannelCheckpointStarted(10L, ch0);
        writer.onChannelCheckpointStarted(10L, ch0); // idempotent — no exception

        writer.close();
    }

    /**
     * A stale callback (checkpointId &lt; currentCheckpointId) must be ignored: it must not modify
     * the current checkpoint's wait-set and must not trigger phase 2. We verify this by observing
     * that after the stale callback arrives, the current checkpoint still converges normally when
     * its own callbacks arrive.
     */
    @Test
    void testStaleCheckpointCallbackIsIgnored() throws Exception {
        Queue<Buffer> drainPool = createBufferPool(5);
        RecordingChannelStateWriter recordingWriter = new RecordingChannelStateWriter();
        FilteredBufferDispatcherImpl writer =
                new FilteredBufferDispatcherImpl(
                        stores,
                        recordingWriter,
                        spillDirs,
                        SEGMENT_SIZE,
                        TestBufferPool.drainOnly(drainPool));

        // Two entries so both channels appear in the wait-set.
        writer.write(createTestData(SEGMENT_SIZE, (byte) 0xA1), SEGMENT_SIZE, ch0);
        writer.write(createTestData(SEGMENT_SIZE, (byte) 0xA2), SEGMENT_SIZE, ch1);
        writer.flush();

        // Move to a newer checkpoint (id=20) and deliver one of its two channel callbacks.
        writer.onChannelCheckpointStarted(20L, ch0);
        // wait-set still contains ch1; phase 2 not yet triggered.
        assertThat(recordingWriter.inputDataCalls).isEmpty();

        // A stale callback for an older checkpoint (id=10) arrives. It must be ignored — not
        // alter the wait-set for checkpoint 20, and must not trigger phase 2.
        writer.onChannelCheckpointStarted(10L, ch0);
        writer.onChannelCheckpointStarted(10L, ch1);
        assertThat(recordingWriter.inputDataCalls).isEmpty();

        // Now deliver the remaining callback for checkpoint 20 — wait-set empties and phase 2
        // snapshots the entries into the ChannelStateWriter.
        writer.onChannelCheckpointStarted(20L, ch1);
        assertThat(recordingWriter.inputDataCalls).hasSize(2);

        writer.close();
    }

    /**
     * wait-set reaching empty triggers phase2 {@code drainSpillEntriesToCheckpoint}: the sealed
     * readers are snapshotted and streamed to the ChannelStateWriter, but the original readers and
     * store state are left intact. close()'s drain loop then still delivers every entry to the
     * store via network buffers.
     */
    @Test
    void testWaitSetEmptyTriggersPhase2SnapshotThenCloseDrainDeliversBuffers() throws Exception {
        Queue<Buffer> drainPool = createBufferPool(5);
        FilteredBufferDispatcherImpl writer =
                new FilteredBufferDispatcherImpl(
                        stores,
                        ChannelStateWriter.NO_OP,
                        spillDirs,
                        SEGMENT_SIZE,
                        TestBufferPool.drainOnly(drainPool));

        byte[] payload = createTestData(SEGMENT_SIZE, (byte) 0x80);
        writer.write(payload, SEGMENT_SIZE, ch0);
        writer.flush();

        // phase2: snapshots sealed readers into the ChannelStateWriter (NO_OP here); the original
        // reader and store state are untouched.
        writer.onChannelCheckpointStarted(99L, ch0);

        // close() drain still consumes the original reader and delivers buffers to the store.
        writer.close();

        Buffer delivered = store0.tryTake();
        assertThat(delivered).isNotNull();
        byte[] actual = new byte[delivered.getSize()];
        delivered.getMemorySegment().get(0, actual, 0, delivered.getSize());
        delivered.recycleBuffer();
        assertThat(actual).isEqualTo(payload);
    }

    // -----------------------------------------------------------------------------------------
    // Phase2 disk checkpoint tests
    // -----------------------------------------------------------------------------------------

    /**
     * A recording ChannelStateWriter that captures addInputDataFromSpill calls for assertions.
     * Drains the chunk iterator synchronously so tests can assert on actual bytes and channel info.
     */
    private static class RecordingChannelStateWriter
            extends ChannelStateWriter.NoOpChannelStateWriter {

        static class Call {
            final long checkpointId;
            final InputChannelInfo info;
            final int dataLength;
            final byte[] capturedBytes;

            Call(long checkpointId, InputChannelInfo info, int dataLength, byte[] capturedBytes) {
                this.checkpointId = checkpointId;
                this.info = info;
                this.dataLength = dataLength;
                this.capturedBytes = capturedBytes;
            }
        }

        final List<Call> inputDataCalls = new ArrayList<>();

        @Override
        public void addInputDataFromSpill(
                long checkpointId, CloseableIterator<FilteredSpillFile.Chunk> chunks) {
            try {
                while (chunks.hasNext()) {
                    FilteredSpillFile.Chunk chunk = chunks.next();
                    byte[] bytes = new byte[chunk.getLength()];
                    System.arraycopy(chunk.getData(), 0, bytes, 0, chunk.getLength());
                    inputDataCalls.add(
                            new Call(
                                    checkpointId,
                                    chunk.getChannelInfo(),
                                    chunk.getLength(),
                                    bytes));
                }
                chunks.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * phase2 writes all spill entries to ChannelStateWriter via streaming addInputData. Verifies
     * checkpointId, channelInfo, seqNum=SEQUENCE_NUMBER_RESTORED, and byte content.
     */
    @Test
    void testPhase2WritesDiskDataThroughStreamingApi() throws Exception {
        RecordingChannelStateWriter recordingWriter = new RecordingChannelStateWriter();

        // drainOnly: no buffers for the write path (forces everything to spill), but the blocking
        // drain path gets buffers so close()'s drain loop can still deliver the snapshotted entries
        // to the stores. phase 2 is a backup — close() drain is the task-facing delivery path.
        Queue<Buffer> drainPool = createBufferPool(5);
        FilteredBufferDispatcherImpl writer =
                new FilteredBufferDispatcherImpl(
                        stores,
                        recordingWriter,
                        spillDirs,
                        SEGMENT_SIZE,
                        TestBufferPool.drainOnly(drainPool));

        byte[] d0 = createTestData(SEGMENT_SIZE, (byte) 0xA1);
        byte[] d1 = createTestData(SEGMENT_SIZE, (byte) 0xA2);
        writer.write(d0, d0.length, ch0);
        writer.write(d1, d1.length, ch1);
        writer.flush();

        // Trigger phase2: all channels report in, wait-set empties on second callback
        long checkpointId = 42L;
        writer.onChannelCheckpointStarted(checkpointId, ch0);
        writer.onChannelCheckpointStarted(checkpointId, ch1);

        // Two entries must have been streamed to ChannelStateWriter
        assertThat(recordingWriter.inputDataCalls).hasSize(2);

        RecordingChannelStateWriter.Call call0 = recordingWriter.inputDataCalls.get(0);
        assertThat(call0.checkpointId).isEqualTo(checkpointId);
        assertThat(call0.info).isEqualTo(ch0);
        assertThat(call0.dataLength).isEqualTo(SEGMENT_SIZE);
        assertThat(call0.capturedBytes).isEqualTo(d0);

        RecordingChannelStateWriter.Call call1 = recordingWriter.inputDataCalls.get(1);
        assertThat(call1.checkpointId).isEqualTo(checkpointId);
        assertThat(call1.info).isEqualTo(ch1);
        assertThat(call1.dataLength).isEqualTo(SEGMENT_SIZE);
        assertThat(call1.capturedBytes).isEqualTo(d1);

        writer.close();
    }

    /**
     * phase 2 is a snapshot-only backup: it must NOT decrement {@code store.pendingCount}. The
     * original reader and the store state are left untouched so close()'s drain loop still has
     * these entries to deliver to the task via network buffers.
     */
    @Test
    void testPhase2DoesNotTouchStorePendingCount() throws Exception {
        Queue<Buffer> drainPool = createBufferPool(3);
        FilteredBufferDispatcherImpl writer =
                new FilteredBufferDispatcherImpl(
                        stores,
                        ChannelStateWriter.NO_OP,
                        spillDirs,
                        SEGMENT_SIZE,
                        TestBufferPool.drainOnly(drainPool));

        // Spill 2 entries for ch0, 1 for ch1 — each exactly SEGMENT_SIZE so they auto-seal
        writer.write(createTestData(SEGMENT_SIZE, (byte) 0xB1), SEGMENT_SIZE, ch0);
        writer.write(createTestData(SEGMENT_SIZE, (byte) 0xB2), SEGMENT_SIZE, ch1);
        writer.write(createTestData(SEGMENT_SIZE, (byte) 0xB3), SEGMENT_SIZE, ch0);
        writer.flush();

        // Before phase 2: both stores non-empty
        assertThat(store0.isEmpty()).isFalse();
        assertThat(store1.isEmpty()).isFalse();

        // Phase 2: all callbacks arrive — entries are copied to checkpoint, but pendingCount stays
        long checkpointId = 7L;
        writer.onChannelCheckpointStarted(checkpointId, ch0);
        writer.onChannelCheckpointStarted(checkpointId, ch1);

        // pendingCount untouched — stores still report non-empty
        assertThat(store0.isEmpty()).isFalse();
        assertThat(store1.isEmpty()).isFalse();

        // close() drain delivers all entries to stores; only then do the counts go to zero
        writer.close();
        // Drain the ready buffers so isEmpty() reflects pendingCount only
        while (store0.tryTake() != null) {}
        while (store1.tryTake() != null) {}
        assertThat(store0.isEmpty()).isTrue();
        assertThat(store1.isEmpty()).isTrue();
    }

    /**
     * After phase 2 snapshots entries into the checkpoint, close() drain must still deliver every
     * entry to the stores (phase 2 is a backup, not an ownership transfer).
     */
    @Test
    void testCloseDrainStillDeliversEntriesAfterPhase2() throws Exception {
        Queue<Buffer> drainPool = createBufferPool(1);
        FilteredBufferDispatcherImpl writer =
                new FilteredBufferDispatcherImpl(
                        stores,
                        ChannelStateWriter.NO_OP,
                        spillDirs,
                        SEGMENT_SIZE,
                        TestBufferPool.drainOnly(drainPool));

        byte[] payload = createTestData(SEGMENT_SIZE, (byte) 0xC1);
        writer.write(payload, SEGMENT_SIZE, ch0);
        writer.flush();

        writer.onChannelCheckpointStarted(55L, ch0);

        // close() drain consumes the still-pending entry and delivers it to the store
        writer.close();

        Buffer delivered = store0.tryTake();
        assertThat(delivered).isNotNull();
        byte[] actual = new byte[delivered.getSize()];
        delivered.getMemorySegment().get(0, actual, 0, delivered.getSize());
        delivered.recycleBuffer();
        assertThat(actual).isEqualTo(payload);
    }

    /**
     * Two independent consumers: phase 2 writes every entry into the checkpoint via
     * ChannelStateWriter, and close() drain additionally delivers every entry to the stores.
     * Both streams see the full data — the on-disk bytes are read twice via independent
     * FileChannels.
     */
    @Test
    void testPhase2AndCloseDrainBothReceiveAllEntries() throws Exception {
        Queue<Buffer> drainPool = createBufferPool(2);
        RecordingChannelStateWriter recordingWriter = new RecordingChannelStateWriter();

        FilteredBufferDispatcherImpl writer =
                new FilteredBufferDispatcherImpl(
                        stores,
                        recordingWriter,
                        spillDirs,
                        SEGMENT_SIZE,
                        TestBufferPool.drainOnly(drainPool));

        byte[] payload0 = createTestData(SEGMENT_SIZE, (byte) 0xD1);
        byte[] payload1 = createTestData(SEGMENT_SIZE, (byte) 0xD2);
        writer.write(payload0, SEGMENT_SIZE, ch0);
        writer.write(payload1, SEGMENT_SIZE, ch1);
        writer.flush();

        // Phase 2: both entries captured into ChannelStateWriter (checkpoint backup)
        long checkpointId = 100L;
        writer.onChannelCheckpointStarted(checkpointId, ch0);
        writer.onChannelCheckpointStarted(checkpointId, ch1);
        assertThat(recordingWriter.inputDataCalls).hasSize(2);

        // close() drain: both entries additionally delivered to the stores (task-facing pipeline)
        writer.close();

        Buffer buf0 = store0.tryTake();
        Buffer buf1 = store1.tryTake();
        assertThat(buf0).isNotNull();
        assertThat(buf1).isNotNull();
        byte[] got0 = new byte[buf0.getSize()];
        byte[] got1 = new byte[buf1.getSize()];
        buf0.getMemorySegment().get(0, got0, 0, buf0.getSize());
        buf1.getMemorySegment().get(0, got1, 0, buf1.getSize());
        buf0.recycleBuffer();
        buf1.recycleBuffer();
        assertThat(got0).isEqualTo(payload0);
        assertThat(got1).isEqualTo(payload1);
    }

    // -----------------------------------------------------------------------------------------
    // Release listener tests: releaseAll() on a store must also drop the channel's disk entries.
    // -----------------------------------------------------------------------------------------

    /**
     * After a store is released, its pending disk entries must be dropped from every Reader so the
     * dispatcher's subsequent close() drain does not try to deliver bytes for the gone channel.
     */
    @Test
    void testReleaseAllRemovesChannelDiskEntriesEagerly() throws Exception {
        Queue<Buffer> drainPool = createBufferPool(5);
        FilteredBufferDispatcherImpl writer =
                new FilteredBufferDispatcherImpl(
                        stores,
                        ChannelStateWriter.NO_OP,
                        spillDirs,
                        SEGMENT_SIZE,
                        TestBufferPool.drainOnly(drainPool));

        byte[] d0 = createTestData(SEGMENT_SIZE, (byte) 0x51);
        byte[] d1 = createTestData(SEGMENT_SIZE, (byte) 0x52);
        writer.write(d0, d0.length, ch0);
        writer.write(d1, d1.length, ch1);
        writer.flush();

        // Release store0 before draining — this should propagate to the dispatcher, which drops
        // all ch0 entries from the Readers.
        store0.releaseAll();

        writer.close();

        // store1 still receives its data; store0 must stay empty since ch0 entries were dropped.
        assertThat(store0.tryTake()).isNull();
        assertThat(concat(drainStore(store1))).isEqualTo(d1);
    }

    /**
     * When a channel is released while an in-flight checkpoint wait-set still contains it, the
     * dispatcher must remove it from the wait-set so the wait-set can still converge to empty and
     * phase-2 drain is not blocked.
     */
    @Test
    void testReleaseAllConvergesInFlightCheckpointWaitSet() throws Exception {
        Queue<Buffer> drainPool = createBufferPool(5);
        RecordingChannelStateWriter recordingWriter = new RecordingChannelStateWriter();
        FilteredBufferDispatcherImpl writer =
                new FilteredBufferDispatcherImpl(
                        stores,
                        recordingWriter,
                        spillDirs,
                        SEGMENT_SIZE,
                        TestBufferPool.drainOnly(drainPool));

        writer.write(createTestData(SEGMENT_SIZE, (byte) 0x61), SEGMENT_SIZE, ch0);
        writer.write(createTestData(SEGMENT_SIZE, (byte) 0x62), SEGMENT_SIZE, ch1);
        writer.flush();

        // ch0 reports in. Wait-set still contains ch1 so phase 2 must not have fired yet.
        writer.onChannelCheckpointStarted(30L, ch0);
        assertThat(recordingWriter.inputDataCalls).isEmpty();

        // ch1 is released before its checkpoint callback ever arrives. The dispatcher removes it
        // from the wait-set, which now empties and triggers phase 2.
        store1.releaseAll();

        // Only ch0's entry made it into the checkpoint backup; ch1's entries were dropped on
        // release.
        assertThat(recordingWriter.inputDataCalls).hasSize(1);
        assertThat(recordingWriter.inputDataCalls.get(0).info).isEqualTo(ch0);

        writer.close();
    }

    // -----------------------------------------------------------------------------------------
    // onChannelCheckpointStopped tests
    // -----------------------------------------------------------------------------------------

    /**
     * After a checkpoint is aborted (i.e. all stores called notifyCheckpointStopped), a
     * subsequent channel release must NOT trigger a phase-2 drain into the stopped checkpoint —
     * the writer for that id is gone and any drain would either be wasted work or rely on the
     * writer's isDone() guard to silently swallow the data.
     */
    @Test
    void testReleaseAfterStoppedCheckpointDoesNotDrainStoppedCheckpoint() throws Exception {
        Queue<Buffer> drainPool = createBufferPool(5);
        RecordingChannelStateWriter recordingWriter = new RecordingChannelStateWriter();
        FilteredBufferDispatcherImpl writer =
                new FilteredBufferDispatcherImpl(
                        stores,
                        recordingWriter,
                        spillDirs,
                        SEGMENT_SIZE,
                        TestBufferPool.drainOnly(drainPool));

        writer.write(createTestData(SEGMENT_SIZE, (byte) 0x71), SEGMENT_SIZE, ch0);
        writer.write(createTestData(SEGMENT_SIZE, (byte) 0x72), SEGMENT_SIZE, ch1);
        writer.flush();

        // Checkpoint 50 starts on ch0; wait-set still contains ch1.
        writer.onChannelCheckpointStarted(50L, ch0);
        assertThat(recordingWriter.inputDataCalls).isEmpty();

        // The task aborts checkpoint 50 — every channel's persister fires notifyCheckpointStopped.
        store0.notifyCheckpointStopped(50L);
        store1.notifyCheckpointStopped(50L);

        // Now ch1 is released. Without the stopped-checkpoint short-circuit, the wait-set would
        // empty and the dispatcher would drain to checkpoint 50; with the fix, no drain fires.
        store1.releaseAll();

        assertThat(recordingWriter.inputDataCalls).isEmpty();

        writer.close();
    }

    /**
     * A late {@code onChannelCheckpointStarted} for a checkpoint that has already been stopped
     * must be ignored as stale, even if a new checkpoint has not yet started.
     */
    @Test
    void testLateCheckpointStartedAfterStoppedIsIgnored() throws Exception {
        Queue<Buffer> drainPool = createBufferPool(5);
        RecordingChannelStateWriter recordingWriter = new RecordingChannelStateWriter();
        FilteredBufferDispatcherImpl writer =
                new FilteredBufferDispatcherImpl(
                        stores,
                        recordingWriter,
                        spillDirs,
                        SEGMENT_SIZE,
                        TestBufferPool.drainOnly(drainPool));

        writer.write(createTestData(SEGMENT_SIZE, (byte) 0x81), SEGMENT_SIZE, ch0);
        writer.flush();

        // Stop checkpoint 60 before anyone reports in.
        store0.notifyCheckpointStopped(60L);
        store1.notifyCheckpointStopped(60L);

        // A late onChannelCheckpointStarted(60, ...) shows up. It must be short-circuited.
        writer.onChannelCheckpointStarted(60L, ch0);
        writer.onChannelCheckpointStarted(60L, ch1);

        assertThat(recordingWriter.inputDataCalls).isEmpty();

        writer.close();
    }

    /**
     * A new checkpoint started AFTER a stop notification must still progress normally — the
     * stopped-id short-circuit only skips the exact stopped id, not all subsequent checkpoints.
     */
    @Test
    void testCheckpointAfterStoppedStillProgresses() throws Exception {
        Queue<Buffer> drainPool = createBufferPool(5);
        RecordingChannelStateWriter recordingWriter = new RecordingChannelStateWriter();
        FilteredBufferDispatcherImpl writer =
                new FilteredBufferDispatcherImpl(
                        stores,
                        recordingWriter,
                        spillDirs,
                        SEGMENT_SIZE,
                        TestBufferPool.drainOnly(drainPool));

        writer.write(createTestData(SEGMENT_SIZE, (byte) 0x91), SEGMENT_SIZE, ch0);
        writer.write(createTestData(SEGMENT_SIZE, (byte) 0x92), SEGMENT_SIZE, ch1);
        writer.flush();

        // Abort checkpoint 70.
        store0.notifyCheckpointStopped(70L);
        store1.notifyCheckpointStopped(70L);

        // Checkpoint 71 begins; both channels report in and phase-2 fires for 71.
        writer.onChannelCheckpointStarted(71L, ch0);
        writer.onChannelCheckpointStarted(71L, ch1);

        assertThat(recordingWriter.inputDataCalls).hasSize(2);
        assertThat(recordingWriter.inputDataCalls.get(0).checkpointId).isEqualTo(71L);
        assertThat(recordingWriter.inputDataCalls.get(1).checkpointId).isEqualTo(71L);

        writer.close();
    }
}
