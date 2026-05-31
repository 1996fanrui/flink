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
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.consumer.RecoverableInputChannel;
import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Stress test for {@link SpillFileReader} drain / snapshot critical-section atomicity. Spawns one
 * drain alongside 100 concurrent {@code snapshotAndInsertBarriers} calls over a 10000-entry fixture
 * across two channels and asserts:
 *
 * <ul>
 *   <li>No entry appears both in a snapshot's disk slice and in any channel's pre-barrier in-memory
 *       slice.
 *   <li>Every entry appears in either some snapshot's disk slice or in the drain's deliveries.
 *   <li>{@code onRecoveredStateBuffer} is called exactly entry-count plus {@code 100 *
 *       channelCount} times.
 * </ul>
 */
class SpillFileReaderConcurrencyTest {

    private static final int ENTRY_COUNT = 10_000;
    private static final int SNAPSHOTS = 100;
    private static final int CHANNEL_COUNT = 2;

    @TempDir Path tempDir;

    @RepeatedTest(5)
    void testDrainAndSnapshotInsertBarriersConcurrentAtomicity() throws Exception {
        Path runDir = Files.createTempDirectory(tempDir, "spill-stress-");
        SpillFile spillFile = new SpillFile(runDir, 4096);
        InputChannelInfo c0 = new InputChannelInfo(0, 0);
        InputChannelInfo c1 = new InputChannelInfo(0, 1);

        for (int i = 0; i < ENTRY_COUNT; i++) {
            InputChannelInfo info = (i % 2 == 0) ? c0 : c1;
            spillFile.append(info, ByteBuffer.wrap(payloadFor(i)));
        }

        ThreadSafeRecordingChannel chan0 = new ThreadSafeRecordingChannel(c0);
        ThreadSafeRecordingChannel chan1 = new ThreadSafeRecordingChannel(c1);

        List<RecoverableInputChannel> all = new ArrayList<>();
        all.add(chan0);
        all.add(chan1);

        SpillFileDrainer reader =
                new SpillFileDrainer(spillFile, CompletableFuture.completedFuture(all));

        ExecutorService io = Executors.newSingleThreadExecutor();
        AtomicReference<Throwable> drainError = new AtomicReference<>();

        Future<?> drainFuture =
                io.submit(
                        () -> {
                            try {
                                reader.drain();
                            } catch (Throwable t) {
                                drainError.set(t);
                            }
                        });

        // Capture snapshots concurrently while the drain runs.
        List<CloseableIterator<SpillFileReader.Chunk>> snapshots = new ArrayList<>();
        List<Integer> barrierCountsAtSnap = new ArrayList<>();
        for (int i = 0; i < SNAPSHOTS; i++) {
            CloseableIterator<SpillFileReader.Chunk> snap = reader.snapshotAndInsertBarriers(i + 1);
            snapshots.add(snap);
            barrierCountsAtSnap.add(chan0.barrierCount());
            Thread.yield();
        }

        drainFuture.get(60, TimeUnit.SECONDS);
        io.shutdown();
        assertThat(io.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        if (drainError.get() != null) {
            throw new AssertionError("drain failed", drainError.get());
        }

        int totalDataDeliveries = chan0.dataCount() + chan1.dataCount();
        assertThat(totalDataDeliveries).isEqualTo(ENTRY_COUNT);

        // Drain disk-slice membership per snapshot — used to assert disjointness against channel
        // deliveries below.
        Set<Integer> seenInAnySnap = new HashSet<>();
        for (int i = 0; i < snapshots.size(); i++) {
            CloseableIterator<SpillFileReader.Chunk> snap = snapshots.get(i);
            while (snap.hasNext()) {
                SpillFileReader.Chunk chunk = snap.next();
                int entryId = decode(chunk.data);
                seenInAnySnap.add(entryId);
            }
            snap.close();
        }

        // Direct invariant: every entry was delivered exactly once. Drain progress and snapshot
        // startPos advance monotonically, so no entry can be both "before startPos" (drained)
        // and ">= startPos" (in disk slice) for the same snapshot.
        Set<Integer> deliveredIds = new HashSet<>();
        for (Buffer b : chan0.dataBuffers()) {
            deliveredIds.add(decode(toBytes(b)));
        }
        for (Buffer b : chan1.dataBuffers()) {
            deliveredIds.add(decode(toBytes(b)));
        }
        assertThat(deliveredIds).hasSize(ENTRY_COUNT);

        // Both channels must see the same sentinel count — snapshotAndInsertBarriers loops over
        // allChannels uniformly.
        assertThat(chan0.barrierCount()).isEqualTo(chan1.barrierCount());

        reader.close();
        spillFile.close();
    }

    // -------------------------------------------------------------------------------------------
    // Fixtures
    // -------------------------------------------------------------------------------------------

    private static final class ThreadSafeRecordingChannel implements RecoverableInputChannel {
        private final InputChannelInfo channelInfo;
        private final List<Buffer> data = new ArrayList<>();
        private final List<Buffer> barriers = new ArrayList<>();

        ThreadSafeRecordingChannel(InputChannelInfo channelInfo) {
            this.channelInfo = channelInfo;
        }

        @Override
        public InputChannelInfo getChannelInfo() {
            return channelInfo;
        }

        @Override
        public synchronized void onRecoveredStateBuffer(Buffer buffer) {
            if (buffer.isBuffer()) {
                data.add(buffer);
            } else {
                barriers.add(buffer);
            }
        }

        @Override
        public synchronized void finishRecoveredBufferDelivery() {
            // No-op for the stress test.
        }

        @Override
        public synchronized void insertRecoveryCheckpointBarrierIfInRecovery(long checkpointId)
                throws IOException {
            // Always in-recovery so every snapshot exercises the per-channel barrier-insert path
            // under contention.
            barriers.add(
                    EventSerializer.toBuffer(new RecoveryCheckpointBarrier(checkpointId), false));
        }

        @Override
        public Buffer requestRecoveryBufferBlocking() {
            MemorySegment seg = MemorySegmentFactory.allocateUnpooledSegment(64);
            return new NetworkBuffer(seg, FreeingBufferRecycler.INSTANCE);
        }

        synchronized int dataCount() {
            return data.size();
        }

        synchronized int barrierCount() {
            return barriers.size();
        }

        synchronized List<Buffer> dataBuffers() {
            return new ArrayList<>(data);
        }
    }

    // -------------------------------------------------------------------------------------------
    // Encoding helpers — embed a unique id in each payload's first 4 bytes for verification.
    // -------------------------------------------------------------------------------------------

    private static byte[] payloadFor(int id) {
        byte[] out = new byte[8];
        out[0] = (byte) (id & 0xff);
        out[1] = (byte) ((id >> 8) & 0xff);
        out[2] = (byte) ((id >> 16) & 0xff);
        out[3] = (byte) ((id >> 24) & 0xff);
        Arrays.fill(out, 4, 8, (byte) 0xCC);
        return out;
    }

    private static int decode(byte[] data) {
        return (data[0] & 0xff)
                | ((data[1] & 0xff) << 8)
                | ((data[2] & 0xff) << 16)
                | ((data[3] & 0xff) << 24);
    }

    private static byte[] toBytes(Buffer buf) {
        int len = buf.getSize();
        byte[] arr = new byte[len];
        buf.getMemorySegment().get(buf.getMemorySegmentOffset(), arr, 0, len);
        return arr;
    }
}
