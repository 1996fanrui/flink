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
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.consumer.RecoverableInputChannel;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link FetchedChannelStateDrainer}: drain demux, finish ordering, snapshot start
 * position, barrier insertion, and edge cases (drain-finished, channel-not-in-recovery).
 */
class FetchedChannelStateDrainerTest {

    @TempDir Path tempDir;

    @Test
    void testDrainEndToEnd() throws Exception {
        InputChannelInfo cInfo = new InputChannelInfo(0, 0);
        FetchedChannelState state = writeRecords(cInfo, payload(1), payload(2), payload(3));

        RecordingChannel rec = new RecordingChannel(cInfo);
        FetchedChannelStateDrainer drainer = newDrainer(state, cInfo, rec);

        drainer.drain();
        drainer.close();

        // All segment bodies must be delivered as buffer(s); at least 3 non-empty deliveries
        // because the segment body contains 3 records but they may be batched into fewer buffers.
        assertThat(rec.recovered).isNotEmpty();
        assertThat(rec.finishCalls).isEqualTo(1);
    }

    @Test
    void testDrainDemuxByChannelInfo() throws Exception {
        InputChannelInfo c0 = new InputChannelInfo(0, 0);
        InputChannelInfo c1 = new InputChannelInfo(0, 1);

        FetchedChannelState state;
        try (TestSpillWriter writer = new TestSpillWriter(tempDir)) {
            writer.writeRecord(c0, payload(11), payload(11).length);
            writer.writeRecord(c1, payload(22), payload(22).length);
            writer.writeRecord(c0, payload(33), payload(33).length);
            writer.writeRecord(c1, payload(44), payload(44).length);
            state = writer.getChannelState();
        }

        RecordingChannel chan0 = new RecordingChannel(c0);
        RecordingChannel chan1 = new RecordingChannel(c1);
        FetchedChannelStateDrainer drainer = newDrainer(state, c0, chan0, c1, chan1);

        drainer.drain();
        drainer.close();

        // Each channel must receive some data buffers
        assertThat(chan0.recovered).isNotEmpty();
        assertThat(chan1.recovered).isNotEmpty();
        // Both channels must have finish called
        assertThat(chan0.finishCalls).isEqualTo(1);
        assertThat(chan1.finishCalls).isEqualTo(1);
    }

    @Test
    void testDrainCallsFinishAfterAllBufferDeliveries() throws Exception {
        InputChannelInfo c0 = new InputChannelInfo(0, 0);
        InputChannelInfo c1 = new InputChannelInfo(0, 1);

        FetchedChannelState state;
        try (TestSpillWriter writer = new TestSpillWriter(tempDir)) {
            writer.writeRecord(c0, payload(1), payload(1).length);
            writer.writeRecord(c1, payload(2), payload(2).length);
            state = writer.getChannelState();
        }

        int[] seq = {0};
        RecordingChannel chan0 = new RecordingChannel(c0, seq);
        RecordingChannel chan1 = new RecordingChannel(c1, seq);
        FetchedChannelStateDrainer drainer = newDrainer(state, c0, chan0, c1, chan1);

        drainer.drain();
        drainer.close();

        int maxDataSeq = Math.max(chan0.maxDataSeq, chan1.maxDataSeq);
        int minFinishSeq = Math.min(chan0.finishSeq, chan1.finishSeq);
        assertThat(maxDataSeq).isLessThan(minFinishSeq);
    }

    @Test
    void testSnapshotCoversAllSegmentsBeforeDrain() throws Exception {
        InputChannelInfo cInfo = new InputChannelInfo(0, 0);
        FetchedChannelState state = writeRecords(cInfo, payload(5), payload(6));

        RecordingChannel chan = new RecordingChannel(cInfo);
        FetchedChannelStateDrainer drainer = newDrainer(state, cInfo, chan);

        long cpId = 42L;
        FetchedChannelStateReader snap = drainer.snapshotAndInsertBarriers(cpId);

        // Snapshot must cover all segments (at least 1 segment for cInfo).
        // The sequential reader requires each segment body to be fully consumed before advancing,
        // mirroring the real consumer (ChannelStateCheckpointWriter#writeInputFromSpill).
        int count = 0;
        Optional<SpillSegment> next;
        while ((next = snap.nextSegment()).isPresent()) {
            drainBody(next.get().body());
            count++;
        }
        snap.close();
        assertThat(count).isGreaterThan(0);
        drainer.close();
    }

    @Test
    void testSnapshotInsertsBarrierPerChannel() throws Exception {
        InputChannelInfo c0 = new InputChannelInfo(0, 0);
        InputChannelInfo c1 = new InputChannelInfo(0, 1);

        FetchedChannelState state;
        try (TestSpillWriter writer = new TestSpillWriter(tempDir)) {
            writer.writeRecord(c0, payload(1), payload(1).length);
            writer.writeRecord(c1, payload(2), payload(2).length);
            state = writer.getChannelState();
        }

        RecordingChannel chan0 = new RecordingChannel(c0);
        RecordingChannel chan1 = new RecordingChannel(c1);
        FetchedChannelStateDrainer drainer = newDrainer(state, c0, chan0, c1, chan1);

        long cpId = 7L;
        FetchedChannelStateReader snap = drainer.snapshotAndInsertBarriers(cpId);
        snap.close();

        assertThat(chan0.recovered).hasSize(1);
        assertThat(chan1.recovered).hasSize(1);
        assertThat(extractRecoveryBarrierCheckpointId(chan0.recovered.get(0))).isEqualTo(cpId);
        assertThat(extractRecoveryBarrierCheckpointId(chan1.recovered.get(0))).isEqualTo(cpId);
        drainer.close();
    }

    @Test
    void testSnapshotInsertsBarrierWhenChannelInRecoveryEvenIfDiskSliceEmpty() throws Exception {
        InputChannelInfo cInfo = new InputChannelInfo(0, 0);
        FetchedChannelState state = writeRecords(cInfo, payload(1));

        RecordingChannel chan = new RecordingChannel(cInfo);
        FetchedChannelStateDrainer drainer = newDrainer(state, cInfo, chan);

        drainer.drain();
        // Drain finished; simulate the channel still in recovery
        chan.inRecovery = true;
        int recoveredBefore = chan.recovered.size();

        long cpId = 6L;
        FetchedChannelStateReader snap = drainer.snapshotAndInsertBarriers(cpId);
        assertThat(snap.nextSegment()).isEmpty();
        snap.close();

        // Barrier must be inserted even though disk slice is empty
        assertThat(chan.recovered).hasSize(recoveredBefore + 1);
        assertThat(extractRecoveryBarrierCheckpointId(chan.recovered.get(recoveredBefore)))
                .isEqualTo(cpId);
        drainer.close();
    }

    @Test
    void testSnapshotInsertsBarrierOnlyForChannelsStillInRecovery() throws Exception {
        InputChannelInfo c0 = new InputChannelInfo(0, 0);
        InputChannelInfo c1 = new InputChannelInfo(0, 1);

        FetchedChannelState state;
        try (TestSpillWriter writer = new TestSpillWriter(tempDir)) {
            writer.writeRecord(c0, payload(1), payload(1).length);
            state = writer.getChannelState();
        }

        RecordingChannel chan0 = new RecordingChannel(c0);
        RecordingChannel chan1 = new RecordingChannel(c1);
        chan1.inRecovery = false;

        FetchedChannelStateDrainer drainer = newDrainer(state, c0, chan0, c1, chan1);

        long cpId = 11L;
        FetchedChannelStateReader snap = drainer.snapshotAndInsertBarriers(cpId);
        snap.close();

        assertThat(chan0.recovered).hasSize(1);
        assertThat(extractRecoveryBarrierCheckpointId(chan0.recovered.get(0))).isEqualTo(cpId);
        assertThat(chan1.recovered).isEmpty();
        drainer.close();
    }

    @Test
    void testSnapshotReturnsEmptyWhenDrainFinishedAndNotInRecovery() throws Exception {
        InputChannelInfo cInfo = new InputChannelInfo(0, 0);
        FetchedChannelState state = writeRecords(cInfo, payload(1), payload(2));

        RecordingChannel chan = new RecordingChannel(cInfo);
        FetchedChannelStateDrainer drainer = newDrainer(state, cInfo, chan);

        drainer.drain();
        chan.inRecovery = false;
        int recoveredBefore = chan.recovered.size();

        FetchedChannelStateReader snap = drainer.snapshotAndInsertBarriers(99L);
        assertThat(snap.nextSegment()).isEmpty();
        snap.close();

        // No barrier added since channel left recovery
        assertThat(chan.recovered).hasSize(recoveredBefore);
        drainer.close();
    }

    @Test
    void testSnapshotAfterDrainerClosedReturnsEmptyWithoutTouchingClosedRootReader()
            throws Exception {
        // Mirrors production order: drain() then close() (which closes the root reader) run before
        // a
        // late checkpoint fires snapshotAndInsertBarriers. The drain-finished flag must
        // short-circuit
        // so the closed root reader is never snapshotted.
        InputChannelInfo cInfo = new InputChannelInfo(0, 0);
        FetchedChannelState state = writeRecords(cInfo, payload(1), payload(2));

        RecordingChannel chan = new RecordingChannel(cInfo);
        FetchedChannelStateDrainer drainer = newDrainer(state, cInfo, chan);

        drainer.drain();
        drainer.close();

        FetchedChannelStateReader snap = drainer.snapshotAndInsertBarriers(99L);
        assertThat(snap.nextSegment()).isEmpty();
        snap.close();
    }

    // -------------------------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------------------------

    private FetchedChannelState writeRecords(InputChannelInfo ch, byte[]... payloads)
            throws IOException {
        try (TestSpillWriter writer = new TestSpillWriter(tempDir)) {
            for (byte[] p : payloads) {
                writer.writeRecord(ch, p, p.length);
            }
            return writer.getChannelState();
        }
    }

    private FetchedChannelStateDrainer newDrainer(
            FetchedChannelState state, Object... infoChannelPairs) {
        List<RecoverableInputChannel> all = new ArrayList<>();
        for (int i = 0; i < infoChannelPairs.length; i += 2) {
            all.add((RecoverableInputChannel) infoChannelPairs[i + 1]);
        }
        return new FetchedChannelStateDrainer(state, CompletableFuture.completedFuture(all));
    }

    private static long extractRecoveryBarrierCheckpointId(Buffer buffer) throws IOException {
        AbstractEvent event =
                EventSerializer.fromBuffer(
                        buffer, RecoveryCheckpointBarrier.class.getClassLoader());
        buffer.setReaderIndex(0);
        assertThat(event).isInstanceOf(RecoveryCheckpointBarrier.class);
        return ((RecoveryCheckpointBarrier) event).getCheckpointId();
    }

    private static byte[] payload(int id) {
        return new byte[] {(byte) (id & 0xff), (byte) ((id >> 8) & 0xff), (byte) 0xAB, (byte) 0xCD};
    }

    /** Fully consumes a segment body so the sequential reader may advance to the next segment. */
    private static void drainBody(InputStream body) throws IOException {
        byte[] buf = new byte[256];
        while (body.read(buf) != -1) {
            // discard
        }
    }

    // -------------------------------------------------------------------------------------------
    // RecordingChannel stub
    // -------------------------------------------------------------------------------------------

    private static final class RecordingChannel implements RecoverableInputChannel {
        private final InputChannelInfo channelInfo;
        final List<Buffer> recovered = new ArrayList<>();
        int finishCalls = 0;
        private final int[] sequence;
        int maxDataSeq = Integer.MIN_VALUE;
        int finishSeq = -1;
        boolean inRecovery = true;

        RecordingChannel(InputChannelInfo channelInfo) {
            this.channelInfo = channelInfo;
            this.sequence = null;
        }

        RecordingChannel(InputChannelInfo channelInfo, int[] sharedSequence) {
            this.channelInfo = channelInfo;
            this.sequence = sharedSequence;
        }

        @Override
        public InputChannelInfo getChannelInfo() {
            return channelInfo;
        }

        @Override
        public void onRecoveredStateBuffer(Buffer buffer) {
            recovered.add(buffer);
            if (sequence != null) {
                maxDataSeq = Math.max(maxDataSeq, ++sequence[0]);
            }
        }

        @Override
        public void finishRecoveredBufferDelivery() {
            finishCalls++;
            if (sequence != null) {
                finishSeq = ++sequence[0];
            }
        }

        @Override
        public void insertRecoveryCheckpointBarrierIfInRecovery(long checkpointId)
                throws IOException {
            if (inRecovery) {
                recovered.add(
                        EventSerializer.toBuffer(
                                new RecoveryCheckpointBarrier(checkpointId), false));
            }
        }

        @Override
        public Buffer requestRecoveryBufferBlocking() {
            MemorySegment seg = MemorySegmentFactory.allocateUnpooledSegment(4096);
            return new NetworkBuffer(seg, FreeingBufferRecycler.INSTANCE);
        }

        @Override
        public void onRecoveredStateConsumed() {}
    }
}
