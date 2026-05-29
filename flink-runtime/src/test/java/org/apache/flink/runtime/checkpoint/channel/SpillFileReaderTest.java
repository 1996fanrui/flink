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
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link SpillFileReader}. */
class SpillFileReaderTest {

    @TempDir Path tempDir;

    @Test
    void testDrainEndToEnd() throws Exception {
        InputChannelInfo cInfo = new InputChannelInfo(0, 0);
        try (SpillFile spillFile = new SpillFile(tempDir)) {
            spillFile.append(cInfo, ByteBuffer.wrap(payload(1)));
            spillFile.append(cInfo, ByteBuffer.wrap(payload(2)));
            spillFile.append(cInfo, ByteBuffer.wrap(payload(3)));

            RecordingChannel rec = new RecordingChannel(cInfo);
            SpillFileReader reader = newReader(spillFile, cInfo, rec);

            reader.drain();
            reader.close();

            assertThat(rec.recovered).hasSize(3);
            assertThat(toByteArrays(rec.recovered))
                    .containsExactly(payload(1), payload(2), payload(3));
            assertThat(rec.finishCalls).isEqualTo(1);
        }
    }

    @Test
    void testDrainDemuxByChannelInfo() throws Exception {
        InputChannelInfo c0 = new InputChannelInfo(0, 0);
        InputChannelInfo c1 = new InputChannelInfo(0, 1);
        try (SpillFile spillFile = new SpillFile(tempDir)) {
            spillFile.append(c0, ByteBuffer.wrap(payload(11)));
            spillFile.append(c1, ByteBuffer.wrap(payload(22)));
            spillFile.append(c0, ByteBuffer.wrap(payload(33)));
            spillFile.append(c1, ByteBuffer.wrap(payload(44)));

            RecordingChannel chan0 = new RecordingChannel(c0);
            RecordingChannel chan1 = new RecordingChannel(c1);
            SpillFileReader reader = newReader(spillFile, c0, chan0, c1, chan1);

            reader.drain();
            reader.close();

            assertThat(toByteArrays(chan0.recovered)).containsExactly(payload(11), payload(33));
            assertThat(toByteArrays(chan1.recovered)).containsExactly(payload(22), payload(44));
        }
    }

    @Test
    void testDrainCallsFinishReadRecoveredStateAfterAllOnRecoveredStateBuffer() throws Exception {
        InputChannelInfo c0 = new InputChannelInfo(0, 0);
        InputChannelInfo c1 = new InputChannelInfo(0, 1);
        try (SpillFile spillFile = new SpillFile(tempDir)) {
            spillFile.append(c0, ByteBuffer.wrap(payload(1)));
            spillFile.append(c1, ByteBuffer.wrap(payload(2)));

            int[] seq = {0};
            RecordingChannel chan0 = new RecordingChannel(c0, seq);
            RecordingChannel chan1 = new RecordingChannel(c1, seq);
            SpillFileReader reader = newReader(spillFile, c0, chan0, c1, chan1);

            reader.drain();
            reader.close();

            // finish must come strictly after every data delivery (sequence monotonic).
            int maxDataSeq = Math.max(chan0.maxDataSeq, chan1.maxDataSeq);
            int minFinishSeq = Math.min(chan0.finishSeq, chan1.finishSeq);
            assertThat(maxDataSeq).isLessThan(minFinishSeq);
        }
    }

    @Test
    void testSnapshotAndInsertBarriersSnapsStartPos() throws Exception {
        InputChannelInfo cInfo = new InputChannelInfo(0, 0);
        try (SpillFile spillFile = new SpillFile(tempDir)) {
            spillFile.append(cInfo, ByteBuffer.wrap(payload(5)));
            spillFile.append(cInfo, ByteBuffer.wrap(payload(6)));

            RecordingChannel chan = new RecordingChannel(cInfo);
            SpillFileReader reader = newReader(spillFile, cInfo, chan);

            long cpId = 42L;
            DiskSnapshot snap = reader.snapshotAndInsertBarriers(cpId);

            int count = 0;
            while (snap.hasNext()) {
                snap.next();
                count++;
            }
            snap.close();
            // Nothing drained — startPos is (0, 0), the snapshot covers every entry.
            assertThat(count).isEqualTo(2);
        }
    }

    @Test
    void testSnapshotAndInsertBarriersInsertsBarrierPerChannel() throws Exception {
        InputChannelInfo c0 = new InputChannelInfo(0, 0);
        InputChannelInfo c1 = new InputChannelInfo(0, 1);
        try (SpillFile spillFile = new SpillFile(tempDir)) {
            spillFile.append(c0, ByteBuffer.wrap(payload(1)));
            spillFile.append(c1, ByteBuffer.wrap(payload(2)));

            RecordingChannel chan0 = new RecordingChannel(c0);
            RecordingChannel chan1 = new RecordingChannel(c1);
            SpillFileReader reader = newReader(spillFile, c0, chan0, c1, chan1);

            long cpId = 7L;
            DiskSnapshot snap = reader.snapshotAndInsertBarriers(cpId);
            snap.close();

            assertThat(chan0.recovered).hasSize(1);
            assertThat(chan1.recovered).hasSize(1);
            // Barriers are wrapped into Buffers via EventSerializer; deserialize to inspect.
            assertThat(extractRecoveryBarrierCheckpointId(chan0.recovered.get(0))).isEqualTo(cpId);
            assertThat(extractRecoveryBarrierCheckpointId(chan1.recovered.get(0))).isEqualTo(cpId);
        }
    }

    /**
     * When the drain cursor has reached end-of-spill but a channel's queue is still in recovery
     * (allDelivered not flipped, or sentinel still queued), the barrier insert must still happen so
     * {@code collectPreRecoveryBarrier} can find it.
     */
    @Test
    void testSnapshotInsertsBarrierWhenChannelInRecoveryEvenIfDiskSliceEmpty() throws Exception {
        InputChannelInfo cInfo = new InputChannelInfo(0, 0);
        try (SpillFile spillFile = new SpillFile(tempDir)) {
            spillFile.append(cInfo, ByteBuffer.wrap(payload(1)));

            RecordingChannel chan = new RecordingChannel(cInfo);
            SpillFileReader reader = newReader(spillFile, cInfo, chan);

            reader.drain();
            // Drain has advanced past the last entry; simulate the channel queue still reporting
            // in-recovery (allDelivered not yet observed, or sentinel still queued).
            chan.inRecovery = true;
            int recoveredBefore = chan.recovered.size();

            long cpId = 6L;
            DiskSnapshot snap = reader.snapshotAndInsertBarriers(cpId);
            assertThat(snap.hasNext()).isFalse();
            snap.close();

            // Barrier must be appended even though the disk slice is empty.
            assertThat(chan.recovered).hasSize(recoveredBefore + 1);
            assertThat(extractRecoveryBarrierCheckpointId(chan.recovered.get(recoveredBefore)))
                    .isEqualTo(cpId);
            reader.close();
        }
    }

    /**
     * Verifies that barrier insertion is driven by per-channel {@code isInRecovery()}, not the
     * global drain cursor — channels that have exited recovery must not be pulled back in.
     */
    @Test
    void testSnapshotInsertsBarrierOnlyForChannelsStillInRecovery() throws Exception {
        InputChannelInfo c0 = new InputChannelInfo(0, 0);
        InputChannelInfo c1 = new InputChannelInfo(0, 1);
        try (SpillFile spillFile = new SpillFile(tempDir)) {
            spillFile.append(c0, ByteBuffer.wrap(payload(1)));

            RecordingChannel chan0 = new RecordingChannel(c0);
            RecordingChannel chan1 = new RecordingChannel(c1);
            chan1.inRecovery = false;

            SpillFileReader reader = newReader(spillFile, c0, chan0, c1, chan1);

            long cpId = 11L;
            DiskSnapshot snap = reader.snapshotAndInsertBarriers(cpId);
            snap.close();

            assertThat(chan0.recovered).hasSize(1);
            assertThat(extractRecoveryBarrierCheckpointId(chan0.recovered.get(0))).isEqualTo(cpId);
            // chan1 must not receive a barrier — it had already exited recovery.
            assertThat(chan1.recovered).isEmpty();
        }
    }

    private static long extractRecoveryBarrierCheckpointId(Buffer buffer) throws IOException {
        AbstractEvent event =
                EventSerializer.fromBuffer(
                        buffer, RecoveryCheckpointBarrier.class.getClassLoader());
        buffer.setReaderIndex(0);
        assertThat(event).isInstanceOf(RecoveryCheckpointBarrier.class);
        return ((RecoveryCheckpointBarrier) event).getCheckpointId();
    }

    @Test
    void testSnapshotReturnsEmptyDiskSliceWhenCursorPastEnd() throws Exception {
        InputChannelInfo cInfo = new InputChannelInfo(0, 0);
        try (SpillFile spillFile = new SpillFile(tempDir)) {
            spillFile.append(cInfo, ByteBuffer.wrap(payload(1)));
            spillFile.append(cInfo, ByteBuffer.wrap(payload(2)));

            RecordingChannel chan = new RecordingChannel(cInfo);
            SpillFileReader reader = newReader(spillFile, cInfo, chan);

            reader.drain();
            // Consumer fully drained the recovery queue; the channel has exited recovery, so the
            // barrier insert must be skipped.
            chan.inRecovery = false;
            int recoveredBefore = chan.recovered.size();

            DiskSnapshot snap = reader.snapshotAndInsertBarriers(99L);
            assertThat(snap.hasNext()).isFalse();
            snap.close();

            assertThat(chan.recovered).hasSize(recoveredBefore);
            reader.close();
        }
    }

    // -------------------------------------------------------------------------------------------
    // Fixtures
    // -------------------------------------------------------------------------------------------

    /**
     * Stub that records pushed buffers and counts finish calls. With a shared sequence counter, it
     * also lets tests assert finish ordering relative to data delivery.
     */
    private static final class RecordingChannel implements RecoverableInputChannel {
        private final InputChannelInfo channelInfo;
        final List<Buffer> recovered = new ArrayList<>();
        int finishCalls = 0;

        // Sequence-tracking fields (only used when a shared counter is provided).
        private final int[] sequence;
        int maxDataSeq = Integer.MIN_VALUE;
        int finishSeq = -1;

        // Stub has no real consumer modelling queue drainage; tests flip this to simulate the
        // channel having exited recovery.
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
        public boolean isInRecovery() {
            return inRecovery;
        }

        @Override
        public Buffer requestRecoveryBufferBlocking() {
            // Stub channels do not park on a real BufferManager; hand out a fresh heap-backed
            // buffer so the drain can fill and forward it through onRecoveredStateBuffer.
            MemorySegment seg = MemorySegmentFactory.allocateUnpooledSegment(4096);
            return new NetworkBuffer(seg, FreeingBufferRecycler.INSTANCE);
        }
    }

    // -------------------------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------------------------

    private static SpillFileReader newReader(SpillFile spillFile, Object... infoChannelPairs) {
        List<RecoverableInputChannel> all = new ArrayList<>();
        for (int i = 0; i < infoChannelPairs.length; i += 2) {
            // The InputChannelInfo argument is redundant (channels expose getChannelInfo) but
            // kept in the call sites for readability.
            RecoverableInputChannel ch = (RecoverableInputChannel) infoChannelPairs[i + 1];
            all.add(ch);
        }
        return new SpillFileReader(spillFile, CompletableFuture.completedFuture(all));
    }

    /** Deterministic 4-byte payload per id. */
    private static byte[] payload(int id) {
        return new byte[] {(byte) (id & 0xff), (byte) ((id >> 8) & 0xff), (byte) 0xAB, (byte) 0xCD};
    }

    private static List<byte[]> toByteArrays(List<Buffer> bufs) {
        List<byte[]> out = new ArrayList<>();
        for (Buffer buf : bufs) {
            int len = buf.getSize();
            byte[] arr = new byte[len];
            buf.getMemorySegment().get(buf.getMemorySegmentOffset(), arr, 0, len);
            out.add(arr);
        }
        return out;
    }

    @SuppressWarnings("unused")
    private static byte[] flatten(byte[]... parts) {
        int total = 0;
        for (byte[] p : parts) {
            total += p.length;
        }
        byte[] out = new byte[total];
        int off = 0;
        for (byte[] p : parts) {
            System.arraycopy(p, 0, out, off, p.length);
            off += p.length;
        }
        return out;
    }

    @SuppressWarnings("unused")
    private static byte[] copyOf(byte[] src) {
        return Arrays.copyOf(src, src.length);
    }
}
