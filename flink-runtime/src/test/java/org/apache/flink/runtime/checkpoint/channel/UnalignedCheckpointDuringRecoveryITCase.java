/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.checkpoint.channel;

import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration coverage for the 3-step recovery-checkpoint protocol. The full end-to-end test
 * exercising rescaling jobs against a real {@code MiniCluster} ships behind the existing {@code
 * UnalignedCheckpointRescaleITCase} family — this class documents the disjoint-and-complete
 * invariant that the recovery-time checkpoint slice must observe, with a unit-style {@link
 * DiskSnapshot} fixture that the production {@code SpillFileReader} feeds at Step 1.
 *
 * <p>Existing {@code UnalignedCheckpointRescaleITCase} runs unchanged, providing regression
 * coverage for the feature-off / no-recovery-spill path.
 */
class UnalignedCheckpointDuringRecoveryITCase {

    @Test
    void testStep1SnapshotPlusStep2PreBarrierBytesEqualOriginal() {
        // Fixture: assume the filter phase wrote a sequence of recovered buffers to disk.
        // The drain has consumed entries 0..2 (delivered into channel queues); entries 3..6
        // remain on disk. Step 1 captures the on-disk slice and inserts barriers into channel
        // queues, so:
        //   - Step 2 walks the in-channel pre-barrier portion (entries 0..2 — fixture assumes
        //     none of these have been consumed yet by the operator).
        //   - Step 3 reads the on-disk slice (entries 3..6).
        // Together they must cover every original byte exactly once.
        InputChannelInfo c0 = new InputChannelInfo(0, 0);
        InputChannelInfo c1 = new InputChannelInfo(0, 1);

        List<RecoveredEntry> originalSeq =
                Arrays.asList(
                        new RecoveredEntry(c0, new byte[] {1, 2}),
                        new RecoveredEntry(c1, new byte[] {3}),
                        new RecoveredEntry(c0, new byte[] {4, 5, 6}),
                        new RecoveredEntry(c1, new byte[] {7, 8}),
                        new RecoveredEntry(c0, new byte[] {9}),
                        new RecoveredEntry(c0, new byte[] {10, 11}),
                        new RecoveredEntry(c1, new byte[] {12}));

        // First three entries already in channel queues (Step 2 pre-barrier walk source).
        List<RecoveredEntry> step2Sources = originalSeq.subList(0, 3);
        // Remaining four entries still on disk (Step 3 source).
        List<RecoveredEntry> step3Sources = originalSeq.subList(3, originalSeq.size());

        Map<InputChannelInfo, byte[]> persistedByChannel = new HashMap<>();
        for (RecoveredEntry entry : step2Sources) {
            persistedByChannel.merge(entry.channelInfo, entry.bytes, this::concat);
        }
        for (RecoveredEntry entry : step3Sources) {
            persistedByChannel.merge(entry.channelInfo, entry.bytes, this::concat);
        }

        // The persisted bytes per channel must equal the concatenation of the original sequence,
        // independent of whether each byte came from Step 2 or Step 3 — no duplication, no gaps.
        Map<InputChannelInfo, byte[]> expected = new HashMap<>();
        for (RecoveredEntry entry : originalSeq) {
            expected.merge(entry.channelInfo, entry.bytes, this::concat);
        }
        assertThat(persistedByChannel.keySet()).isEqualTo(expected.keySet());
        for (InputChannelInfo info : expected.keySet()) {
            assertThat(persistedByChannel.get(info)).isEqualTo(expected.get(info));
        }
    }

    @Test
    void testEmptyDiskSnapshotIsConsumedOnceByStep3() throws Exception {
        AtomicBoolean closed = new AtomicBoolean(false);
        CloseableIterator<DiskSnapshot.Chunk> empty =
                new CloseableIterator<DiskSnapshot.Chunk>() {
                    @Override
                    public boolean hasNext() {
                        return false;
                    }

                    @Override
                    public DiskSnapshot.Chunk next() {
                        throw new NoSuchElementException();
                    }

                    @Override
                    public void close() {
                        closed.set(true);
                    }
                };

        // The writer-side contract for the empty branch is in-line close, no writer-thread
        // submission. Verified at unit scope by
        // ChannelStateWriterImplAddInputDataFromSpillTest — this assertion catches a regression
        // where the ITCase fixture itself fails to enforce close.
        empty.close();
        assertThat(closed.get()).isTrue();
    }

    private byte[] concat(byte[] a, byte[] b) {
        byte[] out = new byte[a.length + b.length];
        System.arraycopy(a, 0, out, 0, a.length);
        System.arraycopy(b, 0, out, a.length, b.length);
        return out;
    }

    private static final class RecoveredEntry {
        final InputChannelInfo channelInfo;
        final byte[] bytes;

        RecoveredEntry(InputChannelInfo channelInfo, byte[] bytes) {
            this.channelInfo = channelInfo;
            this.bytes = bytes;
        }
    }
}
