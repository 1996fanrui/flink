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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link DiskSnapshot}. */
class DiskSnapshotTest {

    @TempDir Path tempDir;

    @Test
    void testSkipsPreDrained() throws IOException {
        try (SpillFile spillFile = newSpillFileWithEntries(10)) {
            SpillFile.Snapshot snap = spillFile.snapshot();
            // startPos at segment 0, offset just past the 4th entry — the first 4 must be skipped.
            long endOf4th = sumLengthsUpTo(snap, 4);
            DiskSnapshot iter =
                    newDiskSnapshot(spillFile, snap, new DiskSnapshot.StartPos(0, endOf4th));

            List<Integer> visitedIndexes = new ArrayList<>();
            int cursor = 4;
            while (iter.hasNext()) {
                DiskSnapshot.Chunk chunk = iter.next();
                assertThat(chunk.channelInfo).isEqualTo(snap.getEntries().get(cursor).channelInfo);
                visitedIndexes.add(cursor);
                cursor++;
            }
            iter.close();

            assertThat(visitedIndexes).containsExactly(4, 5, 6, 7, 8, 9);
        }
    }

    @Test
    void testChunkDataMatchesDisk() throws IOException {
        try (SpillFile spillFile = newSpillFileWithEntries(5)) {
            SpillFile.Snapshot snap = spillFile.snapshot();
            DiskSnapshot iter = newDiskSnapshot(spillFile, snap, new DiskSnapshot.StartPos(0, 0L));

            int cursor = 0;
            while (iter.hasNext()) {
                DiskSnapshot.Chunk chunk = iter.next();
                SpillFile.Entry expected = snap.getEntries().get(cursor++);
                assertThat(chunk.length).isEqualTo(expected.length);
                byte[] direct =
                        spillFile.readBytes(
                                expected.segmentIndex, expected.offset, expected.length);
                assertThat(chunk.data).isEqualTo(direct);
            }
            iter.close();
        }
    }

    @Test
    void testCloseStopsIteration() throws IOException {
        try (SpillFile spillFile = newSpillFileWithEntries(3)) {
            SpillFile.Snapshot snap = spillFile.snapshot();
            DiskSnapshot iter = newDiskSnapshot(spillFile, snap, new DiskSnapshot.StartPos(0, 0L));

            assertThat(iter.hasNext()).isTrue();
            iter.close();
            assertThat(iter.hasNext()).isFalse();
        }
    }

    /**
     * Mirrors production's {@link SpillFile#acquire()} pairing so the ref-count invariant holds.
     */
    private static DiskSnapshot newDiskSnapshot(
            SpillFile spillFile, SpillFile.Snapshot snap, DiskSnapshot.StartPos startPos) {
        spillFile.acquire();
        return new DiskSnapshot(snap, startPos, spillFile);
    }

    private SpillFile newSpillFileWithEntries(int count) throws IOException {
        SpillFile spillFile = new SpillFile(tempDir);
        for (int i = 0; i < count; i++) {
            byte[] payload = new byte[i + 1];
            for (int j = 0; j < payload.length; j++) {
                payload[j] = (byte) ((i * 31 + j) & 0xff);
            }
            spillFile.append(new InputChannelInfo(0, i % 2), ByteBuffer.wrap(payload));
        }
        return spillFile;
    }

    /** Sum the lengths of the first {@code n} entries' byte ranges within their segments. */
    private static long sumLengthsUpTo(SpillFile.Snapshot snap, int n) {
        // All entries belong to segment 0 in this fixture (small payloads, default segment size).
        long sum = 0;
        for (int i = 0; i < n; i++) {
            sum += snap.getEntries().get(i).length;
        }
        return sum;
    }
}
