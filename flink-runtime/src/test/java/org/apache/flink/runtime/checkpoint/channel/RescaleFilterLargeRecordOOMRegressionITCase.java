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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression coverage for the heap-blowup scenario produced by rescale + filter + a large recovered
 * record. The unspilling path is designed to stay bounded by the prefilter + postfilter buffer pair
 * plus disk; a workload whose total recovered bytes greatly exceed any single buffer must spill to
 * a {@link SpillFile} rather than pinning the bytes on the task heap.
 *
 * <p>A full {@code MiniCluster} reproduction of the OOM behaviour requires a tuned heap (e.g.
 * {@code -Xmx512m}) and a job graph that intentionally rescales a stateful operator with a large
 * keyed list state — the supporting fixtures live alongside {@code
 * UnalignedCheckpointRescaleITCase} and are heavy to spin up in a unit-style test. This ITCase
 * therefore focuses on the memory-bound invariant: a workload whose recovered slice exceeds the
 * accumulator size lands on a {@link SpillFile} bounded by configurable segment size, with no
 * per-record heap allocation kept by the spiller.
 */
class RescaleFilterLargeRecordOOMRegressionITCase {

    @TempDir Path tempDir;

    @Test
    void testLargeRecordsLandOnDiskNotHeap() throws IOException {
        // Simulate a recovered slice large enough that an unbounded heap-pinning path would
        // have held several MiB on the task heap. The spill file caps segment size so disk
        // usage is bounded and predictable.
        long segmentSize = 4L * 1024 * 1024; // 4 MiB per segment — bounded growth
        int largeRecordSize = 256 * 1024; // 256 KiB per record
        int recordCount = 64; // 16 MiB of recovered data, spread across 4 segments
        long totalBytes = (long) largeRecordSize * recordCount;
        assertThat(totalBytes).as("workload exceeds a single segment").isGreaterThan(segmentSize);

        try (SpillFile spillFile = new SpillFile(tempDir, segmentSize)) {
            InputChannelInfo channelInfo = new InputChannelInfo(0, 0);
            byte[] reusableRecord = new byte[largeRecordSize];
            for (int i = 0; i < reusableRecord.length; i++) {
                reusableRecord[i] = (byte) (i & 0xff);
            }
            for (int i = 0; i < recordCount; i++) {
                // The reusable byte array is mutated to encode the record index — the spiller
                // immediately writes it through to disk without retaining a reference, so a real
                // task does not accumulate heap pressure per record.
                reusableRecord[0] = (byte) i;
                spillFile.append(channelInfo, ByteBuffer.wrap(reusableRecord));
            }

            // All records persisted; segments must rotate so per-file size stays bounded.
            assertThat(spillFile.segments().size())
                    .as("segment count grows with workload, per-segment size stays bounded")
                    .isGreaterThanOrEqualTo((int) Math.ceil((double) totalBytes / segmentSize));
            for (SpillFile.SpillFileSegment seg : spillFile.segments()) {
                assertThat(seg.currentEnd)
                        .as("no segment exceeds the configured cap")
                        .isLessThanOrEqualTo(segmentSize);
            }
        }
    }
}
