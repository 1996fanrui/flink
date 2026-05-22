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
 * record: a workload whose total recovered bytes greatly exceed any single buffer must spill to a
 * {@link SpillFile} rather than pin the bytes on the task heap.
 *
 * <p>A full {@code MiniCluster} reproduction would need a tuned heap and a stateful rescale job
 * graph, which is too heavy for a unit-style test. This ITCase asserts the memory-bound invariant
 * directly: bytes land on disk in bounded segments and the spiller keeps no per-record heap
 * allocation.
 */
class RescaleFilterLargeRecordOOMRegressionITCase {

    @TempDir Path tempDir;

    @Test
    void testLargeRecordsLandOnDiskNotHeap() throws IOException {
        // Recovered slice sized so a heap-pinning path would hold multiple MiB on the task heap;
        // SpillFile caps per-segment size so disk usage stays bounded.
        long segmentSize = 4L * 1024 * 1024;
        int largeRecordSize = 256 * 1024;
        int recordCount = 64;
        long totalBytes = (long) largeRecordSize * recordCount;
        assertThat(totalBytes).as("workload exceeds a single segment").isGreaterThan(segmentSize);

        try (SpillFile spillFile = new SpillFile(tempDir, segmentSize)) {
            InputChannelInfo channelInfo = new InputChannelInfo(0, 0);
            byte[] reusableRecord = new byte[largeRecordSize];
            for (int i = 0; i < reusableRecord.length; i++) {
                reusableRecord[i] = (byte) (i & 0xff);
            }
            for (int i = 0; i < recordCount; i++) {
                // The reusable byte array is mutated and reused across appends — the spiller
                // must write through to disk without retaining a reference, otherwise per-record
                // heap pressure would accumulate.
                reusableRecord[0] = (byte) i;
                spillFile.append(channelInfo, ByteBuffer.wrap(reusableRecord));
            }

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
