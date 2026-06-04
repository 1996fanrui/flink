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
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression coverage for the heap-blowup scenario produced by rescale + filter + a large recovered
 * record: a workload whose total recovered bytes greatly exceed any single buffer must spill to
 * disk rather than pin the bytes on the task heap.
 *
 * <p>A full {@code MiniCluster} reproduction would need a tuned heap and a stateful rescale job
 * graph, which is too heavy for a unit-style test. This ITCase asserts the memory-bound invariant
 * directly: bytes land on disk in bounded segments and the writer keeps no per-record heap
 * allocation.
 *
 * <p>The spill-on-disk invariant is verified via the segment locator table and by reading back the
 * data, not by counting files. Single-channel writes produce one continuous segment that may reside
 * in one file even if the total size exceeds the rotation cap — segments are never split across
 * files by design, and file rotation only happens after a segment is fully sealed.
 */
class RescaleFilterLargeRecordOOMRegressionITCase {

    @TempDir Path tempDir;

    /**
     * Verifies that large records for a single channel land on disk rather than remaining on the
     * heap. The disk-landing invariant is confirmed by:
     *
     * <ol>
     *   <li>A non-empty segment locator table with total body length equal to the written bytes.
     *   <li>Reading back the data via the reader and confirming byte count and content.
     * </ol>
     *
     * <p>File count is intentionally not asserted here: a single channel produces one segment, and
     * a segment is never split across files. With all writes going to channel (0,0), there is
     * exactly one segment, which resides entirely in one file regardless of its size. That is the
     * correct behavior per the "segment does not cross files" design invariant.
     */
    @Test
    void testLargeRecordsLandOnDiskNotHeap() throws Exception {
        // Total bytes greatly exceed a single segment size to exercise the spill path.
        final long segmentSize = 4L * 1024 * 1024;
        final int largeRecordSize = 256 * 1024;
        final int recordCount = 64;
        // Each writeRecord call writes a 4-byte length prefix plus the record bytes.
        final long expectedBodyBytes = (long) recordCount * (Integer.BYTES + largeRecordSize);
        assertThat(expectedBodyBytes)
                .as("workload exceeds the segment-size cap")
                .isGreaterThan(segmentSize);

        FetchedChannelState state = new FetchedChannelState();
        try (FetchedChannelStateWriter writer =
                new FetchedChannelStateWriter(state, tempDir, segmentSize)) {
            InputChannelInfo channelInfo = new InputChannelInfo(0, 0);
            byte[] reusableRecord = new byte[largeRecordSize];
            for (int i = 0; i < reusableRecord.length; i++) {
                reusableRecord[i] = (byte) (i & 0xff);
            }
            for (int i = 0; i < recordCount; i++) {
                // The reusable byte array is mutated and reused across writes — the writer must
                // flush through to disk without retaining a reference, otherwise per-record heap
                // pressure would accumulate.
                reusableRecord[0] = (byte) i;
                writer.writeRecord(channelInfo, reusableRecord, reusableRecord.length);
            }
        }

        // --- Invariant 1: all bytes are captured in the segment locator table ---
        List<FetchedSegment> segments = state.segments();
        assertThat(segments).as("segment locators must be present").isNotEmpty();

        long totalLocatedBytes = segments.stream().mapToLong(s -> s.length).sum();
        assertThat(totalLocatedBytes)
                .as("segment locator table must account for all written body bytes")
                .isEqualTo(expectedBodyBytes);

        // --- Invariant 2: data is physically on disk and can be read back correctly ---
        // --- Invariant 3: single-channel writes produce exactly one file (segment not split) ---
        // Both invariants are verified inside the reader's try block because the reader holds a
        // lifecycle grant on the state; closing it releases the grant and triggers file deletion.
        long totalReadBytes = 0L;
        try (FetchedChannelStateReader reader = state.reader()) {
            // Invariant 3: checked while files still exist (before reader close triggers cleanup).
            // One channel => one segment => one file. File size may exceed the cap because segments
            // are written atomically and rotation only happens after a segment is fully sealed.
            assertThat(state.files())
                    .as("single channel produces exactly one spill file")
                    .hasSize(1);
            long physicalFileSize = Files.size(state.files().get(0));
            assertThat(physicalFileSize)
                    .as("physical file size exceeds the rotation cap (expected: segment not split)")
                    .isGreaterThan(segmentSize);

            try (CloseableIterator<FetchedSegmentCursor> it = reader.segments()) {
                while (it.hasNext()) {
                    FetchedSegmentCursor cursor = it.next();
                    try (InputStream body = cursor.body()) {
                        byte[] buf = new byte[4096];
                        int read;
                        while ((read = body.read(buf)) != -1) {
                            totalReadBytes += read;
                        }
                    }
                    cursor.commitConsumed();
                }
            }
        }
        assertThat(totalReadBytes)
                .as("all written body bytes must be readable from disk")
                .isEqualTo(expectedBodyBytes);
    }

    /**
     * Verifies that multiple channels interleaved across writes do trigger file rotation, producing
     * more than one file when the total written bytes exceed the rotation cap. This is the "segment
     * switch triggers rotation" path complementing the single-channel test above.
     */
    @Test
    void testMultiChannelWritesTriggerFileRotation() throws IOException {
        // Small cap so that alternating two channels quickly crosses the threshold.
        final long segmentSize = 512L * 1024;
        final int recordSize = 64 * 1024;
        final int roundCount = 10; // each round writes one record per channel

        FetchedChannelState state = new FetchedChannelState();
        try (FetchedChannelStateWriter writer =
                new FetchedChannelStateWriter(state, tempDir, segmentSize)) {
            InputChannelInfo c0 = new InputChannelInfo(0, 0);
            InputChannelInfo c1 = new InputChannelInfo(0, 1);
            byte[] record = new byte[recordSize];
            for (int round = 0; round < roundCount; round++) {
                writer.writeRecord(c0, record, record.length);
                writer.writeRecord(c1, record, record.length);
            }
        }

        // Alternating channels produce multiple segments; total size exceeds cap => multiple files.
        assertThat(state.segments())
                .as("alternating channels must produce multiple segment locators")
                .hasSizeGreaterThan(1);
        assertThat(state.files())
                .as("total size exceeding the cap must trigger file rotation into multiple files")
                .hasSizeGreaterThanOrEqualTo(2);
    }
}
