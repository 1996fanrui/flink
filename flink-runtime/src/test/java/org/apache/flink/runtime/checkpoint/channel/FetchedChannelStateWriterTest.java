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

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link FetchedChannelStateWriter}: direct OutputStream writes, segment locator table
 * construction, channel switching, and file rotation.
 */
class FetchedChannelStateWriterTest {

    @TempDir Path tempDir;

    // -------------------------------------------------------------------------------------------
    // writeRecord tests
    // -------------------------------------------------------------------------------------------

    @Test
    void testSingleRecordProducesOneSegmentLocator() throws Exception {
        FetchedChannelState state = newState();
        try (FetchedChannelStateWriter writer = newWriter(state)) {
            InputChannelInfo ch = new InputChannelInfo(0, 0);
            byte[] record = bytes(1, 2, 3, 4);
            writer.writeRecord(ch, record, record.length);
        }

        assertThat(state.segments()).hasSize(1);
        FetchedSegment seg = state.segments().get(0);
        assertThat(seg.channelInfo).isEqualTo(new InputChannelInfo(0, 0));
        assertThat(seg.fileIndex).isEqualTo(0);
        // Segment body = 4B recordLength + 4B record = 8B
        assertThat(seg.length).isEqualTo(Integer.BYTES + 4);
    }

    @Test
    void testChannelSwitchProducesTwoSegmentLocators() throws Exception {
        FetchedChannelState state = newState();
        InputChannelInfo c0 = new InputChannelInfo(0, 0);
        InputChannelInfo c1 = new InputChannelInfo(0, 1);
        byte[] rec0 = bytes(0xAA, 0xBB);
        byte[] rec1 = bytes(0xCC, 0xDD, 0xEE);

        try (FetchedChannelStateWriter writer = newWriter(state)) {
            writer.writeRecord(c0, rec0, rec0.length);
            writer.writeRecord(c1, rec1, rec1.length);
        }

        assertThat(state.segments()).hasSize(2);
        assertThat(state.segments().get(0).channelInfo).isEqualTo(c0);
        assertThat(state.segments().get(1).channelInfo).isEqualTo(c1);
    }

    @Test
    void testSameChannelContinuousWritesProduceOneSegment() throws Exception {
        FetchedChannelState state = newState();
        InputChannelInfo ch = new InputChannelInfo(0, 0);

        try (FetchedChannelStateWriter writer = newWriter(state)) {
            writer.writeRecord(ch, bytes(1), 1);
            writer.writeRecord(ch, bytes(2, 3), 2);
            writer.writeRecord(ch, bytes(4, 5, 6), 3);
        }

        // Three writes on the same channel = 1 segment with all bytes aggregated.
        assertThat(state.segments()).hasSize(1);
        long expectedBodyLen = 3 * Integer.BYTES + (1 + 2 + 3); // 3 length fields + data
        assertThat(state.segments().get(0).length).isEqualTo(expectedBodyLen);
    }

    @Test
    void testWrittenBytesMatchSegmentLocatorLength() throws Exception {
        FetchedChannelState state = newState();
        InputChannelInfo ch = new InputChannelInfo(0, 0);
        byte[] record = bytes(10, 20, 30);

        try (FetchedChannelStateWriter writer = newWriter(state)) {
            writer.writeRecord(ch, record, record.length);
        }

        FetchedSegment seg = state.segments().get(0);
        assertThat(state.files()).hasSize(1);

        // Body bytes on disk = 4B length prefix + 3B record
        long fileSize = state.files().get(0).toFile().length();
        // File contains: 8B header (gateIdx + channelIdx) + 4B length + 3B data = 15B
        assertThat(fileSize).isEqualTo(2 * Integer.BYTES + Integer.BYTES + 3);
        // Segment body starts after the 8B header.
        assertThat(seg.offset).isEqualTo(2 * Integer.BYTES);
        assertThat(seg.length).isEqualTo(Integer.BYTES + 3);
    }

    // -------------------------------------------------------------------------------------------
    // writePassThrough tests
    // -------------------------------------------------------------------------------------------

    @Test
    void testPassThroughWriteProducesSegmentLocator() throws Exception {
        FetchedChannelState state = newState();
        InputChannelInfo ch = new InputChannelInfo(1, 2);
        byte[] data = bytes(0x01, 0x02, 0x03, 0x04, 0x05);

        try (FetchedChannelStateWriter writer = newWriter(state)) {
            writer.writePassThrough(ch, data, 0, data.length);
        }

        assertThat(state.segments()).hasSize(1);
        assertThat(state.segments().get(0).channelInfo).isEqualTo(ch);
        assertThat(state.segments().get(0).length).isEqualTo(data.length);
    }

    @Test
    void testPassThroughSameChannelMergesIntoOneSegment() throws Exception {
        FetchedChannelState state = newState();
        InputChannelInfo ch = new InputChannelInfo(0, 0);
        byte[] data1 = bytes(1, 2, 3);
        byte[] data2 = bytes(4, 5, 6, 7);

        try (FetchedChannelStateWriter writer = newWriter(state)) {
            writer.writePassThrough(ch, data1, 0, data1.length);
            writer.writePassThrough(ch, data2, 0, data2.length);
        }

        assertThat(state.segments()).hasSize(1);
        assertThat(state.segments().get(0).length).isEqualTo(data1.length + data2.length);
    }

    // -------------------------------------------------------------------------------------------
    // File rotation tests
    // -------------------------------------------------------------------------------------------

    @Test
    void testFileRotationHappensOnlyAfterSegmentSeal() throws Exception {
        // Set the file size limit just below the size of one segment.
        // A segment with 10 bytes of data = 8B header + 4B length + 10B record = 22B body.
        // We set the limit to 1 so every seal triggers rotation, but rotation must not split
        // a segment in progress.
        FetchedChannelState state = newState();
        InputChannelInfo c0 = new InputChannelInfo(0, 0);
        InputChannelInfo c1 = new InputChannelInfo(0, 1);
        byte[] data = bytes(1, 2, 3);

        // maxFileSizeBytes = 1 ensures rotation after every sealed segment.
        try (FetchedChannelStateWriter writer = new FetchedChannelStateWriter(state, tempDir, 1L)) {
            writer.writeRecord(c0, data, data.length);
            writer.writeRecord(c1, data, data.length);
        }

        // Two channel segments -> two seals -> first seal triggers rotation -> two files.
        assertThat(state.files()).hasSize(2);
        // Each segment is fully contained in its own file.
        assertThat(state.segments()).hasSize(2);
        assertThat(state.segments().get(0).fileIndex).isEqualTo(0);
        assertThat(state.segments().get(1).fileIndex).isEqualTo(1);
    }

    @Test
    void testSegmentNeverSpansFiles() throws Exception {
        // Force rotation after every segment to verify the hard constraint.
        FetchedChannelState state = newState();
        InputChannelInfo c0 = new InputChannelInfo(0, 0);
        InputChannelInfo c1 = new InputChannelInfo(0, 1);
        InputChannelInfo c2 = new InputChannelInfo(0, 2);

        try (FetchedChannelStateWriter writer = new FetchedChannelStateWriter(state, tempDir, 1L)) {
            writer.writeRecord(c0, bytes(1), 1);
            writer.writeRecord(c1, bytes(2, 3), 2);
            writer.writeRecord(c2, bytes(4), 1);
        }

        List<FetchedSegment> segments = state.segments();
        assertThat(segments).hasSize(3);
        // Each segment must reference only the file it started in.
        for (FetchedSegment seg : segments) {
            long fileSize = state.files().get(seg.fileIndex).toFile().length();
            assertThat(seg.offset + seg.length).isLessThanOrEqualTo(fileSize);
        }
    }

    @Test
    void testSingleLargeSegmentStaysInOneFile() throws Exception {
        // If a segment itself is larger than the file size limit, it must still stay in one file.
        FetchedChannelState state = newState();
        InputChannelInfo ch = new InputChannelInfo(0, 0);
        // maxFileSizeBytes = 1, but the single segment is ~20B; it still fits in one file.
        try (FetchedChannelStateWriter writer = new FetchedChannelStateWriter(state, tempDir, 1L)) {
            writer.writeRecord(ch, bytes(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 10);
        }

        assertThat(state.files()).hasSize(1);
        assertThat(state.segments()).hasSize(1);
        assertThat(state.segments().get(0).fileIndex).isEqualTo(0);
    }

    // -------------------------------------------------------------------------------------------
    // Close behaviour
    // -------------------------------------------------------------------------------------------

    @Test
    void testCloseWithNoWriteProducesNoFilesOrSegments() throws Exception {
        FetchedChannelState state = newState();
        try (FetchedChannelStateWriter writer = newWriter(state)) {
            // no writes
        }
        assertThat(state.files()).isEmpty();
        assertThat(state.segments()).isEmpty();
    }

    @Test
    void testCloseIsIdempotent() throws Exception {
        FetchedChannelState state = newState();
        FetchedChannelStateWriter writer = newWriter(state);
        writer.close();
        writer.close();
        // No exception expected.
    }

    // -------------------------------------------------------------------------------------------
    // Disk-format verification
    // -------------------------------------------------------------------------------------------

    @Test
    void testDiskFormatMatchesSpec() throws Exception {
        // Verify that the on-disk bytes match the documented segment-header + length-prefixed
        // format, so the reader can reconstruct all data correctly.
        FetchedChannelState state = newState();
        InputChannelInfo ch = new InputChannelInfo(7, 3);
        byte[] record = bytes(0xAB, 0xCD, 0xEF);

        try (FetchedChannelStateWriter writer = newWriter(state)) {
            writer.writeRecord(ch, record, record.length);
        }

        assertThat(state.files()).hasSize(1);
        Path file = state.files().get(0);
        try (DataInputStream in = new DataInputStream(new FileInputStream(file.toFile()))) {
            // Segment header: gateIdx + channelIdx
            assertThat(in.readInt()).isEqualTo(7);
            assertThat(in.readInt()).isEqualTo(3);
            // Record: length prefix + data
            assertThat(in.readInt()).isEqualTo(3);
            assertThat(in.read()).isEqualTo(0xAB);
            assertThat(in.read()).isEqualTo(0xCD);
            assertThat(in.read()).isEqualTo(0xEF);
            assertThat(in.read()).isEqualTo(-1); // EOF
        }
    }

    @Test
    void testSegmentOffsetPointsAfterHeader() throws Exception {
        FetchedChannelState state = newState();
        InputChannelInfo ch = new InputChannelInfo(0, 0);
        byte[] record = bytes(0xAA);

        try (FetchedChannelStateWriter writer = newWriter(state)) {
            writer.writeRecord(ch, record, record.length);
        }

        FetchedSegment seg = state.segments().get(0);
        // The 8-byte segment header precedes the body; body offset should be 8.
        assertThat(seg.offset).isEqualTo(2 * Integer.BYTES);
    }

    // -------------------------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------------------------

    private FetchedChannelState newState() {
        FetchedChannelState state = new FetchedChannelState();
        // Caller owns one lifecycle grant so that transient reader opens inside tests don't
        // drop the refcount to zero and delete files prematurely.
        state.acquire();
        return state;
    }

    private FetchedChannelStateWriter newWriter(FetchedChannelState state) {
        return new FetchedChannelStateWriter(state, tempDir);
    }

    private static byte[] bytes(int... values) {
        byte[] out = new byte[values.length];
        for (int i = 0; i < values.length; i++) {
            out[i] = (byte) values[i];
        }
        return out;
    }
}
