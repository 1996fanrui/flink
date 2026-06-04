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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link FetchedChannelStateWriter}: direct OutputStream writes, 12-byte header format,
 * channel switching, file rotation, and end-to-end roundtrip with the reader.
 */
class FetchedChannelStateWriterTest {

    @TempDir Path tempDir;

    // -------------------------------------------------------------------------------------------
    // writeRecord: basic segment creation
    // -------------------------------------------------------------------------------------------

    @Test
    void testSingleRecordProducesOneFile() throws Exception {
        FetchedChannelState state = newState();
        try (FetchedChannelStateWriter writer = newWriter(state)) {
            InputChannelInfo ch = new InputChannelInfo(0, 0);
            byte[] record = bytes(1, 2, 3, 4);
            writer.writeRecord(ch, record, record.length);
        }

        assertThat(state.files()).hasSize(1);
    }

    @Test
    void testChannelSwitchProducesTwoSegmentsInFile() throws Exception {
        FetchedChannelState state = newState();
        InputChannelInfo c0 = new InputChannelInfo(0, 0);
        InputChannelInfo c1 = new InputChannelInfo(0, 1);
        byte[] rec0 = bytes(0xAA, 0xBB);
        byte[] rec1 = bytes(0xCC, 0xDD, 0xEE);

        try (FetchedChannelStateWriter writer = newWriter(state)) {
            writer.writeRecord(c0, rec0, rec0.length);
            writer.writeRecord(c1, rec1, rec1.length);
        }

        // Two channel segments -> two 12-byte headers in the file.
        // Segment 0 body: 4B len + 2B data = 6B; Segment 1 body: 4B len + 3B data = 7B
        // File size = (12 + 6) + (12 + 7) = 37B
        assertThat(state.files()).hasSize(1);
        long fileSize = state.files().get(0).toFile().length();
        int seg0Body = Integer.BYTES + 2;
        int seg1Body = Integer.BYTES + 3;
        assertThat(fileSize)
                .isEqualTo(
                        (long) (FetchedChannelStateWriter.SEGMENT_HEADER_BYTES + seg0Body)
                                + (FetchedChannelStateWriter.SEGMENT_HEADER_BYTES + seg1Body));
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

        // One segment: header 12B + (4+1) + (4+2) + (4+3) body = 12 + 18 = 30B
        long expectedBodyLen = 3 * Integer.BYTES + (1 + 2 + 3);
        long expectedFileSize = FetchedChannelStateWriter.SEGMENT_HEADER_BYTES + expectedBodyLen;
        assertThat(state.files()).hasSize(1);
        assertThat(state.files().get(0).toFile().length()).isEqualTo(expectedFileSize);
    }

    // -------------------------------------------------------------------------------------------
    // writePassThrough tests
    // -------------------------------------------------------------------------------------------

    @Test
    void testPassThroughWriteProducesSegment() throws Exception {
        FetchedChannelState state = newState();
        InputChannelInfo ch = new InputChannelInfo(1, 2);
        byte[] data = bytes(0x01, 0x02, 0x03, 0x04, 0x05);

        try (FetchedChannelStateWriter writer = newWriter(state)) {
            writer.writePassThrough(ch, data, 0, data.length);
        }

        assertThat(state.files()).hasSize(1);
        long expectedFileSize = FetchedChannelStateWriter.SEGMENT_HEADER_BYTES + data.length;
        assertThat(state.files().get(0).toFile().length()).isEqualTo(expectedFileSize);
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

        // One segment with both payloads merged
        long expectedFileSize =
                FetchedChannelStateWriter.SEGMENT_HEADER_BYTES + data1.length + data2.length;
        assertThat(state.files()).hasSize(1);
        assertThat(state.files().get(0).toFile().length()).isEqualTo(expectedFileSize);
    }

    // -------------------------------------------------------------------------------------------
    // File rotation tests
    // -------------------------------------------------------------------------------------------

    @Test
    void testFileRotationHappensOnlyAfterSegmentSeal() throws Exception {
        FetchedChannelState state = newState();
        InputChannelInfo c0 = new InputChannelInfo(0, 0);
        InputChannelInfo c1 = new InputChannelInfo(0, 1);
        byte[] data = bytes(1, 2, 3);

        // maxFileSizeBytes = 1 ensures rotation after every sealed segment.
        try (FetchedChannelStateWriter writer = new FetchedChannelStateWriter(state, tempDir, 1L)) {
            writer.writeRecord(c0, data, data.length);
            writer.writeRecord(c1, data, data.length);
        }

        // Two channel segments -> two seals -> two files (rotation after first seal).
        assertThat(state.files()).hasSize(2);
    }

    @Test
    void testSegmentNeverSpansFiles() throws Exception {
        FetchedChannelState state = newState();
        InputChannelInfo c0 = new InputChannelInfo(0, 0);
        InputChannelInfo c1 = new InputChannelInfo(0, 1);
        InputChannelInfo c2 = new InputChannelInfo(0, 2);

        try (FetchedChannelStateWriter writer = new FetchedChannelStateWriter(state, tempDir, 1L)) {
            writer.writeRecord(c0, bytes(1), 1);
            writer.writeRecord(c1, bytes(2, 3), 2);
            writer.writeRecord(c2, bytes(4), 1);
        }

        // Each segment must be entirely within one file.
        assertThat(state.files()).hasSize(3);
        for (Path file : state.files()) {
            assertThat(file.toFile().length()).isGreaterThan(0);
        }
    }

    @Test
    void testSingleLargeSegmentStaysInOneFile() throws Exception {
        FetchedChannelState state = newState();
        InputChannelInfo ch = new InputChannelInfo(0, 0);
        // maxFileSizeBytes = 1, but the single segment is much larger; it still fits in one file.
        try (FetchedChannelStateWriter writer = new FetchedChannelStateWriter(state, tempDir, 1L)) {
            writer.writeRecord(ch, bytes(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 10);
        }

        assertThat(state.files()).hasSize(1);
    }

    // -------------------------------------------------------------------------------------------
    // Close behaviour
    // -------------------------------------------------------------------------------------------

    @Test
    void testCloseWithNoWriteProducesNoFiles() throws Exception {
        FetchedChannelState state = newState();
        try (FetchedChannelStateWriter writer = newWriter(state)) {
            // no writes
        }
        assertThat(state.files()).isEmpty();
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
    // Disk-format verification: 12-byte header with bufferLength
    // -------------------------------------------------------------------------------------------

    @Test
    void testDiskFormatMatchesSpec() throws Exception {
        // Verify that the on-disk bytes match the documented 12-byte segment-header + body format.
        FetchedChannelState state = newState();
        InputChannelInfo ch = new InputChannelInfo(7, 3);
        byte[] record = bytes(0xAB, 0xCD, 0xEF);

        try (FetchedChannelStateWriter writer = newWriter(state)) {
            writer.writeRecord(ch, record, record.length);
        }

        assertThat(state.files()).hasSize(1);
        Path file = state.files().get(0);
        try (DataInputStream in = new DataInputStream(new FileInputStream(file.toFile()))) {
            // Segment header: gateIdx + channelIdx + bufferLength
            assertThat(in.readInt()).isEqualTo(7); // gateIdx
            assertThat(in.readInt()).isEqualTo(3); // channelIdx
            int bufferLength = in.readInt(); // bufferLength = 4B length + 3B data = 7
            assertThat(bufferLength).isEqualTo(Integer.BYTES + 3);
            // Segment body: length prefix + data
            assertThat(in.readInt()).isEqualTo(3); // record length
            assertThat(in.read()).isEqualTo(0xAB);
            assertThat(in.read()).isEqualTo(0xCD);
            assertThat(in.read()).isEqualTo(0xEF);
            assertThat(in.read()).isEqualTo(-1); // EOF
        }
    }

    @Test
    void testBufferLengthMatchesActualBodyBytes() throws Exception {
        // bufferLength in header must equal the body bytes written for that segment.
        FetchedChannelState state = newState();
        InputChannelInfo c0 = new InputChannelInfo(0, 0);
        InputChannelInfo c1 = new InputChannelInfo(0, 1);

        try (FetchedChannelStateWriter writer = newWriter(state)) {
            writer.writeRecord(c0, bytes(10, 20, 30), 3); // body = 4 + 3 = 7B
            writer.writeRecord(c1, bytes(40, 50), 2); // body = 4 + 2 = 6B
        }

        Path file = state.files().get(0);
        try (DataInputStream in = new DataInputStream(new FileInputStream(file.toFile()))) {
            in.readInt(); // gateIdx c0
            in.readInt(); // channelIdx c0
            int bl0 = in.readInt();
            assertThat(bl0).isEqualTo(Integer.BYTES + 3); // 7B
            in.skipBytes(bl0); // skip body of segment 0

            in.readInt(); // gateIdx c1
            in.readInt(); // channelIdx c1
            int bl1 = in.readInt();
            assertThat(bl1).isEqualTo(Integer.BYTES + 2); // 6B
            in.skipBytes(bl1);
            assertThat(in.read()).isEqualTo(-1); // EOF
        }
    }

    @Test
    void testSameChannelMultipleRecordsBufferLengthCoversAll() throws Exception {
        // Multiple writeRecord calls on the same channel must merge into one segment.
        // bufferLength must equal the sum of all record [length+data] bytes.
        FetchedChannelState state = newState();
        InputChannelInfo ch = new InputChannelInfo(0, 0);

        try (FetchedChannelStateWriter writer = newWriter(state)) {
            writer.writeRecord(ch, bytes(1, 2), 2); // 4 + 2 = 6B
            writer.writeRecord(ch, bytes(3, 4, 5), 3); // 4 + 3 = 7B
        }

        Path file = state.files().get(0);
        try (DataInputStream in = new DataInputStream(new FileInputStream(file.toFile()))) {
            in.readInt(); // gateIdx
            in.readInt(); // channelIdx
            int bufferLength = in.readInt();
            // Body = (4+2) + (4+3) = 13B
            assertThat(bufferLength).isEqualTo((Integer.BYTES + 2) + (Integer.BYTES + 3));
            in.skipBytes(bufferLength);
            assertThat(in.read()).isEqualTo(-1); // EOF
        }
    }

    // -------------------------------------------------------------------------------------------
    // End-to-end roundtrip with reader
    // -------------------------------------------------------------------------------------------

    @Test
    void testRoundtripSingleSegment() throws Exception {
        InputChannelInfo ch = new InputChannelInfo(0, 0);
        byte[] record = bytes(10, 20, 30, 40);

        FetchedChannelState state = newState();
        try (FetchedChannelStateWriter writer = newWriter(state)) {
            writer.writeRecord(ch, record, record.length);
        }

        try (FetchedChannelStateReader reader = state.reader();
                var segs = reader.segments()) {
            assertThat(segs.hasNext()).isTrue();
            var seg = segs.next();
            assertThat(seg.channelInfo()).isEqualTo(ch);
            // Body = 4B length prefix + 4B data
            assertThat(seg.length()).isEqualTo((long) (Integer.BYTES + record.length));
            byte[] body = readAll(seg.body());
            assertThat(body).hasSize((int) seg.length());
            assertThat(segs.hasNext()).isFalse();
        }
    }

    @Test
    void testRoundtripChannelSwitchProducesTwoSegments() throws Exception {
        InputChannelInfo c0 = new InputChannelInfo(0, 0);
        InputChannelInfo c1 = new InputChannelInfo(0, 1);

        FetchedChannelState state = newState();
        try (FetchedChannelStateWriter writer = newWriter(state)) {
            writer.writeRecord(c0, bytes(1, 2), 2);
            writer.writeRecord(c1, bytes(3, 4, 5), 3);
        }

        java.util.List<InputChannelInfo> channels = new java.util.ArrayList<>();
        java.util.List<Long> lengths = new java.util.ArrayList<>();
        try (FetchedChannelStateReader reader = state.reader();
                var segs = reader.segments()) {
            while (segs.hasNext()) {
                var seg = segs.next();
                channels.add(seg.channelInfo());
                lengths.add(seg.length());
                readAll(seg.body());
            }
        }
        assertThat(channels).containsExactly(c0, c1);
        assertThat(lengths).containsExactly((long) (Integer.BYTES + 2), (long) (Integer.BYTES + 3));
    }

    @Test
    void testRoundtripSameChannelMerged() throws Exception {
        InputChannelInfo ch = new InputChannelInfo(0, 0);

        FetchedChannelState state = newState();
        try (FetchedChannelStateWriter writer = newWriter(state)) {
            writer.writeRecord(ch, bytes(1), 1);
            writer.writeRecord(ch, bytes(2, 3), 2);
            writer.writeRecord(ch, bytes(4, 5, 6), 3);
        }

        try (FetchedChannelStateReader reader = state.reader();
                var segs = reader.segments()) {
            assertThat(segs.hasNext()).isTrue();
            var seg = segs.next();
            assertThat(seg.channelInfo()).isEqualTo(ch);
            // All three records merged: body = 3*(4B) + (1+2+3) bytes = 18B
            long expectedLen = 3L * Integer.BYTES + (1 + 2 + 3);
            assertThat(seg.length()).isEqualTo(expectedLen);
            assertThat(segs.hasNext()).isFalse();
        }
    }

    @Test
    void testRoundtripFileRotation() throws Exception {
        InputChannelInfo c0 = new InputChannelInfo(0, 0);
        InputChannelInfo c1 = new InputChannelInfo(0, 1);
        InputChannelInfo c2 = new InputChannelInfo(0, 2);

        FetchedChannelState state = newState();
        // Force rotation after every segment.
        try (FetchedChannelStateWriter writer = new FetchedChannelStateWriter(state, tempDir, 1L)) {
            writer.writeRecord(c0, bytes(10), 1);
            writer.writeRecord(c1, bytes(20, 21), 2);
            writer.writeRecord(c2, bytes(30), 1);
        }

        assertThat(state.files()).hasSize(3);

        java.util.List<InputChannelInfo> channels = new java.util.ArrayList<>();
        try (FetchedChannelStateReader reader = state.reader();
                var segs = reader.segments()) {
            while (segs.hasNext()) {
                var seg = segs.next();
                channels.add(seg.channelInfo());
                readAll(seg.body()); // consume body
            }
        }
        assertThat(channels).containsExactly(c0, c1, c2);
    }

    @Test
    void testRoundtripPassThrough() throws Exception {
        InputChannelInfo ch = new InputChannelInfo(1, 2);
        // pass-through bytes are written verbatim (no length prefix added by writer)
        byte[] data = bytes(0x01, 0x02, 0x03, 0x04, 0x05);

        FetchedChannelState state = newState();
        try (FetchedChannelStateWriter writer = newWriter(state)) {
            writer.writePassThrough(ch, data, 0, data.length);
        }

        try (FetchedChannelStateReader reader = state.reader();
                var segs = reader.segments()) {
            assertThat(segs.hasNext()).isTrue();
            var seg = segs.next();
            assertThat(seg.channelInfo()).isEqualTo(ch);
            assertThat(seg.length()).isEqualTo(data.length);
            byte[] body = readAll(seg.body());
            assertThat(body).isEqualTo(data);
            assertThat(segs.hasNext()).isFalse();
        }
    }

    // -------------------------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------------------------

    private FetchedChannelState newState() {
        FetchedChannelState state = new FetchedChannelState();
        // Caller holds one lifecycle grant so transient reader opens don't trigger premature
        // cleanup.
        state.acquire();
        return state;
    }

    private FetchedChannelStateWriter newWriter(FetchedChannelState state) {
        return new FetchedChannelStateWriter(state, tempDir);
    }

    private static byte[] readAll(java.io.InputStream in) throws java.io.IOException {
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        byte[] buf = new byte[256];
        int n;
        while ((n = in.read(buf)) != -1) {
            out.write(buf, 0, n);
        }
        return out.toByteArray();
    }

    private static byte[] bytes(int... values) {
        byte[] out = new byte[values.length];
        for (int i = 0; i < values.length; i++) {
            out[i] = (byte) values[i];
        }
        return out;
    }
}
