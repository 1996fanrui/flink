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
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SpillFileTest {

    @TempDir Path tempDir;

    @Test
    void testAppendRoundtrip() throws IOException {
        try (SpillFile spillFile = new SpillFile(tempDir, 4096)) {
            InputChannelInfo channelInfo = new InputChannelInfo(0, 1);
            byte[] payload = bytes(0xAB, 0xCD, 0xEF, 0x12, 0x34);
            spillFile.append(channelInfo, ByteBuffer.wrap(payload));

            try (SpillFileReader reader = spillFile.reader()) {
                SpillFileReader.Chunk c = reader.peek();
                assertThat(c).isNotNull();
                assertThat(c.channelInfo).isEqualTo(channelInfo);
                assertThat(c.length).isEqualTo(payload.length);
                assertThat(Arrays.copyOf(c.data, c.length)).isEqualTo(payload);
                reader.advance();
                assertThat(reader.peek()).isNull();
            }
        }
    }

    @Test
    void testSegmentRotationAcrossDefaultSegmentSize() throws IOException {
        InputChannelInfo channelInfo = new InputChannelInfo(0, 0);
        byte[] payloadA = bytes(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        byte[] payloadB = bytes(11, 12, 13, 14, 15, 16, 17, 18);
        byte[] payloadC = bytes(19, 20, 21, 22, 23);

        // Sized so the first two records (payload plus inline header each) exactly fill segment 0,
        // forcing the third to rotate into segment 1.
        long segmentSize = 2L * SpillFile.HEADER_BYTES + payloadA.length + payloadB.length;
        try (SpillFile spillFile = new SpillFile(tempDir, segmentSize, 4096)) {
            spillFile.append(channelInfo, ByteBuffer.wrap(payloadA));
            spillFile.append(channelInfo, ByteBuffer.wrap(payloadB));
            spillFile.append(channelInfo, ByteBuffer.wrap(payloadC));

            assertThat(spillFile.segments()).hasSize(2);

            List<byte[]> readBack = drainAll(spillFile);
            assertThat(readBack).hasSize(3);
            assertThat(readBack.get(0)).isEqualTo(payloadA);
            assertThat(readBack.get(1)).isEqualTo(payloadB);
            assertThat(readBack.get(2)).isEqualTo(payloadC);
        }
    }

    @Test
    void testReaderRejectsEmptySegment() throws Exception {
        long segmentSize = 4L;
        try (SpillFile spillFile = new SpillFile(tempDir, segmentSize, 4096)) {
            InputChannelInfo channelInfo = new InputChannelInfo(0, 0);
            byte[] payloadA = bytes(1, 2, 3, 4);
            byte[] payloadB = bytes(5);

            spillFile.append(channelInfo, ByteBuffer.wrap(payloadA));
            spillFile.append(channelInfo, ByteBuffer.wrap(payloadB));

            assertThat(spillFile.segments()).hasSize(2);
            markSegmentEmpty(spillFile.segments().get(1));

            try (SpillFileReader reader = spillFile.reader()) {
                SpillFileReader.Chunk first = reader.peek();
                assertThat(first).isNotNull();
                assertThat(Arrays.copyOf(first.data, first.length)).isEqualTo(payloadA);
                reader.advance();

                assertThatThrownBy(reader::peek)
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessageContaining("is empty");
            }
        }
    }

    @Test
    void testAppendAfterCloseThrows() throws IOException {
        SpillFile spillFile = new SpillFile(tempDir, 4096);
        spillFile.close();
        assertThatThrownBy(
                        () ->
                                spillFile.append(
                                        new InputChannelInfo(0, 0),
                                        ByteBuffer.wrap(bytes(1, 2, 3))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("closed");
    }

    @Test
    void testEntriesInterleavedAcrossChannels() throws IOException {
        // Two channels interleaved within one segment. Reading the file back must surface every
        // payload in append order with the right channelInfo.
        long segmentSize = 256L;
        try (SpillFile spillFile = new SpillFile(tempDir, segmentSize, 4096)) {
            InputChannelInfo c0 = new InputChannelInfo(0, 0);
            InputChannelInfo c1 = new InputChannelInfo(0, 1);

            byte[] p0 = bytes(0xA, 0xB);
            byte[] p1 = bytes(0xC, 0xD, 0xE);
            byte[] p2 = bytes(0xF);
            byte[] p3 = bytes(0x1, 0x2, 0x3, 0x4);

            spillFile.append(c0, ByteBuffer.wrap(p0));
            spillFile.append(c1, ByteBuffer.wrap(p1));
            spillFile.append(c0, ByteBuffer.wrap(p2));
            spillFile.append(c1, ByteBuffer.wrap(p3));

            byte[][] expectedPayloads = {p0, p1, p2, p3};
            InputChannelInfo[] expectedChannels = {c0, c1, c0, c1};

            try (SpillFileReader reader = spillFile.reader()) {
                for (int i = 0; i < expectedPayloads.length; i++) {
                    SpillFileReader.Chunk c = reader.peek();
                    assertThat(c).isNotNull();
                    assertThat(c.channelInfo).isEqualTo(expectedChannels[i]);
                    assertThat(c.length).isEqualTo(expectedPayloads[i].length);
                    assertThat(Arrays.copyOf(c.data, c.length)).isEqualTo(expectedPayloads[i]);
                    reader.advance();
                }
                assertThat(reader.peek()).isNull();
            }
        }
    }

    @Test
    void testSnapshotIncludesPeekedEntryUntilAdvance() throws IOException {
        try (SpillFile spillFile = new SpillFile(tempDir, 4096)) {
            InputChannelInfo channelInfo = new InputChannelInfo(0, 0);
            byte[] payloadA = bytes(1, 2, 3);
            byte[] payloadB = bytes(4, 5, 6);
            spillFile.append(channelInfo, ByteBuffer.wrap(payloadA));
            spillFile.append(channelInfo, ByteBuffer.wrap(payloadB));

            try (SpillFileReader reader = spillFile.reader()) {
                assertThat(reader.peek()).isNotNull();

                try (SpillFileReader snapshot = reader.snapshot()) {
                    SpillFileReader.Chunk first = snapshot.peek();
                    assertThat(first).isNotNull();
                    assertThat(Arrays.copyOf(first.data, first.length)).isEqualTo(payloadA);
                    snapshot.advance();

                    SpillFileReader.Chunk second = snapshot.peek();
                    assertThat(second).isNotNull();
                    assertThat(Arrays.copyOf(second.data, second.length)).isEqualTo(payloadB);
                }
            }
        }
    }

    @Test
    void testSnapshotExcludesAdvancedEntry() throws IOException {
        try (SpillFile spillFile = new SpillFile(tempDir, 4096)) {
            InputChannelInfo channelInfo = new InputChannelInfo(0, 0);
            byte[] payloadA = bytes(1, 2, 3);
            byte[] payloadB = bytes(4, 5, 6);
            spillFile.append(channelInfo, ByteBuffer.wrap(payloadA));
            spillFile.append(channelInfo, ByteBuffer.wrap(payloadB));

            try (SpillFileReader reader = spillFile.reader()) {
                assertThat(reader.peek()).isNotNull();
                reader.advance();

                try (SpillFileReader snapshot = reader.snapshot()) {
                    SpillFileReader.Chunk first = snapshot.peek();
                    assertThat(first).isNotNull();
                    assertThat(Arrays.copyOf(first.data, first.length)).isEqualTo(payloadB);
                    snapshot.advance();
                    assertThat(snapshot.peek()).isNull();
                }
            }
        }
    }

    @Test
    void testCloseIsIdempotent() throws IOException {
        SpillFile spillFile = new SpillFile(tempDir, 4096);
        spillFile.append(new InputChannelInfo(0, 0), ByteBuffer.wrap(bytes(1, 2, 3)));
        spillFile.close();
        assertThat(spillFile.isClosed()).isTrue();
        spillFile.close();
        assertThat(spillFile.isClosed()).isTrue();
    }

    private static List<byte[]> drainAll(SpillFile spillFile) throws IOException {
        List<byte[]> out = new ArrayList<>();
        try (SpillFileReader reader = spillFile.reader()) {
            SpillFileReader.Chunk c;
            while ((c = reader.peek()) != null) {
                out.add(Arrays.copyOf(c.data, c.length));
                reader.advance();
            }
        }
        return out;
    }

    private static void markSegmentEmpty(SpillFile.SpillFileSegment segment) throws Exception {
        Field currentEndField = SpillFile.SpillFileSegment.class.getDeclaredField("currentEnd");
        currentEndField.setAccessible(true);
        currentEndField.setLong(segment, 0L);
    }

    private static byte[] bytes(int... values) {
        byte[] out = new byte[values.length];
        for (int i = 0; i < values.length; i++) {
            out[i] = (byte) values[i];
        }
        return out;
    }
}
