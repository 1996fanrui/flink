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
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link SpillFile}. */
class SpillFileTest {

    @TempDir Path tempDir;

    @Test
    void testAppendRoundtrip() throws IOException {
        try (SpillFile spillFile = new SpillFile(tempDir)) {
            InputChannelInfo channelInfo = new InputChannelInfo(0, 1);
            byte[] payload = bytes(0xAB, 0xCD, 0xEF, 0x12, 0x34);
            spillFile.append(channelInfo, ByteBuffer.wrap(payload));

            List<SpillFile.Entry> entries = spillFile.entries();
            assertThat(entries).hasSize(1);
            SpillFile.Entry entry = entries.get(0);
            assertThat(entry.channelInfo).isEqualTo(channelInfo);
            assertThat(entry.segmentIndex).isEqualTo(0);
            assertThat(entry.offset).isEqualTo(0L);
            assertThat(entry.length).isEqualTo(payload.length);

            byte[] readBack = spillFile.readBytes(entry.segmentIndex, entry.offset, entry.length);
            assertThat(readBack).isEqualTo(payload);
        }
    }

    @Test
    void testSegmentRotationAcrossDefaultSegmentSize() throws IOException {
        // Use a tiny custom segment size to exercise rotation deterministically.
        long segmentSize = 16L;
        try (SpillFile spillFile = new SpillFile(tempDir, segmentSize)) {
            InputChannelInfo channelInfo = new InputChannelInfo(0, 0);
            byte[] payloadA = bytes(1, 2, 3, 4, 5, 6, 7, 8, 9, 10); // 10 bytes
            byte[] payloadB = bytes(11, 12, 13, 14, 15, 16, 17, 18); // 8 bytes — would overflow
            byte[] payloadC = bytes(19, 20, 21, 22, 23); // 5 bytes — fits in segment 1

            spillFile.append(channelInfo, ByteBuffer.wrap(payloadA));
            spillFile.append(channelInfo, ByteBuffer.wrap(payloadB));
            spillFile.append(channelInfo, ByteBuffer.wrap(payloadC));

            List<SpillFile.Entry> entries = spillFile.entries();
            assertThat(entries).hasSize(3);
            assertThat(entries.get(0).segmentIndex).isEqualTo(0);
            assertThat(entries.get(0).offset).isEqualTo(0L);
            assertThat(entries.get(1).segmentIndex)
                    .as("payload B should rotate to segment 1 since 10+8 > segmentSize")
                    .isEqualTo(1);
            assertThat(entries.get(1).offset).isEqualTo(0L);
            assertThat(entries.get(2).segmentIndex)
                    .as("payload C fits in segment 1 after payload B")
                    .isEqualTo(1);
            assertThat(entries.get(2).offset).isEqualTo((long) payloadB.length);

            assertThat(spillFile.segments()).hasSize(2);

            byte[] readA = spillFile.readBytes(0, 0L, payloadA.length);
            byte[] readB = spillFile.readBytes(1, 0L, payloadB.length);
            byte[] readC = spillFile.readBytes(1, payloadB.length, payloadC.length);
            assertThat(readA).isEqualTo(payloadA);
            assertThat(readB).isEqualTo(payloadB);
            assertThat(readC).isEqualTo(payloadC);
        }
    }

    @Test
    void testAppendAfterCloseThrows() throws IOException {
        SpillFile spillFile = new SpillFile(tempDir);
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
    void testEntriesMatchDiskLayout() throws IOException {
        // Two channels interleaved within one segment. Verify that each entry's (offset, length)
        // matches both the cumulative bytes written and the actual bytes recoverable from disk.
        long segmentSize = 256L;
        try (SpillFile spillFile = new SpillFile(tempDir, segmentSize)) {
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

            List<SpillFile.Entry> entries = spillFile.entries();
            assertThat(entries).hasSize(4);

            long cumulative = 0;
            byte[][] payloads = {p0, p1, p2, p3};
            InputChannelInfo[] channels = {c0, c1, c0, c1};
            for (int i = 0; i < entries.size(); i++) {
                SpillFile.Entry e = entries.get(i);
                assertThat(e.segmentIndex).isEqualTo(0);
                assertThat(e.offset).isEqualTo(cumulative);
                assertThat(e.length).isEqualTo(payloads[i].length);
                assertThat(e.channelInfo).isEqualTo(channels[i]);
                byte[] readBack = spillFile.readBytes(0, e.offset, e.length);
                assertThat(readBack).isEqualTo(payloads[i]);
                cumulative += payloads[i].length;
            }
        }
    }

    @Test
    void testCloseIsIdempotent() throws IOException {
        SpillFile spillFile = new SpillFile(tempDir);
        spillFile.append(new InputChannelInfo(0, 0), ByteBuffer.wrap(bytes(1, 2, 3)));
        spillFile.close();
        assertThat(spillFile.isClosed()).isTrue();
        // Second close must not throw.
        spillFile.close();
        assertThat(spillFile.isClosed()).isTrue();
    }

    private static byte[] bytes(int... values) {
        byte[] out = new byte[values.length];
        for (int i = 0; i < values.length; i++) {
            out[i] = (byte) values[i];
        }
        return out;
    }
}
