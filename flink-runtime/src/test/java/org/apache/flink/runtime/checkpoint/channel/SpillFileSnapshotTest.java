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

/** Unit tests for {@link SpillFile#snapshot()}. */
class SpillFileSnapshotTest {

    @TempDir Path tempDir;

    @Test
    void testSnapshotIsImmutable() throws IOException {
        try (SpillFile spillFile = new SpillFile(tempDir)) {
            InputChannelInfo ch = new InputChannelInfo(0, 0);
            spillFile.append(ch, ByteBuffer.wrap(bytes(1, 2, 3)));
            spillFile.append(ch, ByteBuffer.wrap(bytes(4, 5)));

            SpillFile.Snapshot snap = spillFile.snapshot();
            List<SpillFile.Entry> entries = snap.getEntries();
            List<SpillFile.SpillFileSegment> segments = snap.getSegments();

            assertThatThrownBy(() -> entries.add(null))
                    .isInstanceOf(UnsupportedOperationException.class);
            assertThatThrownBy(() -> segments.add(null))
                    .isInstanceOf(UnsupportedOperationException.class);
        }
    }

    @Test
    void testAppendAfterSnapshotDoesNotAffectSnapshot() throws IOException {
        try (SpillFile spillFile = new SpillFile(tempDir)) {
            InputChannelInfo ch = new InputChannelInfo(0, 0);
            spillFile.append(ch, ByteBuffer.wrap(bytes(1, 2, 3)));

            SpillFile.Snapshot snap = spillFile.snapshot();
            int entryCountBefore = snap.getEntries().size();
            int segmentCountBefore = snap.getSegments().size();

            spillFile.append(ch, ByteBuffer.wrap(bytes(4, 5)));
            spillFile.append(ch, ByteBuffer.wrap(bytes(6, 7, 8, 9)));

            // Previously-taken snapshot must not see the new appends; the live file must.
            assertThat(snap.getEntries()).hasSize(entryCountBefore);
            assertThat(snap.getSegments()).hasSize(segmentCountBefore);
            assertThat(spillFile.entries()).hasSize(entryCountBefore + 2);
        }
    }

    @Test
    void testMultipleSnapshotsAreIndependent() throws IOException {
        try (SpillFile spillFile = new SpillFile(tempDir)) {
            InputChannelInfo ch = new InputChannelInfo(0, 0);
            spillFile.append(ch, ByteBuffer.wrap(bytes(1)));

            SpillFile.Snapshot s1 = spillFile.snapshot();

            spillFile.append(ch, ByteBuffer.wrap(bytes(2, 3)));
            SpillFile.Snapshot s2 = spillFile.snapshot();

            spillFile.append(ch, ByteBuffer.wrap(bytes(4, 5, 6)));
            SpillFile.Snapshot s3 = spillFile.snapshot();

            assertThat(s1.getEntries()).hasSize(1);
            assertThat(s2.getEntries()).hasSize(2);
            assertThat(s3.getEntries()).hasSize(3);

            assertThat(s1.getEntries().get(0).length).isEqualTo(1);
            assertThat(s2.getEntries().get(1).length).isEqualTo(2);
            assertThat(s3.getEntries().get(2).length).isEqualTo(3);
        }
    }

    private static byte[] bytes(int... values) {
        byte[] out = new byte[values.length];
        for (int i = 0; i < values.length; i++) {
            out[i] = (byte) values[i];
        }
        return out;
    }
}
