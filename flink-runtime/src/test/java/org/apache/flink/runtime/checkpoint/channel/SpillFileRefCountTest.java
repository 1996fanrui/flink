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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies the reference counter on {@link SpillFile}: acquire/release pairing, zero-triggered
 * segment deletion, idempotency, abort-path equivalence, and the forced {@link SpillFile#close()}
 * cleanup entry.
 */
class SpillFileRefCountTest {

    @TempDir Path tempDir;

    @Test
    void testAcquireReleaseCountsMatch() throws IOException {
        try (SpillFile spillFile = newSpillFileWithEntries(3)) {
            spillFile.acquire();
            spillFile.acquire();
            List<SpillFile.SpillFileSegment> segs = spillFile.segments();
            assertSegmentsExist(segs, true);

            spillFile.release();
            assertSegmentsExist(segs, true);

            spillFile.release();
            assertSegmentsExist(segs, false);
        }
    }

    @Test
    void testReachingZeroDeletesSegments() throws IOException {
        SpillFile spillFile = newSpillFileWithEntries(2);
        spillFile.acquire();
        List<SpillFile.SpillFileSegment> segs = spillFile.segments();
        assertSegmentsExist(segs, true);

        spillFile.release();
        assertSegmentsExist(segs, false);
    }

    @Test
    void testReleaseAfterZeroIsNoOp() throws IOException {
        SpillFile spillFile = newSpillFileWithEntries(2);
        spillFile.acquire();
        List<SpillFile.SpillFileSegment> segs = spillFile.segments();
        spillFile.release();
        assertSegmentsExist(segs, false);

        // Extra releases past zero must be a no-op: the CAS guard on cleanedUp keeps
        // deleteAllSegments idempotent and never throws.
        spillFile.release();
        spillFile.release();
        assertSegmentsExist(segs, false);
    }

    @Test
    void testAbortPathReleasesViaSameRoute() throws IOException {
        SpillFile spillFile = newSpillFileWithEntries(2);
        // Mirror the production wiring: one acquire held by SpillFileReader, one per in-flight
        // SpillFileReader. Releasing the snapshots while the reader still holds its acquire must
        // keep the segments on disk.
        spillFile.acquire();
        spillFile.acquire();
        spillFile.acquire();
        List<SpillFile.SpillFileSegment> segs = spillFile.segments();

        spillFile.release();
        spillFile.release();
        assertSegmentsExist(segs, true);

        spillFile.release();
        assertSegmentsExist(segs, false);
    }

    @Test
    void testForceCloseStillCleansSegments() throws IOException {
        SpillFile spillFile = newSpillFileWithEntries(2);
        // Outstanding acquires that are never released emulate shutdown with abandoned cpId
        // futures; close() must still wipe disk.
        spillFile.acquire();
        spillFile.acquire();
        List<SpillFile.SpillFileSegment> segs = spillFile.segments();
        assertSegmentsExist(segs, true);

        spillFile.close();
        assertSegmentsExist(segs, false);

        spillFile.close();

        // After close() wins the CAS, late release() must neither re-delete nor throw.
        spillFile.release();
        assertSegmentsExist(segs, false);
    }

    private SpillFile newSpillFileWithEntries(int count) throws IOException {
        SpillFile spillFile = new SpillFile(tempDir, 4096);
        for (int i = 0; i < count; i++) {
            spillFile.append(
                    new InputChannelInfo(0, i % 2),
                    ByteBuffer.wrap(new byte[] {(byte) i, (byte) (i + 1)}));
        }
        return spillFile;
    }

    private static void assertSegmentsExist(List<SpillFile.SpillFileSegment> segs, boolean exists) {
        for (SpillFile.SpillFileSegment seg : segs) {
            assertThat(Files.exists(seg.path))
                    .as("segment " + seg.path + " exists=" + exists)
                    .isEqualTo(exists);
        }
    }
}
