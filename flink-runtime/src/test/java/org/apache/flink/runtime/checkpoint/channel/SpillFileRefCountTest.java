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
            // Two acquires, two releases — net zero, so segment files should be gone afterwards.
            spillFile.acquire();
            spillFile.acquire();
            List<SpillFile.SpillFileSegment> segs = spillFile.segments();
            assertSegmentsExist(segs, true);

            spillFile.release();
            // Still one reference outstanding → files must remain.
            assertSegmentsExist(segs, true);

            spillFile.release();
            // Zero references → segments deleted.
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

        // Extra release after zero — must not throw, must not re-attempt deletion. The CAS guard
        // on cleanedUp ensures deleteAllSegments runs at most once.
        spillFile.release();
        spillFile.release();
        assertSegmentsExist(segs, false);
    }

    @Test
    void testAbortPathReleasesViaSameRoute() throws IOException {
        SpillFile spillFile = newSpillFileWithEntries(2);
        // Simulate the production wiring: SpillFileReader takes one acquire on construction; each
        // in-flight DiskSnapshot takes another inside the lock.
        spillFile.acquire();
        spillFile.acquire();
        spillFile.acquire();
        List<SpillFile.SpillFileSegment> segs = spillFile.segments();

        // Abort fires whenComplete on the cpId future, which closes DiskSnapshot →
        // spillFile.release.
        // Drain still holds the first acquire — files must remain after the snapshot releases.
        spillFile.release();
        spillFile.release();
        assertSegmentsExist(segs, true);

        // Drain finishes — last release brings the count to zero, deleting segments.
        spillFile.release();
        assertSegmentsExist(segs, false);
    }

    @Test
    void testForceCloseStillCleansSegments() throws IOException {
        SpillFile spillFile = newSpillFileWithEntries(2);
        // Acquire twice but never release — emulate a shutdown where references are still
        // outstanding (e.g. cpId futures abandoned). Force-close must still wipe disk.
        spillFile.acquire();
        spillFile.acquire();
        List<SpillFile.SpillFileSegment> segs = spillFile.segments();
        assertSegmentsExist(segs, true);

        spillFile.close();
        assertSegmentsExist(segs, false);

        // Repeated close is a no-op.
        spillFile.close();

        // Subsequent release must not re-delete (CAS already won by close) and must not throw.
        spillFile.release();
        assertSegmentsExist(segs, false);
    }

    private SpillFile newSpillFileWithEntries(int count) throws IOException {
        SpillFile spillFile = new SpillFile(tempDir);
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
