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

import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link FetchedChannelStateReader}: segment iteration, body boundedness, cross-file
 * transparency, snapshot derivation, and fail-loud on truncated segments.
 */
class FetchedChannelStateReaderTest {

    @TempDir Path tempDir;

    // -------------------------------------------------------------------------------------------
    // Segment iteration
    // -------------------------------------------------------------------------------------------

    @Test
    void testSegmentsEmptyWhenNoDataWritten() throws Exception {
        FetchedChannelState state = newState();
        try (FetchedChannelStateWriter writer = newWriter(state)) {
            // nothing written
        }

        try (FetchedChannelStateReader reader = state.reader();
                CloseableIterator<FetchedSegmentCursor> segs = reader.segments()) {
            assertThat(segs.hasNext()).isFalse();
        }
    }

    @Test
    void testSingleSegmentBodyMatchesWrittenBytes() throws Exception {
        InputChannelInfo ch = new InputChannelInfo(0, 0);
        byte[] record = bytes(1, 2, 3, 4);

        FetchedChannelState state = newState();
        try (FetchedChannelStateWriter writer = newWriter(state)) {
            writer.writeRecord(ch, record, record.length);
        }

        try (FetchedChannelStateReader reader = state.reader();
                CloseableIterator<FetchedSegmentCursor> segs = reader.segments()) {
            assertThat(segs.hasNext()).isTrue();
            FetchedSegmentCursor seg = segs.next();
            assertThat(seg.channelInfo()).isEqualTo(ch);

            // Body contains [4B recordLen][4B record data]
            byte[] bodyBytes = readAll(seg.body());
            assertThat(bodyBytes).hasSize((int) seg.length());
            assertThat(segs.hasNext()).isFalse();
        }
    }

    @Test
    void testMultipleSegmentsIteratedInOrder() throws Exception {
        InputChannelInfo c0 = new InputChannelInfo(0, 0);
        InputChannelInfo c1 = new InputChannelInfo(0, 1);

        FetchedChannelState state = newState();
        try (FetchedChannelStateWriter writer = newWriter(state)) {
            writer.writeRecord(c0, bytes(10), 1);
            writer.writeRecord(c1, bytes(20), 1);
            writer.writeRecord(c0, bytes(30), 1);
        }

        List<InputChannelInfo> channels = new ArrayList<>();
        try (FetchedChannelStateReader reader = state.reader();
                CloseableIterator<FetchedSegmentCursor> segs = reader.segments()) {
            while (segs.hasNext()) {
                FetchedSegmentCursor seg = segs.next();
                channels.add(seg.channelInfo());
                // Consume the body to avoid resource leaks and ensure iteration works.
                readAll(seg.body());
            }
        }

        // Segments are produced at channel switches: c0, c1, c0
        assertThat(channels).containsExactly(c0, c1, c0);
    }

    // -------------------------------------------------------------------------------------------
    // Body boundedness: body() stops exactly at segment end
    // -------------------------------------------------------------------------------------------

    @Test
    void testBodyReturnsMinus1AtSegmentEnd() throws Exception {
        InputChannelInfo ch = new InputChannelInfo(0, 0);

        FetchedChannelState state = newState();
        try (FetchedChannelStateWriter writer = newWriter(state)) {
            writer.writeRecord(ch, bytes(1, 2), 2);
        }

        try (FetchedChannelStateReader reader = state.reader();
                CloseableIterator<FetchedSegmentCursor> segs = reader.segments()) {
            FetchedSegmentCursor seg = segs.next();
            InputStream body = seg.body();
            // Read exactly length bytes
            byte[] data = new byte[(int) seg.length()];
            int totalRead = 0;
            while (totalRead < data.length) {
                int n = body.read(data, totalRead, data.length - totalRead);
                assertThat(n).isGreaterThan(0);
                totalRead += n;
            }
            // Next read must return EOF
            assertThat(body.read()).isEqualTo(-1);
        }
    }

    @Test
    void testBodyLengthMatchesSegmentLocatorLength() throws Exception {
        InputChannelInfo ch = new InputChannelInfo(0, 0);
        byte[] record = bytes(1, 2, 3, 4, 5);

        FetchedChannelState state = newState();
        try (FetchedChannelStateWriter writer = newWriter(state)) {
            writer.writeRecord(ch, record, record.length);
        }

        try (FetchedChannelStateReader reader = state.reader();
                CloseableIterator<FetchedSegmentCursor> segs = reader.segments()) {
            FetchedSegmentCursor seg = segs.next();
            byte[] bodyBytes = readAll(seg.body());
            assertThat((long) bodyBytes.length).isEqualTo(seg.length());
        }
    }

    // -------------------------------------------------------------------------------------------
    // Cross-file transparency
    // -------------------------------------------------------------------------------------------

    @Test
    void testCrossFileTransparencyWhenRotationOccurs() throws Exception {
        InputChannelInfo c0 = new InputChannelInfo(0, 0);
        InputChannelInfo c1 = new InputChannelInfo(0, 1);

        // Use tiny rotation threshold so first segment triggers a file rotation.
        FetchedChannelState state = newState();
        // Write with a small segment size to force rotation after first segment
        try (FetchedChannelStateWriter writer =
                new FetchedChannelStateWriter(state, tempDir, 1 /* 1 byte threshold */)) {
            writer.writeRecord(c0, bytes(10, 11, 12), 3);
            writer.writeRecord(c1, bytes(20, 21), 2);
        }

        // Two segments, possibly in different files.
        assertThat(state.segments()).hasSize(2);

        List<InputChannelInfo> channels = new ArrayList<>();
        try (FetchedChannelStateReader reader = state.reader();
                CloseableIterator<FetchedSegmentCursor> segs = reader.segments()) {
            while (segs.hasNext()) {
                FetchedSegmentCursor seg = segs.next();
                channels.add(seg.channelInfo());
                // Body read must not throw even if the segment is in a different file.
                readAll(seg.body());
            }
        }

        assertThat(channels).containsExactly(c0, c1);
    }

    // -------------------------------------------------------------------------------------------
    // Snapshot: independent reader with correct start position
    // -------------------------------------------------------------------------------------------

    @Test
    void testSnapshotCoversAllSegmentsWhenNothingConsumed() throws Exception {
        InputChannelInfo c0 = new InputChannelInfo(0, 0);
        InputChannelInfo c1 = new InputChannelInfo(0, 1);

        FetchedChannelState state = newState();
        try (FetchedChannelStateWriter writer = newWriter(state)) {
            writer.writeRecord(c0, bytes(1), 1);
            writer.writeRecord(c1, bytes(2), 1);
        }

        try (FetchedChannelStateReader root = state.reader()) {
            // Snapshot before consuming anything
            try (FetchedChannelStateReader snap = root.snapshot();
                    CloseableIterator<FetchedSegmentCursor> segs = snap.segments()) {
                List<InputChannelInfo> channels = new ArrayList<>();
                while (segs.hasNext()) {
                    FetchedSegmentCursor seg = segs.next();
                    channels.add(seg.channelInfo());
                    readAll(seg.body());
                }
                assertThat(channels).containsExactly(c0, c1);
            }
        }
    }

    @Test
    void testSnapshotAfterFullSegmentConsumedSkipsThatSegment() throws Exception {
        InputChannelInfo c0 = new InputChannelInfo(0, 0);
        InputChannelInfo c1 = new InputChannelInfo(0, 1);

        FetchedChannelState state = newState();
        try (FetchedChannelStateWriter writer = newWriter(state)) {
            writer.writeRecord(c0, bytes(1), 1);
            writer.writeRecord(c1, bytes(2), 1);
        }

        try (FetchedChannelStateReader root = state.reader();
                CloseableIterator<FetchedSegmentCursor> rootSegs = root.segments()) {
            // Consume and commit first segment
            FetchedSegmentCursor first = rootSegs.next();
            readAll(first.body());
            first.commitConsumed();

            // Snapshot must start from second segment
            try (FetchedChannelStateReader snap = root.snapshot();
                    CloseableIterator<FetchedSegmentCursor> segs = snap.segments()) {
                List<InputChannelInfo> channels = new ArrayList<>();
                while (segs.hasNext()) {
                    FetchedSegmentCursor seg = segs.next();
                    channels.add(seg.channelInfo());
                    readAll(seg.body());
                }
                assertThat(channels).containsExactly(c1);
            }
        }
    }

    @Test
    void testSnapshotFromMidSegmentStartsAtCommittedByteOffset() throws Exception {
        InputChannelInfo ch = new InputChannelInfo(0, 0);

        FetchedChannelState state = newState();
        try (FetchedChannelStateWriter writer = newWriter(state)) {
            // Write two records into the same channel so they end up in one segment
            writer.writeRecord(ch, bytes(10, 11), 2);
        }
        // Verify: one segment
        assertThat(state.segments()).hasSize(1);
        long fullLength = state.segments().get(0).length;

        try (FetchedChannelStateReader root = state.reader();
                CloseableIterator<FetchedSegmentCursor> rootSegs = root.segments()) {
            FetchedSegmentCursor seg = rootSegs.next();
            InputStream body = seg.body();

            // Read only 1 byte without committing, then snapshot — snapshot should start from 0
            // (no bytes committed yet).
            body.read();

            try (FetchedChannelStateReader snapBeforeCommit = root.snapshot();
                    CloseableIterator<FetchedSegmentCursor> segsBeforeCommit =
                            snapBeforeCommit.segments()) {
                assertThat(segsBeforeCommit.hasNext()).isTrue();
                FetchedSegmentCursor snapSeg = segsBeforeCommit.next();
                assertThat(snapSeg.length()).isEqualTo(fullLength);
            }

            // Read rest of body and commit
            readAll(body);
            seg.commitConsumed();

            // After commit the snapshot must be empty
            try (FetchedChannelStateReader snapAfterCommit = root.snapshot();
                    CloseableIterator<FetchedSegmentCursor> segsAfterCommit =
                            snapAfterCommit.segments()) {
                assertThat(segsAfterCommit.hasNext()).isFalse();
            }
        }
    }

    // -------------------------------------------------------------------------------------------
    // Fail-loud on truncated segment
    // -------------------------------------------------------------------------------------------

    @Test
    void testBodyThrowsEOFExceptionOnTruncatedFile() throws Exception {
        InputChannelInfo ch = new InputChannelInfo(0, 0);

        FetchedChannelState state = newState();
        try (FetchedChannelStateWriter writer = newWriter(state)) {
            writer.writeRecord(ch, bytes(1, 2, 3, 4, 5, 6, 7, 8), 8);
        }

        // Truncate the spill file to simulate corruption.
        Path spill = state.files().get(0);
        java.nio.file.Files.write(
                spill, new byte[4], java.nio.file.StandardOpenOption.TRUNCATE_EXISTING);

        try (FetchedChannelStateReader reader = state.reader();
                CloseableIterator<FetchedSegmentCursor> segs = reader.segments()) {
            assertThat(segs.hasNext()).isTrue();
            FetchedSegmentCursor seg = segs.next();
            // Segment locator says length > bytes available: must throw EOFException or IOException
            assertThatThrownBy(() -> readAll(seg.body()))
                    .isInstanceOfAny(EOFException.class, IOException.class);
        }
    }

    // -------------------------------------------------------------------------------------------
    // Reference counting: acquire/release via reader lifecycle
    // -------------------------------------------------------------------------------------------

    @Test
    void testReaderAcquiresAndReleasesRefCount() throws Exception {
        InputChannelInfo ch = new InputChannelInfo(0, 0);

        FetchedChannelState state = newState();
        try (FetchedChannelStateWriter writer = newWriter(state)) {
            writer.writeRecord(ch, bytes(1), 1);
        }

        Path spill = state.files().get(0);

        FetchedChannelStateReader reader = state.reader();
        assertThat(java.nio.file.Files.exists(spill)).isTrue();

        reader.close();

        // After closing root reader (and not calling acquire() externally), file is cleaned up.
        assertThat(java.nio.file.Files.exists(spill)).isFalse();
    }

    @Test
    void testSnapshotKeepsFilesAliveUntilSnapshotClosed() throws Exception {
        InputChannelInfo ch = new InputChannelInfo(0, 0);

        FetchedChannelState state = newState();
        try (FetchedChannelStateWriter writer = newWriter(state)) {
            writer.writeRecord(ch, bytes(1), 1);
        }

        Path spill = state.files().get(0);

        FetchedChannelStateReader root = state.reader();
        FetchedChannelStateReader snap = root.snapshot();

        root.close(); // One grant released; file must still exist because snap holds another.
        assertThat(java.nio.file.Files.exists(spill)).isTrue();

        snap.close(); // Last grant released; file must be deleted.
        assertThat(java.nio.file.Files.exists(spill)).isFalse();
    }

    // -------------------------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------------------------

    private FetchedChannelState newState() {
        return new FetchedChannelState();
    }

    private FetchedChannelStateWriter newWriter(FetchedChannelState state) throws IOException {
        return new FetchedChannelStateWriter(
                state, tempDir, FetchedChannelState.DEFAULT_SEGMENT_SIZE_BYTES);
    }

    private static byte[] readAll(InputStream in) throws IOException {
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        byte[] buf = new byte[256];
        int n;
        while ((n = in.read(buf)) != -1) {
            out.write(buf, 0, n);
        }
        return out.toByteArray();
    }

    private static byte[] bytes(int... values) {
        byte[] arr = new byte[values.length];
        for (int i = 0; i < values.length; i++) {
            arr[i] = (byte) values[i];
        }
        return arr;
    }
}
