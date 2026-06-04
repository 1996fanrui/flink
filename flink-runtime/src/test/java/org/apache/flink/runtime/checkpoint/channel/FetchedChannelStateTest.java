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
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link FetchedChannelState} lifecycle and segment locator table management. */
class FetchedChannelStateTest {

    @TempDir Path tempDir;

    @Test
    void testInitialStateIsEmpty() {
        FetchedChannelState state = new FetchedChannelState();
        assertThat(state.files()).isEmpty();
        assertThat(state.segments()).isEmpty();
        assertThat(state.isClosed()).isFalse();
    }

    @Test
    void testAddFileReturnsIncreasingIndex() throws IOException {
        try (FetchedChannelState state = new FetchedChannelState()) {
            Path file0 = tempDir.resolve("spill-0.bin");
            Path file1 = tempDir.resolve("spill-1.bin");

            int idx0 = state.addFile(file0);
            int idx1 = state.addFile(file1);

            assertThat(idx0).isEqualTo(0);
            assertThat(idx1).isEqualTo(1);
            assertThat(state.files()).containsExactly(file0, file1);
        }
    }

    @Test
    void testAppendSegmentBuildsList() throws IOException {
        try (FetchedChannelState state = new FetchedChannelState()) {
            InputChannelInfo ch0 = new InputChannelInfo(0, 0);
            InputChannelInfo ch1 = new InputChannelInfo(0, 1);

            FetchedSegment seg0 = new FetchedSegment(ch0, 0, 8L, 100L);
            FetchedSegment seg1 = new FetchedSegment(ch1, 0, 116L, 50L);

            state.appendSegment(seg0);
            state.appendSegment(seg1);

            assertThat(state.segments()).hasSize(2);
            assertThat(state.segments().get(0)).isSameAs(seg0);
            assertThat(state.segments().get(1)).isSameAs(seg1);
        }
    }

    @Test
    void testSegmentLocatorFields() {
        InputChannelInfo ch = new InputChannelInfo(2, 3);
        FetchedSegment seg = new FetchedSegment(ch, 1, 64L, 512L);

        assertThat(seg.channelInfo).isEqualTo(ch);
        assertThat(seg.fileIndex).isEqualTo(1);
        assertThat(seg.offset).isEqualTo(64L);
        assertThat(seg.length).isEqualTo(512L);
    }

    @Test
    void testFilesListIsUnmodifiable() throws IOException {
        try (FetchedChannelState state = new FetchedChannelState()) {
            state.addFile(tempDir.resolve("f0.bin"));
            assertThatThrownBy(() -> state.files().add(tempDir.resolve("f1.bin")))
                    .isInstanceOf(UnsupportedOperationException.class);
        }
    }

    @Test
    void testSegmentsListIsUnmodifiable() throws IOException {
        try (FetchedChannelState state = new FetchedChannelState()) {
            assertThatThrownBy(
                            () ->
                                    state.segments()
                                            .add(
                                                    new FetchedSegment(
                                                            new InputChannelInfo(0, 0), 0, 0, 0)))
                    .isInstanceOf(UnsupportedOperationException.class);
        }
    }

    @Test
    void testAcquireReleaseDoesNotDeleteFilesBeforeLastRelease() throws IOException {
        FetchedChannelState state = new FetchedChannelState();
        Path realFile = tempDir.resolve("spill-0.bin");
        realFile.toFile().createNewFile();
        state.addFile(realFile);

        state.acquire();
        state.acquire();

        state.release();
        // File must still exist after first release.
        assertThat(realFile.toFile()).exists();

        state.release();
        // Last release should delete the file.
        assertThat(realFile.toFile()).doesNotExist();
        assertThat(state.isClosed()).isTrue();
    }

    @Test
    void testCloseDeletesAllFiles() throws IOException {
        FetchedChannelState state = new FetchedChannelState();
        Path file0 = tempDir.resolve("f0.bin");
        Path file1 = tempDir.resolve("f1.bin");
        file0.toFile().createNewFile();
        file1.toFile().createNewFile();

        state.addFile(file0);
        state.addFile(file1);

        state.close();

        assertThat(file0.toFile()).doesNotExist();
        assertThat(file1.toFile()).doesNotExist();
        assertThat(state.isClosed()).isTrue();
    }

    @Test
    void testCloseIsIdempotent() throws IOException {
        FetchedChannelState state = new FetchedChannelState();
        state.close();
        assertThat(state.isClosed()).isTrue();
        // Second close must not throw.
        state.close();
        assertThat(state.isClosed()).isTrue();
    }

    @Test
    void testCloseAfterReleaseIsIdempotent() throws IOException {
        FetchedChannelState state = new FetchedChannelState();
        state.acquire();
        state.release();
        assertThat(state.isClosed()).isTrue();
        // close() after last release must be a no-op (no double-delete attempt).
        state.close();
        assertThat(state.isClosed()).isTrue();
    }
}
