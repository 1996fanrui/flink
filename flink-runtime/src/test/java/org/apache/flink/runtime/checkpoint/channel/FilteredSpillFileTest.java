/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.checkpoint.channel;

import org.apache.flink.runtime.memory.MemoryManager;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link FilteredSpillFile.Writer}, {@link FilteredSpillFile.Reader}, and {@link
 * FilteredSpillFile.Entry}.
 */
class FilteredSpillFileTest {

    @TempDir private Path temporaryFolder;

    private static final int MEMORY_SEGMENT_SIZE = MemoryManager.DEFAULT_PAGE_SIZE;

    /**
     * Write data, read back, verify raw bytes match. The file contains pure bytes with no metadata.
     */
    @Test
    void testPureByteStream() throws Exception {
        String[] spillDirs = {temporaryFolder.toString()};
        Random random = new Random(42);
        byte[] data = new byte[1024];
        random.nextBytes(data);

        try (FilteredSpillFile.Writer writer = new FilteredSpillFile.Writer(spillDirs)) {
            long offset = writer.write(data, 0, data.length);
            assertThat(offset).isEqualTo(0L);

            try (FilteredSpillFile.Reader reader = writer.getCurrentFileReader()) {
                byte[] readBack = new byte[data.length];
                reader.read(offset, readBack, data.length);
                assertThat(readBack).isEqualTo(data);
            }
        }
    }

    /** Verify multiple writes produce contiguous offsets. */
    @Test
    void testMultipleWritesContiguous() throws Exception {
        String[] spillDirs = {temporaryFolder.toString()};
        byte[] data1 = new byte[] {1, 2, 3, 4};
        byte[] data2 = new byte[] {5, 6, 7, 8};

        try (FilteredSpillFile.Writer writer = new FilteredSpillFile.Writer(spillDirs)) {
            long offset1 = writer.write(data1, 0, data1.length);
            long offset2 = writer.write(data2, 0, data2.length);

            assertThat(offset1).isEqualTo(0L);
            assertThat(offset2).isEqualTo(4L);

            try (FilteredSpillFile.Reader reader = writer.getCurrentFileReader()) {
                byte[] readBack1 = new byte[4];
                reader.read(offset1, readBack1, 4);
                assertThat(readBack1).isEqualTo(data1);

                byte[] readBack2 = new byte[4];
                reader.read(offset2, readBack2, 4);
                assertThat(readBack2).isEqualTo(data2);
            }
        }
    }

    /**
     * Write more than 64MB to trigger file rotation, verify multiple files created and data correct
     * across files.
     */
    @Test
    void testFileRotation() throws Exception {
        // Use two directories to also verify round-robin selection
        Path dir1 = Files.createDirectory(temporaryFolder.resolve("dir1"));
        Path dir2 = Files.createDirectory(temporaryFolder.resolve("dir2"));
        String[] spillDirs = {dir1.toString(), dir2.toString()};

        // Write enough data to trigger at least one rotation (threshold is 64MB)
        int chunkSize = MEMORY_SEGMENT_SIZE;
        // 64MB / 32KB = 2048 chunks for one file, need > 2048 to rotate
        int numChunks = 2100;
        byte[][] chunks = new byte[numChunks][];
        long[] offsets = new long[numChunks];
        FilteredSpillFile.Reader[] readers = new FilteredSpillFile.Reader[numChunks];

        Random random = new Random(42);
        try (FilteredSpillFile.Writer writer = new FilteredSpillFile.Writer(spillDirs)) {
            for (int i = 0; i < numChunks; i++) {
                chunks[i] = new byte[chunkSize];
                random.nextBytes(chunks[i]);
                offsets[i] = writer.write(chunks[i], 0, chunkSize);
                readers[i] = writer.getCurrentFileReader();
            }

            // Verify multiple files were created
            assertThat(writer.getAllFiles().size()).isGreaterThan(1);

            // Verify data correctness across files
            for (int i = 0; i < numChunks; i++) {
                byte[] readBack = new byte[chunkSize];
                readers[i].read(offsets[i], readBack, chunkSize);
                assertThat(readBack).isEqualTo(chunks[i]);
            }

            // Clean up readers
            for (FilteredSpillFile.Reader reader : readers) {
                if (reader != null) {
                    reader.close();
                }
            }
        }
    }

    /** Writer.close() releases file handle even on error. Verify no resource leaks. */
    @Test
    void testCloseReleasesFileHandle() throws Exception {
        String[] spillDirs = {temporaryFolder.toString()};
        FilteredSpillFile.Writer writer = new FilteredSpillFile.Writer(spillDirs);

        // Write some data to open a file
        byte[] data = new byte[] {1, 2, 3};
        writer.write(data, 0, data.length);

        // Close the writer
        writer.close();

        // After close, writing should throw
        assertThatThrownBy(() -> writer.write(data, 0, data.length))
                .isInstanceOf(IllegalStateException.class);
    }

    /** Truncate file, read throws IOException on partial read. */
    @Test
    void testTruncatedFileThrows() throws Exception {
        String[] spillDirs = {temporaryFolder.toString()};
        byte[] data = new byte[1024];
        new Random(42).nextBytes(data);
        long offset;
        Path filePath;

        try (FilteredSpillFile.Writer writer = new FilteredSpillFile.Writer(spillDirs)) {
            offset = writer.write(data, 0, data.length);
            filePath = writer.getAllFiles().get(0);
        }

        // Truncate the file to half the data length
        try (RandomAccessFile raf = new RandomAccessFile(filePath.toFile(), "rw")) {
            raf.setLength(data.length / 2);
        }

        // Reading full length from the truncated file should throw IOException
        try (FilteredSpillFile.Reader reader = new FilteredSpillFile.Reader(filePath)) {
            byte[] readBack = new byte[data.length];
            assertThatThrownBy(() -> reader.read(offset, readBack, data.length))
                    .isInstanceOf(IOException.class);
        }
    }

    /** Verify openSequentialStream reads the correct bytes starting from the given offset. */
    @Test
    void testOpenSequentialStream() throws Exception {
        String[] spillDirs = {temporaryFolder.toString()};
        byte[] data = new byte[256];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) i;
        }

        try (FilteredSpillFile.Writer writer = new FilteredSpillFile.Writer(spillDirs)) {
            long offset = writer.write(data, 0, data.length);

            try (FilteredSpillFile.Reader reader = writer.getCurrentFileReader()) {
                // Open a sequential stream at a mid-file offset and read exactly readLength bytes.
                // Simulates drainSpillEntriesToCheckpoint using one stream per physical file.
                int readOffset = 64;
                int readLength = 128;
                InputStream is = reader.openSequentialStream(offset + readOffset);
                byte[] readBack = new byte[readLength];
                int totalRead = 0;
                while (totalRead < readLength) {
                    int n = is.read(readBack, totalRead, readLength - totalRead);
                    if (n < 0) {
                        break;
                    }
                    totalRead += n;
                }
                assertThat(totalRead).isEqualTo(readLength);

                byte[] expected = new byte[readLength];
                System.arraycopy(data, readOffset, expected, 0, readLength);
                assertThat(readBack).isEqualTo(expected);

                // Stream continues past readLength — verify next byte is correct
                int nextByte = is.read();
                assertThat((byte) nextByte).isEqualTo(data[readOffset + readLength]);
            }
        }
    }

    /** Verify deleteAllFiles removes all spill files. */
    @Test
    void testDeleteAllFiles() throws Exception {
        String[] spillDirs = {temporaryFolder.toString()};
        byte[] data = new byte[64];

        FilteredSpillFile.Writer writer = new FilteredSpillFile.Writer(spillDirs);
        writer.write(data, 0, data.length);
        writer.close();

        // Verify files exist
        for (Path file : writer.getAllFiles()) {
            assertThat(Files.exists(file)).isTrue();
        }

        writer.deleteAllFiles();

        // Verify files deleted
        for (Path file : writer.getAllFiles()) {
            assertThat(Files.exists(file)).isFalse();
        }
    }

    /** Verify constructor throws on empty spillDirs. */
    @Test
    void testEmptySpillDirsThrows() {
        assertThatThrownBy(() -> new FilteredSpillFile.Writer(new String[0]))
                .isInstanceOf(IOException.class);
    }

    /** Verify Entry is immutable and holds correct values. */
    @Test
    void testEntryImmutability() throws Exception {
        InputChannelInfo channelInfo = new InputChannelInfo(0, 1);
        long offset = 42L;
        int length = 100;

        FilteredSpillFile.Entry entry = new FilteredSpillFile.Entry(channelInfo, offset, length);

        assertThat(entry.getChannelInfo()).isSameAs(channelInfo);
        assertThat(entry.getOffset()).isEqualTo(offset);
        assertThat(entry.getLength()).isEqualTo(length);
    }
}
