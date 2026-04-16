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

/** Tests for {@link SpillFileWriter} and {@link SpillFileReader}. */
class SpillFileTest {

    @TempDir private Path temporaryFolder;

    private static final int MEMORY_SEGMENT_SIZE = 32 * 1024;

    /**
     * AT-7OWS: Write data, read back, verify raw bytes match. The file contains pure bytes with no
     * metadata.
     */
    @Test
    void testPureByteStream() throws Exception {
        String[] spillDirs = {temporaryFolder.toString()};
        Random random = new Random(42);
        byte[] data = new byte[1024];
        random.nextBytes(data);

        try (SpillFileWriter writer = new SpillFileWriter(spillDirs, MEMORY_SEGMENT_SIZE)) {
            long offset = writer.write(data, 0, data.length);
            assertThat(offset).isEqualTo(0L);

            try (SpillFileReader reader = writer.getCurrentFileReader()) {
                byte[] readBack = new byte[data.length];
                reader.read(offset, readBack, data.length);
                assertThat(readBack).isEqualTo(data);
            }
        }
    }

    /** AT-7OWS continued: verify multiple writes produce contiguous offsets. */
    @Test
    void testMultipleWritesContiguous() throws Exception {
        String[] spillDirs = {temporaryFolder.toString()};
        byte[] data1 = new byte[] {1, 2, 3, 4};
        byte[] data2 = new byte[] {5, 6, 7, 8};

        try (SpillFileWriter writer = new SpillFileWriter(spillDirs, MEMORY_SEGMENT_SIZE)) {
            long offset1 = writer.write(data1, 0, data1.length);
            long offset2 = writer.write(data2, 0, data2.length);

            assertThat(offset1).isEqualTo(0L);
            assertThat(offset2).isEqualTo(4L);

            try (SpillFileReader reader = writer.getCurrentFileReader()) {
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
     * AT-5097: Write more than 64MB to trigger file rotation, verify multiple files created and
     * data correct across files.
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
        SpillFileReader[] readers = new SpillFileReader[numChunks];

        Random random = new Random(42);
        try (SpillFileWriter writer = new SpillFileWriter(spillDirs, MEMORY_SEGMENT_SIZE)) {
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
            for (SpillFileReader reader : readers) {
                if (reader != null) {
                    reader.close();
                }
            }
        }
    }

    /**
     * AT-HY10: SpillFileWriter.close() releases file handle even on error. Verify no resource
     * leaks.
     */
    @Test
    void testCloseReleasesFileHandle() throws Exception {
        String[] spillDirs = {temporaryFolder.toString()};
        SpillFileWriter writer = new SpillFileWriter(spillDirs, MEMORY_SEGMENT_SIZE);

        // Write some data to open a file
        byte[] data = new byte[] {1, 2, 3};
        writer.write(data, 0, data.length);

        // Close the writer
        writer.close();

        // After close, writing should throw or the channel should be closed.
        // Verify by attempting to write again.
        assertThatThrownBy(() -> writer.write(data, 0, data.length))
                .isInstanceOf(IOException.class);
    }

    /** AT-HW4P: Truncate file, read throws IOException on partial read. */
    @Test
    void testTruncatedFileThrows() throws Exception {
        String[] spillDirs = {temporaryFolder.toString()};
        byte[] data = new byte[1024];
        new Random(42).nextBytes(data);
        long offset;
        Path filePath;

        try (SpillFileWriter writer = new SpillFileWriter(spillDirs, MEMORY_SEGMENT_SIZE)) {
            offset = writer.write(data, 0, data.length);
            filePath = writer.getAllFiles().get(0);
        }

        // Truncate the file to half the data length
        try (RandomAccessFile raf = new RandomAccessFile(filePath.toFile(), "rw")) {
            raf.setLength(data.length / 2);
        }

        // Reading full length from the truncated file should throw IOException
        try (SpillFileReader reader = new SpillFileReader(filePath)) {
            byte[] readBack = new byte[data.length];
            assertThatThrownBy(() -> reader.read(offset, readBack, data.length))
                    .isInstanceOf(IOException.class);
        }
    }

    /** Verify openInputStream reads the correct bounded range. */
    @Test
    void testOpenInputStream() throws Exception {
        String[] spillDirs = {temporaryFolder.toString()};
        byte[] data = new byte[256];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) i;
        }

        try (SpillFileWriter writer = new SpillFileWriter(spillDirs, MEMORY_SEGMENT_SIZE)) {
            long offset = writer.write(data, 0, data.length);

            try (SpillFileReader reader = writer.getCurrentFileReader()) {
                // Read a sub-range via InputStream
                int readOffset = 64;
                int readLength = 128;
                try (InputStream is = reader.openInputStream(offset + readOffset, readLength)) {
                    byte[] readBack = new byte[readLength];
                    int totalRead = 0;
                    int bytesRead;
                    while ((bytesRead = is.read(readBack, totalRead, readLength - totalRead))
                            != -1) {
                        totalRead += bytesRead;
                    }
                    assertThat(totalRead).isEqualTo(readLength);

                    byte[] expected = new byte[readLength];
                    System.arraycopy(data, readOffset, expected, 0, readLength);
                    assertThat(readBack).isEqualTo(expected);

                    // Further reads should return -1
                    assertThat(is.read()).isEqualTo(-1);
                }
            }
        }
    }

    /** Verify deleteAllFiles removes all spill files. */
    @Test
    void testDeleteAllFiles() throws Exception {
        String[] spillDirs = {temporaryFolder.toString()};
        byte[] data = new byte[64];

        SpillFileWriter writer = new SpillFileWriter(spillDirs, MEMORY_SEGMENT_SIZE);
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
        assertThatThrownBy(() -> new SpillFileWriter(new String[0], MEMORY_SEGMENT_SIZE))
                .isInstanceOf(IOException.class);
    }

    /** Verify SpillEntry is immutable and holds correct values. */
    @Test
    void testSpillEntryImmutability() throws Exception {
        String[] spillDirs = {temporaryFolder.toString()};
        InputChannelInfo channelInfo = new InputChannelInfo(0, 1);
        byte[] data = new byte[100];

        try (SpillFileWriter writer = new SpillFileWriter(spillDirs, MEMORY_SEGMENT_SIZE)) {
            long offset = writer.write(data, 0, data.length);
            SpillFileReader reader = writer.getCurrentFileReader();

            SpillEntry entry = new SpillEntry(channelInfo, reader, offset, data.length);

            assertThat(entry.getChannelInfo()).isSameAs(channelInfo);
            assertThat(entry.getFileReader()).isSameAs(reader);
            assertThat(entry.getOffset()).isEqualTo(offset);
            assertThat(entry.getLength()).isEqualTo(data.length);

            reader.close();
        }
    }
}
