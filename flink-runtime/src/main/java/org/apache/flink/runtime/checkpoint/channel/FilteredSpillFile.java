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

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.FileUtils;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 * Spill file I/O for the {@code filterAndRewrite} recovery path. Groups the writer, reader, and
 * per-entry metadata as nested static classes so their tight coupling is visible at a glance.
 */
@Internal
public final class FilteredSpillFile {

    private FilteredSpillFile() {}

    // -------------------------------------------------------------------------
    // Entry
    // -------------------------------------------------------------------------

    /**
     * Immutable metadata for a single spilled buffer entry. References a byte range within a spill
     * file that contains the raw data for one buffer destined for a specific input channel
     * (post-rescaling).
     */
    public static final class Entry {

        private final InputChannelInfo channelInfo;
        private final long offset;
        private final int length;

        public Entry(InputChannelInfo channelInfo, long offset, int length) {
            this.channelInfo = channelInfo;
            this.offset = offset;
            this.length = length;
        }

        public InputChannelInfo getChannelInfo() {
            return channelInfo;
        }

        public long getOffset() {
            return offset;
        }

        public int getLength() {
            return length;
        }
    }

    // -------------------------------------------------------------------------
    // Writer
    // -------------------------------------------------------------------------

    /**
     * Appends raw bytes to spill files via {@link FileChannel}. Supports file rotation at a
     * configurable threshold (64 MB) and round-robin directory selection across multiple spill
     * directories.
     *
     * <p>The writer does NOT call fsync/force, trading durability for throughput. Data is pure
     * bytes with no metadata headers. Files are created lazily on the first write.
     */
    public static class Writer implements Closeable {

        private static final int FILE_ROTATION_THRESHOLD = 64 * 1024 * 1024; // 64MB

        private final String[] spillDirs;
        private int currentDirIndex;
        private FileChannel currentChannel;
        private Path currentFilePath;
        private long currentFileOffset;
        private final List<Path> allFiles;
        private boolean closed;

        /**
         * Creates a new Writer.
         *
         * @param spillDirs directories for writing spill files, obtained from
         *     IOManager.getSpillingDirectoriesPaths()
         * @throws IOException if spillDirs is empty
         */
        public Writer(String[] spillDirs) throws IOException {
            if (spillDirs.length == 0) {
                throw new IOException("Spill directories must not be empty");
            }
            this.spillDirs = spillDirs;
            this.currentDirIndex = 0;
            this.currentFileOffset = 0;
            this.allFiles = new ArrayList<>();
            this.closed = false;
        }

        /**
         * Writes raw bytes to the current spill file.
         *
         * @param data the byte array containing data to write
         * @param offset the start offset in the data array
         * @param length the number of bytes to write
         * @return the file offset where the data was written
         * @throws IOException if writing fails or the writer is closed
         */
        public long write(byte[] data, int offset, int length) throws IOException {
            if (closed) {
                throw new IllegalStateException("FilteredSpillFile.Writer is already closed");
            }

            // Lazy file creation or rotation when threshold is exceeded
            if (currentChannel == null) {
                openNewFile();
            } else if (currentFileOffset > FILE_ROTATION_THRESHOLD) {
                rotateFile();
            }

            long writeOffset = currentFileOffset;

            FileUtils.writeCompletely(currentChannel, ByteBuffer.wrap(data, offset, length));

            currentFileOffset += length;
            return writeOffset;
        }

        /**
         * Returns a reader for the current spill file. The caller is responsible for closing the
         * returned reader.
         *
         * @return a Reader for the current file
         * @throws IOException if no file has been created yet or reader creation fails
         */
        public Reader getCurrentFileReader() throws IOException {
            if (currentFilePath == null) {
                throw new IOException("No spill file has been created yet");
            }
            return new Reader(currentFilePath);
        }

        /**
         * Returns an unmodifiable list of all spill file paths created by this writer. Useful for
         * cleanup and verification.
         */
        public List<Path> getAllFiles() {
            return Collections.unmodifiableList(allFiles);
        }

        @Override
        public void close() throws IOException {
            closed = true;
            try {
                if (currentChannel != null) {
                    currentChannel.close();
                }
            } finally {
                currentChannel = null;
            }
        }

        /** Deletes all spill files created by this writer. Called after drain is complete. */
        public void deleteAllFiles() {
            for (Path file : allFiles) {
                try {
                    Files.deleteIfExists(file);
                } catch (IOException ignored) {
                    // Best effort cleanup
                }
            }
        }

        private void openNewFile() throws IOException {
            String dir = spillDirs[currentDirIndex];
            currentDirIndex = (currentDirIndex + 1) % spillDirs.length;

            Path dirPath = Paths.get(dir);
            Files.createDirectories(dirPath);

            currentFilePath = dirPath.resolve("spill-" + UUID.randomUUID() + ".bin");
            currentChannel =
                    FileChannel.open(
                            currentFilePath,
                            StandardOpenOption.CREATE_NEW,
                            StandardOpenOption.WRITE,
                            StandardOpenOption.READ);
            currentFileOffset = 0;
            allFiles.add(currentFilePath);
        }

        private void rotateFile() throws IOException {
            if (currentChannel != null) {
                currentChannel.close();
            }
            openNewFile();
        }
    }

    // -------------------------------------------------------------------------
    // Reader
    // -------------------------------------------------------------------------

    /**
     * Reads from a spill file via {@link FileChannel} positional reads. Supports both direct byte
     * array reads and bounded {@link InputStream} creation for checkpoint streaming.
     *
     * <p>The reader owns the FileChannel lifecycle; callers must close this reader when done.
     */
    public static class Reader implements Closeable {

        private final FileChannel channel;
        private final Path filePath;

        /**
         * Opens a FileChannel for reading the specified spill file.
         *
         * @param filePath path to the spill file
         * @throws IOException if the file cannot be opened
         */
        public Reader(Path filePath) throws IOException {
            this.filePath = filePath;
            this.channel = FileChannel.open(filePath, StandardOpenOption.READ);
        }

        /**
         * Performs a positional read from the spill file.
         *
         * @param offset byte offset in the file to start reading from
         * @param buffer destination byte array
         * @param length number of bytes to read
         * @throws IOException if a partial read is detected or an I/O error occurs
         */
        public void read(long offset, byte[] buffer, int length) throws IOException {
            ByteBuffer bb = ByteBuffer.wrap(buffer, 0, length);
            int totalRead = 0;
            long position = offset;

            while (bb.hasRemaining()) {
                int bytesRead = channel.read(bb, position);
                if (bytesRead < 0) {
                    throw new IOException(
                            "Truncated spill file: expected "
                                    + length
                                    + " bytes at offset "
                                    + offset
                                    + " but only read "
                                    + totalRead
                                    + " bytes from "
                                    + filePath);
                }
                totalRead += bytesRead;
                position += bytesRead;
            }
        }

        /**
         * Returns an InputStream that reads sequentially from {@code startOffset} to end-of-file.
         * The stream does NOT close the underlying FileChannel when it is closed — the Reader owns
         * the channel lifecycle.
         *
         * <p>Used for checkpoint streaming: the caller opens one stream per physical file and
         * passes it sequentially to each {@link ChannelStateWriter#addInputData} call for that
         * file. Each call reads exactly the number of bytes it is given ({@code dataLength}),
         * advancing the stream position automatically so subsequent calls continue from the correct
         * offset.
         *
         * @param startOffset byte offset in the file at which reading begins
         * @return a sequential InputStream backed by positional FileChannel reads
         */
        public InputStream openSequentialStream(long startOffset) {
            return new SequentialFileChannelInputStream(channel, startOffset);
        }

        @Override
        public void close() throws IOException {
            channel.close();
        }

        /**
         * An InputStream backed by positional reads on a FileChannel. Tracks the current file
         * position internally, advancing it with each read. Does NOT close the underlying
         * FileChannel on close — the Reader owns the channel lifecycle.
         */
        private static class SequentialFileChannelInputStream extends InputStream {

            private final FileChannel channel;
            private long currentPosition;

            SequentialFileChannelInputStream(FileChannel channel, long startOffset) {
                this.channel = channel;
                this.currentPosition = startOffset;
            }

            @Override
            public int read() throws IOException {
                byte[] single = new byte[1];
                int result = read(single, 0, 1);
                if (result == -1) {
                    return -1;
                }
                return single[0] & 0xFF;
            }

            @Override
            public int read(byte[] b, int off, int len) throws IOException {
                if (len == 0) {
                    return 0;
                }
                ByteBuffer bb = ByteBuffer.wrap(b, off, len);
                int bytesRead = channel.read(bb, currentPosition);
                if (bytesRead < 0) {
                    return -1;
                }
                currentPosition += bytesRead;
                return bytesRead;
            }

            @Override
            public void close() {
                // Do NOT close the FileChannel — Reader owns the channel lifecycle
            }
        }
    }
}
