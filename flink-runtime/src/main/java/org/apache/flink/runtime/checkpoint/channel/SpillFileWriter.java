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

import java.io.Closeable;
import java.io.IOException;
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
 * Appends raw bytes to spill files via {@link FileChannel}. Supports file rotation at a
 * configurable threshold (64MB) and round-robin directory selection across multiple spill
 * directories.
 *
 * <p>The writer does NOT call fsync/force, trading durability for throughput. Data is pure bytes
 * with no metadata headers. Files are created lazily on the first write.
 */
@Internal
public class SpillFileWriter implements Closeable {

    private static final int FILE_ROTATION_THRESHOLD = 64 * 1024 * 1024; // 64MB

    private final String[] spillDirs;
    private final int memorySegmentSize;
    private int currentDirIndex;
    private FileChannel currentChannel;
    private Path currentFilePath;
    private long currentFileOffset;
    private final List<Path> allFiles;
    private boolean closed;

    /**
     * Creates a new SpillFileWriter.
     *
     * @param spillDirs directories for writing spill files, obtained from
     *     IOManager.getSpillingDirectoriesPaths()
     * @param memorySegmentSize the memory segment size, used as context for callers
     * @throws IOException if spillDirs is empty
     */
    public SpillFileWriter(String[] spillDirs, int memorySegmentSize) throws IOException {
        if (spillDirs.length == 0) {
            throw new IOException("Spill directories must not be empty");
        }
        this.spillDirs = spillDirs;
        this.memorySegmentSize = memorySegmentSize;
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
            throw new IOException("SpillFileWriter is already closed");
        }

        // Lazy file creation or rotation when threshold is exceeded
        if (currentChannel == null) {
            openNewFile();
        } else if (currentFileOffset > FILE_ROTATION_THRESHOLD) {
            rotateFile();
        }

        long writeOffset = currentFileOffset;

        ByteBuffer bb = ByteBuffer.wrap(data, offset, length);
        while (bb.hasRemaining()) {
            currentChannel.write(bb);
        }

        currentFileOffset += length;
        return writeOffset;
    }

    /**
     * Returns a reader for the current spill file. The caller is responsible for closing the
     * returned reader.
     *
     * @return a SpillFileReader for the current file
     * @throws IOException if no file has been created yet or reader creation fails
     */
    public SpillFileReader getCurrentFileReader() throws IOException {
        if (currentFilePath == null) {
            throw new IOException("No spill file has been created yet");
        }
        return new SpillFileReader(currentFilePath);
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
