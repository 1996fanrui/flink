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
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Reads from a spill file via {@link FileChannel} positional reads. Supports both direct byte array
 * reads and bounded {@link InputStream} creation for checkpoint streaming.
 *
 * <p>The reader owns the FileChannel lifecycle; callers must close this reader when done.
 */
@Internal
public class SpillFileReader implements Closeable {

    private final FileChannel channel;
    private final Path filePath;

    /**
     * Opens a FileChannel for reading the specified spill file.
     *
     * @param filePath path to the spill file
     * @throws IOException if the file cannot be opened
     */
    public SpillFileReader(Path filePath) throws IOException {
        this.filePath = filePath;
        this.channel = FileChannel.open(filePath, StandardOpenOption.READ);
    }

    /**
     * Performs a positional read from the spill file.
     *
     * @param offset byte offset in the file to start reading from
     * @param buffer destination byte array
     * @param length number of bytes to read
     * @throws IOException if a partial read is detected (REQ-T5AJ) or an I/O error occurs
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
     * Returns a bounded InputStream that reads exactly {@code length} bytes starting from {@code
     * offset}. The InputStream uses FileChannel positional reads internally and does NOT close the
     * FileChannel when it is closed.
     *
     * <p>This is used for checkpoint streaming: data flows from the spill file directly to the
     * checkpoint DataOutputStream without consuming Network Buffer Pool or heap buffers.
     *
     * @param offset starting byte offset in the file
     * @param length number of bytes the InputStream will produce
     * @return a bounded InputStream
     */
    public InputStream openInputStream(long offset, int length) {
        return new BoundedFileChannelInputStream(channel, offset, length);
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }

    /**
     * A bounded InputStream backed by positional reads on a FileChannel. Returns exactly {@code
     * length} bytes starting from {@code startOffset}, then returns -1.
     *
     * <p>Does NOT close the underlying FileChannel on close — the SpillFileReader owns the channel
     * lifecycle.
     */
    private static class BoundedFileChannelInputStream extends InputStream {

        private final FileChannel channel;
        private long currentPosition;
        private int remaining;

        BoundedFileChannelInputStream(FileChannel channel, long startOffset, int length) {
            this.channel = channel;
            this.currentPosition = startOffset;
            this.remaining = length;
        }

        @Override
        public int read() throws IOException {
            if (remaining <= 0) {
                return -1;
            }
            byte[] single = new byte[1];
            int result = read(single, 0, 1);
            if (result == -1) {
                return -1;
            }
            return single[0] & 0xFF;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (remaining <= 0) {
                return -1;
            }
            int toRead = Math.min(len, remaining);
            ByteBuffer bb = ByteBuffer.wrap(b, off, toRead);
            int bytesRead = channel.read(bb, currentPosition);
            if (bytesRead < 0) {
                throw new IOException(
                        "Unexpected end of spill file at position " + currentPosition);
            }
            currentPosition += bytesRead;
            remaining -= bytesRead;
            return bytesRead;
        }

        @Override
        public void close() {
            // Do NOT close the FileChannel — SpillFileReader owns the channel lifecycle
        }
    }
}
