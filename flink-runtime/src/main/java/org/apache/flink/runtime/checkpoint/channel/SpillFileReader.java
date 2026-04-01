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

import org.apache.flink.runtime.io.network.buffer.Buffer;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.StandardOpenOption;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Reads raw buffer byte data from a spill file written by {@link SpillFileWriter}. The file
 * contains only raw bytes with no metadata; all entry boundaries and metadata are maintained in
 * memory by {@link SpillingBufferManager}.
 */
class SpillFileReader implements Closeable {

    private final FileChannel fileChannel;

    SpillFileReader(File file) throws IOException {
        this.fileChannel = FileChannel.open(file.toPath(), StandardOpenOption.READ);
    }

    /**
     * Reads {@code length} bytes from the file into the target buffer's MemorySegment. On success,
     * sets target.setSize(length) and returns true. Returns false on clean EOF (0 bytes read).
     * Throws IOException on partial read. On any exception, seeks back to the position before the
     * read attempt so the entry can be retried.
     *
     * @return true if data was read successfully, false if EOF
     */
    boolean readNextTo(Buffer target, int length) throws IOException {
        long posBeforeRead = fileChannel.position();
        try {
            ByteBuffer dataBuf = target.getMemorySegment().wrap(0, length);
            int totalRead = 0;
            while (dataBuf.hasRemaining()) {
                int read = fileChannel.read(dataBuf);
                if (read == -1) {
                    if (totalRead == 0) {
                        return false;
                    }
                    throw new IOException(
                            "Unexpected end of spill file: expected "
                                    + length
                                    + " bytes but only read "
                                    + totalRead);
                }
                totalRead += read;
            }
            target.setSize(length);
            return true;
        } catch (Exception e) {
            try {
                fileChannel.position(posBeforeRead);
            } catch (IOException seekError) {
                e.addSuppressed(seekError);
            }
            if (e instanceof IOException) {
                throw (IOException) e;
            }
            throw new IOException("Failed to read spill entry", e);
        }
    }

    /**
     * Reads {@code length} bytes from the file and writes them directly to the output stream.
     * Throws IOException on partial read.
     *
     * <p>Partial data may already have been written to the stream on failure, so this method is not
     * retryable.
     */
    void readNextTo(OutputStream out, int length) throws IOException {
        WritableByteChannel channel = Channels.newChannel(out);
        long position = fileChannel.position();
        long transferred = 0;
        while (transferred < length) {
            long n = fileChannel.transferTo(position + transferred, length - transferred, channel);
            if (n == 0) {
                throw new IOException(
                        "Unexpected end of spill file: expected "
                                + length
                                + " bytes but only transferred "
                                + transferred);
            }
            transferred += n;
        }
        fileChannel.position(position + length);
    }

    boolean hasRemaining() throws IOException {
        return fileChannel.position() < fileChannel.size();
    }

    /**
     * Skips forward by the given number of bytes. Used by checkpoint iteration to start reading
     * from a position past already-consumed entries.
     */
    void skipBytes(long bytesToSkip) throws IOException {
        checkArgument(bytesToSkip >= 0, "bytesToSkip must be non-negative: %s", bytesToSkip);
        fileChannel.position(fileChannel.position() + bytesToSkip);
    }

    @Override
    public void close() throws IOException {
        fileChannel.close();
    }
}
