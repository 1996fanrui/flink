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
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

/**
 * Writes buffer data to a spill file using a length-prefixed format that includes channel context.
 *
 * <p>Each entry format:
 *
 * <pre>
 * [4 bytes: old subtask index (big-endian int)]
 * [4 bytes: old channel index (big-endian int)]
 * [4 bytes: buffer data length (big-endian int)]
 * [N bytes: buffer data]
 * [1 byte: buffer type flag (0=data, 1=event)]
 * </pre>
 */
class SpillFileWriter implements Closeable {

    private final File file;
    private final FileChannel fileChannel;
    private long bytesWritten;

    SpillFileWriter(File file) throws IOException {
        this.file = file;
        this.fileChannel =
                FileChannel.open(
                        file.toPath(),
                        StandardOpenOption.CREATE,
                        StandardOpenOption.WRITE,
                        StandardOpenOption.TRUNCATE_EXISTING);
        this.bytesWritten = 0;
    }

    /**
     * Writes a buffer's data to the spill file with its channel context.
     *
     * @param buffer the buffer whose data to write
     * @param oldSubtaskIndex the old subtask index for delivery context
     * @param oldChannelIndex the old channel index for delivery context
     */
    void writeBuffer(Buffer buffer, int oldSubtaskIndex, int oldChannelIndex) throws IOException {
        int dataLength = buffer.readableBytes();
        byte typeFlag = buffer.isBuffer() ? (byte) 0 : (byte) 1;

        // Write channel context (old subtask index + old channel index)
        ByteBuffer contextBuf = ByteBuffer.allocate(8);
        contextBuf.putInt(oldSubtaskIndex);
        contextBuf.putInt(oldChannelIndex);
        contextBuf.flip();
        writeAll(contextBuf);

        // Write length prefix
        ByteBuffer lengthBuf = ByteBuffer.allocate(4);
        lengthBuf.putInt(dataLength);
        lengthBuf.flip();
        writeAll(lengthBuf);

        // Write data
        ByteBuffer dataBuf = buffer.getNioBufferReadable();
        writeAll(dataBuf);

        // Write type flag
        ByteBuffer flagBuf = ByteBuffer.allocate(1);
        flagBuf.put(typeFlag);
        flagBuf.flip();
        writeAll(flagBuf);

        bytesWritten += 8 + 4 + dataLength + 1;
    }

    private void writeAll(ByteBuffer buf) throws IOException {
        while (buf.hasRemaining()) {
            fileChannel.write(buf);
        }
    }

    long getBytesWritten() {
        return bytesWritten;
    }

    File getFile() {
        return file;
    }

    @Override
    public void close() throws IOException {
        fileChannel.force(true);
        fileChannel.close();
    }
}
