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

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

/**
 * Reads buffer data from a spill file written by {@link SpillFileWriter}. Returns entries one at a
 * time in the order they were written, each including the channel context needed for delivery.
 */
class SpillFileReader implements Closeable {

    private final FileChannel fileChannel;

    SpillFileReader(File file) throws IOException {
        this.fileChannel = FileChannel.open(file.toPath(), StandardOpenOption.READ);
    }

    /**
     * Reads the next entry from the spill file.
     *
     * @return the next SpillEntry, or null if end of file reached
     */
    @Nullable
    SpillEntry readNext() throws IOException {
        // Read 8-byte channel context (subtask index + channel index)
        ByteBuffer contextBuf = ByteBuffer.allocate(8);
        int bytesRead = readAll(contextBuf);
        if (bytesRead < 8) {
            return null; // EOF
        }
        contextBuf.flip();
        int oldSubtaskIndex = contextBuf.getInt();
        int oldChannelIndex = contextBuf.getInt();

        // Read 4-byte length prefix
        ByteBuffer lengthBuf = ByteBuffer.allocate(4);
        readAll(lengthBuf);
        lengthBuf.flip();
        int dataLength = lengthBuf.getInt();

        // Read data
        byte[] data = new byte[dataLength];
        ByteBuffer dataBuf = ByteBuffer.wrap(data);
        readAll(dataBuf);

        // Read 1-byte type flag
        ByteBuffer flagBuf = ByteBuffer.allocate(1);
        readAll(flagBuf);
        flagBuf.flip();
        byte typeFlag = flagBuf.get();

        // Create buffer from data
        MemorySegment segment = MemorySegmentFactory.allocateUnpooledSegment(dataLength);
        segment.put(0, data, 0, dataLength);

        Buffer.DataType dataType =
                (typeFlag == 0) ? Buffer.DataType.DATA_BUFFER : Buffer.DataType.EVENT_BUFFER;
        Buffer buffer =
                new NetworkBuffer(segment, FreeingBufferRecycler.INSTANCE, dataType, dataLength);

        return new SpillEntry(oldSubtaskIndex, oldChannelIndex, buffer);
    }

    private int readAll(ByteBuffer buf) throws IOException {
        int totalRead = 0;
        while (buf.hasRemaining()) {
            int read = fileChannel.read(buf);
            if (read == -1) {
                return totalRead;
            }
            totalRead += read;
        }
        return totalRead;
    }

    boolean hasRemaining() throws IOException {
        return fileChannel.position() < fileChannel.size();
    }

    @Override
    public void close() throws IOException {
        fileChannel.close();
    }

    /** An entry read from a spill file, containing the buffer and its channel context. */
    static class SpillEntry {
        final int oldSubtaskIndex;
        final int oldChannelIndex;
        final Buffer buffer;

        SpillEntry(int oldSubtaskIndex, int oldChannelIndex, Buffer buffer) {
            this.oldSubtaskIndex = oldSubtaskIndex;
            this.oldChannelIndex = oldChannelIndex;
            this.buffer = buffer;
        }
    }
}
