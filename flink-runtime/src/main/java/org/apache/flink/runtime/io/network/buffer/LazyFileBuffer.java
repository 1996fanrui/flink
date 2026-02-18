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

package org.apache.flink.runtime.io.network.buffer;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBufAllocator;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * A lazy file-backed buffer implementation that writes data to a temporary file during the write
 * phase and loads the data into memory only when first accessed.
 *
 * <p>This buffer is designed to reduce memory footprint during checkpoint state recovery by
 * deferring memory allocation until the data is actually needed.
 *
 * <p>The lifecycle of this buffer:
 *
 * <ol>
 *   <li><b>Write phase:</b> Data is written directly to a temp file via FileChannel.
 *   <li><b>Read phase:</b> First call to {@link #getMemorySegment()} loads data from file into an
 *       unpooled off-heap memory segment.
 *   <li><b>Recycle phase:</b> Deletes temp file and releases the memory segment.
 * </ol>
 */
public class LazyFileBuffer implements Buffer {

    /** The temporary file for storing buffer data. */
    private final File file;

    /** The maximum capacity of this buffer. */
    private final int capacity;

    /** The file channel for writing data. */
    private FileChannel fileChannel;

    /** The random access file handle. */
    private RandomAccessFile randomAccessFile;

    /** The current size of written data. */
    private int size;

    /** The reader index for reading data. */
    private int readerIndex;

    /** The lazily loaded memory segment. */
    private MemorySegment memorySegment;

    /** The network buffer wrapper for ByteBuf access. */
    private NetworkBuffer networkBuffer;

    /** The data type of this buffer. */
    private DataType dataType = DataType.DATA_BUFFER;

    /** Whether this buffer is compressed. */
    private boolean isCompressed;

    /** The reference count for this buffer. */
    private int referenceCount = 1;

    /** Whether this buffer has been recycled. */
    private boolean isRecycled;

    /** The buffer allocator for netty. */
    private ByteBufAllocator allocator;

    /** The buffer recycler. */
    private BufferRecycler recycler;

    /**
     * Creates a new LazyFileBuffer.
     *
     * @param file the temporary file to store buffer data
     * @param capacity the maximum capacity of this buffer
     */
    public LazyFileBuffer(File file, int capacity) {
        this.file = file;
        this.capacity = capacity;
        this.size = 0;
        this.readerIndex = 0;
        this.recycler = FreeingBufferRecycler.INSTANCE;
    }

    /**
     * Writes bytes from the input stream to the file.
     *
     * @param inputStream the input stream to read from
     * @param length the number of bytes to write
     * @return the number of bytes actually written
     * @throws IOException if an I/O error occurs
     */
    public int writeBytes(InputStream inputStream, int length) throws IOException {
        ensureFileChannelOpen();

        byte[] buffer = new byte[length];
        int bytesRead = inputStream.read(buffer, 0, length);
        if (bytesRead > 0) {
            ByteBuffer byteBuffer = ByteBuffer.wrap(buffer, 0, bytesRead);
            while (byteBuffer.hasRemaining()) {
                fileChannel.write(byteBuffer);
            }
            size += bytesRead;
        }
        return bytesRead > 0 ? bytesRead : 0;
    }

    /**
     * Finishes writing to the file. This ensures all data is flushed to disk.
     *
     * @throws IOException if an I/O error occurs
     */
    public void finishWriting() throws IOException {
        if (fileChannel != null && fileChannel.isOpen()) {
            fileChannel.force(true);
        }
    }

    /** Ensures the file channel is open for writing. */
    private void ensureFileChannelOpen() throws IOException {
        if (randomAccessFile == null) {
            randomAccessFile = new RandomAccessFile(file, "rw");
            fileChannel = randomAccessFile.getChannel();
        }
    }

    /**
     * Lazily loads data from the file into memory. If already loaded, returns the existing memory
     * segment.
     */
    private void loadFromFile() throws IOException {
        if (memorySegment != null) {
            return;
        }

        // Allocate unpooled heap segment with exact size to minimize memory usage.
        // Use at least 1 byte to avoid creating an empty segment.
        int allocationSize = Math.max(size, 1);
        memorySegment = MemorySegmentFactory.allocateUnpooledSegment(allocationSize);

        if (size > 0 && fileChannel != null) {
            // Read data from file into the memory segment
            ByteBuffer byteBuffer = memorySegment.wrap(0, size);
            fileChannel.position(0);
            while (byteBuffer.hasRemaining()) {
                int read = fileChannel.read(byteBuffer);
                if (read == -1) {
                    break;
                }
            }
        }

        // Create a NetworkBuffer wrapper for ByteBuf access
        networkBuffer =
                new NetworkBuffer(memorySegment, FreeingBufferRecycler.INSTANCE, dataType, size);
        networkBuffer.setCompressed(isCompressed);
        if (allocator != null) {
            networkBuffer.setAllocator(allocator);
        }
    }

    @Override
    public boolean isBuffer() {
        return dataType.isBuffer();
    }

    @Override
    public MemorySegment getMemorySegment() {
        try {
            loadFromFile();
        } catch (IOException e) {
            throw new RuntimeException("Failed to load buffer from file", e);
        }
        return memorySegment;
    }

    @Override
    public int getMemorySegmentOffset() {
        return 0;
    }

    @Override
    public BufferRecycler getRecycler() {
        return recycler;
    }

    @Override
    public void setRecycler(BufferRecycler bufferRecycler) {
        this.recycler = bufferRecycler;
    }

    @Override
    public void recycleBuffer() {
        referenceCount--;
        if (referenceCount == 0) {
            isRecycled = true;
            // Close file channel and delete file
            try {
                if (fileChannel != null) {
                    fileChannel.close();
                    fileChannel = null;
                }
                if (randomAccessFile != null) {
                    randomAccessFile.close();
                    randomAccessFile = null;
                }
            } catch (IOException e) {
                // Ignore close errors
            }

            // Delete the temp file
            if (file.exists()) {
                file.delete();
            }

            // Recycle the network buffer (which will free the memory segment)
            if (networkBuffer != null) {
                networkBuffer.recycleBuffer();
                networkBuffer = null;
                memorySegment = null;
            } else if (memorySegment != null) {
                memorySegment.free();
                memorySegment = null;
            }
        }
    }

    @Override
    public boolean isRecycled() {
        return isRecycled;
    }

    @Override
    public Buffer retainBuffer() {
        referenceCount++;
        return this;
    }

    @Override
    public Buffer readOnlySlice() {
        return readOnlySlice(readerIndex, readableBytes());
    }

    @Override
    public Buffer readOnlySlice(int index, int length) {
        // Ensure data is loaded first
        getMemorySegment();
        checkState(
                !isCompressed || index + length != size,
                "Unable to partially slice a compressed buffer.");
        return new ReadOnlySlicedNetworkBuffer(networkBuffer, index, length, isCompressed);
    }

    @Override
    public int getMaxCapacity() {
        return capacity;
    }

    @Override
    public int getReaderIndex() {
        return readerIndex;
    }

    @Override
    public void setReaderIndex(int readerIndex) throws IndexOutOfBoundsException {
        if (readerIndex < 0 || readerIndex > size) {
            throw new IndexOutOfBoundsException(
                    "readerIndex: "
                            + readerIndex
                            + " (expected: 0 <= readerIndex <= "
                            + size
                            + ")");
        }
        this.readerIndex = readerIndex;
        if (networkBuffer != null) {
            networkBuffer.setReaderIndex(readerIndex);
        }
    }

    @Override
    public int getSize() {
        return size;
    }

    @Override
    public void setSize(int writerIndex) {
        if (writerIndex < readerIndex || writerIndex > capacity) {
            throw new IndexOutOfBoundsException(
                    "writerIndex: "
                            + writerIndex
                            + " (expected: "
                            + readerIndex
                            + " <= writerIndex <= "
                            + capacity
                            + ")");
        }
        this.size = writerIndex;
        if (networkBuffer != null) {
            networkBuffer.setSize(writerIndex);
        }
    }

    @Override
    public int readableBytes() {
        return size - readerIndex;
    }

    @Override
    public ByteBuffer getNioBufferReadable() {
        getMemorySegment();
        networkBuffer.setReaderIndex(readerIndex);
        return networkBuffer.getNioBufferReadable();
    }

    @Override
    public ByteBuffer getNioBuffer(int index, int length) throws IndexOutOfBoundsException {
        getMemorySegment();
        return networkBuffer.getNioBuffer(index, length);
    }

    @Override
    public void setAllocator(ByteBufAllocator allocator) {
        this.allocator = allocator;
        if (networkBuffer != null) {
            networkBuffer.setAllocator(allocator);
        }
    }

    @Override
    public ByteBuf asByteBuf() {
        getMemorySegment();
        return networkBuffer.asByteBuf();
    }

    @Override
    public boolean isCompressed() {
        return isCompressed;
    }

    @Override
    public void setCompressed(boolean isCompressed) {
        this.isCompressed = isCompressed;
        if (networkBuffer != null) {
            networkBuffer.setCompressed(isCompressed);
        }
    }

    @Override
    public DataType getDataType() {
        return dataType;
    }

    @Override
    public void setDataType(DataType dataType) {
        this.dataType = dataType;
        if (networkBuffer != null) {
            networkBuffer.setDataType(dataType);
        }
    }

    @Override
    public int refCnt() {
        return referenceCount;
    }
}
