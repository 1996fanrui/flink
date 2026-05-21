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
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBufAllocator;

import java.nio.ByteBuffer;

/**
 * A sentinel {@link Buffer} that carries the id of the checkpoint that triggered it. Inserted into
 * each channel's {@code recoveredBuffers} queue by {@link RecoveryCheckpointTrigger} at Step 1 of
 * the recovery-checkpoint protocol; consumed by the task thread at Step 2 to demarcate the
 * in-memory portion of recovered state.
 *
 * <p>The sentinel lives only within a single task and is never serialised or transmitted over the
 * network. It must not be confused with {@code CheckpointBarrier}, which is a network event
 * serialised and transmitted across task boundaries.
 *
 * <p>Consumers MUST detect this sentinel via {@code instanceof RecoveryCheckpointBarrier} before
 * calling any other {@link Buffer} method. All methods not required on the dequeue path throw
 * {@link UnsupportedOperationException}. Methods that are called on the dequeue path return
 * harmless defaults: {@link #isBuffer()} returns {@code false}, {@link #getDataType()} returns
 * {@link DataType#NONE}, {@link #recycleBuffer()} is a no-op, and {@link #refCnt()} returns 1.
 */
@Internal
public final class RecoveryCheckpointBarrier implements Buffer {

    private final long checkpointId;

    public RecoveryCheckpointBarrier(long checkpointId) {
        this.checkpointId = checkpointId;
    }

    /**
     * Returns the checkpoint id that triggered this sentinel.
     *
     * @return checkpoint id
     */
    public long getCheckpointId() {
        return checkpointId;
    }

    // --- Methods safe to call on the dequeue path ---

    /** Returns {@code false}: this sentinel is not a data buffer. */
    @Override
    public boolean isBuffer() {
        return false;
    }

    /**
     * Returns {@link DataType#NONE}: no routing or priority semantics apply to this sentinel;
     * consumers must detect it via {@code instanceof} before inspecting the data type.
     */
    @Override
    public DataType getDataType() {
        return DataType.NONE;
    }

    /** No-op: the sentinel holds no pooled memory to recycle. */
    @Override
    public void recycleBuffer() {}

    /** Returns {@code false}: the sentinel is never recycled. */
    @Override
    public boolean isRecycled() {
        return false;
    }

    /** Returns {@code this}: the sentinel needs no reference counting. */
    @Override
    public Buffer retainBuffer() {
        return this;
    }

    /** Returns 1: the sentinel is logically always live. */
    @Override
    public int refCnt() {
        return 1;
    }

    // --- Unsupported methods: not called on the dequeue path ---

    @Override
    public MemorySegment getMemorySegment() {
        throw new UnsupportedOperationException("RecoveryCheckpointBarrier has no memory segment");
    }

    @Override
    public int getMemorySegmentOffset() {
        throw new UnsupportedOperationException("RecoveryCheckpointBarrier has no memory segment");
    }

    @Override
    public BufferRecycler getRecycler() {
        throw new UnsupportedOperationException("RecoveryCheckpointBarrier has no recycler");
    }

    @Override
    public void setRecycler(BufferRecycler bufferRecycler) {
        throw new UnsupportedOperationException("RecoveryCheckpointBarrier has no recycler");
    }

    @Override
    public Buffer readOnlySlice() {
        throw new UnsupportedOperationException("RecoveryCheckpointBarrier cannot be sliced");
    }

    @Override
    public Buffer readOnlySlice(int index, int length) {
        throw new UnsupportedOperationException("RecoveryCheckpointBarrier cannot be sliced");
    }

    @Override
    public int getMaxCapacity() {
        throw new UnsupportedOperationException("RecoveryCheckpointBarrier has no capacity");
    }

    @Override
    public int getReaderIndex() {
        throw new UnsupportedOperationException("RecoveryCheckpointBarrier has no reader index");
    }

    @Override
    public void setReaderIndex(int readerIndex) throws IndexOutOfBoundsException {
        throw new UnsupportedOperationException("RecoveryCheckpointBarrier has no reader index");
    }

    @Override
    public int getSize() {
        throw new UnsupportedOperationException("RecoveryCheckpointBarrier has no size");
    }

    @Override
    public void setSize(int writerIndex) {
        throw new UnsupportedOperationException("RecoveryCheckpointBarrier has no size");
    }

    @Override
    public int readableBytes() {
        throw new UnsupportedOperationException("RecoveryCheckpointBarrier has no readable bytes");
    }

    @Override
    public ByteBuffer getNioBufferReadable() {
        throw new UnsupportedOperationException("RecoveryCheckpointBarrier has no NIO buffer");
    }

    @Override
    public ByteBuffer getNioBuffer(int index, int length) throws IndexOutOfBoundsException {
        throw new UnsupportedOperationException("RecoveryCheckpointBarrier has no NIO buffer");
    }

    @Override
    public void setAllocator(ByteBufAllocator allocator) {
        throw new UnsupportedOperationException("RecoveryCheckpointBarrier has no allocator");
    }

    @Override
    public ByteBuf asByteBuf() {
        throw new UnsupportedOperationException("RecoveryCheckpointBarrier cannot be a ByteBuf");
    }

    @Override
    public boolean isCompressed() {
        throw new UnsupportedOperationException("RecoveryCheckpointBarrier has no compression");
    }

    @Override
    public void setCompressed(boolean isCompressed) {
        throw new UnsupportedOperationException("RecoveryCheckpointBarrier has no compression");
    }

    @Override
    public void setDataType(DataType dataType) {
        throw new UnsupportedOperationException("RecoveryCheckpointBarrier data type is immutable");
    }
}
