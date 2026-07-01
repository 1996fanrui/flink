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

import org.apache.flink.shaded.netty4.io.netty.buffer.UnpooledByteBufAllocator;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies how a retained {@link Buffer} reference behaves when read/consumed from another thread.
 *
 * <p>The central question: after {@link Buffer#retainBuffer()} hands a reference to another thread,
 * does that thread's reading mutate what the original holder observes (readableBytes / readerIndex
 * / underlying bytes)?
 *
 * <p>The behavior depends entirely on <em>which</em> read API the consumer uses, because {@code
 * retainBuffer()} only bumps the reference count and returns the very same instance -- both holders
 * share one reader index. The tests below make that distinction explicit.
 */
class BufferRetainConcurrentReadTest {

    private static final int BUFFER_SIZE = 4096;
    private static final int DATA_SIZE = 256;

    /**
     * Fills a segment with a recognizable, position-dependent pattern so any accidental overwrite
     * or shift would be detectable byte-for-byte.
     */
    private static NetworkBuffer newFilledBuffer() {
        MemorySegment seg = MemorySegmentFactory.allocateUnpooledSegment(BUFFER_SIZE);
        byte[] pattern = {(byte) 0xAB, (byte) 0xCD, (byte) 0xEA, (byte) 0xFC};
        for (int i = 0; i < DATA_SIZE; i++) {
            // Blend a fixed marker pattern with an ascending value to make each byte unique-ish.
            seg.put(i, (byte) (pattern[i % pattern.length] ^ (i & 0xFF)));
        }
        NetworkBuffer buffer =
                new NetworkBuffer(seg, MemorySegment::free, Buffer.DataType.DATA_BUFFER, DATA_SIZE);
        // readBytes(int) copies into a freshly allocated ByteBuf, so an allocator must be present.
        buffer.setAllocator(UnpooledByteBufAllocator.DEFAULT);
        return buffer;
    }

    private static byte[] snapshotReadable(Buffer buffer) {
        ByteBuffer nio = buffer.getNioBufferReadable();
        byte[] out = new byte[nio.remaining()];
        nio.get(out);
        return out;
    }

    /**
     * Consumer uses non-mutating read APIs ({@code getNioBufferReadable}). Expectation: the main
     * thread's view is untouched, because each {@code getNioBufferReadable()} returns a fresh NIO
     * buffer with its own position/limit over the shared memory, and never advances the shared
     * reader index.
     */
    @Test
    void asyncNonMutatingReadDoesNotAffectMainThread() throws Exception {
        NetworkBuffer buffer = newFilledBuffer();
        try {
            byte[] before = snapshotReadable(buffer);
            int readableBefore = buffer.readableBytes();
            int readerIndexBefore = buffer.getReaderIndex();

            Buffer retained = buffer.retainBuffer();
            assertThat(buffer.refCnt()).isEqualTo(2);

            ExecutorService executor = Executors.newSingleThreadExecutor();
            try {
                CompletableFuture<Long> checksum = new CompletableFuture<>();
                executor.submit(
                        () -> {
                            try {
                                // Fully consume via a fresh NIO buffer (does not move reader
                                // index),
                                // plus a read-only slice consumed to its end.
                                ByteBuffer nio = retained.getNioBufferReadable();
                                long sum = 0;
                                while (nio.hasRemaining()) {
                                    sum += nio.get() & 0xFF;
                                }
                                Buffer slice = retained.readOnlySlice();
                                ByteBuffer sliceNio = slice.getNioBufferReadable();
                                while (sliceNio.hasRemaining()) {
                                    sum += sliceNio.get() & 0xFF;
                                }
                                checksum.complete(sum);
                            } catch (Throwable t) {
                                checksum.completeExceptionally(t);
                            }
                        });
                // Propagate any async failure.
                checksum.get(30, TimeUnit.SECONDS);
            } finally {
                executor.shutdown();
                executor.awaitTermination(30, TimeUnit.SECONDS);
            }

            // Main thread's view must be byte-for-byte identical.
            assertThat(buffer.readableBytes()).isEqualTo(readableBefore);
            assertThat(buffer.getReaderIndex()).isEqualTo(readerIndexBefore);
            assertThat(snapshotReadable(buffer)).containsExactly(before);

            retained.recycleBuffer();
        } finally {
            buffer.recycleBuffer();
        }
    }

    /**
     * Control case: consumer uses a reader-index-mutating API ({@link Buffer#readBytes(int)}) on
     * the <em>same</em> retained instance. Because {@code retainBuffer()} returns the same object
     * with one shared reader index, this DOES advance the index the main thread sees.
     *
     * <p>This pins down the only condition under which cross-thread reading corrupts the original
     * holder's view: sharing a single mutable reader index rather than using per-holder views.
     */
    @Test
    void asyncMutatingReadOnSharedInstanceAffectsMainThread() throws Exception {
        NetworkBuffer buffer = newFilledBuffer();
        try {
            int readableBefore = buffer.readableBytes();
            assertThat(readableBefore).isEqualTo(DATA_SIZE);

            Buffer retained = buffer.retainBuffer();

            ExecutorService executor = Executors.newSingleThreadExecutor();
            try {
                CompletableFuture<Void> done = new CompletableFuture<>();
                executor.submit(
                        () -> {
                            try {
                                // readBytes moves the shared reader index forward.
                                retained.asByteBuf().readBytes(DATA_SIZE);
                                done.complete(null);
                            } catch (Throwable t) {
                                done.completeExceptionally(t);
                            }
                        });
                done.get(30, TimeUnit.SECONDS);
            } finally {
                executor.shutdown();
                executor.awaitTermination(30, TimeUnit.SECONDS);
            }

            // The shared reader index was advanced by the other thread, so the main thread now sees
            // zero readable bytes -- demonstrating interference only via the shared mutable index.
            assertThat(buffer.getReaderIndex()).isEqualTo(DATA_SIZE);
            assertThat(buffer.readableBytes()).isEqualTo(0);

            retained.recycleBuffer();
        } finally {
            buffer.recycleBuffer();
        }
    }

    /**
     * Guards against a subtle interpretation: even the non-mutating path shares underlying bytes.
     * This confirms the async consumer does not <em>write</em> through its view -- the bytes stay
     * intact.
     */
    @Test
    void asyncReadOnlySliceDoesNotModifyUnderlyingBytes()
            throws InterruptedException, ExecutionException {
        NetworkBuffer buffer = newFilledBuffer();
        try {
            byte[] before = snapshotReadable(buffer);
            Buffer retained = buffer.retainBuffer();

            ExecutorService executor = Executors.newSingleThreadExecutor();
            try {
                CompletableFuture<Void> done = new CompletableFuture<>();
                executor.submit(
                        () -> {
                            try {
                                Buffer slice = retained.readOnlySlice();
                                // Drain the slice's own reader index fully.
                                slice.asByteBuf().readBytes(slice.readableBytes());
                                done.complete(null);
                            } catch (Throwable t) {
                                done.completeExceptionally(t);
                            }
                        });
                done.get();
            } finally {
                executor.shutdown();
                executor.awaitTermination(30, TimeUnit.SECONDS);
            }

            // readOnlySlice has its own indices; the parent's reader index and bytes are unchanged.
            assertThat(buffer.getReaderIndex()).isEqualTo(0);
            assertThat(buffer.readableBytes()).isEqualTo(DATA_SIZE);
            assertThat(snapshotReadable(buffer)).containsExactly(before);

            retained.recycleBuffer();
        } finally {
            buffer.recycleBuffer();
        }
    }
}
