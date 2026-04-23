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

package org.apache.flink.runtime.io.network.partition.consumer;

import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.checkpoint.channel.RecordingChannelStateWriter;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link RecoveredBufferStoreImpl}. */
class RecoveredBufferStoreTest {

    private static final InputChannelInfo DEFAULT_CHANNEL_INFO = new InputChannelInfo(0, 0);

    /**
     * Create store, addBuffer, tryTake, markComplete, isComplete. Verify the full lifecycle of the
     * store.
     */
    @Test
    void testStoreLifecycle() {
        RecoveredBufferStoreImpl store = new RecoveredBufferStoreImpl(DEFAULT_CHANNEL_INFO);

        // Initially empty and not complete
        assertThat(store.isEmpty()).isTrue();
        assertThat(store.isComplete()).isFalse();
        assertThat(store.size()).isEqualTo(0);
        assertThat(store.peekNextDataType()).isEqualTo(Buffer.DataType.NONE);

        // Add a buffer
        NetworkBuffer buffer1 = createBuffer(new byte[] {1, 2, 3, 4});
        store.addBuffer(buffer1);

        assertThat(store.isEmpty()).isFalse();
        assertThat(store.size()).isEqualTo(1);
        assertThat(store.peekNextDataType()).isEqualTo(Buffer.DataType.DATA_BUFFER);

        // Take the buffer
        Buffer taken = store.tryTake();
        assertThat(taken).isNotNull();
        assertThat(store.isEmpty()).isTrue();
        assertThat(store.size()).isEqualTo(0);
        taken.recycleBuffer();

        // Mark complete when empty => isComplete should be true
        store.markComplete();
        assertThat(store.isComplete()).isTrue();

        // tryTake on empty returns null
        assertThat(store.tryTake()).isNull();
    }

    /** Verify markComplete when buffers remain means isComplete is false until drained. */
    @Test
    void testCompleteWithRemainingBuffers() {
        RecoveredBufferStoreImpl store = new RecoveredBufferStoreImpl(DEFAULT_CHANNEL_INFO);

        NetworkBuffer buffer = createBuffer(new byte[] {1, 2, 3});
        store.addBuffer(buffer);
        store.markComplete();

        // Not complete because there are still buffered entries
        assertThat(store.isComplete()).isFalse();

        // Drain the buffer
        Buffer taken = store.tryTake();
        assertThat(taken).isNotNull();
        taken.recycleBuffer();

        // Now complete
        assertThat(store.isComplete()).isTrue();
    }

    /**
     * Checkpoint with ready buffers. Ready buffers should be retained and passed to the
     * ChannelStateWriter.
     */
    @Test
    void testCheckpointWithReadyBuffers() throws Exception {
        RecoveredBufferStoreImpl store = new RecoveredBufferStoreImpl(DEFAULT_CHANNEL_INFO);

        byte[] data = new byte[] {10, 20, 30, 40};
        NetworkBuffer buffer = createBuffer(data);
        store.addBuffer(buffer);

        RecordingChannelStateWriter writer = new RecordingChannelStateWriter();
        long checkpointId = 1L;
        writer.start(checkpointId, null);

        store.checkpoint(writer, checkpointId);

        // Verify the buffer was recorded by the writer
        assertThat(writer.getAddedInput().get(DEFAULT_CHANNEL_INFO)).hasSize(1);

        // The original buffer should still be in the store (retained, not consumed)
        assertThat(store.size()).isEqualTo(1);

        // Clean up: recycle the buffer recorded by writer
        writer.getAddedInput().get(DEFAULT_CHANNEL_INFO).forEach(Buffer::recycleBuffer);
        store.releaseAll();
    }

    /** Concurrent access from two threads. One thread adds buffers and the other takes them. */
    @Test
    void testConcurrentCheckpointAndReplay() throws Exception {
        RecoveredBufferStoreImpl store = new RecoveredBufferStoreImpl(DEFAULT_CHANNEL_INFO);
        int numBuffers = 100;
        CyclicBarrier barrier = new CyclicBarrier(2);
        AtomicReference<Throwable> error = new AtomicReference<>();

        // Producer thread: adds buffers
        Thread producer =
                new Thread(
                        () -> {
                            try {
                                barrier.await();
                                for (int i = 0; i < numBuffers; i++) {
                                    NetworkBuffer buf = createBuffer(new byte[] {(byte) i});
                                    store.addBuffer(buf);
                                }
                                store.markComplete();
                            } catch (Throwable t) {
                                error.set(t);
                            }
                        });

        // Consumer thread: takes buffers
        CountDownLatch consumedAll = new CountDownLatch(1);
        Thread consumer =
                new Thread(
                        () -> {
                            try {
                                barrier.await();
                                int consumed = 0;
                                while (consumed < numBuffers) {
                                    Buffer buf = store.tryTake();
                                    if (buf != null) {
                                        buf.recycleBuffer();
                                        consumed++;
                                    }
                                }
                                consumedAll.countDown();
                            } catch (Throwable t) {
                                error.set(t);
                            }
                        });

        producer.start();
        consumer.start();
        producer.join(10_000);
        consumer.join(10_000);

        assertThat(error.get()).isNull();
        assertThat(store.isComplete()).isTrue();
    }

    /**
     * Simulate store transfer by adding buffers, then taking them in another "context" (simulating
     * conversion). Continue consuming after conversion.
     */
    @Test
    void testConsumptionAfterConversion() {
        RecoveredBufferStoreImpl store = new RecoveredBufferStoreImpl(DEFAULT_CHANNEL_INFO);

        // Add buffers in "recovery" phase
        NetworkBuffer buf1 = createBuffer(new byte[] {1, 2});
        NetworkBuffer buf2 = createBuffer(new byte[] {3, 4});
        NetworkBuffer buf3 = createBuffer(new byte[] {5, 6});
        store.addBuffer(buf1);
        store.addBuffer(buf2);
        store.addBuffer(buf3);
        store.markComplete();

        // Simulate partial consumption before conversion
        Buffer taken1 = store.tryTake();
        assertThat(taken1).isNotNull();
        taken1.recycleBuffer();

        // After conversion, continue consuming remaining buffers
        Buffer taken2 = store.tryTake();
        assertThat(taken2).isNotNull();
        taken2.recycleBuffer();

        Buffer taken3 = store.tryTake();
        assertThat(taken3).isNotNull();
        taken3.recycleBuffer();

        assertThat(store.isEmpty()).isTrue();
        assertThat(store.isComplete()).isTrue();
        assertThat(store.tryTake()).isNull();
    }

    /** Verify releaseAll recycles all buffers and clears state. */
    @Test
    void testReleaseAll() {
        RecoveredBufferStoreImpl store = new RecoveredBufferStoreImpl(DEFAULT_CHANNEL_INFO);

        NetworkBuffer buf1 = createBuffer(new byte[] {1});
        NetworkBuffer buf2 = createBuffer(new byte[] {2});
        store.addBuffer(buf1);
        store.addBuffer(buf2);

        store.releaseAll();

        assertThat(buf1.isRecycled()).isTrue();
        assertThat(buf2.isRecycled()).isTrue();
        assertThat(store.isEmpty()).isTrue();
        assertThat(store.size()).isEqualTo(0);
    }

    /** Verify data-available callback fires when buffer is added to empty store. */
    @Test
    void testDataAvailableCallback() {
        RecoveredBufferStoreImpl store = new RecoveredBufferStoreImpl(DEFAULT_CHANNEL_INFO);
        int[] callbackCount = {0};
        store.setDataAvailableCallback(() -> callbackCount[0]++);

        // Add first buffer: should trigger callback (store was empty)
        store.addBuffer(createBuffer(new byte[] {1}));
        assertThat(callbackCount[0]).isEqualTo(1);

        // Add second buffer: should NOT trigger callback (store was not empty)
        store.addBuffer(createBuffer(new byte[] {2}));
        assertThat(callbackCount[0]).isEqualTo(1);

        // Drain both buffers
        store.tryTake().recycleBuffer();
        store.tryTake().recycleBuffer();

        // Add buffer again to empty store: should trigger callback
        store.addBuffer(createBuffer(new byte[] {3}));
        assertThat(callbackCount[0]).isEqualTo(2);

        store.releaseAll();
    }

    /** Verify pending spill entry count tracking. */
    @Test
    void testPendingCount() {
        RecoveredBufferStoreImpl store = new RecoveredBufferStoreImpl(DEFAULT_CHANNEL_INFO);

        store.incrementPending();

        // Store not empty when pending entries exist
        assertThat(store.isEmpty()).isFalse();

        store.decrementPending();
        assertThat(store.isEmpty()).isTrue();
    }

    // ---------------------------------------------------------------------------
    // Tests for ChannelCheckpointStartedListener
    // ---------------------------------------------------------------------------

    /**
     * Verify that setCheckpointListener registers a listener that is fired during checkpoint()
     * after snapshotting ready buffers. The listener should receive the correct checkpointId and
     * channelInfo.
     */
    @Test
    void testCheckpointCallbackFiredAfterSnapshot() throws Exception {
        InputChannelInfo channelInfo = new InputChannelInfo(0, 0);
        RecoveredBufferStoreImpl store = new RecoveredBufferStoreImpl(channelInfo);

        List<Long> capturedIds = new ArrayList<>();
        List<InputChannelInfo> capturedInfos = new ArrayList<>();

        store.setCheckpointListener(
                (id, info) -> {
                    capturedIds.add(id);
                    capturedInfos.add(info);
                });

        store.addBuffer(createBuffer(new byte[] {1, 2}));

        RecordingChannelStateWriter writer = new RecordingChannelStateWriter();
        long checkpointId = 42L;
        writer.start(checkpointId, null);

        store.checkpoint(writer, checkpointId);

        // Callback must have been fired exactly once with correct args
        assertThat(capturedIds).containsExactly(42L);
        assertThat(capturedInfos).containsExactly(channelInfo);

        // Writer received the ready buffer before callback fired (snapshot happened first)
        assertThat(writer.getAddedInput().get(channelInfo)).hasSize(1);

        writer.getAddedInput().get(channelInfo).forEach(Buffer::recycleBuffer);
        store.releaseAll();
    }

    /**
     * Verify checkpoint() without any ready buffers still fires the
     * ChannelCheckpointStartedListener.
     */
    @Test
    void testCheckpointCallbackFiredEvenWhenNoReadyBuffers() throws Exception {
        InputChannelInfo channelInfo = new InputChannelInfo(1, 2);
        RecoveredBufferStoreImpl store = new RecoveredBufferStoreImpl(channelInfo);

        int[] callCount = {0};
        store.setCheckpointListener((id, info) -> callCount[0]++);

        RecordingChannelStateWriter writer = new RecordingChannelStateWriter();
        writer.start(1L, null);
        store.checkpoint(writer, 1L);

        assertThat(callCount[0]).isEqualTo(1);
    }

    /** Verify no callback is fired if setCheckpointListener was never called. */
    @Test
    void testCheckpointWithNoCallbackSetDoesNotThrow() throws Exception {
        RecoveredBufferStoreImpl store = new RecoveredBufferStoreImpl(DEFAULT_CHANNEL_INFO);
        store.addBuffer(createBuffer(new byte[] {1}));

        RecordingChannelStateWriter writer = new RecordingChannelStateWriter();
        writer.start(1L, null);
        // Should not throw even without a callback registered
        store.checkpoint(writer, 1L);

        writer.getAddedInput().get(DEFAULT_CHANNEL_INFO).forEach(Buffer::recycleBuffer);
        store.releaseAll();
    }

    // ---------------------------------------------------------------------------
    // Tests for setDataAvailableCallback via interface (Item 2)
    // ---------------------------------------------------------------------------

    /**
     * Verify setDataAvailableCallback can be called through the RecoveredBufferStore interface
     * without instanceof casts.
     */
    @Test
    void testSetDataAvailableCallbackViaInterface() {
        RecoveredBufferStore store = new RecoveredBufferStoreImpl(DEFAULT_CHANNEL_INFO);
        int[] callCount = {0};
        // Must compile and run without instanceof check
        store.setDataAvailableCallback(() -> callCount[0]++);

        ((RecoveredBufferStoreImpl) store).addBuffer(createBuffer(new byte[] {1}));
        assertThat(callCount[0]).isEqualTo(1);

        store.releaseAll();
    }

    // ---------------------------------------------------------------------------
    // Tests for RecoveredBufferStore.EMPTY singleton (Item 5)
    // ---------------------------------------------------------------------------

    /** Verify all methods of EMPTY return expected no-op / sentinel values. */
    @Test
    void testEmptySingletonBehavior() throws Exception {
        RecoveredBufferStore empty = RecoveredBufferStore.EMPTY;

        assertThat(empty.tryTake()).isNull();
        assertThat(empty.peekNextDataType()).isEqualTo(Buffer.DataType.NONE);
        assertThat(empty.isEmpty()).isTrue();
        assertThat(empty.isComplete()).isTrue();
        assertThat(empty.size()).isEqualTo(0);
    }

    /** Verify checkpoint() on EMPTY is a no-op and does not write any channel state. */
    @Test
    void testEmptySingletonCheckpointIsNoOp() throws Exception {
        RecoveredBufferStore empty = RecoveredBufferStore.EMPTY;

        RecordingChannelStateWriter writer = new RecordingChannelStateWriter();
        writer.start(1L, null);
        empty.checkpoint(writer, 1L);

        // No data must have been written
        assertThat(writer.getAddedInput().isEmpty()).isTrue();
    }

    /** Verify releaseAll() on EMPTY does not throw. */
    @Test
    void testEmptySingletonReleaseAllIsNoOp() {
        RecoveredBufferStore.EMPTY.releaseAll();
    }

    /** Verify all setter callbacks on EMPTY are no-ops (accept and discard without throwing). */
    @Test
    void testEmptySingletonSettersAreNoOp() {
        RecoveredBufferStore empty = RecoveredBufferStore.EMPTY;

        // Both setters must silently discard
        empty.setCheckpointListener((id, info) -> {});
        empty.setDataAvailableCallback(() -> {});
        // No exception == pass
    }

    private static NetworkBuffer createBuffer(byte[] data) {
        org.apache.flink.core.memory.MemorySegment segment =
                MemorySegmentFactory.allocateUnpooledSegment(data.length);
        segment.put(0, data, 0, data.length);
        NetworkBuffer buffer = new NetworkBuffer(segment, FreeingBufferRecycler.INSTANCE);
        buffer.setSize(data.length);
        return buffer;
    }
}
