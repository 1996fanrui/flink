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

import org.apache.flink.shaded.guava33.com.google.common.collect.LinkedListMultimap;
import org.apache.flink.shaded.guava33.com.google.common.collect.ListMultimap;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link RecoveredBufferStoreImpl}. */
class RecoveredBufferStoreTest {

    /**
     * AT-IAMJ: Create store, addBuffer, tryTake, markComplete, isComplete. Verify the full
     * lifecycle of the store.
     */
    @Test
    void testStoreLifecycle() {
        RecoveredBufferStoreImpl store = new RecoveredBufferStoreImpl();

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
        RecoveredBufferStoreImpl store = new RecoveredBufferStoreImpl();

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
     * AT-CTTS: Checkpoint with ready buffers. Ready buffers should be retained and passed to the
     * ChannelStateWriter.
     */
    @Test
    void testCheckpointWithReadyBuffers() throws Exception {
        RecoveredBufferStoreImpl store = new RecoveredBufferStoreImpl();
        InputChannelInfo channelInfo = new InputChannelInfo(0, 0);

        byte[] data = new byte[] {10, 20, 30, 40};
        NetworkBuffer buffer = createBuffer(data);
        store.addBuffer(buffer);

        RecordingChannelStateWriter writer = new RecordingChannelStateWriter();
        long checkpointId = 1L;
        writer.start(checkpointId, null);

        store.checkpoint(writer, checkpointId, channelInfo);

        // Verify the buffer was recorded by the writer
        assertThat(writer.getAddedInput().get(channelInfo)).hasSize(1);

        // The original buffer should still be in the store (retained, not consumed)
        assertThat(store.size()).isEqualTo(1);

        // Clean up: recycle the buffer recorded by writer
        writer.getAddedInput().get(channelInfo).forEach(Buffer::recycleBuffer);
        store.releaseAll();
    }

    /**
     * AT-N3YQ: Concurrent access from two threads. One thread adds buffers and the other takes
     * them.
     */
    @Test
    void testConcurrentCheckpointAndReplay() throws Exception {
        RecoveredBufferStoreImpl store = new RecoveredBufferStoreImpl();
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
     * AT-OOJG: Simulate store transfer by adding buffers, then taking them in another "context"
     * (simulating conversion). Continue consuming after conversion.
     */
    @Test
    void testConsumptionAfterConversion() {
        RecoveredBufferStoreImpl store = new RecoveredBufferStoreImpl();

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
        RecoveredBufferStoreImpl store = new RecoveredBufferStoreImpl();

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

    /** Verify notification callback fires when buffer is added to empty store. */
    @Test
    void testNotificationCallback() {
        RecoveredBufferStoreImpl store = new RecoveredBufferStoreImpl();
        int[] callbackCount = {0};
        store.setNotificationCallback(() -> callbackCount[0]++);

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
        RecoveredBufferStoreImpl store = new RecoveredBufferStoreImpl();

        store.incrementPending();

        // Store not empty when pending entries exist
        assertThat(store.isEmpty()).isFalse();

        store.decrementPending();
        assertThat(store.isEmpty()).isTrue();
    }

    /**
     * Verify that checkpoint() streams pending spill entries via the ChannelStateWriter streaming
     * overload.
     */
    @Test
    void testCheckpointWithPendingSpillEntries() throws Exception {
        RecoveredBufferStoreImpl store = new RecoveredBufferStoreImpl();
        InputChannelInfo channelInfo = new InputChannelInfo(0, 0);
        String[] spillDirs = {temporaryFolder.toString()};

        try (SpillFileWriter spillWriter = new SpillFileWriter(spillDirs, MEMORY_SEGMENT_SIZE)) {
            // Write two spill entries
            byte[] data1 = new byte[] {10, 20, 30, 40};
            long offset1 = spillWriter.write(data1, 0, data1.length);
            SpillFileReader reader1 = spillWriter.getCurrentFileReader();
            SpillEntry entry1 = new SpillEntry(channelInfo, reader1, offset1, data1.length);
            store.addPendingSpillEntry(entry1);

            byte[] data2 = new byte[] {50, 60, 70, 80, 90};
            long offset2 = spillWriter.write(data2, 0, data2.length);
            SpillFileReader reader2 = spillWriter.getCurrentFileReader();
            SpillEntry entry2 = new SpillEntry(channelInfo, reader2, offset2, data2.length);
            store.addPendingSpillEntry(entry2);

            // Create a recording writer that captures streaming data
            StreamRecordingChannelStateWriter writer = new StreamRecordingChannelStateWriter();
            long checkpointId = 1L;
            writer.start(checkpointId, null);

            store.checkpoint(writer, checkpointId, channelInfo);

            // Verify two streaming writes were recorded
            assertThat(writer.getStreamedInputData().get(channelInfo)).hasSize(2);

            // Verify data content matches
            assertThat(writer.getStreamedInputData().get(channelInfo).get(0)).isEqualTo(data1);
            assertThat(writer.getStreamedInputData().get(channelInfo).get(1)).isEqualTo(data2);

            reader1.close();
            // reader2 is the same file reader as reader1 (same file), so no separate close needed
        }

        store.releaseAll();
    }

    private static NetworkBuffer createBuffer(byte[] data) {
        org.apache.flink.core.memory.MemorySegment segment =
                MemorySegmentFactory.allocateUnpooledSegment(data.length);
        segment.put(0, data, 0, data.length);
        NetworkBuffer buffer = new NetworkBuffer(segment, FreeingBufferRecycler.INSTANCE);
        buffer.setSize(data.length);
        return buffer;
    }

    /**
     * A ChannelStateWriter that extends RecordingChannelStateWriter and additionally captures
     * streaming (InputStream) data passed via the streaming overload of addInputData.
     */
    private static class StreamRecordingChannelStateWriter extends RecordingChannelStateWriter {

        private final ListMultimap<InputChannelInfo, byte[]> streamedInputData =
                LinkedListMultimap.create();

        @Override
        public void addInputData(
                long checkpointId,
                InputChannelInfo info,
                int startSeqNum,
                java.io.InputStream data,
                int dataLength) {
            try {
                byte[] bytes = new byte[dataLength];
                int offset = 0;
                while (offset < dataLength) {
                    int read = data.read(bytes, offset, dataLength - offset);
                    if (read < 0) {
                        break;
                    }
                    offset += read;
                }
                streamedInputData.put(info, bytes);
            } catch (java.io.IOException e) {
                throw new RuntimeException(e);
            }
        }

        ListMultimap<InputChannelInfo, byte[]> getStreamedInputData() {
            return streamedInputData;
        }
    }
}
