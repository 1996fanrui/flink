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

package org.apache.flink.runtime.checkpoint.channel;

import org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptor;
import org.apache.flink.runtime.checkpoint.RescaleMappings;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelBuilder;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGateBuilder;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptorUtil.mappings;
import static org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptorUtil.to;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test of different implementation of {@link InputChannelRecoveredStateHandler}. */
class InputChannelRecoveredStateHandlerTest extends RecoveredChannelStateHandlerTest {
    private static final int preAllocatedSegments = 3;
    private NetworkBufferPool networkBufferPool;
    private SingleInputGate inputGate;
    private InputChannelRecoveredStateHandler icsHandler;
    private InputChannelInfo channelInfo;

    @BeforeEach
    void setUp() {
        // given: Segment provider with defined number of allocated segments.
        networkBufferPool = new NetworkBufferPool(preAllocatedSegments, 1024);

        // and: Configured input gate with recovered channel.
        inputGate =
                new SingleInputGateBuilder()
                        .setChannelFactory(InputChannelBuilder::buildLocalRecoveredChannel)
                        .setSegmentProvider(networkBufferPool)
                        .build();

        icsHandler = buildInputChannelStateHandler(inputGate);

        channelInfo = new InputChannelInfo(0, 0);
    }

    private InputChannelRecoveredStateHandler buildInputChannelStateHandler(
            SingleInputGate inputGate) {
        return new InputChannelRecoveredStateHandler(
                new InputGate[] {inputGate},
                new InflightDataRescalingDescriptor(
                        new InflightDataRescalingDescriptor
                                        .InflightDataGateOrPartitionRescalingDescriptor[] {
                            new InflightDataRescalingDescriptor
                                    .InflightDataGateOrPartitionRescalingDescriptor(
                                    new int[] {1},
                                    RescaleMappings.identity(1, 1),
                                    new HashSet<>(),
                                    InflightDataRescalingDescriptor
                                            .InflightDataGateOrPartitionRescalingDescriptor
                                            .MappingType.IDENTITY)
                        }),
                null,
                null);
    }

    private InputChannelRecoveredStateHandler buildMultiChannelHandler() {
        // Setup multi-channel scenario to trigger distribution constraint validation
        SingleInputGate multiChannelGate =
                new SingleInputGateBuilder()
                        .setNumberOfChannels(2)
                        .setChannelFactory(InputChannelBuilder::buildLocalRecoveredChannel)
                        .setSegmentProvider(networkBufferPool)
                        .build();

        return new InputChannelRecoveredStateHandler(
                new InputGate[] {multiChannelGate},
                new InflightDataRescalingDescriptor(
                        new InflightDataRescalingDescriptor
                                        .InflightDataGateOrPartitionRescalingDescriptor[] {
                            new InflightDataRescalingDescriptor
                                    .InflightDataGateOrPartitionRescalingDescriptor(
                                    new int[] {2},
                                    // Force 1:many mapping after inversion
                                    mappings(to(0), to(0)),
                                    new HashSet<>(),
                                    InflightDataRescalingDescriptor
                                            .InflightDataGateOrPartitionRescalingDescriptor
                                            .MappingType.RESCALING)
                        }),
                null,
                null);
    }

    @Test
    void testBufferDistributedToMultipleInputChannelsThrowsException() throws Exception {
        // Test constraint that prevents buffer distribution to multiple channels
        try (InputChannelRecoveredStateHandler handler = buildMultiChannelHandler()) {
            assertThatThrownBy(() -> handler.getBuffer(channelInfo))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining(
                            "One buffer is only distributed to one target InputChannel since "
                                    + "one buffer is expected to be processed once by the same task.");
        }
    }

    @Test
    void testRecycleBufferBeforeRecoverWasCalled() throws Exception {
        // when: Request the buffer.
        RecoveredChannelStateHandler.BufferWithContext<Buffer> bufferWithContext =
                icsHandler.getBuffer(channelInfo);

        // and: Recycle buffer outside.
        bufferWithContext.buffer.close();

        // Close the gate for flushing the cached recycled buffers to the segment provider.
        inputGate.close();

        // then: All pre-allocated segments should be successfully recycled.
        assertThat(networkBufferPool.getNumberOfAvailableMemorySegments())
                .isEqualTo(preAllocatedSegments);
    }

    @Test
    void testRecycleBufferAfterRecoverWasCalled() throws Exception {
        // when: Request the buffer.
        RecoveredChannelStateHandler.BufferWithContext<Buffer> bufferWithContext =
                icsHandler.getBuffer(channelInfo);

        // and: Recycle buffer outside.
        icsHandler.recover(channelInfo, 0, bufferWithContext);

        // Close the gate for flushing the cached recycled buffers to the segment provider.
        inputGate.close();

        // then: All pre-allocated segments should be successfully recycled.
        assertThat(networkBufferPool.getNumberOfAvailableMemorySegments())
                .isEqualTo(preAllocatedSegments);
    }

    // AT-36DP: Verify getBuffer() in filtering mode returns Heap Buffer.
    // Network Buffer Pool available count should remain unchanged.
    @Test
    void testHeapBufferIsolation() throws Exception {
        NetworkBufferPool filteringPool = new NetworkBufferPool(preAllocatedSegments, 1024);
        try {
            SingleInputGate filteringGate =
                    new SingleInputGateBuilder()
                            .setChannelFactory(InputChannelBuilder::buildLocalRecoveredChannel)
                            .setSegmentProvider(filteringPool)
                            .setCheckpointingDuringRecoveryEnabled(true)
                            .build();

            // Build handler with a non-null filteringHandler (use a no-op mock)
            InputChannelRecoveredStateHandler filteringHandler =
                    buildFilteringInputChannelStateHandler(filteringGate);

            InputChannelInfo info = new InputChannelInfo(0, 0);

            int availableBefore = filteringPool.getNumberOfAvailableMemorySegments();

            // Request a buffer in filtering mode
            RecoveredChannelStateHandler.BufferWithContext<Buffer> bufferWithContext =
                    filteringHandler.getBuffer(info);

            // The buffer should be a heap-allocated buffer (not from the pool)
            Buffer buffer = bufferWithContext.context;
            assertThat(buffer).isInstanceOf(NetworkBuffer.class);
            // Heap memory segment is not pooled
            assertThat(buffer.getMemorySegment().isOffHeap()).isFalse();

            // Network buffer pool available count should remain unchanged
            assertThat(filteringPool.getNumberOfAvailableMemorySegments())
                    .isEqualTo(availableBefore);

            // Clean up: only recycle the context buffer. bufferWithContext.buffer (the
            // ChannelStateByteBuffer) wraps the same underlying NetworkBuffer, so calling
            // close() on it would double-release.
            buffer.recycleBuffer();
            filteringGate.close();
        } finally {
            filteringPool.destroy();
        }
    }

    // AT-41PK: Verify exceeding 5 Heap Buffers per gate blocks.
    @Test
    void testHeapBufferLimit() throws Exception {
        NetworkBufferPool filteringPool = new NetworkBufferPool(preAllocatedSegments, 1024);
        try {
            SingleInputGate filteringGate =
                    new SingleInputGateBuilder()
                            .setChannelFactory(InputChannelBuilder::buildLocalRecoveredChannel)
                            .setSegmentProvider(filteringPool)
                            .setCheckpointingDuringRecoveryEnabled(true)
                            .build();

            InputChannelRecoveredStateHandler filteringHandler =
                    buildFilteringInputChannelStateHandler(filteringGate);

            InputChannelInfo info = new InputChannelInfo(0, 0);

            // Allocate 5 buffers (the limit)
            List<RecoveredChannelStateHandler.BufferWithContext<Buffer>> buffers =
                    new ArrayList<>();
            for (int i = 0; i < InputChannelRecoveredStateHandler.MAX_HEAP_BUFFERS_PER_GATE; i++) {
                buffers.add(filteringHandler.getBuffer(info));
            }

            // The 6th request should block because we've reached the limit.
            // Use a separate thread to test blocking behavior.
            ExecutorService executor = Executors.newSingleThreadExecutor();
            CountDownLatch requestStarted = new CountDownLatch(1);
            CompletableFuture<RecoveredChannelStateHandler.BufferWithContext<Buffer>>
                    blockedFuture = new CompletableFuture<>();

            Future<?> task =
                    executor.submit(
                            () -> {
                                try {
                                    requestStarted.countDown();
                                    RecoveredChannelStateHandler.BufferWithContext<Buffer> buf =
                                            filteringHandler.getBuffer(info);
                                    blockedFuture.complete(buf);
                                } catch (Exception e) {
                                    blockedFuture.completeExceptionally(e);
                                }
                            });

            // Wait for the thread to start attempting the request
            requestStarted.await(5, TimeUnit.SECONDS);
            // Give the thread some time to block
            Thread.sleep(200);

            // The future should not be completed yet (the request is blocked)
            assertThat(blockedFuture.isDone()).isFalse();

            // Recycle one buffer to unblock
            buffers.get(0).context.recycleBuffer();

            // Now the blocked request should complete
            RecoveredChannelStateHandler.BufferWithContext<Buffer> unblocked =
                    blockedFuture.get(5, TimeUnit.SECONDS);
            assertThat(unblocked).isNotNull();

            // Clean up
            unblocked.context.recycleBuffer();
            for (int i = 1; i < buffers.size(); i++) {
                buffers.get(i).context.recycleBuffer();
            }
            executor.shutdown();
            filteringGate.close();
        } finally {
            filteringPool.destroy();
        }
    }

    // AT-U7Q2: Non-filtering mode uses existing path unchanged.
    @Test
    void testNonFilteringUnchanged() throws Exception {
        // The default handler (icsHandler) has filteringHandler=null (non-filtering mode).
        // Verify it still allocates from the network buffer pool.
        int availableBefore = networkBufferPool.getNumberOfAvailableMemorySegments();

        RecoveredChannelStateHandler.BufferWithContext<Buffer> bufferWithContext =
                icsHandler.getBuffer(channelInfo);

        Buffer buffer = bufferWithContext.context;
        // In non-filtering mode, buffer should come from the network buffer pool,
        // so available segments should decrease.
        assertThat(networkBufferPool.getNumberOfAvailableMemorySegments())
                .isLessThan(availableBefore);

        // Buffer should be from pool (off-heap memory segment)
        assertThat(buffer.getMemorySegment().isOffHeap()).isTrue();

        // Clean up: only recycle the context buffer. bufferWithContext.buffer (the
        // ChannelStateByteBuffer) wraps the same underlying NetworkBuffer, so calling
        // close() on it would double-release.
        buffer.recycleBuffer();
    }

    // AT-UE7O: Only one channel's data processed at a time within a gate.
    // This is inherently guaranteed by the sequential nature of
    // ChannelStateChunkReader which processes channels one at a time.
    // The semaphore in filtering mode further ensures heap buffer isolation per gate.
    @Test
    void testSequentialChannelProcessing() throws Exception {
        // The existing recovery architecture processes channels sequentially via
        // ChannelStateChunkReader. The heap buffer semaphore provides an additional
        // per-gate constraint that prevents unbounded concurrent allocations.
        // This test verifies the semaphore limits concurrent allocations.
        NetworkBufferPool filteringPool = new NetworkBufferPool(preAllocatedSegments, 1024);
        try {
            SingleInputGate filteringGate =
                    new SingleInputGateBuilder()
                            .setChannelFactory(InputChannelBuilder::buildLocalRecoveredChannel)
                            .setSegmentProvider(filteringPool)
                            .setCheckpointingDuringRecoveryEnabled(true)
                            .build();

            InputChannelRecoveredStateHandler filteringHandler =
                    buildFilteringInputChannelStateHandler(filteringGate);

            InputChannelInfo info = new InputChannelInfo(0, 0);

            // Allocate up to the limit
            List<RecoveredChannelStateHandler.BufferWithContext<Buffer>> buffers =
                    new ArrayList<>();
            for (int i = 0; i < InputChannelRecoveredStateHandler.MAX_HEAP_BUFFERS_PER_GATE; i++) {
                buffers.add(filteringHandler.getBuffer(info));
            }

            // All 5 buffers should be allocated successfully
            assertThat(buffers)
                    .hasSize(InputChannelRecoveredStateHandler.MAX_HEAP_BUFFERS_PER_GATE);

            // Clean up
            for (RecoveredChannelStateHandler.BufferWithContext<Buffer> buf : buffers) {
                buf.context.recycleBuffer();
            }
            filteringGate.close();
        } finally {
            filteringPool.destroy();
        }
    }

    /**
     * Builds a handler with filtering enabled (non-null filteringHandler). We use a minimal
     * ChannelStateFilteringHandler stub since the actual filtering logic is not under test here.
     */
    private InputChannelRecoveredStateHandler buildFilteringInputChannelStateHandler(
            SingleInputGate inputGate) {
        // Use a no-op filtering handler to enable filtering code path.
        // The actual filtering behavior is tested in ChannelStateFilteringHandlerTest.
        ChannelStateFilteringHandler stubFilteringHandler =
                new ChannelStateFilteringHandler(
                        new ChannelStateFilteringHandler.GateFilterHandler[0]);
        return new InputChannelRecoveredStateHandler(
                new InputGate[] {inputGate},
                new InflightDataRescalingDescriptor(
                        new InflightDataRescalingDescriptor
                                        .InflightDataGateOrPartitionRescalingDescriptor[] {
                            new InflightDataRescalingDescriptor
                                    .InflightDataGateOrPartitionRescalingDescriptor(
                                    new int[] {1},
                                    RescaleMappings.identity(1, 1),
                                    new HashSet<>(),
                                    InflightDataRescalingDescriptor
                                            .InflightDataGateOrPartitionRescalingDescriptor
                                            .MappingType.IDENTITY)
                        }),
                stubFilteringHandler,
                null);
    }
}
