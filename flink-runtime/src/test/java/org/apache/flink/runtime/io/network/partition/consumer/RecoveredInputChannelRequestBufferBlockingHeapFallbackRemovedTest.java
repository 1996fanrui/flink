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

package org.apache.flink.runtime.io.network.partition.consumer;

import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionIndexSet;
import org.apache.flink.runtime.memory.MemoryManager;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that {@link RecoveredInputChannel#requestBufferBlocking} no longer falls back to
 * unpooled heap-segment allocation once the source channel's exclusive buffer pool is exhausted.
 * Replaces the pre-Phase-4 heap path that fixed OOM by spilling instead.
 */
class RecoveredInputChannelRequestBufferBlockingHeapFallbackRemovedTest {

    private NetworkBufferPool pool;

    @AfterEach
    void tearDown() {
        if (pool != null) {
            pool.destroy();
            pool = null;
        }
    }

    @Test
    void testBufferPoolExhaustedBlocksRatherThanHeapAllocate() throws Exception {
        // Tiny pool to force exhaustion quickly.
        int totalSegments = 4;
        pool = new NetworkBufferPool(totalSegments, MemoryManager.DEFAULT_PAGE_SIZE);
        RecoveredInputChannel channel = buildChannel(pool, totalSegments);

        // Drain the exclusive pool — request more buffers than the pool can offer.
        for (int i = 0; i < totalSegments; i++) {
            channel.requestBufferBlocking();
        }

        // The next request must block (no heap fallback path returns a non-pooled buffer).
        CountDownLatch entered = new CountDownLatch(1);
        AtomicReference<Buffer> result = new AtomicReference<>();
        Thread blocker =
                new Thread(
                        () -> {
                            try {
                                entered.countDown();
                                result.set(channel.requestBufferBlocking());
                            } catch (Exception ignored) {
                                // Thread will be interrupted at teardown.
                            }
                        },
                        "blocking-requester");
        blocker.start();

        assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
        // Give the requester a chance to attempt allocation.
        Thread.sleep(200);
        assertThat(result.get()).as("buffer should not have been allocated").isNull();

        // Interrupt the blocked thread to release the test.
        blocker.interrupt();
        blocker.join(5_000);
    }

    @Test
    void testFilterOnPathTakesSameRouteAsFilterOff() throws Exception {
        // Both filter-on and filter-off paths must allocate from the network pool — no behavior
        // divergence based on isCheckpointingDuringRecoveryEnabled.
        // Two channels each need an exclusive buffer; pool must hold both at once.
        int exclusivePerChannel = 1;
        int totalSegments = 4;
        pool = new NetworkBufferPool(totalSegments, MemoryManager.DEFAULT_PAGE_SIZE);

        Buffer filterOnBuf = buildChannel(pool, exclusivePerChannel, true).requestBufferBlocking();
        Buffer filterOffBuf =
                buildChannel(pool, exclusivePerChannel, false).requestBufferBlocking();

        // Both must come from the pool — pre-Phase-4 the filter-on path would wrap the segment in
        // FreeingBufferRecycler.INSTANCE for the heap-fallback case. Verify neither path takes
        // that route (the recycler is the BufferManager's own implementation, not Freeing-).
        assertThat(filterOnBuf.getMemorySegment()).isNotNull();
        assertThat(filterOffBuf.getMemorySegment()).isNotNull();
        assertThat(filterOnBuf.getRecycler().getClass().getName())
                .doesNotContain("FreeingBufferRecycler");
        assertThat(filterOffBuf.getRecycler().getClass().getName())
                .doesNotContain("FreeingBufferRecycler");

        filterOnBuf.recycleBuffer();
        filterOffBuf.recycleBuffer();
    }

    private RecoveredInputChannel buildChannel(
            NetworkBufferPool segmentProvider, int exclusivePerChannel) {
        return buildChannel(segmentProvider, exclusivePerChannel, true);
    }

    private RecoveredInputChannel buildChannel(
            NetworkBufferPool segmentProvider,
            int exclusivePerChannel,
            boolean checkpointingDuringRecoveryEnabled) {
        try {
            SingleInputGate inputGate =
                    new SingleInputGateBuilder()
                            .setSegmentProvider(segmentProvider)
                            .setCheckpointingDuringRecoveryEnabled(
                                    checkpointingDuringRecoveryEnabled)
                            .build();
            return new RecoveredInputChannel(
                    inputGate,
                    0,
                    new ResultPartitionID(),
                    new ResultSubpartitionIndexSet(0),
                    0,
                    0,
                    new SimpleCounter(),
                    new SimpleCounter(),
                    exclusivePerChannel) {
                @Override
                protected InputChannel toInputChannelInternal() {
                    throw new AssertionError("not expected during this test");
                }
            };
        } catch (Exception e) {
            throw new AssertionError("channel construction failed", e);
        }
    }
}
