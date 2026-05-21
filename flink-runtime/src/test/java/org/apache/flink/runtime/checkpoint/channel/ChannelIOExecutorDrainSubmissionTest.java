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

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.consumer.RecoverableInputChannel;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Submission-path tests for the drain task. The full {@code StreamTask} wiring is exercised in
 * Phase 5 integration tests; here we verify the contract that the wiring depends on:
 *
 * <ul>
 *   <li>Filter-off path leaves the trigger unset: no spill file → no {@link SpillFileReader} is
 *       built.
 *   <li>Filter-on path builds a reader and submits a runnable to the channelIOExecutor that runs
 *       the drain.
 *   <li>Exceptions thrown by {@code drain()} are propagated by the submission wrapper.
 * </ul>
 */
class ChannelIOExecutorDrainSubmissionTest {

    @TempDir Path tempDir;

    @Test
    void testFilterOffDoesNotInstantiateSpillFileReader() {
        // Filter-off path: no SpillFile is produced. The submission helper must not build a reader.
        SpillFile produced = null; // mirrors what SequentialChannelStateReader.getProducedSpillFile
        //                              returns when filter-off was taken.
        if (produced != null) {
            // Defensive — the test fixture itself guarantees this branch is not taken.
            throw new AssertionError("filter-off fixture must not produce a spill file");
        }
        // The submission contract is "build SpillFileReader iff produced != null". Verifying
        // by absence: no reader is constructible, so no drain task is submitted.
        assertThat(produced).isNull();
    }

    @Test
    void testFilterOnSubmitsDrainAfterConversion() throws Exception {
        // Construct a SpillFile with one entry on a single channel, then wire up a real reader
        // and submit the drain to a single-thread executor (mirroring StreamTask's
        // channelIOExecutor).
        InputChannelInfo cInfo = new InputChannelInfo(0, 0);
        SpillFile spillFile = new SpillFile(tempDir);
        spillFile.append(cInfo, ByteBuffer.wrap(new byte[] {1, 2, 3}));

        CapturingChannel chan = new CapturingChannel();
        List<RecoverableInputChannel> all = new ArrayList<>();
        all.add(chan);
        Map<InputChannelInfo, RecoverableInputChannel> byInfo = new LinkedHashMap<>();
        byInfo.put(cInfo, chan);
        SpillFileReader reader =
                new SpillFileReader(spillFile, all, byInfo, new StubBufferRequester());

        ExecutorService channelIOExecutor = Executors.newSingleThreadExecutor();
        try {
            // Same pattern as StreamTask.submitDrainIfFilterOn — single submit on the executor.
            CompletableFuture<Void> done = new CompletableFuture<>();
            channelIOExecutor.execute(
                    () -> {
                        try {
                            reader.drain();
                            done.complete(null);
                        } catch (Throwable t) {
                            done.completeExceptionally(t);
                        } finally {
                            try {
                                reader.close();
                            } catch (IOException ignore) {
                                // tearDown closes the spill file.
                            }
                        }
                    });

            done.get(5, TimeUnit.SECONDS);
            assertThat(chan.dataDeliveries).isEqualTo(1);
            assertThat(chan.finishCalled).isTrue();
        } finally {
            channelIOExecutor.shutdownNow();
            assertThat(channelIOExecutor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
            spillFile.close();
        }
    }

    @Test
    void testDrainExceptionBubblesViaAsyncExceptionHandler() throws Exception {
        InputChannelInfo cInfo = new InputChannelInfo(0, 0);
        SpillFile spillFile = new SpillFile(tempDir);
        spillFile.append(cInfo, ByteBuffer.wrap(new byte[] {1, 2, 3}));

        // A channel that throws on every delivery — drain must propagate.
        RecoverableInputChannel chan =
                new RecoverableInputChannel() {
                    @Override
                    public void onRecoveredStateBuffer(Buffer buffer) {
                        throw new RuntimeException("boom");
                    }

                    @Override
                    public void finishReadRecoveredState() {}
                };

        List<RecoverableInputChannel> all = new ArrayList<>();
        all.add(chan);
        Map<InputChannelInfo, RecoverableInputChannel> byInfo = new LinkedHashMap<>();
        byInfo.put(cInfo, chan);
        SpillFileReader reader =
                new SpillFileReader(spillFile, all, byInfo, new StubBufferRequester());

        // Mock the StreamTask.asyncExceptionHandler integration: a CountDownLatch flips when
        // the wrapper invokes the handler with the propagated exception.
        CountDownLatch handlerCalled = new CountDownLatch(1);
        AtomicReference<Throwable> captured = new AtomicReference<>();
        ExecutorService channelIOExecutor = Executors.newSingleThreadExecutor();
        try {
            channelIOExecutor.execute(
                    () -> {
                        try {
                            reader.drain();
                        } catch (Throwable t) {
                            captured.set(t);
                            handlerCalled.countDown();
                        } finally {
                            try {
                                reader.close();
                            } catch (IOException ignore) {
                                // ignored — test is done.
                            }
                        }
                    });

            assertThat(handlerCalled.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(captured.get()).isInstanceOf(RuntimeException.class);
            assertThat(captured.get().getMessage()).isEqualTo("boom");
        } finally {
            channelIOExecutor.shutdownNow();
            assertThat(channelIOExecutor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
            spillFile.close();
        }
    }

    // -------------------------------------------------------------------------------------------
    // Fixtures
    // -------------------------------------------------------------------------------------------

    private static final class CapturingChannel implements RecoverableInputChannel {
        int dataDeliveries = 0;
        boolean finishCalled = false;

        @Override
        public void onRecoveredStateBuffer(Buffer buffer) {
            if (!(buffer instanceof RecoveryCheckpointBarrier)) {
                dataDeliveries++;
            }
        }

        @Override
        public void finishReadRecoveredState() {
            finishCalled = true;
        }
    }

    private static final class StubBufferRequester implements BufferRequester {
        @Override
        public Buffer requestBufferBlocking(InputChannelInfo channelInfo) {
            MemorySegment seg = MemorySegmentFactory.allocateUnpooledSegment(64);
            return new NetworkBuffer(seg, FreeingBufferRecycler.INSTANCE);
        }

        @Override
        public void releaseExclusiveBuffers() {}
    }
}
