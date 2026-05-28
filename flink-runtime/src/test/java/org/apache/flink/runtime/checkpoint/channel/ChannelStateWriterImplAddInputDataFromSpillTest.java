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

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter.ChannelStateWriteResult;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies {@link ChannelStateWriterImpl#addInputDataFromSpill}: async demux for non-empty
 * snapshots, in-line short-circuit on empty snapshots, failure propagation via {@link
 * ChannelStateWriteResult}, and the chunks-always-closed invariant.
 */
class ChannelStateWriterImplAddInputDataFromSpillTest {

    private static final JobID JOB_ID = new JobID();
    private static final JobVertexID JOB_VERTEX_ID = new JobVertexID();
    private static final int SUBTASK_INDEX = 0;
    private static final long CHECKPOINT_ID = 7L;
    private static final String TASK_NAME = "test";

    @Test
    void testNonEmptySnapshotAsyncDemux() throws Exception {
        SyncChannelStateWriteRequestExecutor worker =
                new SyncChannelStateWriteRequestExecutor(JOB_ID);
        try (ChannelStateWriterImpl writer = newWriter(worker)) {
            worker.registerSubtask(JOB_VERTEX_ID, SUBTASK_INDEX);
            writer.start(CHECKPOINT_ID, CheckpointOptions.forCheckpointWithDefaultLocation());

            InputChannelInfo c0 = new InputChannelInfo(0, 0);
            InputChannelInfo c1 = new InputChannelInfo(0, 1);
            TrackingChunkIterator chunks =
                    new TrackingChunkIterator(
                            Arrays.asList(
                                    new DiskSnapshot.Chunk(c0, new byte[] {1, 2, 3}, 3),
                                    new DiskSnapshot.Chunk(c1, new byte[] {4, 5}, 2),
                                    new DiskSnapshot.Chunk(c0, new byte[] {6}, 1)));

            writer.addInputDataFromSpill(CHECKPOINT_ID, chunks);
            // Request is queued but not yet processed — iteration must not have happened yet.
            assertThat(chunks.iteratedCount.get()).isEqualTo(0);

            worker.processAllRequests();
            assertThat(chunks.iteratedCount.get()).isEqualTo(3);
            assertThat(chunks.closed.get())
                    .as("chunks iterator must be closed by the request once demux is done")
                    .isTrue();
        }
    }

    @Test
    void testEmptySnapshotInlineEarlyReturn() throws Exception {
        QueueCountingExecutor worker = new QueueCountingExecutor(JOB_ID);
        try (ChannelStateWriterImpl writer =
                new ChannelStateWriterImpl(
                        JOB_VERTEX_ID,
                        TASK_NAME,
                        SUBTASK_INDEX,
                        new ConcurrentHashMap<>(),
                        worker,
                        5)) {
            worker.registerSubtask(JOB_VERTEX_ID, SUBTASK_INDEX);
            writer.start(CHECKPOINT_ID, CheckpointOptions.forCheckpointWithDefaultLocation());

            int submittedBefore = worker.submitCount.get();
            TrackingChunkIterator empty = new TrackingChunkIterator(Collections.emptyList());
            writer.addInputDataFromSpill(CHECKPOINT_ID, empty);

            assertThat(worker.submitCount.get())
                    .as("empty DiskSnapshot must skip writer-thread submission")
                    .isEqualTo(submittedBefore);
            assertThat(empty.closed.get()).as("empty chunks closed inline").isTrue();
        }
    }

    @Test
    void testWriteFailurePropagatesViaWriteResult() throws Exception {
        // start() must succeed first so the cpId result is registered; the failing executor is
        // then armed for the subsequent addInputDataFromSpill enqueue only.
        SwappableExecutor worker = new SwappableExecutor(JOB_ID);
        try (ChannelStateWriterImpl writer =
                new ChannelStateWriterImpl(
                        JOB_VERTEX_ID,
                        TASK_NAME,
                        SUBTASK_INDEX,
                        new ConcurrentHashMap<>(),
                        worker,
                        5)) {
            worker.registerSubtask(JOB_VERTEX_ID, SUBTASK_INDEX);
            writer.start(CHECKPOINT_ID, CheckpointOptions.forCheckpointWithDefaultLocation());
            ChannelStateWriteResult result = writer.getWriteResult(CHECKPOINT_ID);
            assertThat(result).isNotNull();

            worker.failNext.set(true);
            TrackingChunkIterator chunks =
                    new TrackingChunkIterator(
                            Collections.singletonList(
                                    new DiskSnapshot.Chunk(
                                            new InputChannelInfo(0, 0), new byte[] {1}, 1)));

            assertThatThrownBy(() -> writer.addInputDataFromSpill(CHECKPOINT_ID, chunks))
                    .isInstanceOf(RuntimeException.class);
            assertThat(chunks.closed.get())
                    .as("chunks iterator closed even on enqueue failure")
                    .isTrue();
            assertThat(result.getInputChannelStateHandles().isCompletedExceptionally())
                    .as("input channel state future propagates the enqueue failure")
                    .isTrue();
        }
    }

    @Test
    void testChunksClosedOnSuccessAndFailure() throws Exception {
        SyncChannelStateWriteRequestExecutor worker =
                new SyncChannelStateWriteRequestExecutor(JOB_ID);
        try (ChannelStateWriterImpl writer = newWriter(worker)) {
            worker.registerSubtask(JOB_VERTEX_ID, SUBTASK_INDEX);
            writer.start(CHECKPOINT_ID, CheckpointOptions.forCheckpointWithDefaultLocation());

            TrackingChunkIterator chunks =
                    new TrackingChunkIterator(
                            Collections.singletonList(
                                    new DiskSnapshot.Chunk(
                                            new InputChannelInfo(0, 0), new byte[] {1}, 1)));
            writer.addInputDataFromSpill(CHECKPOINT_ID, chunks);
            worker.processAllRequests();
            assertThat(chunks.closed.get()).isTrue();
        }
    }

    private ChannelStateWriterImpl newWriter(SyncChannelStateWriteRequestExecutor worker) {
        return new ChannelStateWriterImpl(
                JOB_VERTEX_ID, TASK_NAME, SUBTASK_INDEX, new ConcurrentHashMap<>(), worker, 5);
    }

    /** Tracks iteration and close calls so the test can assert on them deterministically. */
    private static final class TrackingChunkIterator
            implements CloseableIterator<DiskSnapshot.Chunk> {

        private final Iterator<DiskSnapshot.Chunk> backing;
        final AtomicInteger iteratedCount = new AtomicInteger(0);
        final AtomicBoolean closed = new AtomicBoolean(false);

        TrackingChunkIterator(List<DiskSnapshot.Chunk> chunks) {
            this.backing = chunks.iterator();
        }

        @Override
        public boolean hasNext() {
            return backing.hasNext();
        }

        @Override
        public DiskSnapshot.Chunk next() {
            if (!backing.hasNext()) {
                throw new NoSuchElementException();
            }
            iteratedCount.incrementAndGet();
            return backing.next();
        }

        @Override
        public void close() {
            closed.set(true);
        }
    }

    /** Counts {@code submit} calls without actually executing the request bodies. */
    private static final class QueueCountingExecutor implements ChannelStateWriteRequestExecutor {

        final AtomicInteger submitCount = new AtomicInteger(0);

        QueueCountingExecutor(JobID jobID) {
            // jobID accepted only for signature parity with SyncChannelStateWriteRequestExecutor.
        }

        @Override
        public void submit(ChannelStateWriteRequest e) {
            submitCount.incrementAndGet();
        }

        @Override
        public void submitPriority(ChannelStateWriteRequest e) {
            submitCount.incrementAndGet();
        }

        @Override
        public void start() throws IllegalStateException {}

        @Override
        public void registerSubtask(JobVertexID jobVertexID, int subtaskIndex) {}

        @Override
        public void releaseSubtask(JobVertexID jobVertexID, int subtaskIndex) {}
    }

    /**
     * Delegates to a sync executor by default; throws on the next submit when {@code failNext} is
     * set. Used so {@code writer.start} can succeed while a later {@code addInputDataFromSpill}
     * enqueue is forced to fail.
     */
    private static final class SwappableExecutor implements ChannelStateWriteRequestExecutor {

        private final SyncChannelStateWriteRequestExecutor delegate;
        final AtomicBoolean failNext = new AtomicBoolean(false);

        SwappableExecutor(JobID jobID) {
            this.delegate = new SyncChannelStateWriteRequestExecutor(jobID);
        }

        @Override
        public void submit(ChannelStateWriteRequest e) throws Exception {
            if (failNext.getAndSet(false)) {
                throw new TestException();
            }
            delegate.submit(e);
        }

        @Override
        public void submitPriority(ChannelStateWriteRequest e) throws Exception {
            if (failNext.getAndSet(false)) {
                throw new TestException();
            }
            delegate.submitPriority(e);
        }

        @Override
        public void start() throws IllegalStateException {
            delegate.start();
        }

        @Override
        public void registerSubtask(JobVertexID jobVertexID, int subtaskIndex) {
            delegate.registerSubtask(jobVertexID, subtaskIndex);
        }

        @Override
        public void releaseSubtask(JobVertexID jobVertexID, int subtaskIndex) {
            delegate.releaseSubtask(jobVertexID, subtaskIndex);
        }
    }

    private static final class TestException extends RuntimeException {}
}
