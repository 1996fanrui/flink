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
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

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
            TrackingSegmentIterator segments =
                    new TrackingSegmentIterator(
                            Arrays.asList(
                                    stubCursor(c0, new byte[] {1, 2, 3}),
                                    stubCursor(c1, new byte[] {4, 5}),
                                    stubCursor(c0, new byte[] {6})));

            writer.addInputDataFromSpill(CHECKPOINT_ID, segments);
            // Request is queued but not yet processed — iteration must not have happened yet.
            assertThat(segments.iteratedCount.get()).isEqualTo(0);

            worker.processAllRequests();
            assertThat(segments.iteratedCount.get()).isEqualTo(3);
            assertThat(segments.closed.get())
                    .as("segments iterator must be closed by the request once done")
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
            TrackingSegmentIterator empty = new TrackingSegmentIterator(Collections.emptyList());
            writer.addInputDataFromSpill(CHECKPOINT_ID, empty);

            assertThat(worker.submitCount.get())
                    .as("empty spill iterator must skip writer-thread submission")
                    .isEqualTo(submittedBefore);
            assertThat(empty.closed.get()).as("empty segments closed inline").isTrue();
        }
    }

    @Test
    void testSegmentsClosedOnSuccessAndFailure() throws Exception {
        SyncChannelStateWriteRequestExecutor worker =
                new SyncChannelStateWriteRequestExecutor(JOB_ID);
        try (ChannelStateWriterImpl writer = newWriter(worker)) {
            worker.registerSubtask(JOB_VERTEX_ID, SUBTASK_INDEX);
            writer.start(CHECKPOINT_ID, CheckpointOptions.forCheckpointWithDefaultLocation());

            TrackingSegmentIterator segments =
                    new TrackingSegmentIterator(
                            Collections.singletonList(
                                    stubCursor(new InputChannelInfo(0, 0), new byte[] {1})));
            writer.addInputDataFromSpill(CHECKPOINT_ID, segments);
            worker.processAllRequests();
            assertThat(segments.closed.get()).isTrue();
        }
    }

    // -------------------------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------------------------

    private ChannelStateWriterImpl newWriter(SyncChannelStateWriteRequestExecutor worker) {
        return new ChannelStateWriterImpl(
                JOB_VERTEX_ID, TASK_NAME, SUBTASK_INDEX, new ConcurrentHashMap<>(), worker, 5);
    }

    /**
     * Creates a minimal stub {@link FetchedSegmentCursor} backed by the given payload bytes. The
     * body stream is a {@link ByteArrayInputStream} over {@code data}. Length equals {@code
     * data.length}. {@link FetchedSegmentCursor#commitConsumed()} is a no-op.
     */
    private static FetchedSegmentCursor stubCursor(InputChannelInfo info, byte[] data) {
        return new FetchedSegmentCursor() {
            @Override
            public InputChannelInfo channelInfo() {
                return info;
            }

            @Override
            public InputStream body() {
                return new ByteArrayInputStream(data);
            }

            @Override
            public int length() {
                return data.length;
            }

            @Override
            public void commitConsumed() {}
        };
    }

    // -------------------------------------------------------------------------------------------
    // Tracking iterator
    // -------------------------------------------------------------------------------------------

    private static final class TrackingSegmentIterator
            implements CloseableIterator<FetchedSegmentCursor> {

        private final Iterator<FetchedSegmentCursor> backing;
        final AtomicInteger iteratedCount = new AtomicInteger(0);
        final AtomicBoolean closed = new AtomicBoolean(false);

        TrackingSegmentIterator(List<FetchedSegmentCursor> cursors) {
            this.backing = cursors.iterator();
        }

        @Override
        public boolean hasNext() {
            return backing.hasNext();
        }

        @Override
        public FetchedSegmentCursor next() {
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

    // -------------------------------------------------------------------------------------------
    // Executor stubs
    // -------------------------------------------------------------------------------------------

    private static final class QueueCountingExecutor implements ChannelStateWriteRequestExecutor {

        final AtomicInteger submitCount = new AtomicInteger(0);

        QueueCountingExecutor(JobID jobID) {}

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
}
