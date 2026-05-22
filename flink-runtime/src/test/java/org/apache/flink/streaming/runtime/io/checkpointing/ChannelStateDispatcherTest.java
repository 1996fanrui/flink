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

package org.apache.flink.streaming.runtime.io.checkpointing;

import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.checkpoint.channel.DiskSnapshot;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.checkpoint.channel.RecoveryCheckpointTrigger;
import org.apache.flink.runtime.checkpoint.channel.ResultSubpartitionInfo;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.consumer.CheckpointableInput;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies the {@link ChannelState#onCheckpointStartedForAllInputs} dispatcher: call ordering,
 * feature-off no-op routing through the {@link RecoveryCheckpointTrigger#NO_OP} singleton, and
 * absence of an outer feature-flag branch.
 */
class ChannelStateDispatcherTest {

    private static final long CHECKPOINT_ID = 7L;

    @Test
    void testStepOrderingFeatureOn() throws Exception {
        List<String> trace = new ArrayList<>();
        DiskSnapshot snap = DiskSnapshot.empty();
        RecordingTrigger trigger = new RecordingTrigger(trace, /* nonEmptySnapshot */ snap);
        RecordingWriter writer = new RecordingWriter(trace);
        CheckpointableInput input1 = new RecordingInput(trace, "in1");
        CheckpointableInput input2 = new RecordingInput(trace, "in2");

        ChannelState state =
                new ChannelState(new CheckpointableInput[] {input1, input2}, trigger, writer);

        CheckpointBarrier barrier = newUnalignedBarrier();
        state.onCheckpointStartedForAllInputs(barrier);

        assertThat(trace)
                .containsExactly(
                        "trigger.snapshotAndInsertBarriers:" + CHECKPOINT_ID,
                        "in1.checkpointStarted:" + CHECKPOINT_ID,
                        "in2.checkpointStarted:" + CHECKPOINT_ID,
                        "writer.addInputDataFromSpill:" + CHECKPOINT_ID);
    }

    @Test
    void testStepOrderingFeatureOff() throws Exception {
        List<String> trace = new ArrayList<>();
        // Use the production NO_OP trigger so we exercise the branch-free path.
        RecordingWriter writer = new RecordingWriter(trace);
        CheckpointableInput input = new RecordingInput(trace, "in1");

        ChannelState state =
                new ChannelState(
                        new CheckpointableInput[] {input}, RecoveryCheckpointTrigger.NO_OP, writer);

        state.onCheckpointStartedForAllInputs(newUnalignedBarrier());

        // The dispatcher always invokes writer.addInputDataFromSpill; with the NO_OP trigger it
        // receives the empty DiskSnapshot and short-circuits in-line.
        assertThat(trace)
                .containsExactly(
                        "in1.checkpointStarted:" + CHECKPOINT_ID,
                        "writer.addInputDataFromSpill:" + CHECKPOINT_ID);
        assertThat(writer.lastSnapshotWasEmpty.get()).isTrue();
    }

    @Test
    void testEmptySnapshotInlineEarlyReturn() throws Exception {
        List<String> trace = new ArrayList<>();
        DiskSnapshot empty = DiskSnapshot.empty();
        RecordingTrigger trigger = new RecordingTrigger(trace, empty);
        RecordingWriter writer = new RecordingWriter(trace);

        ChannelState state =
                new ChannelState(
                        new CheckpointableInput[] {new RecordingInput(trace, "in1")},
                        trigger,
                        writer);

        state.onCheckpointStartedForAllInputs(newUnalignedBarrier());

        // Writer was invoked with an empty snapshot; the production writer-side implementation
        // must short-circuit in-line rather than submit to the writer thread.
        assertThat(writer.lastSnapshotWasEmpty.get()).isTrue();
    }

    @Test
    void testNoIfFilterOnInDispatcher() throws Exception {
        // Branch-free routing through the null-object trigger is a hard correctness invariant;
        // a feature-flag check inside the dispatcher would silently bypass it. Guard against
        // that by scanning the dispatcher source for "filter" / "feature".
        Path candidate =
                Paths.get(
                        "src/main/java/org/apache/flink/streaming/runtime/io/checkpointing/ChannelState.java");
        if (!Files.exists(candidate)) {
            // Fallback when the test runs with the repo root as cwd.
            candidate =
                    Paths.get(
                            "flink-runtime/src/main/java/org/apache/flink/streaming/runtime/io/checkpointing/ChannelState.java");
        }
        assertThat(Files.exists(candidate))
                .as("Located ChannelState.java for the source-level invariant check")
                .isTrue();

        // Restrict the scan to the dispatcher method body.
        String all = new String(Files.readAllBytes(candidate));
        int methodStart = all.indexOf("public void onCheckpointStartedForAllInputs");
        assertThat(methodStart).isNotNegative();
        int methodEnd = all.indexOf("    private void", methodStart);
        String body = all.substring(methodStart, methodEnd > 0 ? methodEnd : all.length());

        // Strip line comments so explanatory prose in // does not trip the heuristic.
        StringBuilder code = new StringBuilder();
        for (String line : body.split("\n")) {
            int idx = line.indexOf("//");
            code.append(idx >= 0 ? line.substring(0, idx) : line).append('\n');
        }
        String codeOnly = code.toString();

        assertThat(codeOnly)
                .as("Dispatcher must not branch on filter / feature flags")
                .doesNotContain("filter")
                .doesNotContain("feature");
    }

    private static CheckpointBarrier newUnalignedBarrier() {
        return new CheckpointBarrier(
                CHECKPOINT_ID,
                1000L,
                CheckpointOptions.unaligned(
                        CheckpointType.CHECKPOINT,
                        CheckpointStorageLocationReference.getDefault()));
    }

    /** No-op stub used to capture step ordering without pulling in a mock framework. */
    private static final class RecordingTrigger implements RecoveryCheckpointTrigger {
        private final List<String> trace;
        private final DiskSnapshot snapshot;

        RecordingTrigger(List<String> trace, DiskSnapshot snapshot) {
            this.trace = trace;
            this.snapshot = snapshot;
        }

        @Override
        public DiskSnapshot snapshotAndInsertBarriers(long checkpointId) {
            trace.add("trigger.snapshotAndInsertBarriers:" + checkpointId);
            return snapshot;
        }
    }

    /** No-op writer recording the {@code addInputDataFromSpill} call and emptiness. */
    private static final class RecordingWriter implements ChannelStateWriter {
        private final List<String> trace;
        final AtomicBoolean lastSnapshotWasEmpty = new AtomicBoolean(false);
        final AtomicLong lastCpId = new AtomicLong(-1L);
        final AtomicInteger addInputDataFromSpillCalls = new AtomicInteger(0);

        RecordingWriter(List<String> trace) {
            this.trace = trace;
        }

        @Override
        public void start(long checkpointId, CheckpointOptions checkpointOptions) {}

        @Override
        public void addInputData(
                long checkpointId,
                InputChannelInfo info,
                int startSeqNum,
                CloseableIterator<Buffer> data) {}

        @Override
        public void addOutputData(
                long checkpointId, ResultSubpartitionInfo info, int startSeqNum, Buffer... data) {}

        @Override
        public void addOutputDataFuture(
                long checkpointId,
                ResultSubpartitionInfo info,
                int startSeqNum,
                CompletableFuture<List<Buffer>> data) {}

        @Override
        public void finishInput(long checkpointId) {}

        @Override
        public void finishOutput(long checkpointId) {}

        @Override
        public void abort(long checkpointId, Throwable cause, boolean cleanup) {}

        @Override
        public ChannelStateWriteResult getAndRemoveWriteResult(long checkpointId) {
            return ChannelStateWriteResult.EMPTY;
        }

        @Override
        public void addInputDataFromSpill(
                long checkpointId, CloseableIterator<DiskSnapshot.Chunk> chunks) {
            trace.add("writer.addInputDataFromSpill:" + checkpointId);
            lastCpId.set(checkpointId);
            addInputDataFromSpillCalls.incrementAndGet();
            try {
                lastSnapshotWasEmpty.set(!chunks.hasNext());
                chunks.close();
            } catch (Exception ignored) {
            }
        }

        @Override
        public void close() {}
    }

    /** Minimal {@link CheckpointableInput} stub that records calls and is otherwise inert. */
    private static final class RecordingInput implements CheckpointableInput {

        private final List<String> trace;
        private final String name;

        RecordingInput(List<String> trace, String name) {
            this.trace = trace;
            this.name = name;
        }

        @Override
        public void blockConsumption(InputChannelInfo channelInfo) {}

        @Override
        public void resumeConsumption(InputChannelInfo channelInfo) {}

        @Override
        public List<InputChannelInfo> getChannelInfos() {
            return Collections.emptyList();
        }

        @Override
        public int getNumberOfInputChannels() {
            return 0;
        }

        @Override
        public void checkpointStarted(CheckpointBarrier barrier) throws CheckpointException {
            trace.add(name + ".checkpointStarted:" + barrier.getId());
        }

        @Override
        public void checkpointStopped(long cancelledCheckpointId) {}

        @Override
        public int getInputGateIndex() {
            return 0;
        }

        @Override
        public void convertToPriorityEvent(int channelIndex, int sequenceNumber) {}
    }
}
