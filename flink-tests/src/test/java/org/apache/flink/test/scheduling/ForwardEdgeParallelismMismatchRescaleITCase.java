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

package org.apache.flink.test.scheduling;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.PipelineOptions.ForwardEdgeParallelismMismatchMode;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobResourceRequirements;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.messages.job.JobDetailsInfo;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.legacy.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.legacy.RichParallelSourceFunction;
import org.apache.flink.test.junit5.InjectClusterClient;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.SerializedThrowable;
import org.apache.flink.util.function.SupplierWithException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end validation for {@link PipelineOptions#FORWARD_EDGE_PARALLELISM_MISMATCH_MODE}
 * (FLINK-40780).
 *
 * <p>A {@code source -> forward -> sink} job is submitted with both operators at parallelism 1 and
 * chaining disabled, so the forward edge is a real POINTWISE network edge between two separate
 * vertices with a matching (1 -> 1) parallelism. The <b>sink</b> is then rescaled to parallelism 2
 * at runtime via the {@code resource-requirements} REST API, which leaves the forward edge spanning
 * a producer(1) -> consumer(2) parallelism mismatch. The three modes are asserted to behave as
 * expected once the runtime hits that mismatch:
 *
 * <ul>
 *   <li>{@code REBALANCE}: the forward partitioner is replaced by a rebalance partitioner, so
 *       records are spread across <b>both</b> sink subtasks.
 *   <li>{@code KEEP_FORWARD}: the forward partitioner is kept, so records are funneled to a
 *       <b>single</b> sink subtask.
 *   <li>{@code FAIL}: the job fails with a clear exception.
 * </ul>
 */
class ForwardEdgeParallelismMismatchRescaleITCase {

    private static final int NUM_SLOTS = 8;
    private static final int SOURCE_PARALLELISM = 1;
    private static final int SINK_PARALLELISM_AFTER_RESCALE = 2;
    private static final Duration TIMEOUT = Duration.ofSeconds(60);

    @RegisterExtension
    private static final MiniClusterExtension MINI_CLUSTER_EXTENSION =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setConfiguration(createClusterConfiguration())
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(NUM_SLOTS)
                            .build());

    private static Configuration createClusterConfiguration() {
        final Configuration configuration = new Configuration();
        configuration.set(JobManagerOptions.SCHEDULER, JobManagerOptions.SchedulerType.Adaptive);
        configuration.set(
                JobManagerOptions.SCHEDULER_EXECUTING_COOLDOWN_AFTER_RESCALING, Duration.ZERO);
        configuration.set(WebOptions.REFRESH_INTERVAL, Duration.ofMillis(50L));
        configuration.set(JobManagerOptions.SLOT_IDLE_TIMEOUT, Duration.ofMillis(50L));
        return configuration;
    }

    /** subtask index -> number of records received. Static because the sink runs in-JVM. */
    private static final ConcurrentHashMap<Integer, AtomicLong> SINK_COUNTS =
            new ConcurrentHashMap<>();

    @BeforeEach
    void resetCounts() {
        SINK_COUNTS.clear();
    }

    @Test
    void rebalanceModeSpreadsAcrossConsumerSubtasks(
            @InjectClusterClient RestClusterClient<?> restClusterClient) throws Exception {
        final JobID jobId =
                submitAndRescaleSink(
                        restClusterClient, ForwardEdgeParallelismMismatchMode.REBALANCE);
        try {
            // With REBALANCE the single source subtask's records must be spread across both sink
            // subtasks.
            SINK_COUNTS.clear();
            waitUntil(
                    "both sink subtasks receive records (rebalance)",
                    () -> countFor(0) > 0 && countFor(1) > 0);
            assertThat(countFor(0)).isPositive();
            assertThat(countFor(1)).isPositive();
        } finally {
            restClusterClient.cancel(jobId).join();
        }
    }

    @Test
    void keepForwardModeFunnelsToSingleSubtask(
            @InjectClusterClient RestClusterClient<?> restClusterClient) throws Exception {
        final JobID jobId =
                submitAndRescaleSink(
                        restClusterClient, ForwardEdgeParallelismMismatchMode.KEEP_FORWARD);
        try {
            // With KEEP_FORWARD the single source subtask forwards to sink channel 0 only.
            SINK_COUNTS.clear();
            waitUntil("sink subtask 0 receives records (keep forward)", () -> countFor(0) > 500);
            // Give subtask 1 ample opportunity to (wrongly) receive something.
            Thread.sleep(1000L);
            assertThat(countFor(0)).isPositive();
            assertThat(countFor(1)).isZero();
        } finally {
            restClusterClient.cancel(jobId).join();
        }
    }

    @Test
    void failModeFailsJob(@InjectClusterClient RestClusterClient<?> restClusterClient)
            throws Exception {
        // No checkpointing is configured, so the default restart strategy is already "no restart";
        // the guard exception therefore fails the job terminally.
        final JobGraph jobGraph = buildJobGraph(ForwardEdgeParallelismMismatchMode.FAIL);
        final JobID jobId = jobGraph.getJobID();
        restClusterClient.submitJob(jobGraph).join();
        awaitRunningTasks(restClusterClient, jobId, 2 * SOURCE_PARALLELISM);

        rescaleSinkUp(restClusterClient, jobGraph);

        waitUntil(
                "job reaches a terminal FAILED state",
                () -> restClusterClient.getJobStatus(jobId).get() == JobStatus.FAILED);

        final Optional<SerializedThrowable> throwable =
                restClusterClient.requestJobResult(jobId).get().getSerializedThrowable();
        assertThat(throwable).isPresent();
        final Throwable cause = throwable.get().deserializeError(getClass().getClassLoader());
        assertThat(
                        ExceptionUtils.findThrowableWithMessage(
                                cause, "Forward partitioning cannot be preserved"))
                .isPresent();
    }

    private JobID submitAndRescaleSink(
            RestClusterClient<?> restClusterClient, ForwardEdgeParallelismMismatchMode mode)
            throws Exception {
        final JobGraph jobGraph = buildJobGraph(mode);
        final JobID jobId = jobGraph.getJobID();
        restClusterClient.submitJob(jobGraph).join();
        // Initial matched forward edge (source=1, sink=1): 2 running tasks, no mismatch.
        awaitRunningTasks(restClusterClient, jobId, 2 * SOURCE_PARALLELISM);

        rescaleSinkUp(restClusterClient, jobGraph);

        // After rescaling the sink up, the forward edge spans source(1) -> sink(2):
        // source(1) + sink(2) = 3 running tasks once the mismatch has been handled.
        awaitRunningTasks(
                restClusterClient, jobId, SOURCE_PARALLELISM + SINK_PARALLELISM_AFTER_RESCALE);
        return jobId;
    }

    private void rescaleSinkUp(RestClusterClient<?> restClusterClient, JobGraph jobGraph) {
        JobVertexID sourceId = null;
        JobVertexID sinkId = null;
        for (JobVertex vertex : jobGraph.getVertices()) {
            if (vertex.getName().toLowerCase().contains("source")) {
                sourceId = vertex.getID();
            } else if (vertex.getName().toLowerCase().contains("sink")) {
                sinkId = vertex.getID();
            }
        }
        assertThat(sourceId).isNotNull();
        assertThat(sinkId).isNotNull();

        final JobResourceRequirements requirements =
                JobResourceRequirements.newBuilder()
                        .setParallelismForJobVertex(
                                sourceId, SOURCE_PARALLELISM, SOURCE_PARALLELISM)
                        .setParallelismForJobVertex(
                                sinkId,
                                SINK_PARALLELISM_AFTER_RESCALE,
                                SINK_PARALLELISM_AFTER_RESCALE)
                        .build();
        restClusterClient.updateJobResourceRequirements(jobGraph.getJobID(), requirements).join();
    }

    private static JobGraph buildJobGraph(ForwardEdgeParallelismMismatchMode mode) {
        final Configuration jobConfiguration = new Configuration();
        jobConfiguration.set(PipelineOptions.FORWARD_EDGE_PARALLELISM_MISMATCH_MODE, mode);

        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(jobConfiguration);
        env.setParallelism(SOURCE_PARALLELISM);
        env.disableOperatorChaining();

        final DataStream<Long> source =
                env.addSource(new InfiniteLongSource())
                        .name("source")
                        .setParallelism(SOURCE_PARALLELISM);
        source.forward()
                .addSink(new CountingBySubtaskSink())
                .name("sink")
                .setParallelism(SOURCE_PARALLELISM);

        return env.getStreamGraph().getJobGraph();
    }

    private static void awaitRunningTasks(RestClusterClient<?> client, JobID jobId, int expected)
            throws Exception {
        final Deadline deadline = Deadline.fromNow(TIMEOUT);
        while (deadline.hasTimeLeft()) {
            if (runningTasks(client, jobId) == expected) {
                return;
            }
            Thread.sleep(200L);
        }
        String failureCause = "";
        if (client.getJobStatus(jobId).get() == JobStatus.FAILED) {
            try {
                final Optional<SerializedThrowable> t =
                        client.requestJobResult(jobId).get().getSerializedThrowable();
                if (t.isPresent()) {
                    failureCause =
                            "; failureCause="
                                    + ExceptionUtils.stringifyException(
                                            t.get()
                                                    .deserializeError(
                                                            ForwardEdgeParallelismMismatchRescaleITCase
                                                                    .class
                                                                    .getClassLoader()));
                }
            } catch (Exception ignored) {
                // best effort
            }
        }
        throw new AssertionError(
                "Timed out after "
                        + TIMEOUT.getSeconds()
                        + "s: expected "
                        + expected
                        + " running tasks, observed "
                        + runningTasks(client, jobId)
                        + "; jobStatus="
                        + client.getJobStatus(jobId).get()
                        + "; vertices="
                        + describeVertices(client, jobId)
                        + failureCause);
    }

    private static String describeVertices(RestClusterClient<?> client, JobID jobId) {
        try {
            final StringBuilder sb = new StringBuilder();
            for (JobDetailsInfo.JobVertexDetailsInfo v :
                    client.getJobDetails(jobId).get().getJobVertexInfos()) {
                sb.append(v.getName())
                        .append("[p=")
                        .append(v.getParallelism())
                        .append(",states=")
                        .append(v.getTasksPerState())
                        .append("] ");
            }
            return sb.toString();
        } catch (Exception e) {
            return "<unavailable: " + e + ">";
        }
    }

    private static int runningTasks(RestClusterClient<?> client, JobID jobId) throws Exception {
        return client.getJobDetails(jobId).get().getJobVertexInfos().stream()
                .map(JobDetailsInfo.JobVertexDetailsInfo::getTasksPerState)
                .map(tasksPerState -> tasksPerState.getOrDefault(ExecutionState.RUNNING, 0))
                .mapToInt(Integer::intValue)
                .sum();
    }

    private static long countFor(int subtask) {
        final AtomicLong counter = SINK_COUNTS.get(subtask);
        return counter == null ? 0L : counter.get();
    }

    private static void waitUntil(
            String description, SupplierWithException<Boolean, Exception> condition)
            throws Exception {
        final Deadline deadline = Deadline.fromNow(TIMEOUT);
        Exception lastError = null;
        while (deadline.hasTimeLeft()) {
            try {
                if (condition.get()) {
                    return;
                }
            } catch (Exception e) {
                lastError = e;
            }
            Thread.sleep(200L);
        }
        throw new AssertionError(
                "Timed out after "
                        + TIMEOUT.getSeconds()
                        + "s waiting for: "
                        + description
                        + (lastError != null ? " (last error: " + lastError + ")" : ""));
    }

    private static final class InfiniteLongSource extends RichParallelSourceFunction<Long> {
        private volatile boolean running = true;

        @Override
        public void run(SourceContext<Long> ctx) throws Exception {
            long value = 0L;
            while (running) {
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(value++);
                }
                Thread.sleep(1L);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    private static final class CountingBySubtaskSink extends RichSinkFunction<Long> {
        @Override
        public void invoke(Long value, Context context) {
            final int subtask = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
            SINK_COUNTS.computeIfAbsent(subtask, k -> new AtomicLong()).incrementAndGet();
        }
    }
}
