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

package org.apache.flink.streaming.examples;

import org.apache.flink.api.common.JobID;
import org.apache.flink.client.deployment.StandaloneClusterId;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.JobResourceRequirements;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.RestClient;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobVertexDetailsHeaders;
import org.apache.flink.runtime.rest.messages.JobVertexDetailsInfo;
import org.apache.flink.runtime.rest.messages.JobVertexMessageParameters;
import org.apache.flink.runtime.rest.messages.job.JobDetailsInfo;
import org.apache.flink.runtime.rest.messages.job.SubtaskExecutionAttemptDetailsInfo;
import org.apache.flink.util.concurrent.Executors;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Drives an already-submitted job through a sequence of rescales (via the resource-requirements
 * REST API, the same mechanism as the Web UI "Rescale" button) and reports, per step, the
 * authoritative INITIALIZING time taken from Flink's own per-subtask {@code status-duration}
 * metric.
 *
 * <p>Before every rescale it lets the running job complete a configurable number of checkpoints so
 * a data backlog (unaligned in-flight buffers) has built up — which is what makes the subsequent
 * recovery / INITIALIZING phase meaningful — and prints each checkpoint's duration and persisted
 * data size (both read from Flink's checkpoint statistics, the same source as the Web UI).
 *
 * <p>Per-step timings:
 *
 * <ul>
 *   <li>{@code trigger->run}: driver wall-clock from issuing the rescale request until all vertices
 *       are first observed RUNNING at the target parallelism (poll based, ±poll interval).
 *   <li>{@code init(max instance)}: Flink's own INITIALIZING duration — the max over every subtask
 *       instance of every vertex (the "INITIALIZING Duration / Max" the Web UI shows).
 * </ul>
 */
class RescaleHarness implements AutoCloseable {

    private static final long POLL_MS = 25;
    private static final long HEARTBEAT_MS = 30_000;

    /** How long to wait for a rescale to reach RUNNING (the recovery / INITIALIZING phase). */
    private static final long RUNNING_TIMEOUT_MS = 3 * 60 * 60 * 1000L;

    /** Timeout for the quick waits (checkpoint backlog, vertices appearing after submit). */
    private static final long SHORT_TIMEOUT_MS = 2 * 60 * 1000L;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final String host;
    private final int port;
    private final JobID jobId;
    private final int checkpointsBeforeRescale;
    private final RestClusterClient<StandaloneClusterId> clusterClient;
    private final RestClient restClient;
    private final List<StepResult> results = new ArrayList<>();
    private final long overallStartNanos = System.nanoTime();

    RescaleHarness(String host, int port, JobID jobId, int checkpointsBeforeRescale)
            throws Exception {
        this.host = host;
        this.port = port;
        this.jobId = jobId;
        this.checkpointsBeforeRescale = checkpointsBeforeRescale;

        final Configuration clientConf = new Configuration();
        clientConf.set(RestOptions.ADDRESS, host);
        clientConf.set(RestOptions.PORT, port);
        this.clusterClient = new RestClusterClient<>(clientConf, StandaloneClusterId.getInstance());
        this.restClient = new RestClient(new Configuration(), Executors.directExecutor());
    }

    /**
     * Step 0: measure the initial bring-up ({@code triggerNanos} = time just before submission).
     * All of this step's own output is buffered and printed as one block at the end, so it stays
     * contiguous instead of being split by Flink's own logs.
     */
    void bringUp(int step, int[] target, long triggerNanos) throws Exception {
        final StringBuilder out = new StringBuilder();
        out.append(
                String.format(
                        "%n=== Step %d (initial) target=%s ===%n", step, Arrays.toString(target)));
        appendVertexMapping(out, target.length);
        measureAndAppend(out, step, target, triggerNanos);
        System.out.print(out);
    }

    /**
     * Accumulate a checkpoint backlog, then rescale to {@code target} and measure. This step's own
     * output is buffered and printed as one block at the end.
     */
    void rescale(int step, int[] target) throws Exception {
        final StringBuilder out = new StringBuilder();
        out.append(
                String.format(
                        "%n=== Step %d rescale target=%s ===%n", step, Arrays.toString(target)));

        appendCheckpointBacklog(out);

        final List<JobVertexID> vertices = orderedVertexIds();
        final JobResourceRequirements.Builder builder = JobResourceRequirements.newBuilder();
        for (int i = 0; i < vertices.size(); i++) {
            builder.setParallelismForJobVertex(vertices.get(i), target[i], target[i]);
        }
        final long triggerNanos = System.nanoTime();
        clusterClient.updateJobResourceRequirements(jobId, builder.build()).get();
        measureAndAppend(out, step, target, triggerNanos);
        System.out.print(out);
    }

    void printSummary() {
        System.out.printf("%n================ RESCALE SUMMARY ================%n");
        System.out.printf(
                "%-5s %-16s %-14s %-18s%n", "step", "target", "trigger->run", "init(max)");
        for (int i = 0; i < results.size(); i++) {
            final StepResult r = results.get(i);
            System.out.printf(
                    "%-5d %-16s %-14s %-18s%n",
                    i,
                    Arrays.toString(r.target),
                    r.triggerToRunningMs + "ms",
                    r.jobInitMaxMs < 0 ? "n/a" : r.jobInitMaxMs + "ms");
        }
        System.out.printf(
                "Total elapsed: %d ms%n", (System.nanoTime() - overallStartNanos) / 1_000_000L);
        System.out.printf("================================================%n");
    }

    @Override
    public void close() throws Exception {
        restClient.close();
        clusterClient.close();
    }

    // ------------------------------------------------------------------------

    private void measureAndAppend(StringBuilder out, int step, int[] target, long triggerNanos)
            throws Exception {
        final long runningNanos = awaitRunning(target);
        final long jobInitMaxMs = maxInitializingMs(orderedVertexIds());
        final StepResult r =
                new StepResult(target, (runningNanos - triggerNanos) / 1_000_000L, jobInitMaxMs);
        results.add(r);
        out.append(
                String.format(
                        ">>> step %d done target=%s | trigger->run=%dms | init(max instance)=%s%n",
                        step,
                        Arrays.toString(target),
                        r.triggerToRunningMs,
                        r.jobInitMaxMs < 0 ? "n/a" : r.jobInitMaxMs + "ms"));
    }

    /**
     * Polls until every vertex is RUNNING at its target parallelism; returns that instant (nanos).
     */
    private long awaitRunning(int[] target) throws Exception {
        final long startMs = System.currentTimeMillis();
        final long deadline = startMs + RUNNING_TIMEOUT_MS;
        long lastBeat = startMs;
        while (true) {
            final List<JobDetailsInfo.JobVertexDetailsInfo> infos = tryVertexInfos();
            boolean allRunningAtTarget = infos.size() == target.length;
            for (int i = 0; i < infos.size() && i < target.length; i++) {
                final ExecutionState state = infos.get(i).getExecutionState();
                if (state == ExecutionState.FAILED) {
                    throw new IllegalStateException(
                            "Vertex " + infos.get(i).getName() + " FAILED during step.");
                }
                if (state != ExecutionState.RUNNING || infos.get(i).getParallelism() != target[i]) {
                    allRunningAtTarget = false;
                }
            }
            if (allRunningAtTarget) {
                return System.nanoTime();
            }
            final long nowMs = System.currentTimeMillis();
            if (nowMs - lastBeat >= HEARTBEAT_MS) {
                // Live progress line (bypasses the per-step buffer) so a long recovery is visibly
                // making progress vs frozen.
                System.out.printf(
                        "    [waiting %ds] %s%n", (nowMs - startMs) / 1000, snapshot(infos));
                lastBeat = nowMs;
            }
            if (nowMs > deadline) {
                throw new IllegalStateException(
                        "Timed out waiting for all vertices to reach RUNNING at target "
                                + Arrays.toString(target)
                                + "; current vertices: "
                                + snapshot(infos)
                                + " (a vertex is stuck — e.g. still INITIALIZING/recovering — not"
                                + " yet RUNNING at the target parallelism)");
            }
            Thread.sleep(POLL_MS);
        }
    }

    private static String snapshot(List<JobDetailsInfo.JobVertexDetailsInfo> infos) {
        return infos.stream()
                .map(v -> v.getName() + "=" + v.getExecutionState() + "@p" + v.getParallelism())
                .collect(Collectors.joining(", "));
    }

    /**
     * Waits until {@link #checkpointsBeforeRescale} more checkpoints complete, then appends each
     * one's duration and persisted (unaligned in-flight) data size to {@code out}.
     */
    private void appendCheckpointBacklog(StringBuilder out) throws Exception {
        if (checkpointsBeforeRescale <= 0) {
            return;
        }
        final long deadline = System.currentTimeMillis() + SHORT_TIMEOUT_MS;
        long lastId = -1L;
        for (Checkpoint c : completedCheckpoints()) {
            lastId = Math.max(lastId, c.id);
        }
        final List<Checkpoint> collected = new ArrayList<>();
        while (collected.size() < checkpointsBeforeRescale) {
            checkDeadline(deadline, "complete " + checkpointsBeforeRescale + " checkpoints");
            Thread.sleep(POLL_MS);
            for (Checkpoint c : completedCheckpoints()) {
                if (c.id > lastId) {
                    lastId = c.id;
                    collected.add(c);
                    if (collected.size() >= checkpointsBeforeRescale) {
                        break;
                    }
                }
            }
        }
        out.append(
                String.format(
                        "    accumulated %d checkpoint(s) for backlog:%n",
                        checkpointsBeforeRescale));
        for (Checkpoint c : collected) {
            out.append(
                    String.format(
                            "      checkpoint %d: duration=%dms, persisted=%s, stateSize=%s%n",
                            c.id, c.durationMs, human(c.persistedBytes), human(c.stateSizeBytes)));
        }
    }

    /** Completed checkpoints (ascending id) from the checkpoint-statistics REST endpoint. */
    private List<Checkpoint> completedCheckpoints() throws Exception {
        final JsonNode root = getJson("/jobs/" + jobId + "/checkpoints");
        final JsonNode history = root.get("history");
        final List<Checkpoint> out = new ArrayList<>();
        if (history != null && history.isArray()) {
            for (JsonNode n : history) {
                if ("COMPLETED".equals(n.path("status").asText())) {
                    out.add(
                            new Checkpoint(
                                    n.path("id").asLong(),
                                    n.path("end_to_end_duration").asLong(),
                                    n.path("persisted_data").asLong(),
                                    n.path("state_size").asLong()));
                }
            }
        }
        out.sort(Comparator.comparingLong(c -> c.id));
        return out;
    }

    private JsonNode getJson(String path) throws Exception {
        final HttpURLConnection conn =
                (HttpURLConnection) new URL("http://" + host + ":" + port + path).openConnection();
        conn.setConnectTimeout(5000);
        conn.setReadTimeout(15000);
        try (InputStream in = conn.getInputStream()) {
            return MAPPER.readTree(in);
        } finally {
            conn.disconnect();
        }
    }

    /**
     * Job-level INITIALIZING duration from Flink's own per-subtask {@code status-duration} metric
     * (same source as the Web UI "INITIALIZING Duration"): the max INITIALIZING over every subtask
     * instance of every vertex. Must be called once the vertices are RUNNING, so the durations are
     * final.
     */
    private long maxInitializingMs(List<JobVertexID> vertices) throws Exception {
        long globalMax = -1L;
        for (JobVertexID vid : vertices) {
            final JobVertexMessageParameters params = new JobVertexMessageParameters();
            params.jobPathParameter.resolve(jobId);
            params.jobVertexIdPathParameter.resolve(vid);
            final JobVertexDetailsInfo info =
                    restClient
                            .sendRequest(
                                    host,
                                    port,
                                    JobVertexDetailsHeaders.getInstance(),
                                    params,
                                    EmptyRequestBody.getInstance())
                            .get();
            for (SubtaskExecutionAttemptDetailsInfo subtask : info.getSubtasks()) {
                final Long initMs = subtask.getStatusDuration().get(ExecutionState.INITIALIZING);
                if (initMs != null) {
                    globalMax = Math.max(globalMax, initMs);
                }
            }
        }
        return globalMax;
    }

    /** Vertex details in topological order (source -> sink). */
    private List<JobDetailsInfo.JobVertexDetailsInfo> orderedVertexInfos() throws Exception {
        return new ArrayList<>(clusterClient.getJobDetails(jobId).get().getJobVertexInfos());
    }

    /** Like {@link #orderedVertexInfos} but returns an empty list on transient REST errors. */
    private List<JobDetailsInfo.JobVertexDetailsInfo> tryVertexInfos() {
        try {
            return orderedVertexInfos();
        } catch (Exception e) {
            return Collections.emptyList();
        }
    }

    private List<JobVertexID> orderedVertexIds() throws Exception {
        return orderedVertexInfos().stream()
                .map(JobDetailsInfo.JobVertexDetailsInfo::getJobVertexID)
                .collect(Collectors.toList());
    }

    private void appendVertexMapping(StringBuilder out, int expected) throws Exception {
        final long deadline = System.currentTimeMillis() + SHORT_TIMEOUT_MS;
        List<JobDetailsInfo.JobVertexDetailsInfo> infos;
        while ((infos = tryVertexInfos()).size() < expected) {
            checkDeadline(deadline, "expose its job vertices");
            Thread.sleep(POLL_MS);
        }
        for (int i = 0; i < infos.size(); i++) {
            out.append(String.format("  index %d -> %s%n", i, infos.get(i).getName()));
        }
    }

    private static void checkDeadline(long deadlineMillis, String what) {
        if (System.currentTimeMillis() > deadlineMillis) {
            throw new IllegalStateException("Timed out waiting for the job to " + what + ".");
        }
    }

    private static String human(long bytes) {
        if (bytes < 1024) {
            return bytes + "B";
        }
        if (bytes < 1024 * 1024) {
            return String.format("%.1fKB", bytes / 1024.0);
        }
        return String.format("%.1fMB", bytes / (1024.0 * 1024.0));
    }

    private static final class Checkpoint {
        final long id;
        final long durationMs;
        final long persistedBytes;
        final long stateSizeBytes;

        Checkpoint(long id, long durationMs, long persistedBytes, long stateSizeBytes) {
            this.id = id;
            this.durationMs = durationMs;
            this.persistedBytes = persistedBytes;
            this.stateSizeBytes = stateSizeBytes;
        }
    }

    private static final class StepResult {
        final int[] target;
        final long triggerToRunningMs;
        final long jobInitMaxMs;

        StepResult(int[] target, long triggerToRunningMs, long jobInitMaxMs) {
            this.target = target;
            this.triggerToRunningMs = triggerToRunningMs;
            this.jobInitMaxMs = jobInitMaxMs;
        }
    }
}
