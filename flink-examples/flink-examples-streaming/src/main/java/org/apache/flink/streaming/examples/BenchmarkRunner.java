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
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStats;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.streaming.api.graph.StreamGraph;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility that drives multi-round benchmark execution on a MiniCluster: submits jobs, polls
 * checkpoints via REST API, collects metrics, manages savepoint-based restarts, and writes CSV
 * results.
 */
public class BenchmarkRunner {

    private static final int REQUIRED_CHECKPOINTS = 3;
    private static final int POLL_INTERVAL_MS = 2000;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final int restPort;
    private final String restBase;
    private final String outputDir;

    public BenchmarkRunner(int restPort, String outputDir) {
        this.restPort = restPort;
        this.restBase = "http://localhost:" + restPort;
        this.outputDir = outputDir;
    }

    /** Functional interface for building a StreamGraph per round. */
    @FunctionalInterface
    public interface StreamGraphFactory {
        StreamGraph create(
                int upstreamParallelism, int downstreamParallelism, String checkpointPath);
    }

    /**
     * Run all rounds: for each parallelism pair, submit job, wait for checkpoints, collect metrics,
     * stop with savepoint, repeat.
     */
    public void run(MiniClusterConfiguration clusterConf, int[][] plan, StreamGraphFactory factory)
            throws Exception {
        new File(outputDir).mkdirs();
        List<RoundResult> results = new ArrayList<>();

        try (MiniCluster miniCluster = new MiniCluster(clusterConf)) {
            miniCluster.start();
            System.out.println("MiniCluster started. REST API at " + restBase);

            String checkpointPath = null;

            for (int round = 0; round < plan.length; round++) {
                int upPar = plan[round][0];
                int downPar = plan[round][1];
                boolean isLastRound = (round == plan.length - 1);

                System.out.printf(
                        "%n--- Round %d/%d (upstream=%d, downstream=%d) ---%n",
                        round + 1, plan.length, upPar, downPar);

                StreamGraph streamGraph = factory.create(upPar, downPar, checkpointPath);
                JobID jobId = miniCluster.submitJob(streamGraph).get().getJobID();
                System.out.println("Job submitted: " + jobId);

                waitForJobRunning(miniCluster, jobId);
                System.out.println("Job is RUNNING");

                CheckpointResult cpResult = waitForCheckpoints(miniCluster, jobId);
                System.out.println(
                        "Collected "
                                + REQUIRED_CHECKPOINTS
                                + " checkpoint durations: "
                                + cpResult.durations);
                System.out.println("Latest checkpoint: " + cpResult.latestPath);

                List<VertexInitMetrics> initMetrics = collectVertexInitMetrics(jobId);
                System.out.println(
                        "Collected initialization metrics for " + initMetrics.size() + " vertices");

                results.add(
                        new RoundResult(
                                round + 1, upPar, downPar, cpResult.durations, initMetrics));

                if (!isLastRound) {
                    checkpointPath = cpResult.latestPath;
                }

                System.out.println("Cancelling job...");
                miniCluster.cancelJob(jobId).get();
                waitForJobTerminated(miniCluster, jobId);
            }
        }

        String suffix =
                LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"))
                        + "_port"
                        + restPort;
        writeCheckpointCsv(results, suffix);
        writeInitializationCsv(results, suffix);
        System.out.println("\n=== Benchmark Complete ===");
        System.out.println("Results written to: " + outputDir);
    }

    // ========================= Job Lifecycle =========================

    private void waitForJobRunning(MiniCluster miniCluster, JobID jobId) throws Exception {
        while (true) {
            JobStatus status = miniCluster.getJobStatus(jobId).get();
            if (status == JobStatus.RUNNING) {
                return;
            }
            if (status.isTerminalState()) {
                throw new RuntimeException("Job terminated unexpectedly with status: " + status);
            }
            Thread.sleep(500);
        }
    }

    private void waitForJobTerminated(MiniCluster miniCluster, JobID jobId) throws Exception {
        while (true) {
            JobStatus status = miniCluster.getJobStatus(jobId).get();
            if (status.isTerminalState()) {
                System.out.println("Job terminated with status: " + status);
                return;
            }
            Thread.sleep(500);
        }
    }

    // ========================= Checkpoint =========================

    /**
     * Wait for REQUIRED_CHECKPOINTS successful checkpoints via MiniCluster API, then return their
     * durations and the latest checkpoint path.
     */
    private CheckpointResult waitForCheckpoints(MiniCluster miniCluster, JobID jobId)
            throws Exception {
        System.out.println("Waiting for " + REQUIRED_CHECKPOINTS + " successful checkpoints...");
        long lastSeenCount = 0;

        while (true) {
            var snapshot = miniCluster.getExecutionGraph(jobId).get().getCheckpointStatsSnapshot();
            if (snapshot == null) {
                Thread.sleep(POLL_INTERVAL_MS);
                continue;
            }

            long completed = snapshot.getCounts().getNumberOfCompletedCheckpoints();
            if (completed > lastSeenCount) {
                System.out.println("  Completed checkpoints: " + completed);
                lastSeenCount = completed;
            }

            if (completed >= REQUIRED_CHECKPOINTS) {
                // Collect durations of completed checkpoints from history
                List<Long> durations = new ArrayList<>();
                for (var cp : snapshot.getHistory().getCheckpoints()) {
                    if (cp instanceof CompletedCheckpointStats) {
                        durations.add(cp.getEndToEndDuration());
                    }
                }
                int startIdx = Math.max(0, durations.size() - REQUIRED_CHECKPOINTS);
                List<Long> lastN = new ArrayList<>(durations.subList(startIdx, durations.size()));

                String path =
                        snapshot.getHistory().getLatestCompletedCheckpoint().getExternalPath();
                return new CheckpointResult(lastN, path);
            }

            Thread.sleep(POLL_INTERVAL_MS);
        }
    }

    private static class CheckpointResult {
        final List<Long> durations;
        final String latestPath;

        CheckpointResult(List<Long> durations, String latestPath) {
            this.durations = durations;
            this.latestPath = latestPath;
        }
    }

    private List<VertexInitMetrics> collectVertexInitMetrics(JobID jobId) throws Exception {
        List<VertexInitMetrics> result = new ArrayList<>();

        String jobJson = httpGet(restBase + "/jobs/" + jobId);
        if (jobJson == null) {
            System.err.println("WARNING: Could not fetch job details for vertex metrics");
            return result;
        }

        JsonNode vertices = MAPPER.readTree(jobJson).path("vertices");
        for (JsonNode vertex : vertices) {
            String vertexId = vertex.path("id").asText();
            String vertexName = vertex.path("name").asText();

            String vertexJson = httpGet(restBase + "/jobs/" + jobId + "/vertices/" + vertexId);
            if (vertexJson == null) {
                continue;
            }

            JsonNode initNode =
                    MAPPER.readTree(vertexJson)
                            .path("aggregated")
                            .path("status-duration")
                            .path("INITIALIZING");
            if (initNode.isMissingNode()) {
                continue;
            }

            result.add(
                    new VertexInitMetrics(
                            vertexName,
                            initNode.path("min").asLong(),
                            initNode.path("avg").asLong(),
                            initNode.path("median").asLong(),
                            initNode.path("max").asLong(),
                            initNode.path("p25").asLong(),
                            initNode.path("p75").asLong(),
                            initNode.path("p95").asLong(),
                            initNode.path("sum").asLong()));
        }
        return result;
    }

    // ========================= HTTP =========================

    private String httpGet(String urlStr) {
        try {
            HttpURLConnection conn = (HttpURLConnection) new URL(urlStr).openConnection();
            conn.setRequestMethod("GET");
            conn.setConnectTimeout(5000);
            conn.setReadTimeout(5000);
            if (conn.getResponseCode() != 200) {
                return null;
            }
            try (BufferedReader reader =
                    new BufferedReader(
                            new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
                StringBuilder sb = new StringBuilder();
                String line;
                while ((line = reader.readLine()) != null) {
                    sb.append(line);
                }
                return sb.toString();
            }
        } catch (IOException e) {
            return null;
        }
    }

    // ========================= CSV Output =========================

    private void writeCheckpointCsv(List<RoundResult> results, String ts) throws IOException {
        File file = new File(outputDir, "checkpoint_" + ts + ".csv");
        try (PrintWriter pw = new PrintWriter(new FileWriter(file))) {
            pw.println(
                    "round,upstream_parallelism,downstream_parallelism,"
                            + "checkpoint_1_duration_ms,checkpoint_2_duration_ms,checkpoint_3_duration_ms,"
                            + "avg_checkpoint_duration_ms");

            List<Long> allDurations = new ArrayList<>();
            for (RoundResult r : results) {
                long d1 = r.checkpointDurations.size() > 0 ? r.checkpointDurations.get(0) : 0;
                long d2 = r.checkpointDurations.size() > 1 ? r.checkpointDurations.get(1) : 0;
                long d3 = r.checkpointDurations.size() > 2 ? r.checkpointDurations.get(2) : 0;
                long avg = (d1 + d2 + d3) / 3;
                pw.printf("%d,%d,%d,%d,%d,%d,%d%n", r.round, r.upPar, r.downPar, d1, d2, d3, avg);
                allDurations.addAll(r.checkpointDurations);
            }

            if (!allDurations.isEmpty()) {
                allDurations.sort(Long::compareTo);
                long totalAvg =
                        allDurations.stream().mapToLong(Long::longValue).sum()
                                / allDurations.size();
                long max = allDurations.get(allDurations.size() - 1);
                int p95Idx = (int) Math.ceil(allDurations.size() * 0.95) - 1;
                long p95 = allDurations.get(Math.max(0, p95Idx));
                pw.printf("SUMMARY,,,,avg=%d,p95=%d,max=%d%n", totalAvg, p95, max);
            }
        }
        System.out.println("Checkpoint results: " + file.getAbsolutePath());
    }

    private void writeInitializationCsv(List<RoundResult> results, String ts) throws IOException {
        File file = new File(outputDir, "initialization_" + ts + ".csv");
        try (PrintWriter pw = new PrintWriter(new FileWriter(file))) {
            pw.println(
                    "round,upstream_parallelism,downstream_parallelism,vertex_name,"
                            + "init_min,init_avg,init_median,init_max,"
                            + "init_p25,init_p75,init_p95,init_sum");

            for (RoundResult r : results) {
                for (VertexInitMetrics m : r.initMetrics) {
                    pw.printf(
                            "%d,%d,%d,%s,%d,%d,%d,%d,%d,%d,%d,%d%n",
                            r.round,
                            r.upPar,
                            r.downPar,
                            m.vertexName,
                            m.min,
                            m.avg,
                            m.median,
                            m.max,
                            m.p25,
                            m.p75,
                            m.p95,
                            m.sum);
                }
            }
        }
        System.out.println("Initialization results: " + file.getAbsolutePath());
    }

    // ========================= Data Classes =========================

    static class RoundResult {
        final int round;
        final int upPar;
        final int downPar;
        final List<Long> checkpointDurations;
        final List<VertexInitMetrics> initMetrics;

        RoundResult(
                int round,
                int upPar,
                int downPar,
                List<Long> checkpointDurations,
                List<VertexInitMetrics> initMetrics) {
            this.round = round;
            this.upPar = upPar;
            this.downPar = downPar;
            this.checkpointDurations = checkpointDurations;
            this.initMetrics = initMetrics;
        }
    }

    static class VertexInitMetrics {
        final String vertexName;
        final long min;
        final long avg;
        final long median;
        final long max;
        final long p25;
        final long p75;
        final long p95;
        final long sum;

        VertexInitMetrics(
                String vertexName,
                long min,
                long avg,
                long median,
                long max,
                long p25,
                long p75,
                long p95,
                long sum) {
            this.vertexName = vertexName;
            this.min = min;
            this.avg = avg;
            this.median = median;
            this.max = max;
            this.p25 = p25;
            this.p75 = p75;
            this.p95 = p95;
            this.sum = sum;
        }
    }
}
