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
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.api.graph.StreamGraph;

import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * Unaligned Checkpoint benchmark with rescale support.
 *
 * <p>Job topology:
 *
 * <pre>
 *   DataGeneratorSource (upstream_parallelism)
 *     → rebalance()
 *     → map(sleep 20ms) (downstream_parallelism)
 *     → DiscardingSink
 * </pre>
 *
 * <p>Usage: UnalignedCheckpointBenchmark [plan]
 *
 * <ul>
 *   <li>No args: 1 round with parallelism 5,5
 *   <li>No-rescale: {@code 5,5;5,5;5,5;5,5;5,5}
 *   <li>Rescale: {@code 5,5;10,3;3,10;7,7}
 * </ul>
 */
public class UnalignedCheckpointBenchmark {

    private static final String CHECKPOINT_DIR = "file:///tmp/flink-benchmark-checkpoints";
    private static final String OUTPUT_DIR =
            "flink-examples/flink-examples-streaming/benchmark_result";

    /** Args: plan [restPort]. Example: "5,5;10,3" 30001 */
    public static void main(String[] args) throws Exception {
        int[][] plan = parsePlan(args);
        int restPort = args.length > 1 ? Integer.parseInt(args[1]) : 12345;

        System.out.println("=== Unaligned Checkpoint Benchmark ===");
        System.out.println("REST port: " + restPort);
        System.out.println(
                "Plan: "
                        + Arrays.stream(plan)
                                .map(p -> p[0] + "," + p[1])
                                .collect(Collectors.joining("; ")));
        System.out.println("Rounds: " + plan.length);

        Configuration conf = buildClusterConfig(restPort);
        MiniClusterConfiguration clusterConf =
                new MiniClusterConfiguration.Builder()
                        .setConfiguration(conf)
                        .setNumTaskManagers(1)
                        .setNumSlotsPerTaskManager(100)
                        .build();

        new BenchmarkRunner(restPort, OUTPUT_DIR)
                .run(
                        clusterConf,
                        plan,
                        (upPar, downPar, checkpointPath) ->
                                buildStreamGraph(conf, upPar, downPar, checkpointPath));
    }

    // ========================= Job Topology =========================

    private static StreamGraph buildStreamGraph(
            Configuration conf, int upPar, int downPar, String checkpointPath) {
        Configuration jobConf = new Configuration(conf);
        if (checkpointPath != null) {
            jobConf.set(StateRecoveryOptions.SAVEPOINT_PATH, checkpointPath);
        }
        StreamExecutionEnvironment env = new StreamExecutionEnvironment(jobConf);

        DataGeneratorSource<Long> source =
                new DataGeneratorSource<>(
                        value -> value,
                        Long.MAX_VALUE,
                        RateLimiterStrategy.perSecond(1000),
                        Types.LONG);

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "Data Generator")
                .setParallelism(upPar)
                .rebalance()
                .map(
                        value -> {
                            Thread.sleep(20);
                            return value;
                        })
                .setParallelism(downPar)
                .sinkTo(new DiscardingSink<>())
                .name("DiscardingSink")
                .setParallelism(downPar);

        StreamGraph streamGraph = env.getStreamGraph();
        streamGraph.setJobName("UC-Benchmark");
        streamGraph.setJobId(new JobID());

        return streamGraph;
    }

    // ========================= Cluster Config =========================

    private static Configuration buildClusterConfig(int restPort) {
        Configuration conf = new Configuration();
        conf.set(RestOptions.PORT, restPort);
        conf.setString("execution.checkpointing.unaligned.enabled", "true");
        conf.setString("rest.flamegraph.enabled", "true");
        conf.setString("execution.checkpointing.interval", "10s");
        conf.setString("execution.checkpointing.min-pause", "8s");
        conf.setString("execution.checkpointing.num-retained", "100");
        conf.setString("jobmanager.scheduler", "adaptive");
        conf.setString("jobmanager.execution.dynamic-configuration.white-list", "*");
        conf.setString("state.checkpoints.dir", CHECKPOINT_DIR);
        conf.set(WebOptions.CHECKPOINTS_HISTORY_SIZE, 100);
        conf.set(CheckpointingOptions.UNALIGNED_RECOVER_OUTPUT_ON_DOWNSTREAM, true);
        conf.set(CheckpointingOptions.UNALIGNED_DURING_RECOVERY_ENABLED, true);
        conf.setString(
                "execution.checkpointing.externalized-checkpoint-retention",
                "RETAIN_ON_CANCELLATION");
        return conf;
    }

    // ========================= Plan Parsing =========================

    private static int[][] parsePlan(String[] args) {
        if (args.length == 0) {
            return new int[][] {{5, 5}};
        }
        String[] rounds = args[0].split(";");
        int[][] plan = new int[rounds.length][2];
        for (int i = 0; i < rounds.length; i++) {
            String[] parts = rounds[i].split(",");
            if (parts.length != 2) {
                throw new IllegalArgumentException(
                        "Each round needs 2 values (upstream,downstream): " + rounds[i]);
            }
            plan[i][0] = Integer.parseInt(parts[0].trim());
            plan[i][1] = Integer.parseInt(parts[1].trim());
        }
        return plan;
    }
}
