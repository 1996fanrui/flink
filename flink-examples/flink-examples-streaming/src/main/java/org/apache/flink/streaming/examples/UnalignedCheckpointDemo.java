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

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;

import java.util.Arrays;
import java.util.List;

/**
 * Submits a 3-vertex unaligned-checkpoint pipeline and drives it through a scripted sequence of
 * rescales, reporting per-step INITIALIZING time and per-checkpoint persisted data. All the REST
 * driving / measuring lives in {@link RescaleHarness}; this class is just the job (DAG) and the
 * rescale plan.
 *
 * <p>The pipeline has three job vertices (topological, upstream -&gt; downstream): {@code Source:
 * Data Generator}, {@code map1} (10ms/record), and {@code map2 -> Sink} (20ms/record, chained).
 * Each row of {@link #RESCALE_PLAN} is the target parallelism of those three, in that order (e.g.
 * {@code {5, 6, 7}} = source 5, map1 6, map2+sink 7). Row 0 is the initial parallelism; later rows
 * are rescales.
 */
public class UnalignedCheckpointDemo {

    private static final int REST_PORT = 12345;
    private static final String CHECKPOINT_DIR = "/tmp/flinkjob";

    /** Checkpoints to complete (to build an in-flight backlog) before each rescale. */
    private static final int CHECKPOINTS_BEFORE_RESCALE = 2;

    /**
     * Target parallelism per step, order = {source, map1, map2+sink}. Row 0 is the initial
     * parallelism; rows 1..N are rescales. Covers both full rescales (all three change) and partial
     * rescales (only some change, so only the touched edges redistribute).
     */
    private static final List<int[]> RESCALE_PLAN =
            Arrays.asList(
                    new int[] {5, 5, 5}, // 0: initial
                    new int[] {7, 5, 5}, // 1: only source changed (partial)
                    new int[] {7, 8, 5}, // 2: only map1 changed (partial)
                    new int[] {7, 8, 9}, // 3: only map2+sink changed (partial)
                    new int[] {4, 4, 4}); // 4: all three changed (full)

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config());
        buildPipeline(env, RESCALE_PLAN.get(0));

        final long submitNanos = System.nanoTime();
        final JobClient jobClient = env.executeAsync();

        try (RescaleHarness harness =
                new RescaleHarness(
                        "localhost", REST_PORT, jobClient.getJobID(), CHECKPOINTS_BEFORE_RESCALE)) {
            harness.bringUp(0, RESCALE_PLAN.get(0), submitNanos);
            for (int step = 1; step < RESCALE_PLAN.size(); step++) {
                harness.rescale(step, RESCALE_PLAN.get(step));
            }
            harness.printSummary();
        } catch (Throwable t) {
            // Print the real cause loudly — otherwise a failed step is swallowed / buried under the
            // orphaned job's logs and we never learn why the run stopped.
            System.err.println();
            System.err.println(">>> DEMO FAILED; cause:");
            t.printStackTrace();
        } finally {
            // Always cancel, even on failure, so a thrown step doesn't leave an orphaned job /
            // MiniCluster running (which keeps the JVM alive and floods the console).
            jobClient.cancel().get();
        }
    }

    private static Configuration config() {
        final Configuration conf = new Configuration();
        conf.set(RestOptions.PORT, REST_PORT);
        conf.setString("execution.checkpointing.unaligned.enabled", "true");
        conf.setString("rest.flamegraph.enabled", "true");
        conf.setString("taskmanager.numberOfTaskSlots", "100");
        conf.setString("execution.checkpointing.interval", "3s");
        conf.setString("execution.checkpointing.min-pause", "1s");
        conf.setString("jobmanager.scheduler", "adaptive");
        conf.setString("jobmanager.execution.dynamic-configuration.white-list", "*");
        // Strip the adaptive scheduler's artificial rescale delays so trigger->run reflects the
        // real
        // rescale latency, not the scheduler's cooldown / stabilization waits (default 30s / 60s /
        // 10s), which would also add ~30s between every step.
        conf.setString("jobmanager.adaptive-scheduler.executing.cooldown-after-rescaling", "0 s");
        conf.setString(
                "jobmanager.adaptive-scheduler.executing.resource-stabilization-timeout", "0 s");
        conf.setString(
                "jobmanager.adaptive-scheduler.submission.resource-stabilization-timeout", "0 s");
        conf.setString("state.checkpoints.dir", "file://" + CHECKPOINT_DIR);
        conf.set(WebOptions.CHECKPOINTS_HISTORY_SIZE, 100);
        conf.set(CheckpointingOptions.UNALIGNED_RECOVER_OUTPUT_ON_DOWNSTREAM, true);
        conf.set(CheckpointingOptions.CHECKPOINTING_DURING_RECOVERY_ENABLED, true);
        return conf;
    }

    private static void buildPipeline(StreamExecutionEnvironment env, int[] p) {
        final DataGeneratorSource<Long> generatorSource =
                new DataGeneratorSource<>(
                        value -> value,
                        Long.MAX_VALUE,
                        RateLimiterStrategy.perSecond(10000),
                        Types.LONG);

        // Three vertices at the initial per-vertex parallelism {source, map1, map2+sink}. The maps'
        // rebalance() breaks chaining; map2 and the sink have no rebalance between them, so they
        // chain into one vertex and share map2's parallelism.
        env.fromSource(generatorSource, WatermarkStrategy.noWatermarks(), "Data Generator")
                .setParallelism(p[0])
                .rebalance()
                .map(
                        value -> {
                            Thread.sleep(10);
                            return value;
                        })
                .setParallelism(p[1])
                .rebalance()
                .map(
                        value -> {
                            Thread.sleep(20);
                            return value;
                        })
                .setParallelism(p[2])
                .sinkTo(new DiscardingSink<>())
                .name("MySink")
                .setParallelism(p[2]);
    }
}
