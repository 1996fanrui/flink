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
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;

public class UnalignedCheckpointDemo1 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set(RestOptions.PORT, 12346);
        conf.setString("execution.checkpointing.unaligned.enabled", "true");
        conf.setString("rest.flamegraph.enabled", "true");
        conf.setString("taskmanager.numberOfTaskSlots", "100");
        conf.setString("execution.checkpointing.interval", "10s");
        conf.setString("execution.checkpointing.min-pause", "8s");
        conf.setString("jobmanager.scheduler", "adaptive");
        conf.setString("jobmanager.execution.dynamic-configuration.white-list", "*");
        conf.setString("state.checkpoints.dir", "file:///tmp/flinkjob");
        conf.set(WebOptions.CHECKPOINTS_HISTORY_SIZE, 100);
        conf.set(CheckpointingOptions.UNALIGNED_RECOVER_OUTPUT_ON_DOWNSTREAM, true);
        conf.set(CheckpointingOptions.CHECKPOINTING_DURING_RECOVERY_ENABLED, true);

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(5);

        DataGeneratorSource<Long> generatorSource =
                new DataGeneratorSource<>(
                        value -> value,
                        Long.MAX_VALUE,
                        RateLimiterStrategy.perSecond(1000),
                        Types.LONG);

        env.fromSource(generatorSource, WatermarkStrategy.noWatermarks(), "Data Generator")
                .rebalance()
                .map(
                        value -> {
                            Thread.sleep(10);
                            return value;
                        })
                .rebalance()
                .map(
                        value -> {
                            Thread.sleep(20);
                            return value;
                        })
                .sinkTo(new DiscardingSink<>())
                .name("MySink");

        env.execute();
    }
}
