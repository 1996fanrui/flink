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
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class MultiTaskBackPressureWithNewSource {

    private static final Logger LOG =
            LoggerFactory.getLogger(MultiTaskBackPressureWithNewSource.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = getEnv();

        env.fromSource(
                new DataGeneratorSource<>(index -> index, 10, RateLimiterStrategy.perSecond(10), Types.LONG),
                WatermarkStrategy.noWatermarks(), "Tuples Source")
                .map((MapFunction<Long, Long>) value -> value)
                .name("Map___1")
                .rebalance()
                .map((MapFunction<Long, Long>) value -> value)
                .name("Map___2")
                .rebalance()
                .map((MapFunction<Long, Long>) value -> value)
                .name("Map___3")
                .rebalance().print();

        env.execute(MultiTaskBackPressureWithNewSource.class.getSimpleName());
    }

    private static StreamExecutionEnvironment getEnv() {
        String OSType = System.getProperty("os.name");
        LOG.info("start job on {}", OSType);

        Configuration conf = new Configuration();
        conf.setString("rest.flamegraph.enabled", "true");
        conf.setString("state.backend", "hashmap");
        conf.setString("minicluster.number-of-taskmanagers", "3");
        conf.setString("taskmanager.numberOfTaskSlots", "2");
//        conf.setString("jobmanager.scheduler", "Adaptive");

        StreamExecutionEnvironment env =
                OSType.startsWith("Mac OS") ? getIdeaEnv(conf) : getProdEnv(conf);
        env.enableCheckpointing(TimeUnit.SECONDS.toMillis(30), CheckpointingMode.EXACTLY_ONCE);
        return env;
    }

    private static StreamExecutionEnvironment getProdEnv(Configuration conf) {
        return StreamExecutionEnvironment.getExecutionEnvironment(conf);
    }

    private static StreamExecutionEnvironment getIdeaEnv(Configuration conf) {
//        conf.set(RestOptions.PORT, 34567);
//        conf.setString("state.checkpoint-storage", "filesystem");
//        conf.setString("state.checkpoints.dir", "file:///tmp/flinkjob");
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(5);
        return env;
    }
}
