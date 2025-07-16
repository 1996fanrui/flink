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
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.util.Collector;

import org.apache.commons.math3.random.RandomDataGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/** FLINK-35051: Reproduce the UC doesn't work well with Async Operator. */
public class AsyncFunctionWithUC {

    private static final Logger LOG = LoggerFactory.getLogger(AsyncFunctionWithUC.class);

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setString("rest.port", "8081");
        conf.setString("jobmanager.scheduler", "adaptive");

        conf.setString("execution.checkpointing.unaligned.enabled", "true");
        conf.setString("execution.checkpointing.interval", "10s");
        conf.setString("execution.checkpointing.min-pause", "8s");
        conf.setString("heartbeat.timeout", "10000s");
        conf.setString("execution.checkpointing.tolerable-failed-checkpoints", "0");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        env.setParallelism(5);

        env.fromSource(
                        new DataGeneratorSource<>(
                                value -> new RandomDataGenerator().nextHexString(300),
                                Long.MAX_VALUE,
                                RateLimiterStrategy.perSecond(100000),
                                Types.STRING),
                        WatermarkStrategy.noWatermarks(),
                        "Source Task")
                .keyBy(new KeySelector<String, Integer>() {
                    @Override
                    public Integer getKey(String value) throws Exception {
                        return value.hashCode() % 100;
                    }
                })
//                .flatMap(new AmplificationAndSleep(2, false))
//                .rebalance()
                .flatMap(new AmplificationAndSleep(10, false))
                .sinkTo(new DiscardingSink<>());

        env.execute(AsyncFunctionWithUC.class.getSimpleName());
    }

    private static class AmplificationAndSleep<V> implements FlatMapFunction<V, V>, CheckpointedFunction {

        private final int factor;
        private final boolean print;

        public AmplificationAndSleep() {
            this(10, true);
        }

        public AmplificationAndSleep(int factor, boolean print) {
            this.factor = factor;
            this.print = print;
        }

        @Override
        public void flatMap(V value, Collector<V> out) throws Exception {
            if (print) {
                LOG.info("flatMap");
            }
            for (int i = 0; i < factor; i++) {
                Thread.sleep(1);
                out.collect(value);
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) {
//            if (context.getCheckpointId() == 3) {
//                throw new IllegalStateException("Mocked exception!");
//            }
        }

        @Override
        public void initializeState(FunctionInitializationContext context) {

        }
    }

}
