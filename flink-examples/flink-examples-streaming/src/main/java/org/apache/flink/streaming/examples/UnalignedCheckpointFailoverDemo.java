/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.examples;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class UnalignedCheckpointFailoverDemo {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set(RestOptions.PORT, 12346);

        conf.setString("taskmanager.numberOfTaskSlots", "100");
        conf.setString("rest.flamegraph.enabled", "true");

        conf.setString("execution.checkpointing.unaligned.enabled", "true");
        conf.setString("execution.checkpointing.aligned-checkpoint-timeout", "1s");
        conf.setString("execution.checkpointing.interval", "5s");
        conf.setString("execution.checkpointing.num-retained", "100");
        conf.setString("jobmanager.scheduler", "adaptive");

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(3);

        DataGeneratorSource<Long> generatorSource =
                new DataGeneratorSource<>(
                        value -> value,
                        Long.MAX_VALUE,
                        RateLimiterStrategy.perSecond(1000),
                        Types.LONG);

        env.fromSource(generatorSource, WatermarkStrategy.noWatermarks(), "Data Generator")
                .rebalance()
                .map(new FailoverMapper())
                .map(
                        value -> {
                            Thread.sleep(500);
                            return value;
                        })
                .print();

        env.execute(UnalignedCheckpointFailoverDemo.class.getSimpleName());
    }

    public static class FailoverMapper extends RichMapFunction<Long, Long>
            implements CheckpointedFunction {

        @Override
        public Long map(Long value) throws Exception {
            return value;
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            if (getRuntimeContext().getTaskInfo().getIndexOfThisSubtask() == 0) {
                long checkpointId = context.getCheckpointId();
                if (checkpointId == 3) {
                    throw new RuntimeException("Excepted exception");
                }
            }
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {}
    }
}
