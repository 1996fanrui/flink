package org.apache.flink.streaming.examples;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;

public class UnalignedCheckpointFailoverDemo {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set(RestOptions.PORT, 12345);

        conf.setString("taskmanager.numberOfTaskSlots", "100");
        conf.setString("rest.flamegraph.enabled", "true");

        conf.setString("execution.checkpointing.unaligned.enabled", "true");
        conf.setString("execution.checkpointing.aligned-checkpoint-timeout", "1s");
        conf.setString("execution.checkpointing.interval", "10s");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(3);

        DataGeneratorSource<Long> generatorSource =
                new DataGeneratorSource<>(
                        value -> value,
                        1000,
                        RateLimiterStrategy.perSecond(1000),
                        Types.LONG);

        env.fromSource(generatorSource, WatermarkStrategy.noWatermarks(), "Data Generator")
                .rebalance()
                .map(new FailoverMapper())
                .map(value -> {
                    Thread.sleep(20);
                    return value;
                })
                .print();

        env.execute(UnalignedCheckpointFailoverDemo.class.getSimpleName());
    }

    public static class FailoverMapper extends RichMapFunction<Long, Long> implements CheckpointedFunction {

        @Override
        public Long map(Long value) throws Exception {
            return value;
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            if (getRuntimeContext().getTaskInfo().getIndexOfThisSubtask() == 0) {
                long checkpointId = context.getCheckpointId();
                if(checkpointId == 3) {
                    throw new RuntimeException("Excepted exception");
                }
            }
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {

        }
    }

}
