package org.apache.flink.streaming.examples.datagen;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Test for adaptive scheduler.
 */
public class AdaptiveSchedulerDemo {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setString("taskmanager.numberOfTaskSlots", "2");
        conf.setString("minicluster.number-of-taskmanagers", "3");
        conf.setString("rest.flamegraph.enabled", "true");

//        conf.setString("jobmanager.scheduler", "adaptive");
        conf.setString("slot.request.max-interval", "0ms");
        conf.setString("job.autoscaler.enabled", "true");
        conf.setString("job.autoscaler.scaling.enabled", "true");
        conf.setString("job.autoscaler.stabilization.interval", "1m");
        conf.setString("job.autoscaler.metrics.window", "2m");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(5);

        DataGeneratorSource<Long> generatorSource =
                new DataGeneratorSource<>(
                        value -> value,
                        20,
                        RateLimiterStrategy.perSecond(20),
                        Types.LONG);

        env.fromSource(generatorSource, WatermarkStrategy.noWatermarks(), "Data Generator")
                .rebalance()
                .map(x -> x)
                .rebalance()
                .map(x -> x).setParallelism(3)
                .rebalance()
                .print();

        env.execute(AdaptiveSchedulerDemo.class.getSimpleName());
    }

}
