package org.apache.flink.streaming.examples.datagen;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * Test for adaptive scheduler.
 */
public class AdaptiveSchedulerDemo {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setString("restart-strategy", "exponential-delay");
        conf.setString("restart-strategy.exponential-delay.initial-backoff", "20 s");
        conf.setString("restart-strategy.exponential-delay.attempts-before-reset-backoff", "5");

        // 1. case1 is default(streaming job with region failover): Job execution failed immediately

        // 2. case2 is streaming job with full failover: Job execution failed after 50s (5 * 10s)
//        conf.setString("jobmanager.execution.failover-strategy", "full");

        // 3. case3 is batch job with region failover: Job execution failed immediately
        // Note: In order to check the restartAttempts is increased rapidly, we should use DefaultScheduler.
        // Because default scheduler is AdaptiveBatchScheduler for batch job. It will use parallelism=1 for this job,
        // 1 region cannot reproduce this issue.
//        conf.setString("execution.runtime-mode", "BATCH");
//        conf.setString("jobmanager.scheduler", "Default");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(6);

        DataGeneratorSource<Long> generatorSource =
                new DataGeneratorSource<>(
                        value -> value,
                        3000,
                        RateLimiterStrategy.perSecond(100),
                        Types.LONG);

        env.fromSource(generatorSource, WatermarkStrategy.noWatermarks(), "Data Generator")
//                .rebalance()
                .addSink(new SinkFunction<Long>() {
                    @Override
                    public void invoke(Long value, Context context) {
                        throw new RuntimeException("Expected exception.");
                    }
                })
                .name("MySink");

        env.execute(AdaptiveSchedulerDemo.class.getSimpleName());
    }

}
