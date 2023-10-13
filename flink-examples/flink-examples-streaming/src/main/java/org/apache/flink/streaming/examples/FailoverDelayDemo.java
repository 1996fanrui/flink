package org.apache.flink.streaming.examples;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/** Test for the failover delay. */
public class FailoverDelayDemo {

    private static final Logger LOG = LoggerFactory.getLogger(FailoverDelayDemo.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = getEnv();
        Configuration globalJobParameters = new Configuration();
        globalJobParameters.setString("aaaaaaaaaaaaa", "bbbbbbbbbbbb");
        env.getConfig().setGlobalJobParameters(globalJobParameters);

        DataGeneratorSource<Long> generatorSource =
                new DataGeneratorSource<>(
                        value -> value,
                        Long.MAX_VALUE,
                        RateLimiterStrategy.perSecond(1),
                        Types.LONG);

        env.fromSource(generatorSource, WatermarkStrategy.noWatermarks(), "Data Generator")
                .map((MapFunction<Long, Long>) value -> value)
                .name("Map___1")
                .print();

        env.execute(FailoverDelayDemo.class.getSimpleName());
    }

    private static StreamExecutionEnvironment getEnv() {
        String OSType = System.getProperty("os.name");
        LOG.info("start job on {}", OSType);

        Configuration conf = new Configuration();
        conf.setString("restart-strategy", "exponential-delay");
        conf.setString("restart-strategy.exponential-delay.initial-backoff", "1s");
        conf.setString("conf_key_aaaaaaaaaaaaaaaa", "conf_value_aaaaaaaaaaaaaaaa");

        StreamExecutionEnvironment env =
                OSType.startsWith("Mac OS") ? getIdeaEnv(conf) : getProdEnv(conf);
        env.enableCheckpointing(TimeUnit.SECONDS.toMillis(15), CheckpointingMode.EXACTLY_ONCE);
        return env;
    }

    private static StreamExecutionEnvironment getProdEnv(Configuration conf) {
        return StreamExecutionEnvironment.getExecutionEnvironment(conf);
    }

    private static StreamExecutionEnvironment getIdeaEnv(Configuration conf) {
        conf.setString("state.checkpoint-storage", "filesystem");
        conf.setString("state.checkpoints.dir", "file:///tmp/flinkjob");
        //        StreamExecutionEnvironment env =
        // StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(5);
        return env;
    }
}
