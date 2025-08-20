# 1. Phenomenon:

Job cannot be recovered from UC(unaligned checkpoint) after rescaling, and the exception is:

```
java.lang.UnsupportedOperationException: Cannot rescale the given pointwise partitioner.
Did you change the partitioner to forward or rescale?
It may also help to add an explicit shuffle().
```


UC is the abbreviation of unaligned checkpoint in this ticket.

#  2. Reason

## 2.1 What types of jobs trigger this bug?

When one upstream task has multiple output exchanges, which including UC SUPPORTED exchanges(likes hash or rebalance) and at least one UC UNSUPPORTED exchanges(likes Forward or rescale).

Or when one downstream task has multiple input exchanges, which including UC SUPPORTED exchanges(likes hash or rebalance) and at least one UC UNSUPPORTED exchanges(likes Forward or rescale).

## 2.2 Why does this bug happen?

When a job is rescaled and recovered from an unaligned checkpoint, Flink needs to redistribute in-flight buffers. The logic for this redistribution in `StateAssignmentOperation` performed a *task-level* check to see if there was any in-flight data associated with the task. In the previous implementation, this was done via global checks at the beginning of the redistribution methods, for example:

- For input buffers redistribution (`reDistributeInputChannelStates`), a check like `!stateAssignment.hasInputState() && !stateAssignment.hasUpstreamOutputStates()` would cause the method to exit early.
- A similar check existed for output buffers redistribution (`reDistributeResultSubpartitionStates`).

The bug occurred in mixed-partitioning scenarios because this check was not granular enough. For example, consider a source task with two outputs: one to a `keyBy` (which supports UC and has in-flight data) and one to a `forward` (which does not support UC and has no in-flight data).

During recovery, the task-level check would detect the in-flight data from the `keyBy` exchange and determine that the *entire task* has state. Consequently, the recovery logic would proceed to try and redistribute state for *all* of the task's exchanges, including the `forward` exchange. Since `ForwardPartitioner` does not support this operation, it would throw the `UnsupportedOperationException`, causing the recovery to fail. The core issue was the incorrect assumption that if one exchange has state, all exchanges for that task must also be processed.

## 2.3 Reproduce

The following job can reproduce this bug easily. A permanent integration test has been added in `org.apache.flink.test.checkpointing.UnalignedCheckpointRescaleWithMixedExchangesITCase`.

```java 

import org.apache.commons.math3.random.RandomDataGenerator;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * It could reproduce this issue:
 * Caused by: java.lang.UnsupportedOperationException: Cannot rescale the given pointwise partitioner.
 * Did you change the partitioner to forward or rescale?
 * It may also help to add an explicit shuffle().
 */
public class UnalignedCheckpointBugDemo {
    private static final Logger LOG = LoggerFactory.getLogger(UnalignedCheckpointBugDemo.class);

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        conf.setString("rest.port", "12348");
        conf.setString("execution.checkpointing.unaligned.enabled", "true");
        conf.setString("execution.checkpointing.interval", "10s");
        conf.setString("execution.checkpointing.min-pause", "8s");
        conf.setString("jobmanager.scheduler", "adaptive");
        conf.setString("state.checkpoints.dir", "file:///tmp/flinkjob");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.disableOperatorChaining();

        env.setParallelism(5);

        SingleOutputStreamOperator<String> stream1 = env.fromSource(
                        new DataGeneratorSource<>(
                                value -> new RandomDataGenerator().nextHexString(300),
                                Long.MAX_VALUE,
                                RateLimiterStrategy.perSecond(100000),
                                Types.STRING),
                        WatermarkStrategy.noWatermarks(),
                        "Source Task");

        stream1
                .keyBy(new KeySelectorFunction())
                .map(x -> {
                    Thread.sleep(50);
                    return x;
                }).name("Map after hash");

        stream1.map(x -> {
                    Thread.sleep(5);
                    return x;
            }).name("Map after forward");

        env.execute(UnalignedCheckpointBugDemo.class.getSimpleName());
    }

    private static class KeySelectorFunction implements KeySelector<String, Integer> {
        @Override
        public Integer getKey(String value) {
            return 0;
        }
    }
```

# 3. Solution

The implemented solution was to make the state redistribution logic more granular by checking for in-flight data on a **per-exchange** basis instead of a per-task basis.

1.  **Precise State Tracking:** The `TaskStateAssignment` class was refactored to no longer use a simple boolean flag. It now precisely tracks which specific input gates and result partitions contain in-flight data.

2.  **Per-Channel/Partition Checks:** The core redistribution methods, `reDistributeInputChannelStates` and `reDistributeResultSubpartitionStates`, were modified. Their internal logic now iterates through each input gate or output partition and uses new helper methods (`hasInFlightDataForInputGate` and `hasInFlightDataForResultPartition`) to check if that *specific channel* has state.

3.  **Conditional Logic:** The state redistribution logic is now wrapped in a conditional block. It is only invoked for a channel if the per-exchange check passes. This ensures that stateless exchanges (like `forward` or `rescale`) are correctly skipped, avoiding the exception.

This approach fixes the bug by applying the redistribution logic only where it is actually needed, allowing jobs with mixed partitioner types to rescale from an unaligned checkpoint successfully.
