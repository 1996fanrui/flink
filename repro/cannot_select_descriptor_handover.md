# Handover: `Cannot select SubtaskConnectionDescriptor` during restore

> Audience: engineers who need to explain, reproduce, review, or fix this issue.
> Branch: `repro/unaligned-channel-remap`, based on official `release-2.3.0`
> commit `c0f8d1a1e09`.

## 1. TL;DR

- A production Flink 2.3 job failed repeatedly while restoring from an unaligned
  checkpoint after Autopilot rescaled it from parallelism 2 to 11.
- The visible error was:
  `Cannot select SubtaskConnectionDescriptor{inputSubtaskIndex=1, outputSubtaskIndex=0}; known channels are [{0,0}]`.
- The bug is triggered when a two-input task receives both its left input and
  right input from the same upstream stream. In Flink terms, this can happen with
  `leftStream.connect(rightStream)` when `leftStream` and `rightStream` are both
  derived from the same upstream operator.
- `TaskStateAssignment` identifies the connection by the position of the
  upstream assignment object. With two edges from the same upstream, the same
  object appears twice, so `indexOf` always selects the first edge. The second
  edge then reuses the first edge's channel-state mapping.
- The issue is not introduced by
  `execution.checkpointing.unaligned.recover-output-on-downstream.enabled`. A
  `UnalignedCheckpointRescaleSameUpstreamITCase` reproduces the failure with
  this option both disabled and enabled.

## 2. Production Evidence

Original incident summary:

> Later around the time when I received the page (since 20:40 UTC), there are
> errors related to Cannot select
> SubtaskConnectionDescriptor{inputSubtaskIndex=1, outputSubtaskIndex=0} which
> interrupts the UDF call and causes the job to be restart constantly.
> Debugging further into 2nd issue, the restart crash loop was triggered by:
> - Autopilot requested a rescale from 2 -> 11
> - The adaptive scheduler executed the rescale
> - During restore, the RescalingStreamTaskNetworkInput failed to correctly remap
>   unaligned checkpoint channel state for the new topology

Stack trace excerpt:

```text
java.lang.IllegalStateException: Cannot select SubtaskConnectionDescriptor{inputSubtaskIndex=1, outputSubtaskIndex=0}; known channels are [SubtaskConnectionDescriptor{inputSubtaskIndex=0, outputSubtaskIndex=0}]
    at org.apache.flink.streaming.runtime.io.recovery.DemultiplexingRecordDeserializer.select(DemultiplexingRecordDeserializer.java:121)
    at org.apache.flink.streaming.runtime.io.recovery.RescalingStreamTaskNetworkInput.processEvent(RescalingStreamTaskNetworkInput.java:188)
    at org.apache.flink.streaming.runtime.io.StreamMultipleInputProcessor.processInput(StreamMultipleInputProcessor.java:86)
    at org.apache.flink.streaming.runtime.tasks.StreamTask.restoreInternal(StreamTask.java:864)
```

Confirmed facts:

| Fact | Source |
|---|---|
| The job was based on Flink 2.3 | User report |
| The job failed during restore after rescaling | Incident report and stack trace |
| The failing task was a multiple-input task | `StreamMultipleInputProcessor` in the stack |
| The error happened during restore | `StreamTask.restoreInternal` in the stack |
| The same issue reproduces with recovered-on-downstream on and off | `UnalignedCheckpointRescaleSameUpstreamITCase` |

## 3. What the Error Means

During rescale restore, one new physical channel can contain data that belonged
to several old channels. Flink uses two pieces of information to route the data:

1. The replayed data carries a `SubtaskConnectionDescriptor(oldSubtask, oldChannel)` label.
2. The receiver builds a lookup table from the rescale descriptor sent by the
   JobManager.

`DemultiplexingRecordDeserializer.select()` fails when the data label is outside
the receiver's lookup table. This is not a missing-state symptom. It means the
data was routed to a task that did not expect that old subtask/channel identity.

In the production-shaped failure, the receiver expected only `(0,0)` but received
`(1,0)`. The channel dimension matched; the old downstream subtask identity did
not.

## 4. Root Cause

The defect is in
`flink-runtime/src/main/java/org/apache/flink/runtime/checkpoint/TaskStateAssignment.java`.

The old code derives the edge index through object position:

```java
private static int getAssignmentIndex(
        TaskStateAssignment[] assignments, TaskStateAssignment assignment) {
    return Arrays.asList(assignments).indexOf(assignment);
}
```

This is ambiguous for this topology:

```text
same upstream stream
  |-- left input  --> two-input downstream task
  `-- right input --> same two-input downstream task
```

Both inputs point back to the same upstream `ExecutionJobVertex`, so the upstream
`TaskStateAssignment` object appears twice. `indexOf` returns `0` for both edges.
The second edge is treated as if it were the first edge.

The result is:

- channel state from the second input is distributed using the first input's
  mapping;
- the replayed handle still carries the second input's real identity;
- the downstream task builds its demux table from the real second-input mapping;
- data and lookup table disagree, so restore fails with `Cannot select
  SubtaskConnectionDescriptor`.

## 5. Reproduction

The regression test is
`flink-tests/src/test/java/org/apache/flink/test/checkpointing/UnalignedCheckpointRescaleSameUpstreamITCase.java`.

It builds the minimal topology:

```text
upstream sequence source
  |-- rebalance() --> left input  --.
  `-- keyBy(...)  --> right input --+--> CoMapFunction --> DiscardingSink
```

The test flow:

1. Run the job at parallelism 2.
2. Wait for 10 completed checkpoints and require the latest checkpoint to contain
   inflight data.
3. Cancel the job while retaining the externalized checkpoint.
4. Restore the same topology at parallelism 4.
5. Assert that restore succeeds and another checkpoint with inflight data can be
   completed.

Without the production fix, the test fails for both parameter values:

```text
recoverOutputOnDownstream=false
Cannot select SubtaskConnectionDescriptor{inputSubtaskIndex=1, outputSubtaskIndex=1}; known channels are [SubtaskConnectionDescriptor{inputSubtaskIndex=0, outputSubtaskIndex=1}]

recoverOutputOnDownstream=true
Cannot select SubtaskConnectionDescriptor{inputSubtaskIndex=1, outputSubtaskIndex=0}; known channels are [SubtaskConnectionDescriptor{inputSubtaskIndex=0, outputSubtaskIndex=0}]
```

This confirms that the bug is in the shared channel-state assignment logic, not
only in the recovered-on-downstream path.

## 6. Fix Direction

The connection identity must be derived from stable edge identity, not from the
position of a reused assignment object.

The fix should:

1. Replace assignment-object `indexOf` lookup with `IntermediateDataSetID` based
   lookup for both input-gate and result-partition descriptor generation.
2. Cover both directions, because the same ambiguity exists when deriving input
   descriptors and output descriptors.
3. Keep `UnalignedCheckpointRescaleSameUpstreamITCase` parameterized over
   `UNALIGNED_RECOVER_OUTPUT_ON_DOWNSTREAM` to prove both paths are fixed.

## 7. Review Checklist

- The test clearly asserts that the downstream two-input task has left and right
  inputs from the same upstream producer.
- The test fails before the runtime fix with the `Cannot select
  SubtaskConnectionDescriptor` signature.
- The runtime fix no longer relies on assignment object position for edge
  identity.
- Existing unaligned-checkpoint rescale tests still pass.
