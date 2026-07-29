# Too Long; Didn't Read

Flink 2.3 can fail during rescale restore from an unaligned checkpoint when a two-input task receives both its left input and right input from the same upstream stream.

## When It Happens

- The job restores from an unaligned checkpoint while changing parallelism.
- A two-input operator is used, for example `leftStream.connect(rightStream)`, and both inputs are derived from the same upstream operator.
- `UnalignedCheckpointRescaleSameUpstreamITCase` reproduces this with recovered-on-downstream both disabled and enabled.

## User-Visible Symptom

- The task fails during restore, so the job can enter a restart loop.
- Typical error:
  `Cannot select SubtaskConnectionDescriptor{inputSubtaskIndex=1, outputSubtaskIndex=0}; known channels are [SubtaskConnectionDescriptor{inputSubtaskIndex=0, outputSubtaskIndex=0}]`

## Cause

- During channel-state assignment, Flink used the position of the upstream assignment object to identify which input edge was being restored.
- With two inputs from the same upstream, the same object appears twice, so the second input is mistaken for the first input.
- Channel state is routed with the wrong mapping, and restore fails when the downstream task receives unexpected data.

## Current Reproduction

- `UnalignedCheckpointRescaleSameUpstreamITCase` feeds one numeric upstream stream into the left and right inputs of the same `CoMapFunction`.
- Without the runtime fix, both recovered-on-downstream settings fail with the same `Cannot select SubtaskConnectionDescriptor` signature.
