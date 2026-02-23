# Jira Ticket

**Title:** [FLINK-39140] Enhance Unaligned Checkpoint ITCases to perform checkpointing during recovery

**Issue Type:** Sub-task of FLIP-547: Support checkpoint during recovery
**Component:** Runtime / Checkpointing

---



Current Unaligned Checkpoint ITCases only restart once from a normal checkpoint. They do not cover restoring from a checkpoint produced by recovery phase — which is the key scenario for checkpointing during recovery.

Proposed mechanism: After restoring from a checkpoint, wait for the first new checkpoint to be produced, then immediately trigger a restart from it. Repeat for a configurable number of rounds (≥ 2). Whether to rescale depends on the specific test case.

This mechanism works on the current master (validating normal checkpoint recovery). Once checkpointing during recovery is enabled, the same tests automatically cover recovery-phase checkpoint scenarios.
Affected ITCases

    UnalignedCheckpointRescaleITCase
    UnalignedCheckpointRescaleWithMixedExchangesITCase
    UnalignedCheckpointITCase
    UnalignedCheckpointCompatibilityITCase
    UnalignedCheckpointStressITCase
    UnalignedCheckpointFailureHandlingITCase
