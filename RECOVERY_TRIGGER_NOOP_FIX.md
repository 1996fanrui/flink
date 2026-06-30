# Premature `NO_OP` Recovery-Checkpoint-Trigger Swap

## Bug

`StreamTask#recoverChannelsWithCheckpointing` swaps `recoveryCheckpointTrigger` to
`NO_OP` as soon as `drain()` completes. But `drain()` only *appends* the
`EndOfFetchedChannelStateEvent` sentinel — a channel actually leaves recovery
(`inRecovery = false`) only later, when the consume loop polls that sentinel in
`RemoteInputChannel#onRecoveredStateConsumed`.

In the window where `trigger == NO_OP` but a channel is still `inRecovery`, a
checkpoint barrier gets **no** `RecoveryCheckpointBarrier` inserted, so
`RemoteInputChannel#collectPreRecoveryBarrier` finds no sentinel and declines with
`CHECKPOINT_DECLINED_TASK_NOT_READY`.

Invariant to restore: the trigger must stay live until **every** channel has actually
consumed its recovery data.

## Option A — Count channels out of recovery

Aggregate the per-channel `onRecoveredStateConsumed()` calls (decrement a counter) and
swap to `NO_OP` when it hits zero.

- `onRecoveredStateConsumed()` is a plain per-channel `void` — no existing future, no
  aggregation to reuse.
- Needs new counting state plus cross-gate aggregation wired back to `StreamTask`.

## Option B — Reuse the existing aggregated `END_OF_RECOVERY` signal *(chosen)*

`AbstractStreamTaskNetworkInput#processEvent` already returns
`DataInputStatus.END_OF_RECOVERY` once `EndOfOutputChannelStateEvent` is consumed **and**
`CheckpointedInputGate#allChannelsRecovered()` is true — a single signal already
aggregated across all channels by `UpstreamRecoveryTracker`. It fires strictly *after*
every channel's `EndOfFetchedChannelStateEvent`, so all channels are guaranteed out of
recovery at that point. `StreamOneInputProcessor#processInput` already handles this
branch (`finishRecovery()`); hook the `NO_OP` swap there.

- No new state; reuses an existing, correctly-ordered, pre-aggregated signal.
- Smaller change, harder to get wrong.

## Decision

Option B. Remove the drain-completion `NO_OP` swap and perform it when `END_OF_RECOVERY`
is observed. Until then the drainer stays installed as the trigger — its
`snapshotAndInsertBarriers` still inserts sentinels into any channel that is still
`inRecovery`, so barriers arriving in the window are handled correctly.

> Follow-up (out of scope for this POC): multi-input tasks
> (`StreamTwoInputProcessor` / `StreamMultipleInputProcessor`) need the same hook,
> swapping only after **every** input reaches `END_OF_RECOVERY`.
