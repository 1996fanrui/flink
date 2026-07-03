# FLINK-38544 Spilling v2 — PR Split Plan (Discussion Draft)

**Scope**: `46d4b743b90` (inclusive; confirmed — this is the pre-range pure-refactor commit `[FLINK-38544][network] Decouple LocalInputChannel recovery wiring from toBeConsumedBuffers`, 1 file +42/-21, not on master) → `c9e831e2cdd`. 20 commits, 101 files, ~+8100/-1160 lines.
**Goal**: re-split into 6 PRs (+1 optional standalone hotfix PR). All leading PRs are refactors or inert introductions; only the last PR wires spilling end to end. Every PR compiles on its own and passes tests.

---

## 0. Splitting Principles

1. **Four types of changes, strictly in this order:**
   - **Type 1 — Pure refactoring** first: no logic or semantic change whatsoever (e.g. splitting `AbstractInputChannelRecoveredStateHandler` and its subclasses out of the monolithic handler).
   - **Type 2 — Changes that work with the non-spilling logic**: the filtering-logic refactor, and the reworked filtering/task interaction — on master the task started processing data while filtering was still running; after this change the task starts processing only after all buffers have been filtered/consumed (this involves the future-related changes on the gates and StreamTask).
   - **Type 3 — Introduce the spilling classes without applying them**: new classes land fully unit-tested, but nothing in production code paths exercises them (handler factory not switched, `needsRecovery` never true, trigger/writer stay `NO_OP`).
   - **Type 4 — Apply the spilling logic** end to end, gated behind `execution.checkpointing.during-recovery.enabled`.
2. **Every PR compiles and everything keeps running**: the whole tree builds, all existing unit tests and ITCases pass unmodified — except tests of APIs that the PR itself deliberately changes or removes — and default-configuration behavior is unchanged until the final "apply" PR.
3. **Each commit inside a PR carries a single concern and should compile on a best-effort basis** — compilability per commit is desirable but not a hard guarantee (only the PR as a whole must compile and pass tests); tests ride with the commit that motivates them.
4. **Independent parts are merged separately as individual PRs** (fetched state; input channels — one PR, separate commits for local/remote; StreamTask; barrier handlers and checkpointing of fetched state): at least 5 PRs, each with multiple commits.

---

## 1. Analysis Summary (basis for the split)

### 1.1 Dependency direction (the most important fact for the split)

```
                    ┌──────────────────────────────────────────┐
                    │ PR6: StreamTask end-to-end wiring (apply) │
                    └───────┬─────────────┬───────────┬────────┘
                            │             │           │
        ┌───────────────────▼──┐   ┌──────▼────────┐ ┌▼──────────────────────┐
        │ PR5: barrier-handler │   │ PR4: fetched  │ │ PR3: input channel    │
        │ 3-step coordination  │──▶│ state read/   │▶│ push-based recovery   │
        │ + writer plumbing    │   │ write (inert: │ │ (inert: needsRecovery │
        │ (inert: NO_OP)       │   │ factory not   │ │ always false)         │
        └──────────────────────┘   │ switched)     │ └───────────┬───────────┘
                                   └──────┬────────┘             │
                                   ┌──────▼─────────────────────▼────────────┐
                                   │ PR2: recovery/processing ordering rework │
                                   │      (futures unified)                   │
                                   │ PR1: pure refactoring (handler split, …) │
                                   └──────────────────────────────────────────┘
```

Key facts (all verified against the actual diffs):

1. **The network side does not depend on the FetchedState classes.** `LocalInputChannel` / `RemoteInputChannel` / the gates only need the two new sentinel classes (`RecoveryCheckpointBarrier`, `EndOfFetchedChannelStateEvent`) to compile. The dependency points the other way: `FetchedChannelStateDrainer` imports `RecoverableInputChannel`.
2. **All new channel-side logic is gated on `needsRecovery` / `inRecovery`.** Across the whole codebase only `UnknownInputChannel` and the default gate path construct channels, and they pass `needsRecovery=false`, in which case behavior is byte-for-byte equivalent to master (the `else` branch of `RemoteInputChannel.checkpointStarted` is identical to master). → "introduce without applying" is cleanly achievable at the channel layer.
3. **`NoSpillingHandler.recover()` is a verbatim extraction of the old `InputChannelRecoveredStateHandler` non-filtering branch**; the channel-mapping logic (`getMappedChannels` / `calculateMapping` / `getChannel`) can be extracted verbatim into an abstract base; `ResultSubpartitionRecoveredStateHandler` is completely untouched. → The handler split can be a strict pure refactor.
4. **Filtering is a behavioral rewrite, not a refactor**: `ChannelStateFilteringHandler.filterAndRewrite` changed from "produce a `List<Buffer>` delivered to the channel" to "write into a `DataOutputSerializer` that goes to disk". This must belong to the spilling-introduction stage; it cannot go into the pure-refactor PR.
5. **The barrier handlers' 3-step coordination is fully inert when trigger/writer are `NO_OP`**, and `InputProcessorUtil` keeps the old signature as a NO_OP delegator. → The skeleton can be merged early.
6. **`SequentialChannelStateReader.readInputData` changes its return type `void` → `Optional<FetchedChannelState>`** — this is the coupling point between StreamTask and the FetchedState classes. In the re-split, this signature change goes into PR4; the StreamTask call site merely ignores the return value to compile.
7. **Netty has a wire-format change**: `PartitionRequest` gains a `needsRecovery` boolean (one extra byte per message). Safe within a single-version cluster, incompatible across mixed TM versions (Flink does not support cross-version network protocol anyway).
8. **The back-and-forth in the original branch disappears naturally in the re-split**: `CHECKPOINT_DECLINED_TASK_NOT_READY` introduced then reverted (`3554802e20b` → `3b828b05853` → `c9e831e2cdd`), the persist-window closed-then-fixed (`9a0a3061359`) — the re-split writes the final form directly.
9. One piece of dead code can be dropped along the way: `SequentialChannelStateReaderImpl.producedChannelState` is never read or written.

### 1.2 Feature flag

`execution.checkpointing.during-recovery.enabled` (`CheckpointingOptions.CHECKPOINTING_DURING_RECOVERY_ENABLED`, `@Experimental`, default **false**), already present on the base commit. All "introduce without applying" stages are gated on it; the factory switch (`AbstractInputChannelRecoveredStateHandler.create()` returning the spilling variants) is deferred to the final apply PR.

---

## 2. PR Series Overview

| # | PR | Type | Depends on | Main content |
|---|----|------|------------|--------------|
| 0 | multiple-input END_OF_INPUT hotfix | standalone fix | none | `StreamMultipleInputProcessor`, 1 file (may be folded into PR2) |
| 1 | Pure refactoring | pure refactor | none | handler class-hierarchy split, `getChannel(InputChannelInfo)`, `46d4b743b90` |
| 2 | Recovery/processing ordering rework | behavior change (non-spilling) | PR1 (weak) | unify futures on `getStateConsumedFuture`, async StreamTask recovery, defer finish, gate-API cleanup |
| 3 | Input channel push-based recovery | introduce, not applied | PR1, PR2 | `RecoverableInputChannel`, local/remote push recovery, sentinel events, netty, gate adaptation |
| 4 | Fetched channel state read/write | introduce, not applied | PR1; PR3 (drainer only) | `FetchedChannelState*`, spill-writing handlers, filter-to-disk rewrite, `RecoveryCheckpointTrigger`, drainer |
| 5 | Checkpoint coordination (barrier handlers + writer) | introduce, not applied | PR4 | `addInputDataFromSpill` writer plumbing, `ChannelState` 3-step dispatch, barrier-handler wiring |
| 6 | Wire spilling end to end | apply | PR2–PR5 | StreamTask `recoverChannelsWithCheckpointing`, factory switch, v1 filtering path removal, ITCases |

### 2.1 Files touched by multiple PRs

About 85% of the files (all new classes, netty, `BufferManager`, `ChannelStateFilteringHandler`, the barrier handlers, the writer plumbing, and the vast majority of tests) appear in exactly one PR. The hub files below are touched by several PRs — an inherent and deliberate consequence of drawing PR boundaries by semantic stage (pure refactor / behavior change / inert introduction / apply): each PR's diff in such a file carries exactly one concern, which reviews far better than one large mixed diff in a single PR.

**Main code:**

| File | Touches | Concern per PR |
|---|---|---|
| `StreamTask.java` | 4 (PR2/4/5/6) | PR2 async recovery rework + defer finish; PR4 adapt to the new `readInputData` return type (~1 line); PR5 `recoveryCheckpointTrigger` field + getter (always NO_OP); PR6 full `recoverChannelsWithCheckpointing` wiring |
| `RecoveredChannelStateHandler.java` | 3 (PR1/4/6) | PR1 pure class-hierarchy split; PR4 add `AbstractSpillingHandler` + the two spilling variants; PR6 factory switch + delete v1 `FilteringHandler` |
| `RecoveredInputChannel.java` | 3 (PR2/3/6) | PR2 remove the dead conversion-time buffer migration and `bufferFilteringCompleteFuture`; PR3 `toInputChannel(boolean)` and constructor reshape; PR6 remove the heap fallback |
| `InputGate.java` | 3 (PR1/2/3) | PR1 `getChannel(InputChannelInfo)`; PR2 remove `getBufferFilteringCompleteFuture`; PR3 `requestPartitions(boolean)` |
| `SingleInputGate.java` | 3 (PR1/2/3) | same three stages as `InputGate`, plus PR3 `stateConsumedFuture` aggregation over `RecoverableInputChannel` |
| `UnionInputGate.java` | 3 (PR1/2/3) | same three stages as `InputGate` |
| `InputGateWithMetrics.java` | 3 (PR1/2/3) | delegation methods mechanically following the gate-API changes above |
| `LocalInputChannel.java` | 2 (PR1/3) | PR1 the pre-range refactor `46d4b743b90`; PR3 the push-based recovery body |
| `CheckpointedInputGate.java` | 2 (PR1/3) | PR1 `getChannel`; PR3 consume `EndOfFetchedChannelStateEvent` |

**Test infrastructure** (follows the API changes it belongs to):

| File | Touches |
|---|---|
| `MockInputGate` / `MockIndexedInputGate` / `SingleInputGateBuilder` | PR2 / PR3 |
| `InputChannelBuilder` / `TestInputChannel` | PR2 / PR3 |
| `LocalInputChannelTest` / `RemoteInputChannelTest` / `RecoveredInputChannelTest` | PR2 / PR3 / (partly PR6) |

Construction implication: these files must be hand-sliced into several intermediate shapes, each of which should compile — they are where lines are most easily lost or misplaced. The final-state `git diff` check in section 5 exists mainly to backstop them.

---

## 3. Detailed PR Design

> Commit titles follow the Flink convention (English). Each PR carries its own tests (see the Tests note per PR).

### PR 0 (optional): `[hotfix] Report END_OF_INPUT from multiple-input processor when all inputs finished`

- **Content**: cherry-pick `b19f9892cfc` as-is, 1 file +11/-1. The `NONE_AVAILABLE` branch returns `END_OF_INPUT` when `inputSelectionHandler.areAllInputsFinished()`, else `NOTHING_AVAILABLE`; fixes a busy-spin.
- **Independence**: imports no feature classes; fully valid standalone. The scenario that triggers it (a task reaching END_OF_INPUT during recovery and resuming after recovery completes) comes from PR2's defer-finish, so it could also be the first commit of PR2.
- **Recommendation**: ship as a small standalone PR first to keep PR2 lean.

---

### PR 1: `[FLINK-38544] Preparatory refactoring for channel state recovery (no behavior change)`

**PR description (draft)**: Pure refactoring in preparation for spilling-based channel state recovery (FLINK-38544). No behavior or semantic change; all existing tests pass unmodified except for renamed trace labels.

| Commit | Description |
|---|---|
| 1. `[FLINK-38544][network] Decouple LocalInputChannel recovery wiring from toBeConsumedBuffers` | Pre-range pure-refactor commit `46d4b743b90`, cherry-picked as-is; the first commit of the whole series (inclusion confirmed). |
| 2. `[FLINK-38544][checkpoint] Extract AbstractInputChannelRecoveredStateHandler with concrete no-filtering/filtering handlers` | Split `InputChannelRecoveredStateHandler` into: abstract base `AbstractInputChannelRecoveredStateHandler` (channel mapping: `inputGates`/`channelMapping`/`rescaledChannels`/`oldToNewMappings` + `getMappedChannels`/`calculateMapping`/`getChannel`, all extracted verbatim) + `NoSpillingHandler` (verbatim extraction of the old non-filtering branch) + `FilteringHandler` (verbatim extraction of the old v1 filtering branch, incl. the reusable heap-segment `getPreFilterBuffer` logic) + a static `create(...)` factory (selects by flag, equivalent to the old internal if-branch). `ResultSubpartitionRecoveredStateHandler` untouched. Add `getProducedChannelState()` (always null) and the `closeInternal()` template hook. Trace label `InputChannelRecoveredStateHandler#recover` → `NoSpillingHandler#recover`. |
| 3. `[FLINK-38544][network] Add InputGate#getChannel(InputChannelInfo)` | New abstract method on `InputGate`; implemented by `SingleInputGate` / `UnionInputGate` (resolve by gateIdx — fixes the global-vs-local index semantics) / `InputGateWithMetrics` / `CheckpointedInputGate`; `AbstractStreamTaskNetworkInput` switches to `getChannel(channelInfo)`. Purely additive API. |
| 4. `[FLINK-38544][network] Additive NetworkActionsLogger#tracePersist overload; widen RecoveredInputChannel#releaseAllResources to public` | Mechanical visibility/logging refactor. |

**Note**: at final state the three subclasses are `NoSpillingHandler` / `SpillingNoFilteringHandler` / `SpillingWithFilteringHandler`. At the pure-refactor stage only two behaviors exist (non-filtering / v1 heap filtering), so the split here produces 2 concrete classes; PR4 introduces the spilling variants, PR6 deletes the v1 `FilteringHandler`. See Q3.

**Tests**: mechanical adaptations in `InputChannelRecoveredStateHandlerTest` / `RecoveredChannelStateHandlerTest` (labels/construction).

---

### PR 2: `[FLINK-38544] Rework recovery completion: start normal processing only after recovered state fully consumed`

This is the dedicated PR for the filtering/task-interaction change. **It is a behavior change, not a pure refactor**: on the base, the flag-on path ran the task concurrently with filtering (filtering delivered output as it went and the task consumed it; conversion was gated on `getBufferFilteringCompleteFuture`); after this PR, recovery completion is uniformly signaled by `getStateConsumedFuture()` (all recovered buffers consumed).

**PR description (draft)**: Unify recovery completion on the gates' state-consumed futures and restructure StreamTask channel-state recovery into an asynchronous future chain on the channelIOExecutor. Normal record processing (and task finish) now begin only after all recovered/filtered state has been fully consumed. Removes the filtering-progress gate API (`getBufferFilteringCompleteFuture`, `set/isCheckpointingDuringRecoveryEnabled`) that allowed processing to overlap with filtering.

| Commit | Description |
|---|---|
| 1. `[FLINK-38544][network] Remove recovery flags and filtering-complete future from the gate API` | Delete `InputGate.getBufferFilteringCompleteFuture()`, `IndexedInputGate.set/isCheckpointingDuringRecoveryEnabled()` and all overrides (`SingleInputGate` / `UnionInputGate` / `InputGateWithMetrics` / mocks); the flag is instead read from the job config at the call sites (content of the original `ba76b91c18e`). |
| 2. `[FLINK-38544] Restructure StreamTask channel-state recovery into an async future chain` | Rework `restoreStateAndGates`: submit the state read to the `channelIOExecutor`; each gate triggers `requestPartitions` off its **own** `getStateConsumedFuture()` (avoids the selective-reading multi-input deadlock hit in the original `731d30d3cd0`); `recoveryCompletionFuture = completeAll(...)`, on completion `mailboxProcessor.suspend()`; `restoreInternal` calls `get()` on the recovery future and rethrows the underlying cause. At this stage `readInputData` still returns `void` (the `Optional<FetchedChannelState>` signature is deferred to PR4). |
| 3. `[FLINK-38544] Defer task finish until recovery completes` | Original `87da2ec0b6a`: `processInput` suspends instead of finishing the task while recovery is incomplete. |
| 4. `[FLINK-38544][network] Remove now-dead recovered-buffer migration on channel conversion` | With recovery completion uniformly gated on state-consumed, `receivedBuffers` must be empty at conversion → delete the buffer migration in `toInputChannel()`, replace with `checkState(receivedBuffers.isEmpty())`; delete the `bufferFilteringCompleteFuture` field and the related race commentary. |

**Tests**: remove the cases for the deleted API in `SingleInputGateTest` / `UnionInputGateTest`, adapt `RecoveredInputChannelTest`, `TaskCheckpointingBehaviourTest`.

**⚠️ Discussion point (Q2)**: between this PR and PR6, the flag-on v1 "checkpoint during recovery" capability is effectively offline (the v1 filtering-complete gating is removed, the new trigger mechanism has not arrived yet). The flag is experimental, defaults to false, and v1 had correctness problems (the very motivation for v2) — I consider this acceptable, but it needs your confirmation.

---

### PR 3: `[FLINK-38544][network] Push-based recovery support in input channels (not yet wired)`

**PR description (draft)**: Teach the physical Local/RemoteInputChannel to be created directly in a recovery state and receive recovered buffers via the new `RecoverableInputChannel` interface, instead of buffering them in `RecoveredInputChannel` and migrating on conversion. All new code paths are gated on `needsRecovery` / `inRecovery`; nothing in this PR passes `needsRecovery=true`, so runtime behavior is unchanged. The drain that will push buffers lands in a follow-up.

Per your call: **one PR for all channels, separate commits for local/remote.**

| Commit | Description |
|---|---|
| 1. `[FLINK-38544][network] BufferManager: gate credit notification behind notifyInitiallyEnabled` | New ctor parameter + `enableNotify()`; all existing callers pass `true`, behavior unchanged. |
| 2. `[FLINK-38544][network] Add RecoverableInputChannel contract and recovery sentinels` | The `RecoverableInputChannel` interface (`onRecoveredStateBuffer` / `finishRecoveredBufferDelivery` / `insertRecoveryCheckpointBarrierIfInRecovery` / `requestRecoveryBufferBlocking` / `onRecoveredStateConsumed` / `getStateConsumedFuture`); `EndOfFetchedChannelStateEvent` (tail sentinel, serialized only via its dedicated `EventSerializer` tag 14); `RecoveryCheckpointBarrier` (per-checkpoint sentinel inside the recovery queue, tag 13); the two new `EventSerializer` tags. |
| 3. `[FLINK-38544][network] LocalInputChannel: push-based recovery state` | Implement `RecoverableInputChannel`: `needsRecovery` ctor param; `recoveredBuffers` becomes `Deque<Buffer>` (itself the monitor); `getNextBuffer()` rewritten under a single `inRecovery` predicate (recovered → priority event → empty; live data hidden); `wrapRecoveredBufferAsAvailability()` (FileRegion/Composite materialization, recovery sequence numbers, `peekNextDataType`); `upstreamReady` future; a `BufferManager` created only when `needsRecovery` to lend exclusive buffers to the drain; `checkpointStarted` split into mutually exclusive in-recovery / normal branches + `collectPreRecoveryBarrier` (missing sentinel throws `IOException` → wrapped as `CHECKPOINT_DECLINED`; write the final form directly, do not re-introduce the later-reverted TASK_NOT_READY). **Persist window: the in-recovery branch only calls `startPersisting`, never an early `stopPersisting`** (absorbs the `9a0a3061359` fix directly; the data-loss bug is never introduced). |
| 4. `[FLINK-38544][network] RemoteInputChannel: push-based recovery state` | The remote counterpart: `appendRecoveredBuffer` goes straight into `receivedBuffers` (consume path identical to the normal case); `recoveryEventStash` (upstream events arriving under suppressed credit during recovery are stashed and replayed after the sentinel is consumed); `upstreamReady` as a `CountDownLatch`; `onRecoveredStateConsumed` → unstash + `bufferManager.enableNotify()` + complete the future; `checkReadability` allows reads while in recovery; new getter `needsRecovery()`. Also written directly in its final persist-window form. |
| 5. `[FLINK-38544][network] Thread needsRecovery through gates and recovered-channel conversion` | `requestPartitions(boolean)` (default overload ignores it), `convertRecoveredInputChannels(boolean)`, `toInputChannel(boolean)`, `UnknownInputChannel` adapted to the new ctor signatures (passes false), `SingleInputGate.getStateConsumedFuture()` aggregates the `RecoverableInputChannel` futures. |
| 6. `[FLINK-38544][network] Propagate needsRecovery in PartitionRequest; start view reader with zero credit` | Wire format: `PartitionRequest` +1 boolean; `CreditBasedSequenceNumberingViewReader` uses `needsRecovery ? 0 : initialCredit`; mechanical pass-through on server/client. The PR description must explicitly note the wire change and the single-version-cluster assumption. |
| 7. `[FLINK-38544][network] CheckpointedInputGate: consume EndOfFetchedChannelStateEvent` | On polling the tail sentinel, assert the channel is a `RecoverableInputChannel` and call `onRecoveredStateConsumed()`. |

**Tests**: new recovery cases in `LocalInputChannelTest` / `RemoteInputChannelTest`; `Local/RemoteRecoveredInputChannelTest`; adaptations of `InputChannelBuilder` / `TestInputChannel` / `SingleInputGateBuilder`; all netty-side test adaptations; `RecoveryCheckpointBarrierTest`.

---

### PR 4: `[FLINK-38544][checkpoint] Fetched channel state: spill-file write, filtered rewrite, read and drain`

**PR description (draft)**: Introduce the spill-file subsystem for recovered channel state: an append-only segmented on-disk format written by the recovery read path (optionally through the rescale record filter), a ref-counted container with snapshot/resume semantics, a forward-only reader, and a drainer that pushes segments into `RecoverableInputChannel`s. The handler factory still returns the existing handlers, so nothing produces or consumes spill files yet.

| Commit | Description |
|---|---|
| 1. `[FLINK-38544][checkpoint] Add FetchedChannelState container and snapshot` | `FetchedChannelState` (ordered list of spill files, `acquire`/`release` ref counting, files deleted at zero) + `FetchedChannelStateSnapshot` (one-shot resume point). Depends only on the package + `java.nio`. |
| 2. `[FLINK-38544][checkpoint] Spill-writing handlers: segmented on-disk format` | `AbstractSpillingHandler` (a `DataOutputSerializer` accumulates `[gateIdx][channelIdx][len][body]` segments, backfills length via `writeIntUnsafe`, flushes through `OffsetAwareOutputStream`, rotates at 64 MB, `closeInternal` seals and produces a `FetchedChannelState`) + `SpillingNoFilteringHandler` (verbatim byte pass-through). `flink-core OffsetAwareOutputStream` ctor widened to public (1 line). The factory does not return them yet. |
| 3. `[FLINK-38544][checkpoint] Rewrite ChannelStateFilteringHandler to emit into a spill segment` | `filterAndRewrite` signature `List<Buffer>`+`BufferSupplier` → `void`+`DataOutputSerializer`; delete the buffer-chunking write-back machinery; add `SpillingWithFilteringHandler` (its `getBuffer` heap pre-filter-segment logic migrated verbatim from the v1 `FilteringHandler`). The v1 `FilteringHandler` stays for now (the factory still points at it); PR6 deletes it. See Q3. |
| 4. `[FLINK-38544][checkpoint] Forward-only spill reader with snapshot/resume` | The `FetchedChannelStateReader` interface (`nextSegment` / `snapshot` / `SpillSegment`) + `FetchedChannelStateReaderImpl` (sequential IO, bounded body stream, `current`/`committed` cursors, snapshot resume). |
| 5. `[FLINK-38544][checkpoint] RecoveryCheckpointTrigger + FetchedChannelStateDrainer` | The trigger interface (`NO_OP`/`NOT_READY` singletons, `snapshotAndInsertBarriers(cpId)` returning a reader); the drainer implements the trigger: `drain()` does disk reads / buffer requests outside the lock, delivery + advance inside; calls `finishRecoveredBufferDelivery()` at the end; `snapshotAndInsertBarriers` atomically snapshots and inserts `RecoveryCheckpointBarrier`s into channels still in recovery. **Depends on PR3's `RecoverableInputChannel`.** |
| 6. `[FLINK-38544][checkpoint] SequentialChannelStateReader#readInputData returns Optional<FetchedChannelState>` | The signature change + `SequentialChannelStateReaderImpl` switches to the `create(...)` factory + close-ordering rework (flush the filter writer before publishing the spill file) + the StreamTask call site ignores the return value. Do not introduce the useless `producedChannelState` field (dead code on the original branch). |

**Tests**: `FetchedChannelStateTest` / `RefCountTest` / snapshot tests, `FetchedChannelStateReaderTest`, `FetchedChannelStateDrainerTest` / `DrainerConcurrencyTest`, `AbstractSpillingHandlerTest`, `TestSpillWriter`, `ChannelStateFilteringHandlerTest`, the `GateFilterHandler*Test` rewrites, `SequentialChannelStateReaderImplTest`, `RecoveredChannelStateHandlerFilterRoutingTest`.

**Parallelizability**: commits 1–4 have no dependency on PR3; only commit 5 (the drainer) does. To review PR3/PR4 in parallel, the drainer could move to PR6.

---

### PR 5: `[FLINK-38544][checkpoint] Checkpoint coordination for fetched channel state (inert until wired)`

This is the "barrier handlers and checkpointing of fetched state" PR. Trigger/writer default to `NO_OP`, so behavior is unchanged after merging.

**PR description (draft)**: Add the checkpoint-time coordination for spilled channel state: the channel-state writer learns to replay spill segments into a checkpoint (`addInputDataFromSpill`), and the barrier handlers dispatch checkpoint start through a single hook that atomically (1) snapshots the undrained spill slice and inserts recovery barriers, (2) starts persisting on live channels, (3) hands the snapshot reader to the writer. With the default `NO_OP` trigger/writer this is behaviorally inert.

| Commit | Description |
|---|---|
| 1. `[FLINK-38544][checkpoint] ChannelStateWriter#addInputDataFromSpill: replay spill segments into the checkpoint` | The interface method (NO_OP impl closes the reader), `ChannelStateWriterImpl` enqueue, `ChannelStateWriteRequest.replayInputDataFromSpill` (closes the reader on cancel), `ChannelStateCheckpointWriter.writeInputFromSpill` (per-segment `transferTo` + offset bookkeeping), the `ChannelStateSerializer.writeData(DataOutputStream, InputStream, int)` overload. |
| 2. `[FLINK-38544][checkpoint] ChannelState: 3-step checkpoint-start dispatch hook` | `ChannelState` holds trigger + writer (1-arg ctor defaults to NO_OP); `onCheckpointStartedForAllInputs`: snapshotAndInsertBarriers → per-input `checkpointStarted` → `addInputDataFromSpill`; on failure close the snap and rethrow `CheckpointException` as-is (routes to abort, not task failure). |
| 3. `[FLINK-38544][checkpoint] Thread RecoveryCheckpointTrigger through barrier-handler construction` | New `InputProcessorUtil` overload (old signature kept as a NO_OP delegator), `SingleCheckpointBarrierHandler.unaligned/alignedWithTimeout` gain params (`aligned` passes NO_OP), the per-input loop in `AlternatingCollectingBarriers` / `AlternatingWaitingForFirstBarrierUnaligned` replaced by `state.onCheckpointStartedForAllInputs(...)`, One/Two/MultipleInputStreamTask pass `StreamTask.getRecoveryCheckpointTrigger()` (this commit introduces the field + getter, always NO_OP; the lifecycle transitions are left for PR6). |

**Tests**: `ChannelStateWriterImplAddInputDataFromSpillTest`, `ChannelStateDispatcherTest`, `AlternatingCollectingBarriersDispatchHookTest`, `AlternatingWaitingForFirstBarrierUnalignedDispatchHookTest`, `MockChannelStateWriter`, `TestBarrierHandlerFactory` adaptation.

---

### PR 6: `[FLINK-38544] Enable checkpointing during recovery via spilled channel state`

**PR description (draft)**: Wire everything together behind `execution.checkpointing.during-recovery.enabled`: when the flag is on, recovered channel state is fetched (and rescale-filtered) into spill files, the physical channels are created in recovery state, a drainer pushes the spilled buffers into them while serving as the recovery-checkpoint trigger, and checkpoints taken during recovery snapshot the undrained slice. The v1 in-memory filtering path (and its unbounded heap fallback) is removed.

| Commit | Description |
|---|---|
| 1. `[FLINK-38544] StreamTask: recoverChannelsWithCheckpointing future chain` | The flag-on branch: trigger `NOT_READY` → `fetchChannelState` (IO executor) → `requestPartitions(state.isPresent())` (mailbox) → `collectPhysicalChannels` / `buildDrainer` → install the drainer as the live trigger → `drain()` (IO executor) → `completeAll(gates' stateConsumedFuture)` → trigger `NO_OP` (absorbs `be988a92980`: gate the swap on the gate futures, not on drain completion); synchronous short-circuit for source tasks with no input gates (avoids the restore-loop early-suspend race); all trigger mutations go through the mailbox. |
| 2. `[FLINK-38544][checkpoint] Switch the handler factory to the spilling handlers; drop the v1 filtering path` | `create(...)` returns `SpillingWithFilteringHandler` / `SpillingNoFilteringHandler` when the flag is on; delete the v1 `FilteringHandler`; delete the heap fallback in `RecoveredInputChannel.requestBufferBlocking` (superseded by spilling; the OOM path is gone). |
| 3. `[FLINK-38544] Harden RecordFilterContext for minimal and batch environments` | Original `01bb49e736c`: `disabled(String[] tmpDirectories)`; non-empty tmpDirs enforced only when the flag is on. |
| 4. `[FLINK-38544][test] IT cases for checkpointing during recovery` | `UnalignedCheckpointDuringRecoveryITCase`, `RescaleFilterLargeRecordOOMRegressionITCase`, `ChannelIOExecutorDrainSubmissionTest`, `RecoveredInputChannelRequestBufferBlockingHeapFallbackRemovedTest`, `CdrRecoveryRaceITCase` (marked incomplete), adaptations of `ChannelPersistenceITCase` / `RecordFilterContextTest`. |

---

## 4. Open Questions

- ~~**Q1 bottom of the range**~~ **Confirmed**: `46d4b743b90` (LocalInputChannel pure refactor) goes into PR1 as the first commit of the whole series.
- **Q2 flag-on capability regression in the intermediate state**: after PR2 and before PR6, the v1 flag-on behavior (processing concurrent with filtering + filtering-complete gating) is dismantled while v2 is not yet wired — in that window, flag-on degrades to "same recovery as flag-off + no checkpoints during recovery". The flag is experimental with default false, and v1 had correctness issues; I consider this acceptable. If not, the fallback is to defer PR2's gate-API removal to PR6 (PR2 then only does the StreamTask async rework + defer-finish, keeping the dual gating temporarily), at the cost of a bigger PR6.
- **Q3 lifetime of the v1 FilteringHandler**: my proposal is PR1 extracts it (pure refactor) → PR4 introduces the spilling variants without switching the factory → PR6 switches and deletes it. Alternative: PR4 switches the factory directly (after PR4, flag-on would write spill files with no drainer — flag-on recovery would hang) — not recommended, but it would shorten the life of transitional code.
- **Q4 where the "task processes only after filtering completes" semantics land**: strictly, this consists of two parts — (a) PR2's "recovery completion uniformly gated on state-consumed" (the futures rework); (b) "filter output goes to disk first, visible only after drain" (spilling itself, PR4/PR6). After PR2, flag-off behavior is unchanged and flag-on's concurrent consumption is removed; the full "all filtering finishes before processing starts" only holds end to end at PR6. If you want a non-spilling intermediate implementation of (b) before spilling (e.g. buffer the filter output on heap until filtering completes), that means writing transitional code that will be thrown away — I advise against it.
- **Q5 drainer location**: keep it in PR4 (cohesive, but PR4 then depends on PR3) or move it to PR6 (PR3/PR4 reviewable in parallel)? I lean toward keeping it in PR4.
- **Q6 PR0**: ship the hotfix as its own PR or fold it into PR2?

## 5. Construction Approach

The original commits cannot be reused one-by-one (they don't individually compile), so build the series as "final-state slices":

1. Branch `38544/pr1-refactor` off master; hand-construct each PR1 commit from the final-state code per the commit list above; per commit run `mvn -pl flink-runtime compile` + the relevant tests (best effort per commit, mandatory per PR).
2. Each subsequent PR branch is based on the previous one (stacked PRs); rebase after each merge.
3. **Final-state verification**: after the whole series is constructed, `git diff c9e831e2cdd <series-tip>` must be empty (or contain only deliberately discarded differences: the TASK_NOT_READY back-and-forth, the dead field, etc. — each item goes on an explicit discard list).
4. Each PR description ends with "Part N of the FLINK-38544 spilling v2 series" and links the preceding PRs.
