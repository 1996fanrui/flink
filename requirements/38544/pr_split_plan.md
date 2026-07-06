# FLINK-38544 Spilling v2 — PR Split Plan

**Scope**: `46d4b743b90` (inclusive; the pre-range pure-refactor commit `[FLINK-38544][network] Decouple LocalInputChannel recovery wiring from toBeConsumedBuffers`, 1 file +42/-21, not on master) → `c9e831e2cdd`. 20 commits, 101 files, ~+8100/-1160 lines.
**Goal**: re-split into **7 PRs in two phases**, one JIRA per PR. Phase A (PR1–PR4) progressively completes all refactoring on the **in-memory** buffer path so that Phase B (PR5–PR7) swaps in spilling as a lightweight backend replacement. Every PR compiles, passes CI, and leaves a fully working tree.

**JIRA mapping** (each PR's commits are prefixed with its own JIRA id; the END_OF_INPUT `[hotfix]` commit rides in PR1 without a JIRA prefix):

| PR | JIRA | Title |
|----|------|-------|
| PR1 | FLINK-39520 | Preparatory refactoring for channel-state recovery |
| PR2 | FLINK-39521 | Push-based recovery support in input channels |
| PR3 | FLINK-39522 | Rework StreamTask recovery: async future chain, consume after filtering completes |
| PR4 | FLINK-39523 | Checkpoint coordination during recovery (memory-based trigger) |
| PR5 | FLINK-39524 | Fetched channel state: spill files, filtered write, reader |
| PR6 | FLINK-40080 | Drain spilled state into channels; replay spilled slices into checkpoints |
| PR7 | FLINK-40081 | Enable the spilling backend for checkpointing during recovery |

**Commit-message rules**: no AI co-author trailers in the final messages (the `Co-Authored-By: Claude …` trailers from the construction pass are stripped). Human attribution per the confirmed table in `attribution_and_renumbering_plan.md`: Roman Khachatryan is **git author** of 3 commits (PR1 hotfix, PR3 defer-finish, PR7 CdrRecoveryRaceITCase) and **Co-authored-by** on 2 commits (PR3 async chain, PR4 trigger); all other commits are Rui's.

---

## 0. Splitting Principles

1. **Four types of changes, strictly in this order:**
   - **Type 1 — Pure refactoring** first: no logic or semantic change whatsoever.
   - **Type 2 — Changes that work with the non-spilling logic**: push-based input channels, the reworked filtering/task interaction (the task starts consuming only after all buffers have been filtered — on master consumption overlapped with filtering), and memory-based checkpoint-during-recovery coordination.
   - **Type 3 — Introduce the spilling classes without applying them**: new classes land fully unit-tested, but no production path produces or consumes spill files.
   - **Type 4 — Apply the spilling logic**: swap the memory backend for the disk backend, gated behind `execution.checkpointing.during-recovery.enabled`.
2. **Every PR compiles and everything keeps running**: the whole tree builds, all existing unit tests and ITCases pass — except tests of APIs/behaviors the PR itself deliberately changes, removes, or (temporarily, with an explicit restore plan) disables — and default-configuration behavior is unchanged throughout.
3. **Each commit inside a PR carries a single concern and should compile on a best-effort basis** — compilability per commit is desirable but not a hard guarantee (only the PR as a whole must compile and pass tests); tests ride with the commit that motivates them.
4. **Independent parts are merged separately as individual PRs**; at least 5 PRs, each with multiple commits.
5. **Phase invariant (the big principle)**: at the end of Phase A, *everything except the storage backend is in its final form* — input-channel layer 100% final, thread/future model final, CDR (checkpointing during recovery) fully working on memory. Phase B may only: add new classes, replace the memory backend at clearly delimited seams, and delete transitional/memory-era code. **Phase B adds zero new logic to the input-channel layer.**

---

## 1. Analysis Summary (verified facts the split is built on)

### 1.1 The shared invariant of v1 and v2

**"Filtering must fully complete before a snapshot can start; a snapshot must not wait for consumption."** This is the core CDR requirement in both generations:

- **v1 (base)**: filtering runs eagerly on the `channelIOExecutor` with an *unbounded heap fallback* in `RecoveredInputChannel#getBuffer` so it never blocks on buffer availability. The restore loop consumes concurrently. The moment filtering completes (`bufferFilteringCompleteFuture`), the gate converts (`toInputChannel` migrates filtered-but-unconsumed buffers into the physical channel via constructor) and the restore loop suspends — the task goes RUNNING early, so barriers flow and the unconsumed recovered buffers are persisted as ordinary in-flight data. Memory-risky (OOM) but working.
- **v2 (this branch)**: simplification — filtering is fast, so **consumption starts only after filtering fully completes** (strict order), which makes the filtering/checkpoint coordination much simpler. Final form stores the filter output on disk and drains it incrementally; a `RecoveryCheckpointTrigger` inserts a `RecoveryCheckpointBarrier` per checkpoint into channels still in recovery, and `checkpointStarted`'s in-recovery branch persists everything before the barrier.

### 1.2 Dependency direction and phases

```
Phase A (memory backend)                                Phase B (disk backend)
────────────────────────────────────────────            ─────────────────────────────────────
PR1 pure refactor (incl. the [hotfix] commit)            PR5 fetched-state classes (new files)
  └─► PR2 input channels, FINAL FORM                         │
        └─► PR3 StreamTask async chain (memory)          PR6 drainer + spilled-state
              └─► PR4 CDR coordination (memory trigger)       checkpoint replay
                    = Phase A complete, CDR works             └─► PR7 apply: swap memory→disk,
                                                                   delete transitional code
```

Verified key facts:

1. **The network side does not depend on any fetched-state class.** Channels/gates only need `RecoveryCheckpointBarrier` and `EndOfFetchedChannelStateEvent` to compile. The dependency points the other way (`FetchedChannelStateDrainer` → `RecoverableInputChannel`), which is what allows the channel layer to reach final form in Phase A.
2. **All new channel logic is gated on `needsRecovery` / `inRecovery`**; with `needsRecovery=false` behavior is byte-for-byte master (e.g. `RemoteInputChannel.checkpointStarted`'s else-branch is identical to master).
3. **CDR requires barrier insertion even with a pure memory backend.** Multi-input scenario: a barrier arrives on channel A (already out of recovery); `checkpointStarted` fans out to all inputs; channel B, still in recovery, runs `collectPreRecoveryBarrier(cpId)` which *requires* a matching `RecoveryCheckpointBarrier` in its queue — otherwise the checkpoint is declined. Hence the trigger mechanism (memory version) belongs to Phase A.
4. **The memory backend needs no snapshot reader**: the in-memory push at conversion is one-shot, so there is never an "undrained residue"; the memory trigger only inserts barriers (snapshot inherently empty). All disk-snapshot replay (`addInputDataFromSpill` / `writeInputFromSpill`) is Phase B.
5. **`NoSpillingHandler` is a verbatim extraction** of the old non-filtering branch; the channel-mapping logic extracts verbatim into an abstract base; `ResultSubpartitionRecoveredStateHandler` is untouched → the handler split is a strict pure refactor. The **filtering rewrite** (deliver `List<Buffer>` → write `DataOutputSerializer` to disk) is behavioral and belongs to Phase B.
6. **Test randomization**: at both range base and head, `TestStreamEnvironment.randomizeConfiguration` does `randomize(conf, CHECKPOINTING_DURING_RECOVERY_ENABLED, true, false)` — ~50% of randomized ITCases run flag-on. Any window where flag-on is degraded needs the randomization pinned off (see PR2 commit 1 / PR4 commit 4). One dedicated flag-on ITCase exists at base: `RecoveredStateFilteringLargeRecordITCase`.
7. **Netty wire-format change**: `PartitionRequest` gains a `needsRecovery` boolean (+1 byte per message). Safe within a single-version cluster; the PR description must state it.
8. The original branch's back-and-forth disappears in the re-split: `CHECKPOINT_DECLINED_TASK_NOT_READY` introduced then reverted (`3554802e20b`→`3b828b05853`→`c9e831e2cdd`), the persist-window fix (`9a0a3061359`), the trigger-retirement timing fix (`be988a92980`) — final forms are written directly. Also do not re-introduce the dead `SequentialChannelStateReaderImpl.producedChannelState` field.

### 1.3 Feature flag

`execution.checkpointing.during-recovery.enabled` (`CheckpointingOptions.CHECKPOINTING_DURING_RECOVERY_ENABLED`, `@Experimental`, default **false**; requires `UNALIGNED_RECOVER_OUTPUT_ON_DOWNSTREAM`). Pre-exists on base. Default config is unaffected by every PR in the series.

---

## 2. PR Series Overview

| PR | JIRA | Phase / type | Depends on | Commits |
|----|------|--------------|------------|---------|
| 1 | FLINK-39520 | A / Type 1 (pure refactoring; incl. the standalone `[hotfix]` commit) | — | 5 |
| 2 | FLINK-39521 | A / Type 2 (inert + one deliberate v1 retirement) | PR1 | 8 |
| 3 | FLINK-39522 | A / Type 2 (behavior change, flag-on) | PR2 | 3 |
| 4 | FLINK-39523 | A / Type 2 | PR3 | 4 |
| 5 | FLINK-39524 | B / Type 3 | PR1 | 5 |
| 6 | FLINK-40080 | B / Type 3 | PR2, PR4, PR5 | 4 |
| 7 | FLINK-40081 | B / Type 4 | all | 7 |

Total: **7 PRs, 36 commits.** After PR1–PR4 the memory-buffer path runs fully on the new thread/future model with working CDR; PR5–PR7 focus exclusively on disk/drainer logic.

### 2.1 Progressive state at each merge point

| After | Default config (flag off) | Flag-on state |
|---|---|---|
| PR1 | unchanged (pure refactor + busy-spin hotfix) | unchanged |
| PR2 | unchanged (new paths inert) | **degraded** (v1 conversion-migration retired) — covered by the randomization pin in PR2 commit 1 |
| PR3 | unchanged (per-gate conversion = master) | recovery fully works, memory-based, strict filter-then-consume; checkpoints during recovery gracefully declined |
| PR4 | unchanged | **CDR fully works (memory)**; randomization restored → the whole randomized ITCase fleet validates Phase A |
| PR5 | unchanged | unchanged (factory not switched; nothing produces spill files) |
| PR6 | unchanged | unchanged (nothing constructs a drainer) |
| PR7 | unchanged | backend swapped to disk; the fleet must stay green |

### 2.2 Files touched by multiple PRs (main code)

~85% of files appear in exactly one PR. The hub files below are touched by several PRs deliberately — each touch carries one concern:

| File | Touches | Concern per PR |
|---|---|---|
| `StreamTask.java` | 4 (PR3/4/5/7) | PR3 async chain + defer finish; PR4 trigger field + lifecycle; PR5 assert new `readInputData` return (~1 line); PR7 fetch→drain swap |
| `RecoveredChannelStateHandler.java` | 3 (PR1/5/7) | PR1 pure split; PR5 spilling handlers; PR7 factory switch + delete v1 `FilteringHandler` |
| `RecoveredInputChannel.java` | 3 (PR2/3/7) | PR2 conversion-push + `toInputChannel(boolean)`; PR3 delete `bufferFilteringCompleteFuture`; PR7 delete conversion-push + heap fallback |
| `InputGate` / `SingleInputGate` / `UnionInputGate` / `InputGateWithMetrics` | 3 (PR1/2/3) | PR1 `getChannel(InputChannelInfo)`; PR2 `requestPartitions(boolean)` + future aggregation; PR3 remove filtering-complete API |
| `LocalInputChannel.java` | **2 (PR1/2)** | PR1 pre-range decoupling refactor; PR2 push recovery written once in final form |
| `RemoteInputChannel.java` | 1 (PR2) | final form in one PR |
| `CheckpointedInputGate.java` | 2 (PR1/2) | PR1 `getChannel`; PR2 sentinel consumption |
| `ChannelState.java` | 2 (PR4/6) | PR4 steps 1–2; PR6 widen + step 3 |
| `RecoveryCheckpointTrigger.java` | 2 (PR4/6) | PR4 narrow signature; PR6 widened to return a reader |
| `InputProcessorUtil` / `SingleCheckpointBarrierHandler` / `Alternating*` | 2 (PR4/6) | PR4 trigger threading; PR6 writer threading |
| `TestStreamEnvironment.java` | 2 (PR2/4) | pin randomization off; restore |

Test infra: `MockInputGate`/`MockIndexedInputGate`/`SingleInputGateBuilder`/`InputChannelBuilder`/`TestInputChannel` → PR2/PR3; `LocalInputChannelTest`/`RemoteInputChannelTest` → PR2 only.

---

## 3. Detailed PR Design

> Commit titles follow the Flink convention, prefixed with the PR's own JIRA id. Every commit lists its full scope. Tests ride with the commit that motivates them. Attribution marks: **(author: Roman)** = git author is Roman Khachatryan; **(+co-author Roman)** = `Co-authored-by: Roman Khachatryan <khachatryan.roman@gmail.com>` trailer.

---

### PR 1 (FLINK-39520) — `[FLINK-39520] Preparatory refactoring for channel state recovery (no behavior change)`

**PR description (draft)**: Pure refactoring in preparation for reworked channel-state recovery (FLINK-38544 series). No behavior or semantic change; all existing tests pass unmodified except mechanical adaptations (renamed trace label, constructor plumbing in tests). Also carries one standalone `[hotfix]` commit fixing a busy-spin in the multiple-input processor.

**Commit 1** — `[hotfix] Report END_OF_INPUT from multiple-input processor when all inputs finished` **(author: Roman)**
Cherry-pick of `b19f9892cfc`, 1 file (`StreamMultipleInputProcessor.java`, +11/-1): the `NONE_AVAILABLE` branch returns `END_OF_INPUT` when `inputSelectionHandler.areAllInputsFinished()`, else `NOTHING_AVAILABLE` — fixes a busy-spin (the availability future completes once all inputs finish, but `processInput` never reported end-of-input). Imports no feature classes; fully standalone.

**Commit 2** — `[FLINK-39520][network] Decouple LocalInputChannel recovery wiring from toBeConsumedBuffers`
Cherry-pick of `46d4b743b90`. Files: `LocalInputChannel.java` (+42/-21).

**Commit 3** — `[FLINK-39520][checkpoint] Extract AbstractInputChannelRecoveredStateHandler with concrete no-filtering/filtering handlers`
File: `RecoveredChannelStateHandler.java` (single-file class split; all classes stay package-private in this file).
- New abstract `AbstractInputChannelRecoveredStateHandler`: fields `inputGates`, `channelMapping`, `rescaledChannels`, `oldToNewMappings`; methods `getMappedChannels`, `calculateMapping`, `getChannel` — **verbatim** extraction from `InputChannelRecoveredStateHandler`. Adds two hooks used by later phases: `getProducedChannelState()` (returns `null` here) and a `closeInternal()` template.
- New `NoSpillingHandler`: `recover()` = **verbatim** old non-filtering branch (`onRecoveredStateBuffer(descriptor)` + `onRecoveredStateBuffer(buffer.retainBuffer())`, same try/finally recycle); inherits the verbatim network-pool `getBuffer`.
- New `FilteringHandler` (the v1 filtering behavior, **verbatim**): `recover()` = old `recoverWithFiltering` (`filteringHandler.filterAndRewrite(..., channel::requestBufferBlocking)` → deliver `List<Buffer>` via `onRecoveredStateBuffer`); `getBuffer` = old `getPreFilterBuffer` (reusable heap `MemorySegment`, `preFilterBufferInUse` invariant); `closeInternal` = old segment-freeing close. *This class lives until PR7 — it is the memory backend's filter.*
- New static `create(...)` factory selecting `NoSpillingHandler` vs `FilteringHandler` on `checkpointingDuringRecoveryEnabled` — equivalent to the old internal if-branch. `SequentialChannelStateReaderImpl` switches from direct construction to the factory (mechanical).
- `ResultSubpartitionRecoveredStateHandler`: untouched.
- Trace label in `RecoveredInputChannel.onRecoveredStateBuffer`: `"InputChannelRecoveredStateHandler#recover"` → `"NoSpillingHandler#recover"` (final label).
- Tests: mechanical adaptation of `InputChannelRecoveredStateHandlerTest`, `RecoveredChannelStateHandlerTest`.

**Commit 4** — `[FLINK-39520][network] Add InputGate#getChannel(InputChannelInfo)`
- `InputGate`: new abstract `InputChannel getChannel(InputChannelInfo)`.
- `SingleInputGate`: `channels[channelInfo.getInputChannelIdx()]`. `UnionInputGate`: resolve via `inputGatesByGateIndex.get(channelInfo.getGateIdx()).getChannel(channelInfo)` (correct global-vs-member index semantics). `InputGateWithMetrics`: delegate. `CheckpointedInputGate`: delegate helper.
- `AbstractStreamTaskNetworkInput`: `getChannel(channelInfo.getInputChannelIdx())` → `getChannel(channelInfo)`.
- Test impls: `MockInputGate`, `MockIndexedInputGate`.

**Commit 5** — `[FLINK-39520][network] Additive logging overload; widen releaseAllResources visibility`
- `NetworkActionsLogger`: additive `tracePersist(String, Object, ...)` overload; the existing overload delegates.
- `RecoveredInputChannel#releaseAllResources`: package-private → `public`.

---

### PR 2 (FLINK-39521) — `[FLINK-39521][network] Push-based recovery support in input channels (final form)`

**PR description (draft)**: Teach the physical Local/RemoteInputChannel to be created directly in a recovery state and receive recovered buffers via the new `RecoverableInputChannel` interface; recovered-channel conversion now hands buffers over through that interface instead of constructor migration. All new paths are gated on `needsRecovery`/`inRecovery` and nothing in this PR passes `needsRecovery=true`, so default behavior is unchanged. The v1 conversion-migration path is retired here; the flag is therefore temporarily removed from ITCase randomization while the flag-on path is being rewired. Note: `PartitionRequest` gains one wire byte (`needsRecovery`); TM network protocol remains single-version as usual.

After this PR the input-channel layer is in its **final form**; no later PR adds logic to it (PR7 only deletes the transitional conversion-push and the heap fallback in `RecoveredInputChannel`).

**Commit 1** — `[FLINK-39521][test] Temporarily remove during-recovery flag from ITCase randomization`
- `TestStreamEnvironment.randomizeConfiguration`: drop the `randomize(conf, CHECKPOINTING_DURING_RECOVERY_ENABLED, true, false)` line (the flag stays at default `false` unless a test sets it explicitly).
- `@Disabled` `RecoveredStateFilteringLargeRecordITCase` (explicitly flag-on; would fail in the window).
- Rationale: commit 6 retires v1 conversion-migration; ~50% of randomized ITCases would otherwise run flag-on and fail while the flag-on path is being rewired.

**Commit 2** — `[FLINK-39521][network] BufferManager: gate credit notification behind notifyInitiallyEnabled`
- `BufferManager`: new ctor param `notifyInitiallyEnabled`; new `enableNotify()`; `notifyAvailable` suppressed until enabled. All existing callers (`RecoveredInputChannel`, `RemoteInputChannel`) pass `true` — behavior unchanged.

**Commit 3** — `[FLINK-39521][network] Add RecoverableInputChannel contract and recovery sentinels`
- New `RecoverableInputChannel` (`@Internal`): `getChannelInfo()`, `onRecoveredStateBuffer(Buffer)`, `finishRecoveredBufferDelivery()`, `insertRecoveryCheckpointBarrierIfInRecovery(long)`, `requestRecoveryBufferBlocking()`, `onRecoveredStateConsumed()`; `getStateConsumedFuture()` lives abstract on the `InputChannel` base class.
- New `EndOfFetchedChannelStateEvent`: singleton `RuntimeEvent` tail sentinel; reflective `write()/read()` throw; deliberately distinct from `EndOfInputChannelStateEvent`.
- New `RecoveryCheckpointBarrier` (`checkpoint.channel`): per-checkpoint sentinel carried inside the recovery queue; `write()/read()` throw.
- `EventSerializer`: type tags 13 (`RECOVERY_CHECKPOINT_BARRIER_EVENT`) and 14 (`END_OF_FETCHED_CHANNEL_STATE_EVENT`).
- Tests: `RecoveryCheckpointBarrierTest`, `InputChannelTest`, `TestInputChannel` double.

**Commit 4** — `[FLINK-39521][network] LocalInputChannel: push-based recovery state`
Written directly in final form (persist window stays open during recovery — only `startPersisting`, no early `stopPersisting`).
- Ctor: drop `ArrayDeque<Buffer> initialRecoveredBuffers` → add `int networkBuffersPerChannel`, `boolean needsRecovery`; `needsRecovery=false` ⇒ `stateConsumedFuture` pre-completed, `inRecovery=false`, no recovery `BufferManager`.
- `recoveredBuffers` becomes `Deque<Buffer>`; the deque is the monitor guarding `inRecovery` / `recoverySequenceNumber` (starts at `Integer.MIN_VALUE`).
- Implements `RecoverableInputChannel`: push append; `finishRecoveredBufferDelivery` waits for `upstreamReady` then appends the sentinel; `insertRecoveryCheckpointBarrierIfInRecovery`; `requestRecoveryBufferBlocking` lends exclusive buffers via the recovery-only nullable `BufferManager` (`setup()` requests exclusive buffers) — no production caller until PR6's drainer, stated in the PR description; `onRecoveredStateConsumed` flips recovery off.
- `getNextBuffer()` rewritten under one `inRecovery` predicate: recovered buffer → pending priority event (`pullPriorityFromSubpartitionView`) → `Optional.empty()` (live data hidden while in recovery). `wrapRecoveredBufferAsAvailability()`: `FileRegionBuffer`/`CompositeBuffer` materialization, recovery sequence numbers, `peekNextDataType`.
- `upstreamReady` future completed on subpartition-view creation; `@VisibleForTesting completeUpstreamReadyForTest()`.
- `checkpointStarted` split into mutually exclusive in-recovery / normal branches. In-recovery: `startPersisting` only; `collectPreRecoveryBarrier(cpId)` walks the queue to the matching `RecoveryCheckpointBarrier`, retains pre-barrier data buffers for the snapshot, removes the sentinel; missing barrier ⇒ `IOException` wrapped as `CheckpointException(CHECKPOINT_DECLINED)`.
- Same-commit mechanical ctor-caller adaptation (compile requirement): `UnknownInputChannel`, `LocalRecoveredInputChannel`, `InputChannelBuilder`, `SingleInputGateBenchmarkFactory`.
- Tests: `LocalInputChannelTest` (push / sentinel / priority / barrier-collect / missing-barrier decline / persist window), `LocalRecoveredInputChannelTest`.

**Commit 5** — `[FLINK-39521][network] RemoteInputChannel: push-based recovery state`
Written directly in final form; the not-in-recovery `checkpointStarted` branch stays byte-identical to master.
- Ctor: drop `initialRecoveredBuffers` → add `needsRecovery` (same pre-completed-future semantics as Local).
- `appendRecoveredBuffer()` appends straight into `receivedBuffers` (`NONE` subpartition id) — the consume path stays identical to the normal case. `recoveryEventStash` (`@GuardedBy receivedBuffers`): ordinary upstream events arriving under suppressed credit are stashed (data buffers asserted absent in `onBuffer` during recovery) and replayed after the sentinel is consumed.
- `upstreamReady` as `CountDownLatch(1)` (counted down by the first `onBuffer` or release); `onRecoveredStateConsumed()`: unstash + `bufferManager.enableNotify()` (credit reopens) + complete `stateConsumedFuture`; `checkReadability()` allows in-recovery reads before the partition-request client is initialized; new public getter `needsRecovery()` (consumed by netty in commit 7); persist window: `startPersisting` only.
- Same-commit ctor-caller adaptation: `UnknownInputChannel`, `RemoteRecoveredInputChannel`, `InputChannelBuilder`, benchmark factory, netty test channel subclasses.
- Tests: `RemoteInputChannelTest` (append / stash / credit / consumed / persist window), `RemoteRecoveredInputChannelTest`.

**Commit 6** — `[FLINK-39521][network] Convert recovered channels via the push interface; thread needsRecovery through gates`
- `RecoveredInputChannel.toInputChannel(boolean needsRecovery)`:
  - `needsRecovery=true` (first used by PR3): create the physical channel in recovery state, then synchronously push every queued recovered data buffer via `onRecoveredStateBuffer` (the legacy `EndOfInputChannelStateEvent` in the queue is dropped in translation; the end-of-recovery sentinel is appended later by the StreamTask chain once upstream is ready). *This is the "simple in-memory drain"; transitional code, replaced by the disk drain in PR7.*
  - `needsRecovery=false`: `checkState(receivedBuffers.isEmpty())` + `inputChannel.setup()` + `checkpointStopped(...)` — constructor migration is gone.
  - `abstract toInputChannelInternal(ArrayDeque<Buffer>)` → `toInputChannelInternal(boolean)`.
- `InputGate`: concrete default `requestPartitions(boolean needsRecovery)` (default overload ignores the flag). `SingleInputGate`: `requestPartitions(boolean)`, `convertRecoveredInputChannels(boolean)`, `getStateConsumedFuture()` aggregates over all channels uniformly. `UnionInputGate`: fan the flag to member gates. `InputGateWithMetrics`: delegate.
- Deliberate consequence (stated in the PR description): base v1 flag-on StreamTask still converts at filtering-complete with unconsumed buffers → the empty-queue `checkState` fires → **flag-on degraded until PR3**. Covered by commit 1's randomization pin. Default config unaffected (flag-off conversion always happens after full consumption).
- Tests: `RecoveredInputChannelTest` (push-conversion, empty-assert), `SingleInputGateTest`/`UnionInputGateTest` adaptations, `SingleInputGateBuilder`/`TestInputChannel`.

**Commit 7** — `[FLINK-39521][network] Propagate needsRecovery in PartitionRequest; start view reader with zero credit`
- `NettyMessage.PartitionRequest`: `final boolean needsRecovery` field, ctor arg, `writeBoolean`/`readBoolean`, `+Byte.BYTES` in the length calc — **wire-format change** (single-version cluster assumption stated in the PR description).
- `CreditBasedSequenceNumberingViewReader`: ctor gains `needsRecovery`; `numCreditsAvailable = needsRecovery ? 0 : initialCredit` (the producer withholds credit while the consumer's exclusive buffers are lent to recovery).
- `PartitionRequestServerHandler`: pass `request.needsRecovery`. `NettyPartitionRequestClient`: pass `inputChannel.needsRecovery()`.
- Tests (mechanical): `NettyMessageServerSideSerializationTest`, `PartitionRequestServerHandlerTest`, `PartitionRequestQueueTest`, `PartitionRequestRegistrationTest`, `CreditBasedSequenceNumberingViewReaderTest`, `CancelPartitionRequestTest`, `ServerTransportErrorHandlingTest`, `CreditBasedPartitionRequestClientHandlerTest`.

**Commit 8** — `[FLINK-39521][network] CheckpointedInputGate: consume EndOfFetchedChannelStateEvent`
- On polling the sentinel: assert the channel is a `RecoverableInputChannel`, call `onRecoveredStateConsumed()`; the event is never delivered to the operator.

---

### PR 3 (FLINK-39522) — `[FLINK-39522] Rework StreamTask recovery: async future chain, consume only after filtering completes`

**PR description (draft)**: Restructure StreamTask channel-state recovery into an asynchronous future chain on the channelIOExecutor and unify recovery completion on the gates' state-consumed futures. With checkpointing-during-recovery enabled, the task now consumes recovered state only **after filtering has fully completed** (buffers are held in the recovered channels' memory queues — including the pre-existing heap fallback — and handed to the physical channels through the push interface at conversion); on master, consumption overlapped with filtering. The filtering-progress gate API becomes unused and is removed. Non-CDR observable behavior is preserved (per-gate conversion off each gate's own state-consumed future matches master); the only non-CDR-visible change is the defer-finish hardening, which fixes an existing race where a finite task could finish before recovery completed. Checkpoints during recovery are declined at this stage.

**Commit 1** — `[FLINK-39522] Restructure StreamTask channel-state recovery into an async future chain` **(+co-author Roman)**
File: `StreamTask.java` (`restoreStateAndGates` and helpers; method names are final).
- Read the flag once via `CheckpointingOptions.isCheckpointingDuringRecoveryEnabled(jobConfig)`; split into `recoverChannelsWithCheckpointing` / `recoverChannelsWithoutCheckpointing`; `recoveryCompletionFuture.whenComplete(→ mailboxProcessor.suspend())`.
- **Without** (flag-off): submit `reader.readInputData(...)` to `channelIOExecutor`; each gate triggers `mainMailboxExecutor.execute(() -> inputGate.requestPartitions(false))` off **its own** `getStateConsumedFuture()` (per-gate, avoiding the selective-reading multi-input deadlock); return `completeAll(all stateConsumedFutures)`. Observable behavior = master.
- **With** (flag-on, memory backend): future chain — `runAsync(readInputChannelState, channelIOExecutor)` (filtering fully completes; output accumulates in the `RecoveredInputChannel` queues + heap fallback) → `thenRunAsync(requestPartitions(true) for all gates, mainMailboxExecutor)` (conversion pushes everything) → sentinel delivery on the channelIOExecutor once upstream is ready → `completeAll(gates' getStateConsumedFuture())`. Empty-input-gates short-circuit completes synchronously (prevents a finite source suspending the restore loop before recovery completes).
- The gate-finishing loop (`inputGate.finishReadRecoveredState()`) moves from the handler's `close()` into `StreamTask#readInputChannelState`.
- `restoreInternal`: `allGatesRecoveredFuture.get()` rethrowing the underlying cause (recovery failures surface instead of being swallowed); shut down `channelIOExecutor` after the restore loop.
- Tests: `TaskCheckpointingBehaviourTest`, StreamTask recovery tests.

**Commit 2** — `[FLINK-39522][network] Remove recovery flags and the filtering-complete future from the gate API`
Pure deletion of the now-unused v1 gating:
- `InputGate.getBufferFilteringCompleteFuture()` + overrides in `SingleInputGate`, `UnionInputGate`, `InputGateWithMetrics`, mocks.
- `IndexedInputGate.setCheckpointingDuringRecoveryEnabled/isCheckpointingDuringRecoveryEnabled` + `SingleInputGate`'s volatile field.
- `RecoveredInputChannel.bufferFilteringCompleteFuture` field, getter, and the `finishReadRecoveredState` lock-ordering commentary tied to it; where the flag is still needed (heap-fallback gating) it travels as a handler-supplied parameter.
- Tests: remove the corresponding `SingleInputGateTest`/`UnionInputGateTest` cases; adapt `MockInputGate`/`MockIndexedInputGate`/`AlignedCheckpointsMassiveRandomTest`.

**Commit 3** — `[FLINK-39522] Defer task finish until recovery completes` **(author: Roman)**
Cherry-pick of `87da2ec0b6a`. `StreamTask.processInput`: on `END_OF_INPUT`, if `recoveryCompletionFuture` is not done, suspend the default action and resume on completion instead of finishing the task.

---

### PR 4 (FLINK-39523) — `[FLINK-39523] Checkpoint coordination during recovery (memory-based trigger)`

**PR description (draft)**: Make checkpoints work while recovered state is still being consumed. A new `RecoveryCheckpointTrigger` is dispatched by the barrier handlers at checkpoint start: it inserts a `RecoveryCheckpointBarrier` into every channel still in recovery, so `checkpointStarted`'s in-recovery branch can persist exactly the pre-barrier recovered data. The in-memory implementation only inserts barriers — the one-shot in-memory push leaves no undrained residue, so there is no snapshot to transfer. StreamTask manages the trigger lifecycle: NOT_READY while filtering (checkpoints declined as task-not-ready), the barrier-inserting trigger during consumption, NO_OP once all gates report state consumed. Finally, the during-recovery flag returns to ITCase randomization — the randomized ITCase fleet now exercises the feature in ~50% of runs.

**Commit 1** — `[FLINK-39523][checkpoint] RecoveryCheckpointTrigger with NOT_READY/NO_OP and a barrier-inserting in-memory implementation` **(+co-author Roman)**
- New `RecoveryCheckpointTrigger` (`checkpoint.channel`): `void snapshotAndInsertBarriers(long checkpointId)` — *narrow signature; widened to return a snapshot reader in PR6*; `NO_OP` singleton (no-op); `NOT_READY` singleton (throws `CheckpointException(CHECKPOINT_DECLINED_TASK_NOT_READY)` — transient, the coordinator retries).
- New transitional in-memory implementation (`InMemoryRecoveryCheckpointTrigger`): holds the task's `List<RecoverableInputChannel>`; `snapshotAndInsertBarriers` = `forEach(ch -> ch.insertRecoveryCheckpointBarrierIfInRecovery(cpId))`. Javadoc states why no snapshot exists (one-shot push ⇒ no undrained residue). *Deleted in PR7.*
- Unit tests for all three implementations.

**Commit 2** — `[FLINK-39523][checkpoint] ChannelState: dispatch checkpoint start through the recovery trigger`
- `ChannelState` (streaming `io.checkpointing`) gains a `RecoveryCheckpointTrigger` (the legacy 1-arg ctor defaults to `NO_OP`); new `onCheckpointStartedForAllInputs(CheckpointBarrier)`: **(1)** `trigger.snapshotAndInsertBarriers(cpId)`; **(2)** `for (input : inputs) input.checkpointStarted(barrier)`. `CheckpointException` rethrown as-is (routes to checkpoint abort, not task failure); other `IOException` via `rethrowIOException`. *(Step 3 — spilled-slice replay — added in PR6.)*
- `AlternatingCollectingBarriers` + `AlternatingWaitingForFirstBarrierUnaligned`: replace the inline per-input `checkpointStarted` loop with `state.onCheckpointStartedForAllInputs(...)` (behaviorally inert with `NO_OP`).
- Tests: `ChannelStateDispatcherTest` (2-step scope), `AlternatingCollectingBarriersDispatchHookTest`, `AlternatingWaitingForFirstBarrierUnalignedDispatchHookTest`.

**Commit 3** — `[FLINK-39523] Thread the trigger through barrier-handler construction and the StreamTask lifecycle`
- `InputProcessorUtil.createCheckpointBarrierHandler`: overload taking a `RecoveryCheckpointTrigger` (old signature preserved as a `NO_OP` delegator); `SingleCheckpointBarrierHandler.unaligned/alternating` gain the trigger param (`aligned` defaults via `ChannelState`'s 1-arg ctor); `OneInputStreamTask`/`TwoInputStreamTask`/`MultipleInputStreamTask` pass `StreamTask.getRecoveryCheckpointTrigger()`.
- `StreamTask`: `recoveryCheckpointTrigger` field + mailbox-thread-asserting getter lambda; all mutations via `setRecoveryCheckpointTrigger` (a mailbox mail). Lifecycle inside `recoverChannelsWithCheckpointing`: `NOT_READY` before/during filtering → install the in-memory trigger at conversion → swap to `NO_OP` when `completeAll(gates' stateConsumedFutures)` completes (gate on the futures, not on push completion; the mailbox-mail gap is safe because a barrier-inserting trigger with no in-recovery channels behaves as `NO_OP`).
- Tests: `TestBarrierHandlerFactory` adaptation; StreamTask lifecycle tests; an end-to-end memory-CDR test (checkpoint during consumption; multi-input with mixed recovery states).

**Commit 4** — `[FLINK-39523][test] Restore during-recovery flag randomization; re-enable filtering ITCase`
- `TestStreamEnvironment`: restore `randomize(conf, CHECKPOINTING_DURING_RECOVERY_ENABLED, true, false)`.
- Re-enable `RecoveredStateFilteringLargeRecordITCase`.
- The randomized fleet now runs flag-on ~50% against the memory backend (same memory profile as base v1, which ran the same randomization with the same heap fallback).

**Phase A complete**: input channels, thread/future model, ordering semantics, and CDR coordination are all final-form; only the storage backend (memory) is temporary.

---

### PR 5 (FLINK-39524) — `[FLINK-39524][checkpoint] Fetched channel state: spill files, filtered write, reader (not applied)`

**PR description (draft)**: Introduce the on-disk subsystem for recovered channel state: an append-only segmented spill-file format written by the recovery read path (optionally through the rescale record filter), a ref-counted file container with snapshot/resume semantics, and a forward-only reader. Runtime behavior is unchanged: the handler factory does not select the new handlers, so no production path produces or consumes spill files.

**Commit 1** — `[FLINK-39524][checkpoint] Add FetchedChannelState spill-file container`
- `FetchedChannelState`: sealed container over an ordered `List<Path>` of spill files; `acquire()`/`release()` ref-counted lifecycle deleting the files at zero (`cleanedUp` guard); `close()` forces cleanup. (The `reader()` entry point and `FetchedChannelStateSnapshot` land with the reader in commit 4 — they depend on it.)
- Tests: `FetchedChannelStateTest`.

**Commit 2** — `[FLINK-39524][checkpoint] Spill-writing handlers: segmented on-disk format`
- New `AbstractSpillingHandler` (in `RecoveredChannelStateHandler.java`, extending the PR1 base): a reusable `DataOutputSerializer` accumulates one channel's segment `[gateIdx][channelIdx][bufferLength][body…]`; the body length is backfilled via `writeIntUnsafe` at seal; flush through `OffsetAwareOutputStream`; 64 MiB soft rotation (`DEFAULT_SPILL_FILE_SIZE_BYTES`); `closeInternal()` seals, closes the stream, builds the `FetchedChannelState` (calling `acquire()`); exposes `SEGMENT_HEADER_BYTES` / `BUFFER_LENGTH_HEADER_OFFSET`.
- New `SpillingNoFilteringHandler`: `recover()` writes the recovered buffer's bytes verbatim via `segmentSerializerFor(...).write(memorySegment, offset, len)`; inherits the network-pool `getBuffer`. Adds the `getProducedChannelState()` hook body on the abstract base.
- `flink-core OffsetAwareOutputStream`: ctor package-private → `public` (1 line; lets flink-runtime construct it).
- Factory **not** switched. Tests: `AbstractSpillingHandlerTest`, `TestSpillWriter`, `FetchedChannelStateRefCountTest`.

**Commit 3** — `[FLINK-39524][checkpoint] Rewrite ChannelStateFilteringHandler to emit into a spill segment`
- `filterAndRewrite`: `List<Buffer>` + `BufferSupplier` → `void` + `DataOutputSerializer` sink; new `serializeElement`: 4-byte length placeholder + `writeIntUnsafe` backfill; no network buffers, no `InterruptedException`. Inline `createDeserializer` (always `filterContext.getTmpDirectories()`).
- New `SpillingWithFilteringHandler`: `getBuffer` = the heap pre-filter-segment logic **moved verbatim from the v1 `FilteringHandler`**; `recover()` routes filter output into the spill segment.
- The v1 `FilteringHandler` remains present and factory-selected — the old buffer-delivering filter loop is kept alive for it as a marked transitional overload (dies with the class in PR7). `create(...)` extended with the spill-tmp-directories parameter.
- Tests: `ChannelStateFilteringHandlerTest`, `GateFilterHandlerTest`/`GateFilterHandlerBufferOwnershipTest` rewrites, `RecoveredChannelStateHandlerFilterRoutingTest`.

**Commit 4** — `[FLINK-39524][checkpoint] Forward-only spill reader with snapshot/resume`
- `FetchedChannelStateReader` interface: `nextSegment()`, `snapshot()`, `emptyReader()`; inner `SpillSegment` (`channelInfo()`, `bodyStream()`, `length()`, `commit()`).
- `FetchedChannelStateReaderImpl`: sequential file IO; a bounded per-segment body `InputStream`; `current`/`committed` positions; snapshot resume with skip-only-on-first-positioning. Plus the deferred `FetchedChannelState.reader()` + `FetchedChannelStateSnapshot`.
- Tests: `FetchedChannelStateReaderTest`.

**Commit 5** — `[FLINK-39524][checkpoint] SequentialChannelStateReader#readInputData returns Optional<FetchedChannelState>`
- Interface + `NO_OP` (`Optional.empty()`); `SequentialChannelStateReaderImpl`: pass `checkpointingDuringRecoveryEnabled` / `memorySegmentSize` / `tmpDirectories` through `create(...)`; close-ordering rework (nested try-with-resources so the spill file is published only after `stateHandler.close()` flushes the writer); return `Optional.ofNullable(handler.getProducedChannelState())`. The dead `producedChannelState` field from the original branch is **not** re-introduced.
- `StreamTask` call sites assert the empty return (`checkState(reader.readInputData(...).isEmpty())`).
- Tests: `SequentialChannelStateReaderImplTest`.

---

### PR 6 (FLINK-40080) — `[FLINK-40080][checkpoint] Drain spilled state into channels; replay spilled slices into checkpoints (inert)`

**PR description (draft)**: The disk-side counterpart of the recovery-checkpoint coordination: the trigger interface is widened so checkpoint start atomically snapshots the not-yet-drained spill slice, the channel-state writer learns to replay such a slice into a checkpoint, the barrier-handler dispatch gains step 3, and the incremental drainer that pushes spill segments into `RecoverableInputChannel`s lands. Runtime behavior is unchanged — no production path constructs a drainer, and the handler factory does not select the spilling handlers.

**Commit 1** — `[FLINK-40080][checkpoint] Widen RecoveryCheckpointTrigger#snapshotAndInsertBarriers to return a state reader`
- Return type `void` → `FetchedChannelStateReader`. `NO_OP`, `NOT_READY`, and the in-memory trigger return `FetchedChannelStateReader.emptyReader()`. The `ChannelState` step-1 call site holds and (for now) closes the returned reader. `StreamTask.getRecoveryCheckpointTrigger()` gains the `return`.

**Commit 2** — `[FLINK-40080][checkpoint] ChannelStateWriter#addInputDataFromSpill: replay spilled segments into the checkpoint`
- `ChannelStateWriter`: `addInputDataFromSpill(long checkpointId, FetchedChannelStateReader reader)` (the NO_OP default closes the reader).
- `ChannelStateWriterImpl`: enqueue; `ChannelStateWriteRequest.replayInputDataFromSpill` (a `CheckpointInProgressRequest`; cancel closes the reader); `ChannelStateCheckpointWriter.writeInputFromSpill(jobVertexID, subtaskIndex, reader)`: loop `nextSegment()`, write each `bodyStream()` via the new serializer overload, record offset/size into `pendingResult.getInputChannelOffsets()`, close the reader in `finally`; `ChannelStateSerializer.writeData(DataOutputStream, InputStream input, int length)` (length prefix + `transferTo`, fail-loud on short read).
- Async demux by `channelInfo`; empty-snapshot inline early return; failures propagate via `ChannelStateWriteResult`.
- Tests: `ChannelStateWriterImplAddInputDataFromSpillTest`, `MockChannelStateWriter` additions.

**Commit 3** — `[FLINK-40080][checkpoint] ChannelState step 3: hand the snapshot reader to the writer`
- `ChannelState` gains a `ChannelStateWriter` (default `NO_OP`); `InputProcessorUtil` pulls it from `checkpointCoordinator.getChannelStateWriter()`; `SingleCheckpointBarrierHandler.unaligned/alternating` gain the writer param.
- `onCheckpointStartedForAllInputs`: step **(3)** `channelStateWriter.addInputDataFromSpill(cpId, snap)` — ownership of the reader transfers to the writer; the failure path closes `snap` (suppressing) before rethrowing.
- Tests: `ChannelStateDispatcherTest` extended to the full 3 steps, `TestBarrierHandlerFactory`.

**Commit 4** — `[FLINK-40080][checkpoint] FetchedChannelStateDrainer: incremental drain with atomic snapshot-and-insert`
- Implements `RecoveryCheckpointTrigger` + `Closeable`; ctor takes `FetchedChannelState` + `List<RecoverableInputChannel>`, derives the `InputChannelInfo` map internally.
- `drain()`: root reader front-to-back; per segment: `requestRecoveryBufferBlocking()` + disk read **outside** the lock; deliver (`onRecoveredStateBuffer`) + `commit()`/offset advance **inside** the lock; `finishRecoveredBufferDelivery()` per channel at the end.
- `snapshotAndInsertBarriers(cpId)`: atomic under the drainer lock — snapshot the committed position (`FetchedChannelStateSnapshot`, ref-count `acquire`) + `insertRecoveryCheckpointBarrierIfInRecovery(cpId)` per channel; returns the snapshot's reader (caller-owned).
- Tests: `FetchedChannelStateDrainerTest`, `FetchedChannelStateDrainerConcurrencyTest`.

---

### PR 7 (FLINK-40081) — `[FLINK-40081] Spill recovered channel state to disk and drain it (switch the flag-on backend)`

**PR description (draft)**: Swap the memory backend for the disk backend behind `execution.checkpointing.during-recovery.enabled`: recovered channel state is fetched (and rescale-filtered) into spill files, the drainer pushes it into the recovery-state channels while serving as the recovery-checkpoint trigger, and checkpoints taken during recovery snapshot the not-yet-drained slice. The transitional memory pieces (in-memory trigger, conversion-time push, v1 filtering handler, unbounded heap fallback) are deleted. The randomized ITCase fleet (which runs the flag in ~50% of runs) must stay green across the backend swap — the acceptance bar for spilling being a drop-in replacement of the memory backend.

**Commit 1** — `[FLINK-40081] Switch flag-on recovery to fetch->drain; retire the in-memory backend`
One semantic unit (the backend swap), atomically:
- `AbstractInputChannelRecoveredStateHandler.create(...)`: flag-on now returns `SpillingWithFilteringHandler` / `SpillingNoFilteringHandler` (flag-on + null filteringHandler → `SpillingNoFilteringHandler`); **delete the v1 `FilteringHandler`** and its transitional filter-delivery overloads in `ChannelStateFilteringHandler`.
- `StreamTask.recoverChannelsWithCheckpointing`: `NOT_READY` → `fetchChannelState` on `channelIOExecutor` (uses the `Optional<FetchedChannelState>` return) → `requestPartitions(state.isPresent())` on the mailbox → `collectPhysicalChannels` → `buildDrainer` → install the drainer as the live trigger → `drain()` on `channelIOExecutor` → `completeAll(gates' stateConsumedFutures)` → `NO_OP`. **Delete the in-memory trigger class + the transitional sentinel-delivery helper.**
- `RecoveredInputChannel.toInputChannel(true)`: **delete the conversion-time push**; unify on `checkState(receivedBuffers.isEmpty())`.
- Tests: StreamTask flag-on chain tests, `ChannelIOExecutorDrainSubmissionTest`, test-helper restorations to the `create(...)` form.

**Commit 2** — `[FLINK-40081][network] Remove the unbounded heap fallback from RecoveredInputChannel#requestBufferBlocking`
- Back to plain `bufferManager.requestBufferBlocking()`; the OOM path is gone — spilling supersedes it.
- Test: `RecoveredInputChannelRequestBufferBlockingHeapFallbackRemovedTest`.

**Commit 3** — `[FLINK-40081] Harden RecordFilterContext for minimal and batch environments`
- `disabled()` → `disabled(String[] tmpDirectories)`; non-empty `tmpDirectories` enforced only when the flag is on; tolerate null/empty otherwise.
- Tests: `RecordFilterContextTest`, `SequentialChannelStateReaderImplTest` + `ChannelPersistenceITCase` adaptations, StreamTask `createRecordFilterContext` hunks.

**Commit 4** — `[FLINK-40081][test] Add UnalignedCheckpointDuringRecoveryITCase`
- End-to-end unaligned checkpoint taken while recovery is in progress.

**Commit 5** — `[FLINK-40081][test] Add RescaleFilterLargeRecordOOMRegressionITCase`
- Regression test for the OOM scenario the disk backend eliminates (rescale + filter + large records).

**Commit 6** — `[FLINK-40081][test] Add CdrRecoveryRaceITCase for the non-source recovery race` **(author: Roman)**
- BATCH+CDR smoke test for the non-source recovery race (incomplete as a deterministic reproducer, per its javadoc TODO).

**Commit 7** — `[FLINK-40081][docs] Add the spilling v2 requirements notes and PR split plan`
- `requirements/38544/**` documentation.

---

## 4. Decision Log

| # | Decision | Source |
|---|---|---|
| D1 | `46d4b743b90` included as a pure-refactor commit of PR1 | confirmed |
| D2 | Channels before StreamTask (PR order: channels → task) | Slack review w/ Roman, 2026-07-04 |
| D3 | Phase A ends with *everything final-form except the storage backend*; the memory backend fully working incl. CDR ("Reading B") | 2026-07-05 |
| D4 | Recovered buffers handed over via the push interface at conversion, not via constructors | 2026-07-05 |
| D5 | `bufferFilteringCompleteFuture` + gate recovery flags deleted in Phase A (PR3) — the filtering-complete signal becomes the future-chain step completion | 2026-07-05 |
| D6 | CDR requires the trigger even memory-based (barrier insertion for in-recovery channels; multi-input scenario); the memory trigger inserts barriers only, snapshot inherently empty | 2026-07-05 |
| D7 | Writer plumbing (`addInputDataFromSpill`/`writeInputFromSpill`) deferred to Phase B — Phase A provably has no spill files; the trigger starts with the narrow `void` signature, widened in PR6 | 2026-07-05 |
| D8 | Coordination as its own PR (PR4) | 2026-07-05 |
| D9 | `UnalignedCheckpointDuringRecoveryITCase` stays in Phase B (PR7) | 2026-07-05 |
| D10 | PR2→PR3 flag-on degradation accepted; handled by pinning the flag out of ITCase randomization in PR2 commit 1 and restoring it in PR4 commit 4 (plus temporarily disabling `RecoveredStateFilteringLargeRecordITCase`) | 2026-07-05 |
| D11 | v1 `FilteringHandler` + heap fallback survive until PR7 (they *are* the memory backend) | standing |
| D12 | Fleet-green from PR4 onward is the acceptance bar for "spilling is a lightweight replacement" | 2026-07-05 |
| D13 | The `[hotfix]` commit folds into PR1 as a standalone commit (7 PRs total, one JIRA each) | 2026-07-06 |
| D14 | JIRA mapping: PR1–PR5 = FLINK-39520..39524, PR6 = FLINK-40080, PR7 = FLINK-40081; commit titles prefixed with the owning PR's JIRA | 2026-07-06 |
| D15 | PR7's IT cases split into three commits (one per ITCase) for precise per-commit authorship | 2026-07-06 |
| D16 | No AI co-author trailers in final commit messages; Roman Khachatryan attribution per the confirmed table (author of 3 commits, co-author on 2) | 2026-07-06 |

---

## 5. Construction Approach

The original commits cannot be reused one-by-one (they don't individually compile). Build the series as **final-state slices plus a small, explicit transitional inventory**:

1. Construct each commit from the final-state code per the scopes above; per commit compile best-effort, per PR the full build + targeted tests are mandatory. Stack each subsequent PR branch on the previous one; rebase after merges.
2. **Transitional-code inventory** (code that exists mid-series but not at the series tip — keep this list authoritative):
   - conversion-time sync push in `RecoveredInputChannel.toInputChannel(true)` (PR2 → deleted PR7)
   - the in-memory `RecoveryCheckpointTrigger` implementation + sentinel-delivery helper (PR4 → deleted PR7)
   - the narrow `void` trigger signature (PR4 → widened PR6)
   - the v1 filter-delivery overloads in `ChannelStateFilteringHandler` (PR5 → deleted PR7)
   - randomization pin + `@Disabled` ITCase (PR2 → reverted PR4)
   - (the v1 `FilteringHandler` and the heap fallback are *base* code retained until PR7, not new transitional code)
3. **Final-state verification**: after PR7, the series tip must match the reference branch tree exactly, modulo the explicit discard list (the dead `producedChannelState` field: intentionally not re-added). The transitional inventory must have burned down to zero.
4. PRs that open/close the randomization window or contain wire-format changes say so explicitly in their descriptions. Cross-PR timing lives in JIRA links (blocked-by/blocks), not in commit messages or JIRA descriptions.
