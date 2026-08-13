# Checkpointing During Recovery — Facts & Timeline

**Scope:** everything related to *checkpointing during recovery* (CDR) since commit
`13be665f` (Dec 2025 baseline), i.e. Apache Flink umbrellas **FLINK-35761 (FLIP-547)** and
**FLINK-38544 (FLIP-547 V2)**, tracked internally under **NGN-987 / INIT-12544**.

**Data as of:** 2026-08-13.
**Sources:** `git log` (apache/flink master + local branches), GitHub API (apache/flink PRs,
1996fanrui/flink fork PRs), Apache JIRA REST API, Confluent JIRA (NGN-987, INIT-12544).

This document contains **facts only** — no interpretation. Analysis lives in
[`cdr-retro-summary.md`](./cdr-retro-summary.md).

---

## 1. Per-JIRA table

One row per JIRA. "Commits" = commits authored under that JIRA (including the `[hotfix]`
commits that shipped inside the same PR).

### 1.1 Umbrellas

| JIRA | Title | Created | Resolved | Status |
|---|---|---|---|---|
| FLINK-35761 | FLIP-547: Support checkpoint during recovery | 2024-07-05 | 2026-06-02 | Resolved |
| FLINK-38544 | FLIP-547 V2: Filtering record before processing **with spilling strategy** | 2025-10-21 | — | **Open** |

### 1.2 Phase 1 — Recover upstream output buffers on the downstream side

| JIRA | Commits | What it does | PR | PR opened | Merged | Open→merge | JIRA resolved |
|---|---|---|---|---|---|---|---|
| FLINK-38542 | 4 | Upstream `ResultSubpartition` output buffers are recovered directly on the downstream task instead of being replayed through the upstream. Adds `ResultSubpartitionDistributor`, enforces one-buffer-to-one-channel distribution, plus test randomization of the flag. | [#27182](https://github.com/apache/flink/pull/27182) | 2025-11-02 | 2026-01-13 | **72 d** | 2026-01-13 |

### 1.3 Phase 2 — CDR **without** spilling (V1, in-memory / heap fallback)

| JIRA | Commits | What it does | PR | PR opened | Merged | Open→merge | JIRA resolved |
|---|---|---|---|---|---|---|---|
| FLINK-39140 | 5 | Test enabler: allow multiple rescales in the Unaligned Checkpoint ITCases so checkpointing actually happens during recovery; disable `CUSTOM_PARTITIONER`; fix `MAX_RETAINED_CHECKPOINTS`; switch record type to `String`. | [#27688](https://github.com/apache/flink/pull/27688) | 2026-02-26 | 2026-03-04 | 6 d | 2026-03-04 |
| FLINK-38541 | 6 (2 PRs) | Introduces `execution.checkpointing.unaligned.during-recovery.enabled`; extracts `RecordFilter` / `VirtualChannel`; documents the option; randomizes it in tests; renames it to `CHECKPOINTING_DURING_RECOVERY_ENABLED`. | [#27782](https://github.com/apache/flink/pull/27782)<br>[#27639](https://github.com/apache/flink/pull/27639) (docs) | 2026-03-18<br>2026-02-20 | 2026-03-26<br>2026-04-13 | 8 d<br>**52 d** | 2026-03-26 |
| FLINK-38930 | 3 | The core V1 mechanism: filter recovered records **before** processing so checkpoints can fire mid-recovery; plus the heap-buffer fallback that avoids deadlock when the buffer pool is exhausted. | [#27783](https://github.com/apache/flink/pull/27783) | 2026-03-18 | 2026-03-31 | 13 d | 2026-03-31 |
| FLINK-39018 | 5 | Makes `LocalInputChannel` checkpointable during recovery: snapshot recovered buffers, fix priority-event & availability handling, notify priority events downstream even when blocked, migrate buffers from `RecoveredInputChannel` to the physical channel. | [#27861](https://github.com/apache/flink/pull/27861) | 2026-03-31 | 2026-04-07 | 7 d | 2026-04-07 |
| FLINK-38543 | 3 | Reworks the overall UC restore process (JM + task init): fixes mailbox loop being interrupted before recovery finished, introduces `bufferFilteringCompleteFuture` for an earlier RUNNING transition. | [#27862](https://github.com/apache/flink/pull/27862) | 2026-03-31 | 2026-04-07 | 7 d | 2026-04-15 |

### 1.4 Test-infrastructure bugs that blocked CDR testing

| JIRA | Commits | What it does | PR | PR opened | Merged | Open→merge |
|---|---|---|---|---|---|---|
| FLINK-39408 | 1 | `TestNameProvider` broken after the JUnit 5 migration. | [#27902](https://github.com/apache/flink/pull/27902) | 2026-04-07 | 2026-04-08 | 1 d |
| FLINK-39423 | 2 | `PseudoRandomValueSelector.randomize()` produced identical values for **all** boolean config options; `UnalignedCheckpointTestBase` was missing config randomization — i.e. the CDR flag was never actually randomized. | [#27917](https://github.com/apache/flink/pull/27917) | 2026-04-11 | 2026-04-13 | 2 d |

### 1.5 Phase 3 — CDR **with** spilling (FLINK-38544 sub-tasks)

| JIRA | Commits | What it does | PR | PR opened | Merged | Open→merge | JIRA resolved |
|---|---|---|---|---|---|---|---|
| FLINK-39519 | 3 | Allocate pre-filter source buffers from a reusable heap segment (unblocks large records through filtering recovery) + ITCase. | [#28001](https://github.com/apache/flink/pull/28001) | 2026-04-22 | 2026-04-28 | 6 d | 2026-04-28 |
| FLINK-39520 | 5 | Preparatory refactoring, no behaviour change: split `AbstractInputChannelRecoveredStateHandler` into no-filtering/filtering handlers, add `InputGate#getChannel(InputChannelInfo)`, decouple `LocalInputChannel` recovery from `toBeConsumedBuffers`. | [#28651](https://github.com/apache/flink/pull/28651) | 2026-07-06 | 2026-07-09 | 3 d | 2026-07-09 |
| FLINK-39521 | 8 | Push-based recovery in input channels: `RecoverableInputChannel` contract + sentinels, push state for Local/Remote channels, `needsRecovery` threaded through gates and `PartitionRequest`, zero-credit view reader, `EndOfFetchedChannelStateEvent`. | [#28652](https://github.com/apache/flink/pull/28652) | 2026-07-06 | 2026-07-10 | 4 d | 2026-07-10 |
| FLINK-39522 | 3 | Restructures `StreamTask` channel-state recovery into an async future chain; consumption starts only after filtering completes; removes the recovery flags / filtering-complete future from the gate API. | [#28659](https://github.com/apache/flink/pull/28659) | 2026-07-06 | 2026-07-17 | 11 d | 2026-07-17 |
| FLINK-39523 | 4 | `RecoveryCheckpointTrigger` (NOT_READY / NO_OP + barrier-inserting in-memory impl); channel-state dispatches checkpoint start through it; trigger threaded through barrier-handler construction and the `StreamTask` lifecycle; restores flag randomization. | [#28660](https://github.com/apache/flink/pull/28660) | 2026-07-06 | 2026-07-29 | 23 d | 2026-07-29 |
| FLINK-39524 | 4 | The spill subsystem: `FetchedChannelState` spill-file container, segmented on-disk format for the spill-writing handlers, `ChannelStateFilteringHandler` rewritten to emit into a spill segment, forward-only spill reader with snapshot/resume. | [#28661](https://github.com/apache/flink/pull/28661) | 2026-07-06 | 2026-08-10 | **35 d** | 2026-08-13 |
| FLINK-40080 | 4 | `ChannelStateWriter#addInputDataFromSpill` replays spilled segments into the checkpoint; `FetchedChannelStateDrainer` does incremental drain with atomic snapshot-and-insert; flag-on recovery switches to fetch→drain and retires the in-memory backend; removes the unbounded heap fallback. **This is the PR that delivers the bounded-memory guarantee.** | [#28662](https://github.com/apache/flink/pull/28662) | 2026-07-06 | 2026-08-13 | **38 d** | 2026-08-13 |
| FLINK-40081 | 2 | Hardens `RecordFilterContext` for minimal/batch environments; adds `CdrRecoveryRaceITCase` for the non-source recovery race. Now rebased onto master: +164/−17 across 3 files, 1 review comment outstanding. | [#28663](https://github.com/apache/flink/pull/28663) | 2026-07-06 | **open** | 38 d & counting | **Open** |
| FLINK-40345 | 1 | Bug fix: buffer recycled twice in `FilteringHandler#recoverWithFiltering` when delivery fails. | [#28936](https://github.com/apache/flink/pull/28936) | 2026-08-06 | 2026-08-07 | 1 d | 2026-08-07 |
| FLINK-40386 | 1 | Logs which channel-state recovery path is taken, the enablement, and its progress/state transitions. | [#28972](https://github.com/apache/flink/pull/28972) | 2026-08-13 | 2026-08-13 | 0 d | 2026-08-13 |

### 1.6 Pre-existing master bugs surfaced by CDR work

Not part of the CDR scope, but they blocked it and had to be fixed first.

| JIRA | Commits | What it does | PR | PR opened | Merged | Open→merge |
|---|---|---|---|---|---|---|
| FLINK-40016 | 2 | `UnalignedCheckpointRescaleITCase` failing with *"Corrupt stream"* — recovered buffers were persisted twice in `LocalInputChannel`. This is the "rare data-loss bug" that blocked the CDR PRs from 2026-06-23 to 2026-07-02. | [#28605](https://github.com/apache/flink/pull/28605) / [#28607](https://github.com/apache/flink/pull/28607) | 2026-07-01 | 2026-07-02 | 1 d |
| FLINK-40269 | 2 (+5 backports) | UC restore could fail after rescale when a two-input task reads from the same upstream twice — channel-state assignment for duplicate connections. Backported to 2.3 / 2.2 / 2.1 / 2.0 / 1.20. | [#28856](https://github.com/apache/flink/pull/28856) + [#28888](https://github.com/apache/flink/pull/28888)/[#28889](https://github.com/apache/flink/pull/28889)/[#28892](https://github.com/apache/flink/pull/28892)/[#28893](https://github.com/apache/flink/pull/28893)/[#28894](https://github.com/apache/flink/pull/28894) | 2026-07-31 | 2026-08-01 | 1 d |

### 1.7 Totals

| | Count |
|---|---|
| CDR JIRAs delivered (excl. umbrellas) | **18** (17 Resolved, 1 Open) |
| Commits merged into `apache/flink` master | **66** (+10 backport commits) |
| Commits written but not yet merged (FLINK-40081) | **2** |
| Merged PRs | **20** (incl. 5 backports) |
| PRs opened and later abandoned / superseded | **6** (#27905, #28073, #28107, #28517, #28613, #28614) |
| Review comments received across CDR PRs | **~248** |
| Fork PRs used for internal pre-review | **11** (1996fanrui/flink #13–#23) |

---

## 2. Chronological timeline

### 2.1 Design phase (before any code in this range)

| Date | Event |
|---|---|
| 2024-07-05 | FLINK-35761 "FLIP-547: Support checkpoint during recovery" created in Apache JIRA. |
| 2024-08-09 | Internal design doc *"[DRAFT] Support checkpointing during recovery"* under discussion (Roman Khachatryan). Estimation explicitly deferred: *"Estimation would be possible after an initial design"*. |
| 2025-07-04 | **NGN-987** epic created. |
| 2025-08-08 | FLIP-547 Confluence page created. |
| 2025-09-19 | **INIT-12544** created (initiative-level tracking). |
| 2025-10-14 | Target Release Month set to **2026-03** (Stefan Richter). Status 🟢 Green. |
| 2025-10-21 | Sub-tasks FLINK-38541 / 38542 / 38543 / **38544** created. |

**NGN-987 development estimate as written at the time — 10 weeks total:**

| Item | Estimate |
|---|---|
| Recover output buffers of upstream task on downstream task side directly (NGN-1000) | 2 weeks |
| Introducing config option `…during-recovery.enabled` (NGN-1180) | (bundled) |
| Filtering record before processing **without** spilling strategy (NGN-1368) | 2 weeks |
| Support checkpoint for `LocalInputChannel` (NGN-1371) | 1.5 weeks |
| Change the overall restore process, JM and task initialization (NGN-1124) | 1.5 weeks |
| Testing and unforeseen code change | 1 week |
| **Filtering record before processing WITH spilling strategy (NGN-1125)** | **2 weeks** |

### 2.2 Phase 1 — recover output buffers on downstream (2025-11 → 2026-01)

| Date | Event |
|---|---|
| 2025-11-19 → 12-12 | Local branches `38542/poc-branch2`, `38542/recover-output-buffers-on-downstream`, `38542/run-tests`. |
| 2025-11-02 | PR **#27182** opened. |
| 2025-11-13 | First review by **pnowojski** (11 days after opening). |
| 2026-01-13 | PR #27182 merged. **72 days open, 61 of them after the first review** (spans the Nov–Dec holiday period). 4 commits, +1034/−187, 17 files, 9 review comments. |

### 2.3 Phase 2 — CDR without spilling, "V1" (2026-01 → 2026-04)

| Date | Event |
|---|---|
| 2026-01-15 | FLINK-38930 created. V1 POC branches start (`38544/poc`, `38544/poc-backup-*`). |
| 2026-01-27 / 01-29 / 02-18 / 02-19 | Successive POC backups; `backup/38544-checkpointing-during-recovery-before-squash`. |
| 2026-02-20 | PR **#27639** (docs + randomization) opened. |
| 2026-02-21 → 03-12 | ~18 local branches: commit organisation, test introduction, checkstyle, batch-shuffle breakage fix, "code review again", "polish code", and 3 branches debugging *"checkpoint is slow after rescaling"*. |
| 2026-02-26 | PR **#27688** (FLINK-39140 ITCase enabler) opened → merged 2026-03-04. |
| **2026-03-10** | Retro: *"Finished the development for checkpointing during recovery V1"*. |
| **2026-03-12** | **Target Release Month slips 2026-03 → 2026-06** (Graeme Morgan). |
| 2026-03-17 | INIT-12544 status: *"On track, Rui will soon complete the POC."* |
| 2026-03-18 | PRs **#27782** (config option) and **#27783** (filtering without spilling) opened. |
| 2026-03-24 | INIT status: *"Rui Fan nearing completion of the POC but is currently on PTO."* |
| 2026-03-26 | #27782 merged (8 d). |
| 2026-03-31 | #27783 merged (13 d, 26 review comments, 13 review submissions). PRs **#27861** and **#27862** opened. |
| **2026-04-07** | #27861 + #27862 merged (7 d each). Retro: *"Checkpointing during recovery V1 is 90% reviewed, only left update docs and randomize the flag."* |
| 2026-04-07 / 04-11 | Test-infra bugs FLINK-39408 and FLINK-39423 found and fixed (1–2 d each). |
| **2026-04-13** | #27639 docs merged (52 d open). **Phase 2 / V1 functionally complete.** |
| 2026-04-22 | FLINK-39519…39524 created as sub-tasks of FLINK-38544. |
| 2026-04-28 | PR #28001 (FLINK-39519) merged. |

**Phase 2 — development window 2026-01-14 → 2026-03-12 (the 03-18…04-13 tail is PR review, and
Phase 3 development had already started on 03-13). Calendar 8 weeks; 28 active authoring days
≈ 5.5 weeks. Estimated: 6 weeks (2 + 1.5 + 1.5 + 1).**

### 2.4 Phase 3a — spilling, first attempt ("v1 spilling", 2026-03-13 → 2026-05-19)

| Date | Event |
|---|---|
| 2026-03-13 | First spilling POC branch (`38544-spilling/20260313-backup-poc-spilling-logic`). |
| 2026-03-25 | Fork PR **#13** — *"Two-stage buffer model and spilling logic for channel state recovery"* (internal pre-review). |
| 2026-04-01 | Fork PR **#14** — *"Refactor spilling core components **per updated design**"* → first design revision. |
| 2026-04-08 | Apache PR **#27905** opened as a placeholder. Branches `20260408-03-redeisn-spilling` → second design revision. |
| 2026-04-10 → 04-16 | `20260410-01/02-redeisn-spilling`, `20260416-01-re-develop` → re-development. |
| 2026-04-20 → 04-24 | Fork PRs **#15–#18** — repeated "organize-commits / address-comments" rounds. |
| 2026-04-21 / 04-28 | INIT status: *"On track, Rui making progress"* → *"On track for TRM, Rui is currently debugging some edge cases and race conditions."* |
| 2026-04-29 | Apache PR **#28073** `[WIP]` opened: 17 commits, **+6469/−753 over 46 files**. Never reviewed. |
| 2026-05-03/04 | Branches `fix-race-condition`, `fix-non-rescale-cases`. |
| 2026-05-04 | Apache PR **#28107** opened — the complete first spilling implementation: **+6643/−796 over 46 files**. |
| 2026-05-06 | Review from **rkhachatryan** (10 review comments). |
| 2026-05-12 | INIT status: *"On track for TRM, Rui is wrapping up the work."* |
| **2026-05-19** | **INIT-12544: *"On track, but requires 2-3 additional week after the team's code review resulted in a request for low level re-design."*** Retro 2026-05-19 agenda item: *"Discuss how to simplify spilling logic for checkpointing during recovery"*; *"Checkpointing during recovery dragging out. Late surfacing of problems or complexity."*; *"Checkpoint during recovery timeline — discussion is in progress, maybe need 2-3 weeks."* |

**Phase 3a before being discarded: calendar 2026-03-13 → 2026-05-19 = 67 days, of which
~14 days were PTO and 11 days were review wait. 24 active authoring days ≈ 5 weeks of work,
all of it written off. Estimated for the whole spilling item: 2 weeks.**

### 2.5 Phase 3b — spilling, redesign & second implementation ("spilling v2", 2026-05-20 → today)

| Date | Event |
|---|---|
| **2026-05-20** | New design doc *"Spilling design for checkpointing during recovery"* created; fork PR **#20** *"Spilling design…"*. Branch namespace changes `38544-spilling/*` → **`38544-spilling-v2/*`**. |
| 2026-05-21/22 | `38544-spilling-v2/20260521-poc`, `20260522-01-poc`; fork PR **#21** *"Spilling v2 POC phases 1-5"*. |
| 2026-05-25 → 05-28 | Fork PRs **#22**, **#23**; branches for reordering, folding fixes into phases, polishing comments. |
| 2026-05-26 | INIT status: *"about 2 more weeks until we expect this to be done."* |
| 2026-05-29 → 06-05 | `address-comments`, `polish-comments`, `single-queue-sentinel`, `address-roman-s-comments` (two rounds). |
| 2026-06-02 | INIT status: *"about 1 more week until we expect this to be done."* **FLINK-35761 (FLIP-547 umbrella) marked Resolved.** |
| 2026-06-16 | INIT status: *"review of the re-achitectured spilling logic in progress."* |
| **2026-06-23** | Apache PR **#28517** opened — the complete v2: **+8031/−1055 over 98 files**. INIT: *"last reviews are currently in progress. Progressive rollout might spill."* |
| 2026-06-23 → 07-02 | **Data-loss / data-corruption hunt**: branches `troubleshoot-data-loss`, `debug-data-loss` (×2), `debug-data-corruption`, `check-data-corruption`, `data-corruption-fixed`. |
| 2026-06-24 | NGN-1735: flink-benchmarks run against the v2 branch — no regression on existing paths. |
| **2026-06-30** | INIT: *"Implementation is complete, but the team is still hunting down a rare data loss bug in master. Flagging as **yellow** because we don't know exactly how long this takes."* **Target Release Month slips 2026-06 → 2026-07.** |
| 2026-07-01/02 | Root cause found: **pre-existing master bug FLINK-40016** (recovered buffers persisted twice in `LocalInputChannel`). Fixed and merged in 1 day. |
| 2026-07-02 | PRs **#28613 / #28614** `[DO_NOT_MERGE]` opened — full-picture branches (36 commits, 104 files) for reviewer context, CDR-off and CDR-on. |
| 2026-07-05/06 | Final commit organisation, rebase on master. |
| **2026-07-06** | **The v2 change is split into 7 stacked PRs**: #28651 (39520), #28652 (39521), #28659 (39522), #28660 (39523), #28661 (39524), #28662 (40080), #28663 (40081). Slack to Roman the same day; Roman replies *"I'll start reviewing this week"*. |
| 2026-07-07 | INIT: *"Known bugs seem all fixed, Rui is preparing the rollout."* |
| 2026-07-08 | Reviews start. #28651 merged **07-09** (3 d), #28652 merged **07-10** (4 d, 32 review comments). |
| 2026-07-09 / 07-17 | Stale abandoned PRs **#27905** and **#28107** closed. |
| 2026-07-17 | #28659 merged (11 d). Reviews of #28660 and #28661 begin. |
| 2026-07-29 | #28660 merged (23 d, first review 11 d after opening). |
| 2026-07-30 → 08-04 | **FLINK-40269** found — another pre-existing UC bug; fixed and backported to 5 release branches. |
| **2026-08-10** | **#28661 (FLINK-39524) merged — 35 days open, 97 review comments, 29 review submissions, 12 from rkhachatryan.** |
| 2026-08-06/07 | FLINK-40345 (double buffer recycle) found and fixed in 1 day. |
| **2026-08-12** | First external review lands on **#28662 (FLINK-40080)** — **37 days after it was opened** (it was blocked behind #28661 in the stack). |
| 2026-08-13 | FLINK-40386 (logging) merged. **#28662 and #28663 still open. FLINK-40080 and FLINK-40081 still Open.** |

**Phase 3b so far: calendar 2026-05-20 → 2026-08-13 = 85 days, of which 35 days were idle
review wait (06-08→06-22 and 07-10→07-31). 41 active authoring days ≈ 8 weeks.**
**Phase 3 total (3a + 3b): 65 active authoring days ≈ 13 weeks of work. Estimated: 2 weeks.**

---

## 2.6 Active development time vs. calendar time

Calendar span double-counts: Phase 2 and Phase 3 overlap, and calendar includes review wait,
holidays and time spent on unrelated projects. The table below uses **distinct authoring days**
— days on which CDR code was actually written — derived from commit author-dates across all
140 local CDR branches, attributed to exactly one phase (a commit inherited by a later phase's
branch is counted only in the phase that created it).

| Phase | Dev window | Commits | Active authoring days | ≈ weeks | Estimated |
|---|---|---|---|---|---|
| P1 — FLINK-38542 | 2025-09-26 → 2025-12-12 | 12 | 9 | ~2 w | 2 w |
| P2 — CDR without spilling | 2026-01-14 → 2026-03-12 | 118 | 28 | ~5.5 w | 6 w |
| P3a — spilling v1 (discarded) | 2026-03-13 → 2026-05-20 | 186 | 24 | ~5 w | — |
| P3b — spilling v2 | 2026-05-15 → 2026-08-13 | 320 | 41 | ~8 w | — |
| **P3 total** | | 506 | 65 | **~13 w** | **2 w** |
| **All phases** | 2025-09-26 → 2026-08-13 | 636 | **102** | **~20 w** | **10 w** |

This is a **floor**: rebases and commit reorganisation collapse several days of work onto a
single author-date, and design/debugging days that produced no commit are invisible. It is not
an upper bound either — it counts a day with one commit the same as a full day.

### Idle gaps (≥ 7 consecutive days with no CDR commit)

| Gap | Days | Attribution |
|---|---|---|
| 2025-12-12 → 2026-01-14 | 33 | Year-end holiday + review wait on #27182 |
| 2026-01-30 → 2026-02-17 | 18 | Spring Festival |
| 2026-03-18 → 2026-03-25 | 7 | PTO (INIT-12544 03/24: *"currently on PTO"*) |
| 2026-04-01 → 2026-04-08 | 7 | PTO, continued |
| 2026-05-04 → 2026-05-15 | 11 | Review wait — ended in the 05-19 re-design request |
| 2026-06-08 → 2026-06-22 | 14 | Review wait on spilling v2 |
| 2026-07-10 → 2026-07-31 | 21 | Blocked behind the 7-PR stack; other projects |
| **Total idle** | **111 days (~16 weeks)** | |

Shorter gaps (2025-11-19→12-04, 2025-09-26→11-03) account for the remainder of the 41-week
calendar span.

---

## 3. Review-latency data

Days from PR open → first review by someone other than the author, and open → merge.

| PR | JIRA | Reviewer(s) | Open→1st review | Open→merge | Review comments | Review submissions |
|---|---|---|---|---|---|---|
| #27182 | 38542 | pnowojski | 11 d | **72 d** | 9 | 5 |
| #27639 | 38541 (docs) | rkhachatryan | 47 d | 52 d | 8 | 7 |
| #27688 | 39140 | rkhachatryan | 5 d | 6 d | 4 | 5 |
| #27782 | 38541 | pnowojski | 5 d | 8 d | 2 | 4 |
| #27783 | 38930 | pnowojski, rkhachatryan | 8 d | 13 d | 26 | 13 |
| #27861 | 39018 | pnowojski | 1 d | 7 d | 19 | 15 |
| #27862 | 38543 | pnowojski | 3 d | 7 d | 9 | 8 |
| #28001 | 39519 | rkhachatryan | 4 d | 6 d | 9 | 7 |
| #28107 | 38544 (v1 spilling) | rkhachatryan | 2 d | *never merged* | 10 | 6 |
| #28651 | 39520 | rkhachatryan | 2 d | 3 d | 3 | 3 |
| #28652 | 39521 | rkhachatryan | 2 d | 4 d | 32 | 11 |
| #28659 | 39522 | rkhachatryan | 10 d | 11 d | 6 | 5 |
| #28660 | 39523 | rkhachatryan | 11 d | 23 d | 4 | 3 |
| #28661 | 39524 | rkhachatryan | 11 d | **35 d** | **97** | **29** |
| #28662 | 40080 | rkhachatryan | **37 d** | *open* | 16 | 4 |
| #28663 | 40081 | — | *no review yet* | *open* | 0 | 0 |
| #28856 | 40269 | rkhachatryan, wenshao | 0 d | 1 d | 4 | 7 |
| #28936 | 40345 | spuru9, rkhachatryan | 1 d | 1 d | 0 | 3 |

Reviewers: **Piotr Nowojski** on Phases 1–2, **Roman Khachatryan** on Phase 3.

---

## 4. Change-size data

| PR | Scope | Commits | Diff | Files |
|---|---|---|---|---|
| #27182 | Phase 1 | 4 | +1034 / −187 | 17 |
| #27783 | Phase 2 core | 3 | +1833 / −35 | 27 |
| #27861 | Phase 2 LocalInputChannel | 5 | +731 / −40 | 16 |
| #27862 | Phase 2 restore process | 3 | +352 / −23 | 14 |
| #28073 | **Spilling v1 (abandoned)** | 17 | +6469 / −753 | 46 |
| #28107 | **Spilling v1 (abandoned)** | 7 | +6643 / −796 | 46 |
| #28517 | **Spilling v2 (umbrella, open)** | 7 | **+8031 / −1055** | **98** |
| #28613/#28614 | Spilling v2 full picture (DO_NOT_MERGE) | 36 | +8163 / −1204 | 104 |
| #28652 | 39521 | 8 | +2509 / −423 | 42 |
| #28661 | 39524 | 4 | +2748 / −205 | 21 |
| #28662 | 40080 | 4 | +1614 / −679 | 23 |
| #28663 | 40081 | 6 | +1748 / −705 | 29 |

---

## 5. Slip history (from Confluent JIRA)

| Date | Field | Change | By |
|---|---|---|---|
| 2025-10-14 | Target Release Month | (empty) → **2026-03** | Stefan Richter |
| 2026-03-12 | Target Release Month | 2026-03 → **2026-06** | Graeme Morgan |
| 2026-06-30 | Current Status | 🟢 Green → 🟡 Yellow | Stefan Richter |
| 2026-06-30 | Target Release Month | 2026-06 → **2026-07** | Yan Cui |
| 2026-06-30 | Current Status | 🟡 Yellow → 🟢 Green | Yan Cui |
| 2026-08-13 | — | Still not shipped (FLINK-40080 / 40081 open) | — |

### INIT-12544 weekly status log (verbatim)

| Date | Status |
|---|---|
| 03/17 | On track, Rui will soon complete the POC. |
| 03/24 | On track. Rui Fan nearing completion of the POC but is currently on PTO. |
| 03/31 | On track, Rui making progress with the implementation. |
| 04/07 | On track, Rui making progress with the implementation. |
| 04/21 | On track, Rui making progress with the implementation. |
| 04/28 | On track for TRM, Rui is currently debugging some edge cases and race conditions. |
| 05/12 | On track for TRM, Rui is wrapping up the work. |
| **05/19** | **On track, but requires 2-3 additional week after the team's code review resulted in a request for low level re-design.** |
| 05/26 | On track, about 2 more weeks until we expect this to be done. |
| 06/02 | On track, about 1 more week until we expect this to be done. |
| 06/16 | On track, review of the re-achitectured spilling logic in progress. |
| 06/23 | On track, last reviews are currently in progress. Progressive rollout might spill. |
| **06/30** | **Implementation is complete, but the team is still hunting down a rare data loss bug in master. Flagging as yellow because we don't know exactly how long this takes.** |
| 07/07 | Known bugs seem all fixed, Rui is preparing the rollout. |

---

## 6. Scope: estimated vs. delivered

The NGN-987 estimate named **6 work items**. The work actually shipped as **18 JIRAs**.

| Originally estimated | Actually delivered under it |
|---|---|
| Recover output buffers on downstream (2 w) | FLINK-38542 |
| Config option (bundled) | FLINK-38541 |
| Filtering without spilling (2 w) | FLINK-38930 |
| `LocalInputChannel` checkpoint (1.5 w) | FLINK-39018 |
| Overall restore process (1.5 w) | FLINK-38543 |
| Testing & unforeseen (1 w) | FLINK-39140, FLINK-39408, FLINK-39423 |
| **Filtering with spilling (2 w)** | **FLINK-39519, 39520, 39521, 39522, 39523, 39524, 40080, 40081, 40345, 40386** |
| *(not estimated at all)* | FLINK-40016, FLINK-40269 — pre-existing master bugs |

---

## 7. Verification note on JIRA status history

The Apache JIRA changelog was checked for all 12 resolved CDR sub-tasks. **Every one shows a
single clean transition `Open → Resolved` / `resolution: None → Fixed`** on the date listed in
section 1 — there are no `Won't Do` / `Won't Fix` resolutions that were later corrected, and
no reopens. If a status was mislabelled at some point, it was in the internal NGN tickets
(NGN-1000 / 1124 / 1125 / 1180 / 1368 / 1371), not in Apache JIRA, so the Apache resolution
dates in this document can be used as-is.
