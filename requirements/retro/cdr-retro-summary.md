# Checkpointing During Recovery — Retro Summary

Data: [`cdr-facts-timeline.md`](./cdr-facts-timeline.md) · As of 2026-08-13

## TL;DR

Estimate was **10 weeks of dev+test**. Actual dev+test is **~22 weeks** — a 2.2× miss.
The rest of the 42-week calendar is review wait, leave, and time on other projects.

**All of the overrun is in Phase 3. Phases 1 and 2 came in close to estimate.**

**Status: feature-complete.** FLINK-40080 merged 2026-08-13 — the last substantial PR. Only
FLINK-40081 remains (#28663): **2 commits, +164/−17 across 3 files** — one test
(`CdrRecoveryRaceITCase`) and one hardening of `RecordFilterContext` for minimal/batch
environments.

### Per-phase breakdown

| Phase | Dev: start → code handed to review | Dev span | Deducted (leave / on-call / offsite / holidays) | **Net dev** | Review: PR → merged | Review calendar | Estimated |
|---|---|---|---|---|---|---|---|
| **1** — FLINK-38542 | 2025-10-21 → 2025-11-02 | 13 d | — | **2.5 w** | 2025-11-02 → 2026-01-13 | 72 d | **2 w** |
| **2** — CDR w/o spilling | 2026-01-14 → **2026-03-12** | 57 d | 4 d on-call | **7.6 w** | 2026-03-18 → 2026-04-13 | 27 d | **6 w** |
| **3a** — spilling v1, **discarded** | 2026-03-13 → 2026-05-04 | 52 d | 7 d leave + 5 d on-call + 2 d holiday | **5.4 w** *(all written off)* | 2026-05-04 → 2026-05-19 | 15 d | — |
| **3·redesign** — new design, discussion, alignment | 2026-05-05 → 2026-05-20 | 15 d | 1 d holiday | **2.0 w** | — | — | — |
| **3b** — spilling v2 | 2026-05-21 → 2026-07-06 | 46 d | 5 d offsite + 6 d on-call + 1 d holiday + 8 d defect hunt | **3.8 w** | 2026-07-06 → 2026-08-13 | 38 d | — |
| **FLINK-40016** — hunt & fix a Phase-2 defect | 2026-06-23 → 2026-07-02 | 9 d | 1 d on-call | **1.1 w** | — | — | *not estimated* |
| **Phase 3 total** *(excl. the defect hunt)* | 2026-03-13 → 2026-07-06 | 116 d | 35 d | **11.2 w** | | 53 d | **2 w** |
| **All phases** | 2025-10-21 → 2026-08-13 | 296 d (42 w) | 5 w leave + **3.5 w on-call, offsite & holidays** | **~22 w** | | 152 d | **10 w** |

**Total effort ≈ 22 weeks against a 10-week estimate (2.2×).** Phase 1 came in at 2.5 w vs 2 w
and Phase 2 at 7.6 w vs 6 w — both close. **Phase 3 came in at 11.2 weeks against 2 — 5.6×**, plus
1.1 w on the FLINK-40016 hunt, which belongs to no phase.

Where the 42 calendar weeks went: **~22 w working · 5 w leave · 3.5 w on-call, offsite and public
holidays · ~11 w waiting on review or on other projects.**

*Development time = from the first line of code in a phase to the point where all its development
is finished, minus confirmed leave, on-call shifts, the team offsite and NRW public holidays.
Splitting the work into
PRs and handling review comments afterwards are counted separately — they are asynchronous and
mostly spent waiting on others.*

### Leave taken (both windows confirmed by zero commits)

| Dates | Length |
|---|---|
| 2025-12-12 → 2026-01-09 | **4 weeks** — fell entirely inside the #27182 review window |
| 2026-03-18 → 2026-03-25 | **1 week** — INIT-12544 03/24: *"currently on PTO"* |

### On-call and offsite — 22 days pulled out of development

Beyond leave, two other things consumed development days: **on-call shifts** and a **one-week
team offsite**. Neither is development, so both are subtracted from each phase's figure.

| Date | Days | Kind | Falls in |
|---|---|---|---|
| 2026-01-21 → 01-23 | 3 | On-call | Phase 2 |
| 2026-02-11 | 1 | On-call | Phase 2 |
| 2026-03-18 | 1 | On-call | *inside the PTO week — not double-counted* |
| 2026-04-05 → 04-08 | 4 | On-call | Phase 3a |
| 2026-04-22 | 1 | On-call | Phase 3a |
| 2026-05-25 | 1 | On-call | Phase 3b |
| **2026-06-08 → 06-12** | **5** | **Team offsite all week + 2 days leave — fully unavailable** | Phase 3b |
| 2026-06-14 → 06-17 | 4 | On-call | Phase 3b |
| 2026-06-26 | 1 | On-call | Phase 3b |
| 2026-08-03 | 1 | On-call | *inside the review window, not development* |

**17 on-call days plus a 5-day offsite. 20 of those 22 days fall inside a development window**
and are deducted; the other two land inside the March PTO week and inside Phase 3b's review
window, so they change nothing.

Per phase:

| Phase | Before | On-call / offsite deducted | After *(intermediate)* |
|---|---|---|---|
| Phase 1 | 2.5 w | — | **2.5 w** |
| Phase 2 | 8.1 w | 4 d on-call | **7.6 w** |
| Phase 3a | 6.4 w | 5 d on-call | **5.7 w** |
| Phase 3 redesign | 2.1 w | — | **2.1 w** |
| Phase 3b | 6.6 w | 5 d offsite + 6 d on-call | **5.0 w** |
| **Total** | 25.7 w | **20 d ≈ 2.9 w** | ≈ 23 w |

These are intermediate figures. Two further deductions follow below — the FLINK-40016 defect
hunt (Phase 3b 5.0 w → 3.9 w) and public holidays — bringing the project to **≈ 22 w**.

The offsite week matters more than its five days suggest: it sits at 2026-06-08, right in the
middle of Phase 3b, and it explains most of the 14-day commit gap that had previously been
attributed entirely to waiting on review.

### Public holidays (North Rhine-Westphalia, 2026) — 4 more days

The work is done from Düsseldorf, so NRW public holidays apply. Most of them either fall on a
weekend or land inside a window already deducted, so only four cost anything:

| Date | Holiday | Falls in | Effect |
|---|---|---|---|
| 2026-04-03 Fri | Karfreitag | Phase 3a | **deducted** |
| 2026-04-06 Mon | Ostermontag | Phase 3a | inside the 04-05→04-08 on-call block |
| 2026-05-01 Fri | Tag der Arbeit | Phase 3a | **deducted** |
| 2026-05-14 Thu | Christi Himmelfahrt | Phase 3 redesign | **deducted** |
| 2026-05-25 Mon | Pfingstmontag | Phase 3b | on-call that day, already deducted |
| 2026-06-04 Thu | Fronleichnam | Phase 3b | **deducted** |
| 2026-10-03, 11-01, 12-26 | Einheit / Allerheiligen / 2. Weihnachtstag | — | fall on weekends |

**4 days ≈ 0.6 weeks**, all inside Phase 3: 3a 5.7 w → **5.4 w**, redesign 2.1 w → **2.0 w**,
3b 3.9 w → **3.8 w**. Phases 1 and 2 are unaffected — no working-day holiday falls inside
either window.

### The FLINK-40016 hunt is counted once, on its own line

From **2026-06-23 to 07-02** the work was not building spilling — it was chasing a data loss
that turned out to be **FLINK-40016, a defect introduced by Phase 2's FLINK-39018** and sitting
in master since 2026-04-07. That is 9 days; one of them (06-26) was an on-call shift already
deducted, leaving **8 days ≈ 1.1 weeks**.

Those days fall inside Phase 3b's calendar span, so they are **subtracted from Phase 3b
(5.0 w → 3.8 w after holidays) and shown as their own line item** rather than being charged to Phase 3b or
back to Phase 2. The project total is unchanged — this is a re-attribution, not an addition.

### Idle windows that were **not** leave

| Dates | Days | |
|---|---|---|
| 2026-01-30 → 2026-02-17 | 18 | *cause not established — needs confirming* |
| 2026-06-08 → 2026-06-22 | 14 | Waiting on review of spilling v2 |
| 2026-07-10 → 2026-07-31 | 21 | Blocked behind the 7-PR stack — worked on other projects |

## Phase 2 in detail — how the 7.6 weeks is pinned down

**Definition used here: development time runs from the first line of code to the point where all
development is finished. Splitting the work into PRs, and handling review comments afterwards,
are counted separately — they are asynchronous and mostly spent waiting on someone else.**

### What actually happened

Phase 2 was **not** submitted as five PRs. It was submitted as **one big PR**,
[#27639](https://github.com/apache/flink/pull/27639), opened on **2026-02-20** under the title
*"FLIP-547: Support checkpoint during recovery"* — the whole feature in a single change. Review
asked for it to be broken up, and over the next six weeks it was progressively carved into four
separate PRs while the original shrank to nothing:

| Date | Event |
|---|---|
| 2026-02-20 | #27639 opened as *"FLIP-547: Support checkpoint during recovery"* |
| 2026-03-09 | Renamed to *"**[WIP]** FLIP-547: Support checkpoint during recovery"* |
| 2026-02-21 → 04-01 | **25 force-pushes** onto the same branch |
| **2026-03-18** | 2 force-pushes — the same day [#27782](https://github.com/apache/flink/pull/27782) (config option) and [#27783](https://github.com/apache/flink/pull/27783) (filtering) are split out |
| **2026-03-31** | 6 force-pushes — the same day [#27861](https://github.com/apache/flink/pull/27861) (LocalInputChannel) and [#27862](https://github.com/apache/flink/pull/27862) (restore process) are split out |
| 2026-04-13 | #27639 merges, reduced to **+23/−8 across 5 files** — just the docs commit |

Each carve-out shows up as a force-push on the original PR removing the extracted commits.

### Why development ended on 2026-03-12

Three independent signals converge on the same date, all of them **before** the splitting began:

1. **Last Phase-2 code was authored 2026-03-12** — the `[FLINK-39018]` "Notify PriorityEvent to
   downstream task even if it is blocked" commit. Nothing new was written after that; the later
   commits (03-26 rename, 03-27 heap fallback, 03-31 buffer ownership) all answer review
   comments.
2. **Last force-push of the WIP big PR was 2026-03-12.** The next events are a self-review on
   03-16 and then the first split on 03-18.
3. **The 2026-03-10 team retro records** *"Finished the development for checkpointing during
   recovery V1."*

The decisive structural argument: **splitting started on 2026-03-18, and you cannot split
finished work that is not finished.** So development was complete on or before that date, and
03-12 is where the code signal stops.

### The number

**2026-01-14 → 2026-03-12 = 57 days, minus 4 days of on-call = 53 days ≈ 7.6 weeks, against a
6-week estimate.** A 27% overrun — the smallest miss in the project.

Everything after 03-12 — the 25 force-pushes, four carve-outs, and review iterations through
04-13 — is PR splitting and asynchronous review, not development. The clearest proof: the code
in #27861 and #27862 carries an **author date of 2026-02-18**, six weeks before those PRs were
created on 03-31.

## The one-sentence cause

**Spilling was estimated at 2 weeks in Oct 2025 with no design behind the number, and the
first spilling design document was written on 2026-05-20 — nine weeks after coding started
and only after the first 6,600-line implementation had already been rejected in review.**

## Where Phase 3's 9.2 extra weeks went

Phase 3 was estimated at 2 weeks and took 11.2. The three components:

1. **5.4 weeks discarded (Phase 3a).** PR [#28107](https://github.com/apache/flink/pull/28107)
   (+6,643/−796, 46 files) was complete and in review when the review concluded the approach
   needed a low-level re-design (INIT-12544, 05/19). Everything from 03-13 to 05-19 was
   written off. Cause: the design was never reviewed before the code was.
2. **2.0 weeks of redesign, discussion and alignment** (05-05 → 05-20) to produce the design that
   should have existed before Phase 3a started.
3. **3.8 weeks rebuilding it (Phase 3b) — i.e. genuine under-scoping.** The single "2 weeks —
   filtering with spilling" line became **10 JIRAs, +8,031/−1,055 across 98 files**: a segmented
   on-disk format, a forward-only reader with snapshot/resume, a push-based input-channel
   contract, an async restructuring of `StreamTask` recovery, a recovery-checkpoint trigger
   protocol, an incremental drainer with atomic snapshot-and-insert, and checkpoint replay of
   spilled segments. Each is comparable in size to a Phase-2 item estimated at 1.5–2 weeks.

Inside those, **~2 weeks went to bugs that were not ours.** FLINK-40016 (a *pre-existing* master
bug that persisted recovered buffers twice) was chased for 9 days as a suspected data loss in
the new code, then fixed in 1 day once identified. Plus FLINK-40269 and the test-infra bugs
FLINK-39408 / FLINK-39423 — the latter revealed the CDR flag had **never actually been
randomized in tests**, so earlier green runs were not testing CDR at all.

**Review wait is the second story.** The v2 change was split into 7 stacked PRs, which was the
right call — the front of the stack merged in 3 and 4 days. But a 7-deep stack with one
reviewer serialises: [#28661](https://github.com/apache/flink/pull/28661) took 35 days and 97
review comments, and [#28662](https://github.com/apache/flink/pull/28662) sat 37 days before
its first review purely because it was behind #28661.

## What went well

- Phases 1 and 2 came in close to estimate (2.5 w vs 2 w, 7.6 w vs 6 w), and Phase-2 PRs merged
  in 6–13 days each.
- The 7-PR split made a 98-file change reviewable at all.
- Every pre-existing master bug found was fixed within a day of correct diagnosis;
  FLINK-40269 was backported to five release branches.
- No performance regression (flink-benchmarks, NGN-1735, 2026-06-24).
- **The feature is complete.** The bounded-memory spilling path merged on 2026-08-13; what
  remains is one 164-line PR carrying a race ITCase and a small hardening.

## Proposals

1. **No estimate on an undesigned component.** The answer is "N days to design, then estimate",
   not a number. The 2-week spilling estimate is the single largest source of the slip.
2. **Design sign-off before implementation above ~1,000 lines.** A one-page design reviewed in
   March would have cost days instead of 4 weeks of discarded code plus 2 weeks of rework.
3. **Re-estimate after a re-design.** A review that requests a re-design invalidates the
   estimate; the new number should come from the new design, not from extending the old one.
4. **Budget reviewer capacity for stacked PRs up front,** or split along axes that allow
   parallel review. 21 of the last 41 calendar days were spent blocked, not building.

## Status today

**17 of 18 CDR JIRAs merged — 66 commits on master. The feature is complete.**
FLINK-40080 merged on 2026-08-13, removing the unbounded heap fallback and switching recovery
to the fetch→drain spilling path. The bounded-memory guarantee that Phase 3 existed to deliver
is now in.

One PR left: **FLINK-40081** ([#28663](https://github.com/apache/flink/pull/28663)) — 2 commits,
+164/−17 across 3 files: a `CdrRecoveryRaceITCase` for the non-source recovery race, and
hardening of `RecordFilterContext` for minimal/batch environments. One review comment
outstanding.
