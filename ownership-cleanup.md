# Spilled channel-state cleanup: ownership model

The recovery stages run on `channelIOExecutor` and are **never interrupted on cancel** (it only gets a
graceful `shutdown()`, and cancel interrupts just the main task thread) — this is the existing design.
So rather than deleting files from an external cancel-time hook, each stage owns its spill state: it
either finishes and hands ownership to the next stage, or fails/aborts and cleans up its own state.
Cancellation is not handled specially — it always surfaces as a stage's own failure, or as a successful
hand-off whose next owner is then responsible.

| Stage | Own run fails (throws) | Cancelled mid-run | Runs successfully |
|---|---|---|---|
| **1 — fetch** (`readInputData`) | `handedOff=false` → `finally` deletes its own spill files | Not handled directly: teardown makes it throw (→ own failure, self-clean) or it finishes first (→ success, handed to stage 2) | `handedOff=true`, no cleanup; `return`s the state to stage 2 |
| **2 — gap** (`requestPartitions` + `buildDrainer`) | Holds the state → `release(state)` deletes the files | Surfaces as its own failure (e.g. mailbox closed) → `release(state)` — this covers a cancel after stage 1 already succeeded | Builds the drainer (which takes a grant) → no cleanup; handed to stage 3 |
| **3 — drain** | try-with-resources `close()` → release deletes the files | Not interrupted → runs to completion or throws; both go through `close()` → cleanup | `close()` → release; files deleted normally |

**Note:** "Cancelled mid-run" is never a separate mechanism — it always collapses into the stage's own
failure or a successful hand-off, so no stage needs to sense cancellation. One edge case:
`thenRunAsync(drain)` rejected after the executor is shut down (drain never runs) — the submit site must
`close(drainer)` in the rejection path.
