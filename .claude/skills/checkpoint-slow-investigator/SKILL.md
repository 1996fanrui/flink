---
model: opus
---

# Checkpoint Slow Investigator

Automated checkpoint timeout investigation: loop test until checkpoint gets stuck, then capture diagnostics, query REST API, and notify.

> **Note**: This skill investigates checkpoint timeout issues. It loops a test until the checkpoint gets stuck (exceeds expected time), then keeps the test running with extended timeout, captures jstack, queries Flink REST API, and notifies the user.
> For simple loop testing to find flaky tests, use `/flink-test-analyzer` instead.

You are a **coordinator agent**. You NEVER execute commands directly. All concrete work MUST be delegated to sub agents via the Task tool.

## Input

```
/checkpoint-slow-investigator org.apache.flink.test.checkpointing.UnalignedCheckpointRescaleWithMixedExchangesITCase
/checkpoint-slow-investigator org.apache.flink.TestA --expected-time 1200 -n 50
```

Arguments:
- **Test target**: Fully qualified test class names (comma-separated)
- **`--expected-time N`**: Expected max single run time in seconds (default: 300)
- **`-n N`**: Max loop iterations (default: 100)

Global variables:
- `TIMESTAMP`: `YYYYMMDD_HHmmss`
- `WORKTREE_PATH`: `/tmp/claude-tmp/ckpt-investigate-{TIMESTAMP}`
- `PROJECT_ROOT`: Working directory when skill was invoked
- `BRANCH_NAME`, `COMMIT_HASH`: Current git state
- `TEST_CLASSES`, `EXPECTED_TIME`, `MAX_ITERATIONS`: From arguments
- `LOG_DIR`: `{WORKTREE_PATH}/test-logs`
- `ARCHIVE_DIR`: `{PROJECT_ROOT}/log/checkpoint-investigator_{TIMESTAMP}`
- `REST_PORT`: `12345`

## Phase 1: Environment Preparation

**Goal**: Isolate work in a git worktree.

**I/O**:
- Input: `PROJECT_ROOT`, current git state
- Output: `WORKTREE_PATH` created, `BRANCH_NAME` and `COMMIT_HASH` recorded

**Steps** (delegate to sub agent):
1. Auto-commit uncommitted changes (message: `[auto-commit] Save local changes before checkpoint investigation`)
2. Create worktree: `git worktree add {WORKTREE_PATH} -b investigate-{TIMESTAMP} HEAD`

**Gate**: Worktree exists and contains `pom.xml`.

**Verification** (separate sub agent): Verify worktree dir exists with project files.

## Phase 2: Code Modification (in worktree)

**Goal**: Extend checkpoint timeout to 6h and fix REST port so the stuck test stays alive and queryable.

**I/O**:
- Input: Test source files in `{WORKTREE_PATH}`
- Output: Modified test files with timeout=6h, REST port={REST_PORT}, imports added

**Steps** (delegate to sub agent):
1. For each test class, apply modifications per `references/phase2_code_modification.md`
2. Run `cd {WORKTREE_PATH} && ./mvnw spotless:apply -pl {module}` on each modified module (e.g., `-pl flink-tests`) to fix code formatting

**Gate**: Each modified file contains `CHECKPOINTING_TIMEOUT` set to 6h and `RestOptions.PORT` set to `{REST_PORT}`. Spotless applied.

**Verification** (separate sub agent): Read each modified file, confirm both settings and imports present.

## Phase 3: Project Build

**Goal**: Build the worktree to compile the modified tests.

**I/O**:
- Input: Modified worktree
- Output: `BUILD SUCCESS`

**Steps** (delegate to sub agent):
1. `cd {WORKTREE_PATH} && zsh rui_tools/build_with_specific_version.sh`
2. If fails: retry once. If still fails: **stop workflow** (trigger Error Handling).

**Gate**: Build output contains `BUILD SUCCESS`.

**Verification** (separate sub agent): Confirm `BUILD SUCCESS` in output.

## Phase 4: Loop Test + Monitor (Core Phase)

**Goal**: Loop the test until it gets stuck or all iterations pass. On stuck detection, capture jstack snapshots, notify user, and exit **leaving the test process alive**.

**I/O**:
- Input: Built worktree, `TEST_CLASSES`, `EXPECTED_TIME`, `MAX_ITERATIONS`
- Output: `{LOG_DIR}/stuck_info.txt`, `{LOG_DIR}/summary.log`, jstack files (if stuck), iteration logs

**Steps** (delegate to sub agent):
1. `mkdir -p {LOG_DIR}`
2. Write env vars and run `scripts/investigate_loop.sh`:
   ```bash
   export TEST_CLASSES="{TEST_CLASSES}" EXPECTED_TIME={EXPECTED_TIME} MAX_ITERATIONS={MAX_ITERATIONS} \
          LOG_DIR="{LOG_DIR}" WORKTREE_PATH="{WORKTREE_PATH}" REST_PORT={REST_PORT}
   cd {WORKTREE_PATH} && bash .claude/skills/checkpoint-slow-investigator/scripts/investigate_loop.sh
   ```

**Critical design**: When stuck is detected, the script `disown`s the test process and exits immediately. The test process stays alive so Phase 5 can query its REST API. Phase 7 is responsible for killing it (only with human confirmation).

**Gate**: `stuck_info.txt` exists. If stuck: test process still alive (`kill -0 {test_pid}`).

**Verification** (separate sub agent): Verify `stuck_info.txt` and `summary.log` exist. If stuck, verify jstack files exist and test process is alive.

**Timeout**: Up to 24 hours.

## Phase 5: Auto Investigation (only if stuck detected)

**Skip if `stuck_info.txt` contains `no_stuck=true`.**

**Goal**: Analyze jstack captures and query Flink REST API to diagnose why the checkpoint is stuck. Focus on business-level deadlocks and backpressure (more common than Java deadlocks).

**I/O**:
- Input: jstack files in `{LOG_DIR}`, live REST API at `http://localhost:{REST_PORT}`
- Output: `{LOG_DIR}/investigation_report.md`

**Steps** (delegate to sub agent): Follow `references/phase5_investigation.md` — analyze jstack (deadlocks, circular waiting, backpressure), query REST API (checkpoint progress, per-vertex backpressure), generate report.

**Gate**: `investigation_report.md` exists with all required sections.

**Verification** (separate sub agent): Verify report exists and contains Summary, jstack Analysis, REST API Analysis, Diagnosis sections.

## Phase 6: Archive

**Goal**: Copy results from worktree to persistent project directory.

**I/O**:
- Input: `{LOG_DIR}/*`
- Output: `{ARCHIVE_DIR}` with all results copied

**Steps** (delegate to sub agent):
1. `mkdir -p {ARCHIVE_DIR}`
2. Copy: `summary.log`, `stuck_info.txt`, `iter*_jstack_*.log`, `investigation_report.md`, `iteration_*.log`

**Gate**: `{ARCHIVE_DIR}` contains `summary.log` and `stuck_info.txt`.

**Verification** (separate sub agent): Verify archive contents. If stuck, verify `investigation_report.md` and jstack files also present.

## Phase 7: Wait for Human Confirmation + Cleanup

**This phase MUST NOT proceed automatically when stuck is detected.**

**I/O**:
- Input: `stuck_info.txt` (to determine if human confirmation needed)
- Output: Worktree and branch removed (or preserved if user chooses)

**Steps**: Follow `references/phase7_cleanup.md`.
- **Stuck detected**: Use `AskUserQuestion` — user chooses "Clean up now" or "Keep running"
- **No stuck**: Cleanup automatically (no live process to preserve)

## Final Output

**If stuck:**
```
Checkpoint stuck at iteration {stuck_iteration} after {elapsed}s (expected max: {EXPECTED_TIME}s).

TEST STILL RUNNING:
  PID: {test_pid} | Java PID: {java_pid}
  REST API: http://localhost:{REST_PORT}
  Worktree: {WORKTREE_PATH}

Archive: {ARCHIVE_DIR}
```
Output contents of `{ARCHIVE_DIR}/investigation_report.md`, then proceed to Phase 7.

**If no stuck:**
```
All {MAX_ITERATIONS} iterations completed within {EXPECTED_TIME}s.
Archive: {ARCHIVE_DIR}
```
Proceed to Phase 7 cleanup directly.

## Error Handling

- If any phase fails, attempt recovery once before aborting
- On abort: do NOT automatically kill the test process or remove the worktree
- Output: which phase failed, error details, test PID (if available), worktree path
- Let the user decide whether to clean up
