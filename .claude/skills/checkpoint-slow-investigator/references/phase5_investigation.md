# Phase 5: Investigation Details

## 5a: jstack Analysis

1. Read all jstack files from `{LOG_DIR}/iter*_jstack_*.log`
2. **Deadlock detection**: grep for `"Found one Java-level deadlock"`. If found, extract the deadlock details — this is the most direct root cause.
3. **Business-level deadlock detection** (more common than Java deadlocks):
   - Compare multiple jstack captures to find threads that are consistently stuck at the same stack position
   - Look for circular waiting patterns: thread A waiting on a condition that thread B should signal, while thread B is waiting on something from thread A
   - Focus on Flink-specific threads: `CheckpointCoordinator`, `SourceCoordinator`, `mailbox`, `InputGate`, `OutputFlusher`, `Checkpoint Timer`, etc.
4. **Backpressure / blocked data flow analysis**:
   - Look for threads stuck in `requestBuffer`, `getNextBuffer`, `waitForData`, or similar buffer pool / network stack frames
   - Identify if upstream tasks are blocked waiting for buffer credits while downstream tasks are blocked waiting for data (mutual waiting)
5. Summarize findings with focus on the actual blocking chain

## 5b: Flink REST API Query

Query `http://localhost:{REST_PORT}`:

1. `GET /jobs` — List running jobs and their states
2. For each running job:
   - `GET /jobs/{job-id}/checkpoints` — Get checkpoint statistics:
     - Which checkpoint ID is currently in progress
     - How long it has been pending
     - Which subtasks have acknowledged vs. pending
   - `GET /jobs/{job-id}/checkpoints/detail/{checkpoint-id}` — Detailed per-subtask status of the in-progress checkpoint
   - `GET /jobs/{job-id}/vertices` — Get all task vertices
   - For each vertex: `GET /jobs/{job-id}/vertices/{vertex-id}/backpressure` — Check backpressure status
   - Identify: which operators have high backpressure, which subtasks are slow
3. Correlate checkpoint pending subtasks with backpressure data to narrow down the bottleneck

Note: REST API may not be accessible if the MiniCluster has already shut down. Handle errors gracefully — if queries fail, note it in the report and rely on jstack analysis alone.

## 5c: Investigation Report Template

Generate `{LOG_DIR}/investigation_report.md`:

```markdown
# Checkpoint Investigation Report

## Summary
- Test class: {TEST_CLASSES}
- Stuck at iteration: {stuck_iteration}
- Time elapsed before stuck detected: {elapsed}s
- Expected max time: {EXPECTED_TIME}s
- Timestamp: {TIMESTAMP}

## jstack Analysis

### Deadlock Detection
(Java-level deadlock findings or "No Java-level deadlocks detected")

### Business-Level Deadlock / Circular Waiting
(threads stuck in circular waiting patterns — e.g., task A waiting for barrier from B, B blocked on buffer from A)

### Backpressure / Buffer Blocking
(threads stuck in buffer request/allocation, network data exchange)

### Stuck Threads Summary
(threads consistently in BLOCKED/WAITING state across multiple captures, with stack traces)

## Flink REST API Analysis

### Job Status
(job list and states)

### Checkpoint Status
(which checkpoint is in progress, how long it's been pending, per-subtask acknowledgement status)

### Backpressure Status
(per-vertex backpressure levels: OK / LOW / HIGH)

### Bottleneck Correlation
(cross-reference: subtasks that haven't acknowledged checkpoint + vertices with high backpressure)

## Diagnosis
(synthesized root cause hypothesis — typical patterns:
 - Checkpoint barrier stuck behind backpressured channel
 - Unaligned checkpoint buffer tracking issue
 - Circular dependency between checkpoint barrier propagation and data flow
 - Operator state snapshot blocking
 - etc.)

## Recommended Next Steps
(suggestions for further investigation)
```
