# Phase 7: Cleanup Details

## Stuck Scenario (requires human confirmation)

Use `AskUserQuestion` with options:
- "Clean up now" — proceed with cleanup below
- "Keep running" — end skill, output PID + worktree path for manual cleanup

### Cleanup Steps

1. Kill the test process:
   ```bash
   kill {test_pid} 2>/dev/null || true
   sleep 3
   kill -9 {test_pid} 2>/dev/null || true
   pkill -f "surefire" 2>/dev/null || true
   sleep 2
   pkill -9 -f "surefire" 2>/dev/null || true
   ```
2. Remove the worktree:
   ```bash
   cd {PROJECT_ROOT} && git worktree remove --force {WORKTREE_PATH}
   ```
3. Clean up the temporary branch:
   ```bash
   git branch -D investigate-{TIMESTAMP} 2>/dev/null || true
   ```

## No-Stuck Scenario (automatic)

No live process to preserve. Proceed directly with worktree removal and branch cleanup (steps 2-3 above).
