#!/bin/bash
# Usage: bash investigate_loop.sh
#
# Required env vars (set by caller):
#   TEST_CLASSES    - Comma-separated fully qualified test class names
#   EXPECTED_TIME   - Max expected single run time in seconds
#   MAX_ITERATIONS  - Max loop iterations
#   LOG_DIR         - Directory for logs and output files
#   WORKTREE_PATH   - Path to the git worktree
#   REST_PORT       - Flink REST API port
#
# Output files:
#   {LOG_DIR}/stuck_info.txt  - Stuck detection result (always created)
#   {LOG_DIR}/summary.log     - Per-iteration results (always created)
#   {LOG_DIR}/iteration_N.log - Test output for iteration N
#   {LOG_DIR}/iterN_jstack_M.log - jstack captures (only if stuck)

set -e

mkdir -p "$LOG_DIR"

for ((i=1; i<=MAX_ITERATIONS; i++)); do
    echo "=== Iteration $i / $MAX_ITERATIONS ==="
    ITER_LOG="$LOG_DIR/iteration_${i}.log"
    START_TIME=$(date +%s)

    # Run test in background (do NOT disown here — we need `wait` for exit code)
    cd "$WORKTREE_PATH/flink-tests"
    ../mvnw \
        -Dtest="$TEST_CLASSES" test \
        -Dflink.XmxUnitTest=6096m -Dflink.forkCountUnitTest=1 \
        -Dmaven.javadoc.skip=true -Drat.skip=true -Dcheckstyle.skip=true -Denforcer.skip=true \
        -P java11,java11-target --no-snapshot-updates \
        > "$ITER_LOG" 2>&1 &
    TEST_PID=$!

    # Monitor: wait until test finishes or exceeds EXPECTED_TIME
    while kill -0 $TEST_PID 2>/dev/null; do
        sleep 30
        ELAPSED=$(( $(date +%s) - START_TIME ))

        if [ $ELAPSED -ge $EXPECTED_TIME ]; then
            echo "STUCK DETECTED at iteration $i after ${ELAPSED}s"

            # Capture 10 jstack snapshots 10 seconds apart
            # Use jps to find the actual Java process (not the /bin/sh wrapper)
            JAVA_PID=$(jps -l 2>/dev/null | grep "surefirebooter" | awk '{print $1}' | head -1 || true)
            if [ -n "$JAVA_PID" ]; then
                for j in $(seq 1 10); do
                    jstack -l "$JAVA_PID" > "$LOG_DIR/iter${i}_jstack_${j}.log" 2>&1 || true
                    [ $j -lt 10 ] && sleep 10
                done
            fi

            # macOS notification
            osascript -e 'display notification "Test stuck at iteration '"$i"' after '"$ELAPSED"'s. REST API: http://localhost:'"$REST_PORT"'" with title "Checkpoint Investigator" sound name "Submarine"' 2>/dev/null || true
            say "Checkpoint investigation needed. Test stuck at iteration $i." 2>/dev/null || true

            # Record stuck info and test PID
            echo "stuck_iteration=$i" > "$LOG_DIR/stuck_info.txt"
            echo "elapsed=${ELAPSED}" >> "$LOG_DIR/stuck_info.txt"
            echo "test_pid=$TEST_PID" >> "$LOG_DIR/stuck_info.txt"
            echo "java_pid=$JAVA_PID" >> "$LOG_DIR/stuck_info.txt"
            echo "Iteration $i: STUCK after ${ELAPSED}s (test still running, pid=$TEST_PID)" >> "$LOG_DIR/summary.log"

            # Disown ONLY here — so the test process survives script exit
            # (Phase 5 needs MiniCluster alive for REST API queries)
            disown $TEST_PID
            echo "RESULT: Stuck detected. Test process left alive (pid=$TEST_PID) for investigation."
            exit 0
        fi
    done

    # Test finished within EXPECTED_TIME — `wait` works because we did NOT disown
    wait $TEST_PID 2>/dev/null
    EXIT_CODE=$?
    ELAPSED=$(( $(date +%s) - START_TIME ))
    echo "Iteration $i completed: exit_code=$EXIT_CODE elapsed=${ELAPSED}s" >> "$LOG_DIR/summary.log"

    if [ $EXIT_CODE -ne 0 ]; then
        echo "Iteration $i FAILED (exit_code=$EXIT_CODE) in ${ELAPSED}s (not stuck). Continuing."
    else
        echo "Iteration $i PASSED in ${ELAPSED}s."
    fi
done

# All iterations completed without stuck
echo "RESULT: All $MAX_ITERATIONS iterations completed without getting stuck."
echo "no_stuck=true" > "$LOG_DIR/stuck_info.txt"
echo "total_iterations=$MAX_ITERATIONS" >> "$LOG_DIR/stuck_info.txt"
