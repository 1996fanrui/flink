#!/bin/bash
# Orchestrate the checkpoint-during-recovery reproduction.
# Usage: ./scripts/run_repro.sh
#
# This script:
# 1. Starts UnalignedCheckpointDemo (background, logs to file)
# 2. Starts periodic jstack collection (background)
# 3. Waits for the job to be RUNNING
# 4. Loops: trigger rescale -> wait for 3 completed checkpoints -> repeat
# 5. Monitors for stuck checkpoints and reports
set -e

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_FILE="${PROJECT_DIR}/log/flink_demo_${TIMESTAMP}.log"
JSTACK_DIR="${PROJECT_DIR}/log/jstack_${TIMESTAMP}"
CHECKPOINT_LOG="${PROJECT_DIR}/log/checkpoint_filtered_${TIMESTAMP}.log"
REST_URL="http://localhost:12345"
REQUIRED_CHECKPOINTS=3
STUCK_THRESHOLD_SECONDS=60

mkdir -p "$JSTACK_DIR"

echo "=== Checkpoint During Recovery Reproduction ==="
echo "Log file: $LOG_FILE"
echo "Jstack dir: $JSTACK_DIR"
echo ""

# --- Step 1: Build classpath and start demo ---
echo "[1/4] Building classpath..."
CP=$(cd "$PROJECT_DIR" && ./mvnw -pl flink-examples/flink-examples-streaming \
    dependency:build-classpath -q -DincludeScope=runtime \
    -Dmdep.outputFile=/dev/stdout 2>/dev/null)
CP="${PROJECT_DIR}/flink-examples/flink-examples-streaming/target/classes:${CP}"

echo "[2/4] Starting UnalignedCheckpointDemo..."
java -cp "$CP" org.apache.flink.streaming.examples.UnalignedCheckpointDemo \
    > "$LOG_FILE" 2>&1 &
DEMO_PID=$!
echo "  PID: $DEMO_PID"

# --- Step 2: Start jstack collection ---
echo "[3/4] Starting jstack collection (every 5s)..."
"${PROJECT_DIR}/scripts/collect_jstack.sh" 5 "$JSTACK_DIR" &
JSTACK_PID=$!

cleanup() {
    echo ""
    echo "=== Cleaning up ==="
    kill "$JSTACK_PID" 2>/dev/null || true
    # Do NOT kill DEMO_PID — leave the Flink job running for manual inspection
    echo "jstack collector stopped. Flink demo still running (PID=$DEMO_PID)."
    echo "To stop: kill $DEMO_PID"
}
trap cleanup EXIT

# --- Step 3: Wait for job to be RUNNING ---
echo "[4/4] Waiting for job to start..."
JOB_ID=""
for i in $(seq 1 60); do
    sleep 2
    JOB_ID=$(curl -sf "${REST_URL}/jobs/overview" 2>/dev/null \
        | python3 -c "
import sys, json
data = json.load(sys.stdin)
for j in data.get('jobs', []):
    if j['state'] == 'RUNNING':
        print(j['jid'])
        break
" 2>/dev/null || true)
    if [ -n "$JOB_ID" ]; then
        break
    fi
done

if [ -z "$JOB_ID" ]; then
    echo "ERROR: No running job found after 120s. Check $LOG_FILE"
    exit 1
fi
echo "  Job ID: $JOB_ID"

# --- Helper: get completed checkpoint count ---
get_completed_count() {
    curl -sf "${REST_URL}/jobs/${JOB_ID}/checkpoints" 2>/dev/null \
        | python3 -c "
import sys, json
data = json.load(sys.stdin)
counts = data.get('counts', {})
print(counts.get('completed', 0))
" 2>/dev/null || echo "0"
}

# --- Helper: check if a checkpoint is stuck ---
check_stuck() {
    curl -sf "${REST_URL}/jobs/${JOB_ID}/checkpoints" 2>/dev/null \
        | python3 -c "
import sys, json, time
data = json.load(sys.stdin)
latest = data.get('latest', {}).get('in_progress')
if latest and latest.get('status') == 'IN_PROGRESS':
    trigger_ts = latest.get('trigger_timestamp', 0) / 1000
    elapsed = time.time() - trigger_ts
    if elapsed > ${STUCK_THRESHOLD_SECONDS}:
        print(f'STUCK checkpoint {latest.get(\"id\")}: {elapsed:.0f}s elapsed')
        sys.exit(0)
print('OK')
" 2>/dev/null || echo "UNKNOWN"
}

# --- Step 4: Rescale loop ---
echo ""
echo "=== Starting rescale loop ==="
echo "  Required checkpoints between rescales: $REQUIRED_CHECKPOINTS"
echo "  Stuck threshold: ${STUCK_THRESHOLD_SECONDS}s"
echo ""

# Wait for initial checkpoints to stabilize
echo "Waiting for first $REQUIRED_CHECKPOINTS checkpoints..."
BASELINE=$(get_completed_count)
while true; do
    CURRENT=$(get_completed_count)
    DIFF=$((CURRENT - BASELINE))
    if [ "$DIFF" -ge "$REQUIRED_CHECKPOINTS" ]; then
        echo "  $DIFF checkpoints completed. Ready to rescale."
        break
    fi
    # Check for stuck
    STATUS=$(check_stuck)
    if [[ "$STATUS" == STUCK* ]]; then
        echo "  WARNING: $STATUS (before first rescale!)"
    fi
    sleep 5
done

ROUND=0
while true; do
    ROUND=$((ROUND + 1))
    echo ""
    echo "--- Rescale round $ROUND (robin_index=$((ROUND - 1))) ---"

    # Trigger rescale
    java -cp "$CP" org.apache.flink.streaming.examples.FlinkRestClientDemo \
        "$REST_URL" "$JOB_ID" "$((ROUND - 1))" 2>&1 \
        | sed 's/^/  /'

    echo "  Waiting for job to be RUNNING after rescale..."
    sleep 5
    for i in $(seq 1 60); do
        STATE=$(curl -sf "${REST_URL}/jobs/${JOB_ID}" 2>/dev/null \
            | python3 -c "import sys,json; print(json.load(sys.stdin).get('state',''))" 2>/dev/null || echo "")
        if [ "$STATE" = "RUNNING" ]; then
            break
        fi
        sleep 2
    done

    # Wait for REQUIRED_CHECKPOINTS completed checkpoints
    BASELINE=$(get_completed_count)
    echo "  Baseline completed count: $BASELINE. Waiting for +${REQUIRED_CHECKPOINTS}..."

    WAIT_START=$(date +%s)
    while true; do
        CURRENT=$(get_completed_count)
        DIFF=$((CURRENT - BASELINE))

        # Check for stuck
        STATUS=$(check_stuck)
        if [[ "$STATUS" == STUCK* ]]; then
            ELAPSED=$(( $(date +%s) - WAIT_START ))
            echo "  *** REPRODUCTION FOUND at round $ROUND! ***"
            echo "  $STATUS"
            echo "  Total wait: ${ELAPSED}s"
            echo "  Timestamp: $(date '+%Y-%m-%d %H:%M:%S')"
            echo "  Check jstack dumps in: $JSTACK_DIR"
            echo ""
            echo "  Filtering checkpoint logs..."
            "${PROJECT_DIR}/scripts/filter_checkpoint_logs.sh" "$LOG_FILE" "$CHECKPOINT_LOG" || true
            echo "  Checkpoint log: $CHECKPOINT_LOG"
            echo ""
            echo "  Issue reproduced. Exiting rescale loop (Flink job still running)."
            exit 0
        fi

        if [ "$DIFF" -ge "$REQUIRED_CHECKPOINTS" ]; then
            echo "  $DIFF checkpoints completed. Ready for next rescale."
            break
        fi
        sleep 5
    done
done
