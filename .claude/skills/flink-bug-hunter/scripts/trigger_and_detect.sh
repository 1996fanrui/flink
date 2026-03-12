#!/bin/bash
# Trigger rescale rounds and detect stuck checkpoints. Stops IMMEDIATELY when issue detected.
# Usage: ./trigger_and_detect.sh <rest_url> <job_id> <classpath> <max_rounds> <stuck_threshold_sec> <checkpoints_between_rounds>
#
# Exits with code 0 when issue detected (prints DETECTED + details).
# Exits with code 1 when max rounds exhausted without detection.
# Exits with code 2 on error.
set -e

REST_URL="${1:?Usage: $0 <rest_url> <job_id> <classpath> <max_rounds> <stuck_threshold_sec> <checkpoints_between_rounds>}"
JOB_ID="${2:?}"
CLASSPATH="${3:?}"
MAX_ROUNDS="${4:-3}"
STUCK_THRESHOLD="${5:-30}"
CHECKPOINTS_BETWEEN="${6:-3}"
POLL_INTERVAL=5

get_completed() {
    curl -sf "${REST_URL}/jobs/${JOB_ID}/checkpoints" 2>/dev/null \
        | python3 -c "import sys,json; print(json.load(sys.stdin).get('counts',{}).get('completed',0))" 2>/dev/null || echo 0
}

check_stuck() {
    curl -sf "${REST_URL}/jobs/${JOB_ID}/checkpoints" 2>/dev/null \
        | python3 -c "
import sys, json, time
data = json.load(sys.stdin)
latest = data.get('latest', {}).get('in_progress')
if latest and latest.get('status') == 'IN_PROGRESS':
    trigger_ts = latest.get('trigger_timestamp', 0) / 1000
    elapsed = time.time() - trigger_ts
    if elapsed > ${STUCK_THRESHOLD}:
        print(f'STUCK checkpoint {latest.get(\"id\")}: {elapsed:.0f}s elapsed')
        sys.exit(0)
print('OK')
" 2>/dev/null || echo "UNKNOWN"
}

check_slow_checkpoint() {
    # Check if any recently completed checkpoint had duration > stuck_threshold * 1000
    curl -sf "${REST_URL}/jobs/${JOB_ID}/checkpoints" 2>/dev/null \
        | python3 -c "
import sys, json
data = json.load(sys.stdin)
history = data.get('history', [])
threshold_ms = ${STUCK_THRESHOLD} * 1000
for cp in history[-5:]:
    if cp.get('status') == 'COMPLETED':
        duration = cp.get('end_to_end_duration', 0)
        if duration > threshold_ms:
            print(f'SLOW checkpoint {cp.get(\"id\")}: {duration}ms')
            sys.exit(0)
print('OK')
" 2>/dev/null || echo "UNKNOWN"
}

wait_running() {
    for i in $(seq 1 60); do
        STATE=$(curl -sf "${REST_URL}/jobs/${JOB_ID}" 2>/dev/null \
            | python3 -c "import sys,json; print(json.load(sys.stdin).get('state',''))" 2>/dev/null || echo "")
        [ "$STATE" = "RUNNING" ] && return 0
        sleep 2
    done
    echo "ERROR: Job not RUNNING after 120s"
    exit 2
}

for ROUND in $(seq 1 "$MAX_ROUNDS"); do
    echo "--- Round $ROUND/$MAX_ROUNDS ---"

    # Trigger rescale
    java -cp "$CLASSPATH" org.apache.flink.streaming.examples.FlinkRestClientDemo \
        "$REST_URL" "$JOB_ID" "$((ROUND - 1))" 2>&1 | sed 's/^/  /'

    echo "  Waiting for RUNNING..."
    sleep 3
    wait_running

    # Poll for issue detection while waiting for checkpoints
    BASELINE=$(get_completed)
    echo "  Baseline=$BASELINE, polling for issue or +${CHECKPOINTS_BETWEEN} checkpoints..."

    while true; do
        # Check for stuck checkpoint (in-progress too long)
        STATUS=$(check_stuck)
        if [[ "$STATUS" == STUCK* ]]; then
            echo ""
            echo "DETECTED at round $ROUND: $STATUS"
            echo "TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')"
            exit 0
        fi

        # Check for slow completed checkpoint
        STATUS=$(check_slow_checkpoint)
        if [[ "$STATUS" == SLOW* ]]; then
            echo ""
            echo "DETECTED at round $ROUND: $STATUS"
            echo "TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')"
            exit 0
        fi

        # Check if enough checkpoints passed to move to next round
        CUR=$(get_completed)
        DIFF=$((CUR - BASELINE))
        if [ "$DIFF" -ge "$CHECKPOINTS_BETWEEN" ]; then
            echo "  +${DIFF} checkpoints, no issue detected. Next round."
            break
        fi

        sleep "$POLL_INTERVAL"
    done
done

echo "No issue detected after $MAX_ROUNDS rounds."
exit 1
