#!/bin/bash
# Wait until N checkpoints have completed since baseline.
# Usage: ./wait_checkpoints.sh <rest_url> <job_id> <needed> [poll_interval_seconds]
#   Prints current completed count when done.
set -e

REST_URL="${1:?Usage: $0 <rest_url> <job_id> <needed> [poll_interval]}"
JOB_ID="${2:?Usage: $0 <rest_url> <job_id> <needed> [poll_interval]}"
NEEDED="${3:?Usage: $0 <rest_url> <job_id> <needed> [poll_interval]}"
POLL="${4:-5}"

BASELINE=$(curl -sf "${REST_URL}/jobs/${JOB_ID}/checkpoints" 2>/dev/null \
    | python3 -c "import sys,json; print(json.load(sys.stdin).get('counts',{}).get('completed',0))" 2>/dev/null || echo 0)

echo "Baseline completed: ${BASELINE}, waiting for +${NEEDED}..."

while true; do
    CUR=$(curl -sf "${REST_URL}/jobs/${JOB_ID}/checkpoints" 2>/dev/null \
        | python3 -c "import sys,json; print(json.load(sys.stdin).get('counts',{}).get('completed',0))" 2>/dev/null || echo 0)
    DIFF=$((CUR - BASELINE))
    if [ "$DIFF" -ge "$NEEDED" ]; then
        echo "Done: +${DIFF} checkpoints (total=${CUR})"
        exit 0
    fi
    sleep "$POLL"
done
