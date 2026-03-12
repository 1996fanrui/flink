#!/bin/bash
# Monitor checkpoint status via REST API. Reports stuck checkpoints.
# Usage: ./monitor_checkpoint.sh <rest_url> <job_id> <stuck_threshold_seconds>
#   rest_url: e.g. http://localhost:12345
#   job_id: Flink job ID
#   stuck_threshold_seconds: default 60
set -e

REST_URL="${1:?Usage: $0 <rest_url> <job_id> [stuck_threshold_seconds]}"
JOB_ID="${2:?Usage: $0 <rest_url> <job_id> [stuck_threshold_seconds]}"
STUCK_THRESHOLD="${3:-60}"

DATA=$(curl -sf "${REST_URL}/jobs/${JOB_ID}/checkpoints" 2>/dev/null)
if [ -z "$DATA" ]; then
    echo "ERROR: Cannot reach ${REST_URL}/jobs/${JOB_ID}/checkpoints"
    exit 1
fi

python3 -c "
import json, time, sys

data = json.loads('''${DATA}''')
counts = data.get('counts', {})
print(f'completed={counts.get(\"completed\",0)} failed={counts.get(\"failed\",0)} in_progress={counts.get(\"in_progress\",0)} restored={counts.get(\"restored\",0)}')

latest = data.get('latest', {})
ip = latest.get('in_progress')
if ip and ip.get('status') == 'IN_PROGRESS':
    trigger_ts = ip.get('trigger_timestamp', 0) / 1000
    elapsed = time.time() - trigger_ts
    if elapsed > ${STUCK_THRESHOLD}:
        print(f'STUCK: checkpoint {ip[\"id\"]} in progress for {elapsed:.0f}s (threshold={${STUCK_THRESHOLD}}s)')
        sys.exit(2)
    else:
        print(f'IN_PROGRESS: checkpoint {ip[\"id\"]} for {elapsed:.0f}s (threshold={${STUCK_THRESHOLD}}s)')
else:
    print('OK: no in-progress checkpoint')
"
