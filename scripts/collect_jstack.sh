#!/bin/bash
# Periodically collect jstack dumps of the Flink demo JVM process.
# Usage: ./scripts/collect_jstack.sh [interval_seconds] [output_dir]
#   interval_seconds: default 5
#   output_dir: default log/jstack_<timestamp>
set -e

INTERVAL="${1:-5}"
OUTPUT_DIR="${2:-$(dirname "$0")/../log/jstack_$(date +%Y%m%d_%H%M%S)}"
mkdir -p "$OUTPUT_DIR"

echo "Collecting jstack every ${INTERVAL}s into ${OUTPUT_DIR}"
echo "Press Ctrl+C to stop."

while true; do
    PID=$(jps -l 2>/dev/null | grep 'UnalignedCheckpointDemo' | awk '{print $1}')
    if [ -z "$PID" ]; then
        echo "$(date '+%Y-%m-%d %H:%M:%S') - UnalignedCheckpointDemo not running, waiting..."
        sleep "$INTERVAL"
        continue
    fi

    TIMESTAMP=$(date +%Y%m%d_%H%M%S)
    OUTFILE="${OUTPUT_DIR}/jstack_${TIMESTAMP}.txt"
    jstack "$PID" > "$OUTFILE" 2>&1 || echo "$(date '+%Y-%m-%d %H:%M:%S') - jstack failed"
    sleep "$INTERVAL"
done
