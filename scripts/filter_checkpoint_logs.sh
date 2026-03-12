#!/bin/bash
# Filter checkpoint-related log lines from Flink demo log file.
# Usage: ./scripts/filter_checkpoint_logs.sh <log_file> [output_file]
#   If output_file is omitted, prints to stdout.
set -e

LOG_FILE="${1:?Usage: $0 <log_file> [output_file]}"
OUTPUT_FILE="${2:-}"

PATTERNS="checkpoint|CheckpointCoordinator|CheckpointBarrier|triggerCheckpoint|completedCheckpoint|abortCheckpoint|Checkpoint.*completed|Checkpoint.*triggered|Checkpoint.*expired|Checkpoint.*failed|priority.event|PendingCheckpoint|notifyCheckpointComplete|notifyCheckpointAborted|LocalInputChannel.*Processing priority|LocalInputChannel.*Received priority"

if [ -n "$OUTPUT_FILE" ]; then
    grep -iE "$PATTERNS" "$LOG_FILE" > "$OUTPUT_FILE"
    echo "Filtered $(wc -l < "$OUTPUT_FILE") lines -> $OUTPUT_FILE"
else
    grep -iE "$PATTERNS" "$LOG_FILE"
fi
