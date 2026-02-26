#!/usr/bin/env bash
################################################################################
# Enhanced test runner with loop and timeout support.
#
# Usage:
#   ./run_tests.sh -t "org.apache.flink.TestA,org.apache.flink.TestB" -n 100 -d ./log
#   ./run_tests.sh -t "org.apache.flink.TestA#method1" -n 5 -T 3600 -d ./log
#
# Options:
#   -t TEST_CLASSES   Comma-separated list of test classes (required)
#   -n ITERATIONS     Number of loop iterations (default: 100)
#   -T TIMEOUT        Timeout per iteration in seconds (default: 21600 = 6 hours)
#   -d LOG_DIR        Log output directory (default: ./log)
#   -m MODULE_DIR     Maven module directory (default: flink-tests)
################################################################################

set -e

# --- Default configuration ---
TEST_CLASSES=""
ITERATIONS=100
TIMEOUT=21600
LOG_DIR="./log"
MODULE_DIR="flink-tests"

# --- Parse command-line arguments ---
while getopts "t:n:T:d:m:" opt; do
  case "$opt" in
    t) TEST_CLASSES="$OPTARG" ;;
    n) ITERATIONS="$OPTARG" ;;
    T) TIMEOUT="$OPTARG" ;;
    d) LOG_DIR="$OPTARG" ;;
    m) MODULE_DIR="$OPTARG" ;;
    *)
      echo "Unknown option: -$OPTARG" >&2
      echo "Run with -h for usage information." >&2
      exit 1
      ;;
  esac
done

if [ -z "$TEST_CLASSES" ]; then
  echo "Error: -t TEST_CLASSES is required." >&2
  echo "Example: ./run_tests.sh -t \"org.apache.flink.TestA,org.apache.flink.TestB\"" >&2
  exit 1
fi

# --- Tracking variables ---
CHILD_PID=""
COMPLETED_COUNT=0
TIMED_OUT_COUNT=0

# --- Kill a process and all its children ---
kill_child_processes() {
  local pid=$1
  if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
    echo "Killing process $pid and its children..."
    pkill -TERM -P "$pid" 2>/dev/null || true
    kill -TERM "$pid" 2>/dev/null || true
    sleep 2
    if kill -0 "$pid" 2>/dev/null; then
      pkill -KILL -P "$pid" 2>/dev/null || true
      kill -KILL "$pid" 2>/dev/null || true
    fi
  fi
}

# --- Print summary and exit ---
print_summary() {
  local total_run=$((COMPLETED_COUNT + TIMED_OUT_COUNT))
  echo ""
  echo "========================================"
  echo "Test Run Summary"
  echo "========================================"
  echo "Total iterations requested: $ITERATIONS"
  echo "Total iterations executed:  $total_run"
  echo "Completed within timeout:   $COMPLETED_COUNT"
  echo "Timed out:                  $TIMED_OUT_COUNT"
  echo "Log directory:              $LOG_DIR"
  echo "========================================"
}

# --- Handle Ctrl+C gracefully ---
cleanup() {
  echo ""
  echo "Ctrl+C detected. Cleaning up..."
  kill_child_processes "$CHILD_PID"
  print_summary
  exit 1
}

trap cleanup SIGINT SIGTERM

# --- Ensure log directory exists ---
mkdir -p "$LOG_DIR"

echo "========================================"
echo "Test Runner Configuration"
echo "========================================"
echo "Test classes:  $TEST_CLASSES"
echo "Iterations:    $ITERATIONS"
echo "Timeout:       ${TIMEOUT}s per iteration"
echo "Log directory: $LOG_DIR"
echo "Module:        $MODULE_DIR"
echo "========================================"
echo ""

# --- Main loop ---
for i in $(seq 1 "$ITERATIONS"); do
  timestamp=$(date +"%Y%m%d_%H%M%S")
  log_file="${LOG_DIR}/${timestamp}.log"

  echo "[$timestamp] Starting iteration $i/$ITERATIONS"

  # Run Maven test in background from the module directory.
  # The mvnw wrapper is one level up from the module directory.
  (
    cd "$MODULE_DIR"
    ../mvnw -am \
      -Dtest="$TEST_CLASSES" \
      test \
      -Dflink.XmxUnitTest=6096m \
      -Dflink.forkCountUnitTest=1 \
      -Dmaven.javadoc.skip=true \
      -Drat.skip=true \
      -Dcheckstyle.skip=true \
      -Denforcer.skip=true \
      -P java11,java11-target \
      --no-snapshot-updates
  ) > "$log_file" 2>&1 &
  CHILD_PID=$!

  echo "  PID: $CHILD_PID | Log: $log_file | Timeout: ${TIMEOUT}s"

  # Poll until the process exits or the timeout is reached.
  elapsed=0
  while kill -0 "$CHILD_PID" 2>/dev/null; do
    sleep 5
    elapsed=$((elapsed + 5))
    if [ "$elapsed" -ge "$TIMEOUT" ]; then
      echo "  TIMEOUT after ${TIMEOUT}s. Killing process $CHILD_PID..."
      echo "TIMEOUT: Process exceeded ${TIMEOUT}s limit" >> "$log_file"
      kill_child_processes "$CHILD_PID"
      TIMED_OUT_COUNT=$((TIMED_OUT_COUNT + 1))
      break
    fi
  done

  wait "$CHILD_PID" 2>/dev/null || true
  CHILD_PID=""

  if [ "$elapsed" -lt "$TIMEOUT" ]; then
    COMPLETED_COUNT=$((COMPLETED_COUNT + 1))
  fi

  echo "  Iteration $i finished at $(date)"
  echo "----------------------------------------"
done

print_summary
