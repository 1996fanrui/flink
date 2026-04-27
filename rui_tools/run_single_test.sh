#!/bin/bash
# Usage:
#   ./rui_tools/run_single_test.sh                       # default UnalignedCheckpointRescaleITCase
#   ./rui_tools/run_single_test.sh OtherITCaseName       # custom test class
#
# Env:
#   JSTACK_INTERVAL_SEC (default 10) — jstack period
#   HEAP_DUMP_AT_SEC    (default 95) — when to take heap dump (best-effort)
#
# 流程：
#   1) 单独编译 flink-runtime 模块（约 30s，跳过 javadoc/rat/checkstyle/enforcer/spotless 与 test）
#   2) 在 flink-tests 里跑指定的单个 ITCase
#   3) stdout/stderr 同时回显到屏幕并写入 log/<TS>/<TS>.log
#   4) 后台 watcher：检测到 surefire 子 JVM 后，每 JSTACK_INTERVAL_SEC 抓一次 jstack；
#      在 HEAP_DUMP_AT_SEC 时做一次 heap dump（best-effort）。
#      产物：log/<TS>/<TS>_NN.jstack（多份），log/<TS>/<TS>.hprof，log/<TS>/<TS>.log
set -o pipefail

cd "$(dirname "$0")/.."

TEST_CLASS="${1:-UnalignedCheckpointRescaleITCase}"
JSTACK_INTERVAL_SEC="${JSTACK_INTERVAL_SEC:-10}"
HEAP_DUMP_AT_SEC="${HEAP_DUMP_AT_SEC:-95}"

mkdir -p log
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
RUN_DIR="$(pwd)/log/${TIMESTAMP}"
mkdir -p "$RUN_DIR"
LOG_FILE="${RUN_DIR}/${TIMESTAMP}.log"
HEAP_FILE="${RUN_DIR}/${TIMESTAMP}.hprof"
JSTACK_PREFIX="${RUN_DIR}/${TIMESTAMP}"

# Mirror everything to both terminal and log file from this point on.
exec > >(tee "$LOG_FILE") 2>&1

echo "=== Run dir:        $RUN_DIR ==="
echo "=== Log file:       $LOG_FILE ==="
echo "=== Heap dump file: $HEAP_FILE (at ${HEAP_DUMP_AT_SEC}s, best-effort) ==="
echo "=== jstack prefix:  ${JSTACK_PREFIX}_NN.jstack (every ${JSTACK_INTERVAL_SEC}s) ==="
echo "=== Test class:     $TEST_CLASS ==="

echo "=== [1/2] Build flink-runtime (skip tests) ==="
./mvnw install -pl flink-runtime -DskipTests -Pfast \
  -Dmaven.javadoc.skip=true -Drat.skip=true -Dcheckstyle.skip=true -Denforcer.skip=true \
  -Dspotless.check.skip=true \
  -P java11-target -P java11

echo "=== [2/2] Run $TEST_CLASS ==="

# Background watcher: wait for the surefire fork JVM, then take a periodic jstack
# every JSTACK_INTERVAL_SEC seconds; once HEAP_DUMP_AT_SEC has elapsed since fork
# detection, also take a single heap dump (best-effort). Stops automatically when
# the fork JVM exits.
(
  set +e
  PID=""
  for i in $(seq 1 180); do
    PID=$(jps -l 2>/dev/null | awk '/surefire\.booter\.ForkedBooter|surefirebooter/ {print $1; exit}')
    if [ -n "$PID" ]; then
      echo "[watcher] forked test JVM PID=$PID (waited ${i}s)"
      break
    fi
    sleep 1
  done
  if [ -z "$PID" ]; then
    echo "[watcher] failed to find ForkedBooter within 180s; giving up"
    exit 0
  fi
  START_EPOCH=$(date +%s)
  HEAP_DONE=0
  STACK_IDX=0
  while kill -0 "$PID" 2>/dev/null; do
    NOW=$(date +%s)
    ELAPSED=$((NOW - START_EPOCH))
    STACK_IDX=$((STACK_IDX + 1))
    NN=$(printf "%02d" "$STACK_IDX")
    JSF="${JSTACK_PREFIX}_${NN}.jstack"
    {
      echo "# ts=$(date +%Y-%m-%dT%H:%M:%S)  elapsed_since_fork=${ELAPSED}s  pid=${PID}"
      jcmd "$PID" Thread.print 2>&1
    } > "$JSF" 2>&1 || echo "[watcher] jstack ${NN} failed"
    echo "[watcher] +${ELAPSED}s  jstack#${NN} -> $(basename "$JSF")"

    if [ "$HEAP_DONE" -eq 0 ] && [ "$ELAPSED" -ge "$HEAP_DUMP_AT_SEC" ]; then
      rm -f "$HEAP_FILE"
      echo "[watcher] capturing heap dump (elapsed=${ELAPSED}s)..."
      jcmd "$PID" GC.heap_dump "$HEAP_FILE" 2>&1 \
        && echo "[watcher] heap dump done: $HEAP_FILE ($(du -h "$HEAP_FILE" 2>/dev/null | cut -f1))" \
        || echo "[watcher] heap dump failed (JVM may have exited)"
      HEAP_DONE=1
    fi
    sleep "$JSTACK_INTERVAL_SEC"
  done
  echo "[watcher] fork JVM exited; collected $STACK_IDX jstack snapshot(s) in $RUN_DIR"
) &
WATCHER_PID=$!

set +e
./mvnw test -pl flink-tests \
  -Dtest="$TEST_CLASS" \
  -Dflink.XmxUnitTest=6096m \
  -Dflink.forkCountUnitTest=1 \
  -Dmaven.javadoc.skip=true -Drat.skip=true -Dcheckstyle.skip=true -Denforcer.skip=true \
  -Dspotless.check.skip=true \
  -P java11,java11-target --no-snapshot-updates
TEST_EXIT=$?
set -e

wait "$WATCHER_PID" 2>/dev/null || true

echo "=== Done. Log: $LOG_FILE  (mvn exit=$TEST_EXIT) ==="
echo "=== Artifacts: $RUN_DIR ==="
exit "$TEST_EXIT"
