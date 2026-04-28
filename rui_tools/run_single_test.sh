#!/bin/bash
# Usage:
#   ./rui_tools/run_single_test.sh                       # default UnalignedCheckpointRescaleITCase
#   ./rui_tools/run_single_test.sh OtherITCaseName       # custom test class
#
# Env:
#   JSTACK_INTERVAL_SEC (default 10)    — jstack period
#   HEAP_DUMP_AT_SEC    (default 30,60,90) — comma-separated seconds at which to
#                                          take heap dumps, measured from the
#                                          moment the fork JVM is detected
#                                          (≈ test start). Each dump is written
#                                          to its own file.
#
# 流程：
#   1) 单独编译 flink-runtime 模块（约 30s，跳过 javadoc/rat/checkstyle/enforcer/spotless 与 test）
#   2) 在 flink-tests 里跑指定的单个 ITCase
#   3) stdout/stderr 同时回显到屏幕并写入 log/<TS>/<TS>.log
#   4) 后台 watcher：检测到 surefire 子 JVM 后，每 JSTACK_INTERVAL_SEC 抓一次 jstack；
#      在 HEAP_DUMP_AT_SEC 列出的每个时间点各做一次 heap dump（best-effort）。
#      产物：log/<TS>/<TS>_NN.jstack（多份）、log/<TS>/<TS>_at<sec>s.hprof（多份）、log/<TS>/<TS>.log
set -o pipefail

cd "$(dirname "$0")/.."

TEST_CLASS="${1:-UnalignedCheckpointRescaleITCase}"
JSTACK_INTERVAL_SEC="${JSTACK_INTERVAL_SEC:-10}"
HEAP_DUMP_AT_SEC="${HEAP_DUMP_AT_SEC:-30,60,90}"

mkdir -p log
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
RUN_DIR="$(pwd)/log/${TIMESTAMP}"
mkdir -p "$RUN_DIR"
LOG_FILE="${RUN_DIR}/${TIMESTAMP}.log"
HEAP_PREFIX="${RUN_DIR}/${TIMESTAMP}"
JSTACK_PREFIX="${RUN_DIR}/${TIMESTAMP}"

# Mirror everything to both terminal and log file from this point on.
exec > >(tee "$LOG_FILE") 2>&1

echo "=== Run dir:        $RUN_DIR ==="
echo "=== Log file:       $LOG_FILE ==="
echo "=== Heap dumps:     ${HEAP_PREFIX}_at<sec>s.hprof (at seconds: ${HEAP_DUMP_AT_SEC}, best-effort) ==="
echo "=== jstack prefix:  ${JSTACK_PREFIX}_NN.jstack (every ${JSTACK_INTERVAL_SEC}s) ==="
echo "=== Test class:     $TEST_CLASS ==="

echo "=== [1/2] Build flink-runtime (skip tests) ==="
./mvnw install -pl flink-runtime -DskipTests -Pfast \
  -Dmaven.javadoc.skip=true -Drat.skip=true -Dcheckstyle.skip=true -Denforcer.skip=true \
  -Dspotless.check.skip=true \
  -P java11-target -P java11

echo "=== [2/2] Run $TEST_CLASS ==="

# Snapshot pre-existing surefire ForkedBooter PIDs so the watcher won't latch
# onto orphans from previous runs.
SNAPSHOT_PIDS=$(jps -l 2>/dev/null | awk '/surefire\.booter\.ForkedBooter|surefirebooter/ {print $1}' | sort -u | tr '\n' ' ')
echo "=== Pre-existing ForkedBooter PIDs (ignored): ${SNAPSHOT_PIDS:-<none>} ==="

set +e
./mvnw test -pl flink-tests \
  -Dtest="$TEST_CLASS" \
  -Dflink.XmxUnitTest=6096m \
  -Dflink.forkCountUnitTest=1 \
  -Dmaven.javadoc.skip=true -Drat.skip=true -Dcheckstyle.skip=true -Denforcer.skip=true \
  -Dspotless.check.skip=true \
  -P java11,java11-target --no-snapshot-updates &
MVN_PID=$!

# Background watcher: find a ForkedBooter that (a) is a descendant of MVN_PID,
# and (b) is NOT in the pre-existing snapshot. Then sample jstack every
# JSTACK_INTERVAL_SEC seconds, and take a single heap dump at HEAP_DUMP_AT_SEC.
(
  set +e

  is_in_snapshot() {
    local pid=$1
    for ex in $SNAPSHOT_PIDS; do
      [ "$pid" = "$ex" ] && return 0
    done
    return 1
  }

  is_descendant_of_mvn() {
    local cur=$1 ppid
    for _ in $(seq 1 30); do
      ppid=$(ps -o ppid= -p "$cur" 2>/dev/null | tr -d ' ')
      if [ -z "$ppid" ] || [ "$ppid" = "0" ] || [ "$ppid" = "1" ]; then
        return 1
      fi
      if [ "$ppid" = "$MVN_PID" ]; then
        return 0
      fi
      cur=$ppid
    done
    return 1
  }

  PID=""
  for i in $(seq 1 180); do
    for cand in $(jps -l 2>/dev/null | awk '/surefire\.booter\.ForkedBooter|surefirebooter/ {print $1}'); do
      if ! is_in_snapshot "$cand" && is_descendant_of_mvn "$cand"; then
        PID=$cand
        break
      fi
    done
    if [ -n "$PID" ]; then
      echo "[watcher] forked test JVM PID=$PID (waited ${i}s, mvn=$MVN_PID)"
      break
    fi
    # Bail out early if mvn already died — there will be no fork.
    if ! kill -0 "$MVN_PID" 2>/dev/null; then
      echo "[watcher] mvn ($MVN_PID) exited before fork was detected; giving up"
      exit 0
    fi
    sleep 1
  done
  if [ -z "$PID" ]; then
    echo "[watcher] failed to find ForkedBooter within 180s; giving up"
    exit 0
  fi
  START_EPOCH=$(date +%s)
  # Parse comma-separated heap-dump trigger times into a sorted ascending array.
  HEAP_TIMES_SORTED=$(echo "$HEAP_DUMP_AT_SEC" | tr ',' '\n' | sed '/^[[:space:]]*$/d' | sort -n)
  HEAP_PENDING=$(echo "$HEAP_TIMES_SORTED" | tr '\n' ' ')
  echo "[watcher] heap dump schedule: ${HEAP_PENDING:-<none>}"
  STACK_IDX=0
  HEAP_IDX=0
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

    # Fire every heap-dump trigger whose time has been reached. Multiple
    # triggers can fire in the same iteration if the previous sleep overran.
    NEW_PENDING=""
    for TARGET in $HEAP_PENDING; do
      if [ "$ELAPSED" -ge "$TARGET" ]; then
        HEAP_IDX=$((HEAP_IDX + 1))
        HF="${HEAP_PREFIX}_at${TARGET}s.hprof"
        rm -f "$HF"
        echo "[watcher] capturing heap dump #${HEAP_IDX} target=+${TARGET}s (elapsed=${ELAPSED}s) -> $(basename "$HF")"
        jcmd "$PID" GC.heap_dump "$HF" 2>&1 \
          && echo "[watcher] heap dump done: $HF ($(du -h "$HF" 2>/dev/null | cut -f1))" \
          || echo "[watcher] heap dump failed (JVM may have exited)"
      else
        NEW_PENDING="$NEW_PENDING $TARGET"
      fi
    done
    HEAP_PENDING="$NEW_PENDING"
    sleep "$JSTACK_INTERVAL_SEC"
  done
  echo "[watcher] fork JVM exited; collected $STACK_IDX jstack snapshot(s), $HEAP_IDX heap dump(s) in $RUN_DIR"
) &
WATCHER_PID=$!

wait "$MVN_PID"
TEST_EXIT=$?
set -e

wait "$WATCHER_PID" 2>/dev/null || true

echo "=== Done. Log: $LOG_FILE  (mvn exit=$TEST_EXIT) ==="
echo "=== Artifacts: $RUN_DIR ==="
exit "$TEST_EXIT"
