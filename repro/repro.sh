#!/usr/bin/env bash
# Portable reproducer for the UnalignedCheckpointRescaleITCase data loss (PR #28517).
# Run from the root of a flink checkout on the commit you want to test.
# PREREQUISITE: production modules (flink-core/flink-runtime/flink-streaming-java, etc.)
# must already be built and installed to the local ~/.m2 for the current code; this
# script only recompiles flink-tests test-classes (narrow.py rewrites the test source).
#   bash repro/repro.sh [workers] [target_runs] [per_run_timeout_s]
# Reproduces only under CPU contention; rate ~0.1-0.3%/run, so budget ~1000+ runs.
# A genuine failure = "Tests run: 1, Failures: 1" with [NUM_OUTPUTS = NUM_INPUTS] and "but was" < "expected".
set -u
# flink-tests requires Java 17 to compile; force it unless the caller's shell already runs Java 17.
# Portable across macOS (/usr/libexec/java_home) and Linux (JAVA_HOME_<v>_X64 / /usr/lib/jvm scan).
ensure_java() {
  local want="$1" home="" d envvar
  if java -version 2>&1 | grep -qE "version \"${want}[.\"]"; then
    echo "Java $want already active; keeping current JAVA_HOME."
    return 0
  fi
  if [ -x /usr/libexec/java_home ]; then                       # macOS
    home="$(/usr/libexec/java_home -v "$want" 2>/dev/null)"
    # java_home silently returns the nearest JDK when the exact version is absent.
    if [ -n "$home" ] && ! "$home/bin/java" -version 2>&1 | grep -qE "version \"${want}[.\"]"; then
      home=""
    fi
  fi
  if [ -z "$home" ]; then                                      # CI (GitHub Actions etc.)
    envvar="JAVA_HOME_${want}_X64"
    home="${!envvar:-}"
  fi
  if [ -z "$home" ]; then                                      # Linux: scan common JVM dirs
    for d in /usr/lib/jvm/*"$want"* /usr/java/*"$want"*; do
      if [ -x "$d/bin/java" ] && "$d/bin/java" -version 2>&1 | grep -qE "version \"${want}[.\"]"; then
        home="$d"; break
      fi
    done
  fi
  [ -n "$home" ] || { echo "JDK $want not found"; exit 1; }
  export JAVA_HOME="$home"
  export PATH="$JAVA_HOME/bin:$PATH"
  echo "Using JDK $want at $JAVA_HOME"
}
ensure_java 17
HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
TESTFILE="$ROOT/flink-tests/src/test/java/org/apache/flink/test/checkpointing/UnalignedCheckpointRescaleITCase.java"
WORKERS="${1:-10}"; TARGET="${2:-3000}"; TIMEOUT="${3:-180}"
RES="$ROOT/repro/results"
cd "$ROOT" || exit 99

echo "Narrowing test to the single failing parameter (revert with: git checkout -- '$TESTFILE')"
python3 "$HERE/narrow.py" "$TESTFILE"

# Production modules are assumed already built and installed to the local ~/.m2
# (skip the install build). Only flink-tests test-classes MUST be recompiled here,
# because narrow.py just rewrote the test source to drop the other parameters.
echo "Recompiling flink-tests test-classes (picks up the narrowed test source)..."
mvn -q -o test-compile -pl flink-tests \
   -Dcheckstyle.skip -Dspotless.check.skip=true -Drat.skip=true -Denforcer.skip=true || { echo TESTCOMPILE_FAIL; exit 1; }

rm -rf "$RES"; mkdir -p "$RES"; STOP="$RES/STOP"

# Ctrl-C / kill: signal workers to stop (STOP file) and hard-kill every child
# (workers + the mvn/surefire processes they fork) so nothing is left running.
cleanup() {
  trap - INT TERM
  touch "$STOP" 2>/dev/null
  echo; echo "Interrupted — stopping workers and killing child processes..."
  pkill -9 -P $$ 2>/dev/null            # direct children (workers)
  pkill -9 -f "surefire_w" 2>/dev/null  # forked surefire JVMs
  kill -- -$$ 2>/dev/null               # whole process group
  git checkout -- "$TESTFILE" 2>/dev/null
  exit 130
}
trap cleanup INT TERM
classify() { # 0 pass, 1 loss-or-dup, 2 infra/crash
  grep -qE "NUM_OUTPUTS = NUM_INPUTS|AssertionFailedError" "$1" && grep -qE "expected:|but was:" "$1" && return 1
  grep -q "Tests run: 1, Failures: 0, Errors: 0" "$1" && grep -q "BUILD SUCCESS" "$1" && return 0
  return 2
}
worker() {
  local id=$1 n=0
  while [ ! -f "$STOP" ]; do
    local total; total=$(cat "$RES"/.{pass,fail,infra} 2>/dev/null | wc -l | tr -d ' ')
    [ "$total" -ge "$TARGET" ] && break
    n=$((n+1)); local log="$RES/w${id}_${n}.log"
    mvn -o surefire:test -pl flink-tests -DtempDir="surefire_w${id}" \
      -Dsurefire.reportsDirectory="$RES/reports_w${id}" \
      -Dtest='UnalignedCheckpointRescaleITCase#shouldRescaleUnalignedCheckpoint' -DfailIfNoTests=false > "$log" 2>&1 &
    local mpid=$! waited=0
    while kill -0 "$mpid" 2>/dev/null; do sleep 5; waited=$((waited+5));
      [ "$waited" -ge "$TIMEOUT" ] && { pkill -9 -f "surefire_w${id}"; kill -9 "$mpid" 2>/dev/null; break; }; done
    wait "$mpid" 2>/dev/null
    classify "$log"; case $? in
      1) cp "$log" "$RES/FAIL_w${id}_${n}.log"; echo "*** FAILURE: $RES/FAIL_w${id}_${n}.log"; grep -m2 -E "expected:|but was:" "$log"; echo F >> "$RES/.fail"; touch "$STOP";;
      0) echo P >> "$RES/.pass"; rm -f "$log";;
      *) echo I >> "$RES/.infra"; rm -f "$log";;
    esac
  done
}
echo "Running up to $TARGET runs across $WORKERS workers (stops on first failure)..."
for w in $(seq 1 "$WORKERS"); do worker "$w" & done; wait
echo "PASS=$(wc -l < "$RES/.pass" 2>/dev/null||echo 0) FAIL=$(wc -l < "$RES/.fail" 2>/dev/null||echo 0) INFRA=$(wc -l < "$RES/.infra" 2>/dev/null||echo 0)"
git checkout -- "$TESTFILE" 2>/dev/null
