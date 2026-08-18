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
# flink-tests requires Java 17 to compile. Only override JAVA_HOME if the current
# java on PATH is not already 17; otherwise respect the caller's environment.
if java -version 2>&1 | grep -qE '"17[.\"]|version "17'; then
  echo "Using current Java 17 from PATH ($(command -v java))"
else
  JAVA_HOME="$(/usr/libexec/java_home -v 17)" || { echo "JDK 17 not found"; exit 1; }
  export JAVA_HOME
  export PATH="$JAVA_HOME/bin:$PATH"
fi
HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
TESTFILE="$ROOT/flink-tests/src/test/java/org/apache/flink/test/checkpointing/UnalignedCheckpointRescaleITCase.java"
WORKERS="${1:-6}"; TARGET="${2:-2000}"; TIMEOUT="${3:-180}"
RES="$ROOT/repro/results"
cd "$ROOT" || exit 99

# Production modules are assumed already built and installed to the local ~/.m2
# (skip the install build). Only flink-tests test-classes MUST be recompiled here,
# because narrow.py just rewrote the test source to drop the other parameters.
echo "Recompiling flink-tests test-classes (picks up the narrowed test source)..."
mvn -q -o test-compile -pl flink-tests \
   -Dcheckstyle.skip -Dspotless.check.skip=true -Drat.skip=true -Denforcer.skip=true || { echo TESTCOMPILE_FAIL; exit 1; }

rm -rf "$RES"; mkdir -p "$RES"; STOP="$RES/STOP"

# One-time ONLINE priming, AFTER test-compile so it runs the freshly compiled
# narrowed test (surefire:test does not compile; it runs existing .class files).
# A fresh ~/.m2 is missing the whole surefire JUnit Platform runtime dependency
# set (the provider jar plus transitive deps such as opentest4j), which surefire
# resolves lazily at runtime; the offline loop below would then die one-by-one
# with "...has not been downloaded". Fetching artifacts individually is
# whack-a-mole, and a non-matching -Dtest pattern downloads nothing (surefire
# fails the no-match check before resolving the provider). The only reliable
# prime is to run the exact same invocation the loop runs, but ONLINE (no -o),
# so Maven downloads everything surefire actually needs. Runs the real test once
# (a few minutes); a warm cache makes subsequent reruns fast. Output goes to a
# log file (like the workers) instead of flooding the console.
echo "Priming local Maven cache: running the test once ONLINE to download all surefire deps (log: $RES/prime.log)..."
mvn surefire:test -pl flink-tests \
  -Dtest='UnalignedCheckpointRescaleITCase#shouldRescaleUnalignedCheckpoint' -DfailIfNoTests=false \
  > "$RES/prime.log" 2>&1 \
  || echo "Prime run finished (test result ignored; only the dependency download matters)."

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
classify() { # 0 pass, 1 loss-or-dup-or-corrupt-stream, 2 infra/crash
  grep -qE "NUM_OUTPUTS = NUM_INPUTS|AssertionFailedError" "$1" && grep -qE "expected:|but was:" "$1" && return 1
  # state corruption surfaces as a read-side exception rather than an assertion:
  # either an IOException ("Corrupt stream") or, when a bad header is read back,
  # an IllegalArgumentException ("Stream corrupted. Cannot find the header ...").
  grep -qF "Caused by: java.io.IOException: Corrupt stream" "$1" && return 1
  grep -qF "Stream corrupted. Cannot find the header" "$1" && return 1
  grep -qE "Tests run: [0-9]+, Failures: 0, Errors: 0" "$1" && grep -q "BUILD SUCCESS" "$1" && return 0
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
      -Dtest='UnalignedCheckpointITCase,UnalignedCheckpointRescaleITCase,UnalignedCheckpointRescaleSameUpstreamITCase,UnalignedCheckpointRescaleWithMixedExchangesITCase,UnalignedCheckpointFailureHandlingITCase,UnalignedCheckpointCompatibilityITCase,UnalignedCheckpointStressITCase' -DfailIfNoTests=false > "$log" 2>&1 &
    local mpid=$! waited=0
    while kill -0 "$mpid" 2>/dev/null; do sleep 5; waited=$((waited+5));
      [ "$waited" -ge "$TIMEOUT" ] && { pkill -9 -f "surefire_w${id}"; kill -9 "$mpid" 2>/dev/null; break; }; done
    wait "$mpid" 2>/dev/null
    classify "$log"; case $? in
      1) cp "$log" "$RES/FAIL_w${id}_${n}.log"; echo "*** FAILURE: $RES/FAIL_w${id}_${n}.log"; grep -m2 -E "expected:|but was:|Corrupt stream|Stream corrupted" "$log"; echo F >> "$RES/.fail"; touch "$STOP";;
      0) echo P >> "$RES/.pass"; rm -f "$log";;
      *) cp "$log" "$RES/INFRA_w${id}_${n}.log"; echo "INFRA: $RES/INFRA_w${id}_${n}.log"; echo I >> "$RES/.infra"; rm -f "$log";;
    esac
  done
}
echo "Running up to $TARGET runs across $WORKERS workers (stops on first failure)..."
for w in $(seq 1 "$WORKERS"); do worker "$w" & done; wait
echo "PASS=$(wc -l < "$RES/.pass" 2>/dev/null||echo 0) FAIL=$(wc -l < "$RES/.fail" 2>/dev/null||echo 0) INFRA=$(wc -l < "$RES/.infra" 2>/dev/null||echo 0)"
git checkout -- "$TESTFILE" 2>/dev/null
