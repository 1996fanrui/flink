#!/bin/bash
# Usage: ./flink-examples/flink-examples-streaming/run_benchmark.sh
# Runs 3 benchmark groups in parallel with different REST ports.
set -e

cd "$(cd "$(dirname "$0")/../.." && pwd)"

CP="$(./mvnw -pl flink-examples/flink-examples-streaming dependency:build-classpath -q -Dmdep.outputFile=/dev/stderr -Pjava11,java11-target 2>&1 1>/dev/null)"
RUN="java -cp $CP:flink-examples/flink-examples-streaming/target/classes org.apache.flink.streaming.examples.UnalignedCheckpointBenchmark"

# Pick 3 free ports from 30000-40000
pick_free_port() {
  while true; do
    port=$((RANDOM % 10000 + 30000))
    if ! lsof -ti :$port >/dev/null 2>&1; then echo $port; return; fi
  done
}
P1=$(pick_free_port)
P2=$(pick_free_port)
P3=$(pick_free_port)
echo "Ports: no-rescale=$P1  scale-up=$P2  scale-down=$P3"

# Launch all 3 in parallel
$RUN "5,5;5,5;5,5;5,5;5,5;5,5;5,5;5,5;5,5;5,5" $P1 &
$RUN "4,6;5,7;6,8;7,9;8,10;9,11;10,12;11,13;12,14;13,15" $P2 &
$RUN "15,16;14,15;13,14;12,13;11,12;10,11;9,10;8,9;7,8;6,7" $P3 &

wait
echo "All benchmarks complete! Results in: flink-examples/flink-examples-streaming/benchmark_result/"
