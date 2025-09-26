#!/bin/bash
# Usage: ./rui_tools/run_test.sh
# Runs all Unaligned Checkpoint ITCase tests from commit ba966a8a382.

set -e  # 遇到错误立即退出
set -x  # 会在执行每个命令之前，先打印出这个命令以及其所有参数。方便排查具体执行到哪里了

cd flink-tests
../mvnw -am \
  -Dtest="org.apache.flink.test.checkpointing.UnalignedCheckpointITCase,org.apache.flink.test.checkpointing.UnalignedCheckpointRescaleITCase,org.apache.flink.test.checkpointing.UnalignedCheckpointRescaleWithMixedExchangesITCase" \
  test \
  -Dflink.XmxUnitTest=6096m \
  -Dflink.forkCountUnitTest=1 \
  -Dmaven.javadoc.skip=true -Drat.skip=true -Dcheckstyle.skip=true -Denforcer.skip=true \
  -P java11,java11-target --no-snapshot-updates
