#!/usr/bin/env bash
################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

################################################################################
# Loop test runner with timeout support
#
# Usage:
#   ./loop.sh                    # Use default 30 minutes timeout
#   TIMEOUT=3600 ./loop.sh       # Use custom timeout (e.g., 1 hour)
#
# The script will run rui_tools/run_test.sh in a loop, with each iteration
# having a configurable timeout. If a test run exceeds the timeout, it will
# be forcefully terminated to prevent hanging processes.
################################################################################

# set -e  # 遇到错误立即退出
set -x  # 会在执行每个命令之前，先打印出这个命令以及其所有参数。方便排查具体执行到哪里了

# Define the log directory
log_dir="log"

# Default timeout in seconds (30 minutes = 1800 seconds)
# TIMEOUT=${TIMEOUT:-1800}
TIMEOUT=${TIMEOUT:-21600}   # 6 hours

# ./mvnw -T 20 clean install -U -Pfast -DskipTests -Dmaven.javadoc.skip=true -Drat.skip=true -Dcheckstyle.skip=true -Denforcer.skip=true -P java11-target -P java11 -pl flink-tests -am

# Function to handle Ctrl+C
cleanup() {
    echo "Ctrl+C detected. Exiting script."
    exit 1
}

# Function to kill child process and its subprocesses
kill_child_processes() {
    local pid=$1
    if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
        echo "Killing process $pid and its children..."
        # Kill the process group to ensure all child processes are terminated
        pkill -TERM -P "$pid" 2>/dev/null || true
        kill -TERM "$pid" 2>/dev/null || true
        sleep 2
        # Force kill if still running
        if kill -0 "$pid" 2>/dev/null; then
            pkill -KILL -P "$pid" 2>/dev/null || true
            kill -KILL "$pid" 2>/dev/null || true
        fi
    fi
}

# Trap the SIGINT signal (Ctrl+C) and call the cleanup function
trap cleanup SIGINT

# Check if the log directory exists, if not, create it
if [ ! -d "$log_dir" ]; then
  mkdir "$log_dir"
fi

# Loop 100 times
for i in {1..100}; do
  echo "Starting iteration $i at $(date)"

  # Get the current date and time in the format YYYYMMDD_HHMMSS
  timestamp=$(date +"%Y%m%d_%H%M%S")

  # Define the log file name
  log_file="${log_dir}/${timestamp}.log"

  # Execute the script in background and get its PID
  sh rui_tools/run_test.sh > "$log_file" 2>&1 &
  child_pid=$!

  echo "Started run_test.sh with PID: $child_pid, timeout: ${TIMEOUT}s"

  # Wait for the process to complete or timeout
  timeout_count=0
  while kill -0 "$child_pid" 2>/dev/null; do
    sleep 30
    timeout_count=$((timeout_count + 30))

    if [ $timeout_count -ge $TIMEOUT ]; then
      echo "Process $child_pid timed out after ${TIMEOUT} seconds. Killing process..."
      echo "TIMEOUT: Process exceeded ${TIMEOUT} seconds limit" >> "$log_file"
      kill_child_processes "$child_pid"
      break
    fi
  done

  # Wait for the process to fully terminate
  wait "$child_pid" 2>/dev/null || true

  echo "Iteration $i completed at $(date)"
  echo "----------------------------------------"
done

echo "Script execution complete. Logs are saved in the '$log_dir' directory."
