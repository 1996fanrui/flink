#!/usr/bin/env bash
################################################################################
# Quick failure location script for Maven test logs.
#
# Usage:
#   ./find_failures.sh ./log/*.log
#   ./find_failures.sh ./log/20260226_*.log
#
# Searches log files for Maven test failure sections and prints them with
# the source filename for quick identification.
################################################################################

set -e

if [ $# -eq 0 ]; then
  echo "Error: No log files specified." >&2
  echo "Usage: ./find_failures.sh ./log/*.log" >&2
  exit 1
fi

# Run awk to extract failure sections from all provided log files.
# The pattern captures lines between "[ERROR] Errors:" and the next "Tests run:" line,
# prefixing each line with the source filename.
failure_output=$(awk '/\[ERROR\] Errors:/{if(!found) print "---"; found=1} found{print FILENAME": "$0} /Tests run:/{if(found) found=0}' "$@" 2>/dev/null || true)

# Count files scanned and files with failures.
total_files=$#
files_with_failures=0

for f in "$@"; do
  if [ -f "$f" ] && grep -q '\[ERROR\] Errors:' "$f" 2>/dev/null; then
    files_with_failures=$((files_with_failures + 1))
  fi
done

# Print failure details (if any).
if [ -n "$failure_output" ]; then
  echo "$failure_output"
  echo ""
fi

# Print summary.
echo "========================================"
echo "Scan Summary"
echo "========================================"
echo "Files scanned:        $total_files"
echo "Files with failures:  $files_with_failures"
echo "========================================"
