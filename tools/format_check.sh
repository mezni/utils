#!/bin/bash
set -e

# Stage 1: format_check
# Run cargo fmt --check --all and report results

echo "=== Stage 1: format_check ==="

OUTPUT=$(cargo fmt --check --all 2>&1)

if [ $? -eq 0 ]; then
  echo "format_check PASSED"
  echo '{"status":"passed","exit_code":0,"summary":"All files formatted"}' > .specify/ci-artifacts/format_check_report.json
else
  echo "format_check FAILED"
  echo "$OUTPUT" > .specify/ci-artifacts/format_check_report.json
  echo '{"status":"failed","exit_code":1,"summary":"Files not formatted"}' > .specify/ci-artifacts/format_check_report.json
  exit 1
fi
