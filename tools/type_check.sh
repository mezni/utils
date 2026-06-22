#!/bin/bash
set -e

# Stage 2: type_check
# Run cargo clippy and report violations

echo "=== Stage 2: type_check ==="

OUTPUT=$(cargo clippy --all-targets --all-features --workspace -- -D warnings 2>&1)

if [ $? -eq 0 ]; then
  echo "type_check PASSED"
  echo '{"status":"passed","exit_code":0,"violation_count":0,"summary":"No clippy violations found"}' > .specify/ci-artifacts/type_check_report.json
else
  echo "type_check FAILED"
  echo "$OUTPUT" > .specify/ci-artifacts/type_check_report.json
  echo '{"status":"failed","exit_code":1,"violation_count":1,"summary":"Clippy violations found"}' > .specify/ci-artifacts/type_check_report.json
  exit 1
fi
