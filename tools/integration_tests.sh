#!/bin/bash
set -e

# Stage 8: integration_tests
# Run cargo test

echo "=== Stage 8: integration_tests ==="

# Run cargo test and capture output
OUTPUT=$(cargo test --workspace --all-features 2>&1)
EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
  echo "integration_tests PASSED"
  # Count passed tests
  PASSED_COUNT=$(echo "$OUTPUT" | grep -oP "passed.*\(total" | grep -oP "passed: \K\d+" || echo "0")
  TOTAL_COUNT=$(echo "$OUTPUT" | grep -oP "passed.*\(total" | grep -oP "total: \K\d+" || echo "0")
  echo '{"status":"passed","exit_code":0,"test_count":'$TOTAL_COUNT',"passed":'$PASSED_COUNT',"summary":"All tests passed"}' > .specify/ci-artifacts/test_results.json
else
  echo "integration_tests FAILED with exit code $EXIT_CODE"
  echo "$OUTPUT" > .specify/ci-artifacts/test_results.json
  # Count failed tests
  FAILED_COUNT=$(echo "$OUTPUT" | grep -oP "failed.*\(total" | grep -oP "failed: \K\d+" || echo "0")
  echo '{"status":"failed","exit_code":'$EXIT_CODE',"test_count":0,"passed":0,"failed":'$FAILED_COUNT',"summary":"Tests failed"}' > .specify/ci-artifacts/test_results.json
  exit 1
fi
