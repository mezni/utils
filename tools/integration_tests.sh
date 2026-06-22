#!/bin/bash
set -e

# Stage 8: integration_tests
# Run cargo test

echo "=== Stage 8: integration_tests ==="

OUTPUT=$(cargo test --workspace --all-features 2>&1)

if [ $? -eq 0 ]; then
  echo "integration_tests PASSED"
  echo '{"status":"passed","exit_code":0,"summary":"All tests passed"}' > .specify/ci-artifacts/test_results.json
else
  echo "integration_tests FAILED"
  echo "$OUTPUT" > .specify/ci-artifacts/test_results.json
  echo '{"status":"failed","exit_code":1,"summary":"Tests failed"}' > .specify/ci-artifacts/test_results.json
  exit 1
fi
