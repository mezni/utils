#!/bin/bash
set -e

# Stage 9: build_success
# Run cargo build

echo "=== Stage 9: build_success ==="

OUTPUT=$(cargo build --release --workspace 2>&1)

if [ $? -eq 0 ]; then
  echo "build_success PASSED"
  echo '{"status":"passed","exit_code":0,"summary":"Build successful"}' > .specify/ci-artifacts/build_state.json
else
  echo "build_success FAILED"
  echo "$OUTPUT" > .specify/ci-artifacts/build_state.json
  echo '{"status":"failed","exit_code":1,"summary":"Build failed"}' > .specify/ci-artifacts/build_state.json
  exit 1
fi
