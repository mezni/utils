#!/bin/bash
set -e

# Stage 6: sqlx_compile_check
# Run SQLx offline verification

echo "=== Stage 6: sqlx_compile_check ==="

# Check if SQLX_OFFLINE is set
if [ -z "$SQLX_OFFLINE" ]; then
  export SQLX_OFFLINE=true
fi

# Generate SQLx offline data if needed
echo "Running cargo sqlx prepare..."
cargo sqlx prepare --all -- --database-url "$DB_URL"

# Check if SQLx prepare succeeded
if [ $? -eq 0 ]; then
  echo "sqlx_compile_check PASSED"
  echo '{"status":"passed","exit_code":0,"summary":"SQLx offline verification passed"}' > .specify/ci-artifacts/sqlx_prepare_state.json
else
  echo "sqlx_compile_check FAILED"
  echo '{"status":"failed","exit_code":1,"summary":"SQLx offline verification failed"}' > .specify/ci-artifacts/sqlx_prepare_state.json
  exit 1
fi
