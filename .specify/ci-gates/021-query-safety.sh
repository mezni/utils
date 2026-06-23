#!/bin/bash

# Query Safety Gate
# Ensures no dynamic SQL or SQL injection vulnerabilities
# Exit code: 0 if pass, 1 if fail

set -e

echo "🔍 Running Query Safety Gate..."

ADMIN_SERVICE_DIR="/home/dali/WORK/BorneMap/services/admin-service"
VALIDATOR_SCRIPT="/home/dali/WORK/BorneMap/services/admin-service/src/validators/query_safety.rs"

# Check if validator module exists
if [ ! -f "$VALIDATOR_SCRIPT" ]; then
    echo "❌ Error: Validator module not found at $VALIDATOR_SCRIPT"
    exit 1
fi

# Run the validator
cd "$ADMIN_SERVICE_DIR"
cargo build --quiet

# Run the validator logic
echo "Checking for dynamic SQL patterns..."
RUST_LOG=info cargo run --bin check-query-safety 2>&1 | grep -q "No dynamic SQL patterns" && EXIT_CODE=0 || EXIT_CODE=1

if [ $EXIT_CODE -eq 0 ]; then
    echo "✅ Query Safety Gate PASSED"
    exit 0
else
    echo "❌ Query Safety Gate FAILED"
    echo "   Found dynamic SQL or SQL injection vulnerabilities"
    exit 1
fi