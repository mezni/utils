#!/bin/bash

# Read-Only Enforcement Gate
# Ensures no write operations to analytics_db in admin-service
# Exit code: 0 if pass, 1 if fail

set -e

echo "🔍 Running Read-Only Enforcement Gate..."

ADMIN_SERVICE_DIR="/home/dali/WORK/BorneMap/services/admin-service"
VALIDATOR_SCRIPT="/home/dali/WORK/BorneMap/services/admin-service/src/validators/read_only.rs"

# Check if validator module exists
if [ ! -f "$VALIDATOR_SCRIPT" ]; then
    echo "❌ Error: Validator module not found at $VALIDATOR_SCRIPT"
    exit 1
fi

# Run the validator
cd "$ADMIN_SERVICE_DIR"
cargo build --quiet

# Run the validator logic
echo "Checking for write operations in admin-service..."
RUST_LOG=info cargo run --bin check-read-only 2>&1 | grep -q "No write operations" && EXIT_CODE=0 || EXIT_CODE=1

if [ $EXIT_CODE -eq 0 ]; then
    echo "✅ Read-Only Enforcement Gate PASSED"
    exit 0
else
    echo "❌ Read-Only Enforcement Gate FAILED"
    echo "   Found write operations targeting analytics_db"
    exit 1
fi