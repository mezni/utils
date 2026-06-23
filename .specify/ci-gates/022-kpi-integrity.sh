#!/bin/bash

# KPI Integrity Gate
# Ensures all KPIs are derived from telemetry events only
# Exit code: 0 if pass, 1 if fail

set -e

echo "🔍 Running KPI Integrity Gate..."

ADMIN_SERVICE_DIR="/home/dali/WORK/BorneMap/services/admin-service"
VALIDATOR_SCRIPT="/home/dali/WORK/BorneMap/services/admin-service/src/validators/kpi_integrity.rs"

# Check if validator module exists
if [ ! -f "$VALIDATOR_SCRIPT" ]; then
    echo "❌ Error: Validator module not found at $VALIDATOR_SCRIPT"
    exit 1
fi

# Run the validator
cd "$ADMIN_SERVICE_DIR"
cargo build --quiet

# Run the validator logic
echo "Checking for KPI integrity violations..."
RUST_LOG=info cargo run --bin check-kpi-integrity 2>&1 | grep -q "All KPIs derived from telemetry events only" && EXIT_CODE=0 || EXIT_CODE=1

if [ $EXIT_CODE -eq 0 ]; then
    echo "✅ KPI Integrity Gate PASSED"
    exit 0
else
    echo "❌ KPI Integrity Gate FAILED"
    echo "   Found KPI calculations using external data sources"
    exit 1
fi