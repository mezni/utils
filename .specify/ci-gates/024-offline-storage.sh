#!/bin/bash

# Offline Storage Gate (Sprint 5)
# Ensures offline functionality has no backend dependency
# Exit code: 0 if pass, 1 if fail

set -e

echo "Running Offline Storage Gate..."

PROJECT_ROOT="/home/dali/WORK/BorneMap"
FAILURES=0

# Check frontend code for offline storage imports
FRONTEND_FILES=$(find "$PROJECT_ROOT/apps" -name "*.rs" -type f 2>/dev/null || true)
for f in $FRONTEND_FILES; do
    # Check for backend dependencies in offline-related code
    if grep -qi "offline\|cache\|local_storage\|async_storage" "$f" 2>/dev/null; then
        if grep -qE "reqwest::|http::|api_url|backend_url|\"http" "$f" 2>/dev/null; then
            echo "FAIL: Offline code has backend dependency in $f"
            FAILURES=$((FAILURES + 1))
        fi
    fi
done

# Check that service files don't contain offline storage logic
SERVICE_FILES=$(find "$PROJECT_ROOT/services" -name "*.rs" -type f 2>/dev/null || true)
for f in $SERVICE_FILES; do
    if grep -qi "offline" "$f" 2>/dev/null; then
        echo "FAIL: Offline logic found in backend service $f"
        FAILURES=$((FAILURES + 1))
    fi
done

if [ $FAILURES -eq 0 ]; then
    echo "PASS: Verified offline storage has no backend dependencies"
    exit 0
else
    echo "FAIL: $FAILURES backend dependency violation(s) detected"
    exit 1
fi
