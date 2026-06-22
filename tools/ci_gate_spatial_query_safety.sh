#!/bin/bash
# CI Gate: Spatial Query Safety
# Purpose: Ensure NO raw SQL string construction in spatial queries
# Fails if non-SQLx queries or dynamic SQL construction found

echo "Running Spatial Query Safety CI Gate..."

# Find all Rust files in driver-service
RUST_FILES=$(find services/driver-service/src -name "*.rs")

PASSED=true

for file in $RUST_FILES; do
    # Check for SQL string construction (raw SQL)
    if grep -qE "sqlx::query!(.+sqlx::query".*".+sqlx::query!".*".*sqlx::query!" "$file"; then
        echo "✗ FAIL: Found raw SQL string construction in $file"
        PASSED=false
    fi

    # Check for dynamic SQL construction
    if grep -qE '\+.*"SELECT"|\+.*"UPDATE"|\+.*"INSERT"|\+.*"DELETE"|\+.*"CREATE"|\+.*"DROP"' "$file"; then
        echo "✗ FAIL: Found dynamic SQL construction in $file"
        PASSED=false
    fi

    # Check for any SQL strings not using sqlx::query! macro
    if grep -qE 'query\(|execute\(|fetch\(' "$file"; then
        echo "✗ FAIL: Found non-SQLx query in $file"
        PASSED=false
    fi
done

if [ "$PASSED" = true ]; then
    echo "✓ PASS: No raw SQL or non-SQLx queries found"
    exit 0
else
    echo "✗ FAIL: Spatial query safety violated"
    exit 1
fi
