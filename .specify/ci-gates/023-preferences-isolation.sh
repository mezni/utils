#!/bin/bash

# Preferences Isolation Gate (Sprint 5)
# Ensures all personalization uses existing users.preferences JSONB — no schema expansion
# Exit code: 0 if pass, 1 if fail

set -e

echo "Running Preferences Isolation Gate..."

PROJECT_ROOT="/home/dali/WORK/BorneMap"
FAILURES=0

# Check Rust migration files for new columns/tables for preferences or favorites
MIGRATION_FILES=$(find "$PROJECT_ROOT/services" -path "*/migrations/*" -name "*.sql" 2>/dev/null || true)
for f in $MIGRATION_FILES; do
    if grep -qiE "CREATE\s+TABLE.*(preferences|favorites)|ADD\s+COLUMN.*(preferences|favorites)" "$f" 2>/dev/null; then
        echo "FAIL: Schema expansion detected in $f"
        FAILURES=$((FAILURES + 1))
    fi
done

# Check for new table creation related to preferences or favorites
RUST_FILES=$(find "$PROJECT_ROOT/services" -name "*.rs" -type f 2>/dev/null || true)
for f in $RUST_FILES; do
    if grep -qE "create_table.*(preferences|favorites)|CREATE TABLE.*(preferences|favorites)" "$f" 2>/dev/null; then
        echo "FAIL: Schema expansion attempt in $f"
        FAILURES=$((FAILURES + 1))
    fi
done

# Check frontend code for direct database references
FRONTEND_FILES=$(find "$PROJECT_ROOT/apps" -name "*.rs" -type f 2>/dev/null || true)
for f in $FRONTEND_FILES; do
    if grep -qiE "sqlx::|pg_pool|Pool<Postgres>" "$f" 2>/dev/null; then
        echo "FAIL: Frontend contains direct database reference in $f"
        FAILURES=$((FAILURES + 1))
    fi
done

if [ $FAILURES -eq 0 ]; then
    echo "PASS: Verified all personalization uses existing users.preferences JSONB"
    exit 0
else
    echo "FAIL: $FAILURES schema violation(s) detected"
    exit 1
fi
