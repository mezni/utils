#!/bin/bash

# UI Boundary Gate (Sprint 5)
# Ensures frontend is consumer-only — no business logic leakage
# Exit code: 0 if pass, 1 if fail

set -e

echo "Running UI Boundary Gate..."

PROJECT_ROOT="/home/dali/WORK/BorneMap"
FAILURES=0

# Check frontend code for business logic patterns
FRONTEND_FILES=$(find "$PROJECT_ROOT/apps" -name "*.rs" -o -name "*.ts" -o -name "*.tsx" | grep -v node_modules | grep -v dist 2>/dev/null || true)
FRONTEND_JS=$(find "$PROJECT_ROOT/apps" -path "*/node_modules" -prune -o -path "*/dist" -prune -o \( -name "*.ts" -o -name "*.tsx" \) -print 2>/dev/null || true)
for f in $FRONTEND_FILES; do
    # Check for direct database calls
    if grep -qE "sqlx::|PgPool|Pool<Postgres>" "$f" 2>/dev/null; then
        echo "FAIL: Frontend has direct database call in $f"
        FAILURES=$((FAILURES + 1))
    fi
    # Check for service topology decisions
    if grep -qi "service_url|backend_url|port.*300[0-2]" "$f" 2>/dev/null; then
        echo "FAIL: Frontend has service topology knowledge in $f"
        FAILURES=$((FAILURES + 1))
    fi
    # Check for identity validation logic (exclude doc comments)
    if grep -qiE "(impl|fn|pub fn).*jwt.*validate|(impl|fn|pub fn).*token.*verify|(impl|fn|pub fn).*keycloak.*auth" "$f" 2>/dev/null; then
        echo "FAIL: Frontend has identity/business logic in $f"
        FAILURES=$((FAILURES + 1))
    fi
done

# Check that backend crates aren't imported by frontend
for f in $FRONTEND_FILES; do
    if grep -qE "^use (actix_web|driver_service|auth_service|admin_service)" "$f" 2>/dev/null; then
        echo "FAIL: Frontend imports backend crate in $f"
        FAILURES=$((FAILURES + 1))
    fi
done

# Check TS/JS frontend for business logic patterns
for f in $FRONTEND_JS; do
    if grep -qE "sqlx|PgPool|Pool<Postgres>" "$f" 2>/dev/null; then
        echo "FAIL: JS frontend has direct database call in $f"
        FAILURES=$((FAILURES + 1))
    fi
    if grep -qE "import.*from.*(driver-service|auth-service|admin-service)" "$f" 2>/dev/null; then
        echo "FAIL: JS frontend imports backend service in $f"
        FAILURES=$((FAILURES + 1))
    fi
    if grep -qE 'localhost:300[0-2]|service_url|backend_url' "$f" 2>/dev/null; then
        echo "FAIL: JS frontend has hardcoded service URL in $f"
        FAILURES=$((FAILURES + 1))
    fi
done

if [ $FAILURES -eq 0 ]; then
    echo "PASS: Verified frontend is consumer-only, no business logic leaked"
    exit 0
else
    echo "FAIL: $FAILURES boundary violation(s) detected"
    exit 1
fi
