#!/bin/bash

# Search Safety Gate (Sprint 5)
# Ensures online search uses SQLx and offline search uses local cache
# Exit code: 0 if pass, 1 if fail

set -e

echo "Running Search Safety Gate..."

PROJECT_ROOT="/home/dali/WORK/BorneMap"
FAILURES=0

# Check backend search implementation uses SQLx
BACKEND_SEARCH=$(find "$PROJECT_ROOT/services/driver-service" -name "*.rs" -type f 2>/dev/null || true)
for f in $BACKEND_SEARCH; do
    if grep -qiE "search|pg_trgm|trigram|% \$" "$f" 2>/dev/null; then
        if ! grep -q "sqlx" "$f" 2>/dev/null; then
            echo "FAIL: Search implementation without SQLx in $f"
            FAILURES=$((FAILURES + 1))
        fi
    fi
done

# Check for external search service dependencies
for f in $BACKEND_SEARCH; do
    if grep -qiE "elasticsearch|algolia|meilisearch|typesense|solr" "$f" 2>/dev/null; then
        echo "FAIL: External search service dependency in $f"
        FAILURES=$((FAILURES + 1))
    fi
done

# Check frontend offline search queries local cache only
FRONTEND_FILES=$(find "$PROJECT_ROOT/apps" -name "*.rs" -type f 2>/dev/null || true)
for f in $FRONTEND_FILES; do
    if grep -qi "offline.*search\|search.*offline" "$f" 2>/dev/null; then
        if grep -qE "reqwest|http::|\"http" "$f" 2>/dev/null; then
            echo "FAIL: Offline search has backend dependency in $f"
            FAILURES=$((FAILURES + 1))
        fi
    fi
done

if [ $FAILURES -eq 0 ]; then
    echo "PASS: Verified online search uses SQLx, offline search uses local cache"
    exit 0
else
    echo "FAIL: $FAILURES search safety violation(s) detected"
    exit 1
fi
