#!/usr/bin/env bash
# Load test for Clickstream Service — SC-004 verification
# Requires: oha (https://github.com/hatoo/oha) or wrk

set -euo pipefail

BASE_URL="${CLICKSTREAM_URL:-http://localhost:8082}"
CONCURRENCY="${CONCURRENCY:-500}"
DURATION="${DURATION:-10s}"
ENDPOINT="${ENDPOINT:-/api/v1/events}"

echo "=== Clickstream Service Load Test ==="
echo "Target: $BASE_URL$ENDPOINT"
echo "Concurrency: $CONCURRENCY"
echo "Duration: $DURATION"
echo ""

PAYLOAD='{"event_name":"map_open","session_id":"load-test-session","client_ts":"2026-06-11T12:00:00Z"}'

if command -v oha &>/dev/null; then
    oha -n 0 -c "$CONCURRENCY" -z "$DURATION" \
        -m POST \
        -H "Content-Type: application/json" \
        -d "$PAYLOAD" \
        "$BASE_URL$ENDPOINT"
elif command -v wrk &>/dev/null; then
    wrk -t"$(nproc)" -c"$CONCURRENCY" -d"${DURATION%s}" \
        -s <(echo 'wrk.method = "POST"') \
        "$BASE_URL$ENDPOINT"
else
    echo "Neither 'oha' nor 'wrk' found. Install one to run load tests."
    echo "  cargo install oha"
    echo "  brew install wrk"
    exit 1
fi
