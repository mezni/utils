#!/usr/bin/env bash
# SLO Benchmark for /api/v1/stations/nearby
# Usage: ./scripts/benchmark-nearby.sh [url] [requests] [concurrency]
set -euo pipefail

URL="${1:-http://localhost:8080/api/v1/stations/nearby?longitude=10.1815&latitude=36.8065&include_test=true}"
REQUESTS="${2:-20}"
CONCURRENCY="${3:-5}"

echo "=== SLO Benchmark: Nearby Endpoint ==="
echo "URL:        $URL"
echo "Requests:   $REQUESTS"
echo "Concurrency: $CONCURRENCY"
echo ""

TMPDIR=$(mktemp -d)
trap 'rm -rf "$TMPDIR"' EXIT

# Run requests in parallel using xargs
seq "$REQUESTS" | xargs -P "$CONCURRENCY" -I {} bash -c '
    start=$(date +%s%N)
    curl -sf -o /dev/null -w "%{http_code} %{time_total}\n" "'"$URL"'" 2>/dev/null || echo "FAIL"
' > "$TMPDIR/results.txt" 2>&1

# Parse results
echo "--- Results ---"
SUCCESS=0
FAIL=0
TIMES=()
while read -r line; do
    if [[ "$line" == "FAIL" ]]; then
        FAIL=$((FAIL + 1))
    else
        code=$(echo "$line" | awk "{print \$1}")
        time_sec=$(echo "$line" | awk "{print \$2}")
        time_ms=$(echo "$time_sec * 1000" | bc 2>/dev/null || echo "0")
        TIMES+=("$time_ms")
        SUCCESS=$((SUCCESS + 1))
    fi
done < "$TMPDIR/results.txt"

# Sort times
IFS=$'\n' sorted=($(sort -n <<<"${TIMES[*]}")); unset IFS

TOTAL=${#sorted[@]}
if [[ "$TOTAL" -gt 0 ]]; then
    p50_idx=$((TOTAL * 50 / 100))
    p95_idx=$((TOTAL * 95 / 100))
    p99_idx=$((TOTAL * 99 / 100))
    [[ "$p50_idx" -ge "$TOTAL" ]] && p50_idx=$((TOTAL - 1))
    [[ "$p95_idx" -ge "$TOTAL" ]] && p95_idx=$((TOTAL - 1))
    [[ "$p99_idx" -ge "$TOTAL" ]] && p99_idx=$((TOTAL - 1))

    MIN=${sorted[0]}
    MAX=${sorted[$((TOTAL - 1))]}
    P50=${sorted[$p50_idx]}
    P95=${sorted[$p95_idx]}
    P99=${sorted[$p99_idx]}

    # Calculate average
    SUM=0
    for t in "${sorted[@]}"; do SUM=$(echo "$SUM + $t" | bc); done
    AVG=$(echo "scale=2; $SUM / $TOTAL" | bc)

    echo "Successful:  $SUCCESS"
    echo "Failed:      $FAIL"
    echo "Min:         ${MIN}ms"
    echo "Avg:         ${AVG}ms"
    echo "P50:         ${P50}ms"
    echo "P95:         ${P95}ms"
    echo "P99:         ${P99}ms"
    echo "Max:         ${MAX}ms"
    echo ""

    if (( $(echo "$P95 <= 200" | bc -l) )); then
        echo "✓ SLO PASS: p95 (${P95}ms) ≤ 200ms"
    else
        echo "✗ SLO FAIL: p95 (${P95}ms) > 200ms"
    fi
else
    echo "No successful requests."
fi
