#!/usr/bin/env bash
# Test Report Aggregation
# Aggregates all test results into a single human-readable report.
# SC-008: Single report with pass/fail summary, timing, and failure details.
# Run: bash report.sh
# Requires: All test scripts to have been run first

set -e

REPORT_DIR="specs/005-integration-testing/tests"
REPORT_FILE="$REPORT_DIR/report-$(date +%Y%m%d-%H%M%S).md"

echo "Generating test report..."
mkdir -p "$REPORT_DIR"

cat > "$REPORT_FILE" << EOF
# Test Report: Integration & Testing
**Generated**: $(date -u +%Y-%m-%dT%H:%M:%SZ)
**Environment**: $(hostname)
**Branch**: $(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "unknown")
**Commit**: $(git rev-parse HEAD 2>/dev/null || echo "unknown")

## Summary

| Test Suite | Status | Passed | Failed | Duration |
|------------|--------|--------|--------|----------|
EOF

run_test() {
  local name=$1
  local script=$2
  local start=$(date +%s%N)

  if [ -f "$script" ]; then
    set +e
    output=$(bash "$script" 2>&1)
    exit_code=$?
    set -e

    local end=$(date +%s%N)
    local duration_ms=$(( (end - start) / 1000000 ))

    if [ $exit_code -eq 0 ]; then
      passed=$(echo "$output" | grep -c "PASS:" || true)
      failed=$(echo "$output" | grep -c "FAIL:" || true)
      echo "| $name | ✅ PASS | $passed | $failed | ${duration_ms}ms |" >> "$REPORT_FILE"
    else
      passed=$(echo "$output" | grep -c "PASS:" || true)
      failed=$(echo "$output" | grep -c "FAIL:" || true)
      echo "| $name | ❌ FAIL | $passed | $failed | ${duration_ms}ms |" >> "$REPORT_FILE"
      echo "" >> "$REPORT_FILE"
      echo "### $name - Failure Details" >> "$REPORT_FILE"
      echo '```' >> "$REPORT_FILE"
      echo "$output" >> "$REPORT_FILE"
      echo '```' >> "$REPORT_FILE"
    fi
  else
    echo "| $name | ⏭️ SKIP | 0 | 0 | 0ms |" >> "$REPORT_FILE"
  fi
}

# Run test suites
# (Scripts are sourced so they can be run independently;
#  this aggregation runs them sequentially and captures output)

run_test "Traefik Routing" "$REPORT_DIR/traefik-routing.sh"
run_test "Event Logging" "$REPORT_DIR/event-logging.sh"
run_test "Auth Rejection" "$REPORT_DIR/auth-rejection.sh"

# Contract tests (Pact) — requires Rust test harness
if command -v cargo &> /dev/null; then
  echo "" >> "$REPORT_FILE"
  echo "### Contract Tests (Pact)" >> "$REPORT_FILE"
  echo '```' >> "$REPORT_FILE"
  (cd source/services/driver-service && cargo test --test contract_tests 2>&1) || true
  (cd source/services/admin-service && cargo test --test contract_tests 2>&1) || true
  echo '```' >> "$REPORT_FILE"
fi

# Load test results (k6) — requires k6
if command -v k6 &> /dev/null; then
  echo "" >> "$REPORT_FILE"
  echo "### Load Test Results (k6)" >> "$REPORT_FILE"
  echo '```' >> "$REPORT_FILE"
  k6 run "$REPORT_DIR/load-test.js" 2>&1 || true
  echo '```' >> "$REPORT_FILE"
fi

cat >> "$REPORT_FILE" << EOF

## Notes
- **Traefik Routing**: Verify all services are reachable through the gateway
- **Event Logging**: Verify events are captured for all interaction types
- **Auth Rejection**: Verify unauthenticated requests are handled gracefully
- **Contract Tests**: Verify API responses match documented schemas
- **Load Tests**: Verify p95 nearby search latency < 100ms at 50 concurrent requests
EOF

echo "Report written to: $REPORT_FILE"
echo "Report saved."
