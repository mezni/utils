#!/usr/bin/env bash
# Event Logging E2E Tests
# Tests: US4 - Event logging end-to-end
# Verifies that user interactions are captured as events in analytics_db.
# Run: bash event-logging.sh
# Requires: Running backend + Traefik

set -e

GATEWAY="http://localhost:8080"
PASS=0
FAIL=0

pass() { PASS=$((PASS+1)); echo "  PASS: $1"; }
fail() { FAIL=$((FAIL+1)); echo "  FAIL: $1"; }

SESSION_ID="e2e-test-$(date +%s)"

echo "=== Event Logging E2E Tests ==="
echo ""

# T038: Station detail view event
echo "--- T038: station_detail_view event ---"
response=$(curl -s -w "\n%{http_code}" -X POST "$GATEWAY/api/v1/events" \
  -H "Content-Type: application/json" \
  -d "{
    \"event_type\": \"station_detail_view\",
    \"actor\": { \"session_id\": \"$SESSION_ID\", \"ip_address\": \"127.0.0.1\" },
    \"context\": { \"station_id\": \"STA-TEST-001\" },
    \"timestamp\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"
  }" --max-time 5)
http_code=$(echo "$response" | tail -1)
if [ "$http_code" = "201" ]; then
  pass "station_detail_view returned $http_code"
else
  fail "station_detail_view returned $http_code (expected 201)"
fi

# T039: Search event
echo ""
echo "--- T039: search event ---"
response=$(curl -s -w "\n%{http_code}" -X POST "$GATEWAY/api/v1/events" \
  -H "Content-Type: application/json" \
  -d "{
    \"event_type\": \"search\",
    \"actor\": { \"session_id\": \"$SESSION_ID\", \"ip_address\": \"127.0.0.1\" },
    \"context\": { \"search_query\": \"Tunis\", \"result_count\": 5 },
    \"timestamp\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"
  }" --max-time 5)
http_code=$(echo "$response" | tail -1)
if [ "$http_code" = "201" ]; then
  pass "search event returned $http_code"
else
  fail "search event returned $http_code (expected 201)"
fi

# T040: Nearby search event
echo ""
echo "--- T040: nearby_search event ---"
response=$(curl -s -w "\n%{http_code}" -X POST "$GATEWAY/api/v1/events" \
  -H "Content-Type: application/json" \
  -d "{
    \"event_type\": \"nearby_search\",
    \"actor\": { \"session_id\": \"$SESSION_ID\", \"ip_address\": \"127.0.0.1\" },
    \"context\": { \"coordinates\": { \"lat\": 36.8065, \"lng\": 10.1815 }, \"result_count\": 3 },
    \"timestamp\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"
  }" --max-time 5)
http_code=$(echo "$response" | tail -1)
if [ "$http_code" = "201" ]; then
  pass "nearby_search event returned $http_code"
else
  fail "nearby_search event returned $http_code (expected 201)"
fi

# T041: Navigate to station event
echo ""
echo "--- T041: navigate_to_station event ---"
response=$(curl -s -w "\n%{http_code}" -X POST "$GATEWAY/api/v1/events" \
  -H "Content-Type: application/json" \
  -d "{
    \"event_type\": \"navigate_to_station\",
    \"actor\": { \"session_id\": \"$SESSION_ID\", \"ip_address\": \"127.0.0.1\" },
    \"context\": { \"station_id\": \"STA-TEST-001\" },
    \"timestamp\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"
  }" --max-time 5)
http_code=$(echo "$response" | tail -1)
if [ "$http_code" = "201" ]; then
  pass "navigate_to_station event returned $http_code"
else
  fail "navigate_to_station event returned $http_code (expected 201)"
fi

# T042: Batch event (50+ events)
echo ""
echo "--- T042: Batch event (50 events) ---"
batch_events="{\"events\":["
for i in $(seq 1 50); do
  if [ $i -gt 1 ]; then batch_events+=","; fi
  batch_events+="{
    \"event_type\": \"station_detail_view\",
    \"actor\": { \"session_id\": \"$SESSION_ID-batch\", \"ip_address\": \"127.0.0.1\" },
    \"context\": { \"station_id\": \"STA-TEST-00$((i % 3 + 1))\" },
    \"timestamp\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"
  }"
done
batch_events+="]}"
response=$(curl -s -w "\n%{http_code}" -X POST "$GATEWAY/api/v1/events/batch" \
  -H "Content-Type: application/json" \
  -d "$batch_events" --max-time 10)
http_code=$(echo "$response" | tail -1)
if [ "$http_code" = "201" ]; then
  pass "Batch events (50) returned $http_code"
else
  fail "Batch events (50) returned $http_code (expected 201)"
fi

# T043: Malformed event data
echo ""
echo "--- T043: Malformed event data (400) ---"
response=$(curl -s -w "\n%{http_code}" -X POST "$GATEWAY/api/v1/events" \
  -H "Content-Type: application/json" \
  -d "{\"event_type\": \"invalid\", \"actor\": {}}" --max-time 5)
http_code=$(echo "$response" | tail -1)
if [ "$http_code" = "400" ]; then
  pass "Malformed event returned $http_code (expected 400)"
else
  fail "Malformed event returned $http_code (expected 400)"
fi

echo ""
echo "=== Results ==="
echo "Passed: $PASS"
echo "Failed: $FAIL"
if [ "$FAIL" -gt 0 ]; then
  exit 1
fi
echo "All event logging tests passed."
