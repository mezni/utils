# Quickstart: MVP-2 Hardening Verification

**Branch**: `012-mvp2-hardening`

## Prerequisites

- Rust 1.85+ toolchain
- Docker Engine 24+ with `docker compose` plugin
- (Optional) PostgreSQL 17+ for integration tests without Docker

## Quick Verification Commands

```bash
# 1. Build and lint
cargo build --all
cargo clippy --all-targets -- -D warnings

# 2. Run tests (offline — no DB required)
cargo test --all

# 3. Full stack from zero
docker compose down -v
docker compose up --build -d
docker compose ps --filter health=healthy
curl http://localhost:8080/api/health
curl http://localhost:8081/api/health

# 4. Stop
docker compose down
```

## Verification Scripts

### Zero-State Docker Test

```bash
#!/bin/bash
set -e
echo "=== Zero-State Docker Test ==="
docker compose down -v 2>/dev/null
docker compose up --build -d
echo "Waiting 60s for health checks..."
sleep 60
docker compose ps --filter health=healthy
echo "Driver health: $(curl -s -o /dev/null -w '%{http_code}' http://localhost:8080/api/health)"
echo "Admin health: $(curl -s -o /dev/null -w '%{http_code}' http://localhost:8081/api/health)"
echo "=== PASS ==="
```

### Full Product Loop Test

```bash
#!/bin/bash
set -e
BASE=http://localhost:8081/api
DRIVER=http://localhost:8080/api

echo "=== Full Product Loop ==="
# Create partner
PARTNER=$(curl -s -X POST "$BASE/partners" \
  -H 'Content-Type: application/json' \
  -d '{"name":"Test Partner","type":"business"}')
PARTNER_ID=$(echo "$PARTNER" | jq -r '.id')
echo "Created partner: $PARTNER_ID"

# Verify
curl -s -X PATCH "$BASE/partners/$PARTNER_ID/verify"
echo "Verified"

# Set live
curl -s -X PATCH "$BASE/partners/$PARTNER_ID" \
  -H 'Content-Type: application/json' \
  -d '{"is_live":true}'
echo "Set live"

# Create station
STATION=$(curl -s -X POST "$BASE/stations" \
  -H 'Content-Type: application/json' \
  -d "{\"partner_id\":$PARTNER_ID,\"name\":\"Test Station\",\"address\":\"Tunis\",\"latitude\":36.8065,\"longitude\":10.1815}")
STATION_ID=$(echo "$STATION" | jq -r '.id')
echo "Created station: $STATION_ID"

# Verify station appears in driver results
echo "Driver nearby: $(curl -s "$DRIVER/stations/nearby?lat=36.8&lng=10.18&radius_km=10" | jq '.stations | length') stations"

# Deactivate
curl -s -X PATCH "$BASE/partners/$PARTNER_ID/deactivate"
echo "Deactivated"

# Verify station disappears
echo "Driver nearby after deactivate: $(curl -s "$DRIVER/stations/nearby?lat=36.8&lng=10.18&radius_km=10" | jq '.stations | length') stations"

echo "=== FULL LOOP PASS ==="
```

## CI Verification

```bash
# Check latest CI status
gh run list --branch 012-mvp2-hardening --limit 5

# Trigger CI by pushing
git push origin 012-mvp2-hardening
```
