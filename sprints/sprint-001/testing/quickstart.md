# Sprint 001 — Quickstart & Testing

## Prerequisites
- Docker 24+ and Docker Compose v2
- Rust 1.85+ (for service development)
- Node.js 22+ (for web app development)

## Setup
```bash
docker compose -f docker/docker-compose.yml up -d
```

## Testing By Story

### US1 (P1) — Nearby Search
```bash
# Pre-load test stations, then:
curl "http://localhost:3001/api/v1/driver/nearby?lat=36.8065&lon=10.1815&radius=5000"
# Verify: stations sorted by distance, power tier present
```

### US2 (P2) — Partner Inventory
```bash
# Create partner, add station with chargers
curl -X POST http://localhost:3002/api/v1/admin/stations \
  -H "Content-Type: application/json" \
  -d '{"partner_id":"PAR-...","name":"Test Station","lat":36.8,"lon":10.18}'
# Verify: station appears in /nearby results
```

### US3 (P3) — OSM Import
```bash
bash scripts/import-osm.sh
# Re-run — verify zero duplicates
psql -d platform_db -c "SELECT COUNT(*) FROM stations;"
```

### US4 (P4) — Station Detail
```bash
curl "http://localhost:3001/api/v1/driver/stations/STA-abc123def456"
# Verify: chargers, connectors, statuses all present
```
