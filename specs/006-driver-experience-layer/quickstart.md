# Quickstart Guide: Driver Experience Layer

## Setup

```bash
# Ensure all services are running
docker-compose up -d

# Install frontend dependencies
cd apps/mobile && npm install
cd apps/web && npm install

# Run database migrations (no new migrations for Sprint 5)
# All personalization uses existing users.preferences JSONB

# Start driver-service
cd services/driver-service && cargo run
```

## Testing Scenarios

### Scenario 1: Favorites CRUD

```bash
# Add a favorite
curl -X POST http://localhost:3001/api/v1/driver/favorites \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"station_id": "STA-abc123def456"}'

# List favorites
curl http://localhost:3001/api/v1/driver/favorites \
  -H "Authorization: Bearer $TOKEN"

# Remove a favorite
curl -X DELETE http://localhost:3001/api/v1/driver/favorites \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"station_id": "STA-abc123def456"}'
```

### Scenario 2: Fuzzy Search

```bash
# Search by name (fuzzy match)
curl "http://localhost:3001/api/v1/driver/search?q=fast+charg" \
  -H "Authorization: Bearer $TOKEN"

# Search by address
curl "http://localhost:3001/api/v1/driver/search?q=Bern+Bahnhof" \
  -H "Authorization: Bearer $TOKEN"

# Search with location for distance sorting
curl "http://localhost:3001/api/v1/driver/search?q=charging&lat=46.948&lng=7.4474" \
  -H "Authorization: Bearer $TOKEN"
```

### Scenario 3: Preferences

```bash
# Get preferences
curl http://localhost:3000/api/v1/auth/preferences \
  -H "Authorization: Bearer $TOKEN"

# Update preferences
curl -X PUT http://localhost:3000/api/v1/auth/preferences \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"connector_type": "CCS", "max_distance": 25}'

# Partial update
curl -X PATCH http://localhost:3000/api/v1/auth/preferences \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"connector_type": "Type2"}'
```

### Scenario 4: Offline Mode

```bash
# 1. Load app with connectivity — browse map, favorite stations
# 2. Enable airplane mode on device
# 3. Verify:
#    - Favorites list is accessible
#    - Previously viewed map tiles display
#    - Cached stations appear
#    - App does not crash or show blank screens
# 4. Disable airplane mode
# 5. Verify:
#    - Pending changes sync automatically
#    - Fresh data loads in background
```

### Scenario 5: Session Continuity

```bash
# 1. Open app, set filter to "CCS only"
# 2. Navigate to Favorites section
# 3. Close app (force close)
# 4. Reopen app within 30 minutes
# 5. Verify:
#    - Map restores previous position
#    - "CCS only" filter is still active
#    - Favorites section is the active view
```

### Scenario 6: Optimistic UI

```bash
# 1. Open app, navigate to station list
# 2. Tap favorite heart on a station
# 3. Verify: heart fills within 150ms (before server confirms)
# 4. Simulate slow network (use devtools throttling)
# 5. Tap unfavorite
# 6. Verify: heart un-fills immediately (optimistic revert on error)
```

### Scenario 7: Telemetry Verification

```bash
# Check analytics_db for telemetry events
psql -h localhost -U bornemap -d analytics_db -c "
SELECT event_type, COUNT(*) as count
FROM raw_events
WHERE event_type IN ('FAVORITE_ADDED', 'FAVORITE_REMOVED', 'SEARCH_EXECUTED', 'SEARCH_SELECTED', 'FILTER_CHANGED', 'OFFLINE_MODE_ENTERED')
GROUP BY event_type;
"
```

## CI Gates

```bash
# Run all Sprint 5 CI gates
bash .specify/ci-gates/023-preferences-isolation.sh
bash .specify/ci-gates/024-offline-storage.sh
bash .specify/ci-gates/025-search-safety.sh
bash .specify/ci-gates/026-ui-boundary.sh
bash .specify/ci-gates/027-performance-regression.sh
```

## Verification Checklist

- [ ] Favorites POST/GET/DELETE return correct responses
- [ ] Favorites persist across app restart
- [ ] Preferences read/write/partial update work
- [ ] Search returns fuzzy matches for typos
- [ ] Search works offline against cache
- [ ] Skeleton placeholders appear <150ms
- [ ] Optimistic UI updates <150ms
- [ ] Optimistic rollback on error
- [ ] Session state restored on re-open
- [ ] Authentication session unaffected
- [ ] Offline mode: cached data available, no backend dependency
- [ ] Offline → online sync: pending writes applied
- [ ] Telemetry events visible in analytics_db
- [ ] All CI gates pass
