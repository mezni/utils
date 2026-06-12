# ADR-004: Clickstream Events in Admin Service (No Dedicated Service)

**Status:** Accepted  
**Date:** 2026-06-10  
**Authors:** Claude Code, Claude (chat)

---

## Context

BorneMap needs to capture user interaction events (clickstream) for analytics:
- `station_viewed` — user opens station detail
- `station_searched` — user initiates search
- `nearby_searched` — user triggers geolocation search
- `map_panned`, `map_zoomed` — map interaction
- `charger_detail_viewed` — user inspects charger info

Initially, we considered a **dedicated clickstream service** with:
- Dedicated `/api/v1/events` endpoint
- Queue-based event processing
- Separate deployment pipeline

However, analysis showed:

1. **Event ingestion is write-heavy, not read-heavy** — no need for a query service
2. **Dashboard is the only client** — not high volume initially (MVP-1: <100 events/sec)
3. **Events are analytics, not core business logic** — secondary to discovery
4. **Microservice overhead** — adds complexity without benefit

---

## Decision

**Implement event ingestion in the admin-service, not a dedicated service.**

Events endpoints live in **admin-service**:
- `POST /api/v1/events` — single event
- `POST /api/v1/events/batch` — up to 100 events

Both endpoints:
- Accept JSON events
- Write to `analytics_db.raw_events` (append-only)
- Return `202 Accepted` (fire-and-forget)
- Do not block on write completion

Event processing (transformation, aggregation) happens **asynchronously** via:
- Background job queue (MVP-2+, optional)
- Scheduled batch processor (future)
- Real-time streaming (MVP-4+, future)

---

## Rationale

### Operational Simplicity
Two services (driver + admin) is simpler than three. Admin service already manages infrastructure (partners, stations), so adding event ingestion is a natural extension.

### Write-Heavy Workload
Event ingestion is append-only, no complex reads. A dedicated service adds zero value.

### Cost-Effective Scaling
- MVP-1: Single admin-service handles thousands of events/sec
- If volume grows (MVP-4+), split into dedicated service **without code migration**

### Architectural Cleanliness
Events are **analytics data**, not inventory. Keeping them in admin-service maintains the separation:
- Driver service = discovery (read inventory + gis)
- Admin service = management (write inventory) + instrumentation (write analytics)

### Future Flexibility
If event throughput becomes a bottleneck (>10k/sec), extract to dedicated service:
1. Create `source/analytics-service/`
2. Move event endpoints there
3. Admin-service proxies events to analytics-service
4. No client code changes (routing via Traefik)

---

## Consequences

### Positive
- **Simplicity:** One less service to deploy, configure, monitor
- **Latency:** Events written to analytics_db in same request as station updates
- **Coupling:** Natural — both operations touch the same transaction boundary
- **Staffing:** Fewer services = lower operational overhead

### Negative
- **Scalability:** Single admin-service may become bottleneck at very high event volume (unlikely MVP-1/2)
- **Lifecycle coupling:** If admin-service goes down, event writes fail (acceptable trade-off)

### Scaling Path (if needed)
When event volume exceeds 10k/sec:
1. Extract analytics-service (file ADR-XXX-analytics-service)
2. Admin-service writes to message queue (Kafka, Redis)
3. Analytics-service consumes queue asynchronously
4. Traefik routes `/api/v1/events` to new service

---

## Implementation Notes

1. **Admin-service event handlers:**
   ```
   src/handlers/
   ├── events.rs       ← POST /api/v1/events
   ├── events_batch.rs ← POST /api/v1/events/batch
   ```

2. **Database:**
   ```sql
   -- analytics_db.raw_events
   CREATE TABLE raw_events (
       id BIGSERIAL PRIMARY KEY,
       event_type VARCHAR(50) NOT NULL,
       session_id VARCHAR(50),
       user_id VARCHAR(50),
       payload JSONB,
       occurred_at TIMESTAMP NOT NULL,
       ingested_at TIMESTAMP DEFAULT NOW() NOT NULL
   );
   -- Append-only: no UPDATE, no DELETE triggers
   ```

3. **Response shape:**
   ```json
   // Single event (202 Accepted)
   { "accepted": true }

   // Batch (202 Accepted)
   { "accepted": 95, "rejected": 5 }
   ```

4. **Error handling:**
   - Invalid event_type → reject (include in rejected count)
   - Database unavailable → return 503 (client retries)
   - Oversized batch (>100 events) → 400 Bad Request

---

## Related ADRs

- ADR-001: Traefik gateway (routes events to admin-service)
- ADR-002: Rust + Actix (backend framework for event handlers)

---

## References

- [Analytics database schema](../../database/analytics-db-schema.md)
- [API contract — event endpoints](../../api-contract.md)
