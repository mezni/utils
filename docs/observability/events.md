# Clickstream Events

**Event Schema:** See `/docs/database/analytics-db.md`

---

## Event Flow

```
Mobile/Web App
  → Clickstream Service (validation)
    → analytics_db.raw_events
      → aggregation jobs (future)
        → analytics dashboards
```

---

## Standard Payload

```json
{
  "event_name": "station_view",
  "user_id": "USR-abc123def456",
  "session_id": "sess-abc123",
  "payload": {
    "station_id": "STA-abc123def456",
    "latency_ms": 120,
    "source": "map"
  }
}
```

---

## Ingestion Rules

- Fire-and-forget (must not block UX)
- Batch endpoint for offline/mobile scenarios
- Server-side enrichment (adds IP-based geo, user agent)
- Rate limiting per user/session
- Validation before write (event_name required, payload is JSONB)
