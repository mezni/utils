# Logging

**Framework:** tracing (Rust)
**Format:** Structured JSON

---

## Rules

- All logs structured JSON (no plain text)
- No sensitive data in logs (passwords, tokens, PII)
- Correlation ID per request (trace_id)
- Log levels: error, warn, info, debug, trace

---

## Standard Fields

```json
{
  "timestamp": "2026-06-10T12:00:00Z",
  "level": "info",
  "service": "driver-service",
  "trace_id": "req-abc123",
  "message": "station queried",
  "latency_ms": 42,
  "station_id": "STA-abc123def456"
}
```

---

## Per-Service

| Service | Key Events |
|---|---|
| driver-service | station queries, nearby searches, errors |
| admin-service | CRUD operations, auth checks, partner changes |
| clickstream-service | event validation, batch inserts, rejections |
| auth-gateway | login attempts, token refresh, realm routing |
