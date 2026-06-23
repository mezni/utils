# OBSERVABILITY SPECIFICATION

---

## 1. Logging Rules

- structured logging required
- JSON-compatible logs preferred
- all logs MUST include request_id

---

## 2. Tracing Rules

- every request MUST have correlation ID
- trace must propagate across layers

---

## 3. Metrics (optional expansion later)

- request latency
- error rate
- DB query time
