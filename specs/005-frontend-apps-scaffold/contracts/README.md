# Contracts — Frontend Apps Scaffold

This directory documents the API contracts between frontend apps and backend services.

## Files

| File | Description |
|------|-------------|
| `driver-service-api.md` | Contract for Driver Web and Driver Mobile consuming driver-service |
| `admin-service-api.md` | Contract for Dashboard consuming admin-service |

## Key Principles

- All endpoints use `/api/v1` prefix (Constitution Principle IX)
- Driver endpoints are public — no auth required (Constitution Principle VI)
- Admin endpoints are public in Sprint 1.5 — auth added in Phase 2
- Error responses follow consistent `{ "error": "..." }` format
