# BorneMap — API Contracts Index
**Version:** 1.0
**Date:** June 2026

---

## OpenAPI Specifications

| Service | Spec File | Base Path | Status |
|---|---|---|---|
| auth-service | `api/openapi/identity.yaml` | `/api/v1/auth` | Draft |
| driver-service | `api/openapi/driver.yaml` | `/api/v1/driver` | Draft |
| admin-service | `api/openapi/admin.yaml` | `/api/v1/admin` | Draft |
| Shared schemas | `api/openapi/shared.yaml` | — | Draft |

---

## Route Summary

### auth-service (`/api/v1/auth`)

| Method | Path | Auth | Description |
|---|---|---|---|
| POST | `/register` | None | Register new user |
| POST | `/login` | None | Login with email + password |
| POST | `/refresh` | None | Refresh access token |
| GET | `/me` | JWT | Get current user profile |
| PUT | `/me` | JWT | Update user profile |
| PUT | `/password` | JWT | Change password |

### driver-service (`/api/v1/driver`)

| Method | Path | Auth | Description |
|---|---|---|---|
| GET | `/stations` | None | Browse charging stations |
| GET | `/stations/:id` | None | Get station details |
| GET | `/stations/nearby` | None | Find nearby stations |
| GET | `/stations/:id/chargers` | None | List chargers at station |
| POST | `/favorites` | JWT | Add station to favorites |
| DELETE | `/favorites/:stationId` | JWT | Remove station from favorites |
| GET | `/favorites` | JWT | List favorite stations |

### admin-service (`/api/v1/admin`)

| Method | Path | Auth | Roles |
|---|---|---|---|
| POST | `/stations` | JWT | partner, admin |
| PUT | `/stations/:id` | JWT | partner, admin |
| DELETE | `/stations/:id` | JWT | admin |
| GET | `/stations` | JWT | partner, admin |
| GET | `/stations/:id` | JWT | partner, admin |
| POST | `/partners` | JWT | admin |
| PUT | `/partners/:id` | JWT | admin |
| GET | `/partners` | JWT | admin |
| GET | `/audit` | JWT | admin |
| POST | `/stations/refresh-materialized-views` | JWT | admin |

---

## Contract-First Enforcement

- All implementation must be preceded by OpenAPI spec
- OpenAPI files become immutable at IMPLEMENTATION phase start
- CI enforces route ↔ spec parity on every commit
- See `constitution/guardrails.md` Section 5 for details
