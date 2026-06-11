# Backend Services

**Stack:** Rust (Actix-web), sqlx, tokio, serde, nanoid, tracing

---

## Service Overview

| Service | Port | Domain | DB Access |
|---|---|---|---|
| driver-service | 8080 | station discovery (read-only) | platform_db.inventory, platform_db.gis |
| admin-service | 8081 | station + partner management | platform_db.inventory |
| clickstream-service | 8082 | event ingestion | analytics_db |
| auth-gateway | MVP-3 | identity abstraction | Keycloak |

---

## Shared Crates

Shared Rust crates live under `source/crates/`, used by all backend services.

| Crate | Path | Description |
|---|---|---|
| ev-core | `source/crates/ev-core/` | Core EV domain types (Station, Charger, Partner, enums, validation) |
| ev-db | `source/crates/ev-db/` | PostgreSQL connectivity, pool config, migration runner |
| ev-auth | `source/crates/ev-auth/` | JWT validation, API key auth, RBAC types |

```
source/
├── crates/
│   ├── ev-core/
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs          ← CoreError, ConnectorType, StationStatus, PartnerType
│   │       ├── models/         ← Station, Charger, Partner
│   │       └── types/          ← GeoPoint, value objects
│   ├── ev-db/
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs          ← DbError, create_pool
│   │       ├── error.rs        ← DbError enum, From<sqlx::Error>
│   │       └── pool.rs         ← PgPool setup, DbConfig
│   └── ev-auth/
│       ├── Cargo.toml
│       └── src/
│           ├── lib.rs          ← AuthError
│           ├── error.rs        ← AuthError enum
│           ├── jwt.rs          ← JwtClaims, validate_claims
│           └── api_key.rs      ← ApiKey parsing
└── services/
    └── libs/
        └── borne-data/         ← legacy data layer (migrating to ev-core + ev-db)
```

---

## Driver Service (8080)

### Component Structure

```
source/services/driver-service/
├── src/
│   ├── main.rs
│   ├── lib.rs
│   ├── api/
│   │   └── v1/
│   │       ├── mod.rs
│   │       ├── stations.rs
│   │       └── health.rs
│   ├── handlers/
│   │   ├── station_handler.rs
│   │   ├── nearby_handler.rs
│   │   └── health_handler.rs
│   ├── dto/
│   │   ├── station_response.rs
│   │   ├── station_detail_response.rs
│   │   ├── nearby_query.rs
│   │   └── error_response.rs
│   ├── config/
│   │   └── settings.rs
│   ├── errors/
│   │   └── app_error.rs
│   └── telemetry/
│       └── middleware.rs
├── tests/
│   └── api_test.rs
├── Cargo.toml
└── Dockerfile
```

---

## Admin Service (8081)

### Component Structure

```
source/services/admin-service/
├── src/
│   ├── main.rs
│   ├── api/
│   │   └── v1/
│   │       ├── mod.rs
│   │       ├── partners.rs
│   │       ├── stations.rs
│   │       └── chargers.rs
│   ├── handlers/
│   │   ├── partner_handler.rs
│   │   ├── station_handler.rs
│   │   └── charger_handler.rs
│   ├── services/
│   │   ├── partner_service.rs
│   │   ├── station_admin_service.rs
│   │   └── rbac_service.rs
│   ├── repositories/
│   │   ├── partner_repository.rs
│   │   └── station_repository.rs
│   ├── dto/
│   │   ├── partner_request.rs
│   │   ├── station_request.rs
│   │   └── charger_request.rs
│   ├── middleware/
│   │   └── auth.rs
│   ├── config/
│   │   └── settings.rs
│   ├── errors/
│   │   └── app_error.rs
│   └── telemetry/
│       └── metrics.rs
├── tests/
│   ├── integration/
│   └── fixtures/
├── Cargo.toml
└── Dockerfile
```

---

## Clickstream Service (8082)

### Component Structure

```
source/services/clickstream-service/
├── src/
│   ├── main.rs
│   ├── api/
│   │   └── v1/
│   │       ├── mod.rs
│   │       └── events.rs
│   ├── handlers/
│   │   └── event_handler.rs
│   ├── services/
│   │   ├── event_ingestion_service.rs
│   │   └── event_validation_service.rs
│   ├── repositories/
│   │   └── event_store.rs
│   ├── dto/
│   │   └── event_payload.rs
│   ├── config/
│   │   └── settings.rs
│   ├── errors/
│   │   └── app_error.rs
│   └── telemetry/
│       └── metrics.rs
├── tests/
│   ├── integration/
│   └── fixtures/
├── Cargo.toml
└── Dockerfile
```

---

## Shared Rules

- **No cross-service calls** — services never call each other directly
- **No cross-service DB access** — database credentials scoped per service
- **JWT validation** — every request validated before processing (via `ev-auth`)
- **Partner scoping** — `WHERE partner_id = JWT.partner_id` enforced server-side
- **API prefix** — all endpoints under `/api/v1/`
