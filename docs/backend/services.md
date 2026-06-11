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

## Driver Service (8080)

### Component Structure

```
source/services/driver-service/
├── src/
│   ├── main.rs
│   ├── api/
│   │   └── v1/
│   │       ├── mod.rs
│   │       ├── stations.rs
│   │       └── nearby.rs
│   ├── handlers/
│   │   ├── station_handler.rs
│   │   └── nearby_handler.rs
│   ├── services/
│   │   ├── geo_search_service.rs
│   │   └── station_query_service.rs
│   ├── repositories/
│   │   ├── station_repository.rs
│   │   └── gis_repository.rs
│   ├── models/
│   │   ├── station.rs
│   │   └── charger.rs
│   ├── dto/
│   │   ├── station_response.rs
│   │   └── nearby_query.rs
│   ├── db/
│   │   └── pool.rs
│   ├── middleware/
│   │   └── logging.rs
│   ├── config/
│   │   └── settings.rs
│   ├── errors/
│   │   └── app_error.rs
│   └── telemetry/
│       └── metrics.rs
├── tests/
│   ├── integration/
│   │   ├── station_tests.rs
│   │   └── nearby_tests.rs
│   └── fixtures/
│       └── seed.rs
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
│   ├── models/
│   │   ├── partner.rs
│   │   ├── station.rs
│   │   └── charger.rs
│   ├── dto/
│   │   ├── partner_request.rs
│   │   ├── station_request.rs
│   │   └── charger_request.rs
│   ├── db/
│   │   └── pool.rs
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
│   ├── models/
│   │   └── raw_event.rs
│   ├── dto/
│   │   └── event_payload.rs
│   ├── db/
│   │   └── pool.rs
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
- **JWT validation** — every request validated before processing
- **Partner scoping** — `WHERE partner_id = JWT.partner_id` enforced server-side
- **API prefix** — all endpoints under `/api/v1/`
