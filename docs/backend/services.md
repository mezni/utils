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
driver-service/
├── src/
│   ├── main.rs
│   ├── controllers/
│   │   ├── station_controller.rs
│   │   └── nearby_controller.rs
│   ├── services/
│   │   ├── geo_search_service.rs
│   │   └── station_query_service.rs
│   ├── repositories/
│   │   ├── station_repository.rs
│   │   └── gis_repository.rs
│   └── domain/
│       ├── station.rs
│       └── charger.rs
```

---

## Admin Service (8081)

### Component Structure

```
admin-service/
├── src/
│   ├── main.rs
│   ├── controllers/
│   │   ├── station_admin_controller.rs
│   │   └── partner_controller.rs
│   ├── services/
│   │   ├── partner_service.rs
│   │   ├── station_admin_service.rs
│   │   └── rbac_service.rs
│   ├── repositories/
│   │   ├── partner_repository.rs
│   │   └── station_repository.rs
│   └── domain/
│       ├── partner.rs
│       ├── station.rs
│       └── charger.rs
```

---

## Clickstream Service (8082)

### Component Structure

```
clickstream-service/
├── src/
│   ├── main.rs
│   ├── controllers/
│   │   └── event_controller.rs
│   ├── services/
│   │   ├── event_ingestion_service.rs
│   │   └── event_validation_service.rs
│   ├── repositories/
│   │   └── event_store.rs
│   └── domain/
│       └── raw_event.rs
```

---

## Shared Rules

- **No cross-service calls** — services never call each other directly
- **No cross-service DB access** — database credentials scoped per service
- **JWT validation** — every request validated before processing
- **Partner scoping** — `WHERE partner_id = JWT.partner_id` enforced server-side
- **API prefix** — all endpoints under `/api/v1/`
