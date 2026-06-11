# Local Development Setup (MVP-1)

**Stack:** Docker Compose + Rust (cargo run)

---

## Services

```yaml
services:
  platform-db:
    image: postgis/postgis:16-3.4
    ports: ["5432:5432"]
    environment:
      POSTGRES_DB: platform_db

  analytics-db:
    image: postgres:16
    ports: ["5433:5432"]
    environment:
      POSTGRES_DB: analytics_db

  keycloak:
    image: quay.io/keycloak/keycloak:24.0
    ports: ["8083:8080"]
    environment:
      KC_DB: postgres
      KC_DB_URL: jdbc:postgresql://keycloak-db:5432/keycloak_db

  keycloak-db:
    image: postgres:16
    environment:
      POSTGRES_DB: keycloak_db
```

---

## Running Locally

```bash
# Start infrastructure
docker compose up -d platform-db analytics-db keycloak

# Run driver service
cd source/services/driver-service && cargo run

# Run admin service
cd source/services/admin-service && cargo run

# Run clickstream service
cd source/services/clickstream-service && cargo run

# Run mobile app
cd source/front/mobile-driver && npx expo start
```

---

## Network

```
localhost:5432  → platform_db
localhost:5433  → analytics_db
localhost:5432  → platform_db
localhost:5433  → analytics_db
localhost:8083  → keycloak (internal)
localhost:8080  → driver-service (direct)
localhost:8081  → admin-service (direct)
localhost:8082  → clickstream-service (direct)
```

---

## Database Initialization

- Run migrations on startup (sqlx migrate)
- Seed data: sample partner → station → charger
- PostGIS enabled on platform_db by default
