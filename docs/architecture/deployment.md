# Deployment Architecture

---

## MVP-1 (Local Development)

```
┌──────────────────────────────┐
│        Developer Machine     │
├──────────────────────────────┤
│ Driver Service (8080)        │
│ Clickstream Service (8082)   │
│ PostgreSQL (platform_db)     │
│ PostgreSQL (analytics_db)    │
│ Keycloak (internal)          │
└──────────────────────────────┘
```

**Stack:** Docker Compose
- PostgreSQL containers (platform_db, analytics_db)
- Keycloak container (internal only)
- Rust services run locally (cargo run)
- No Traefik required

---

## MVP-6 (Production)

```
                    ┌────────────────────────────┐
                    │         INTERNET           │
                    └────────────┬───────────────┘
                                 │
                          ┌──────▼───────┐
                          │   TRAEFIK    │
                          │  (Ingress)   │
                          └──────┬───────┘
                                 │
        ┌────────────────────────┼────────────────────────┐
        │                        │                        │

┌───────▼────────┐   ┌──────────▼──────────┐   ┌──────────▼──────────┐
│ Driver Service │   │ Admin Service       │   │ Clickstream Service │
│ (Rust 8080)    │   │ (Rust 8081)         │   │ (Rust 8082)         │
└───────┬────────┘   └──────────┬──────────┘   └──────────┬──────────┘
        │                       │                        │
        └──────────────┬────────┴──────────┬─────────────┘
                       ▼                   ▼

        ┌──────────────────────────────────────────────┐
        │                DATA LAYER                   │
        ├──────────────────────────────────────────────┤
        │ platform_db   (PostGIS + PostgreSQL)        │
        │ analytics_db  (append-only events)          │
        │ keycloak_db   (internal identity store)     │
        └──────────────────────────────────────────────┘
```

---

## Deployment Flow

```
Git Push → CI Pipeline → Docker Build → Registry → Deploy → Health Check → Traefik Route Update
```

---

## Environment Matrix

| Environment | Services | Traefik | Keycloak | Databases |
|---|---|---|---|---|
| local | cargo run | no | Docker | Docker |
| staging | Docker | optional | Docker | Docker |
| production | Docker | yes | internal | managed |
