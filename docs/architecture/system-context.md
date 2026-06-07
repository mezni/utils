# System Context

**Phase**: 1 — Foundation
**Related Tasks**: TASK-01 through TASK-58
**Last Updated**: 2026-06-07

---

## C4 Model — Context Diagram (Level 1)

```
                    ┌──────────────────────────────────────┐
                    │         Public Driver                │
                    │   (Anonymous, no login required)     │
                    └──────────────┬──────────────────────-┘
                                   │
                                   │ HTTPS
                                   ▼
                    ┌──────────────────────────────┐
                    │                              │
                    │         Traefik              │
                    │      (Edge Router)           │
                    │   Ports 80 / 443             │
                    │   TLS termination            │
                    │   Rate limiting              │
                    └──────┬──────────────┬───────-┘
                           │              │
                    ┌──────▼──────┐ ┌─────▼──────┐
                    │  Driver     │ │  Admin     │
                    │  Service    │ │  Service   │
                    │  :8080      │ │  :8081     │
                    │  /api/v1/*     │ │  /api/v1/*    │
                    └──────┬──────┘ └─────┬──────┘
                           │              │
                           └──────┬───────┘
                                  │
                                  ▼
                    ┌──────────────────────────────┐
                    │     PostgreSQL 16 + PostGIS  │
                    │                              │
                    │  ┌──────────┐ ┌──────────┐  │
                    │  │inventory │ │   gis    │  │
                    │  │ schema   │ │  schema  │  │
                    │  └──────────┘ └──────────┘  │
                    │  ┌──────────┐ ┌──────────┐  │
                    │  │  users   │ │analytics │  │
                    │  │  schema  │ │  schema  │  │
                    │  └──────────┘ └──────────┘  │
                    └──────────────────────────────┘
```

## External Systems (Future)

| System | Phase | Interaction |
|---|---|---|
| Keycloak (Auth) | Phase 2 | JWT issuance, token validation, role management |
| OSM Data Import | Phase 6 | Populates gis.osm_nodes, gis.roads, etc. |
| Email provider | Future | Account verification, notifications |
| Google/Facebook OAuth | Future | Social login via Keycloak |

## Users

| Persona | Role | Description |
|---|---|---|
| Public Driver | Anonymous | Browses stations on map, no login required |
| Registered Driver | registered_driver | Authenticated, manages favorites and reviews |
| Partner | partner | Manages own stations and chargers via dashboard |
| Admin | admin | Global platform management via dashboard |

---

## Related Tasks

- TASK-01 through TASK-14: Monorepo and CI/CD
- TASK-15 through TASK-24: Database schemas
- TASK-25 through TASK-32: Driver Service
- TASK-33 through TASK-42: Admin Service
- TASK-43 through TASK-51: Frontend Apps
