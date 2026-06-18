# Sprint Backlog — Active: MVP-1 Spatial Validation

**Last Updated:** June 2026

---

## Sprint 1.1: Core Data & Storage Foundations
**Goal:** Dockerized PostGIS, schema DDL, OSM ingestion pipeline, spatial function.

- [x] **1.1.0** — Sprint 1.1 specification & analysis complete
- [ ] **1.1.1** — Dockerize PostGIS 16 instance with customized spatial limits
- [ ] **1.1.2** — Script schema migrations (`gis`, `inventory`) and NanoID validations
- [ ] **1.1.3** — Build `osm-importer` pipeline loading Tunisia PBF map parameters
- [ ] **1.1.4** — Implement native `get_nearby_stations` PostGIS distance handler

---

## Sprint 1.2: Rust Edge Engine Services
**Goal:** Actix-web driver-service, SQLx pooling, `/api/v1/nearby` endpoint, Traefik routing.

- [ ] **1.2.1** — Setup Actix-web `driver-service` configuration structures
- [ ] **1.2.2** — Build SQLx connection pooling logic with health diagnostics
- [ ] **1.2.3** — Construct `/api/v1/nearby` coordinate stream controller
- [ ] **1.2.4** — Configure Traefik gateway routing rules across the proxy layer

---

## Sprint 1.3: Premium Front-End GIS Viewports
**Goal:** Expo mobile and React/Leaflet web clients with shared hooks, markers, and UX states.

- [ ] **1.3.1** — Scaffold Expo SDK 54 shell with coordinate lock restrictions
- [ ] **1.3.2** — Wire Leaflet web component client using shared layout elements
- [ ] **1.3.3** — Build unified map markers, loading skeletons, and out-of-bounds indicators

---

## Future Sprints (Post MVP-1)
- **Sprint 2.x:** MVP-2 — Keycloak + Auth Service
- **Sprint 3.x:** MVP-3 — Admin Service + Dashboard
- **Sprint 4.x:** MVP-4 — Metadata Extensibility
- **Sprint 5.x:** MVP-5 — Analytics + Caching
- **Sprint 6.x:** MVP-6 — Production Hardening
