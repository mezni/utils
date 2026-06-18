# Sprint Backlog — Active: MVP-1 Spatial Validation

**Last Updated:** June 2026

---

## Sprint 1.1: Core Data & Storage Foundations
**Goal:** Dockerized PostGIS, schema DDL, OSM ingestion pipeline, spatial function.

- [x] **1.1.0** — Sprint 1.1 specification & analysis complete
- [x] **1.1.1** — Dockerize PostGIS 16 instance with customized spatial limits
- [x] **1.1.2** — Script schema migrations (`gis`, `inventory`) and NanoID validations
- [x] **1.1.3** — Build `osm-importer` pipeline loading Tunisia PBF map parameters
- [x] **1.1.4** — Implement native `gis.get_nearby_stations` PostGIS distance handler (moved from inventory schema)
- [x] **1.1.5** — Build inventory→GIS sync layer: `gis.osm_stations` mirror table, `inventory.sync_outbox` event outbox, trigger-captured station changes, `gis.process_sync_outbox()` worker

---

## Sprint 1.2: Rust Edge Engine Services
**Goal:** Actix-web driver-service, SQLx pooling, `/api/v1/nearby` endpoint, Traefik routing.

- [x] **1.2.1** — Setup Actix-web `driver-service` configuration structures
- [x] **1.2.2** — Build SQLx connection pooling logic with health diagnostics
- [x] **1.2.3** — Construct `/api/v1/nearby` coordinate stream controller
- [x] **1.2.4** — Configure Traefik gateway routing rules across the proxy layer

---

## Sprint 1.3: Mobile Driver App (Expo)
**Goal:** Expo SDK 54 mobile driver app with interactive map, station markers, and Tunisia viewport bounds.

- [x] **1.3.0** — Sprint 1.3 specification & planning (spec, plan, tasks, analysis all passed)
- [ ] **1.3.1** — Scaffold Expo SDK 54 project at `source/apps/mobile-driver/`
- [ ] **1.3.2** — Implement MapContainer with react-native-maps, Tunisia boundary constraints, custom markers
- [ ] **1.3.3** — Implement viewport debouncing (300ms) with AbortController and pull-to-refresh
- [ ] **1.3.4** — Implement shimmer loading, error boundary, and empty-state UI components
- [ ] **1.3.5** — Implement AsyncStorage offline cache with offline banner
- [ ] **1.3.6** — Implement macro-zoom overlay at zoom level < 8

---

## Sprint 1.4: Web Driver Client & Map Visualization
**Goal:** React + Leaflet web driver client with debounced viewport, shimmer loading, station drawer, and macro-zoom overlay.

- [ ] **1.4.0** — Sprint 1.4 specification & planning
- [ ] **1.4.1** — Scaffold web driver app at `source/apps/web-driver/` with Leaflet + Tailwind + React
- [ ] **1.4.2** — Implement MapContainer with Tunisia boundary constraints and custom charging pin markers
- [ ] **1.4.3** — Implement useDebounce hook (300ms) cancelling in-flight requests via AbortController
- [ ] **1.4.4** — Implement shimmer loading, error boundary, and empty-state UI components
- [ ] **1.4.5** — Implement macro-zoom overlay at zoom level < 8
- [ ] **1.4.6** — Implement StationDrawer premium sliding card with station details

---

## Future Sprints (Post MVP-1)
- **Sprint 2.x:** MVP-2 — Keycloak + Auth Service
- **Sprint 3.x:** MVP-3 — Admin Service + Dashboard
- **Sprint 4.x:** MVP-4 — Metadata Extensibility
- **Sprint 5.x:** MVP-5 — Analytics + Caching
- **Sprint 6.x:** MVP-6 — Production Hardening
