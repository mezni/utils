# Sprint Backlog — BorneMap MVP-1

## Sprint Goal

Deliver end-to-end EV charging station discovery for Tunisia: OSM data → PostGIS → Rust API → Mobile map.

## Story Points: 34

| ID | Task | Phase | Points | Dependencies |
|----|------|-------|--------|-------------|
| A1 | Create platform-init.sql | Database | 3 | — |
| A2 | Create functions.sql | Database | 3 | A1 |
| A3 | Create triggers.sql | Database | 2 | A1 |
| B1 | Implement geo-core lib | Libraries | 1 | — |
| B2 | Implement db-core lib | Libraries | 1 | — |
| B3 | Implement services-shared | Libraries | 3 | B1, B2 |
| C1 | Driver service skeleton | Service | 3 | B2, B3 |
| C2 | Health endpoint | Service | 1 | C1 |
| C3 | Stations nearby endpoint | Service | 5 | C1, A2 |
| D1 | OSM import script | Import | 3 | A1 |
| D2 | Seed data script | Import | 2 | A1 |
| E1 | Shared mobile constants | Mobile Lib | 1 | — |
| E2 | Shared mobile types | Mobile Lib | 1 | — |
| E3 | Shared mobile index + util | Mobile Lib | 1 | — |
| F1 | Expo project init | Mobile App | 2 | E1-E3 |
| F2 | API service layer | Mobile App | 2 | F1, C3 |
| F3 | Map screen with markers | Mobile App | 5 | F2 |
| G1 | Dockerfiles | Infra | 2 | C1 |
| G2 | docker-compose.yml | Infra | 2 | G1, A1 |
| G3 | Traefik config | Infra | 1 | G2 |
| G4 | .env.example + Makefile | Infra | 1 | G2 |

## Definition of Done

- [ ] SQL migrations run cleanly
- [ ] `cargo check` passes for all Rust crates
- [ ] `curl` against API returns correct JSON
- [ ] OSM import script populates database
- [ ] Mobile app displays stations on map
- [ ] `docker compose up` starts full stack
