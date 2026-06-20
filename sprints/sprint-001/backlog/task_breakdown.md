# Sprint 001 — Task Breakdown

## Phase 1: Setup & Docker Compose

- T001 Rust service scaffolds
- T002 Node.js web app scaffold
- T003 SQL migrations directory
- T004 Docker Compose config
- T005 Dockerfiles
- T006 PostGIS init script
- T007 Rust workspace
- T008 nanoid utility
- T009 DB connection pool

## Phase 2: OSM → GIS Ingestion

- T010 OSM staging table
- T011 OSM Overpass fetcher
- T012 OSM parser
- T013 import-osm.sh script
- T014 import error handling

## Phase 3: Inventory Schema

- T015 Partners table
- T016 Stations table
- T017 Chargers table
- T018 Connectors table
- T019 Sync jobs table
- T020 Lookup tables
- T021 GiST index on location
- T022 FK cascade enforcement

## Phase 4: Sync System + Nearby

- T023 mv_stations_geo materialized view
- T024 find_nearby_stations function
- T025 sync_pipeline module
- T026 idempotent upsert logic
- T027 sync_jobs audit trail
- T028 refresh-mv.sh
- T029 seed data script
- T030 validate query performance

## Phase 5: Driver Service API

- T031 GET /health endpoint
- T032 GET /nearby endpoint
- T033 latency tracking middleware
- T034 error handling middleware
- T035 API client service
- T036 route handlers
- T037 structured logging
- T038 validate API SLA

## Phase 6: Driver Web App

- T039 MapView component
- T040 StationList component
- T041 StationDetail component
- T042 wire components in App.tsx
- T043 handle empty/loading states
- T044 handle 404 and no chargers
- T045 distance indicators
- T046 handle geolocation denial
- T047 CORS configuration
- T048 seed-dev.sh script

---

## Summary

| Phase | Tasks | Parallelizable |
|-------|-------|----------------|
| 1 | 9 | 8 |
| 2 | 5 | 4 |
| 3 | 8 | 5 |
| 4 | 8 | 3 |
| 5 | 8 | 4 |
| 6 | 10 | 3 |
| **Total** | **48** | **27** |
