# BorneMap — Architectural Foundations & Ecosystem Topology

## Overview

BorneMap is a high-performance, multi-tenant geospatial ecosystem localized to Tunisia. The platform implements a **modular monorepo pattern** under a root `sources/` workspace directory, structuring domain layers to easily split into standalone microservices.

## Ecosystem Topology

```
                        ┌────────────────────────┐
                        │   sources/backend/     │
                        │  (Modular Monorepo)   │
                        └───────────┬────────────┘
                                    │  [/api/v1/*]
         ┌──────────────────────────┼──────────────────────────┐
         ▼                          ▼                          ▼
┌──────────────────┐      ┌──────────────────┐      ┌──────────────────┐
│  1. Admin Web    │      │  2. Mobile App   │      │ 3. Partner Web   │
│    Portal        │      │    (Drivers)     │      │    Dashboard     │
├──────────────────┤      ├──────────────────┤      ├──────────────────┤
│ Full system data │      │ Map-centric,     │      │ Station telemetry│
│ auditing, config │      │ fast discovery,  │      │ manager, station │
│ control, types.  │      │ nearby searches. │      │ uptime, chargers.│
└──────────────────┘      └──────────────────┘      └──────────────────┘
```

### Client Applications

| # | Application | Purpose |
|---|------------|---------|
| 1 | **Admin Web Portal** | Full system data auditing, configuration control, type management |
| 2 | **Mobile App (Drivers)** | Map-centric fast discovery, nearby searches |
| 3 | **Partner Web Dashboard** | Station telemetry, manager, station uptime, chargers |

## Backend Stack

| Component | Technology | Role |
|-----------|-----------|------|
| Language | Rust | Compiled binary core |
| HTTP Framework | Actix-web | Asynchronous HTTP routing |
| SQL Driver | SQLx | Compile-time verified asynchronous SQL execution |
| Runtime | Tokio | Multi-threaded async runtime driver |

## API Version Control

All endpoints exposed by the API backend are mounted strictly behind a versioned resource namespace block:

```
/api/v1/*
```

No unversioned endpoints are permitted. All future breaking changes must introduce a new versioned namespace (e.g., `/api/v2/`).

## Database Management Engine

- **Engine**: PostgreSQL 16+
- **Spatial Extension**: PostGIS
- **Coordinate System**: SRID 4326 (Longitude-first geography points)

## Performance Invariant (SLO)

Critical geospatial discovery search routines (`ST_DWithin` / bounding boxes) must execute within a strict latency boundary:

> **≤ 200ms** under concurrent production application workloads.

This SLO applies to the `/api/v1/stations/nearby` endpoint and any future spatial discovery queries.
