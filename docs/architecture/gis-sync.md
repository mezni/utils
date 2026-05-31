# GIS Sync Architecture

## Overview

GIS data is always a derived projection of business state. No service
writes to the `gis` schema directly. The GIS Sync Worker consumes
outbox events from the `inventory` schema and updates spatial
projections.

## Flow

```text
Admin Service
  │
  ├── writes station to inventory.station
  ├── writes outbox event (same transaction)
  │
  ▼
PostgreSQL (commits)
  │
  ▼
GIS Sync Worker (polls outbox)
  │
  ├── reads event
  ├── transforms to GIS projection
  └── upserts gis schema
```

## Idempotency

Each event carries a `sync_version` that prevents duplicate processing.
If the same event is consumed twice, the GIS projection remains correct.
