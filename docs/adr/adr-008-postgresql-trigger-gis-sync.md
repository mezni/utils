# ADR-008: PostgreSQL trigger for GIS sync (from MVP-4)

**Status:** Accepted
**Date:** 2026-06-09

## Context

Station coordinates are authored in `inventory.station` (source of truth). GIS spatial queries need PostGIS geometry. Two approaches: application-level sync (service writes to both tables) or database-level sync (trigger keeps GIS layer in sync).

## Decision

Use a PostgreSQL trigger on `inventory.station` to automatically sync coordinates to `gis.station_locations`. The trigger fires on INSERT, UPDATE, and DELETE within the same transaction. GIS failure logs a WARNING but does not block the station write.

## Consequences

- Application code never needs to know about the GIS layer
- Sync is transactional and consistent
- GIS failures are non-blocking (station write always succeeds)
- Trigger adds a small overhead to every station write
- Resync function available for rebuilding all GIS artifacts
