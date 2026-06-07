# ADR-008: PostgreSQL Trigger for GIS Synchronization

**Status**: Accepted
**Date**: 2026-06-07

## Context

When a station is created or updated in inventory.station, GIS artifacts (gis.station_locations) need to be synchronized — computing geometry, finding nearest road, determining administrative region. Options: application-level sync (service writes to both schemas), a worker consuming events, or a database trigger.

## Decision

Use a PostgreSQL trigger function on inventory.station to synchronize gis.station_locations.

## Rationale

- GIS sync is atomic with the station write — same transaction
- No application service needs knowledge of the gis schema (simplifying service code)
- No worker infrastructure needed
- A trigger failure logs WARNING but does not block the station write (resilient)

## Consequences

- GIS sync logic lives in PL/pgSQL, not Rust code
- Testing trigger behavior requires integration tests
- The trigger function must be created in a migration
- Spatial indexes must exist before the trigger is active

## Compliance

- No application code writes to gis.station_locations directly
- A failed sync logs a WARNING — it never rolls back the station write
- gis.resync_all_stations() procedure available for manual recovery
