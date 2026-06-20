# Sprint 001 — EV Charging Platform Foundation

**Sprint ID**: sprint-001
**Status**: Active
**Phase**: FOUNDATION

## Goal

Deliver a fully functional geospatial EV charging backbone including database foundation,
OpenStreetMap ingestion, inventory domain model, sync engine, nearby query system,
driver API service, and minimal driver web application.

## User Stories

### P1 — Driver finds nearby charging stations
As a driver, I want to find EV charging stations near my current location so that
I can quickly locate the closest place to charge my vehicle.

### P2 — Partner manages charging station inventory
As a charging station operator, I want to register and manage my stations, chargers,
and connectors so that drivers can discover and use my infrastructure.

### P3 — System operator imports geospatial data
As a system operator, I want to import charging station data from public geospatial
sources so that the platform can bootstrap its coverage.

### P4 — Driver views station details
As a driver, I want to see detailed information about a specific charging station
so that I can determine whether it meets my vehicle's requirements before driving there.

## Key Entities

- Partner → Station → Charger → Connector (strict hierarchy)
- Sync Job (audit trail for imports)

## Deliverables

1. Database schema (EV inventory + GIS layer)
2. OSM ingestion pipeline
3. Sync engine foundation
4. Nearby station query function
5. Driver REST API
6. Driver web map application
7. Docker Compose infrastructure
