# Glossary

## Core Terms
- BorneMap: EV station discovery and management platform for Tunisia
- Station: a charging location exposed to drivers and managed by partners or admins
- Charger: a physical charger record belonging to a station
- Partner: organization that manages its own stations and chargers
- Driver: public or registered person browsing stations
- Admin: global platform manager

## Data Terms
- `inventory.station`: source of truth for station records
- `gis`: spatial support area, never the master record store
- `analytics`: dedicated schema for reporting data

## Interface Terms
- Driver Web: browser-based map experience for drivers
- Driver Mobile: mobile map experience for drivers
- Dashboard: partner and admin management interface

## Architecture Terms
- MVP: a complete, usable slice of the platform
- ADR: architecture decision record
- Class A bug: blocks correctness, security, or user access
- Class B bug: quality issue that must be fixed before the target MVP closes
- Class C bug: improvement or nice-to-have
