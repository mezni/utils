# ADR-021: Audit trail on all inventory tables

**Status:** Accepted
**Date:** 2026-06-09

## Context

Inventory data (partners, stations, chargers) is business-critical. When disputes arise or data is incorrect, the platform must be able to determine who created or last modified each record. Without audit fields, accountability is lost.

## Decision

Every inventory table carries four audit fields: `created_at` (immutable, set on insert), `created_by` (USR-... nullable), `updated_at` (updated on every write), `updated_by` (USR-... of last writer, nullable). These fields are set by the application (Admin Service), never by database triggers.

## Consequences

- Full accountability for all inventory changes
- Simple implementation — four consistent fields across all tables
- Field values are set by business logic, not DB defaults
- Slightly wider tables
- Querying audit trail requires no additional tables or joins
