# ADR-003: Outbox Pattern for GIS Sync

## Status

Accepted

## Context

GIS projections must stay in sync with business data in the `inventory`
schema. Direct dual-writes risk inconsistency if one write fails.

## Decision

Use the transactional outbox pattern. When the Admin Service writes a
station change, it also writes an outbox event in the same database
transaction. The GIS Sync Worker polls the outbox and processes events
idempotently.

## Consequences

- Strong consistency between inventory and outbox
- Reliable GIS projection updates
- GIS Sync Worker must handle at-least-once delivery
- Idempotency via `station_id` + `sync_version`
- Additional storage for outbox table
