# ADR-004: Direct Analytics Insert over RabbitMQ

**Status**: Accepted
**Date**: 2026-06-07

## Context

Analytics events (clickstream, page views, searches) need to be collected from frontend applications and persisted. Options: direct HTTP POST to a service, or publish to a message queue (RabbitMQ) consumed by a worker.

## Decision

Use direct HTTP POST to the Clickstream Service, which writes directly to PostgreSQL. No message queue.

## Rationale

- Current scale does not justify a message broker (Principle 5: Build for current scale)
- Removing RabbitMQ eliminates an entire class of operational complexity (clustering, queue management, consumer failures)
- The Clickstream Service is a lightweight HTTP endpoint — no worker infrastructure needed
- PostgreSQL can handle the write volume at current scale
- ADR-001 (single database) means no cross-database consistency concerns

## Consequences

- Analytics writes compete with business data writes for database I/O
- No built-in retry or dead-letter mechanism — failed writes are lost
- Event volume must be monitored; if it grows significantly, RabbitMQ or similar may be needed (requires new ADR)
- The Clickstream Service must be horizontally scalable (future concern)

## Compliance

- Clickstream Service writes only to analytics schema
- Event taxonomy is enforced — unknown events rejected with HTTP 400
- Frontend API client swallows errors silently (analytics must never break the UI)
