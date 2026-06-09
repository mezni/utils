# ADR-004: Direct analytics insert over message broker

**Status:** Accepted
**Date:** 2026-06-09

## Context

Analytics events need to be collected from all frontend applications and stored in PostgreSQL. Message brokers (Kafka, RabbitMQ) add operational complexity, require additional infrastructure, and are disproportionate for the expected event volume in early MVPs.

## Decision

Use direct PostgreSQL INSERT for analytics events. Clickstream Service receives POST /api/events and inserts directly into `analytics.raw_events`. No message broker. Events are validated against a canonical taxonomy before insert.

## Consequences

- Zero additional infrastructure beyond PostgreSQL
- Simpler operational model — one operator can manage the stack
- Event insert is synchronous (acceptable for fire-and-forget, errors swallowed client-side)
- If event volume grows significantly, a message broker can be introduced later with an ADR
