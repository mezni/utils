# Data Model: Dev Environment + CI/CD + Runnable Skeleton

## Overview

Phase 1 has no database or persistent storage. Data entities are limited to
type-level definitions in the shared `bornemap-types` crate and the runtime
health status model.

## Entities

### StationId
- **Type**: `String` (aliased)
- **Format**: `st_<nanoid-12>` (generated via `generate_id("st")`)
- **Description**: Unique identifier for EV charging stations
- **Rules**: Immutable after creation; scoped to partner ownership

### UserId
- **Type**: `String` (aliased)
- **Format**: `usr_<nanoid-12>` (generated via `generate_id("usr")`)
- **Description**: Unique identifier for registered driver accounts
- **Rules**: Immutable after creation

### PartnerId
- **Type**: `String` (aliased)
- **Format**: `prt_<nanoid-12>` (generated via `generate_id("prt")`)
- **Description**: Unique identifier for infrastructure partner accounts
- **Rules**: Immutable after creation

### HealthStatus
- **Type**: runtime enum/struct
- **Fields**: `status` (String), `service` (String)
- **Values**: `"alive"` for liveness, `"ready"` for readiness
- **Description**: Response model for `/api/v1/health/*` endpoints
- **Serialization**: JSON via serde

## Relationships

No relationships or references between entities exist in Phase 1. All types
are standalone identifiers used as foundation for future phases.

## State Transitions

Health status is stateless — each request returns the current runtime status.
No persistence, no state machine.
