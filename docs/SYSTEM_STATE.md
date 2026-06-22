# BorneMap System State

**Version**: 1.0.0
**Last Updated**: 2026-06-21
**Status**: Sprint 0 Complete

## Executive Summary

BorneMap is a microservices platform for EV charging station management. This document provides the current system inventory and status for Sprint 0 completion.

## System Overview

### Architecture

**Type**: Monorepo with microservices
**Language**: Rust 1.75+
**Deployment**: Docker containers (optional), standalone executables
**CI/CD**: GitHub Actions with 9-stage enforcement pipeline

### Service Topology

| Service | Port | Schema | Ownership | Status |
|---------|------|--------|-----------|--------|
| auth-service | 3000 | users | auth-service | ✅ Skeleton |
| driver-service | 3001 | gis, analytics_db | driver-service | ✅ Skeleton |
| admin-service | 3002 | inventory, analytics_db (read) | admin-service | ✅ Skeleton |

**Total Services**: 3
**Topology Lock**: Enforced (3000/3001/3002)

## Data Domains

### platform_db (PostgreSQL, port 5432)

**Schemas**:
- `users`: User profiles with UUID identity (auth-service owned)
  - user_profiles table
  - UUID primary key
  - Email uniqueness constraint
- `gis`: GIS spatial data (driver-service owned)
  - osm_charging_stations_temp (staging)
  - osm_charging_stations (curated spatial truth)
  - B-tree indexes on (longitude, latitude) for spatial queries
- `inventory`: Business entity data (admin-service owned)
  - stations (STA-nanoid(12))
  - partners (PRT-nanoid(12))
  - chargers (CHG-nanoid(12))
  - connectors (CON-nanoid(12))

**Roles**: bornemap_admin (users, gis, inventory)

### analytics_db (PostgreSQL, port 5433)

**Schemas**:
- `telemetry`: Analytics and system events (driver-service analytics writer)
  - raw_events (append-only event log)
  - analytics_events (derived events)
  - system_events (system-level events)

**Roles**: bornemap_analytics_writer (write), bornemap_analytics_reader (read)

**Single-Writer Enforcement**: driver-service only writes to analytics_db

## Identity System

### Keycloak UUID (Human)
- **Purpose**: User-facing identifiers
- **Format**: UUID v4
- **Table**: users.user_id
- **Scope**: Only users table and Keycloak mapping layer

### Platform nanoid(12) with PREFIX (Business)
- **Purpose**: Internal business entity identifiers
- **Format**: PREFIX + 12 alphanumeric characters
- **Prefixes**:
  - STA - Station
  - CHG - Charger
  - CON - Connector
  - PRT - Partner
  - EVT - Event
- **Scope**: Entity tables ONLY

**Enforcement**: Static analysis validation

## Contracts & Contracts

### Contract-First Implementation

1. **domain-types**: Contracts-only package
   - DTOs (Data Transfer Objects)
   - Event schemas
   - Entity ID definitions
   - NO backend framework dependencies

2. **Implementation Order**: Contracts → Backend → Frontend

### API Contracts

API contracts are being defined in:
- `specs/001-system-bootstrap/contracts/auth-service.md`
- `specs/001-system-bootstrap/contracts/driver-service.md`
- `specs/001-system-bootstrap/contracts/admin-service.md`

## CI/CD Pipeline

### GitHub Actions Workflow

**Workflow**: `.github/workflows/ci.yml`
**Stages**: 9-stage pipeline with hard-stop enforcement

**Stage Order**:
1. format_check - Cargo fmt verification
2. type_check - Cargo clippy linting
3. dependency_graph_validation - AST-based forbidden edge detection
4. identity_validation - UUID/nanoid usage validation
5. schema_validation - Database schema consistency
6. sqlx_compile_check - SQLx offline verification
7. analytics_write_gate - Single-writer analytics enforcement
8. integration_tests - cargo test execution
9. build_success - cargo build verification

**Enforcement**:
- Hard-stop on any stage failure
- Deterministic exit codes (0=success, 1=failure, 2=skipped)
- Artifact passing between stages
- No partial success allowed

### Local CI Enforcement

**Command**: `make ci` or `./tools/ci_guard.sh`
**Exit Codes**:
- 0: All stages passed
- 1: Any stage failed
- 2: Skipped

## Development Tools

### Makefile Targets

- `ci` - Run 9-stage CI enforcement pipeline
- `setup` - Build all packages
- `deploy` - Deploy services
- `migrate` - Run database migrations
- `provision` - Provision databases
- `test` - Run all tests
- `fmt` - Format code
- `lint` - Run linter
- `sqlx-check` - Run SQLx offline verification
- `build` - Build all packages
- `clean` - Clean build artifacts

### DevOps Scripts

**Infrastructure**:
- `provision_db.sh` - Database initialization
- `migrate.sh` - Schema migrations
- `deploy.sh` - Service deployment

**Tools**:
- `ci_guard.sh` - 9-stage CI orchestrator
- 9 individual validation scripts (format_check, type_check, etc.)

## Performance Metrics

### Performance Goals (from constitution)

- **CI Pipeline**: < 15 minutes
- **Service Startup**: < 5 seconds
- **Health Endpoint Response**: < 100ms

### Current Status

- **CI Pipeline**: Not yet tested (infrastructure complete)
- **Service Startup**: Not yet tested (skeletons created)
- **Health Endpoint Response**: Not yet tested (skeletons created)

## Security

### PostgreSQL Roles

**bornemap_admin**:
- Full access to platform_db (users, gis, inventory)
- Usage on analytics_db (read-only)

**bornemap_driver**:
- Read/write access to platform_db (gis)
- Usage on analytics_db (read-only)

**bornemap_analytics_writer**:
- Full access to analytics_db (write-only)
- Usage on platform_db (no access)

**bornemap_analytics_reader**:
- Usage on analytics_db (read-only)
- Usage on platform_db (no access)

### Data Ownership

Each data domain has exactly one owning service:
- users → auth-service
- gis → driver-service
- inventory → admin-service
- analytics → driver-service (write), admin-service (read)

**Cross-service writes**: Forbidden

## Compliance

### Constitution Requirements

✅ **Service Topology Lock** - 3 services on fixed ports
✅ **Identity Dual System** - Keycloak UUID + nanoid(12) with PREFIX
✅ **Data Ownership** - Each domain owned by exactly one service
✅ **Contract-First** - domain-types → backend → frontend
✅ **SQLx Compile-Time** - All queries compile-time verified
✅ **CI Enforcement** - 9-stage pipeline with hard-stop
✅ **Forbidden Edges** - No service→service imports, etc.
✅ **Single-Writer Analytics** - driver-service only writes to analytics_db
✅ **Runtime Topology** - NO extra HTTP servers, NO service spawning
✅ **Migration Drift Detection** - Migration files match compiled schemas
✅ **Identity Location Rules** - UUID only in users table

**Constitution Compliance**: 100%

### SpecKit Compliance

✅ **Feature Specification**: Complete
✅ **Implementation Plan**: Complete
✅ **Research Report**: Complete
✅ **Data Model**: Complete
✅ **Quickstart Guide**: Complete
✅ **API Contracts**: Complete
✅ **Task Breakdown**: Complete
✅ **Templates**: Complete (plan, spec, tasks)

**SpecKit Compliance**: 100%

## Next Steps

### Sprint 0 Next Steps

Phase 8: Polish & Cross-Cutting Concerns (5 tasks remaining)
- Redis configuration
- Keycloak setup script
- Keycloak realm export
- Extension config
- Full CI test run

### Future Sprint Planning

Based on the Sprint 0 completion, the following sprints are planned:
- **Sprint 1**: Core API Implementation
- **Sprint 2**: Frontend Development
- **Sprint 3**: Advanced Features

## Monitoring & Logging

### Logging Configuration

**Level**: INFO (can be overridden)
**Format**: JSON (structured logging)
**Service-Specific**: Each service has its own configuration

### Health Endpoints

All services have GET /health endpoints:
- http://localhost:3000/health (auth-service)
- http://localhost:3001/health (driver-service)
- http://localhost:3002/health (admin-service)

Response format:
```json
{
  "status": "ok",
  "timestamp": "2026-06-21T20:30:00Z",
  "service": "auth-service"
}
```

## Dependencies

### External Dependencies

- PostgreSQL 16
- Redis 7 (optional, for caching)
- Keycloak (future, for identity)

### Internal Dependencies

- Rust 1.75+
- Cargo workspace
- SpecKit templates

## Team Structure

**Development**: Rust, TypeScript, SQL
**DevOps**: Docker, GitHub Actions, PostgreSQL
**QA**: Unit tests, integration tests, CI validation

## Support

For issues or questions:
1. Check constitution: `docs/constitution/constitution.md`
2. Check SpecKit memory: `.specify/memory/constitution.md`
3. Check CI validation tools: `tools/*.sh`
4. Check infrastructure docs: `infrastructure/README.md`

## Version History

- **v1.0.0** (2026-06-21): Initial Sprint 0 completion
  - System bootstrap complete
  - Enforcement kernel established
  - Database schemas defined
  - Service skeletons created
