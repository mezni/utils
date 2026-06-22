# Sprint 00 Follow-Up: Action Items

**Sprint**: 00
**Branch**: 001-system-bootstrap
**Date**: 2026-06-21
**Status**: Active

## Overview

This document captures action items from Sprint 00 review, compliance audit, and outstanding work that needs attention before or during Sprint 1.

## Immediate Action Items (Next 24 Hours)

### High Priority

| # | Action Item | Owner | Status | Notes |
|---|-------------|-------|--------|-------|
| AI-001 | Test CI pipeline end-to-end (`make ci`) | TBD | ❌ Not Started | Requires Cargo.lock and target directory |
| AI-002 | Generate SQLx offline data (`cargo sqlx prepare --all`) | TBD | ❌ Not Started | Requires database connection |
| AI-003 | Verify database migrations (`docker-compose up -d && ./migrate.sh`) | TBD | ❌ Not Started | Requires Docker |
| AI-004 | Test service health endpoints (3000/3001/3002) | TBD | ❌ Not Started | Requires compiled binaries |

### Medium Priority

| # | Action Item | Owner | Status | Notes |
|---|-------------|-------|--------|-------|
| AI-005 | Add unit tests for CI validation scripts | TBD | ❌ Not Started | test_*.sh files |
| AI-006 | Document Rust cargo clean build steps | TBD | ❌ Not Started | Quickstart updates |
| AI-007 | Create .env.example file | TBD | ❌ Not Started | For development setup |

### Low Priority

| # | Action Item | Owner | Status | Notes |
|---|-------------|-------|--------|-------|
| AI-008 | Complete Phase 8 polish tasks | TBD | ❌ Not Started | Optional Redis/Keycloak setup |
| AI-009 | Add performance benchmarks | TBD | ❌ Not Started | Future improvement |
| AI-010 | Create onboarding documentation | TBD | ❌ Not Started | Team benefit |

## Work from Phase 8 (Polish & Cross-Cutting)

### T109: Redis Configuration

**Action**: Create infrastructure/redis/redis.conf for Redis configuration
**Priority**: Optional
**Estimated Effort**: 30 minutes

**Acceptance Criteria**:
- [ ] Minimal Redis configuration for caching
- [ ] Proper memory limits
- [ ] Persistence configuration
- [ ] Security settings (password, bind)

### T110: Keycloak Setup Script

**Action**: Create infrastructure/scripts/setup_keycloak.sh for Keycloak setup
**Priority**: Optional
**Estimated Effort**: 1 hour

**Acceptance Criteria**:
- [ ] Automated Keycloak container startup
- [ ] Realm configuration
- [ ] Client registration
- [ ] User federation setup

### T111: Keycloak Realm Export

**Action**: Create infrastructure/realm-bornemap.json for Keycloak realm export
**Priority**: Optional
**Estimated Effort**: 1 hour

**Acceptance Criteria**:
- [ ] Realm definition
- [ ] Client configurations
- [ ] User federation settings
- [ ] Authentication flows

### T112: Git Auto-Commit Hooks

**Action**: Add .specify/extensions/git/git-config.yml for auto-commit hooks
**Priority**: Optional
**Estimated Effort**: 30 minutes

**Acceptance Criteria**:
- [ ] Pre-commit hooks for formatting
- [ ] Post-commit hooks for update
- [ ] Proper error handling
- [ ] SpecKit integration

### T113: Independent Testing Verification

**Action**: Verify all 5 user stories can be tested independently
**Priority**: Required
**Estimated Effort**: 1 hour

**Acceptance Criteria**:
- [ ] US1: Directory structure matches spec
- [ ] US2: `make ci` runs all 9 stages
- [ ] US3: Database schemas created correctly
- [ ] US4: Health endpoints respond on 3000/3001/3002
- [ ] US5: SpecKit compliance passes

### T114: Full CI Pipeline Test

**Action**: Run full CI pipeline (9 stages) and verify all pass
**Priority**: Required
**Estimated Effort**: 30 minutes

**Acceptance Criteria**:
- [ ] All 9 stages execute
- [ ] No hard-stop failures
- [ ] JSON artifacts created for each stage
- [ ] Exit code 0 on success

## Technical Debt

### Known Issues

| # | Issue | Severity | Notes |
|---|-------|----------|-------|
| TD-001 | `chrono` dependency missing from workspace Cargo.toml | Low | Services use chrono in main.rs |
| TD-002 | `.cargo/config.toml` missing `[registries]` section | Low | For private registry support |
| TD-003 | No `rust-toolchain.toml` for pinned Rust version | Low | Team should pin 1.75+ |
| TD-004 | Docker Compose healthcheck intervals may be too short | Low | Increase to 30s for stability |
| TD-005 | Service config.toml files use hard-coded passwords | Medium | Should use environment variables |

### Planned Fixes

| # | Fix | Sprint | Priority |
|---|-----|--------|----------|
| TF-001 | Add `chrono` to workspace dependencies | Sprint 1 | Low |
| TF-002 | Create `rust-toolchain.toml` | Sprint 1 | Low |
| TF-003 | Update config to use env vars for passwords | Sprint 1 | Medium |
| TF-004 | Add `[registries]` to .cargo/config.toml | Sprint 1 | Low |

## Infrastructure Improvements

### CI Pipeline

- [ ] Benchmark CI pipeline performance
- [ ] Optimize slowest stages
- [ ] Implement caching for dependencies
- [ ] Add parallel stage execution where possible

### Database

- [ ] Set up database backup strategy
- [ ] Create database user management script
- [ ] Implement connection pooling monitoring
- [ ] Add database health check endpoints

### Monitoring

- [ ] Set up service health monitoring
- [ ] Create alerting rules
- [ ] Set up log aggregation
- [ ] Implement metrics collection

## Documentation Updates

### Required Updates

- [ ] Add Rust installation instructions
- [ ] Document PostgreSQL setup
- [ ] Add troubleshooting guide
- [ ] Create development workflow document

### Nice-to-Have

- [ ] Add architecture diagrams
- [ ] Create API documentation portal
- [ ] Write tutorial for onboarding
- [ ] Create deployment guide

## Future Considerations

### Sprint 1 Planning

**Goal**: Core API Implementation
**Estimated Tasks**: ~50 tasks
**Estimated Time**: 3-4 days

**Key Areas**:
1. Authentication and authorization
2. GIS operations (nearby stations, spatial queries)
3. Inventory CRUD operations
4. Analytics ingestion and reporting

### Sprint 2 Planning

**Goal**: Frontend Development
**Estimated Tasks**: ~40 tasks
**Estimated Time**: 3-4 days

**Key Areas**:
1. UI component implementation
2. API client integration
3. Admin dashboard
4. Driver interface

### Sprint 3 Planning

**Goal**: Advanced Features
**Estimated Tasks**: ~30 tasks
**Estimated Time**: 2-3 days

**Key Areas**:
1. OSM ETL integration
2. Real-time analytics
3. Performance optimization
4. Comprehensive testing

## Team Capacity

### Current Team

- **Lead**: TBD
- **Backend Developers**: TBD
- **Frontend Developers**: TBD
- **DevOps**: TBD

### Recommendations

- **Sprint 1**: Assign 2 backend developers, 1 frontend developer
- **Sprint 2**: Assign 1 backend developer, 2 frontend developers
- **Sprint 3**: Assign all team members

## Timeline

### Immediate (Next 1 week)

- Complete AI-001 through AI-004 (high priority action items)
- Assign owners to remaining action items
- Begin Phase 8 polish tasks if feasible

### Sprint 1 Start

- Target: 2026-06-28
- Preparation: Review action items, assign tasks, begin core API implementation

### Sprint 1 Completion

- Target: 2026-07-01
- Key Deliverables: Core APIs operational, CI tests passing

## Decision Log

| # | Decision | Date | Rationale |
|---|----------|------|-----------|
| D-001 | Monorepo with workspace | 2026-06-21 | Shared contracts, independent team development |
| D-002 | 9-stage CI pipeline | 2026-06-21 | Constitutional compliance, early failure detection |
| D-003 | Identity dual system | 2026-06-21 | Separate concerns, business logic vs user identity |
| D-004 | Data ownership | 2026-06-21 | Clear boundaries, single-writer analytics |
| D-005 | Contract-first | 2026-06-21 | Independent team work, clear contracts |
| D-006 | SQLx compile-time verification | 2026-06-21 | Type safety, early detection |
| D-007 | Single-writer analytics | 2026-06-21 | Prevent corruption, enforce CQRS separation |
| D-008 | Migration drift detection | 2026-06-21 | Prevent schema divergence |
| D-009 | Identity location rules | 2026-06-21 | Enforce business logic, prevent mixing |

## Resources

### Sprint 00 Documents

- Feature Specification: `specs/001-system-bootstrap/spec.md`
- Implementation Plan: `specs/001-system-bootstrap/plan.md`
- Data Model: `specs/001-system-bootstrap/data-model.md`
- Progress Report: `specs/001-system-bootstrap/PROGRESS.md`
- System State: `docs/SYSTEM_STATE.md`
- Roadmap: `docs/roadmap_status.md`
- Sprint Review: `docs/sprints/sprint_00/review/sprint_00_review.md`
- Compliance Audit: `docs/sprints/sprint_00/validation_report.md`
- Sprint State: `docs/sprints/sprint_00/sprint_state.json`

### Key Files

- Workspace: `Cargo.toml`
- CI Workflow: `.github/workflows/ci.yml`
- Constitution: `docs/constitution/constitution.md`
- CI Guard: `tools/ci_guard.sh`
- Docker Compose: `infrastructure/docker-compose/local.yml`

## Next Review

**Next Sprint Review**: Sprint 01 Review
**Target Date**: 2026-06-28
**Location**: TBD
**Attendees**: All team members
