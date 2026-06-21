# Sprint 3 — Inventory System (Admin Domain)

**Status**: NOT_STARTED
**Constitution Version**: 1.15.2
**Dependencies**: Sprint 2 (GIS system operational)

---

## Must Have (Exit Criteria)

| ID | Task | Owner | Status |
|----|------|-------|--------|
| S3-001 | Create `inventory.partners` table (OPR prefix, soft delete) | team | NOT_STARTED |
| S3-002 | Create `inventory.stations` table (STA prefix, FK → partners) | team | NOT_STARTED |
| S3-003 | Create `inventory.chargers` table (CHG prefix, FK → stations) | team | NOT_STARTED |
| S3-004 | Implement partner CRUD (create, update, deactivate) | team | NOT_STARTED |
| S3-005 | Implement station CRUD (create, update location, soft delete) | team | NOT_STARTED |
| S3-006 | Implement charger CRUD (assign to station, update status) | team | NOT_STARTED |
| S3-007 | Implement soft delete on all inventory entities | team | NOT_STARTED |
| S3-008 | Implement referential integrity (station → partner, charger → station) | team | NOT_STARTED |
| S3-009 | Create PREFIX validation checks in CI | team | NOT_STARTED |
| S3-010 | Create materialized views (mv_station_inventory, mv_partner_summary, mv_charger_status) | team | NOT_STARTED |
| S3-011 | Implement admin-service audit event emission | team | NOT_STARTED |
| S3-012 | Implement inventory API endpoints (POST/PATCH for partners, stations, chargers) | team | NOT_STARTED |

## Should Have

| ID | Task | Owner | Status |
|----|------|-------|--------|
| S3-013 | Create admin dashboard partner CRUD UI | team | NOT_STARTED |
| S3-014 | Create admin dashboard station management UI | team | NOT_STARTED |
| S3-015 | Create admin dashboard charger management UI | team | NOT_STARTED |
| S3-016 | Create CI inventory ownership gate | team | NOT_STARTED |
| S3-017 | Create CI entity identity gate | team | NOT_STARTED |
| S3-018 | Create CI referential integrity gate | team | NOT_STARTED |
| S3-019 | Create CI soft delete enforcement gate | team | NOT_STARTED |
| S3-020 | Create CI audit pipeline gate | team | NOT_STARTED |

## Nice to Have

| ID | Task | Owner | Status |
|----|------|-------|--------|
| S3-021 | Add audit log viewer (read-only) in admin dashboard | team | NOT_STARTED |
| S3-022 | Add partner-level aggregation views | team | NOT_STARTED |

## CI Additions (Sprint 3)

| ID | Gate | Rule |
|----|------|------|
| CI-3.1 | Inventory Ownership Gate | FAIL if admin-service writes outside inventory schema or other service writes inventory |
| CI-3.2 | Entity Identity Gate | FAIL if station not prefixed STA-, partner not OPR-, charger not CHG- |
| CI-3.3 | Referential Integrity Gate | FAIL if station references missing partner or charger references missing station |
| CI-3.4 | Soft Delete Enforcement Gate | FAIL if hard delete used on inventory tables |
| CI-3.5 | Audit Pipeline Gate | FAIL if inventory changes not emitting event or events bypass driver-service ingestion |

## Exit Criteria

Sprint 3 is COMPLETE ONLY IF:
- [ ] Full CRUD working for partners, stations, chargers
- [ ] Referential integrity enforced (FK constraints active)
- [ ] Correct PREFIX enforcement validated in CI
- [ ] All changes generate audit events
- [ ] Ownership gates pass
- [ ] Soft delete enforced
- [ ] FK integrity validated
