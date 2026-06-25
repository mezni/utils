# Validation Report — Sprint 04

**Date**: 2026-06-25
**Branch**: `sprint/04-ev-domain-bootstrap`

---

## 1. SQL Validation

| Check | Tool | Status | Details |
|-------|------|--------|---------|
| Syntax | `psql -f` | ✅ PASS | All 6 migrations apply without syntax errors |
| Idempotency | `psql -f` 2x | ✅ PASS | All CREATE use IF NOT EXISTS, all INSERT use ON CONFLICT DO NOTHING |
| Re-apply | Second run | ✅ PASS | Zero errors, only NOTICE messages |

## 2. Constitution Compliance

| Rule | Check | Status |
|------|-------|--------|
| §2.1 No new services | Services untouched | ✅ |
| §2.4 Entity ID: PREFIX-nanoid(12) | OPR-, STA-, CHG- prefixes | ✅ |
| §4.1 Schema ownership | `ev` schema → admin-service | ✅ |
| §10.3 Identity separation | No UUID on entities | ✅ |
| §17 Forward-only migrations | IF NOT EXISTS / OR REPLACE | ✅ |
| §19 KNOWN-001 fix | Migration filters `is_test = FALSE` | ✅ APPLIED |
| §19 KNOWN-002 fix | `deleted_at` on partners, stations, chargers | ✅ APPLIED |
| Scope lock | No scope expansion | ✅ |

## 3. Security Validation

| Check | Status | Notes |
|-------|--------|-------|
| No UUID as entity ID | ✅ | OPR/STA/CHG-nanoid(12) used |
| UUID audit fields only | ✅ | created_by_uuid, updated_by_uuid are UUID |
| FK integrity | ✅ | chargers → stations → partners |
| Soft-delete on all entities | ✅ | deleted_at on all 3 entity tables |
| Schema ownership documented | ✅ | ev → admin-service |
| Idempotent migration | ✅ | ON CONFLICT DO NOTHING |

## 4. Test Results

| ID | Test | Status |
|----|------|--------|
| T-001 | ev schema exists | ✅ PASS |
| T-002 | postgis extension installed | ✅ PASS |
| T-003 | hstore extension installed | ✅ PASS |
| T-004 | ev.access_types exists | ✅ PASS |
| T-005 | ev.data_sources exists | ✅ PASS |
| T-006 | ev.connector_types exists | ✅ PASS |
| T-007 | ev.current_types exists | ✅ PASS |
| T-008 | ev.connector_statuses exists | ✅ PASS |
| T-009 | ev.partners exists | ✅ PASS |
| T-010 | ev.stations exists | ✅ PASS |
| T-011 | ev.chargers exists | ✅ PASS |
| T-012 | partner_id format | ✅ PASS |
| T-013 | station_id column | ✅ PASS |
| T-014 | charger_id column | ✅ PASS |
| T-015 | location GEOGRAPHY type | ✅ PASS |
| T-016 | GIST spatial index | ✅ PASS |
| T-017 | partners.deleted_at | ✅ PASS |
| T-018 | stations.deleted_at | ✅ PASS |
| T-019 | chargers.deleted_at | ✅ PASS |
| T-020 | unique_connector constraint | ✅ PASS |
| T-021 | FK chargers → stations | ✅ PASS |
| T-022 | FK stations → partners | ✅ PASS |
| T-023 | count_available CHECK >= 0 | ✅ PASS |
| T-024 | access_types seed data | ✅ PASS |
| T-025 | data_sources seed data | ✅ PASS |
| T-026 | connector_types seed data | ✅ PASS |

**Total**: 26/26 PASS

## 5. Hard Stop Verification

| Condition | Status |
|-----------|--------|
| UUID used as entity ID | ✅ None |
| Spatial index omitted | ✅ Created |
| Soft-delete omitted | ✅ Applied to all 3 entity tables |
| Schema ownership violated | ✅ Documented: ev → admin-service |
| Scope expansion | ✅ None attempted |
| SQL syntax error | ✅ None found |
| Architecture boundary violation | ✅ None |

## 6. Summary

```
PASS: 26/26 integration tests
PASS: All 6 migrations applied, idempotent
PASS: Constitution compliance
PASS: PostGIS + hstore extensions
PASS: GIST spatial index
PASS: Soft-delete on all entities
PASS: Entity ID standards (OPR-, STA-, CHG-)
PASS: FK chain integrity
PASS: Charger count constraints
```
