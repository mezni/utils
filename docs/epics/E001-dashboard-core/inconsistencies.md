# Inconsistency Report: E001 Documentation

**Date**: 2026-06-23
**Branch**: 001-ev-dashboard
**Status**: 🚨 CRITICAL ISSUES FOUND

---

## Summary

This report identifies **7 major inconsistencies** between the docs/epics and specs/001-ev-dashboard directories. The specs/001-ev-dashboard directory (spec.md, data-model.md, research.md, corrections.md) is the source of truth and aligns with the E001 Database Specification. The docs/epics directory has outdated information that must be synchronized.

---

## CRITICAL INCONSISTENCIES

### 1. Entity Naming: "Operators" vs "Partners" 🔴

**Location**: docs/epics/E001-dashboard-core/epic.md

**Issue**:
- Uses "operators" for API endpoints and table names
- Example: `GET /api/v1/operators`, table `operators`, column `operator_id`

**Expected** (from spec.md and database spec):
- Uses "partners" for API endpoints and table names
- Example: `GET /api/v1/partners`, table `partners`, column `partner_id`

**Files Affected**:
- ✅ docs/epics/E001-dashboard-core/epic.md (Lines 27, 27-29, 156-158, 182-184)
- ❌ docs/epics/E001-dashboard-core/api.md (Correct: uses "partners")
- ✅ specs/001-ev-dashboard/spec.md (Correct: uses "partners")
- ✅ specs/001-ev-dashboard/data-model.md (Correct: uses "partners")

**Impact**: MAJOR
- Breaking API endpoint changes
- Database schema inconsistencies
- Frontend routing conflicts
- Requires complete rename across all docs/epics files

---

### 2. Status Enum Not Documented 🟥

**Location**: docs/epics/E001-dashboard-core/epic.md

**Issue**:
- Does NOT mention status field on any entities
- Table definitions (lines 182-199) are missing status column
- No status enum definition
- No status validation rules

**Expected** (from spec.md and database spec):
- Status field on all entities (Partner, Station, Charger)
- Unified status enum: ACTIVE, INACTIVE, MAINTENANCE, DISABLED
- Status validation rules in requirements

**Files Affected**:
- ❌ docs/epics/E001-dashboard-core/epic.md (Lines 182-199)
- ✅ specs/001-ev-dashboard/spec.md (FR-072 to FR-074)
- ✅ specs/001-ev-dashboard/data-model.md (Status field section)
- ✅ specs/001-ev-dashboard/research.md (Status enum consistency)

**Impact**: CRITICAL
- Missing status field in table definitions
- No status validation in API layer
- Frontend cannot display status
- Incomplete data model

---

### 3. Soft Delete vs Hard Delete Unclear 🟥

**Location**: docs/epics/E001-dashboard-core/epic-constraints.md

**Issue**:
- Says "cascade delete required" (line 13) but doesn't specify:
  - Which operations cascade (hard delete only?)
  - Which operations don't cascade (soft delete?)
- No deleted_at column mentioned
- No undelete operation mentioned

**Expected** (from spec.md and database spec):
- Hard delete: CASCADE (deletes children automatically)
- Soft delete: NO cascade (children remain)
- deleted_at column for soft delete
- Undelete operation (remove deleted_at)

**Files Affected**:
- ❌ docs/epics/E001-dashboard-core/epic-constraints.md (Line 13)
- ❌ docs/epics/E001-dashboard-core/epic.md (Lines 106-108, 203-206)
- ✅ specs/001-ev-dashboard/spec.md (FR-028 to FR-031, FR-068)
- ✅ specs/001-ev-dashboard/data-model.md (Cascade delete rule section)
- ✅ specs/001-ev-dashboard/research.md (Cascade strategy)

**Impact**: CRITICAL
- Ambiguous delete behavior
- Data integrity risks
- Cannot implement correctly
- Test failures expected

---

### 4. Admin Dependency Not Mentioned 🟥

**Location**: docs/epics/E001-dashboard-core/epic.md

**Issue**:
- Table definitions missing admin FK columns:
  - No `created_by` column in table definitions
  - No `updated_by` column in table definitions
  - No mention of admins table
- No acknowledgment of admin dependency

**Expected** (from spec.md and database spec):
- `created_by` FK to admins table
- `updated_by` FK to admins table
- Explicit statement: "admins table exists in separate system module"
- No auth system in scope

**Files Affected**:
- ❌ docs/epics/E001-dashboard-core/epic.md (Lines 182-199)
- ✅ specs/001-ev-dashboard/spec.md (FR-071)
- ✅ specs/001-ev-dashboard/data-model.md (Audit rules section)

**Impact**: CRITICAL
- Missing audit trail columns
- Cannot track data ownership
- Cannot implement CRUD operations
- Scope boundary unclear

---

### 5. Deleted Records Filtering Not Documented 🟡

**Location**: docs/epics/E001-dashboard-core/epic.md

**Issue**:
- No mention of deleted_at column
- No mention of query filtering (`deleted_at IS NULL`)
- No user stories for soft delete/undelete
- Default list queries include soft-deleted records?

**Expected** (from spec.md and database spec):
- Soft delete via `deleted_at` timestamp
- All queries filter by `deleted_at IS NULL`
- Views provided for active records only
- User stories for soft delete and undelete

**Files Affected**:
- ❌ docs/epics/E001-dashboard-core/epic.md (Lines 156-169, 182-199)
- ✅ specs/001-ev-dashboard/spec.md (FR-030 to FR-031, FR-058)
- ✅ specs/001-ev-dashboard/data-model.md (Soft delete section)

**Impact**: MEDIUM
- List queries may return deleted records
- Data exposure risk
- Inconsistent user experience

---

### 6. API Endpoint Path Inconsistencies 🟡

**Location**: Multiple files

**Issue**:
- docs/epics/E001-dashboard-core/api.md uses:
  - `/partners`, `/stations`, `/chargers` (no /api/v1)
- docs/epics/E001-dashboard-core/epic.md uses:
  - `/api/v1/operators`, `/api/v1/stations`, `/api/v1/chargers`
- Inconsistent base path specification
- Inconsistent entity naming

**Expected** (from spec.md and database spec):
- `/api/v1/partners`, `/api/v1/stations`, `/api/v1/chargers`
- Base path specified: `/api/v1`
- Entity naming: partners (not operators)

**Files Affected**:
- ❌ docs/epics/E001-dashboard-core/epic.md (Lines 156-169)
- ✅ specs/001-ev-dashboard/spec.md (Base path section)
- ✅ specs/001-ev-dashboard/data-model.md (API paths)

**Impact**: MEDIUM
- API contract inconsistency
- Frontend routing confusion
- API testing failures

---

### 7. Deterministic ID Generation Not Mentioned 🟡

**Location**: docs/epics/E001-dashboard-core/epic.md

**Issue**:
- Only says "nanoid(12)" format (line 85-87)
- Does NOT specify deterministic vs random generation
- No mention of seed-based generation
- No mention of infrastructure layer implementation

**Expected** (from spec.md and database spec):
- Deterministic (hash-based nanoid from seed)
- Infrastructure layer only
- Format: ENTITY-{12 chars}
- Implementation details in research.md

**Files Affected**:
- ❌ docs/epics/E001-dashboard-core/epic.md (Lines 85-87)
- ✅ specs/001-ev-dashboard/spec.md (FR-032 to FR-036)
- ✅ specs/001-ev-dashboard/data-model.md (Identity generation rules)
- ✅ specs/001-ev-dashboard/research.md (Identity generation section)

**Impact**: MEDIUM
- Cannot implement correctly
- Random IDs across instances
- Test reproducibility issues

---

## INCONSISTENCY SUMMARY TABLE

| # | Issue | Severity | Scope | Priority |
|---|---|---|---|---|
| 1 | Entity naming: operators vs partners | MAJOR | API + DB + Frontend | 🔴 HIGH |
| 2 | Status enum missing from docs | CRITICAL | All entities | 🔴 HIGH |
| 3 | Soft delete vs hard delete unclear | CRITICAL | Delete operations | 🔴 HIGH |
| 4 | Admin dependency not documented | CRITICAL | Audit fields | 🔴 HIGH |
| 5 | Deleted records filtering not documented | MEDIUM | List queries | 🟡 MEDIUM |
| 6 | API endpoint path inconsistencies | MEDIUM | API contracts | 🟡 MEDIUM |
| 7 | Deterministic ID not specified | MEDIUM | ID generation | 🟡 MEDIUM |

---

## FILES AFFECTED

### Must be Updated (CRITICAL):
1. ✅ docs/epics/E001-dashboard-core/epic.md
   - Replace all "operators" with "partners"
   - Add status field to all tables
   - Clarify soft delete vs hard delete
   - Add admin dependency section
   - Add deleted_at filtering rules

### Should be Updated (MEDIUM):
2. ✅ docs/epics/E001-dashboard-core/api.md
   - Ensure consistent base path: /api/v1/partners
   - Add status field to endpoints
   - Add undelete endpoints

3. ✅ docs/epics/E001-dashboard-core/domain-model.md
   - Add status enum to relationships
   - Clarify cascade delete behavior

4. ✅ docs/epics/E001-dashboard-core/epic-constraints.md
   - Clarify cascade delete rules
   - Add soft delete constraint
   - Add admin dependency constraint

### Already Correct:
- ✅ specs/001-ev-dashboard/spec.md
- ✅ specs/001-ev-dashboard/data-model.md
- ✅ specs/001-ev-dashboard/research.md
- ✅ specs/001-ev-dashboard/corrections.md

---

## RECOMMENDATIONS

### Immediate Actions (Priority 1):
1. **Sync docs/epics directory with specs/001-ev-dashboard directory**
   - Replace "operators" with "partners" throughout docs/epics
   - Add status field to all table definitions
   - Clarify hard delete vs soft delete cascade rules
   - Document admin dependency

2. **Update API contracts**
   - Ensure all API paths use `/api/v1/partners`, `/api/v1/stations`, `/api/v1/chargers`
   - Add status fields to all endpoints
   - Add undelete endpoints

3. **Update data model documentation**
   - Ensure all tables have status field
   - Document deleted_at filtering
   - Clarify cascade delete behavior

### Follow-up Actions (Priority 2):
4. **Create sync consistency check script**
   - Automate comparison between docs/epics and specs/001-ev-dashboard
   - Flag naming inconsistencies
   - Flag missing fields

5. **Update project onboarding**
   - Include consistency rules in onboarding docs
   - Train developers on entity naming conventions
   - Document decision-making process

---

## SCORING UPDATE

### Before Inconsistency Fix:
| Category | Score | Notes |
|---|---|---|
| Clean Architecture | 9/10 | ✅ Correct |
| E001 alignment | 10/10 | ✅ Spec.md correct |
| Implementation readiness | 9/10 | ✅ Spec.md complete |
| Consistency | **3/10** | ❌ Major inconsistencies |
| Scalability | 9/10 | ✅ Correct |
| **Overall** | **7.75/10** | Down from 9.4/10 |

### After Inconsistency Fix (Expected):
| Category | Score | Notes |
|---|---|---|
| Clean Architecture | 9/10 | ✅ Correct |
| E001 alignment | 10/10 | ✅ Spec.md correct |
| Implementation readiness | 9/10 | ✅ Spec.md complete |
| Consistency | **10/10** | ✅ Fully synchronized |
| Scalability | 9/10 | ✅ Correct |
| **Overall** | **9.4/10** | ✅ Back to original score |

---

## VERIFICATION CHECKLIST

Before marking this report as resolved:

- [ ] All "operators" references replaced with "partners" in docs/epics
- [ ] Status field added to all tables in docs/epics
- [ ] Hard delete CASCADE explicitly documented
- [ ] Soft delete NO CASCADE explicitly documented
- [ ] deleted_at filtering documented for all queries
- [ ] Admin dependency explicitly stated
- [ ] API paths use `/api/v1/partners`, `/api/v1/stations`, `/api/v1/chargers`
- [ ] Status enum (ACTIVE, INACTIVE, MAINTENANCE, DISABLED) documented
- [ ] Undelete operation documented
- [ ] Deterministic ID generation specified

---

## CONCLUSION

The **docs/epics directory** is significantly outdated and does NOT match the **specs/001-ev-dashboard directory**, which is the source of truth aligned with the E001 Database Specification. Critical inconsistencies must be resolved before proceeding with Phase 1 implementation.

**Recommendation**: Update docs/epics directory to match specs/001-ev-dashboard directory, then regenerate implementation tasks.
