# E001 CONSTRAINTS

---

## Architectural Constraints
- Clean Architecture enforced strictly
- No cross-layer dependency violations

---

## Data Constraints
- id is primary identifier
- Hard delete cascade required
- Soft delete no cascade (children remain)
- No orphan records (except after hard delete)
- Status field required on all entities
- Status enum: ACTIVE, INACTIVE, MAINTENANCE, DISABLED
- Audit fields required (created_by, updated_by)
- Admin dependency (admins table exists externally)

---

## Delete Behavior
- Hard delete: CASCADE (deletes children automatically)
- Soft delete: NO cascade (children remain active)
- All queries filter by deleted_at IS NULL

---

## Frontend Constraints
- no fetch in UI
- API client mandatory
- React Query required
