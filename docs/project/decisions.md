# Decisions

Small architecture decisions that do not rise to ADR level. Recorded before code.

## MVP-1 Decisions

### D-001: Seeding Strategy

**Sprint**: 1.1

**Decision**: Use hardcoded Python script to seed database with 3 partners, 15 Tunisian stations, 24 chargers.

**Rationale**: 
- Simplest approach for MVP-1.
- Enables reproducible testing and demo data.
- No seed management framework needed yet.

**Status**: Decided, pending implementation.

---

### D-002: Charger Status Enum

**Sprint**: 1.1

**Decision**: Use database TEXT column for charger status with three values: `available`, `in_use`, `maintenance`.

**Rationale**:
- Human-readable in queries.
- No separate table needed for MVP-1.
- Frontend StatusBadge component maps colors to these values.

**Status**: Decided, pending implementation.

---

### D-003: Nearby Distance Calculation

**Sprint**: 1.1

**Decision**: Use simple Euclidean distance formula (Python math.sqrt) for MVP-1. Upgrade to PostGIS ST_DWithin in MVP-2.

**Rationale**:
- Euclidean distance sufficient for MVP validation.
- No PostGIS overhead in MVP-1.
- Clear migration path to spatial queries in MVP-2.

**Status**: Decided, pending implementation.

---

### D-004: API Response Envelope

**Sprint**: 1.1

**Decision**: Do not wrap responses in `data` or `metadata` envelopes. Return resources directly. List endpoints return JSON array.

**Rationale**:
- RESTful convention.
- Simpler client code.
- Consistent with industry standards.

**Status**: Decided, pending implementation.

---

### D-005: Error Response Format

**Sprint**: 1.1

**Decision**: Return error responses as JSON with `detail` field: `{"detail": "Station not found"}`.

**Rationale**:
- FastAPI Pydantic validation default behavior.
- Clear, consistent error messages.

**Status**: Decided, pending implementation.

---

### D-006: Dashboard Form Validation

**Sprint**: 1.2

**Decision**: Validate forms on client side (React + Zod or similar) before API call. API validates again (defense in depth).

**Rationale**:
- Fast user feedback on client.
- Server never receives invalid data.
- Both layers protect against malformed requests.

**Status**: Decided, pending implementation.

---

### D-007: Marker Color Logic

**Sprint**: 1.3

**Decision**: 
- Available station (at least one charger available): `brand.glow` (#00E676)
- Unavailable station (all chargers in_use or maintenance): `status.maintenance` (#EF4444)

**Rationale**:
- Clear visual distinction for drivers.
- Token-based, not hardcoded.
- Matches design system intention.

**Status**: Decided, pending implementation.

---

### D-008: Map Initial View

**Sprint**: 1.3 and 1.4

**Decision**: Center on Tunisia coordinates (lat 33.8869, lng 9.5375, zoom 7).

**Rationale**:
- Appropriate for Tunisian user base.
- Covers all seeded station locations.
- Consistent across Driver Web and Driver Mobile.

**Status**: Decided, pending implementation.

---

### D-009: Location Permission Handling (Mobile)

**Sprint**: 1.4

**Decision**: If user denies location permission, use Tunisia center coordinates without error modal or retry prompt.

**Rationale**:
- Graceful degradation.
- User can still browse stations from static center.
- No annoying permission retry UX.

**Status**: Decided, pending implementation.

---

## How to Add a New Decision

1. Assign a sequential number (D-NNN).
2. Record the sprint it applies to.
3. Write decision, rationale, and status.
4. Link to any ADRs or constitution sections that relate.
5. Do not edit decisions after they are recorded. If superseded, create a new decision that references the old one.
