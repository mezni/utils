# Data Model: Dashboard Admin View

This feature does not introduce new backend entities. It consumes the four existing API resources from `source/mock/db.json`. The data model below documents the frontend state shapes and API response contracts consumed by the Dashboard App.

## API Resources Consumed

### Partner

| Field | Type | Source |
|-------|------|--------|
| id | string | `PRT-...` NanoID |
| name | string | Display name |
| type | `"business" | "personal"` | Partner type |
| is_verified | boolean | Admin-verified flag |
| is_live | boolean | Has visible stations |
| is_active | boolean | Account enabled |
| created_at | ISO 8601 | Audit trail |
| created_by | string | `USR-...` |
| updated_at | ISO 8601 | Audit trail |
| updated_by | string | `USR-...` |

**API endpoints**:
- `GET /api/partners` — list all
- `GET /api/partners/:id` — single
- `POST /api/partners` — create
- `PATCH /api/partners/:id` — update
- `DELETE /api/partners/:id` — delete

### Station

| Field | Type | Source |
|-------|------|--------|
| id | string | `STN-...` NanoID |
| partner_id | string | FK to Partner |
| name | string | Display name |
| address | string | Street address |
| latitude | number | -90 to 90 |
| longitude | number | -180 to 180 |
| created_at | ISO 8601 | Audit trail |
| created_by | string | `USR-...` |
| updated_at | ISO 8601 | Audit trail |
| updated_by | string | `USR-...` |

**API endpoints**:
- `GET /api/stations` — list all
- `GET /api/stations?partner_id=:id` — filter by partner
- `GET /api/stations/:id` — single
- `POST /api/stations` — create
- `PATCH /api/stations/:id` — update
- `DELETE /api/stations/:id` — delete

### Charger

| Field | Type | Source |
|-------|------|--------|
| id | string | `CHG-...` NanoID |
| station_id | string | FK to Station |
| connector_type | `"type2" | "ccs" | "chademo" | "type1"` | Connector standard |
| power_kw | number | Power output in kW |
| status | `"available" | "in_use" | "maintenance" | "offline"` | Operational status |
| created_at | ISO 8601 | Audit trail |
| created_by | string | `USR-...` |
| updated_at | ISO 8601 | Audit trail |
| updated_by | string | `USR-...` |

**API endpoints**:
- `GET /api/chargers` — list all
- `GET /api/chargers?station_id=:id` — filter by station
- `GET /api/chargers/:id` — single
- `POST /api/chargers` — create
- `PATCH /api/chargers/:id` — update
- `DELETE /api/chargers/:id` — delete

### Station Availability

| Field | Type | Source |
|-------|------|--------|
| id | string | Auto-generated |
| station_id | string | FK to Station |
| status | `"available" | "partial" | "unavailable"` | Station availability |
| updated_by | string | `USR-...` |
| updated_at | ISO 8601 | Timestamp |

**API endpoints**:
- `GET /api/station_availability` — list all
- `GET /api/station_availability?station_id=:id` — filter by station

(Append-only resource — read by Partner View in Sprint 1.3, not by Admin View)

## Frontend State Shapes

### Dev Role Context

```typescript
interface RoleContextState {
  role: 'admin' | 'partner';
  selectedPartnerId: string | null;
  setRole: (role: 'admin' | 'partner') => void;
  setSelectedPartnerId: (id: string | null) => void;
}
```

### Page State (used by every admin screen)

```typescript
interface PageState<T> {
  data: T[];
  loading: boolean;
  error: string | null;
}
```

### Form State (used by every CRUD modal)

```typescript
interface FormModalState<T> {
  isOpen: boolean;
  mode: 'create' | 'edit';
  item: T | null; // null for create, existing item for edit
  errors: Record<string, string>; // field name → error message
}
```

## Computed / Derived Fields

- **Station charger count**: `GET /api/chargers?station_id={stationId}` → array length
- **Partner station count**: `GET /api/stations?partner_id={partnerId}` → array length
- **Partner charger count**: sum of all chargers across partner's stations
- **Overview stat cards**: total partners length, total stations length, total chargers length

## State Transitions

| Entity | Action | Before | After |
|--------|--------|--------|-------|
| Partner | Verify | is_verified=false | is_verified=true |
| Partner | Deactivate | is_active=true | is_active=false |
| Partner | Reactivate | is_active=false | is_active=true |
| Station | Edit | old values | updated values |
| Charger | Edit | old values | updated values |
