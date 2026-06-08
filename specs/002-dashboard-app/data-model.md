# Data Model: Dashboard App

**Date**: June 8, 2026

**Status**: Phase 1 Design Output

---

## Overview

The Dashboard manages three core entities from the BorneMap inventory: **Partner**, **Station**, and **Charger**. All entities are persisted server-side in PostgreSQL via the FastAPI backend. The Dashboard frontend is a read-write client that fetches, displays, and submits mutations to the API.

---

## Entity Definitions

### Partner

**Purpose**: Represents an EV charging network operator or company that owns and manages stations.

**Table**: `inventory.partner` (PostgreSQL)

| Field | Type | Nullable | Notes |
|-------|------|----------|-------|
| id | UUID | No | Primary key, auto-generated v4 |
| name | string (varchar 255) | No | Operator/company name |
| created_at | timestamp | No | UTC, auto-set by server |

**Constraints**:
- name: required, 1-255 characters
- id: unique, immutable

**Relationships**:
- Has many **Stations** (one-to-many via station.partner_id)

**Dashboard Lifecycle**:
- **Create**: POST `/api/v1/partners` with `name`
- **Read**: GET `/api/v1/partners` (list), GET `/api/v1/partners/{id}` (detail)
- **Update**: PUT `/api/v1/partners/{id}` with updated `name`
- **Delete**: DELETE `/api/v1/partners/{id}` (may cascade to stations per backend logic)

**Form Model** (TypeScript):
```typescript
interface PartnerCreatePayload {
  name: string;  // 1-255 chars
}

interface PartnerUpdatePayload {
  name: string;  // 1-255 chars
}

interface Partner {
  id: string;           // UUID
  name: string;
  created_at: string;   // ISO8601
}
```

**Client-Side Validation**:
```typescript
function validatePartnerName(name: string): string | null {
  if (!name || name.trim().length === 0) return "Partner name is required";
  if (name.length > 255) return "Partner name must be 255 characters or less";
  return null;
}
```

---

### Station

**Purpose**: Represents a physical EV charging location with geographic coordinates and associated chargers.

**Table**: `inventory.station` (PostgreSQL)

| Field | Type | Nullable | Notes |
|-------|-------|----------|-------|
| id | UUID | No | Primary key, auto-generated v4 |
| partner_id | UUID | No | Foreign key → partner.id |
| name | string (varchar 255) | No | Location name (e.g., "Tunis Central Station") |
| address | string (varchar 500) | No | Street address |
| latitude | float | No | Range: -90 to 90 (decimal degrees) |
| longitude | float | No | Range: -180 to 180 (decimal degrees) |
| created_at | timestamp | No | UTC, auto-set by server |
| updated_at | timestamp | No | UTC, auto-updated on every change |

**Computed Fields** (returned in API responses):
- `charger_count`: int — total chargers at this station
- `available_count`: int — chargers with status "available"

**Constraints**:
- name: required, 1-255 characters
- address: required, 1-500 characters
- latitude: required, -90 ≤ latitude ≤ 90
- longitude: required, -180 ≤ longitude ≤ 180
- partner_id: required, must reference existing partner

**Relationships**:
- Belongs to **Partner** (many-to-one via partner_id)
- Has many **Chargers** (one-to-many via charger.station_id)

**Dashboard Lifecycle**:
- **Create**: POST `/api/v1/stations` with `name`, `address`, `latitude`, `longitude`, `partner_id`
- **Read**: GET `/api/v1/stations` (list with filters), GET `/api/v1/stations/{id}` (detail with charger list), GET `/api/v1/stations/nearby` (proximity search)
- **Update**: PUT `/api/v1/stations/{id}` with updated fields
- **Delete**: DELETE `/api/v1/stations/{id}` (cascades to chargers per backend logic)

**Form Model** (TypeScript):
```typescript
interface StationCreatePayload {
  name: string;           // 1-255 chars
  address: string;        // 1-500 chars
  latitude: number;       // -90 to 90
  longitude: number;      // -180 to 180
  partner_id: string;     // UUID
}

interface StationUpdatePayload {
  name?: string;
  address?: string;
  latitude?: number;
  longitude?: number;
  partner_id?: string;
}

interface Station {
  id: string;             // UUID
  partner_id: string;     // UUID
  name: string;
  address: string;
  latitude: number;
  longitude: number;
  created_at: string;     // ISO8601
  updated_at: string;     // ISO8601
  charger_count: number;  // Computed
  available_count: number; // Computed
}

interface StationWithChargers extends Station {
  chargers: Charger[];    // Full list for detail view
}
```

**Client-Side Validation**:
```typescript
function validateStationName(name: string): string | null {
  if (!name || name.trim().length === 0) return "Station name is required";
  if (name.length > 255) return "Station name must be 255 characters or less";
  return null;
}

function validateAddress(address: string): string | null {
  if (!address || address.trim().length === 0) return "Address is required";
  if (address.length > 500) return "Address must be 500 characters or less";
  return null;
}

function validateLatitude(lat: number | string): string | null {
  const num = typeof lat === 'string' ? parseFloat(lat) : lat;
  if (isNaN(num)) return "Latitude must be a valid number";
  if (num < -90 || num > 90) return "Latitude must be between -90 and 90";
  return null;
}

function validateLongitude(lon: number | string): string | null {
  const num = typeof lon === 'string' ? parseFloat(lon) : lon;
  if (isNaN(num)) return "Longitude must be a valid number";
  if (num < -180 || num > 180) return "Longitude must be between -180 and 180";
  return null;
}

function validatePartnerId(id: string): string | null {
  if (!id) return "Partner selection is required";
  if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(id)) {
    return "Invalid partner selection";
  }
  return null;
}
```

---

### Charger

**Purpose**: Represents an individual charging unit at a station with a specific connector type, power rating, and operational status.

**Table**: `inventory.charger` (PostgreSQL)

| Field | Type | Nullable | Notes |
|-------|-------|----------|-------|
| id | UUID | No | Primary key, auto-generated v4 |
| station_id | UUID | No | Foreign key → station.id |
| connector_type | string (varchar 50) | No | Type: Type2, CCS, CHAdeMO, J1772, etc. |
| power_kw | float | No | Charging power in kilowatts (e.g., 7, 22, 50, 150) |
| status | enum | No | One of: `available`, `in_use`, `maintenance` |
| created_at | timestamp | No | UTC, auto-set by server |
| updated_at | timestamp | No | UTC, auto-updated on every change |

**Status Enum**:
```sql
CREATE TYPE charger_status AS ENUM ('available', 'in_use', 'maintenance');
```

**Dashboard Status Colors** (from design tokens):
- `available`: green (#10B981) — `status.available`
- `in_use`: amber (#F59E0B) — `status.inUse`
- `maintenance`: red (#EF4444) — `status.maintenance`

**Constraints**:
- connector_type: required, one of [Type2, CCS, CHAdeMO, J1772, ...] (backend defines enum)
- power_kw: required, positive number
- status: required, one of [available, in_use, maintenance]
- station_id: required, must reference existing station

**Relationships**:
- Belongs to **Station** (many-to-one via station_id)

**Dashboard Lifecycle**:
- **Create**: POST `/api/v1/chargers` with `station_id`, `connector_type`, `power_kw`, `status`
- **Read**: GET `/api/v1/chargers` (list with optional station_id filter), GET `/api/v1/chargers/{id}` (detail)
- **Update**: PUT `/api/v1/chargers/{id}` with updated fields (status is primary use case)
- **Delete**: DELETE `/api/v1/chargers/{id}`

**Form Model** (TypeScript):
```typescript
type ChargerStatus = 'available' | 'in_use' | 'maintenance';

interface ChargerCreatePayload {
  station_id: string;      // UUID
  connector_type: string;  // Enum value
  power_kw: number;        // Positive number
  status: ChargerStatus;   // One of the three statuses
}

interface ChargerUpdatePayload {
  connector_type?: string;
  power_kw?: number;
  status?: ChargerStatus;
}

interface Charger {
  id: string;              // UUID
  station_id: string;      // UUID
  connector_type: string;
  power_kw: number;
  status: ChargerStatus;
  created_at: string;      // ISO8601
  updated_at: string;      // ISO8601
}

interface ChargerWithStationName extends Charger {
  station_name: string;    // For table display (from station join)
}
```

**Client-Side Validation**:
```typescript
const VALID_CONNECTOR_TYPES = ['Type2', 'CCS', 'CHAdeMO', 'J1772'];

function validateConnectorType(type: string): string | null {
  if (!type) return "Connector type is required";
  if (!VALID_CONNECTOR_TYPES.includes(type)) {
    return `Connector type must be one of: ${VALID_CONNECTOR_TYPES.join(', ')}`;
  }
  return null;
}

function validatePowerKw(power: number | string): string | null {
  const num = typeof power === 'string' ? parseFloat(power) : power;
  if (isNaN(num)) return "Power must be a valid number";
  if (num <= 0) return "Power must be greater than 0 kW";
  return null;
}

function validateStatus(status: string): string | null {
  const validStatuses = ['available', 'in_use', 'maintenance'];
  if (!status) return "Status is required";
  if (!validStatuses.includes(status)) {
    return `Status must be one of: ${validStatuses.join(', ')}`;
  }
  return null;
}

function validateStationId(id: string): string | null {
  if (!id) return "Station selection is required";
  if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(id)) {
    return "Invalid station selection";
  }
  return null;
}
```

---

## API Response Shape

All API responses from the backend follow this shape:

```typescript
// For single resource
interface ApiResponse<T> {
  data: T;
  // or
  // error: { code: string; message: string };
}

// For list resources
interface ApiListResponse<T> {
  data: T[];
  meta?: {
    total: number;
    limit: number;
    offset: number;
  };
}

// List endpoint examples:
// GET /api/v1/partners → ApiListResponse<Partner>
// GET /api/v1/stations?partner_id=UUID → ApiListResponse<Station>
// GET /api/v1/chargers?station_id=UUID → ApiListResponse<ChargerWithStationName>

// Detail endpoint examples:
// GET /api/v1/partners/{id} → Partner
// GET /api/v1/stations/{id} → StationWithChargers
// GET /api/v1/chargers/{id} → Charger
```

---

## Relationships Diagram

```
Partner (1) ──────── (many) Station
  ├─ id                         ├─ id
  ├─ name                       ├─ partner_id (FK)
  └─ created_at                 ├─ name, address
                                ├─ latitude, longitude
                                ├─ charger_count (computed)
                                └─ created_at, updated_at

Station (1) ──────── (many) Charger
                               ├─ id
                               ├─ station_id (FK)
                               ├─ connector_type, power_kw
                               ├─ status (enum)
                               └─ created_at, updated_at
```

---

## State Management Architecture

### Data Fetching (per screen)

**usePartners()** Hook:
```typescript
function usePartners() {
  const [partners, setPartners] = useState<Partner[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<Error | null>(null);

  const fetch = async () => {
    setLoading(true);
    try {
      const res = await api.get('/api/v1/partners');
      setPartners(res.data.data);
      setError(null);
    } catch (err) {
      setError(err);
    } finally {
      setLoading(false);
    }
  };

  const create = async (payload: PartnerCreatePayload) => {
    const res = await api.post('/api/v1/partners', payload);
    setPartners([...partners, res.data.data]);
    return res.data.data;
  };

  const update = async (id: string, payload: PartnerUpdatePayload) => {
    const res = await api.put(`/api/v1/partners/${id}`, payload);
    setPartners(partners.map(p => p.id === id ? res.data.data : p));
    return res.data.data;
  };

  const delete = async (id: string) => {
    await api.delete(`/api/v1/partners/${id}`);
    setPartners(partners.filter(p => p.id !== id));
  };

  return { partners, loading, error, fetch, create, update, delete };
}
```

**Similar patterns** for `useStations()` and `useChargers()` with additional filter support.

### Form State (per form)

**Using React Hook Form**:
```typescript
const { register, handleSubmit, formState: { errors }, watch } = useForm({
  mode: 'onBlur',
  defaultValues: initialValues,
});

const onSubmit = async (data) => {
  try {
    if (isEdit) {
      await updateMutation(id, data);
    } else {
      await createMutation(data);
    }
    closeModal();
  } catch (error) {
    setSubmitError(error.message);
  }
};
```

---

## Error Handling

**Error Codes** (from FastAPI backend):

| Code | HTTP | Meaning | Dashboard Action |
|------|------|---------|------------------|
| 200 | 200 | Success | Update table, close modal, show success toast (optional) |
| 201 | 201 | Created | Update table, close modal |
| 204 | 204 | Deleted | Update table (remove row) |
| 400 | 400 | Bad request | Show generic error, preserve form |
| 422 | 422 | Validation error | Show field-specific errors inline |
| 404 | 404 | Not found | Show "Not found" message, navigate away |
| 500 | 500 | Server error | Show generic error toast |
| 503 | 503 | Service unavailable | Show "API unreachable" ErrorState with retry |

**Error Message Mapping**:
```typescript
function getErrorMessage(error: AxiosError): string {
  if (error.response?.status === 422) {
    // Validation errors → return field-specific messages
    return error.response.data.detail; // Array of field errors
  }
  if (error.response?.status === 404) {
    return "Resource not found";
  }
  if (!error.response) {
    return "API unreachable. Check backend is running on port 8000.";
  }
  return error.response.data?.message || "An error occurred";
}
```

---

## Summary

The data model reflects the backend schema and provides:
- Type safety via TypeScript interfaces
- Client-side validation matching server-side constraints
- Clear error handling and display strategies
- Reusable hooks for data fetching and mutations
- Form state management with React Hook Form
- Color-coded status badges from design tokens

Ready for component and service implementation.
