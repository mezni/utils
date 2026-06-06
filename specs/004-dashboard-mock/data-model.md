# Data Model: Dashboard App with Mock Data

**Feature**: Dashboard App with Mock Data (Sprint 1.4)
**Date**: 2026-06-06

## Overview

This document defines the data model for the dashboard application's mock data. All data is sourced from local TypeScript files and is replaced with real API data in Phase 5. The data model aligns with the entities defined in the specification and reuses the same data structures established in Sprint 1.2 (Driver Web App) where applicable.

## Entity Relationships

```
Partner (1) ────< Station (N)
   │                     │
   │                (1) │
   │                     ▼
   │               Charger (N)
   │
   └───< User (N)
            │
            │ (N)
            ▼
         Review (N)
            │
            │ (1)
            ▼
        Station (N)
```

## Entities

### Partner

**Description**: Organization that owns and operates charging stations. Partners manage their station network through the dashboard.

**Fields**:

| Field | Type | Validation | Notes |
|-------|------|------------|-------|
| id | string | Format: `PRT-{nanoid}` | Unique identifier with NanoID prefix |
| name | string | Required, min 3 chars, max 100 chars | Partner organization name |
| stationCount | number | Required, min 0 | Number of stations owned by partner |
| status | enum | Required, values: `active`, `inactive`, `pending` | Partner account status |
| createdAt | string | Required, ISO 8601 date-time | Partner registration timestamp |

**Example**:
```typescript
{
  id: "PRT-3A7K8L2M9",
  name: "Tunisie Électricité",
  stationCount: 5,
  status: "active",
  createdAt: "2024-01-15T08:30:00Z"
}
```

---

### Station

**Description**: Charging location with coordinates, address, and charger count. Stations belong to partners and are visible to drivers.

**Fields**:

| Field | Type | Validation | Notes |
|-------|------|------------|-------|
| id | string | Required, Format: `STN-{nanoid}` | Unique identifier |
| name | string | Required, min 3 chars, max 100 chars | Station name |
| address | string | Required, min 5 chars, max 200 chars | Physical address |
| latitude | number | Required, range: 32.0 - 37.0 (Tunisia bounds) | Y coordinate |
| longitude | number | Required, range: 8.0 - 11.5 (Tunisia bounds) | X coordinate |
| partnerId | string | Required, foreign key to Partner | Owning partner |
| chargerCount | number | Required, min 0 | Total chargers at this station |
| status | enum | Required, values: `available`, `in-use`, `maintenance` | Station operational status |
| availability | number | Required, range: 0-100 | Percentage of chargers available |
| reviews | number | Required, min 0 | Total number of reviews |
| averageRating | number | Required, range: 1-5, step: 0.1 | Average star rating |

**Example**:
```typescript
{
  id: "STN-4B8N2P6Q9",
  name: "Centre Urbain Nord",
  address: "Avenue Habib Bourguiba, Tunis",
  latitude: 36.8008,
  longitude: 10.1859,
  partnerId: "PRT-3A7K8L2M9",
  chargerCount: 4,
  status: "available",
  availability: 75,
  reviews: 24,
  averageRating: 4.2
}
```

---

### Charger

**Description**: Charging connector with type, power rating, and status. Chargers belong to stations and provide the actual charging infrastructure.

**Fields**:

| Field | Type | Validation | Notes |
|-------|------|------------|-------|
| id | string | Required, Format: `CHG-{nanoid}` | Unique identifier |
| stationId | string | Required, foreign key to Station | Parent station |
| connectorType | enum | Required, values: `Type2`, `CCS`, `CHAdeMO`, `Tesla` | Connector standard |
| powerRating | number | Required, min 7, max 350 | Power in kW |
| status | enum | Required, values: `available`, `in-use`, `offline`, `maintenance` | Charger operational status |

**Example**:
```typescript
{
  id: "CHG-5C9O3R7S0",
  stationId: "STN-4B8N2P6Q9",
  connectorType: "Type2",
  powerRating: 22,
  status: "available"
}
```

---

### User

**Description**: Platform user with role-based access. Users can be partners, admins, or registered drivers.

**Fields**:

| Field | Type | Validation | Notes |
|-------|------|------------|-------|
| id | string | Required, Format: `USR-{nanoid}` | Unique identifier |
| name | string | Required, min 2 chars, max 50 chars | User full name |
| email | string | Required, email format | User email address |
| role | enum | Required, values: `partner`, `admin`, `registered_driver` | User role for access control |
| status | enum | Required, values: `active`, `inactive`, `suspended` | Account status |
| partnerId | string | Optional, foreign key to Partner | Associated partner (if partner role) |
| createdAt | string | Required, ISO 8601 date-time | Account creation timestamp |

**Example**:
```typescript
{
  id: "USR-6D1P4S8T1",
  name: "Ahmed Ben Ali",
  email: "ahmed.benali@example.tn",
  role: "partner",
  status: "active",
  partnerId: "PRT-3A7K8L2M9",
  createdAt: "2024-02-10T14:22:00Z"
}
```

---

### Review

**Description**: User feedback for a station with rating, text, and date. Reviews are written by users and associated with stations.

**Fields**:

| Field | Type | Validation | Notes |
|-------|------|------------|-------|
| id | string | Required, Format: `REV-{nanoid}` | Unique identifier |
| stationId | string | Required, foreign key to Station | Reviewed station |
| userId | string | Required, foreign key to User | Review author |
| rating | number | Required, range: 1-5, integer | Star rating (1-5) |
| text | string | Required, min 10 chars, max 500 chars | Review content |
| date | string | Required, ISO 8601 date-time | Review timestamp |
| language | enum | Required, values: `ar`, `fr`, `en` | Review language |

**Example**:
```typescript
{
  id: "REV-7E2Q5T9U2",
  stationId: "STN-4B8N2P6Q9",
  userId: "USR-8F3R6U0V3",
  rating: 5,
  text: "ممتاز! محطة نظيفة وسريعة.",
  date: "2024-03-05T16:45:00Z",
  language: "ar"
}
```

---

### Report

**Description**: Statistical metric for dashboard KPI cards. Reports aggregate data for overview screens.

**Fields**:

| Field | Type | Validation | Notes |
|-------|------|------------|-------|
| id | string | Required, unique | Report identifier (e.g., "total_stations") |
| label | string | Required, min 3 chars, max 30 chars | Display label |
| value | number | Required, min 0 | Metric value |
| trend | enum | Optional, values: `up`, `down`, `neutral` | Directional indicator |
| trendValue | number | Optional, min 0 | Percentage change |

**Example**:
```typescript
{
  id: "total_stations",
  label: "Total Stations",
  value: 15,
  trend: "up",
  trendValue: 12.5
}
```

---

## Data Validation Rules

### Partner Validation
- ID must follow `PRT-{nanoid}` format
- Status must be one of: `active`, `inactive`, `pending`
- `stationCount` must be >= 0

### Station Validation
- ID must follow `STN-{nanoid}` format
- Coordinates must be within Tunisia bounds (lat: 32-37, lon: 8-11.5)
- `availability` must be between 0 and 100
- `averageRating` must be between 1 and 5

### Charger Validation
- ID must follow `CHG-{nanoid}` format
- `connectorType` must be one of: `Type2`, `CCS`, `CHAdeMO`, `Tesla`
- `powerRating` must be between 7 and 350 kW

### User Validation
- ID must follow `USR-{nanoid}` format
- `role` must be one of: `partner`, `admin`, `registered_driver`
- If `role` is `partner`, `partnerId` is required
- Email must be valid email format

### Review Validation
- ID must follow `REV-{nanoid}` format
- `rating` must be integer between 1 and 5
- `language` must be one of: `ar`, `fr`, `en`
- `text` must be between 10 and 500 characters

---

## Data Consistency Rules

1. **Station-Partner Relationship**: Every station must have a valid `partnerId` that references an existing partner.
2. **Charger-Station Relationship**: Every charger must have a valid `stationId` that references an existing station.
3. **Review-Station-User Relationship**: Every review must have valid `stationId` and `userId` references.
4. **User-Partner Relationship**: If a user has `role: "partner"`, they must have a `partnerId` that references an existing partner.
5. **Station Availability Calculation**: `availability` should approximately match the ratio of available chargers to total chargers (mock approximation is acceptable).

---

## Mock Data Volumes

Per specification assumptions:

| Entity | Count | Source |
|--------|-------|--------|
| Partners | 5 | New mock data file |
| Stations | 15 | Reused from driver apps |
| Chargers | 50+ | Reused from driver apps (~3-4 per station) |
| Users | 10 | New mock data file |
| Reviews | 60+ | Reused from driver apps (~3-5 per station) |
| Reports | 7-10 | New mock data file (stat cards for overview screens) |

---

## TypeScript Interfaces

All interfaces will be defined in `apps/dashboard/src/types/index.ts`:

```typescript
export interface Partner {
  id: string;
  name: string;
  stationCount: number;
  status: 'active' | 'inactive' | 'pending';
  createdAt: string;
}

export interface Station {
  id: string;
  name: string;
  address: string;
  latitude: number;
  longitude: number;
  partnerId: string;
  chargerCount: number;
  status: 'available' | 'in-use' | 'maintenance';
  availability: number;
  reviews: number;
  averageRating: number;
}

export interface Charger {
  id: string;
  stationId: string;
  connectorType: 'Type2' | 'CCS' | 'CHAdeMO' | 'Tesla';
  powerRating: number;
  status: 'available' | 'in-use' | 'offline' | 'maintenance';
}

export interface User {
  id: string;
  name: string;
  email: string;
  role: 'partner' | 'admin' | 'registered_driver';
  status: 'active' | 'inactive' | 'suspended';
  partnerId?: string;
  createdAt: string;
}

export interface Review {
  id: string;
  stationId: string;
  userId: string;
  rating: number;
  text: string;
  date: string;
  language: 'ar' | 'fr' | 'en';
}

export interface Report {
  id: string;
  label: string;
  value: number;
  trend?: 'up' | 'down' | 'neutral';
  trendValue?: number;
}

export type UserRole = 'partner' | 'admin' | 'registered_driver';
```