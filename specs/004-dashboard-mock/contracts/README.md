# Mock Data Contracts

**Feature**: Dashboard App with Mock Data (Sprint 1.4)
**Date**: 2026-06-06

## Overview

This document describes the mock data contracts for the dashboard application. All mock data is stored in TypeScript files under `apps/dashboard/src/mocks/` and is designed to be replaced by real API calls in Phase 5.

## Purpose

Mock data serves two purposes in this phase:

1. **UI Development**: Provides realistic data to populate screens and test component rendering
2. **User Experience**: Enables testing of user flows and interactions without backend dependencies

## Data Files

### partners.ts

**Purpose**: Mock data for partner entities.

**Structure**:
```typescript
import { Partner } from '../types';

export const mockPartners: Partner[] = [
  // 5 partner entities
];

export const getPartnerById = (id: string): Partner | undefined => { ... };
export const getPartnersByStatus = (status: string): Partner[] => { ... };
```

**Exported Functions**:
- `mockPartners`: Array of 5 partner mock entities
- `getPartnerById(id)`: Retrieve partner by ID
- `getPartnersByStatus(status)`: Filter partners by status

**Usage**: Used in Partners screen (admin) and for partner authentication.

---

### stations.ts

**Purpose**: Mock data for station entities. Reuses same 15 stations from driver apps.

**Structure**:
```typescript
import { Station } from '../types';

export const mockStations: Station[] = [
  // 15 station entities (reused from driver apps)
];

export const getStationsByPartner = (partnerId: string): Station[] => { ... };
export const getStationById = (id: string): Station | undefined => { ... };
export const getStationsByStatus = (status: string): Station[] => { ... };
```

**Exported Functions**:
- `mockStations`: Array of 15 station mock entities
- `getStationsByPartner(partnerId)`: Filter stations by partner ID
- `getStationById(id)`: Retrieve station by ID
- `getStationsByStatus(status)`: Filter stations by status

**Usage**: Used in Overview, My Stations, Stations screens, and station-related tables.

---

### chargers.ts

**Purpose**: Mock data for charger entities. Reuses same chargers from driver apps.

**Structure**:
```typescript
import { Charger } from '../types';

export const mockChargers: Charger[] = [
  // 50+ charger entities (reused from driver apps)
];

export const getChargersByStation = (stationId: string): Charger[] => { ... };
export const getChargerById = (id: string): Charger | undefined => { ... };
export const getChargersByType = (type: string): Charger[] => { ... };
```

**Exported Functions**:
- `mockChargers`: Array of 50+ charger mock entities
- `getChargersByStation(stationId)`: Filter chargers by station ID
- `getChargerById(id)`: Retrieve charger by ID
- `getChargersByType(type)`: Filter chargers by connector type

**Usage**: Used in Charger Management, Chargers screens, and charger-related tables.

---

### users.ts

**Purpose**: Mock data for user entities.

**Structure**:
```typescript
import { User } from '../types';

export const mockUsers: User[] = [
  // 10 user entities
];

export const getUserById = (id: string): User | undefined => { ... };
export const getUsersByRole = (role: UserRole): User[] => { ... };
export const getUsersByStatus = (status: string): User[] => { ... };
```

**Exported Functions**:
- `mockUsers`: Array of 10 user mock entities
- `getUserById(id)`: Retrieve user by ID
- `getUsersByRole(role)`: Filter users by role
- `getUsersByStatus(status)`: Filter users by status

**Usage**: Used in Users screen and for mock authentication.

---

### reviews.ts

**Purpose**: Mock data for review entities. Reuses same reviews from driver apps.

**Structure**:
```typescript
import { Review } from '../types';

export const mockReviews: Review[] = [
  // 60+ review entities (reused from driver apps)
];

export const getReviewsByStation = (stationId: string): Review[] => { ... };
export const getReviewsByUser = (userId: string): Review[] => { ... };
export const getReviewById = (id: string): Review | undefined => { ... };
```

**Exported Functions**:
- `mockReviews`: Array of 60+ review mock entities
- `getReviewsByStation(stationId)`: Filter reviews by station ID
- `getReviewsByUser(userId)`: Filter reviews by user ID
- `getReviewById(id)`: Retrieve review by ID

**Usage**: Used in Reviews screen, station detail views, and report calculations.

---

### reports.ts

**Purpose**: Mock data for report/stat card entities.

**Structure**:
```typescript
import { Report } from '../types';

export const mockPartnerReports: Report[] = [
  // 4-5 report entities for partner overview
];

export const mockAdminReports: Report[] = [
  // 6-7 report entities for admin overview
];

export const getPartnerReports = (): Report[] => { ... };
export const getAdminReports = (): Report[] => { ... };
```

**Exported Functions**:
- `mockPartnerReports`: Array of 4-5 report entities for partner overview
- `mockAdminReports`: Array of 6-7 report entities for admin overview
- `getPartnerReports()`: Retrieve partner reports
- `getAdminReports()`: Retrieve admin reports

**Usage**: Used in Overview screens and Reports screens for KPI cards.

---

## Data Relationships

The mock data maintains the following relationships:

```
Partner (5) ────< Station (15)
   │                     │
   │                (1) │
   │                     ▼
   │               Charger (50+)
   │
   └───< User (10)
            │
            │ (N)
            ▼
         Review (60+)
            │
            │ (1)
            ▼
        Station (15)
```

**Constraints**:
- Every station has a valid `partnerId` referencing an existing partner
- Every charger has a valid `stationId` referencing an existing station
- Every review has valid `stationId` and `userId` references
- Partner users have a `partnerId` referencing their partner

---

## API Contract (Future - Phase 5)

In Phase 5, these mock data contracts will be replaced by REST API endpoints:

### Partner Endpoints

```
GET    /api/partners              - List all partners (admin only)
GET    /api/partners/:id          - Get partner by ID
POST   /api/partners              - Create partner (admin only)
PUT    /api/partners/:id          - Update partner
DELETE /api/partners/:id          - Delete partner (admin only)
```

### Station Endpoints

```
GET    /api/stations              - List all stations (admin) or stations by partner (partner)
GET    /api/stations/:id          - Get station by ID
POST   /api/stations              - Create station
PUT    /api/stations/:id          - Update station
DELETE /api/stations/:id          - Delete station
```

### Charger Endpoints

```
GET    /api/chargers              - List all chargers
GET    /api/chargers/:id          - Get charger by ID
POST   /api/chargers              - Create charger
PUT    /api/chargers/:id          - Update charger (status updates)
DELETE /api/chargers/:id          - Delete charger
```

### User Endpoints

```
GET    /api/users                 - List all users (admin only)
GET    /api/users/:id             - Get user by ID
POST   /api/users                 - Create user (admin only)
PUT    /api/users/:id             - Update user
DELETE /api/users/:id             - Delete user (admin only)
```

### Review Endpoints

```
GET    /api/reviews               - List all reviews (admin) or reviews by station
GET    /api/reviews/:id           - Get review by ID
POST   /api/reviews               - Create review
PUT    /api/reviews/:id           - Update review
DELETE /api/reviews/:id           - Delete review
```

### Report Endpoints

```
GET    /api/reports/partner       - Get partner reports (partner)
GET    /api/reports/admin         - Get admin reports (admin)
```

---

## Mock Data Generation

Mock data is generated manually in TypeScript files to ensure:

1. **Type Safety**: TypeScript interfaces prevent data structure errors
2. **Consistency**: All entities follow the same validation rules
3. **Realism**: Addresses use real Tunisian locations and names
4. **Reusability**: Stations, chargers, and reviews are reused from driver apps

---

## Migration Strategy (Phase 5)

When transitioning from mock data to real API:

1. **Replace Imports**: Change imports from `./mocks/*` to API service calls
2. **Add Loading States**: Implement loading indicators during API calls
3. **Add Error Handling**: Handle API failures gracefully with error states
4. **Add Caching**: Cache frequently accessed data (stations, partners) to reduce API calls
5. **Update Role Context**: Replace mock role context with Keycloak authentication
6. **Remove Dev Toggle**: Remove dev-only role switcher UI control

---

## Validation

All mock data files must pass TypeScript compilation with zero errors.

TypeScript interfaces are defined in `apps/dashboard/src/types/index.ts` and must be used consistently across all mock data files.