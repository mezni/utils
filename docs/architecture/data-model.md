# Data Model Architecture

## Version: 1.0
## Status: Active
## Core Philosophy: Documentation is the system. Code is just its execution.

---

## 🗄️ OVERVIEW

BorneMap uses a multi-database architecture with clear ownership rules and constraints. Each database serves a specific purpose with defined access patterns.

---

## 🏗️ DATABASE LAYER

```
┌─────────────────────────────────────────────────────────────┐
│                      Data Layer                              │
├─────────────────────────────────────────────────────────────┤
│  🗄️ platform_db (PostgreSQL + PostGIS)                      │
│    ├── inventory schema (system of record)                  │
│    ├── gis schema (read-only)                               │
│    └── users schema (auth-service metadata)                 │
│                                                              │
│  📊 analytics_db (append-only events)                       │
│                                                              │
│  🔑 keycloak_db (internal only)                             │
└─────────────────────────────────────────────────────────────┘
```

---

## 🗄️ PLATFORM_DB

**Purpose:** System of record for all application data

**Technology:** PostgreSQL + PostGIS (for geospatial data)

**Schemas:**

### 1. INVENTORY SCHEMA

**Owner:** admin-service (write), driver-service (read)

**Contents:**

#### Stations Table
```typescript
interface Station {
  id: string;
  name: string;
  location: {
    lat: number;
    lng: number;
    geometry: Point;  // PostGIS
  };
  address: string;
  description: string;
  amenities: string[];
  status: StationStatus;
  capacity: number;
  operationalHours: {
    start: string;  // "09:00"
    end: string;    // "18:00"
  };
  contact: {
    phone: string;
    email: string;
    website?: string;
  };
  createdAt: Date;
  updatedAt: Date;
  createdBy?: string;  // Admin user ID
}

enum StationStatus {
  ACTIVE = 'active',
  MAINTENANCE = 'maintenance',
  OFFLINE = 'offline',
  CLOSED = 'closed'
}
```

#### Chargers Table
```typescript
interface Charger {
  id: string;
  stationId: string;
  type: ChargerType;
  power: number;  // kW
  connectorType: string;
  status: ChargerStatus;
  createdAt: Date;
  updatedAt: Date;
}

enum ChargerType {
  LEVEL_1 = 'level1',
  LEVEL_2 = 'level2',
  DC_FAST = 'dc_fast'
}

enum ChargerStatus {
  AVAILABLE = 'available',
  OCCUPIED = 'occupied',
  MAINTENANCE = 'maintenance',
  OFFLINE = 'offline'
}
```

**Access Patterns:**
- Read: driver-service, admin-service
- Write: admin-service
- Constraint: No direct modifications by driver-service

**Constraints:**
- Station status cannot be changed by driver-service
- Only admin-service can create/update stations
- Audit trail required (createdBy, updatedAt)

---

### 2. GIS SCHEMA

**Owner:** Read-only

**Contents:**

#### Map Data Table
```typescript
interface MapFeature {
  id: string;
  featureType: string;  // 'station', 'road', 'poi'
  geometry: Geometry;   // PostGIS geometry
  properties: Record<string, any>;
  metadata: {
    layer: string;
    visibility: boolean;
  };
}

type Geometry =
  | Point
  | LineString
  | Polygon
  | MultiPoint
  | MultiLineString
  | MultiPolygon;
```

**Access Patterns:**
- Read: driver-service, admin-service
- Write: None (read-only)

**Constraints:**
- **NO MODIFICATIONS ALLOWED**
- No deletions
- No updates
- Used only for reference
- Archival strategy required

**Architectural Rule:** Never delete from gis schema

---

### 3. USERS SCHEMA

**Owner:** auth-service (metadata), admin-service (read)

**Contents:**

#### Users Table
```typescript
interface User {
  id: string;
  email: string;
  username?: string;
  firstName: string;
  lastName: string;
  phone?: string;
  profileImage?: string;
  role: UserRole;
  status: UserStatus;
  preferences: UserPreferences;
  createdAt: Date;
  updatedAt: Date;
  lastLoginAt?: Date;
  lastLogoutAt?: Date;
}

enum UserRole {
  PUBLIC = 'public',
  DRIVER = 'driver',
  PARTNER = 'partner',
  ADMIN = 'admin'
}

enum UserStatus {
  ACTIVE = 'active',
  INACTIVE = 'inactive',
  SUSPENDED = 'suspended'
}

interface UserPreferences {
  notifications: {
    email: boolean;
    push: boolean;
  };
  language: string;
  theme: 'light' | 'dark' | 'auto';
}
```

**Access Patterns:**
- Read: auth-service, admin-service
- Write: auth-service (for authentication operations)
- Constraint: No direct access from driver-service

**Constraints:**
- No direct database access from driver-service
- Password hash stored in Keycloak, not in platform_db
- Only auth-service can modify authentication data
- Admin-service can only read user metadata

---

## 📊 ANALYTICS_DB

**Purpose:** Append-only analytics storage

**Technology:** PostgreSQL or specialized analytics database

**Contents:**

### Event Table
```typescript
interface AnalyticsEvent {
  id: string;
  eventType: string;
  userId?: string;
  sessionId: string;
  sessionId?: string;  // Optional for anonymous events
  data: Record<string, any>;
  metadata: {
    timestamp: Date;
    device?: string;
    location?: {
      lat: number;
      lng: number;
    };
    sessionDuration?: number;
  };
}

interface AnalyticsEvent {
  // Station Events
  'station_viewed': {
    stationId: string;
    searchPerformed?: boolean;
  };

  // Nearby Search Events
  'nearby_search_performed': {
    location: {
      lat: number;
      lng: number;
    };
    radius: number;
    resultsCount: number;
  };

  // Station Detail Events
  'station_detail_viewed': {
    stationId: string;
    featuresViewed?: string[];
  };

  // Map Interaction Events
  'map_tap': {
    location: {
      lat: number;
      lng: number;
    };
    stationTapped?: string;
  };
}
```

**Access Patterns:**
- Write: driver-service, admin-service, auth-service
- Read: admin-service (analytics)

**Constraints:**
- **APPEND-ONLY, NO DELETIONS**
- No modifications to existing events
- No direct reads from driver-service (only admin-service)
- High volume of inserts
- Archival strategy required

**Architectural Rule:** Never delete from analytics_db

**Retention Policy:**
- Critical events: 2 years
- Standard events: 1 year
- Archival to cold storage after retention period

---

## 🔑 KEYCLOAK_DB

**Purpose:** Internal authentication database

**Technology:** PostgreSQL (internal to Keycloak)

**Contents:**

### User Authentication Data
```typescript
interface KeycloakUser {
  id: string;
  username: string;
  email: string;
  emailVerified: boolean;
  firstName: string;
  lastName: string;
  enabled: boolean;
  attributes: Record<string, string[]>;
  credentials: Array<{
    id: string;
    type: 'password' | 'totp' | 'otp';
    secret?: string;
    createdDate: Date;
    value?: string;
  }>;
  realmId: string;
  totp: {
    enabled: boolean;
    config: any;
  };
  // Other Keycloak-specific fields
}
```

**Access Patterns:**
- Internal: Keycloak service only
- No external access
- No direct database access from services

**Constraints:**
- **INTERNAL ONLY**
- No external access
- No direct database access from services
- Keycloak manages all authentication operations

**Architectural Rule:** Never access keycloak_db directly

---

## 🔄 DATA FLOW

### Station Discovery Flow

```
1. Driver Service receives request
2. Queries platform_db.inventory for stations
3. Uses platform_db.gis for location data (read-only)
4. Returns station data to frontend
5. Logs analytics event to analytics_db (append-only)
```

### Authentication Flow

```
1. User submits credentials
2. Auth Service validates with Keycloak (internal)
3. Keycloak returns JWT token
4. Auth Service returns token to frontend
5. Frontend uses token for subsequent requests
6. Driver Service validates token before processing
```

### Admin Management Flow

```
1. Admin Service receives request
2. Validates user authentication
3. Validates user authorization
4. Modifies platform_db.inventory or platform_db.users
5. Logs audit trail
6. Logs analytics event to analytics_db (append-only)
```

---

## 🛡️ DATA SECURITY

### Database Security

**Access Control:**
- Service-specific database access
- Role-based access control
- No shared database access patterns

**Connection Security:**
- TLS encrypted connections
- Connection pooling
- Connection limits
- Timeout handling

**Data Protection:**
- No sensitive data in plaintext
- Passwords stored in Keycloak only
- No hardcoded credentials

---

## ⚡ PERFORMANCE OPTIMIZATION

### Database Optimization

**Indexing:**
- Station location indexes (PostGIS)
- Station status indexes
- User email indexes
- Timestamp indexes

**Query Optimization:**
- Efficient joins
- Avoid N+1 queries
- Use database functions
- Query result limiting

**Caching:**
- Frequently accessed data
- Station lists
- Station details
- User profiles

---

## 🧪 DATA VALIDATION

### Input Validation

**All inputs validated before database operations:**
- SQL injection prevention
- XSS prevention
- Data type validation
- Business rule validation

### Schema Validation

**TypeScript interfaces for:**
- All database entities
- API request/response shapes
- Event data structures

**Zod validation for:**
- API inputs
- Database updates
- Event payloads

---

## 🔄 DOCUMENTATION IS SYSTEM

**Data model architecture rules are documented here.**
**Code must implement documented architecture.**
**Documentation must be updated with changes.**

**Documentation is the system. Code is just its execution.**
