# Contract: Core Service Events

**Path**: `services/core-service/src/events/`
**Consumers**: analytics-service, external integrations, audit systems
**Source**: Constitution Principle III, spec FR-003

**Implementation**: Events will be implemented using Rust structs with serde serialization and published via lapin (RabbitMQ client).

## Event Publishing

The core-service publishes domain events using the outbox pattern (Constitution Principle III). All events are written to the PostgreSQL `outbox` table in the same transaction as the business mutation, then published to RabbitMQ by a relay worker.

## Event Format

All events follow this standard format:

```json
{
  "event_id": "evt_abc123def456ghi789",
  "event_type": "CompanyCreated",
  "aggregate_type": "Company",
  "aggregate_id": "CMP-abc123def",
  "version": 1,
  "timestamp": "2026-05-23T10:00:00Z",
  "correlation_id": "corr_abc123def456",
  "causation_id": "caus_abc123def456",
  "user_id": "USR-pqr345stu",
  "user_email": "user@example.com",
  "payload": {
    // Event-specific data
  },
  "metadata": {
    "source": "core-service",
    "version": "1.0.0",
    "ip_address": "192.168.1.100"
  }
}
```

## Event Types

### Company Events

#### CompanyCreated

Emitted when a new company is created.

**Event Type**: `CompanyCreated`

**Aggregate Type**: `Company`

**Payload**:
```json
{
  "id": "CMP-abc123def",
  "name": "Tunisia EV Charging",
  "description": "Leading EV charging network in Tunisia",
  "email": "contact@tunisiaev.tn",
  "phone": "+216-71-123-456",
  "website": "https://tunisiaev.tn",
  "address": "123 Avenue Habib Bourguiba, Tunis, Tunisia",
  "logo_url": "https://tunisiaev.tn/logo.png",
  "is_active": true
}
```

#### CompanyUpdated

Emitted when a company is updated.

**Event Type**: `CompanyUpdated`

**Aggregate Type**: `Company`

**Payload**:
```json
{
  "id": "CMP-abc123def",
  "name": "Tunisia EV Charging Network",
  "description": "Updated description",
  "email": "updated@tunisiaev.tn",
  "phone": "+216-71-123-456",
  "website": "https://tunisiaev.tn",
  "address": "123 Avenue Habib Bourguiba, Tunis, Tunisia",
  "logo_url": "https://tunisiaev.tn/logo.png",
  "is_active": true,
  "changes": {
    "name": {
      "from": "Tunisia EV Charging",
      "to": "Tunisia EV Charging Network"
    },
    "description": {
      "from": "Leading EV charging network in Tunisia",
      "to": "Updated description"
    },
    "email": {
      "from": "contact@tunisiaev.tn",
      "to": "updated@tunisiaev.tn"
    }
  }
}
```

#### CompanyDeleted

Emitted when a company is soft-deleted.

**Event Type**: `CompanyDeleted`

**Aggregate Type**: `Company`

**Payload**:
```json
{
  "id": "CMP-abc123def",
  "name": "Tunisia EV Charging Network",
  "deleted_at": "2026-05-23T10:01:00Z"
}
```

### Station Events

#### StationCreated

Emitted when a new station is created.

**Event Type**: `StationCreated`

**Aggregate Type**: `Station`

**Payload**:
```json
{
  "id": "STA-def456ghi",
  "company_id": "CMP-abc123def",
  "name": "Tunis Mall Charging Station",
  "description": "Fast charging station at Tunis Mall",
  "address": "Tunis Mall, Avenue Habib Bourguiba, Tunis, Tunisia",
  "latitude": 36.8065,
  "longitude": 10.1815,
  "phone": "+216-71-123-456",
  "email": "tunismall@tunisiaev.tn",
  "website": "https://tunisiamall.tn",
  "access_type": "public",
  "operating_hours": {
    "monday": "08:00-22:00",
    "tuesday": "08:00-22:00",
    "wednesday": "08:00-22:00",
    "thursday": "08:00-22:00",
    "friday": "08:00-22:00",
    "saturday": "09:00-20:00",
    "sunday": "09:00-20:00"
  },
  "amenities": ["restroom", "cafe", "wifi", "parking"],
  "is_active": true
}
```

#### StationUpdated

Emitted when a station is updated.

**Event Type**: `StationUpdated`

**Aggregate Type**: `Station`

**Payload**:
```json
{
  "id": "STA-def456ghi",
  "company_id": "CMP-abc123def",
  "name": "Tunis Mall Premium Charging Station",
  "description": "Updated description",
  "address": "Tunis Mall, Avenue Habib Bourguiba, Tunis, Tunisia",
  "latitude": 36.8065,
  "longitude": 10.1815,
  "phone": "+216-71-123-456",
  "email": "tunismall@tunisiaev.tn",
  "website": "https://tunisiamall.tn",
  "access_type": "public",
  "operating_hours": {
    "monday": "08:00-22:00",
    "tuesday": "08:00-22:00",
    "wednesday": "08:00-22:00",
    "thursday": "08:00-22:00",
    "friday": "08:00-22:00",
    "saturday": "09:00-20:00",
    "sunday": "09:00-20:00"
  },
  "amenities": ["restroom", "cafe", "wifi", "parking", "restaurant"],
  "is_active": true,
  "changes": {
    "name": {
      "from": "Tunis Mall Charging Station",
      "to": "Tunis Mall Premium Charging Station"
    },
    "description": {
      "from": "Fast charging station at Tunis Mall",
      "to": "Updated description"
    },
    "amenities": {
      "from": ["restroom", "cafe", "wifi", "parking"],
      "to": ["restroom", "cafe", "wifi", "parking", "restaurant"]
    }
  }
}
```

#### StationDeleted

Emitted when a station is soft-deleted.

**Event Type**: `StationDeleted`

**Aggregate Type**: `Station`

**Payload**:
```json
{
  "id": "STA-def456ghi",
  "company_id": "CMP-abc123def",
  "name": "Tunis Mall Premium Charging Station",
  "deleted_at": "2026-05-23T10:01:00Z"
}
```

### Charger Events

#### ChargerCreated

Emitted when a new charger is created.

**Event Type**: `ChargerCreated`

**Aggregate Type**: `Charger`

**Payload**:
```json
{
  "id": "CHR-ghi789jkl",
  "station_id": "STA-def456ghi",
  "name": "Fast Charger 1",
  "charger_type": "DCFC",
  "power_kw": 150.0,
  "voltage": 400.0,
  "amperage": 375.0,
  "connectors": [
    {
      "type": "CCS2",
      "power_kw": 150.0,
      "status": "available"
    }
  ],
  "status": "available",
  "network_id": "TN-DC-001",
  "last_maintenance_date": "2026-05-01",
  "next_maintenance_date": "2026-11-01",
  "is_active": true
}
```

#### ChargerUpdated

Emitted when a charger is updated.

**Event Type**: `ChargerUpdated`

**Aggregate Type**: `Charger`

**Payload**:
```json
{
  "id": "CHR-ghi789jkl",
  "station_id": "STA-def456ghi",
  "name": "Fast Charger 1 - Updated",
  "charger_type": "DCFC",
  "power_kw": 150.0,
  "voltage": 400.0,
  "amperage": 375.0,
  "connectors": [
    {
      "type": "CCS2",
      "power_kw": 150.0,
      "status": "available"
    }
  ],
  "status": "available",
  "network_id": "TN-DC-001",
  "last_maintenance_date": "2026-05-01",
  "next_maintenance_date": "2026-11-01",
  "is_active": true,
  "changes": {
    "name": {
      "from": "Fast Charger 1",
      "to": "Fast Charger 1 - Updated"
    }
  }
}
```

#### ChargerDeleted

Emitted when a charger is soft-deleted.

**Event Type**: `ChargerDeleted`

**Aggregate Type**: `Charger`

**Payload**:
```json
{
  "id": "CHR-ghi789jkl",
  "station_id": "STA-def456ghi",
  "name": "Fast Charger 1 - Updated",
  "deleted_at": "2026-05-23T10:01:00Z"
}
```

### User Events

#### UserCreated

Emitted when a new user is created (typically from Keycloak sync).

**Event Type**: `UserCreated`

**Aggregate Type**: `User`

**Payload**:
```json
{
  "id": "USR-pqr345stu",
  "keycloak_id": "keycloak-abc123def",
  "email": "user@example.com",
  "first_name": "John",
  "last_name": "Doe",
  "roles": ["user"],
  "is_active": true
}
```

#### UserUpdated

Emitted when a user is updated.

**Event Type**: `UserUpdated`

**Aggregate Type**: `User`

**Payload**:
```json
{
  "id": "USR-pqr345stu",
  "keycloak_id": "keycloak-abc123def",
  "email": "user@example.com",
  "first_name": "John",
  "last_name": "Doe",
  "roles": ["user", "operator"],
  "is_active": true,
  "changes": {
    "roles": {
      "from": ["user"],
      "to": ["user", "operator"]
    }
  }
}
```

### Review Events

#### ReviewCreated

Emitted when a new review is created.

**Event Type**: `ReviewCreated`

**Aggregate Type**: `Review`

**Payload**:
```json
{
  "id": "REV-mno345pqr",
  "user_id": "USR-pqr345stu",
  "station_id": "STA-def456ghi",
  "rating": 5,
  "title": "Excellent charging experience",
  "comment": "Fast charging, clean facilities, and great amenities. Highly recommended!",
  "is_moderated": false,
  "moderation_status": "pending"
}
```

#### ReviewUpdated

Emitted when a review is updated.

**Event Type**: `ReviewUpdated`

**Aggregate Type**: `Review`

**Payload**:
```json
{
  "id": "REV-mno345pqr",
  "user_id": "USR-pqr345stu",
  "station_id": "STA-def456ghi",
  "rating": 4,
  "title": "Very good charging experience",
  "comment": "Fast charging and clean facilities. Great amenities!",
  "is_moderated": false,
  "moderation_status": "pending",
  "changes": {
    "rating": {
      "from": 5,
      "to": 4
    },
    "title": {
      "from": "Excellent charging experience",
      "to": "Very good charging experience"
    },
    "comment": {
      "from": "Fast charging, clean facilities, and great amenities. Highly recommended!",
      "to": "Fast charging and clean facilities. Great amenities!"
    }
  }
}
```

#### ReviewDeleted

Emitted when a review is deleted.

**Event Type**: `ReviewDeleted`

**Aggregate Type**: `Review`

**Payload**:
```json
{
  "id": "REV-mno345pqr",
  "user_id": "USR-pqr345stu",
  "station_id": "STA-def456ghi",
  "rating": 4,
  "title": "Very good charging experience",
  "deleted_at": "2026-05-23T10:01:00Z"
}
```

#### ReviewModerated

Emitted when a review is moderated.

**Event Type**: `ReviewModerated`

**Aggregate Type**: `Review`

**Payload**:
```json
{
  "id": "REV-mno345pqr",
  "user_id": "USR-pqr345stu",
  "station_id": "STA-def456ghi",
  "rating": 4,
  "title": "Very good charging experience",
  "moderation_status": "approved",
  "moderated_by": "USR-admin678",
  "moderated_at": "2026-05-23T10:02:00Z"
}
```

### Favorite Events

#### FavoriteCreated

Emitted when a user adds a station to favorites.

**Event Type**: `FavoriteCreated`

**Aggregate Type**: `Favorite`

**Payload**:
```json
{
  "id": "FAV-jkl012mno",
  "user_id": "USR-pqr345stu",
  "station_id": "STA-def456ghi",
  "note": "My favorite charging spot at the mall"
}
```

#### FavoriteUpdated

Emitted when a user updates a favorite.

**Event Type**: `FavoriteUpdated`

**Aggregate Type**: `Favorite`

**Payload**:
```json
{
  "id": "FAV-jkl012mno",
  "user_id": "USR-pqr345stu",
  "station_id": "STA-def456ghi",
  "note": "Updated note about my favorite charging spot",
  "changes": {
    "note": {
      "from": "My favorite charging spot at the mall",
      "to": "Updated note about my favorite charging spot"
    }
  }
}
```

#### FavoriteDeleted

Emitted when a user removes a station from favorites.

**Event Type**: `FavoriteDeleted`

**Aggregate Type**: `Favorite`

**Payload**:
```json
{
  "id": "FAV-jkl012mno",
  "user_id": "USR-pqr345stu",
  "station_id": "STA-def456ghi",
  "note": "Updated note about my favorite charging spot",
  "deleted_at": "2026-05-23T10:01:00Z"
}
```

## RabbitMQ Configuration

### Exchange

**Name**: `bornemap.events`

**Type**: `topic`

**Purpose**: All domain events are published to this exchange with routing keys based on event type.

### Routing Keys

Events are routed with the following pattern:
```
bornemap.{aggregate_type}.{event_type}
```

Examples:
- `bornemap.company.created`
- `bornemap.station.updated`
- `bornemap.charger.deleted`
- `bornemap.review.moderated`

### Queues

Consumers should bind to the exchange with appropriate routing key patterns:

#### Analytics Service Queue
**Name**: `analytics-service`
**Binding**: `bornemap.#` (all events)

#### Audit Service Queue
**Name**: `audit-service`
**Binding**: `bornemap.#` (all events)

#### Geo Service Queue
**Name**: `geo-service`
**Binding**: `bornemap.station.*`, `bornemap.charger.*`

#### External Integration Queue
**Name**: `external-integration`
**Binding**: `bornemap.company.*`, `bornemap.station.*`

### Message Properties

All RabbitMQ messages include these properties:
- `content-type`: `application/json`
- `message-id`: Event ID
- `correlation-id`: Event correlation ID
- `timestamp`: Event timestamp
- `user-id`: User who triggered the event
- `source`: `core-service`

## Event Delivery Guarantees

### At-Least-Once Delivery

Events are delivered with at-least-once semantics. Consumers MUST implement idempotency using the `event_id` field.

### Idempotency

Consumers should track processed event IDs to prevent duplicate processing:

```sql
CREATE TABLE processed_events (
  event_id VARCHAR(255) PRIMARY KEY,
  processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

### Error Handling

If event publishing fails, the outbox record status is set to `failed` and the error message is logged. A retry mechanism with exponential backoff should be implemented for failed events.

### Ordering

Events for the same aggregate are delivered in the order they were created. However, events for different aggregates may be delivered out of order.