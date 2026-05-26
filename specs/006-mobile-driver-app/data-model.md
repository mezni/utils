# Data Model: Mobile Driver App — Map Discovery

## Entity: Station (From Nearby API)

| Field | Type | Source | Notes |
|-------|------|--------|-------|
| id | string | API | Not displayed to driver directly |
| name | string | API | Displayed in bottom sheet title |
| address | string | API | Displayed in bottom sheet |
| city | string | API | Displayed in bottom sheet |
| latitude | number | API | Marker position on map |
| longitude | number | API | Marker position on map |
| available_chargers | number | API | Displayed in bottom sheet and marker popup |
| distance_meters | number | API | Calculated by backend, displayed in bottom sheet |
| is_operational | boolean | API | Not displayed but affects availability |
| is_test | boolean | API | Always false in results (filtered by backend) |

**Relationships**: Has many Chargers (fetchable via separate endpoint). No local persistence.

---

## Entity: Charger (From Station Detail API)

| Field | Type | Source | Notes |
|-------|------|--------|-------|
| id | string | API | Not displayed to driver |
| station_id | string | API | Links to parent station |
| connector_type_id | string | API | Mapped to connector type name in display |
| power_kw | number | API | Displayed in bottom sheet |
| current_type | enum | API | "AC" or "DC" — displayed in bottom sheet |
| status | enum | API | "available" / "occupied" / "faulted" / "offline" |

**Status → Display Mapping**:
- available → green badge, "Available"
- occupied → amber badge, "Occupied"
- faulted → red badge, "Faulted"
- offline → gray badge, "Offline"

---

## Entity: Driver Location

| Field | Type | Source | Notes |
|-------|------|--------|-------|
| latitude | number | Device GPS | Obtained via expo-location |
| longitude | number | Device GPS | Obtained via expo-location |
| accuracy | number | Device GPS | Used to determine search center quality |
| timestamp | number | Device GPS | When location was obtained |
