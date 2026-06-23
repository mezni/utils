# E001 API SPECIFICATION

Base Path: /api/v1

---

## Response Contract

Success:
{
  "success": true,
  "data": {},
  "error": null
}

Error:
{
  "success": false,
  "data": null,
  "error": {
    "code": "ERROR_CODE",
    "message": "message"
  }
}

---

## Endpoints

### Dashboard
GET /api/v1/dashboard/kpis

### Partners
GET /api/v1/partners
POST /api/v1/partners
GET /api/v1/partners/{id}
DELETE /api/v1/partners/{id} (hard delete)
PUT /api/v1/partners/{id} (soft delete/undelete)

### Stations
GET /api/v1/stations
POST /api/v1/stations
GET /api/v1/stations/{id}
DELETE /api/v1/stations/{id} (hard delete)
PUT /api/v1/stations/{id} (soft delete/undelete)

### Chargers
GET /api/v1/chargers
POST /api/v1/chargers
GET /api/v1/chargers/{id}
DELETE /api/v1/chargers/{id} (hard delete)
PUT /api/v1/chargers/{id} (soft delete/undelete)

---

## Rules
- ONLY `id` used externally
- No UUID exposure
- Versioned under /api/v1
- Status field included in all entities
- Deleted records excluded from list queries
- Hard delete CASCADE (deletes children)
- Soft delete (sets deleted_at, no cascade)
