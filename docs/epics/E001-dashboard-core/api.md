# E001 API SPECIFICATION

Base Path:
GET/POST /api/v1

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

### Partners
GET /partners
POST /partners
GET /partners/{id}

### Stations
GET /stations
POST /stations
GET /stations/{id}

### Chargers
GET /chargers
POST /chargers
GET /chargers/{id}

---

## Rules
- ONLY `id` used externally
- No UUID exposure
- Versioned under /api/v1
