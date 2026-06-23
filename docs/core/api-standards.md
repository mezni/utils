# API STANDARDS

---

## 1. Base Rules

- All APIs MUST use /api/v1
- All responses MUST follow standard format
- No framework-native responses allowed

---

## 2. Response Format

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

## 3. Rules

- Only `id` is exposed externally
- UUID is forbidden in API layer
