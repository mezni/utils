# SYSTEM CONVENTIONS

---

## 1. Naming Rules

- APIs use plural nouns (partners, stations, chargers)
- Files use snake_case (Rust) / kebab-case (frontend)

---

## 2. ID Convention

- Partners → PRT-<nanoid(12)>
- Stations → STA-<nanoid(12)>
- Chargers → CHR-<nanoid(12)>

Rule:
- `id` is the ONLY external identifier

---

## 3. API Convention

- Base path: /api/v1
- Response format is standardized
- No raw responses allowed

---

## 4. Layer Convention

- presentation = HTTP/UI only
- application = use-case logic
- domain = business rules
- infrastructure = IO
