# DATA MODELING RULES

---

## 1. Identity Model

System uses ONLY external IDs:

- PRT-xxx → partners
- STA-xxx → stations
- CHR-xxx → chargers

---

## 2. Relationship Rules

- Stations belong to Partners
- Chargers belong to Stations
- Cascading deletes required

---

## 3. Database Rules

- No surrogate keys
- id is primary key everywhere
- schema namespace = ev
