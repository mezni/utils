# E001 DATABASE SPECIFICATION

Schema: ev

---

## Tables

### operators
- id (PRT-xxx PRIMARY KEY)
- name
- created_at

### stations
- id (STA-xxx PRIMARY KEY)
- operator_id (FK → operators.id)
- name
- location

### chargers
- id (CHR-xxx PRIMARY KEY)
- station_id (FK → stations.id)
- status
- power_rating

---

## Rules
- id is PRIMARY KEY everywhere
- No surrogate keys
- Cascading deletes enforced
