# E001 DATABASE SPECIFICATION

Schema: ev

---

## Tables

### partners
- id (PRT-xxx PRIMARY KEY)
- name
- status
- is_valid
- created_by (FK → admins.id)
- created_at
- updated_by (FK → admins.id)
- updated_at
- deleted_at

### stations
- id (STA-xxx PRIMARY KEY)
- partner_id (FK → partners.id)
- name
- location
- status
- created_by (FK → admins.id)
- created_at
- updated_by (FK → admins.id)
- updated_at
- deleted_at

### chargers
- id (CHR-xxx PRIMARY KEY)
- station_id (FK → stations.id)
- status
- power_rating
- created_by (FK → admins.id)
- created_at
- updated_by (FK → admins.id)
- updated_at
- deleted_at

---

## Rules
- id is PRIMARY KEY everywhere
- No surrogate keys
- Soft delete via deleted_at (excluded from queries by default)
- Cascading deletes enforced
- Audit columns (created_by, updated_by) reference admin users
