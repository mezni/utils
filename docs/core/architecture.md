# ARCHITECTURE SPECIFICATION

---

## 1. Architecture Pattern

The system uses Clean Architecture:

presentation → application → domain → infrastructure

---

## 2. Rules

- dependencies flow inward only
- domain is framework-free
- infrastructure handles all IO
- presentation handles transport only

---

## 3. Allowed Technologies

- Actix-Web → presentation only
- SQLx → infrastructure only
- React → frontend only
