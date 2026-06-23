# EPIC LIFECYCLE RULES

---

## 1. States

- Draft
- Active
- Locked
- Deprecated

---

## 2. Rules

### Draft
- free modification allowed
- no production dependency

### Active
- implementation allowed
- must follow constitution strictly

### Locked
- no structural changes allowed
- only patches allowed

### Deprecated
- read-only
- retained for backward reference

---

## 3. Transition Rules

- Draft → Active requires compliance check
- Active → Locked requires system freeze approval
- Locked → Deprecated only after replacement exists
