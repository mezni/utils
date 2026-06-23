# COMPLIANCE CHECKLIST

---

## 1. Architecture

- [ ] Clean Architecture respected
- [ ] no cross-layer dependency violations
- [ ] domain remains pure

---

## 2. API

- [ ] uses /api/v1
- [ ] standard response format enforced
- [ ] no raw responses

---

## 3. Identity

- [ ] only `id` exposed externally
- [ ] no UUID leakage
- [ ] consistent ID formats (PRT/STA/CHR)

---

## 4. Database

- [ ] no surrogate keys
- [ ] cascading rules defined
- [ ] schema under `ev`

---

## 5. Frontend

- [ ] no fetch in components
- [ ] React Query used
- [ ] API client abstraction enforced

---

## 6. Observability

- [ ] request_id present
- [ ] structured logging enabled
- [ ] trace propagation implemented
