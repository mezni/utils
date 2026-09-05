# Quickstart — Infrastructure & Multi-Schema Engine

Runnable validation scenarios proving the feature works end-to-end. See
`contracts/` for the underlying contracts and `data-model.md` for the schema
topology.

## Prerequisites

- Docker with Compose v2 on the host machine.
- Nothing else — no local Python or PostgreSQL installation required.

## Scenario 1: Cold Start (US1, SC-001/SC-002)

```bash
docker compose up -d --build
docker compose logs -f app
```

**Expected outcome**: the `db` service becomes healthy; the app polls the
database, runs `alembic upgrade head`, then logs the pinned readiness line
`READY: app listening on {API_HOST}:{API_PORT}`. Verify:

```bash
curl -s http://localhost:8000/health
# {"status":"ok","database":"up"}
```

## Scenario 2: Six Schemas Created (US2, FR-007)

```bash
docker compose exec db psql -U telco -d telco -c '\dn'
docker compose exec app alembic current
```

**Expected outcome**: `\dn` lists `catalog`, `inventory`, `crm`, `usage`,
`billing`, `dunning` (plus `public`); `alembic current` reports the head revision
(no manual steps — SC-002).

## Scenario 3: Migration Idempotency (US2, FR-009, SC-006)

```bash
docker compose restart app
docker compose logs app | tail -20
```

**Expected outcome**: restart is quick; migrations report already-at-head (no-op);
`GET /health` returns `ok` again.

## Scenario 4: Data Persistence (US1/AC2-3, FR-003, SC-005)

```bash
docker compose exec db psql -U telco -d telco -c 'CREATE TABLE public.probe(id int);'
docker compose down
docker compose up -d
docker compose exec db psql -U telco -d telco -c '\dt public.probe'
```

**Expected outcome**: the `probe` table survives `down`/`up` (named volume). A
full reset is explicit: `docker compose down -v`.

## Scenario 5: Configuration Override (US3, FR-005, SC-004)

```bash
DATABASE_URL=postgresql+asyncpg://telco:telco@db:5432/telco docker compose up -d app
docker compose logs app | grep READY
```

**Expected outcome**: with an overridden `DATABASE_URL`, the app still reaches
readiness — change the target to an alternate instance and observe it connect via
`GET /health` (SC-004).

## Scenario 6: Database-Not-Ready Failure (US3/AC4, FR-014)

```bash
docker compose stop db
docker compose start app       # or restart app
docker compose logs app | tail -20
docker compose start db
```

**Expected outcome**: the app retries within a bounded window but does not crash on
a transient wait; if the database remains down, it exits with a clear error and
non-zero status. Once `db` returns and the app restarts, readiness resumes.

## Scenario 7: Port Conflict (Edge Case)

Run `docker compose up -d` while another process holds host port `8000` or
`5432`.

**Expected outcome**: Compose reports the port binding conflict with a clear
message; adjust the host binding per `contracts/container-topology.md` and retry.