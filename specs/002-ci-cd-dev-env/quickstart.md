# Quickstart: Verify Phase 1 — CI/CD & Dev Environment

**Feature**: [spec.md](./spec.md)
**Plan**: [plan.md](./plan.md)
**Data model**: [data-model.md](./data-model.md)
**Date**: 2026-05-22

## Purpose

Mechanical, repeatable runbook to verify Phase 1 is ratified. Each step maps to an entity in `data-model.md` and one or more functional requirements in `spec.md`. Running the whole runbook end-to-end and seeing every step pass means Phase 1 is **Ratified**.

## Prerequisites

- Local clone of the repository at the tip of `main` after Phase 1 merges.
- Docker and Docker Compose installed locally.
- A web browser with access to the GitHub repository UI as a user with `admin` permission.
- `git`, `make`, `curl`, `grep` available locally.
- `GITHUB_TOKEN` with `workflow` and `packages:write` scope for CI verification.

## Step 0 — Environment configuration exists

Maps to: data-model §7, FR-009.

1. Confirm `.env.example` exists at repository root.
2. Confirm it contains entries for: PostgreSQL (host, port, db, user, password), MongoDB (host, port, db), RabbitMQ (host, port, user, password), Keycloak (url, realm), all four service ports, and LOG_LEVEL.
3. Confirm `.env` is listed in `.gitignore`.

```bash
test -f .env.example || echo "MISSING: .env.example"
grep -q '^POSTGRES_HOST=' .env.example || echo "MISSING: POSTGRES_HOST"
grep -q '^MONGO_HOST=' .env.example || echo "MISSING: MONGO_HOST"
grep -q '^RABBITMQ_HOST=' .env.example || echo "MISSING: RABBITMQ_HOST"
grep -q '^KEYCLOAK_URL=' .env.example || echo "MISSING: KEYCLOAK_URL"
grep -q '^.env$' .gitignore || echo "MISSING: .env in .gitignore"
```

✅ Pass when no "MISSING" lines are printed.

## Step 1 — docker-compose starts the full stack

Maps to: data-model §5, FR-005, FR-006, FR-007, SC-002.

1. Copy `.env.example` to `.env`.
2. Run `make up` (or `docker compose up -d`).
3. Wait up to 2 minutes and confirm all containers are running:

```bash
docker compose ps --services --filter "status=running" | sort
```

Expected services: `nginx`, `keycloak`, `postgres`, `mongodb`, `rabbitmq`, `auth-service`, `core-service`, `geo-service`, `analytics-service`.

4. Confirm the gateway is reachable:

```bash
curl -s -o /dev/null -w "%{http_code}" http://localhost
```

Expected: `200` or `302` (NGINX welcome or redirect).

5. Run `make down` and confirm containers stop.

✅ Pass when all 9 services are running and the gateway responds.

## Step 2 — Health and metrics endpoints respond

Maps to: data-model §6, FR-011, FR-012, FR-013, SC-003, SC-004.

With the local stack running:

```bash
# Test health endpoint on each service path
for service in auth-service core-service geo-service analytics-service; do
    STATUS=$(curl -s -o /dev/null -w "%{http_code}" "http://localhost/health/$service")
    [ "$STATUS" = "200" ] || echo "FAIL: /health/$service returned $STATUS"
done

# Test metrics endpoint returns Prometheus text
curl -s http://localhost/metrics | grep -q '^#' || echo "FAIL: /metrics not Prometheus format"
```

✅ Pass when all health checks return 200 and metrics starts with `#`.

## Step 3 — Makefile targets work

Maps to: data-model §9, FR-010.

```bash
make --just-print up down logs test lint openapi 2>&1 | grep -E '^(up|down|logs|test|lint|openapi)$'
```

Expected: each target prints its recipe. No "No rule to make target" errors.

✅ Pass when all six targets are defined.

## Step 4 — CI pipeline triggers on push

Maps to: data-model §1, FR-001, SC-001.

1. Push a trivial commit to a feature branch with an open PR.
2. In GitHub Actions, confirm that these jobs run:
   - `lint`
   - `unit`
   - `dco`
   - `integration`
   - `openapi-bundle`
   - `docker-build`
3. Confirm all jobs pass (or the failing job correctly reports failure).
4. Measure wall-clock time from push to final status check — MUST be under 10 minutes.

✅ Pass when all 6 jobs appear in the Actions tab and complete within 10 minutes.

## Step 5 — DCO check blocks unsigned commits

Maps to: data-model §2, FR-014, FR-015, SC-005.

1. On a feature branch, create a commit **without** `Signed-off-by:`:

```bash
git commit --allow-empty -m "test: unsigned commit"
git push origin HEAD
```

2. Open (or update) a PR. Confirm the `dco` status check fails.
3. Amend the commit to add `Signed-off-by`:

```bash
git commit --amend -s --allow-empty -m "test: signed commit"
git push origin HEAD --force
```

4. Confirm the `dco` status check passes.

✅ Pass when unsigned commit fails DCO and signed commit passes.

## Step 6 — Stale-branch workflow

Maps to: data-model §3, §4, FR-016, FR-017, SC-006.

1. Browse to GitHub → Actions → `Stale Branches` workflow.
2. Confirm the workflow is scheduled (weekly cron).
3. Manually trigger the workflow (via `workflow_dispatch` if supported).
4. After it completes, check Issues for a `stale-branch` label.
5. Confirm the issue lists any branches matching the stale criteria (or states "No stale branches found").

✅ Pass when the workflow runs and creates/updates a `stale-branch` issue.

## Step 7 — NGINX routing works

Maps to: contracts/nginx-routing.md, FR-008, FR-013.

With the local stack running:

```bash
# Test routing to each service path
curl -s -o /dev/null -w "%{http_code}" http://localhost/auth/     # Expected: 200 or 404 (service-specific)
curl -s -o /dev/null -w "%{http_code}" http://localhost/api/core/ # Expected: 200 or 404
curl -s -o /dev/null -w "%{http_code}" http://localhost/api/geo/  # Expected: 200 or 404
curl -s -o /dev/null -w "%{http_code}" http://localhost/api/analytics/ # Expected: 200 or 404

# Test unknown path returns 404
curl -s -o /dev/null -w "%{http_code}" http://localhost/unknown-path
```

Expected: last command returns `404`.

✅ Pass when routing table matches contracts/nginx-routing.md.

## Step 8 — Branch protection updated with CI checks

Maps to: FR-004, SC-008.

1. In GitHub UI: Repository → Settings → Rules → Rulesets → `main-protection`.
2. Confirm the "Require status checks to pass" clause now lists:
   - `lint`
   - `unit`
   - `dco`
   - `integration`
   - `openapi-bundle`
   - `docker-build`
3. Confirm these were previously empty (Phase 0) and are now populated.

✅ Pass when all 6 status checks are listed in the ruleset.

## Aggregate result

Phase 1 is **Ratified** when every step above passes on a fresh clone of `main`. Record the runbook execution date and the executor's handle in the PR that closes Phase 1.

If any step fails, file an issue tagged `phase-1` and fix before declaring ratification.
