# ADR-0002 — `geo-service` implemented in Rust

- **Status**: Accepted
- **Date**: 2026-05-22
- **Deciders**: BorneMap core team
- **Tags**: performance, language, service-boundary

## Context

Geospatial queries (nearby search, bounding-box reads, routing, ETA
calculation) are the latency-critical path for the public map. The
Constitution sets a performance posture (Principle I separates services;
Roadmap Phase 4 targets p99 < 200 ms for geo queries) and treats geo as
a distinct bounded context.

The rest of the backend is NestJS (TypeScript). Choosing Rust for one
service trades developer-stack uniformity for predictable tail latency
and CPU efficiency on PostGIS-heavy workloads.

## Decision

We will implement `geo-service` in **Rust** using **Actix-Web**. All
other backend services remain on NestJS.

`geo-service` is read-only against PostgreSQL/PostGIS, exposes REST
endpoints behind the NGINX gateway, and may use in-process caches (e.g.,
DashMap) to absorb hot reads.

## Alternatives considered

- **NestJS for geo-service** — Rejected. Acceptable for correctness, but
  GC pauses and per-request overhead make the p99 < 200 ms target
  harder under load with PostGIS-heavy queries.
- **Go for geo-service** — Rejected. Reasonable performance, but the
  ecosystem advantage over Rust for spatial workloads is not large
  enough to justify a third language. Rust gives us tighter latency
  control and zero-cost abstractions over PostGIS rows.
- **PostgREST / DB-only** — Rejected. Caching, routing/ETA composition,
  and observability requirements (Principle VI) need a real service.

## Consequences

- **Positive**
  - Predictable tail latency for the public map.
  - Memory and CPU headroom for cache layers without GC pressure.
- **Negative**
  - Two backend toolchains (Node + Rust) to maintain.
  - Smaller pool of contributors familiar with Rust.
  - CI pipeline must build both ecosystems (`unit` job has matrix
    entries for TS and Rust).
- **Follow-ups**
  - Phase 1 CI must include Clippy + `cargo test` in the `lint` and
    `unit` jobs.
  - Phase 4 must publish a benchmark report grounding the p99 target.

## Compliance check

- CI runs `cargo test` and `cargo clippy --deny warnings` on every PR
  affecting `services/geo-service/`.
- Phase 4 benchmark report committed under
  `docs/operations/` (added when produced).
- No new NestJS code is added under `services/geo-service/`.
