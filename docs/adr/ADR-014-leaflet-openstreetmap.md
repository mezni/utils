# ADR-014: Leaflet + OpenStreetMap for Web Map

**Status**: Accepted

**Date**: 2026-01-01

## Context

Driver Web App requires a map interface for station discovery and visualization. Options: Leaflet + OSM, Mapbox, Google Maps, Deck.gl.

## Decision

**Use Leaflet with OpenStreetMap tiles for Driver Web App map.**

No paid map provider. No API keys required. `react-leaflet` library for React integration.

## Rationale

- **Zero licensing cost**: OpenStreetMap tiles are free and open-source.
- **No API key management**: Simplifies deployment and reduces operational complexity.
- **Leaflet ecosystem**: Mature library, excellent for marker-based station discovery. Clean React wrapper available.
- **Performance**: Lightweight. Full-bleed map with 15 markers loads instantly.
- **Open data**: Aligns with open-source philosophy.

## Consequences

- Tile rendering depends on OSM tile service availability (acceptable risk for MVP-1).
- No turn-by-turn directions or complex routing (deferred in out-of-scope-registry).
- Custom tile layer cannot be changed without new ADR.
- Offline maps not supported (acceptable for MVP-1).

## Non-Negotiable

**No paid map provider (Mapbox, Google Maps) without a new ADR.** If licensing becomes necessary, that decision must be documented and approved.

## References

- Constitution section 4: Frontend Applications, Map Library Rules
- Constitution section 13: Non-Negotiable Rules, Frontend Rules
- Implementation Plan, Sprint 1.3: Driver Web App
