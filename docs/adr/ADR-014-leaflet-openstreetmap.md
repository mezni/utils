# ADR-014: Leaflet + OpenStreetMap for Web Map

**Status**: Accepted
**Date**: 2026-06-07

## Context

The Driver Web App needs an interactive map. Options: Google Maps, Mapbox, Leaflet (OpenStreetMap), Azure Maps.

## Decision

Use Leaflet with OpenStreetMap tile layer for the web map.

## Rationale

- Free — no API key required, no usage billing
- OpenStreetMap coverage for Tunisia is sufficient
- Leaflet is lightweight (~40KB gzipped)
- react-leaflet provides clean React integration
- CircleMarker components are sufficient for station markers (no custom markers needed initially)
- Principle 5 (Build for current scale): a paid tile provider is premature

## Consequences

- OpenStreetMap tiles may be slower than commercial providers
- No satellite imagery or turn-by-turn navigation
- Custom marker styling is limited compared to Mapbox
- Map terrain color must be customized via CSS (surface.mapTerrain = #EAF0E6)
- Switching to a paid provider requires an ADR

## Compliance

- No paid tile provider without an approved ADR
- Map uses OpenStreetMap attribution
- brand.glow (#00E676) for available station markers
- status.maintenance (#EF4444) for unavailable station markers
