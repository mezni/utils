# ADR-014: Leaflet + OpenStreetMap

**Status:** Accepted
**Date:** 2026-06-09

## Context

The Driver Web App needs an interactive map with station markers, popups, and custom styling. The map library must work with free tile providers and not require API keys.

## Decision

Use Leaflet with OpenStreetMap tiles for the Driver Web App. No API key required. react-native-maps (default provider) for the Driver Mobile App. No paid map library or tile provider without an approved ADR.

## Consequences

- Zero cost for map tiles
- No API key management
- Leaflet is mature and well-documented
- OpenStreetMap tiles are sufficient for station discovery
- Custom marker styling via CircleMarker
