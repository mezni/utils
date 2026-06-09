# ADR-018: Dashboard built before driver apps in every MVP

**Status:** Accepted
**Date:** 2026-06-09

## Context

Within an MVP, the Dashboard (admin/partner tools) and driver apps (discovery) both need to be built. If driver apps are built first, there may be no data to display or manage. If Dashboard is built first, data creation and management are functional before discovery exists.

## Decision

In every MVP, build the Dashboard App before the driver apps. Data must exist before discovery is meaningful. This applies to MVP-1 (admin creates data, partner manages it, then driver discovers it), MVP-2 (admin service before driver service), and all subsequent MVPs.

## Consequences

- Driver apps always have data to display during development
- Data creation, validation, and management are verified before consumption
- Dashboard and API surface are battle-tested before driver app development begins
- More back-and-forth if Dashboard design changes during driver app development
