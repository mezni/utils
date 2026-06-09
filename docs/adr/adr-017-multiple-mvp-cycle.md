# ADR-017: Multiple MVP cycle delivery strategy

**Status:** Accepted
**Date:** 2026-06-09

## Context

Building the complete platform before launch risks building features nobody uses. An incremental approach with progressive infrastructure replacement allows validation at each stage.

## Decision

Deliver in six MVP cycles. Each MVP is complete and deployable. Each MVP validates the product before adding infrastructure. The sequence is: core loop (MVP-1), real backend (MVP-2), auth (MVP-3), GIS (MVP-4), analytics (MVP-5), production (MVP-6). Each MVP builds on the previous without breaking it.

## Consequences

- Value delivered early (MVP-1 is functional in weeks)
- Infrastructure added only when validated need exists
- Risk of over-investment in unvalidated features minimized
- Later MVPs can be descoped if earlier ones don't validate the product
- Clear milestones for stakeholders
