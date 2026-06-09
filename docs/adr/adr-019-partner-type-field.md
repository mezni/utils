# ADR-019: Partner type field (business / personal)

**Status:** Accepted
**Date:** 2026-06-09

## Context

Partners onboard to BorneMap with different legal structures. Some are companies with registration numbers, others are individuals operating a single station at their home. The system must accommodate both without forcing a complex onboarding on personal partners.

## Decision

Add a `type` field to `inventory.partner` with values `business` or `personal` (default `business`). Business partners require legal name and registration. Personal partners have simplified onboarding with fewer required fields. The field is set on creation and is not editable by the partner.

## Consequences

- Flexible onboarding for different partner types
- Clear distinction in the data model
- Personal partners have a lower barrier to entry
- UI must adapt form fields based on type
- Future: different reporting or verification flows per type
