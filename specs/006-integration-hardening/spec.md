# Feature Specification: Integration and Hardening

**Feature Branch**: `006-integration-hardening`

**Created**: 2026-06-09

**Status**: Draft

**Input**: Sprint 1.6 — Full product loop verification, edge case fixes across all apps, and documentation completion to close out MVP-1.

## User Scenarios & Testing *(mandatory)*

### User Story 1 — Full Loop Verification (Priority: P1)

An admin creates a partner with type "business", then verifies the partner, sets is_live, creates a station, and creates chargers. The partner logs into the Dashboard partner view, sees their station and chargers, and updates charger status to "maintenance". A driver opens the Driver Web or Driver Mobile app and sees the station marker turn red (zero available chargers). The admin deactivates the partner — the station disappears from both driver apps on reload. The admin deletes a station — it disappears from both driver apps.

**Why this priority**: This verifies the entire MVP-1 product loop end to end. If the loop breaks anywhere, MVP-1 is not complete. This is the final validation before closing MVP-1.

**Independent Test**: Walk through the full loop on a running json-server instance: admin create → verify → set live → partner manages → driver sees → admin deactivates → driver no longer sees. All steps verifiable without writing code.

**Acceptance Scenarios**:

1. **Given** json-server is running and seeded, **When** an admin creates a new partner with type "business", **Then** the partner appears in the admin Partners table with is_verified=false, is_live=false, is_active=true.
2. **Given** a partner exists with is_verified=false, **When** the admin clicks Verify, **Then** is_verified becomes true and the badge turns green.
3. **Given** a partner is verified, **When** the admin sets is_live=true, **Then** the partner's stations become visible on the Driver Web and Driver Mobile apps (on reload).
4. **Given** a partner is verified and live, **When** the partner creates a station and chargers in the Dashboard partner view, **Then** the station appears on both driver apps within one reload.
5. **Given** a partner has chargers, **When** the partner sets a charger status to "maintenance", **Then** the station marker turns red on both driver apps on reload.
6. **Given** a partner is active, **When** the admin deactivates the partner (is_active=false), **Then** the partner's stations disappear from both driver apps on reload.
7. **Given** a station exists, **When** the admin deletes it, **Then** the station disappears from both driver apps on reload.

---

### User Story 2 — Fix Sweep (Priority: P2)

All four apps (Dashboard admin view, Dashboard partner view, Driver Web, Driver Mobile) are reviewed for edge case handling. Empty required fields show validation errors. Lat/lng out of range shows field errors. All screens handle API offline gracefully with an error message and retry button. Cross-browser testing on Chrome, Firefox, and Safari for web apps. Cross-platform testing on iOS simulator and Android emulator for the mobile app.

**Why this priority**: Edge case handling prevents crashes and data corruption. This ensures MVP-1 quality is production-adjacent even though the backend is mocked.

**Independent Test**: Start json-server, then stop it. All screens in all apps show an error state with a retry button. No app crashes when API is unreachable. Form validation catches empty required fields and out-of-range coordinates.

**Acceptance Scenarios**:

1. **Given** the API is offline, **When** any screen in any app loads, **Then** an error message is displayed with a retry button — no app crashes.
2. **Given** the Dashboard admin Stations form, **When** a latitude outside -90 to 90 is entered, **Then** a field-level validation error is shown.
3. **Given** the Dashboard admin Stations form, **When** a longitude outside -180 to 180 is entered, **Then** a field-level validation error is shown.
4. **Given** any Dashboard form with required fields, **When** the form is submitted with empty required fields, **Then** inline validation errors appear before submission.
5. **Given** the Dashboard partner view, **When** switching between partners, **Then** each partner sees only their own scoped data.
6. **Given** the Driver Web app, **When** a station belongs to an unverified partner, **Then** it never appears on the map.
7. **Given** the Driver Mobile app with location permission denied, **When** the map loads, **Then** it centers on Tunisia without crashing or showing an error.
8. **Given** the Driver Web app, **When** viewed in Chrome, Firefox, and Safari, **Then** the map and all UI elements function identically.

---

### User Story 3 — Documentation Completion (Priority: P3)

An onboarding guide is written so a new developer can set up and run the full product from scratch. API documentation describes all json-server resources, fields, filter parameters, and known limitations. An MVP-1 status document records what was built and what decisions were made.

**Why this priority**: Documentation enables onboarding and serves as the reference point for MVP-2 development. Without it, the project loses context between sprints.

**Independent Test**: A new developer follows the onboarding guide from a fresh clone and gets all four apps running within the documented time. The API doc correctly describes all endpoints and filters.

**Acceptance Scenarios**:

1. **Given** a fresh clone of the repository, **When** a developer follows the onboarding guide step by step, **Then** all four apps start without errors.
2. **Given** the mock API documentation, **When** a developer reads it, **Then** all four resources (partners, stations, chargers, station_availability) are documented with their fields, filter parameters, and json-server limitations.
3. **Given** the MVP-1 status document, **When** a reader reviews it, **Then** it accurately describes all completed sprints and any known decisions or trade-offs.

---

### Edge Cases

- Admin deletes a partner that owns stations — cascade or block? This decision must be made and recorded during the sprint.
- API returns non-JSON response (e.g., HTML error page) — all apps show error state gracefully.
- Driver Mobile location permission granted but GPS unavailable — map uses last known location or Tunisia fallback.
- Dashboard partner view URL manipulation — a partner cannot see other partners' data via URL editing (dev-only, full enforcement in MVP-3).
- All chargers at a station are in "maintenance" or "offline" status — marker is red on both driver apps.
- No stations nearby — driver apps show empty terrain with no markers, no crash.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: Admin MUST be able to create a partner with type set to "business" or "personal".
- **FR-002**: Admin MUST be able to verify a partner, setting is_verified from false to true.
- **FR-003**: Admin MUST be able to toggle is_live on a verified partner.
- **FR-004**: Admin MUST be able to deactivate and reactivate a partner.
- **FR-005**: When a partner is deactivated, their stations MUST disappear from both driver apps on reload.
- **FR-006**: When a partner is deleted, their stations MUST be handled according to the recorded cascade/block decision.
- **FR-007**: Partner MUST be able to see only their own stations and chargers in the Dashboard partner view.
- **FR-008**: Partner MUST be able to update charger status from the Dashboard partner view.
- **FR-009**: Driver Web MUST reflect charger status changes within one reload.
- **FR-010**: Driver Mobile MUST reflect charger status changes within one reload.
- **FR-011**: All Dashboard forms MUST validate required fields before submission.
- **FR-012**: Latitude field MUST reject values outside -90 to 90 with an inline error.
- **FR-013**: Longitude field MUST reject values outside -180 to 180 with an inline error.
- **FR-014**: All screens in all apps MUST display an error message with retry when the API is unreachable.
- **FR-015**: Driver Mobile MUST handle location permission denial without crashing — map centers on Tunisia.
- **FR-016**: Driver Web MUST work in Chrome, Firefox, and Safari.
- **FR-017**: Driver Mobile MUST work on iOS Simulator and Android Emulator.
- **FR-018**: Onboarding guide MUST be written and tested from a fresh clone.
- **FR-019**: Mock API documentation MUST describe all four resources, their fields, filter parameters, and json-server limitations.
- **FR-020**: MVP-1 status document MUST record completed sprints and any decisions made.

### Key Entities

No new entities. The existing entities (Partner, Station, Charger, Station_Availability) are verified end to end.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Full product loop can be completed end to end in under 15 minutes by a tester familiar with the apps.
- **SC-002**: All four apps handle API offline gracefully — zero crashes when json-server is stopped.
- **SC-003**: All Dashboard forms reject invalid input before submission — zero invalid records created through the UI.
- **SC-004**: A new developer can set up and run all apps from a fresh clone within 30 minutes following the onboarding guide.
- **SC-005**: Driver Mobile works on both iOS Simulator and Android Emulator with identical behavior.
- **SC-006**: Driver Web works in Chrome, Firefox, and Safari with identical behavior.
- **SC-007**: Zero Class A bugs (crash, data loss, incorrect data display) across all four apps.

## Assumptions

- json-server continues to serve as the mock backend — no real database in MVP-1.
- All four apps are already built and working individually (Sprints 1.1–1.5 complete).
- The full loop is verified manually — no automated end-to-end tests in MVP-1.
- Documentation is written in Markdown under `docs/` directory.
- The cascade/block decision for partner deletion is made during the sprint and recorded.
- Cross-browser testing covers Chrome, Firefox, and Safari — no Edge or IE testing in MVP-1.
- Cross-platform testing for Driver Mobile covers iOS Simulator and Android Emulator — no physical device testing in MVP-1.
