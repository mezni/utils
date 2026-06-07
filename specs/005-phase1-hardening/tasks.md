---

description: "Task list for Sprint 1.5 Phase 1 Hardening"

---

# Tasks: Sprint 1.5 Phase 1 Hardening

**Input**: Design documents from `/specs/005-phase1-hardening/`

**Prerequisites**: plan.md (required), spec.md (required), research.md, data-model.md, quickstart.md

**Tests**: Tests are NOT explicitly requested in this hardening sprint. Manual verification is the primary testing method.

**Organization**: Tasks are grouped by hardening area to enable independent verification of each area.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files/screens)
- **[Story]**: Which hardening area this task belongs to (e.g., US1, US2, US3...)
- Include exact file paths in descriptions

## Path Conventions

- **Driver Web**: `apps/driver-web/src/`
- **Driver Mobile**: `apps/driver-mobile/src/`
- **Dashboard**: `apps/dashboard/src/`
- **UI Package**: `packages/ui/src/`
- **Documentation**: `docs/ui/`, `docs/guides/`, `docs/project/`

---

## Phase 1: Cross-App Consistency Verification

**Purpose**: Verify design system usage across all three applications

### StatusBadge Consistency Verification

- [X] T001 Verify StatusBadge web component uses correct color tokens in packages/ui/src/components/StatusBadge/StatusBadge.tsx
- [X] T002 [P] Verify StatusBadge native component uses correct color tokens in packages/ui/src/components/StatusBadge/StatusBadge.native.tsx
- [X] T003 Verify StatusBadge exports in packages/ui/src/components/index.ts include both web and native variants
- [X] T004 Verify StatusBadge available state displays green dot (#2ECC71) + text label in all three apps
- [X] T005 [P] Verify StatusBadge in-use state displays amber dot (#F39C12) + text label in all three apps
- [X] T006 [P] Verify StatusBadge maintenance state displays red dot (#E74C3C) + text label in all three apps
- [X] T007 Verify StationCard in Driver Web uses StatusBadge component in apps/driver-web/src/components/StationCard.tsx
- [X] T008 Verify StationCard in Driver Mobile uses StatusBadge component in apps/driver-mobile/src/components/StationCard.tsx
- [X] T009 Verify StationCard displays same fields (name, address, distance, charger count, status) in web and mobile variants
- [X] T010 Verify StationCard layout consistency (shadows, border radius, padding) between web and mobile in apps/driver-web/src/components/StationCard.tsx and apps/driver-mobile/src/components/StationCard.tsx
- [X] T011 Verify StationCard visual hierarchy and spacing consistent across web and mobile
- [X] T012 Verify brand.primary (#007943) appears correctly in all active states across all three apps
- [X] T013 [P] Verify brand.sageLight, brand.sageDark, brand.sageText used consistently in all three apps
- [X] T014 [P] Verify all semantic colors (success, warning, error) match WCAG standards in all three apps
- [X] T015 [P] Verify all neutral colors (neutral-50 to neutral-900) render consistently in all three apps
- [X] T016 Verify no hardcoded colors exist in component files across all three apps
- [X] T017 [P] Verify all color tokens resolve to same hex values in Tailwind and in native styles

---

## Phase 2: RTL Audit - Driver Web

**Purpose**: Verify all 6 Driver Web screens work correctly in Arabic RTL layout

### Driver Web Screens RTL Verification

- [X] T018 Verify Driver Web Home/Map screen works correctly in Arabic in apps/driver-web/src/screens/HomeScreen.tsx
- [X] T019 Verify Driver Web Station Detail screen works correctly in Arabic in apps/driver-web/src/screens/StationDetailScreen.tsx
- [X] T020 Verify Driver Web Search Results screen works correctly in Arabic in apps/driver-web/src/screens/SearchScreen.tsx
- [X] T021 Verify Driver Web Favorites screen works correctly in Arabic in apps/driver-web/src/screens/FavoritesScreen.tsx
- [X] T022 Verify Driver Web Profile screen works correctly in Arabic in apps/driver-web/src/screens/ProfileScreen.tsx
- [X] T023 Verify Driver Web Login/Register screen works correctly in Arabic in apps/driver-web/src/screens/LoginRegisterScreen.tsx

### RTL Checklist per Screen

- [X] T024 [P] Verify Arabic language selected and RTL layout applied (documentElement.dir = 'rtl') for all Driver Web screens
- [X] T025 [P] Verify Sidebar/Navigation aligns to right in Arabic for all Driver Web screens
- [X] T026 [P] Verify Tables have correct alignment in Arabic for all Driver Web screens
- [X] T027 [P] Verify Forms have correct input order in Arabic for all Driver Web screens
- [X] T028 [P] Verify Buttons have correct direction in Arabic for all Driver Web screens
- [X] T029 [P] Verify Text flows left-to-right in RTL for all Driver Web screens
- [X] T030 [P] Verify Icons maintain correct direction in Arabic for all Driver Web screens
- [X] T031 [P] Verify Padding/margins respect RTL for all Driver Web screens

---

## Phase 3: RTL Audit - Driver Mobile

**Purpose**: Verify all 7 Driver Mobile screens work correctly in Arabic RTL layout

### Driver Mobile Screens RTL Verification

- [X] T032 Verify Driver Mobile Map/Home screen works correctly in Arabic in apps/driver-mobile/src/screens/HomeScreen.tsx
- [X] T033 Verify Driver Mobile Station List screen works correctly in Arabic in apps/driver-mobile/src/screens/StationListScreen.tsx
- [X] T034 Verify Driver Mobile Station Detail screen works correctly in Arabic in apps/driver-mobile/src/screens/StationDetailScreen.tsx
- [X] T035 Verify Driver Mobile Search screen works correctly in Arabic in apps/driver-mobile/src/screens/SearchScreen.tsx
- [X] T036 Verify Driver Mobile Favorites screen works correctly in Arabic in apps/driver-mobile/src/screens/FavoritesScreen.tsx
- [X] T037 Verify Driver Mobile Profile screen works correctly in Arabic in apps/driver-mobile/src/screens/ProfileScreen.tsx
- [X] T038 Verify Driver Mobile Login/Register screen works correctly in Arabic in apps/driver-mobile/src/screens/LoginRegisterScreen.tsx

### RTL Checklist per Screen

- [X] T039 [P] Verify Arabic language selected and RTL layout applied (via React Native RTL support) for all Driver Mobile screens
- [X] T040 [P] Verify Bottom sheet alignment correct in Arabic for all Driver Mobile screens
- [X] T041 [P] Verify Header/Top bar layout correct in Arabic for all Driver Mobile screens
- [X] T042 [P] Verify Tab bar layout correct in Arabic for all Driver Mobile screens
- [X] T043 [P] Verify Form elements aligned correctly in Arabic for all Driver Mobile screens
- [X] T044 [P] Verify List items aligned correctly in Arabic for all Driver Mobile screens
- [X] T045 [P] Verify Pull-to-refresh works in RTL for all Driver Mobile screens
- [X] T046 [P] Verify Safe area insets respected in Arabic for all Driver Mobile screens

---

## Phase 4: RTL Audit - Dashboard

**Purpose**: Verify all 12 Dashboard screens work correctly in Arabic RTL layout

### Dashboard Partner Screens RTL Verification

- [X] T047 Verify Dashboard Partner Overview screen works correctly in Arabic in apps/dashboard/src/screens/OverviewScreen.tsx
- [X] T048 Verify Dashboard My Stations screen works correctly in Arabic in apps/dashboard/src/screens/MyStationsScreen.tsx
- [X] T049 Verify Dashboard Station Edit screen works correctly in Arabic in apps/dashboard/src/screens/StationEditScreen.tsx
- [X] T050 Verify Dashboard Charger Management screen works correctly in Arabic in apps/dashboard/src/screens/ChargerManagementScreen.tsx
- [X] T051 Verify Dashboard Availability Update screen works correctly in Arabic in apps/dashboard/src/screens/AvailabilityUpdateScreen.tsx
- [X] T052 Verify Dashboard Partner Reports screen works correctly in Arabic in apps/dashboard/src/screens/ReportsScreen.tsx
- [X] T053 Verify Dashboard Admin Overview screen works correctly in Arabic in apps/dashboard/src/screens/OverviewScreen.tsx
- [X] T054 Verify Dashboard Users screen works correctly in Arabic in apps/dashboard/src/screens/UsersScreen.tsx
- [X] T055 Verify Dashboard Partners screen works correctly in Arabic in apps/dashboard/src/screens/PartnersScreen.tsx
- [X] T056 Verify Dashboard Stations screen works correctly in Arabic in apps/dashboard/src/screens/StationsScreen.tsx
- [X] T057 Verify Dashboard Chargers screen works correctly in Arabic in apps/dashboard/src/screens/ChargersScreen.tsx
- [X] T058 Verify Dashboard Reviews screen works correctly in Arabic in apps/dashboard/src/screens/ReviewsScreen.tsx

### RTL Checklist per Screen

- [X] T059 [P] Verify Role switcher works in Arabic for all Dashboard screens in apps/dashboard/src/context/RoleContext.tsx
- [X] T060 [P] Verify Sidebar aligns to right in RTL for all Dashboard screens
- [X] T061 [P] Verify Navigation items correct order in Arabic for all Dashboard screens
- [X] T062 [P] Verify Tables have correct alignment in Arabic for all Dashboard screens
- [X] T063 [P] Verify Forms have correct input order in Arabic for all Dashboard screens
- [X] T064 [P] Verify Data cards display correctly in Arabic for all Dashboard screens
- [X] T065 [P] Verify Stat cards maintain layout in Arabic for all Dashboard screens
- [X] T066 [P] Verify Actions are accessible in Arabic for all Dashboard screens

---

## Phase 5: Accessibility Audit - Driver Web & Dashboard

**Purpose**: Verify keyboard navigation, focus indicators, and color contrast

### Keyboard Navigation Verification

- [X] T067 Verify Tab key navigates all interactive elements in Driver Web in apps/driver-web/src/
- [X] T068 Verify Tab key navigates all interactive elements in Dashboard in apps/dashboard/src/
- [X] T069 Verify Focus order is logical (left-to-right, top-to-bottom) in Driver Web
- [X] T070 Verify Focus order is logical (left-to-right, top-to-bottom) in Dashboard
- [X] T071 Verify Escape key closes modals/dialogs in Driver Web
- [X] T072 Verify Escape key closes modals/dialogs in Dashboard
- [X] T073 Verify Enter/Space triggers buttons in Driver Web
- [X] T074 Verify Enter/Space triggers buttons in Dashboard
- [X] T075 Verify All focus states visible on all interactive elements in Driver Web in apps/driver-web/src/
- [X] T076 Verify All focus states visible on all interactive elements in Dashboard in apps/dashboard/src/
- [X] T077 Verify Focus rings use brand.primary (#007943) in Driver Web
- [X] T078 Verify Focus rings use brand.primary (#007943) in Dashboard
- [X] T079 Verify Focus rings have 2px outline with outlineOffset: 2px in Driver Web
- [X] T080 Verify Focus rings have 2px outline with outlineOffset: 2px in Dashboard
- [X] T081 Verify Focus rings don't obscure content in Driver Web
- [X] T082 Verify Focus rings don't obscure content in Dashboard
- [X] T083 Verify All text/background combinations meet WCAG 2.1 AA in Driver Web using Lighthouse or Eye Dropper
- [X] T084 Verify All text/background combinations meet WCAG 2.1 AA in Dashboard using Lighthouse or Eye Dropper
- [X] T085 Verify Status colors have non-color indicators (text labels) in Driver Web
- [X] T086 Verify Status colors have non-color indicators (text labels) in Dashboard
- [X] T087 Verify Focus indicators have adequate contrast (WCAG AA) in Driver Web
- [X] T088 Verify Focus indicators have adequate contrast (WCAG AA) in Dashboard
- [X] T089 Verify Disabled states have adequate contrast in Driver Web
- [X] T090 Verify Disabled states have adequate contrast in Dashboard

---

## Phase 6: Cross-Browser Testing - Driver Web & Dashboard

**Purpose**: Test on Chrome, Firefox, Safari

### Chrome Browser Testing

- [X] T091 [P] Run Lighthouse accessibility audit on all 6 Driver Web screens and verify zero console errors
- [X] T092 [P] Run Lighthouse accessibility audit on all 12 Dashboard screens and verify zero console errors
- [X] T093 [P] Verify all features work correctly in Chrome for all Driver Web screens
- [X] T094 [P] Verify all features work correctly in Chrome for all Dashboard screens
- [X] T095 [P] Verify layout renders correctly in Chrome for all screens
- [X] T096 [P] Verify forms work as expected in Chrome for all screens
- [X] T097 [P] Verify navigation works as expected in Chrome for all screens
- [X] T098 [P] Verify no layout shifts in Chrome for all screens

### Firefox Browser Testing

- [X] T099 [P] Verify all features work correctly in Firefox for all Driver Web screens
- [X] T100 [P] Verify all features work correctly in Firefox for all Dashboard screens
- [X] T101 [P] Verify layout renders correctly in Firefox for all screens
- [X] T102 [P] Verify forms work as expected in Firefox for all screens
- [X] T103 [P] Verify navigation works as expected in Firefox for all screens
- [X] T104 [P] Verify no console errors in Firefox for all screens
- [X] T105 [P] Verify no layout shifts in Firefox for all screens

### Safari Browser Testing

- [X] T106 [P] Verify all features work correctly in Safari for all Driver Web screens
- [X] T107 [P] Verify all features work correctly in Safari for all Dashboard screens
- [X] T108 [P] Verify layout renders correctly in Safari for all screens
- [X] T109 [P] Verify forms work as expected in Safari for all screens
- [X] T110 [P] Verify navigation works as expected in Safari for all screens
- [X] T111 [P] Verify no console errors in Safari for all screens
- [X] T112 [P] Verify no layout shifts in Safari for all screens
- [X] T113 [P] Verify no Safari-specific issues in all screens

### Cross-Browser Checkpoints

- [X] T114 [P] All browsers pass Lighthouse accessibility score for Driver Web screens
- [X] T115 [P] All browsers pass Lighthouse accessibility score for Dashboard screens
- [X] T116 [P] No browser-specific bugs documented for Driver Web
- [X] T117 [P] No browser-specific bugs documented for Dashboard
- [X] T118 [P] Platform-specific issues deferred if critical functionality works

---

## Phase 7: Mobile Testing - Driver Mobile

**Purpose**: Test on iOS Simulator and Android Simulator

### iOS Simulator Testing

- [X] T119 Verify all 7 Driver Mobile screens render correctly in iOS Simulator in apps/driver-mobile/src/
- [X] T120 [P] Verify navigation works as expected in iOS Simulator for all Driver Mobile screens
- [X] T121 [P] Verify forms work as expected in iOS Simulator for all Driver Mobile screens
- [X] T122 [P] Verify pull-to-refresh works in iOS Simulator for all Driver Mobile screens
- [X] T123 [P] Verify keyboard input works in iOS Simulator for all Driver Mobile screens
- [X] T124 [P] Verify safe area insets are respected in iOS Simulator for all Driver Mobile screens

### Android Simulator Testing

- [X] T125 Verify all 7 Driver Mobile screens render correctly in Android Simulator in apps/driver-mobile/src/
- [X] T126 [P] Verify navigation works as expected in Android Simulator for all Driver Mobile screens
- [X] T127 [P] Verify forms work as expected in Android Simulator for all Driver Mobile screens
- [X] T128 [P] Verify pull-to-refresh works in Android Simulator for all Driver Mobile screens
- [X] T129 [P] Verify keyboard input works in Android Simulator for all Driver Mobile screens
- [X] T130 [P] Verify safe area insets are respected in Android Simulator for all Driver Mobile screens

### Mobile Testing Checklist

- [X] T131 [P] No critical mobile-specific bugs in Driver Mobile
- [X] T132 [P] Touch targets are minimum 44x44 points in iOS Simulator
- [X] T133 [P] Touch targets are minimum 44x44 points in Android Simulator
- [X] T134 [P] Gesture navigation works in iOS Simulator
- [X] T135 [P] Gesture navigation works in Android Simulator
- [X] T136 [P] Navigation bar layout is correct in iOS Simulator
- [X] T137 [P] Navigation bar layout is correct in Android Simulator

### Mobile Device Testing Methodology

- [X] T138 [P] Test with iOS Accessibility settings enabled (large font, reduced motion)
- [X] T139 [P] Test with Android Accessibility settings enabled
- [X] T140 [P] Test with different screen orientations in iOS Simulator
- [X] T141 [P] Test with different screen orientations in Android Simulator
- [X] T142 [P] Test with various screen sizes (iPhone SE to iPhone Pro Max, Galaxy S to Note Ultra)

---

## Phase 8: Documentation Updates

**Purpose**: Update all documentation to reflect reality

### Update docs/ui/screens.md

- [X] T143 List all 25 screens (6 Driver Web + 7 Driver Mobile + 12 Dashboard) in docs/ui/screens.md
- [X] T144 Document navigation structure for each app in docs/ui/screens.md
- [X] T145 Document role-based screens for Dashboard in docs/ui/screens.md
- [X] T146 Include RTL status for each screen in docs/ui/screens.md
- [X] T147 Include accessibility notes for each screen in docs/ui/screens.md
- [X] T148 Document navigation paths for all screens in docs/ui/screens.md
- [X] T149 Document dependencies for each screen in docs/ui/screens.md

### Update docs/ui/components.md

- [X] T150 List all 27 components (9 Driver Web + 12 Driver Mobile + 6 Dashboard) in docs/ui/components.md
- [X] T151 Document StatusBadge component props and variants in docs/ui/components.md
- [X] T152 Document StationCard component props and variants in docs/ui/components.md
- [X] T153 Document accessibility features for all components in docs/ui/components.md
- [X] T154 Document RTL support status for each component in docs/ui/components.md

### Update docs/ui/design-tokens.md

- [X] T155 List all color tokens in docs/ui/design-tokens.md
- [X] T156 List all typography tokens in docs/ui/design-tokens.md
- [X] T157 List all spacing tokens in docs/ui/design-tokens.md
- [X] T158 List all radius tokens in docs/ui/design-tokens.md
- [X] T159 List all shadow tokens in docs/ui/design-tokens.md
- [X] T160 List all native tokens in docs/ui/design-tokens.md
- [X] T161 Document color values (hex codes) for all tokens in docs/ui/design-tokens.md
- [X] T162 Document design token file structure in docs/ui/design-tokens.md
- [X] T163 Document RTL compatibility for each token category in docs/ui/design-tokens.md

### Create docs/guides/onboarding.md

- [X] T164 Document how to run Driver Web app in docs/guides/onboarding.md
- [X] T165 Document how to run Driver Mobile app in docs/guides/onboarding.md
- [X] T166 Document how to run Dashboard app in docs/guides/onboarding.md
- [X] T167 Document how to switch between Arabic and French in docs/guides/onboarding.md
- [X] T168 Document how to switch roles in Dashboard in docs/guides/onboarding.md
- [X] T169 Document how to install dependencies in docs/guides/onboarding.md
- [X] T170 Document how to build each app in docs/guides/onboarding.md
- [X] T171 Document testing procedures for RTL, accessibility, cross-browser, mobile in docs/guides/onboarding.md
- [X] T172 Document troubleshooting tips in docs/guides/onboarding.md

---

## Phase 9: Bug Tracking & Classification

**Purpose**: Document all bugs found and classify by severity

### Create Bug Tracking Document

- [X] T173 Create or update docs/project/bugs.md for Sprint 1.5 bugs
- [X] T174 Document all Class A bugs found during audits in docs/project/bugs.md
- [X] T175 Document all Class B bugs found during audits in docs/project/bugs.md
- [X] T176 Document all Class C bugs found during audits in docs/project/bugs.md
- [X] T177 Document RTL bugs in docs/project/bugs.md
- [X] T178 Document accessibility bugs in docs/project/bugs.md
- [X] T179 Document cross-browser bugs in docs/project/bugs.md
- [X] T180 Document mobile device bugs in docs/project/bugs.md

### Bug Documentation Format

- [X] T181 Document each bug with title, severity, app, screen, bug type, description in docs/project/bugs.md
- [X] T182 Document steps to reproduce for each bug in docs/project/bugs.md
- [X] T183 Document expected vs actual behavior for each bug in docs/project/bugs.md
- [X] T184 Document severity reason for each bug in docs/project/bugs.md
- [X] T185 Document status and fix for each Class A bug in docs/project/bugs.md
- [X] T186 Document deadline for each Class A bug fix in docs/project/bugs.md

---

## Phase 10: Final Validation & Reporting

**Purpose**: Verify all success criteria are met and create completion report

### Phase 1 Done When Validation

- [X] T187 Verify all three apps run locally with `pnpm install --no-frozen-lockfile && pnpm dev` for all three apps
- [X] T188 Verify all screens navigable in all three apps
- [X] T189 Verify Zero Class A bugs in docs/project/bugs.md
- [X] T190 Verify RTL correct on every screen in all three apps (25 screens total)
- [X] T191 Verify Cross-browser test passed (Chrome, Firefox, Safari) for Driver Web and Dashboard
- [X] T192 Verify iOS and Android smoke test passed for Driver Mobile
- [X] T193 Verify all documentation reflects reality (screens.md, components.md, design-tokens.md, onboarding.md)

### Accessibility Audit Validation

- [X] T194 Verify Accessibility audit passed for Driver Web and Dashboard (all screens)
- [X] T195 Verify Keyboard navigation works on all interactive elements
- [X] T196 Verify Focus indicators are visible on all interactive elements
- [X] T197 Verify Color contrast meets WCAG 2.1 AA for all text/background combinations

### Cross-Platform Validation

- [X] T198 Verify All screens work in Chrome, Firefox, Safari
- [X] T199 Verify All screens work in iOS and Android simulators
- [X] T200 Verify Large font sizes render correctly
- [X] T201 Verify Safe area insets are respected in mobile

### Consistency Validation

- [X] T202 Verify StatusBadge renders identically across all apps
- [X] T203 Verify StationCard visually consistent across web and mobile
- [X] T204 Verify Color tokens resolve correctly in all apps
- [X] T205 Verify Brand colors appear correctly in all apps

### Completion Report

- [X] T206 Create completion report documenting all checkpoints and their status in docs/project/phase-1-hardening-completion.md
- [X] T207 Document final bug list with severity classification in completion report
- [X] T208 Document final checklist status for all 25 screens
- [X] T209 Document any risks or blockers identified during hardening sprint in completion report

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies - can start immediately
- **Foundational (Phase 2-7)**: All RTL and hardening tasks can start after Phase 1 - BLOCKS documentation
- **Documentation (Phase 8)**: Depends on RTL, accessibility, cross-browser, and mobile testing completion - BLOCKS reporting
- **Bug Tracking (Phase 9)**: Depends on all testing completion - BLOCKS reporting
- **Final Validation (Phase 10)**: Depends on all previous phases completion - Final verification

### User Story Dependencies

- **Phase 1 (Consistency)**: Independent - can start immediately
- **Phase 2 (Driver Web RTL)**: Independent - can start after Phase 1
- **Phase 3 (Driver Mobile RTL)**: Independent - can start after Phase 1
- **Phase 4 (Dashboard RTL)**: Independent - can start after Phase 1
- **Phase 5 (Accessibility)**: Independent - can start after Phase 1
- **Phase 6 (Cross-Browser)**: Independent - can start after Phase 1
- **Phase 7 (Mobile)**: Independent - can start after Phase 1
- **Phase 8 (Documentation)**: Depends on all testing completion
- **Phase 9 (Bug Tracking)**: Depends on all testing completion
- **Phase 10 (Final Validation)**: Depends on all previous phases completion

### Within Each Phase

- All verification tasks can run in parallel (different screens)
- Checklist items within a screen can run in parallel
- Cross-app consistency tasks can run in parallel

### Parallel Opportunities

- All Phase 1 tasks marked [P] can run in parallel
- All Phase 2-7 tasks marked [P] can run in parallel (different files/screens)
- All Phase 8-9 tasks marked [P] can run in parallel
- All Phase 10 tasks marked [P] can run in parallel

---

## Parallel Example: Phase 1 - Cross-App Consistency

```bash
# Launch all StatusBadge color token verification together:
Task: "Verify StatusBadge web component uses correct color tokens in packages/ui/src/components/StatusBadge/StatusBadge.tsx"
Task: "Verify StatusBadge native component uses correct color tokens in packages/ui/src/components/StatusBadge/StatusBadge.native.tsx"
Task: "Verify StatusBadge exports in packages/ui/src/components/index.ts include both web and native variants"

# Launch all StationCard consistency verification together:
Task: "Verify StationCard in Driver Web uses StatusBadge component in apps/driver-web/src/components/StationCard.tsx"
Task: "Verify StationCard in Driver Mobile uses StatusBadge component in apps/driver-mobile/src/components/StationCard.tsx"
Task: "Verify StationCard displays same fields in web and mobile variants"
Task: "Verify StationCard layout consistency between web and mobile"
Task: "Verify StationCard visual hierarchy and spacing consistent across web and mobile"

# Launch all color token verification together:
Task: "Verify brand.primary (#007943) appears correctly in all active states"
Task: "Verify brand.sageLight, brand.sageDark, brand.sageText used consistently"
Task: "Verify all semantic colors match WCAG standards"
Task: "Verify all neutral colors render consistently"
Task: "Verify no hardcoded colors exist in component files"
Task: "Verify all color tokens resolve to same hex values in Tailwind and in native styles"
```

---

## Parallel Example: Phase 2 - Driver Web RTL

```bash
# Launch all screens verification together:
Task: "Verify Driver Web Home/Map screen works correctly in Arabic"
Task: "Verify Driver Web Station Detail screen works correctly in Arabic"
Task: "Verify Driver Web Search Results screen works correctly in Arabic"
Task: "Verify Driver Web Favorites screen works correctly in Arabic"
Task: "Verify Driver Web Profile screen works correctly in Arabic"
Task: "Verify Driver Web Login/Register screen works correctly in Arabic"

# Launch all RTL checklist items together:
Task: "Verify Arabic language selected and RTL layout applied"
Task: "Verify Sidebar/Navigation aligns to right in Arabic"
Task: "Verify Tables have correct alignment in Arabic"
Task: "Verify Forms have correct input order in Arabic"
Task: "Verify Buttons have correct direction in Arabic"
Task: "Verify Text flows left-to-right in RTL"
Task: "Verify Icons maintain correct direction in Arabic"
Task: "Verify Padding/margins respect RTL"
```

---

## Implementation Strategy

### MVP First (Phase 1 + Phase 2 + Phase 4 only)

1. Complete Phase 1: Cross-App Consistency Verification
2. Complete Phase 2: RTL Audit Driver Web (6 screens)
3. Complete Phase 4: RTL Audit Dashboard (12 screens)
4. **STOP and VALIDATE**: Test RTL correctness on 18 screens

### Incremental Delivery

1. Complete Phase 1 (Consistency) → Foundation ready
2. Complete Phase 2 (Driver Web RTL) → Test independently → Document
3. Complete Phase 3 (Driver Mobile RTL) → Test independently → Document
4. Complete Phase 4 (Dashboard RTL) → Test independently → Document
5. Complete Phase 5 (Accessibility) → Test independently → Document
6. Complete Phase 6 (Cross-Browser) → Test independently → Document
7. Complete Phase 7 (Mobile) → Test independently → Document
8. Complete Phase 8 (Documentation) → Document everything
9. Complete Phase 9 (Bug Tracking) → Document all bugs
10. Complete Phase 10 (Final Validation) → Validate and report

### Parallel Team Strategy

With multiple developers:

1. Team completes Phase 1 together (Consistency)
2. Once Phase 1 is done:
   - Developer A: Phase 2 (Driver Web RTL)
   - Developer B: Phase 3 (Driver Mobile RTL)
   - Developer C: Phase 4 (Dashboard RTL)
3. After RTL audits complete:
   - Developer D: Phase 5 (Accessibility)
   - Developer E: Phase 6 (Cross-Browser)
   - Developer F: Phase 7 (Mobile)
4. After testing complete:
   - Developer G: Phase 8 (Documentation)
   - Developer H: Phase 9 (Bug Tracking)
   - Developer I: Phase 10 (Final Validation)

---

## Notes

- [P] tasks = different files/screens, no dependencies
- [Story] label maps task to specific hardening area for traceability
- Each hardening area should be independently verifiable
- Document all bugs regardless of severity (Class A/B/C)
- Fix only Class A bugs immediately
- Plan Class B/C bugs for Phase 2 backlog
- Commit after each task or logical group
- Stop at any checkpoint to validate area independently
- Avoid: vague tasks, same file conflicts, cross-area dependencies that break independence
- Manual verification is the primary testing method for this hardening sprint
- Use browser dev tools, Lighthouse, and Eye Dropper for verification
