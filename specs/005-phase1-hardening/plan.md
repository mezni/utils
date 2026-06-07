# Sprint 1.5 — Phase 1 Hardening

**Goal:** All three apps are solid, consistent, and RTL-correct before any backend work begins.

**Duration:** 1 week (1 sprint)

**Status:** In Progress

---

## Overview

This hardening sprint ensures that all three frontend applications (Driver Web, Driver Mobile, Dashboard) are production-ready with consistent design systems, correct RTL layout, and accessible user interfaces before Phase 2 (Database Foundation) begins.

---

## Cross-App Consistency Review

### StatusBadge Consistency

**Verification:**
- [ ] Verify StatusBadge renders identically across Driver Web, Driver Mobile, and Dashboard
- [ ] Verify color tokens resolve to the same hex values in Tailwind and in native styles
- [ ] Verify brand.primary (#007943) appears correctly in all active states across all apps

**Expected Behavior:**
- Available → Green dot + text
- In-use → Amber dot + text
- Maintenance → Red dot + text

**Checkpoints:**
- [ ] All three apps use the same StatusBadge component (web/mobile) or equivalent
- [ ] Color tokens match across all apps
- [ ] Active states show brand.primary (#007943) correctly
- [ ] All states render in all three apps

### StationCard Consistency

**Verification:**
- [ ] Verify StationCard is visually consistent between Driver Web and Driver Mobile
- [ ] Verify same fields displayed (name, address, distance, charger count, availability)
- [ ] Verify visual hierarchy and spacing is consistent

**Checkpoints:**
- [ ] Layout matches between web and mobile variants
- [ ] Card styling is consistent (shadows, border radius, padding)
- [ ] Information hierarchy is preserved
- [ ] Responsive behavior matches expectations

### Color Token Verification

**Verification:**
- [ ] brand.primary (#007943) appears correctly across all apps
- [ ] brand.sageLight, brand.sageDark, brand.sageText used consistently
- [ ] Neutral colors (neutral-50 to neutral-900) render consistently
- [ ] Semantic colors (success, warning, error) match WCAG standards

**Checkpoints:**
- [ ] All apps reference tokens from `@borne-map/ui`
- [ ] No hardcoded colors in components
- [ ] All color values match specification
- [ ] Dark mode not yet required (Phase 9)

---

## RTL Audit — Every Screen in Every App

### Driver Web App Screens

**Screens to audit:**
1. Home/Map Screen
2. Station Detail Screen
3. Search Results Screen
4. Favorites Screen
5. Profile Screen
6. Login/Register Screen

**Verification Checklist per Screen:**
- [ ] Arabic language selected
- [ ] RTL layout applied (documentElement.dir = 'rtl')
- [ ] Sidebar/Navigation aligns to right
- [ ] Tables have correct alignment
- [ ] Forms have correct input order
- [ ] Buttons have correct direction
- [ ] Text flows left-to-right in RTL
- [ ] Icons maintain correct direction
- [ ] Padding/margins respect RTL

### Driver Mobile App Screens

**Screens to audit:**
1. Map/Home Screen
2. Station List Screen
3. Station Detail Screen
4. Search Screen
5. Favorites Screen
6. Profile Screen
7. Login/Register Screen

**Verification Checklist per Screen:**
- [ ] Arabic language selected
- [ ] RTL layout applied (via React Native RTL support)
- [ ] Bottom sheet alignment correct
- [ ] Header/Top bar layout correct
- [ ] Tab bar layout correct
- [ ] Form elements aligned correctly
- [ ] List items aligned correctly
- [ ] Pull-to-refresh works in RTL
- [ ] Safe area insets respected

### Dashboard App Screens

**Screens to audit:**
1. Overview Screen (Partner)
2. My Stations Screen
3. Station Edit Screen
4. Charger Management Screen
5. Availability Update Screen
6. Reports Screen
7. Users Screen
8. Partners Screen
9. Stations Screen
10. Chargers Screen
11. Reviews Screen
12. Login/Register Screen (Admin)

**Verification Checklist per Screen:**
- [ ] Role switcher works in Arabic
- [ ] Sidebar aligns to right in RTL
- [ ] Navigation items correct order
- [ ] Tables have correct alignment
- [ ] Forms have correct input order
- [ ] Data cards display correctly
- [ ] Stat cards maintain layout
- [ ] Actions are accessible

**Class A RTL Bugs:**
- [ ] Any RTL bugs must be documented and fixed
- [ ] Bugs with medium/low priority are acceptable for Phase 2
- [ ] Document all bugs in `docs/project/bugs.md`

---

## Accessibility Audit (Driver Web and Dashboard)

### Keyboard Navigation

**Verification:**
- [ ] All interactive elements are focusable with Tab key
- [ ] Focus indicators visible on all interactive elements
- [ ] Focus order is logical (left-to-right, top-to-bottom)
- [ ] Escape key closes modals/dialogs
- [ ] Enter/Space triggers buttons
- [ ] Link navigation works correctly

**Checkpoints:**
- [ ] Tab key navigates all buttons, inputs, links
- [ ] Focus rings use brand.primary (#007943)
- [ ] Focus rings have adequate contrast
- [ ] No element traps focus
- [ ] Focus management in modals is correct

### Focus Indicators

**Verification:**
- [ ] All focus states visible
- [ ] Focus states have adequate contrast (WCAG AA)
- [ ] Focus rings use design token values
- [ ] Focus rings don't obscure content
- [ ] Focus rings on buttons, inputs, links

**Checkpoints:**
- [ ] Focus ring color: brand.primary (#007943) with 2px outline
- [ ] Focus ring has outlineOffset: 2px
- [ ] Focus ring opacity: 1 (not transparent)
- [ ] Focus ring is not covered by other elements

### Color Contrast

**Verification:**
- [ ] All text/background combinations meet WCAG 2.1 AA
- [ ] Status colors (green/amber/red) have non-color indicators
- [ ] Focus indicators have adequate contrast
- [ ] Disabled states have adequate contrast

**Checkpoints:**
- [ ] Primary text: neutral-900 on neutral-50 (4.5:1)
- [ ] Secondary text: neutral-600 on neutral-50 (7:1)
- [ ] Buttons: white on brand.primary (3.5:1 minimum)
- [ ] Focus rings: brand.primary on any background
- [ ] Status dots + text labels used for status colors

**Test Tools:**
- [ ] Lighthouse accessibility audit
- [ ] axe DevTools (Chrome extension)
- [ ] Browser accessibility developer tools

---

## Cross-Browser Test (Driver Web and Dashboard)

### Chrome Browser

**Screens to test:**
- [ ] All 6 Driver Web screens
- [ ] All 12 Dashboard screens

**Verification:**
- [ ] No console errors
- [ ] All features work correctly
- [ ] Layout renders correctly
- [ ] Forms work as expected
- [ ] Navigation works as expected
- [ ] No layout shifts

### Firefox Browser

**Screens to test:**
- [ ] All 6 Driver Web screens
- [ ] All 12 Dashboard screens

**Verification:**
- [ ] No console errors
- [ ] All features work correctly
- [ ] Layout renders correctly
- [ ] Forms work as expected
- [ ] Navigation works as expected
- [ ] No layout shifts

### Safari Browser

**Screens to test:**
- [ ] All 6 Driver Web screens
- [ ] All 12 Dashboard screens

**Verification:**
- [ ] No console errors
- [ ] All features work correctly
- [ ] Layout renders correctly
- [ ] Forms work as expected
- [ ] Navigation works as expected
- [ ] No layout shifts
- [ ] No Safari-specific issues

**Checkpoints:**
- [ ] All browsers pass Lighthouse accessibility score
- [ ] No browser-specific bugs documented
- [ ] Platform-specific issues deferred if critical functionality works

---

## Mobile Device Test (Driver Mobile)

### iOS Simulator

**Screens to test:**
- [ ] All 7 Driver Mobile screens

**Verification:**
- [ ] All screens render correctly
- [ ] Navigation works as expected
- [ ] Forms work as expected
- [ ] Pull-to-refresh works
- [ ] Keyboard input works
- [ ] Safe area insets respected

**Large Font Test:**
- [ ] Large font size (200% or iOS Accessibility setting)
- [ ] All content is accessible
- [ ] No layout breaks
- [ ] Text is readable

### Android Simulator

**Screens to test:**
- [ ] All 7 Driver Mobile screens

**Verification:**
- [ ] All screens render correctly
- [ ] Navigation works as expected
- [ ] Forms work as expected
- [ ] Pull-to-refresh works
- [ ] Keyboard input works
- [ ] Safe area insets respected

**Large Font Test:**
- [ ] Large font size (200% or Accessibility setting)
- [ ] All content is accessible
- [ ] No layout breaks
- [ ] Text is readable

**Checkpoints:**
- [ ] No critical mobile-specific bugs
- [ ] Touch targets are minimum 44x44 points
- [ ] Gesture navigation works
- [ ] Navigation bar layout is correct

---

## Documentation Updates

### Update `docs/ui/screens.md`

**Content:**
- [ ] List all screens in Driver Web (6 screens)
- [ ] List all screens in Driver Mobile (7 screens)
- [ ] List all screens in Dashboard (12 screens)
- [ ] Document navigation structure for each app
- [ ] Document role-based screens for Dashboard
- [ ] Include RTL status for each screen

**Format:**
- Screen name
- Description
- Navigation path
- Dependencies
- RTL status
- Accessibility notes

### Update `docs/ui/components.md`

**Content:**
- [ ] List all components in Driver Web (9 components)
- [ ] List all components in Driver Mobile (12 components)
- [ ] List all components in Dashboard (6 components)
- [ ] Document component props and variants
- [ ] Document component accessibility features

**Format:**
- Component name
- Description
- Props/variants
- Accessibility features
- RTL support status

### Update `docs/ui/design-tokens.md`

**Content:**
- [ ] List all color tokens
- [ ] List all typography tokens
- [ ] List all spacing tokens
- [ ] List all radius tokens
- [ ] List all shadow tokens
- [ ] List all native tokens
- [ ] Document color values (hex codes)
- [ ] Document design token file structure

**Format:**
- Token category
- Token name
- Value (hex, px, etc.)
- Usage notes
- RTL compatibility

### Write `docs/guides/onboarding.md`

**Content:**
- [ ] How to run Driver Web app
- [ ] How to run Driver Mobile app
- [ ] How to run Dashboard app
- [ ] How to switch between Arabic and French
- [ ] How to switch roles in Dashboard
- [ ] How to install dependencies
- [ ] How to build each app

**Format:**
- Installation instructions
- Development commands
- Testing procedures
- Troubleshooting tips

---

## Class A Bugs Documentation

### Bug Documentation Format

For each Class A bug found during audits, document in `docs/project/bugs.md`:

```markdown
## Sprint 1.5 Bugs

### Bug [N]: [Bug Title]

**Severity:** Class A
**App:** [Driver Web / Driver Mobile / Dashboard]
**Screen:** [Screen Name]
**Bug Type:** RTL / Accessibility / Cross-Browser / Mobile
**Description:** [Clear description of the bug]

**Steps to Reproduce:**
1. [Step 1]
2. [Step 2]
3. [Step 3]

**Expected Behavior:** [What should happen]

**Actual Behavior:** [What actually happens]

**Severity Reason:** [Why it's Class A - blocks users]

**Status:** [Fixed / Deferred / In Progress]
**Fix:** [Description of fix if available]
**Deadline:** [When fix must be complete]
```

### Bug Categories

**Class A Bugs** (Must fix before Phase 2):
- RTL bugs that completely block functionality in Arabic
- Accessibility bugs that block keyboard navigation
- Critical cross-browser bugs
- Critical mobile device bugs

**Class B Bugs** (Can defer):
- Minor RTL layout issues
- Minor accessibility issues
- Non-critical cross-browser issues
- Non-critical mobile device issues

**Class C Bugs** (Can defer):
- Cosmetic issues
- Minor UX issues
- Non-critical bugs

---

## Phase 1 Done When

- [ ] All three apps run locally with `pnpm install --no-frozen-lockfile && pnpm dev`
- [ ] All screens navigable in all three apps
- [ ] Zero Class A bugs
- [ ] RTL correct on every screen in all three apps
- [ ] Cross-browser test passed (Chrome, Firefox, Safari)
- [ ] iOS and Android smoke test passed
- [ ] All documentation reflects reality
- [ ] Accessibility audit passed (Driver Web and Dashboard)
- [ ] Documentation updated (screens.md, components.md, design-tokens.md, onboarding.md)

---

## Success Criteria

### Consistency

- [ ] StatusBadge renders identically across all apps
- [ ] StationCard visually consistent across web and mobile
- [ ] Color tokens resolve correctly in all apps
- [ ] Brand colors appear correctly in all apps

### RTL Quality

- [ ] All screens work correctly in Arabic
- [ ] Zero Class A RTL bugs
- [ ] RTL layout is professional and consistent
- [ ] Tables, forms, and navigation align correctly

### Accessibility

- [ ] Keyboard navigation works on all interactive elements
- [ ] Focus indicators are visible
- [ ] Color contrast meets WCAG 2.1 AA
- [ ] Status colors have non-color indicators

### Cross-Platform Compatibility

- [ ] All screens work in Chrome, Firefox, Safari
- [ ] All screens work in iOS and Android simulators
- [ ] Large font sizes render correctly
- [ ] Safe area insets are respected

### Documentation

- [ ] screens.md updated with all screens
- [ ] components.md updated with all components
- [ ] design-tokens.md updated with all tokens
- [ ] onboarding.md created with setup instructions

---

## Tasks Breakdown

### Week 1, Day 1-2: Cross-App Consistency Review
- [ ] Verify StatusBadge consistency
- [ ] Verify StationCard consistency
- [ ] Verify color token resolution
- [ ] Document any inconsistencies

### Week 1, Day 3: RTL Audit
- [ ] Audit Driver Web all screens
- [ ] Audit Driver Mobile all screens
- [ ] Audit Dashboard all screens
- [ ] Document Class A RTL bugs

### Week 1, Day 4: Accessibility & Cross-Browser
- [ ] Accessibility audit (Driver Web and Dashboard)
- [ ] Cross-browser test (Chrome, Firefox, Safari)
- [ ] Document accessibility findings

### Week 1, Day 5: Mobile Testing & Documentation
- [ ] Mobile device test (iOS and Android)
- [ ] Update documentation
- [ ] Document final bug list
- [ ] Create onboarding guide

---

## Risks

**Risk 1:** Missing accessibility features in some screens
**Mitigation:** Use browser accessibility tools to systematically check all interactive elements

**Risk 2:** Browser-specific bugs not caught during development
**Mitigation:** Comprehensive cross-browser testing with actual browsers

**Risk 3:** Mobile device issues not caught during development
**Mitigation:** Test on actual simulators and devices

**Risk 4:** Incomplete documentation
**Mitigation:** Allocate dedicated time for documentation updates at end of sprint

---

## Constitution Check

### Principle Review

**I. Pragmatic Architecture** ✅
- Hardening sprint doesn't add new services
- All work focuses on existing apps and design system
- No architecture changes required

**II. Single Source of Truth** ✅
- Design tokens remain in `packages/ui` as single source of truth
- No data model changes in this sprint
- All visual values consumed from tokens

**III. Simple Operations** ✅
- Testing procedures are straightforward and manual
- No complex operations required
- Clear testing checklists provided

**IV. Domain Separation by Schema** ✅
- No database schema changes
- Frontend-only work

**V. Build for Current Scale** ✅
- No optimization work required
- Testing for current feature set

**VI. Public Access First** ✅
- All screens remain accessible without authentication
- No feature gating changes

**VII. RTL & Arabic Built-In** ✅
- Core principle: RTL bugs are Class A
- Arabic layout verification is key focus
- Constitution requirement enforced

**VIII. Visual Consistency** ✅
- All visual values from `packages/ui` tokens
- Cross-app consistency verification is key focus
- No hardcoded values allowed

### Non-Negotiable Rules

- ✅ Arabic RTL layout works correctly on every screen
- ✅ Design tokens used for all visual values
- ✅ No additional services added (no backend work)

### Gate Evaluation

**FAIL if**:
- ❌ RTL bugs prevent users from completing tasks
- ❌ Hardcoded visual values found in components
- ❌ Accessibility violations block basic usage

**PASS if**:
- ✅ Zero Class A bugs found
- ✅ All screens work in Arabic RTL
- ✅ Design tokens used for all visual values
- ✅ Accessibility compliance verified

---

## Phase 1 Done When

- [ ] All three apps run locally with `pnpm install --no-frozen-lockfile && pnpm dev`
- [ ] All screens navigable in all three apps
- [ ] Zero Class A bugs
- [ ] RTL correct on every screen in all three apps
- [ ] Cross-browser test passed (Chrome, Firefox, Safari)
- [ ] iOS and Android smoke test passed
- [ ] All documentation reflects reality
- [ ] Accessibility audit passed (Driver Web and Dashboard)
- [ ] Documentation updated (screens.md, components.md, design-tokens.md, onboarding.md)

---

**Status:** Ready for Implementation
**Start Date:** 2026-06-09
**End Date:** 2026-06-15
**Expected Outcome:** Production-ready frontend applications with zero Class A bugs

---

## Technical Context

**Resolved**: ✅ All unknowns resolved in research.md

**Project Dependencies**:
- Frontend Frameworks: React 19 + TypeScript 5.7 + Vite 6 (Driver Web & Dashboard)
- UI Package: @borne-map/ui (design tokens + components)
- Routing: React Router v7 (Driver Web & Dashboard), React Navigation (Driver Mobile)
- Localization: react-i18next (Arabic, French, English)
- Styling: Tailwind CSS (Driver Web & Dashboard), StyleSheet (Driver Mobile)
- Testing: Vitest + React Testing Library (web), Jest + React Native Testing Library (mobile)

**Architecture Patterns**:
- Shared components in packages/ui
- App-specific components in each app's src/components/
- Composition over inheritance
- Props-driven components
- TypeScript for type safety
- State management via React Context and hooks
- RTL architecture using CSS logical properties and documentElement.dir

**Development Tools**:
- Package Manager: pnpm (latest)
- Build Tools: Vite 6, Expo CLI
- Linting: ESLint + Prettier
- Testing: Vitest (web), Jest (mobile)
- Version Control: Git with feature branches

**Testing Strategy**:
- Unit Testing: ≥ 80% coverage
- Accessibility Testing: Lighthouse + axe DevTools + manual verification
- Cross-Browser Testing: Chrome, Firefox, Safari
- Mobile Testing: iOS Simulator, Android Simulator

---

## Research & Design

**Research Completed**: ✅ research.md created
- All unknowns resolved
- Testing methodologies defined
- Bug classification criteria established

**Data Model**: ✅ data-model.md created
- Documented existing data structures
- Verified design token consistency requirements
- No data model changes in this sprint

**Quick Start Guide**: ✅ quickstart.md created
- How to run all three applications
- Testing checklist for RTL, accessibility, cross-browser, mobile
- Bug tracking and documentation procedures

---

## Clarifications

All clarifications documented in spec.md: