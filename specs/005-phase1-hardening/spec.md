# Sprint 1.5 — Phase 1 Hardening

## Overview

This hardening sprint ensures that all three frontend applications (Driver Web, Driver Mobile, Dashboard) are production-ready with consistent design systems, correct RTL layout, and accessible user interfaces before Phase 2 (Database Foundation) begins.

## Goal

**All three apps are solid, consistent, and RTL-correct before any backend work begins.**

### Key Objectives

1. **Cross-app consistency** - Verify design system usage across all apps
2. **RTL audit** - Ensure every screen works correctly in Arabic
3. **Accessibility** - Verify keyboard navigation, focus indicators, and color contrast
4. **Cross-browser compatibility** - Test on Chrome, Firefox, Safari
5. **Mobile compatibility** - Test on iOS and Android
6. **Documentation** - Update all documentation to match reality

## Apps to Audit

- **Driver Web** - 6 screens
- **Driver Mobile** - 7 screens
- **Dashboard** - 12 screens (6 for Partner, 7 for Admin)

### Navigation Structure

**Driver Web:**
- `/` - Home/Map
- `/stations/:id` - Station Detail
- `/search` - Search Results
- `/favorites` - Favorites
- `/profile` - Profile
- `/login` - Login/Register

**Driver Mobile:**
- HomeMap (tab)
- StationList (tab)
- Search (tab)
- Favorites (tab)
- Profile (tab)
- StationDetail (stack)
- LoginRegister (stack)

**Dashboard:**
- `/` - Overview (role-based)
- `/stations` - My Stations (Partner)
- `/stations/:id/edit` - Station Edit
- `/chargers` - Charger Management
- `/availability` - Availability Update
- `/reports` - Reports
- `/users` - Users (Admin)
- `/partners` - Partners (Admin)
- `/admin/stations` - Stations (Admin)
- `/admin/chargers` - Chargers (Admin)
- `/admin/reviews` - Reviews (Admin)

## Cross-App Consistency

### StatusBadge Components

**Verify across all apps:**
- Available state: Green dot + text label
- In-use state: Amber dot + text label
- Maintenance state: Red dot + text label

### StationCard Components

**Verify across web and mobile:**
- Layout consistency
- Visual hierarchy
- Spacing and padding
- Color usage

### Color Tokens

**Verify brand colors:**
- brand.primary: #007943 (must appear in all active states)
- brand.sageLight, brand.sageDark, brand.sageText
- All semantic colors (success, warning, error)
- All neutral colors

## RTL Audit Requirements

### All Screens Must Work in Arabic

**Driver Web Screens:**
1. Home/Map
2. Station Detail
3. Search Results
4. Favorites
5. Profile
6. Login/Register

**Driver Mobile Screens:**
1. Map/Home
2. Station List
3. Station Detail
4. Search
5. Favorites
6. Profile
7. Login/Register

**Dashboard Screens:**
1. Overview (Partner & Admin variants)
2. My Stations
3. Station Edit
4. Charger Management
5. Availability Update
6. Reports
7. Users
8. Partners
9. Stations
10. Chargers
11. Reviews

### RTL Requirements

- Sidebar aligns to right in Arabic
- Tables have correct alignment
- Forms have correct input order
- Buttons have correct direction
- Text flows left-to-right in RTL
- Icons maintain correct direction
- Padding/margins respect RTL

### Class A RTL Bugs

**Class A Bugs** (Must fix before Phase 2):
- RTL bugs that completely block functionality
- RTL bugs preventing user from completing tasks
- RTL bugs that cause layout breaking
- RTL bugs that make content unreadable

## Accessibility Requirements (Driver Web & Dashboard)

### Keyboard Navigation

- [ ] Tab key navigates all interactive elements
- [ ] Focus indicators visible
- [ ] Focus order is logical
- [ ] Escape closes modals/dialogs
- [ ] Enter/Space triggers buttons

### Focus Indicators

- [ ] All focus states visible
- [ ] Focus rings use brand.primary (#007943)
- [ ] 2px outline with outlineOffset: 2px
- [ ] Adequate contrast

### Color Contrast

- [ ] All text/background meet WCAG 2.1 AA
- [ ] Primary text: neutral-900 on neutral-50 (4.5:1)
- [ ] Secondary text: neutral-600 on neutral-50 (7:1)
- [ ] Buttons: white on brand.primary (3.5:1 minimum)
- [ ] Focus rings: brand.primary on any background
- [ ] Status colors: use dots + text labels

## Cross-Browser Requirements

**Test on Chrome, Firefox, Safari** for Driver Web and Dashboard:

- [ ] No console errors
- [ ] All features work correctly
- [ ] Layout renders correctly
- [ ] Forms work as expected
- [ ] Navigation works as expected
- [ ] No layout shifts
- [ ] Lighthouse accessibility score ≥ 90

## Mobile Requirements (Driver Mobile)

**Test on iOS Simulator and Android Simulator**:

- [ ] All 7 screens render correctly
- [ ] Navigation works
- [ ] Forms work
- [ ] Pull-to-refresh works
- [ ] Keyboard input works
- [ ] Safe area insets respected
- [ ] Large font sizes render correctly
- [ ] Touch targets ≥ 44x44 points

## Documentation Requirements

### Update `docs/ui/screens.md`

- List all 25 screens (6 + 7 + 12)
- Document navigation structure
- Document role-based screens
- Include RTL status for each screen

### Update `docs/ui/components.md`

- List all 27 components (9 + 12 + 6)
- Document props and variants
- Document accessibility features

### Update `docs/ui/design-tokens.md`

- List all color tokens
- List all typography tokens
- List all spacing tokens
- List all radius tokens
- List all shadow tokens
- List all native tokens

### Create `docs/guides/onboarding.md`

- How to run each app
- How to switch languages
- How to switch roles in Dashboard
- How to install dependencies
- How to build each app

## Class A Bugs

### Bug Documentation

For each Class A bug, document in `docs/project/bugs.md`:

- Bug title
- Severity: Class A
- App and screen
- Bug type
- Description
- Steps to reproduce
- Expected vs actual behavior
- Severity reason
- Status and fix

### Bug Categories

- RTL bugs
- Accessibility bugs
- Cross-browser bugs
- Mobile device bugs

## Phase 1 Done When

- [ ] All three apps run locally with `pnpm install --no-frozen-lockfile && pnpm dev`
- [ ] All screens navigable in all three apps
- [ ] Zero Class A bugs
- [ ] RTL correct on every screen
- [ ] Cross-browser test passed
- [ ] iOS and Android smoke test passed
- [ ] All documentation updated

## Success Criteria

### Consistency
- StatusBadge renders identically across all apps
- StationCard visually consistent across web and mobile
- Color tokens resolve correctly
- Brand colors appear correctly

### RTL Quality
- All screens work in Arabic
- Zero Class A RTL bugs
- RTL layout is professional
- Tables, forms, navigation align correctly

### Accessibility
- Keyboard navigation works
- Focus indicators are visible
- Color contrast meets WCAG AA
- Status colors have non-color indicators

### Cross-Platform
- All screens work in Chrome, Firefox, Safari
- All screens work in iOS and Android
- Large font sizes render correctly
- Safe area insets respected

### Documentation
- screens.md updated
- components.md updated
- design-tokens.md updated
- onboarding.md created

## Deliverables

1. **Code** - Any fixes for bugs found
2. **Documentation** - Updated docs/ui/screens.md, components.md, design-tokens.md
3. **Guide** - New docs/guides/onboarding.md
4. **Bug List** - Class A bugs documented in docs/project/bugs.md
5. **Audit Report** - Completion report with status of all checkpoints

## Timeline

- **Day 1-2**: Cross-app consistency review
- **Day 3**: RTL audit
- **Day 4**: Accessibility & cross-browser testing
- **Day 5**: Mobile testing & documentation

## Notes

- This sprint is a hardening sprint - focus on quality and completeness
- No new features should be added
- Focus on fixing bugs and improving quality
- Documentation must match reality exactly