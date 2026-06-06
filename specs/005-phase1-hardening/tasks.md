# Sprint 1.5 Task Breakdown

## Cross-App Consistency Review

### StatusBadge Verification
- [ ] Verify StatusBadge exists in all three apps
- [ ] Verify color tokens match across apps
- [ ] Verify StatusBadge variants (available, in-use, maintenance)
- [ ] Verify StatusBadge is used consistently

### StationCard Verification
- [ ] Verify StationCard exists in all three apps
- [ ] Verify web and mobile variants match visually
- [ ] Verify all fields displayed consistently
- [ ] Verify layout and spacing consistent

### Color Token Verification
- [ ] Verify brand.primary (#007943) used in all active states
- [ ] Verify semantic colors match
- [ ] Verify neutral colors match
- [ ] Verify no hardcoded colors exist

---

## RTL Audit

### Driver Web Screens
- [ ] Home/Map Screen - RTL test
- [ ] Station Detail Screen - RTL test
- [ ] Search Results Screen - RTL test
- [ ] Favorites Screen - RTL test
- [ ] Profile Screen - RTL test
- [ ] Login/Register Screen - RTL test

### Driver Mobile Screens
- [ ] Map/Home Screen - RTL test
- [ ] Station List Screen - RTL test
- [ ] Station Detail Screen - RTL test
- [ ] Search Screen - RTL test
- [ ] Favorites Screen - RTL test
- [ ] Profile Screen - RTL test
- [ ] Login/Register Screen - RTL test

### Dashboard Screens
- [ ] Overview Screen (Partner) - RTL test
- [ ] Overview Screen (Admin) - RTL test
- [ ] My Stations Screen - RTL test
- [ ] Station Edit Screen - RTL test
- [ ] Charger Management Screen - RTL test
- [ ] Availability Update Screen - RTL test
- [ ] Reports Screen - RTL test
- [ ] Users Screen - RTL test
- [ ] Partners Screen - RTL test
- [ ] Stations Screen - RTL test
- [ ] Chargers Screen - RTL test
- [ ] Reviews Screen - RTL test

---

## Accessibility Audit (Driver Web & Dashboard)

### Keyboard Navigation
- [ ] Tab key navigation on all screens
- [ ] Focus order logical
- [ ] Escape closes modals
- [ ] Enter/Space triggers buttons
- [ ] Link navigation

### Focus Indicators
- [ ] All focus states visible
- [ ] Focus rings use brand.primary
- [ ] Adequate contrast
- [ ] OutlineOffset: 2px

### Color Contrast
- [ ] Test all text/background combinations
- [ ] WCAG 2.1 AA compliance check
- [ ] Focus ring contrast check
- [ ] Status color indicators (dot + text)

---

## Cross-Browser Test

### Chrome
- [ ] Driver Web - All 6 screens
- [ ] Dashboard - All 12 screens
- [ ] Console errors check
- [ ] Layout verification
- [ ] Functionality test

### Firefox
- [ ] Driver Web - All 6 screens
- [ ] Dashboard - All 12 screens
- [ ] Console errors check
- [ ] Layout verification
- [ ] Functionality test

### Safari
- [ ] Driver Web - All 6 screens
- [ ] Dashboard - All 12 screens
- [ ] Console errors check
- [ ] Layout verification
- [ ] Functionality test

---

## Mobile Device Test

### iOS Simulator
- [ ] All 7 Driver Mobile screens
- [ ] Navigation functionality
- [ ] Form functionality
- [ ] Pull-to-refresh
- [ ] Keyboard input
- [ ] Safe area insets
- [ ] Large font test

### Android Simulator
- [ ] All 7 Driver Mobile screens
- [ ] Navigation functionality
- [ ] Form functionality
- [ ] Pull-to-refresh
- [ ] Keyboard input
- [ ] Safe area insets
- [ ] Large font test

---

## Documentation Updates

### docs/ui/screens.md
- [ ] List all 25 screens
- [ ] Document navigation structure
- [ ] Document role-based screens
- [ ] Include RTL status

### docs/ui/components.md
- [ ] List all 27 components
- [ ] Document props and variants
- [ ] Document accessibility features

### docs/ui/design-tokens.md
- [ ] List color tokens
- [ ] List typography tokens
- [ ] List spacing tokens
- [ ] List radius tokens
- [ ] List shadow tokens
- [ ] List native tokens

### docs/guides/onboarding.md
- [ ] Run instructions for each app
- [ ] Language switching instructions
- [ ] Role switching instructions
- [ ] Installation instructions
- [ ] Build instructions

---

## Bug Documentation

### Class A Bugs Documentation
- [ ] Create bugs.md template
- [ ] Document all Class A bugs found
- [ ] Document bug severity and impact
- [ ] Document fixes

### Bug Categories
- [ ] RTL bugs
- [ ] Accessibility bugs
- [ ] Cross-browser bugs
- [ ] Mobile device bugs

---

## Phase 1 Done When
- [ ] All apps run locally
- [ ] All screens navigable
- [ ] Zero Class A bugs
- [ ] RTL correct on all screens
- [ ] Cross-browser test passed
- [ ] Mobile test passed
- [ ] Documentation updated
- [ ] Bug list documented