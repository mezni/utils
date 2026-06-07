# Sprint 1.5 — Phase 1 Hardening - Quick Start

**Date**: 2026-06-06
**Feature**: Sprint 1.5 Phase 1 Hardening
**Branch**: `005-phase1-hardening`

---

## Purpose

This guide helps developers run, test, and verify the three frontend applications during the Sprint 1.5 Phase 1 hardening sprint.

---

## Prerequisites

### Required Tools

- **Node.js**: v20.x or higher
- **pnpm**: Latest stable version
- **Xcode**: For iOS Simulator (Mac only)
- **Android Studio**: For Android Simulator (optional)

### Installation

1. **Install pnpm** (if not already installed):
```bash
npm install -g pnpm
```

2. **Clone the repository**:
```bash
git clone https://github.com/mezni/BorneMap.git
cd BorneMap
```

3. **Install dependencies**:
```bash
pnpm install --no-frozen-lockfile
```

---

## Running the Applications

### 1. Driver Web App

**Location**: `apps/driver-web/`

**Start the development server**:
```bash
cd apps/driver-web
pnpm dev
```

**Default URL**: http://localhost:5173

**Features to test**:
- [ ] All 6 screens navigable
- [ ] Arabic (RTL) layout correct
- [ ] French language works
- [ ] Station List, Station Detail, Search, Favorites, Profile, Login screens

**Test in Arabic**:
1. Go to the settings or language selector
2. Select "العربية" (Arabic)
3. Verify RTL layout is correct
4. Test navigation in Arabic

**Test in French**:
1. Go to the settings or language selector
2. Select "Français"
3. Verify layout is correct

**Run accessibility audit**:
```bash
cd apps/driver-web
pnpm test:a11y
# Or use Lighthouse in browser
```

**Run tests**:
```bash
cd apps/driver-web
pnpm test
```

**Build for production**:
```bash
cd apps/driver-web
pnpm build
```

---

### 2. Driver Mobile App

**Location**: `apps/driver-mobile/`

**Start the development server**:
```bash
cd apps/driver-mobile
pnpm dev
```

**Note**: For iOS, use Expo Go on a physical device or run the iOS simulator:
```bash
cd apps/driver-mobile
npx expo start --ios
```

For Android:
```bash
cd apps/driver-mobile
npx expo start --android
```

**Features to test**:
- [ ] All 7 screens navigable
- [ ] Arabic (RTL) layout correct
- [ ] French language works
- [ ] HomeMap, StationList, Search, Favorites, Profile, StationDetail, LoginRegister screens

**Test in Arabic**:
1. Go to the settings or language selector
2. Select "العربية" (Arabic)
3. Verify RTL layout is correct
4. Test navigation in Arabic
5. Test pull-to-refresh in Arabic

**Test in French**:
1. Go to the settings or language selector
2. Select "Français"
3. Verify layout is correct

**Run tests**:
```bash
cd apps/driver-mobile
pnpm test
```

**Build for production**:
```bash
cd apps/driver-mobile
pnpm build
```

**Run EAS Build** (for production):
```bash
cd apps/driver-mobile
eas build --platform ios --profile production
eas build --platform android --profile production
```

---

### 3. Dashboard App

**Location**: `apps/dashboard/`

**Start the development server**:
```bash
cd apps/dashboard
pnpm dev
```

**Default URL**: http://localhost:5174

**Features to test**:
- [ ] All 12 screens navigable (6 Partner + 7 Admin screens)
- [ ] Role switching works
- [ ] Arabic (RTL) layout correct
- [ ] French language works
- [ ] Partner Overview, My Stations, Station Edit, Charger Management, Availability Update, Reports
- [ ] Admin Overview, Users, Partners, Stations, Chargers, Reviews

**Test in Arabic**:
1. Go to the settings or language selector
2. Select "العربية" (Arabic)
3. Verify RTL layout is correct
4. Test navigation in Arabic
5. Test role switching in Arabic

**Test in French**:
1. Go to the settings or language selector
2. Select "Français"
3. Verify layout is correct

**Test Role Switching**:
1. Click on the user/profile icon
2. Select a different role (Partner/Admin)
3. Verify sidebar and navigation update correctly
4. Test navigation in both roles in Arabic

**Run tests**:
```bash
cd apps/dashboard
pnpm test
```

**Build for production**:
```bash
cd apps/dashboard
pnpm build
```

---

## Testing Checklist

### RTL Testing (All 25 Screens)

**Driver Web (6 screens)**:
- [ ] Home/Map screen in Arabic
- [ ] Station Detail screen in Arabic
- [ ] Search Results screen in Arabic
- [ ] Favorites screen in Arabic
- [ ] Profile screen in Arabic
- [ ] Login/Register screen in Arabic

**Driver Mobile (7 screens)**:
- [ ] Map/Home screen in Arabic
- [ ] Station List screen in Arabic
- [ ] Station Detail screen in Arabic
- [ ] Search screen in Arabic
- [ ] Favorites screen in Arabic
- [ ] Profile screen in Arabic
- [ ] Login/Register screen in Arabic

**Dashboard (12 screens)**:
- [ ] Partner Overview screen in Arabic
- [ ] My Stations screen in Arabic
- [ ] Station Edit screen in Arabic
- [ ] Charger Management screen in Arabic
- [ ] Availability Update screen in Arabic
- [ ] Reports screen in Arabic
- [ ] Admin Overview screen in Arabic
- [ ] Users screen in Arabic
- [ ] Partners screen in Arabic
- [ ] Stations screen in Arabic
- [ ] Chargers screen in Arabic
- [ ] Reviews screen in Arabic

**RTL Verification Checklist**:
- [ ] Sidebar aligns to right in Arabic
- [ ] Tables have correct alignment
- [ ] Forms have correct input order
- [ ] Buttons have correct direction
- [ ] Text flows left-to-right in RTL
- [ ] Icons maintain correct direction
- [ ] Padding/margins respect RTL

---

### Accessibility Testing (Driver Web & Dashboard)

**Keyboard Navigation**:
- [ ] Tab key navigates all interactive elements
- [ ] Focus indicators visible on all interactive elements
- [ ] Focus order is logical (left-to-right, top-to-bottom)
- [ ] Escape key closes modals/dialogs
- [ ] Enter/Space triggers buttons
- [ ] Link navigation works correctly

**Focus Indicators**:
- [ ] All focus states visible
- [ ] Focus rings use brand.primary (#007943)
- [ ] Focus rings have 2px outline with outlineOffset: 2px
- [ ] Focus rings don't obscure content

**Color Contrast**:
- [ ] All text/background combinations meet WCAG 2.1 AA
- [ ] Status colors have non-color indicators
- [ ] Focus indicators have adequate contrast

**Run Accessibility Audit**:
```bash
# Driver Web
cd apps/driver-web
pnpm test:a11y

# Dashboard
cd apps/dashboard
pnpm test:a11y
```

Or use Lighthouse in browser:
- Open Chrome DevTools
- Go to Lighthouse tab
- Run Accessibility audit
- Check for violations

---

### Cross-Browser Testing (Driver Web & Dashboard)

**Chrome**:
```bash
# Driver Web
cd apps/driver-web
pnpm build
# Open dist/index.html in Chrome

# Dashboard
cd apps/dashboard
pnpm build
# Open dist/index.html in Chrome
```

**Firefox**:
```bash
# Open the built files in Firefox
```

**Safari**:
```bash
# Open the built files in Safari
```

**Verification Checklist**:
- [ ] No console errors
- [ ] All features work correctly
- [ ] Layout renders correctly
- [ ] Forms work as expected
- [ ] Navigation works as expected
- [ ] No layout shifts

---

### Mobile Testing (Driver Mobile)

**iOS Simulator**:
```bash
cd apps/driver-mobile
npx expo start --ios
```

**Android Simulator**:
```bash
cd apps/driver-mobile
npx expo start --android
```

**Verification Checklist**:
- [ ] All 7 screens render correctly
- [ ] Navigation works
- [ ] Forms work
- [ ] Pull-to-refresh works
- [ ] Keyboard input works
- [ ] Safe area insets respected
- [ ] Large font sizes render correctly
- [ ] Touch targets ≥ 44x44 points

**Test with large font**:
```bash
# iOS Simulator
Settings → Accessibility → Display & Text Size → Larger Text

# Android Simulator
Settings → Display → Font Size
```

---

## Design Token Verification

### Check Color Tokens

```bash
# Verify all apps use @borne-map/ui
# Check packages/ui/src/tokens/ for definitions
```

**Verify**:
- [ ] All apps reference tokens from `@borne-map/ui`
- [ ] No hardcoded colors in components
- [ ] All color values match specification
- [ ] Brand colors appear correctly in all apps

**Check files**:
```bash
# Driver Web
grep -r "#007943" apps/driver-web/src/components/

# Driver Mobile
grep -r "#007943" apps/driver-mobile/src/components/

# Dashboard
grep -r "#007943" apps/dashboard/src/components/
```

**Expected**: All hardcoded colors should reference tokens, not be hardcoded.

---

## Component Consistency Verification

### StatusBadge Consistency

**Verify across all apps**:
- [ ] Available state: Green dot + text label
- [ ] In-use state: Amber dot + text label
- [ ] Maintenance state: Red dot + text label
- [ ] Same color values in web and mobile variants
- [ ] Same text labels in all languages

### StationCard Consistency

**Verify across web and mobile**:
- [ ] Layout consistency
- [ ] Visual hierarchy
- [ ] Spacing and padding
- [ ] Color usage

### Color Token Verification

**Verify brand colors**:
- [ ] brand.primary: #007943 (must appear in all active states)
- [ ] brand.sageLight, brand.sageDark, brand.sageText
- [ ] All semantic colors (success, warning, error)
- [ ] All neutral colors

---

## Bug Tracking

### Document Class A Bugs

```bash
# Create or update bug tracking document
# Location: docs/project/bugs.md

# Format:
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

**Class A (Must fix before Phase 2)**:
- RTL bugs that completely block functionality in Arabic
- Accessibility bugs that block keyboard navigation
- Critical cross-browser bugs
- Critical mobile device bugs

**Class B (Can defer to Phase 2)**:
- Minor RTL layout issues
- Minor accessibility issues
- Non-critical cross-browser issues
- Non-critical mobile device issues

**Class C (Can defer to Phase 2)**:
- Cosmetic issues
- Minor UX issues
- Non-critical bugs

---

## Common Issues & Troubleshooting

### Issue: App doesn't start

**Solution**:
```bash
# Clear node_modules and reinstall
rm -rf node_modules apps/*/node_modules
pnpm install --no-frozen-lockfile
```

### Issue: RTL layout broken

**Solution**:
```bash
# Verify language is set to Arabic
# Check documentElement.dir = 'rtl'
# Verify CSS logical properties are used (ms, me, ps, pe instead of ml, mr, pl, pr)
```

### Issue: Accessibility audit fails

**Solution**:
- Run Lighthouse audit in browser
- Check specific violations
- Fix focus indicators and color contrast
- Add keyboard navigation

### Issue: Mobile app crashes

**Solution**:
- Check console for errors
- Verify all dependencies are installed
- Test with Expo Go on physical device
- Check Expo documentation for known issues

---

## Documentation Updates

### Update `docs/ui/screens.md`

**Content**:
- List all 25 screens (6 + 7 + 12)
- Document navigation structure
- Document role-based screens
- Include RTL status for each screen

**Format**:
```markdown
## Screen Name

**App**: Driver Web / Driver Mobile / Dashboard
**Path**: /path/to/screen
**Dependencies**: [components used]
**RTL Status**: ✅ / ❌
**Accessibility**: ✅ / ❌ / Partial
```

### Update `docs/ui/components.md`

**Content**:
- List all 27 components (9 + 12 + 6)
- Document props and variants
- Document accessibility features

**Format**:
```markdown
## Component Name

**App**: Driver Web / Driver Mobile / Dashboard
**Props**: [prop definitions]
**Variants**: [variant options]
**Accessibility**: [features]
**RTL Support**: ✅ / ❌ / Partial
```

### Update `docs/ui/design-tokens.md`

**Content**:
- List all color tokens
- List all typography tokens
- List all spacing tokens
- List all radius tokens
- List all shadow tokens

**Format**:
```markdown
## Token Category

**Category**: Colors / Typography / Spacing / Radius / Shadows

### Token Name

**Value**: [hex/px value]
**Usage**: [description]
**RTL Compatible**: Yes/No
```

### Create `docs/guides/onboarding.md`

**Content**:
- How to run each app
- How to switch languages
- How to switch roles in Dashboard
- How to install dependencies
- How to build each app

**Format**:
```markdown
# Driver Web Onboarding

## Installation

```bash
cd apps/driver-web
pnpm install --no-frozen-lockfile
```

## Development

```bash
pnpm dev
```

## Testing

[Instructions for testing RTL, accessibility, etc.]
```

---

## Build & Deploy

### Build All Apps

```bash
# Build Driver Web
cd apps/driver-web
pnpm build

# Build Driver Mobile
cd apps/driver-mobile
pnpm build

# Build Dashboard
cd apps/dashboard
pnpm build
```

### Deploy to Production

**Driver Web**:
```bash
cd apps/driver-web
pnpm build
# Deploy dist/ directory
```

**Driver Mobile**:
```bash
cd apps/driver-mobile
eas build --platform all --profile production
```

**Dashboard**:
```bash
cd apps/dashboard
pnpm build
# Deploy dist/ directory
```

---

## Quick Commands Reference

```bash
# Install dependencies
pnpm install --no-frozen-lockfile

# Driver Web
cd apps/driver-web && pnpm dev
cd apps/driver-web && pnpm test
cd apps/driver-web && pnpm build

# Driver Mobile
cd apps/driver-mobile && pnpm dev
cd apps/driver-mobile && pnpm test
cd apps/driver-mobile && pnpm build

# Dashboard
cd apps/dashboard && pnpm dev
cd apps/dashboard && pnpm test
cd apps/dashboard && pnpm build

# Run all tests
pnpm test
```

---

## Conclusion

Use this guide to run and test all three applications during the Sprint 1.5 Phase 1 hardening sprint. Follow the testing checklist for each application and document any bugs found in `docs/project/bugs.md`.

Good luck with the hardening sprint! 🚀
