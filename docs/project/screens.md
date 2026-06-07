# Screens Specification — Sprint 1.5 Phase 1 Hardening

**Document Version:** 2.0
**Last Updated:** 2026-06-06
**Status:** Complete with Phase 1 Hardening Requirements

---

## Overview

This document documents all 25 screens across three applications (Driver Web, Driver Mobile, Dashboard) with Phase 1 hardening requirements including RTL, accessibility, cross-browser, and mobile testing checklist items.

**Clarifications from Session 2026-06-06:**
- Equal hardening effort across all 25 screens (6 Driver Web + 7 Driver Mobile + 12 Dashboard)
- Class A bugs defined as: bugs that completely prevent users from completing tasks OR bugs that make content completely unreadable in Arabic RTL layout
- Only Class A bugs must be fixed before Phase 2; Class B and C bugs can be deferred to Phase 2 backlog
- Mobile testing on latest stable versions (iOS 18+ and Android 15+)
- Accessibility verification uses automated tools for first pass, manual verification for high-risk areas

---

## Driver Web App Screens (6 Screens)

### 1. Home / Map Screen

**Purpose:** Primary discovery interface with map-based station exploration
**Navigation Path:** `/` (root)
**Dependencies:** `SearchBar`, `FilterPills`, `MapPinMarker`, `ZoomControls`, `BottomStationCard`

**Phase 1 Hardening Checklist:**

#### RTL Requirements
- [ ] Arabic language selected
- [ ] RTL layout applied (documentElement.dir = 'rtl')
- [ ] Search bar aligns to right edge in RTL
- [ ] Filter pills align to right in RTL
- [ ] Zoom controls align to left edge in RTL (mirrored)
- [ ] Station card sidebar aligns to right in RTL
- [ ] Map interaction remains consistent in RTL

#### Accessibility
- [ ] Keyboard navigation works (Tab, Arrow keys for map controls)
- [ ] Focus indicators visible on all interactive elements
- [ ] Search bar has proper ARIA label
- [ ] Filter pills have aria-labels
- [ ] All map controls have accessible descriptions
- [ ] Color contrast meets WCAG AA (labels on pins, buttons)
- [ ] Screen reader announces map markers correctly
- [ ] Map has appropriate alt text description

#### Cross-Browser Compatibility
- [ ] Chrome (all versions) renders correctly
- [ ] Firefox renders correctly
- [ ] Safari renders correctly
- [ ] No console errors
- [ ] No layout shifts when switching RTL
- [ ] Map interaction works consistently

#### Mobile Testing
- [ ] Touch targets ≥ 44×44px for all map controls
- [ ] Pin tapping works reliably
- [ ] Search bar works on touch devices
- [ ] Safe area insets respected on iPhone with notch
- [ ] Bottom sheet opens correctly from bottom edge
- [ ] No scroll issues on mobile browsers

**Class A Bug Definition:**
- [ ] RTL layout breaks completely (content not readable)
- [ ] Map markers don't appear in RTL
- [ ] Station card sidebar aligns incorrectly in RTL
- [ ] Search input doesn't accept Arabic text in RTL
- [ ] Navigation controls missing in RTL

---

### 2. Station Detail Screen

**Purpose:** Detailed view of individual charging station with chargers, reviews, and actions
**Navigation Path:** `/stations/:id`
**Dependencies:** `StationHeader`, `SpecRow`, `ChargerRow`, `StatusBadge`, `ReviewCard`, `MapPreview`

**Phase 1 Hardening Checklist:**

#### RTL Requirements
- [ ] Arabic language selected
- [ ] RTL layout applied (documentElement.dir = 'rtl')
- [ ] Station header content aligns correctly in RTL
- [ ] SpecRow labels align to right in RTL
- [ ] SpecRow values align to left in RTL
- [ ] Charger list flows correctly in RTL
- [ ] Review cards align properly in RTL
- [ ] Action buttons align correctly (Favorite, Write Review, Share)
- [ ] Map preview orientation correct in RTL

#### Accessibility
- [ ] Station header has proper heading hierarchy
- [ ] All charger rows keyboard navigable
- [ ] Status badges have non-color indicators
- [ ] Review cards have aria-labels for ratings
- [ ] Favorite button has aria-pressed state
- [ ] Map preview has aria-label
- [ ] Form inputs (if any) have proper labels
- [ ] Color contrast meets WCAG AA for all text

#### Cross-Browser Compatibility
- [ ] Chrome displays all charger rows correctly
- [ ] Firefox displays all charger rows correctly
- [ ] Safari displays all charger rows correctly
- [ ] Review cards scroll correctly in RTL
- [ ] Map preview renders without errors

#### Mobile Testing
- [ ] Touch targets for all buttons ≥ 44×44px
- [ ] Review list scrolls smoothly
- [ ] Favorite button toggles reliably on touch
- [ ] Map preview is touch-interactive
- [ ] Bottom sheet/modal closes correctly on touch
- [ ] Large font sizes render correctly

**Class A Bug Definition:**
- [ ] RTL causes content to overlap or hide
- [ ] Charger list reverses incorrectly in RTL (not just column order)
- [ ] Review ratings don't display correctly in RTL
- [ ] Station header layout breaks in RTL
- [ ] Map preview doesn't show in RTL

---

### 3. Search Results Screen

**Purpose:** Display filtered station list with search and filter controls
**Navigation Path:** `/search`
**Dependencies:** `SearchBar`, `FilterPills`, `StationCardList`, `Pagination`

**Phase 1 Hardening Checklist:**

#### RTL Requirements
- [ ] Arabic language selected
- [ ] RTL layout applied (documentElement.dir = 'rtl')
- [ ] Search bar aligns to right edge
- [ ] Filter pills align to right in RTL
- [ ] Station card list starts from right in RTL
- [ ] Results count ("42 stations found") aligns correctly
- [ ] Clear filters button aligns correctly
- [ ] Pagination aligns correctly (right to left flow)

#### Accessibility
- [ ] Search input has ARIA label
- [ ] Filter pills have aria-labels
- [ ] Station cards have aria-labels
- [ ] Pagination buttons have accessible labels
- [ ] Keyboard navigation through search and results
- [ ] Results count announced to screen readers
- [ ] Focus management when switching tabs

#### Cross-Browser Compatibility
- [ ] Chrome search results list renders correctly
- [ ] Firefox search results list renders correctly
- [ ] Safari search results list renders correctly
- [ ] Pagination works in RTL
- [ ] Clear filters button works in RTL
- [ ] Search filtering works correctly

#### Mobile Testing
- [ ] Touch targets for filter pills ≥ 44×44px
- [ ] Station cards scroll smoothly
- [ ] Search bar works reliably on touch
- [ ] Clear filters button is accessible on touch
- [ ] Pagination buttons accessible on touch

**Class A Bug Definition:**
- [ ] Station cards display in reversed order in RTL (should be right-aligned list)
- [ ] Search bar doesn't accept Arabic input in RTL
- [ ] Filter pills misaligned in RTL
- [ ] Pagination buttons don't work in RTL
- [ ] Results count text displays incorrectly in RTL

---

### 4. Favorites Screen

**Purpose:** View and manage saved station favorites
**Navigation Path:** `/favorites`
**Dependencies:** `SearchBar`, `StationCardList`, `EmptyState`, `FavoriteButton`

**Phase 1 Hardening Checklist:**

#### RTL Requirements
- [ ] Arabic language selected
- [ ] RTL layout applied (documentElement.dir = 'rtl')
- [ ] Favorites header aligns correctly
- [ ] Search bar aligns to right edge
- [ ] Station card list starts from right in RTL
- [ ] Empty state button aligns correctly
- [ ] Favorite heart icon maintains direction

#### Accessibility
- [ ] All station cards keyboard navigable
- [ ] Favorite button has aria-pressed state
- [ ] Search bar has ARIA label
- [ ] Empty state has clear description
- [ ] Favorite button provides feedback on click
- [ ] Focus indicators visible on all interactive elements

#### Cross-Browser Compatibility
- [ ] Chrome displays all favorites correctly
- [ ] Firefox displays all favorites correctly
- [ ] Safari displays all favorites correctly
- [ ] Sorting works correctly in RTL
- [ ] Empty state displays correctly in RTL

#### Mobile Testing
- [ ] Touch targets for favorite buttons ≥ 44×44px
- [ ] Station cards scroll smoothly
- [ ] Search bar works reliably on touch
- [ ] Empty state button is accessible
- [ ] Sorting options accessible on touch

**Class A Bug Definition:**
- [ ] Favorites list reversed incorrectly in RTL (should be right-to-left, not reversed column order)
- [ ] Empty state button misaligned in RTL
- [ ] Search bar doesn't filter favorites correctly in RTL
- [ ] Sorting doesn't work in RTL

---

### 5. Profile Screen

**Purpose:** User account settings and personal information management
**Navigation Path:** `/profile`
**Dependencies:** `Avatar`, `NameInput`, `EmailInput`, `PhoneInput`, `LanguageSelect`, `SaveButton`, `LogoutButton`

**Phase 1 Hardening Checklist:**

#### RTL Requirements
- [ ] Arabic language selected
- [ ] RTL layout applied (documentElement.dir = 'rtl')
- [ ] Profile header aligns correctly
- [ ] Form labels align to right in RTL
- [ ] Form inputs align to left in RTL
- [ ] Language select dropdown aligns correctly
- [ ] Save button aligns correctly
- [ ] Logout button aligns correctly
- [ ] Avatar alignment correct in RTL

#### Accessibility
- [ ] All form inputs have proper labels
- [ ] Focus indicators visible on all inputs
- [ ] Form validation messages are visible
- [ ] Avatar upload is keyboard accessible
- [ ] Language select has accessible description
- [ ] Save button has aria-label
- [ ] Logout button has aria-label and clear visual
- [ ] Form fields are properly grouped

#### Cross-Browser Compatibility
- [ ] Chrome form rendering correct
- [ ] Firefox form rendering correct
- [ ] Safari form rendering correct
- [ ] Form validation works in RTL
- [ ] Avatar upload works in all browsers
- [ ] Language select works in RTL

#### Mobile Testing
- [ ] Touch targets for all buttons ≥ 44×44px
- [ ] Form inputs work reliably on touch
- [ ] Avatar upload works on touch
- [ ] Language select works on touch
- [ ] Save button provides feedback on touch
- [ ] Keyboard works reliably on mobile

**Class A Bug Definition:**
- [ ] Form labels don't align correctly in RTL (should be right-aligned)
- [ ] Form inputs reverse direction in RTL (should be left-aligned values)
- [ ] Language select dropdown doesn't open in RTL
- [ ] Form validation breaks in RTL

---

### 6. Login/Register Screen

**Purpose:** Authentication entry point for users
**Navigation Path:** `/login` (or `/register`)
**Dependencies:** `Logo`, `EmailInput`, `PasswordInput`, `SignInButton`, `SocialLoginButtons`, `CreateAccountLink`, `ForgotPasswordLink`

**Phase 1 Hardening Checklist:**

#### RTL Requirements
- [ ] Arabic language selected
- [ ] RTL layout applied (documentElement.dir = 'rtl')
- [ ] Login card aligns to right in RTL
- [ ] Logo centering correct in RTL
- [ ] Email input aligns to right in RTL
- [ ] Password input aligns to right in RTL
- [ ] Social buttons align correctly
- [ ] "Create Account" link aligns correctly
- [ ] "Forgot Password" link aligns correctly
- [ ] Form labels align to right in RTL

#### Accessibility
- [ ] Form inputs have proper labels
- [ ] Focus indicators visible on all inputs
- [ ] Submit button has aria-label
- [ ] Social login buttons have aria-labels
- [ ] Create account link has aria-label
- [ ] Forgot password link has aria-label
- [ ] Form validation works
- [ ] Error messages are visible and descriptive

#### Cross-Browser Compatibility
- [ ] Chrome login form renders correctly
- [ ] Firefox login form renders correctly
- [ ] Safari login form renders correctly
- [ ] Form validation works in all browsers
- [ ] Social login buttons work in all browsers
- [ ] Password field masking works correctly

#### Mobile Testing
- [ ] Touch targets for all buttons ≥ 44×44px
- [ ] Form inputs work reliably on touch
- [ ] Social login buttons work on touch
- [ ] Keyboard input works reliably
- [ ] Form validation provides feedback on touch
- [ ] No layout issues on small screens

**Class A Bug Definition:**
- [ ] Login card doesn't align to right in RTL
- [ ] Form labels don't align correctly in RTL
- [ ] Form inputs reverse direction in RTL
- [ ] Social login buttons misaligned
- [ ] Form validation doesn't work in RTL

---

## Driver Mobile App Screens (7 Screens)

### 7. Map / Home Screen

**Purpose:** Primary mobile discovery with map and floating UI elements
**Navigation Path:** Bottom tab "Home"
**Dependencies:** `MobileShell`, `MobileTopBar`, `SearchBar`, `FilterPills`, `MapPinMarker`, `BottomStationCard`, `BottomTabBar`, `CenterActionButton`, `ZoomControls`

**Phase 1 Hardening Checklist:**

#### RTL Requirements
- [ ] Arabic language selected
- [ ] RTL layout applied (via React Native RTL support)
- [ ] Bottom tab bar aligns to right in RTL
- [ ] Bottom station card expands correctly in RTL
- [ ] Header aligns to right in RTL
- [ ] Search bar aligns to right in RTL
- [ ] Filter pills align to right in RTL
- [ ] Center action button position correct in RTL
- [ ] Safe area insets respected in RTL
- [ ] Pull-to-refresh works in RTL

#### Accessibility
- [ ] All interactive elements are touch accessible
- [ ] Station pins have aria-labels
- [ ] Search bar has aria-label
- [ ] Filter pills have aria-labels
- [ ] Bottom station card has aria-label for expansion
- [ ] Bottom tab bar has accessible descriptions
- [ ] Center action button has aria-label
- [ ] Zoom controls have aria-labels

#### Cross-Browser Compatibility
- [ ] iOS (all versions) renders correctly
- [ ] Android (all versions) renders correctly
- [ ] No console errors
- [ ] Map interaction works consistently
- [ ] Bottom sheet animations work in RTL
- [ ] Safe area handling works correctly

#### Mobile Testing
- [ ] Touch targets ≥ 44×44px for all interactive elements
- [ ] Station pin tapping works reliably
- [ ] Bottom station card expands/collapses correctly
- [ ] Pull-to-refresh works reliably
- [ ] Tab bar navigation works correctly
- [ ] Safe area insets handled correctly
- [ ] Keyboard input works reliably
- [ ] Large font sizes render correctly

**Class A Bug Definition:**
- [ ] Bottom tab bar misaligned in RTL
- [ ] Bottom station card expansion direction incorrect in RTL
- [ ] Pull-to-refresh doesn't work in RTL
- [ ] Safe area insets cause layout breaks in RTL

---

### 8. Station List Screen

**Purpose:** Scrollable list of stations with search
**Navigation Path:** Bottom tab "Station List"
**Dependencies:** `SearchBar`, `StationCardList`, `EmptyState`, `InfiniteScroll`

**Phase 1 Hardening Checklist:**

#### RTL Requirements
- [ ] Arabic language selected
- [ ] RTL layout applied (via React Native RTL support)
- [ ] Search bar aligns to right in RTL
- [ ] Station card list starts from right in RTL
- [ ] Empty state aligns correctly
- [ ] Infinite scroll indicator aligns correctly
- [ ] Pull-to-refresh works in RTL

#### Accessibility
- [ ] Station cards have aria-labels
- [ ] Search bar has aria-label
- [ ] Empty state has clear description
- [ ] Infinite scroll indicator has aria-label
- [ ] All cards are keyboard accessible
- [ ] Focus indicators visible

#### Cross-Browser Compatibility
- [ ] iOS renders list correctly
- [ ] Android renders list correctly
- [ ] Infinite scroll works in RTL
- [ ] Empty state displays correctly
- [ ] Search filtering works in RTL

#### Mobile Testing
- [ ] Touch targets ≥ 44×44px
- [ ] Station cards scroll smoothly
- [ ] Infinite scroll works reliably
- [ ] Search bar works reliably on touch
- [ ] Empty state button accessible

**Class A Bug Definition:**
- [ ] Station list reversed incorrectly in RTL (should be right-to-left)
- [ ] Search bar doesn't filter correctly in RTL
- [ ] Infinite scroll indicator misaligned

---

### 9. Station Detail Screen

**Purpose:** Full-screen station information
**Navigation Path:** Station List → Tap Card
**Dependencies:** `StationHeader`, `SpecRow`, `ChargerRow`, `StatusBadge`, `ReviewCard`, `CTAButtons`, `BottomTabBar`, `BottomSheet`

**Phase 1 Hardening Checklist:**

#### RTL Requirements
- [ ] Arabic language selected
- [ ] RTL layout applied (via React Native RTL support)
- [ ] Header aligns to right in RTL
- [ ] SpecRow labels align to right in RTL
- [ ] SpecRow values align to left in RTL
- [ ] Charger list flows correctly
- [ ] Review cards align correctly
- [ ] CTA buttons align correctly
- [ ] Bottom sheet expands from bottom in RTL

#### Accessibility
- [ ] Station header has proper heading hierarchy
- [ ] Charger rows keyboard accessible
- [ ] Status badges have non-color indicators
- [ ] Review cards have aria-labels
- [ ] Favorite button has aria-pressed state
- [ ] CTA buttons have accessible labels
- [ ] Bottom sheet has aria-label

#### Cross-Browser Compatibility
- [ ] iOS displays all components correctly
- [ ] Android displays all components correctly
- [ ] Bottom sheet animation works in RTL
- [ ] Review cards scroll correctly
- [ ] Pull-down to dismiss works in RTL

#### Mobile Testing
- [ ] Touch targets ≥ 44×44px
- [ ] Bottom sheet works reliably
- [ ] Pull-down to dismiss works reliably
- [ ] Favorite button toggles reliably
- [ ] CTA buttons work on touch
- [ ] Review list scrolls smoothly

**Class A Bug Definition:**
- [ ] SpecRow labels misaligned in RTL
- [ ] Charger list reversed incorrectly in RTL
- [ ] Bottom sheet expansion direction incorrect
- [ ] Review cards display incorrectly in RTL

---

### 10. Search Screen

**Purpose:** Full-screen search with filters
**Navigation Path:** Bottom tab "Search"
**Dependencies:** `SearchBar`, `FilterPills`, `StationCardList`, `EmptyState`, `BottomTabBar`

**Phase 1 Hardening Checklist:**

#### RTL Requirements
- [ ] Arabic language selected
- [ ] RTL layout applied (via React Native RTL support)
- [ ] Search bar large and aligned to right
- [ ] Filter pills align to right in RTL
- [ ] Results list starts from right in RTL
- [ ] Clear button aligns correctly
- [ ] Bottom tab bar aligns to right

#### Accessibility
- [ ] Search bar has ARIA label
- [ ] Filter pills have aria-labels
- [ ] Station cards have aria-labels
- [ ] Empty state has clear description
- [ ] Search input auto-focuses
- [ ] Clear button has aria-label

#### Cross-Browser Compatibility
- [ ] iOS search works correctly
- [ ] Android search works correctly
- [ ] Real-time filtering works in RTL
- [ ] Empty state displays correctly
- [ ] Filter pills scroll correctly

#### Mobile Testing
- [ ] Touch targets ≥ 44×44px
- [ ] Search input works reliably
- [ ] Filter pills scroll smoothly
- [ ] Results list scrolls smoothly
- [ ] Swipe-down to dismiss keyboard works

**Class A Bug Definition:**
- [ ] Search input reversed in RTL
- [ ] Filter pills misaligned
- [ ] Real-time filtering doesn't work in RTL
- [ ] Clear button misaligned

---

### 11. Favorites Screen

**Purpose:** View and manage saved stations
**Navigation Path:** Bottom tab "Favorites"
**Dependencies:** `StationCardList`, `FavoriteButton`, `EmptyState`, `SwipeActions`, `BottomTabBar`

**Phase 1 Hardening Checklist:**

#### RTL Requirements
- [ ] Arabic language selected
- [ ] RTL layout applied (via React Native RTL support)
- [ ] Favorites header aligns correctly
- [ ] Station card list starts from right in RTL
- [ ] Empty state aligns correctly
- [ ] Swipe actions visible correctly in RTL
- [ ] Favorite heart icon maintains direction

#### Accessibility
- [ ] Station cards have aria-labels
- [ ] Favorite button has aria-pressed state
- [ ] Swipe actions have aria-labels
- [ ] Empty state has clear description
- [ ] All cards keyboard accessible
- [ ] Sorting options accessible

#### Cross-Browser Compatibility
- [ ] iOS swipe actions work correctly
- [ ] Android swipe actions work correctly
- [ ] Sorting works in RTL
- [ ] Empty state displays correctly
- [ ] Favorites persist correctly

#### Mobile Testing
- [ ] Touch targets ≥ 44×44px
- [ ] Swipe actions work reliably
- [ ] Station cards scroll smoothly
- [ ] Sorting works on touch
- [ ] Empty state button accessible

**Class A Bug Definition:**
- [ ] Swipe actions reversed in RTL
- [ ] Station list reversed incorrectly
- [ ] Sorting doesn't work in RTL
- [ ] Swipe-to-remove doesn't work in RTL

---

### 12. Profile Screen

**Purpose:** User settings and account management
**Navigation Path:** Bottom tab "Profile"
**Dependencies:** `Avatar`, `NameInputs`, `EmailInput`, `PhoneInput`, `LanguageSelect`, `ThemeToggle`, `SaveButton`, `LogoutButton`, `VersionNumber`, `BottomTabBar`

**Phase 1 Hardening Checklist:**

#### RTL Requirements
- [ ] Arabic language selected
- [ ] RTL layout applied (via React Native RTL support)
- [ ] Profile header aligns to right in RTL
- [ ] Form labels align to right in RTL
- [ ] Form inputs align to left in RTL
- [ ] Language select aligns correctly
- [ ] Theme toggle aligns correctly
- [ ] Save button aligns correctly
- [ ] Logout button aligns correctly
- [ ] Bottom tab bar aligns to right
- [ ] Safe area insets respected

#### Accessibility
- [ ] All form inputs have labels
- [ ] Focus indicators visible
- [ ] Form validation messages visible
- [ ] Avatar upload accessible
- [ ] Language select accessible
- [ ] Theme toggle accessible
- [ ] Logout button has warning state
- [ ] Form validation works

#### Cross-Browser Compatibility
- [ ] iOS profile works correctly
- [ ] Android profile works correctly
- [ ] Form validation works in RTL
- [ ] Avatar upload works in both platforms
- [ ] Language select works in RTL

#### Mobile Testing
- [ ] Touch targets ≥ 44×44px
- [ ] Form inputs work reliably
- [ ] Avatar upload works on touch
- [ ] Language select works on touch
- [ ] Logout button provides confirmation
- [ ] Safe area handled correctly

**Class A Bug Definition:**
- [ ] Form labels misaligned in RTL
- [ ] Form inputs reversed in RTL
- [ ] Language select doesn't work in RTL
- [ ] Form validation breaks in RTL

---

### 13. Write Review Screen

**Purpose:** Create station review with star rating
**Navigation Path:** Station Detail → "Write Review"
**Dependencies:** `StationNameHeader`, `StarRating`, `Textarea`, `CharacterCounter`, `SubmitButton`, `CancelButton`

**Phase 1 Hardening Checklist:**

#### RTL Requirements
- [ ] Arabic language selected
- [ ] RTL layout applied (via React Native RTL support)
- [ ] Station name header aligns correctly
- [ ] Star rating aligns correctly
- [ ] Textarea aligns correctly
- [ ] Character counter aligns correctly
- [ ] Submit button aligns correctly
- [ ] Cancel button aligns correctly

#### Accessibility
- [ ] Star rating has aria-label
- [ ] Textarea has aria-label
- [ ] Character counter has aria-label
- [ ] Submit button has aria-label
- [ ] Cancel button has aria-label
- [ ] Form validation messages visible
- [ ] Star selection announced to screen readers

#### Cross-Browser Compatibility
- [ ] iOS review form works correctly
- [ ] Android review form works correctly
- [ ] Star rating works in RTL
- [ ] Character counter updates in RTL
- [ ] Form validation works in RTL

#### Mobile Testing
- [ ] Touch targets ≥ 44×44px
- [ ] Star rating works reliably on touch
- [ ] Textarea works reliably
- [ ] Character counter updates reliably
- [ ] Submit button provides feedback
- [ ] Cancel button dismisses form

**Class A Bug Definition:**
- [ ] Star rating misaligned in RTL
- [ ] Textarea reversed in RTL
- [ ] Character counter misaligned
- [ ] Form validation doesn't work in RTL

---

### 14. Login/Register Screen

**Purpose:** Authentication entry point
**Navigation Path:** Login/Register screens (separate)
**Dependencies:** `Logo`, `EmailInput`, `PasswordInput`, `SignInButton`, `SocialLoginButtons`, `CreateAccountLink`, `ForgotPasswordLink`

**Phase 1 Hardening Checklist:**

#### RTL Requirements
- [ ] Arabic language selected
- [ ] RTL layout applied (via React Native RTL support)
- [ ] Logo centering correct
- [ ] Email input aligns to right in RTL
- [ ] Password input aligns to right in RTL
- [ ] Social buttons align correctly
- [ ] "Create Account" link aligns correctly
- [ ] "Forgot Password" link aligns correctly
- [ ] Form labels align to right in RTL

#### Accessibility
- [ ] Form inputs have labels
- [ ] Focus indicators visible
- [ ] Submit button has aria-label
- [ ] Social buttons have aria-labels
- [ ] Create account link has aria-label
- [ ] Forgot password link has aria-label
- [ ] Form validation works
- [ ] Error messages visible

#### Cross-Browser Compatibility
- [ ] iOS login works correctly
- [ ] Android login works correctly
- [ ] Form validation works in RTL
- [ ] Social login buttons work in both platforms
- [ ] Password masking works correctly

#### Mobile Testing
- [ ] Touch targets ≥ 44×44px
- [ ] Form inputs work reliably
- [ ] Social login buttons work on touch
- [ ] Keyboard input works reliably
- [ ] Form validation provides feedback

**Class A Bug Definition:**
- [ ] Form inputs reversed in RTL
- [ ] Social buttons misaligned
- [ ] Form validation breaks in RTL

---

## Dashboard App Screens (12 Screens)

### 15. Overview Screen (Partner)

**Purpose:** Partner dashboard overview with KPIs and station list
**Navigation Path:** `/` (Partner role)
**Dependencies:** `AppShell`, `StatCardList`, `DataCard`

**Phase 1 Hardening Checklist:**

#### RTL Requirements
- [ ] Arabic language selected
- [ ] RTL layout applied (documentElement.dir = 'rtl')
- [ ] Role switcher works in Arabic
- [ ] Sidebar aligns to right in RTL
- [ ] Navigation items correct order
- [ ] Stat cards align correctly
- [ ] Data card aligns correctly
- [ ] Action buttons align correctly

#### Accessibility
- [ ] Stat cards have aria-labels
- [ ] Data card has aria-label
- [ ] All interactive elements focusable
- [ ] Stat cards announce values to screen readers
- [ ] Focus indicators visible
- [ ] Color contrast meets WCAG AA

#### Cross-Browser Compatibility
- [ ] Chrome Partner Overview renders correctly
- [ ] Firefox Partner Overview renders correctly
- [ ] Safari Partner Overview renders correctly
- [ ] Role switcher works in RTL
- [ ] Navigation works correctly

#### Mobile Testing
- [ ] Touch targets ≥ 44×44px
- [ ] Stat cards scroll smoothly
- [ ] Navigation items accessible
- [ ] Role switcher works on touch
- [ ] Responsive behavior works

**Class A Bug Definition:**
- [ ] Sidebar misaligned in RTL
- [ ] Stat cards reversed in RTL
- [ ] Navigation items incorrect order in RTL
- [ ] Role switcher doesn't work in RTL
- [ ] Content completely unreadable in RTL

---

### 16. My Stations Screen

**Purpose:** Partner station management list
**Navigation Path:** `/stations` (Partner role)
**Dependencies:** `AppShell`, `DataTable`

**Phase 1 Hardening Checklist:**

#### RTL Requirements
- [ ] Arabic language selected
- [ ] RTL layout applied (documentElement.dir = 'rtl')
- [ ] Sidebar aligns to right
- [ ] DataTable columns align correctly
- [ ] Row actions align correctly
- [ ] Action buttons align correctly

#### Accessibility
- [ ] Table has proper ARIA labels
- [ ] Sortable columns have aria-label
- [ ] Row actions have aria-labels
- [ ] Pagination has accessible labels
- [ ] Focus management works
- [ ] Color contrast meets WCAG AA

#### Cross-Browser Compatibility
- [ ] Chrome Partner Stations renders correctly
- [ ] Firefox Partner Stations renders correctly
- [ ] Safari Partner Stations renders correctly
- [ ] Sorting works in RTL
- [ ] Pagination works in RTL

#### Mobile Testing
- [ ] Touch targets ≥ 44×44px
- [ ] Table scrolls smoothly
- [ ] Action buttons accessible
- [ ] Sorting works on touch
- [ ] Pagination accessible

**Class A Bug Definition:**
- [ ] DataTable columns reversed incorrectly in RTL
- [ ] Row actions misaligned
- [ ] Action buttons reversed in RTL
- [ ] Sorting doesn't work in RTL

---

### 17. Station Edit Screen

**Purpose:** Edit station information form
**Navigation Path:** `/stations/:id/edit` (Partner role)
**Dependencies:** `AppShell`, `StationForm`

**Phase 1 Hardening Checklist:**

#### RTL Requirements
- [ ] Arabic language selected
- [ ] RTL layout applied (documentElement.dir = 'rtl')
- [ ] Form labels align to right
- [ ] Form inputs align to left
- [ ] Dropdowns align correctly
- [ ] Action buttons align correctly

#### Accessibility
- [ ] All form inputs have labels
- [ ] Form validation visible
- [ ] Focus indicators visible
- [ ] Submit button has aria-label
- [ ] Cancel button has aria-label
- [ ] Form validation messages visible

#### Cross-Browser Compatibility
- [ ] Chrome Station Edit renders correctly
- [ ] Firefox Station Edit renders correctly
- [ ] Safari Station Edit renders correctly
- [ ] Form validation works in RTL
- [ ] Dropdowns work in RTL

#### Mobile Testing
- [ ] Touch targets ≥ 44×44px
- [ ] Form inputs work reliably
- [ ] Dropdowns work on touch
- [ ] Form validation provides feedback
- [ ] Keyboard works reliably

**Class A Bug Definition:**
- [ ] Form labels misaligned in RTL
- [ ] Form inputs reversed in RTL
- [ ] Form validation breaks in RTL

---

### 18. Charger Management Screen

**Purpose:** View and manage chargers for stations
**Navigation Path:** `/chargers` (Partner role)
**Dependencies:** `AppShell`, `DataTable`

**Phase 1 Hardening Checklist:**

#### RTL Requirements
- [ ] Arabic language selected
- [ ] RTL layout applied (documentElement.dir = 'rtl')
- [ ] Sidebar aligns to right
- [ ] DataTable columns align correctly
- [ ] Row actions align correctly

#### Accessibility
- [ ] Table has ARIA labels
- [ ] Sortable columns have aria-label
- [ ] Row actions have aria-labels
- [ ] Focus indicators visible

#### Cross-Browser Compatibility
- [ ] Chrome Charger Management renders correctly
- [ ] Firefox Charger Management renders correctly
- [ ] Safari Charger Management renders correctly
- [ ] Sorting works in RTL

#### Mobile Testing
- [ ] Touch targets ≥ 44×44px
- [ ] Table scrolls smoothly
- [ ] Action buttons accessible

**Class A Bug Definition:**
- [ ] DataTable columns reversed incorrectly in RTL
- [ ] Row actions misaligned

---

### 19. Availability Update Screen

**Purpose:** Update charger availability status
**Navigation Path:** `/availability` (Partner role)
**Dependencies:** `AppShell`, `ChargerStatusTable`

**Phase 1 Hardening Checklist:**

#### RTL Requirements
- [ ] Arabic language selected
- [ ] RTL layout applied (documentElement.dir = 'rtl')
- [ ] Sidebar aligns to right
- [ ] Status toggles align correctly
- [ ] Action buttons align correctly

#### Accessibility
- [ ] Status toggles have aria-labels
- [ ] Toggle buttons have accessible labels
- [ ] Form inputs have labels
- [ ] Focus indicators visible

#### Cross-Browser Compatibility
- [ ] Chrome Availability Update renders correctly
- [ ] Firefox Availability Update renders correctly
- [ ] Safari Availability Update renders correctly
- [ ] Status toggles work in RTL

#### Mobile Testing
- [ ] Touch targets ≥ 44×44px
- [ ] Status toggles work reliably on touch
- [ ] Action buttons accessible

**Class A Bug Definition:**
- [ ] Status toggles misaligned in RTL
- [ ] Toggle buttons reversed in RTL

---

### 20. Reports Screen (Partner)

**Purpose:** Partner reports and analytics
**Navigation Path:** `/reports` (Partner role)
**Dependencies:** `AppShell`, `StatCardList`, `DataCard`

**Phase 1 Hardening Checklist:**

#### RTL Requirements
- [ ] Arabic language selected
- [ ] RTL layout applied (documentElement.dir = 'rtl')
- [ ] Sidebar aligns to right
- [ ] Stat cards align correctly
- [ ] Data card aligns correctly

#### Accessibility
- [ ] Stat cards have aria-labels
- [ ] Data card has aria-label
- [ ] Charts have aria-labels
- [ ] Focus indicators visible

#### Cross-Browser Compatibility
- [ ] Chrome Partner Reports renders correctly
- [ ] Firefox Partner Reports renders correctly
- [ ] Safari Partner Reports renders correctly
- [ ] Charts display correctly in RTL

#### Mobile Testing
- [ ] Touch targets ≥ 44×44px
- [ ] Stat cards scroll smoothly

**Class A Bug Definition:**
- [ ] Stat cards reversed in RTL
- [ ] Charts display incorrectly in RTL

---

### 21. Overview Screen (Admin)

**Purpose:** Admin dashboard overview with platform metrics
**Navigation Path:** `/` (Admin role)
**Dependencies:** `AppShell`, `StatCardList`, `DataTable`, `DataCard`

**Phase 1 Hardening Checklist:**

#### RTL Requirements
- [ ] Arabic language selected
- [ ] RTL layout applied (documentElement.dir = 'rtl')
- [ ] Role switcher works in Arabic
- [ ] Sidebar aligns to right
- [ ] Stat cards align correctly
- [ ] Data cards align correctly
- [ ] Action buttons align correctly

#### Accessibility
- [ ] Stat cards have aria-labels
- [ ] Data cards have aria-labels
- [ ] Focus indicators visible
- [ ] Color contrast meets WCAG AA

#### Cross-Browser Compatibility
- [ ] Chrome Admin Overview renders correctly
- [ ] Firefox Admin Overview renders correctly
- [ ] Safari Admin Overview renders correctly
- [ ] Role switcher works in RTL

#### Mobile Testing
- [ ] Touch targets ≥ 44×44px
- [ ] Stat cards scroll smoothly
- [ ] Navigation accessible

**Class A Bug Definition:**
- [ ] Sidebar misaligned in RTL
- [ ] Content completely unreadable in RTL

---

### 22. Users Screen

**Purpose:** Admin user management table
**Navigation Path:** `/users` (Admin role)
**Dependencies:** `AppShell`, `DataTable`

**Phase 1 Hardening Checklist:**

#### RTL Requirements
- [ ] Arabic language selected
- [ ] RTL layout applied (documentElement.dir = 'rtl')
- [ ] Sidebar aligns to right
- [ ] DataTable columns align correctly
- [ ] Row actions align correctly

#### Accessibility
- [ ] Table has ARIA labels
- [ ] Sortable columns have aria-label
- [ ] Row actions have aria-labels
- [ ] Focus indicators visible

#### Cross-Browser Compatibility
- [ ] Chrome Users renders correctly
- [ ] Firefox Users renders correctly
- [ ] Safari Users renders correctly
- [ ] Sorting works in RTL

#### Mobile Testing
- [ ] Touch targets ≥ 44×44px
- [ ] Table scrolls smoothly
- [ ] Action buttons accessible

**Class A Bug Definition:**
- [ ] DataTable columns reversed incorrectly in RTL
- [ ] Row actions misaligned

---

### 23. Partners Screen

**Purpose:** Admin partner management table
**Navigation Path:** `/partners` (Admin role)
**Dependencies:** `AppShell`, `DataTable`

**Phase 1 Hardening Checklist:**

#### RTL Requirements
- [ ] Arabic language selected
- [ ] RTL layout applied (documentElement.dir = 'rtl')
- [ ] Sidebar aligns to right
- [ ] DataTable columns align correctly
- [ ] Row actions align correctly

#### Accessibility
- [ ] Table has ARIA labels
- [ ] Sortable columns have aria-label
- [ ] Row actions have aria-labels
- [ ] Focus indicators visible

#### Cross-Browser Compatibility
- [ ] Chrome Partners renders correctly
- [ ] Firefox Partners renders correctly
- [ ] Safari Partners renders correctly
- [ ] Sorting works in RTL

#### Mobile Testing
- [ ] Touch targets ≥ 44×44px
- [ ] Table scrolls smoothly
- [ ] Action buttons accessible

**Class A Bug Definition:**
- [ ] DataTable columns reversed incorrectly in RTL
- [ ] Row actions misaligned

---

### 24. Stations Screen

**Purpose:** Admin station management table
**Navigation Path:** `/admin/stations` (Admin role)
**Dependencies:** `AppShell`, `DataTable`

**Phase 1 Hardening Checklist:**

#### RTL Requirements
- [ ] Arabic language selected
- [ ] RTL layout applied (documentElement.dir = 'rtl')
- [ ] Sidebar aligns to right
- [ ] DataTable columns align correctly
- [ ] Row actions align correctly

#### Accessibility
- [ ] Table has ARIA labels
- [ ] Sortable columns have aria-label
- [ ] Row actions have aria-labels
- [ ] Focus indicators visible

#### Cross-Browser Compatibility
- [ ] Chrome Admin Stations renders correctly
- [ ] Firefox Admin Stations renders correctly
- [ ] Safari Admin Stations renders correctly
- [ ] Sorting works in RTL

#### Mobile Testing
- [ ] Touch targets ≥ 44×44px
- [ ] Table scrolls smoothly
- [ ] Action buttons accessible

**Class A Bug Definition:**
- [ ] DataTable columns reversed incorrectly in RTL
- [ ] Row actions misaligned

---

### 25. Reviews Screen

**Purpose:** Admin review moderation table
**Navigation Path:** `/admin/reviews` (Admin role)
**Dependencies:** `AppShell`, `DataTable`

**Phase 1 Hardening Checklist:**

#### RTL Requirements
- [ ] Arabic language selected
- [ ] RTL layout applied (documentElement.dir = 'rtl')
- [ ] Sidebar aligns to right
- [ ] DataTable columns align correctly
- [ ] Row actions align correctly

#### Accessibility
- [ ] Table has ARIA labels
- [ ] Sortable columns have aria-label
- [ ] Row actions have aria-labels
- [ ] Status badges have non-color indicators
- [ ] Focus indicators visible

#### Cross-Browser Compatibility
- [ ] Chrome Admin Reviews renders correctly
- [ ] Firefox Admin Reviews renders correctly
- [ ] Safari Admin Reviews renders correctly
- [ ] Sorting works in RTL
- [ ] Moderation actions work in RTL

#### Mobile Testing
- [ ] Touch targets ≥ 44×44px
- [ ] Table scrolls smoothly
- [ ] Moderation actions accessible

**Class A Bug Definition:**
- [ ] DataTable columns reversed incorrectly in RTL
- [ ] Moderation actions misaligned
- [ ] Content completely unreadable in RTL

---

## Common Hardening Requirements

### Class A Bug Definition (Critical Before Phase 2)

**Class A bugs are blocked until Phase 2:**

- [ ] Bugs that completely prevent users from completing tasks (cannot browse stations, cannot login, cannot update availability)
- [ ] Bugs that make content completely unreadable in Arabic RTL layout (text overlapping, reversed content, reversed RTL direction)
- [ ] Other critical issues: crashes, data loss, security vulnerabilities, WCAG 2.1 AA violations preventing basic accessibility

**Class A vs Other Bugs:**
- **Class A** (block Phase 2): Complete functional failure or complete RTL unreadability
- **Class B** (accept in Phase 2): Minor UX issues, visual glitches, edge cases
- **Class C** (accept in Phase 2): Documentation gaps, typos, improved performance opportunities

### Bug Handling Process

1. [ ] Perform all audits (RTL, accessibility, cross-browser, mobile) on all 25 screens
2. [ ] Document all bugs in tracking system with severity classification
3. [ ] Fix all Class A bugs immediately
4. [ ] Plan Class B and C bugs for Phase 2 backlog
5. [ ] Verify Class A fixes are complete before Phase 2 begins

---

**Document Version:** 2.0
**Last Updated:** 2026-06-06
**Status:** Complete with Phase 1 Hardening Requirements
