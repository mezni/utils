<!-- SPECKIT START -->
For additional context about technologies to be used, project structure,
shell commands, and other important information, read the current plan
at specs/005-phase1-hardening/plan.md
<!-- SPECKIT END -->

## Active Feature: Sprint 1.5 Phase 1 Hardening

**Plan**: [plan.md](specs/005-phase1-hardening/plan.md)

**Spec**: [spec.md](specs/005-phase1-hardening/spec.md)

**Status**: Planning complete — ready for implementation tasks

---

### Key Deliverables

1. **Cross-App Consistency Verification**
    - StatusBadge consistency across all 3 apps (6 web, 12 mobile, 6 dashboard components)
    - StationCard consistency between web and mobile variants
    - Color token verification across all applications
    - Document any inconsistencies found

2. **RTL Audit** (25 screens total)
    - Driver Web: 6 screens (Home/Map, Station Detail, Search, Favorites, Profile, Login/Register)
    - Driver Mobile: 7 screens (Map/Home, Station List, Station Detail, Search, Favorites, Profile, Login/Register)
    - Dashboard: 12 screens (6 Partner + 7 Admin screens)
    - Zero Class A RTL bugs before Phase 2

3. **Accessibility Audit** (Driver Web + Dashboard)
    - Keyboard navigation verification
    - Focus indicators visible and accessible
    - Color contrast meets WCAG 2.1 AA
    - All interactive elements accessible

4. **Cross-Browser Testing** (Driver Web + Dashboard)
    - Chrome (latest stable)
    - Firefox (latest stable)
    - Safari (latest stable)
    - No Class A cross-browser bugs

5. **Mobile Testing** (Driver Mobile)
    - iOS Simulator (latest stable)
    - Android Simulator (latest stable)
    - Screen size variations
    - Touch targets ≥ 44x44 points

6. **Documentation Updates**
    - docs/ui/screens.md (all 25 screens documented)
    - docs/ui/components.md (all 27 components documented)
    - docs/ui/design-tokens.md (all tokens documented)
    - docs/guides/onboarding.md (setup instructions created)

---

### Technical Approach

- **Equal hardening effort** across all 25 screens (6 Driver Web + 7 Driver Mobile + 12 Dashboard)
- **RTL verification** using manual checklist + browser dev tools
- **Accessibility testing** using Lighthouse + axe DevTools + manual verification
- **Cross-browser testing** with latest stable versions of Chrome, Firefox, Safari
- **Mobile testing** with latest iOS 18+ and Android 15+ simulators
- **Bug tracking** - Document all bugs, fix only Class A bugs
- **Documentation updates** to reflect reality

---

### Hardening Scope

**All 25 screens must be hardened equally**:

- Driver Web (6 screens): Overview, Station List, Station Detail, Charging History, Profile, Settings
- Driver Mobile (7 screens): Overview, Station List, Station Detail, Charging History, Add Review, Profile, Settings
- Dashboard (12 screens): Partner Overview, My Stations, Station Edit, Charger Management, Availability Update, Partner Reports, Admin Overview, Users, Partners, Stations, Chargers, Reviews

**Hardening Coverage Target**:
- [ ] Each screen passes RTL audit in Arabic
- [ ] Each screen passes accessibility audit
- [ ] Each screen passes cross-browser testing (Chrome, Firefox, Safari)
- [ ] Each screen passes mobile testing (iOS Simulator, Android Simulator)
- [ ] Each screen documented in relevant sections of project docs

---

### Project Structure

```
apps/dashboard/
├── src/
│   ├── components/
│   │   ├── AppShell/
│   │   │   ├── AppShell.tsx
│   │   │   ├── Sidebar/
│   │   │   │   ├── Sidebar.tsx
│   │   │   │   ├── BrandHeader.tsx
│   │   │   │   ├── NavigationItem.tsx
│   │   │   │   └── BottomActions.tsx
│   │   │   └── TopBar.tsx
│   │   ├── PageContent/
│   │   │   └── PageContent.tsx
│   │   ├── DataCard/
│   │   │   └── DataCard.tsx
│   │   ├── DataTable/
│   │   │   ├── DataTable.tsx
│   │   │   └── table.types.ts
│   │   └── StatCard/
│   │   │   └── StatCard.tsx
│   ├── screens/
│   │   ├── OverviewScreen.tsx
│   │   ├── MyStationsScreen.tsx
│   │   ├── StationEditScreen.tsx
│   │   ├── ChargerManagementScreen.tsx
│   │   ├── AvailabilityUpdateScreen.tsx
│   │   ├── ReportsScreen.tsx
│   │   ├── UsersScreen.tsx
│   │   ├── PartnersScreen.tsx
│   │   ├── StationsScreen.tsx
│   │   ├── ChargersScreen.tsx
│   │   └── ReviewsScreen.tsx
│   ├── mocks/
│   │   ├── partners.ts
│   │   ├── stations.ts
│   │   ├── chargers.ts
│   │   ├── users.ts
│   │   ├── reviews.ts
│   │   └── reports.ts
│   ├── i18n/
│   │   ├── ar.json
│   │   ├── fr.json
│   │   └── index.ts
│   ├── hooks/
│   │   ├── useRole.ts
│   │   ├── useMockData.ts
│   │   └── useNavigation.ts
│   ├── context/
│   │   └── RoleContext.tsx
│   ├── types/
│   │   └── index.ts
│   ├── App.tsx
│   └── index.css
├── package.json
├── tsconfig.json
├── vite.config.ts
└── tailwind.config.js
```

---

### Success Criteria

- ✅ All 13 screens render with realistic mock data in browser
- ✅ Navigation between all screens works via sidebar (Partner: 6 screens, Admin: 7 screens)
- ✅ Arabic RTL layout is correct on every screen (sidebar aligns right, tables formatted correctly)
- ✅ French layout renders correctly with translated strings on all screens
- ✅ Role switching (Partner ↔ Admin) completes within 1 second and updates UI correctly
- ✅ No backend calls made (verified via network inspector)
- ✅ All 6 dashboard components render with required props and all visual states
- ✅ `pnpm build` passes for `apps/dashboard` with zero warnings
- ✅ All visual values consumed from `packages/ui` design tokens (zero hardcoded values)

---

### Implementation Plan Artifacts

**Research**: [research.md](specs/005-phase1-hardening/research.md)
- Resolved all unknowns
- Defined testing methodologies
- Established bug classification criteria

**Data Model**: [data-model.md](specs/005-phase1-hardening/data-model.md)
- Documented existing data structures
- Verified design token consistency requirements
- No data model changes in this sprint

**Quick Start Guide**: [quickstart.md](specs/005-phase1-hardening/quickstart.md)
- How to run all three applications
- Testing checklist for RTL, accessibility, cross-browser, mobile
- Bug tracking and documentation procedures

### Clarifications Made (Session 2026-06-06)

1. **Accessibility Testing Method**: Automated tools for first pass, manual verification for high-risk areas
2. **Mobile Device Testing**: Latest stable versions (iOS 18+ and Android 15+), default settings, no feature flags
3. **Class A Bug Definition**: Bugs that completely block functionality OR make content unreadable in Arabic RTL layout
4. **Hardening Scope**: Equal effort across all 25 screens (6 Driver Web + 7 Driver Mobile + 12 Dashboard)
5. **Bug Handling Strategy**: Document all bugs, fix only Class A bugs, plan Class B/C for Phase 2 backlog

### Class A Bug Classification

**Class A (Must fix before Phase 2)**:
- Bugs that completely prevent users from completing tasks
- Bugs that make content completely unreadable in Arabic RTL layout
- Critical issues: crashes, data loss, security vulnerabilities, WCAG 2.1 AA violations preventing basic accessibility

**Class B (Acceptable to defer to Phase 2)**:
- Minor UX issues that don't block functionality
- Visual glitches or minor inconsistencies
- Inconsistent animations or transitions
- Edge cases not covered in primary flows

**Class C (Acceptable to defer to Phase 2)**:
- Documentation gaps or typos
- Missing tooltips or help text
- Performance improvements possible but not critical
- Polish and refinement opportunities

### Project Structure

```
apps/driver-web/
├── src/
│   ├── components/
│   │   └── [existing components]
│   ├── screens/
│   │   ├── HomeScreen.tsx
│   │   ├── StationDetailScreen.tsx
│   │   └── [existing screens]
│   └── [existing files]
├── package.json
├── tsconfig.json
└── vite.config.ts

apps/driver-mobile/
├── src/
│   ├── components/
│   │   └── [existing components]
│   ├── screens/
│   │   ├── HomeScreen.tsx
│   │   ├── StationDetailScreen.tsx
│   │   └── [existing screens]
│   └── [existing files]
├── package.json
└── [existing files]

apps/dashboard/
├── src/
│   ├── components/
│   │   └── [existing components]
│   ├── screens/
│   │   ├── OverviewScreen.tsx
│   │   ├── StationEditScreen.tsx
│   │   └── [existing screens]
│   └── [existing files]
├── package.json
└── [existing files]

packages/ui/
├── src/
│   ├── components/
│   │   ├── StatusBadge/
│   │   │   ├── StatusBadge.tsx (web)
│   │   │   └── StatusBadge.native.tsx (mobile)
│   │   └── [existing components]
│   └── tokens/ [existing tokens]
└── [existing files]
```
