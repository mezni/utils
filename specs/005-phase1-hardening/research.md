# Sprint 1.5 — Phase 1 Hardening - Research

**Date**: 2026-06-06
**Feature**: Sprint 1.5 Phase 1 Hardening
**Branch**: `005-phase1-hardening`

---

## Unknowns & Clarifications

### 1. RTL Testing Methodology for All 25 Screens

**Question**: What specific testing methodology should be used to verify RTL correctness across 25 screens in 3 applications?

**Decision**: Use manual verification with systematic checklist per screen, supported by browser dev tools for layout inspection.

**Rationale**:
- Manual verification is necessary because RTL bugs can be subtle and context-dependent
- Systematic checklist ensures all screens are tested consistently
- Browser dev tools (Layout tab, Accessibility inspector) provide visual feedback
- Lighthouse accessibility tests include RTL checks but can't verify all 25 screens comprehensively

**Alternatives Considered**:
- ❌ Automated RTL testing tools (not available for React Native)
- ❌ Visual regression testing (overkill for hardening sprint)
- ❌ Manual testing without checklist (inconsistent coverage)

**Best Practices**:
- Set `documentElement.dir = 'rtl'` before loading each screen
- Test all navigation paths to ensure context switching works
- Check both layout direction and text flow
- Verify icons maintain correct direction
- Test padding/margins on all sides of elements

---

### 2. Accessibility Testing Tools & Methodology

**Question**: Which accessibility testing tools should be used and what methodology ensures comprehensive coverage?

**Decision**: Use automated tools (Lighthouse, axe DevTools) for first pass, manual verification for high-risk areas (keyboard navigation, focus management, color contrast).

**Rationale**:
- Automated tools catch most common accessibility violations
- Manual verification catches edge cases and complex issues
- Lighthouse provides scores and specific violations
- axe DevTools offers detailed violation reports
- Manual testing ensures correct user experience

**Alternatives Considered**:
- ❌ Only automated tools (misses complex user flow issues)
- ❌ Only manual testing (slow, inconsistent coverage)
- ❌ Screen reader testing only (misses visual accessibility issues)

**Best Practices**:
- Run Lighthouse accessibility audit on all web screens
- Use axe DevTools browser extension for detailed violation reports
- Manually test keyboard navigation on each interactive element
- Use Eye Dropper for color contrast verification
- Test with large font accessibility settings enabled
- Check focus indicators on all interactive elements

---

### 3. Mobile Device Testing Scope

**Question**: Which specific mobile device configurations should be tested for Driver Mobile app?

**Decision**: Test with latest stable versions (iOS 18+ and Android 15+), default settings, no feature flags enabled.

**Rationale**:
- Latest versions represent current development standards
- Default settings match typical user behavior
- No feature flags ensures consistent testing
- Testing multiple versions would exceed sprint scope

**Alternatives Considered**:
- ❌ Multiple iOS versions (16, 17, 18) - would double scope
- ❌ Real devices (iPhone 14+, Samsung Galaxy S24+) - not available
- ❌ Testing with accessibility features enabled - beyond Phase 1 scope

**Best Practices**:
- Use iOS Simulator and Android Simulator with latest versions
- Test with different screen orientations
- Test with standard iOS and Android navigation gestures
- Verify safe area insets are respected
- Test with standard font sizes, then large font accessibility setting

---

### 4. Class A Bug Classification Criteria

**Question**: What specific criteria should define a Class A bug that blocks Phase 2?

**Decision**: Bugs that completely block functionality OR make content completely unreadable in Arabic RTL layout.

**Rationale**:
- Functional blocking bugs prevent users from completing tasks
- RTL unreadability bugs violate non-negotiable constitution principles
- These bugs must be fixed before Phase 2 begins
- Class B/C bugs can be deferred to Phase 2

**Alternatives Considered**:
- ❌ All bugs blocking functionality (too broad - includes minor UX issues)
- ❌ All accessibility violations (would be too strict - some AA violations are acceptable)
- ❌ Only crashes and data loss (misses RTL and accessibility issues)

**Best Practices**:
- Class A: Complete functional failure, RTL unreadability, critical accessibility violations
- Class B: Minor UX issues, visual glitches, non-critical accessibility issues
- Class C: Documentation gaps, typos, performance improvements

---

### 5. Equal Hardening vs Prioritization

**Question**: Should we focus on certain screens/applications first or ensure all 25 screens are hardened equally?

**Decision**: Equal effort across all 25 screens (6 Driver Web + 7 Driver Mobile + 12 Dashboard) to ensure each app is equally solid.

**Rationale**:
- All three apps are critical and used by different user segments
- Equal effort ensures consistency and fairness
- Each app has unique requirements (web vs mobile vs dashboard)
- Time is limited in hardening sprint

**Alternatives Considered**:
- ❌ Prioritize Driver Web + Dashboard first - would leave Mobile untested
- ❌ Prioritize high-traffic screens - need traffic data to determine
- ❌ Prioritize screens with most failures - circular reasoning

**Best Practices**:
- Create systematic checklist for each of the 25 screens
- Apply same hardening criteria to all screens
- Track progress per screen to ensure equal coverage
- Document any deviations and their impact

---

### 6. Bug Handling Strategy for Non-Class A Bugs

**Question**: What should be done with Class B and Class C bugs discovered during hardening?

**Decision**: Document all bugs in tracking system, fix only Class A bugs, plan Class B/C for Phase 2 backlog.

**Rationale**:
- Fixing all bugs would exceed sprint scope
- Class A bugs are blocking issues that must be resolved
- Class B/C bugs are quality improvements that can be deferred
- Documentation ensures transparency and prevents loss of findings

**Alternatives Considered**:
- ❌ Fix all Class B bugs immediately - would double scope
- ❌ Ignore all non-Class A bugs - would lose important information
- ❌ Fix all bugs regardless of severity - would miss Phase 2 timeline

**Best Practices**:
- Document all bugs with severity classification
- Fix Class A bugs immediately
- Plan Class B/C bugs for Phase 2 backlog
- Create bug tracking document with all findings
- Review bug list with stakeholders before Phase 2 begins

---

## Technical Context

### Project Dependencies

**Frontend Frameworks**:
- Driver Web: React 19 + TypeScript 5.7 + Vite 6
- Driver Mobile: React Native 0.74 + Expo SDK 51
- Dashboard: React 19 + TypeScript 5.7 + Vite 6

**UI Package**:
- `@borne-map/ui`: Shared design system with tokens and components
  - StatusBadge (web & mobile variants)
  - StationCard (web & mobile variants)
  - Color tokens, typography tokens, spacing tokens
  - Radius tokens, shadow tokens

**Routing**:
- Driver Web: React Router v7
- Driver Mobile: React Navigation (native routing)
- Dashboard: React Router v7

**Localization**:
- react-i18next for multi-language support
- Arabic (RTL), French, English
- Language switching without page reload

**Styling**:
- Driver Web: Tailwind CSS (v3.4)
- Driver Mobile: StyleSheet (React Native)
- Dashboard: Tailwind CSS (v3.4)

**Testing**:
- Driver Web: Vitest + React Testing Library
- Driver Mobile: Jest + React Native Testing Library
- Dashboard: Vitest + React Testing Library

---

### Architecture Patterns

**Component Architecture**:
- Shared components in `packages/ui`
- App-specific components in each app's `src/components/`
- Composition over inheritance
- Props-driven components
- TypeScript for type safety

**State Management**:
- React Context for role-based navigation (Dashboard)
- React hooks for local state
- Mock data providers (no backend in Phase 1)
- Context API for global theme/language settings

**Design System Pattern**:
- Single source of truth: `packages/ui/src/tokens/`
- All visual values consumed from tokens
- No hardcoded colors or spacing
- TypeScript definitions for token types

**RTL Architecture**:
- CSS logical properties for margin/padding (margin-inline-start, etc.)
- Document element direction switching for Arabic
- Flexbox/Grid for RTL-aware layouts
- CSS logical properties in Tailwind (ms, me instead of ml, mr)
- React Native RTL support for text and flex layouts

**Accessibility Pattern**:
- WCAG 2.1 AA compliance for web apps
- Keyboard navigation focus management
- Focus indicators using design tokens
- Semantic HTML elements
- ARIA labels for interactive elements

---

### Development Tools

**Package Manager**:
- pnpm for dependency management
- Lockfile versioning: latest
- Supply chain policy: `--no-frozen-lockfile` for installation

**Build Tools**:
- Vite 6 for Driver Web and Dashboard
- Expo CLI for Driver Mobile
- TypeScript compiler for type checking

**Linting**:
- ESLint with TypeScript rules
- Prettier for formatting
- No lint errors allowed

**Testing**:
- Vitest for web apps
- Jest for React Native
- React Testing Library
- Coverage reporting required

**Version Control**:
- Git with feature branches
- Conventional commit messages
- PR-based workflow

---

### Testing Strategy

**Unit Testing**:
- All components must have tests
- Test coverage: ≥ 80% for Phase 1
- Focus on critical paths

**Integration Testing**:
- Screen navigation tests
- Component composition tests
- State management tests

**E2E Testing**:
- Not required for Phase 1 hardening
- Focus on manual verification instead

**Accessibility Testing**:
- Lighthouse accessibility audit (web apps)
- axe DevTools for detailed violation reports
- Manual keyboard navigation testing
- Color contrast verification

**Cross-Browser Testing**:
- Chrome (latest stable)
- Firefox (latest stable)
- Safari (latest stable)

**Mobile Testing**:
- iOS Simulator (latest stable)
- Android Simulator (latest stable)
- Screen size variations
- Orientation variations

---

## Best Practices Researched

### 1. RTL Layout Best Practices

**CSS Logical Properties**:
```css
/* Good - RTL aware */
margin-inline-start: 1rem;
padding-inline: 0.5rem;

/* Bad - RTL unaware */
margin-left: 1rem;  /* Will break in RTL */
```

**Tailwind RTL Classes**:
- `ms-*` instead of `ml-*` (margin-start)
- `me-*` instead of `mr-*` (margin-end)
- `ps-*` instead of `pl-*` (padding-start)
- `pe-*` instead of `pr-*` (padding-end)
- `ps-2` (padding-start: 0.5rem)

**Flexbox RTL**:
```css
/* Ensure flex direction is correct */
display: flex;
flex-direction: row;  /* Default - works in both LTR and RTL */
justify-content: space-between;  /* Correctly spaces items */
```

**Icons in RTL**:
- Use SVG with `start` and `end` attributes
- Don't rely on `left` and `right` positioning
- Use transform or absolute positioning with logical properties

---

### 2. Accessibility Testing Checklist

**Keyboard Navigation**:
- [ ] Tab key navigates all interactive elements
- [ ] Focus order is logical (left-to-right, top-to-bottom)
- [ ] Escape key closes modals/dialogs
- [ ] Enter/Space triggers buttons and links
- [ ] Focus doesn't get trapped

**Focus Indicators**:
- [ ] Visible focus ring on all interactive elements
- [ ] Focus ring uses brand.primary (#007943)
- [ ] Focus ring has 2px outline with outlineOffset: 2px
- [ ] Focus ring contrast meets WCAG AA (3:1 minimum)
- [ ] Focus ring doesn't obscure content

**Color Contrast**:
- [ ] Text-to-background contrast ≥ 4.5:1 (AA standard)
- [ ] Large text (18pt+) contrast ≥ 3:1 (AA standard)
- [ ] Focus indicators have adequate contrast
- [ ] Disabled states have adequate contrast
- [ ] Use Eye Dropper for verification

**Semantic HTML**:
- [ ] Use semantic elements (nav, main, section, article)
- [ ] ARIA labels for interactive elements
- [ ] Alt text for images
- [ ] Correct heading hierarchy
- [ ] Skip links for keyboard navigation

---

### 3. Cross-Browser Testing

**Chrome**:
- Test latest stable version
- Check console for errors
- Verify all features work
- Check layout consistency
- Test with developer tools enabled

**Firefox**:
- Test latest stable version
- Check console for errors
- Verify all features work
- Check layout consistency
- Test with developer tools enabled

**Safari**:
- Test latest stable version
- Check console for errors
- Verify all features work
- Check layout consistency
- Test with developer tools enabled
- Test iOS Safari specifically

**Common Issues**:
- Flexbox layout differences
- CSS Grid differences
- Font rendering differences
- JavaScript behavior differences
- Layout shifts (CLS)

---

### 4. Mobile Testing Best Practices

**iOS Simulator**:
- Use Xcode Simulator
- Test on iPhone 14 and iPhone Pro Max
- Test both orientations
- Verify safe area insets
- Test with standard and large font settings
- Test with standard and reduced motion settings

**Android Simulator**:
- Use Android Studio Emulator
- Test on Pixel 6 and Samsung Galaxy S24
- Test both orientations
- Verify safe area insets
- Test with standard and large font settings
- Test gesture navigation

**Key Testing Areas**:
- Touch targets ≥ 44x44 points
- Form inputs work correctly
- Pull-to-refresh works
- Keyboard input works
- Navigation bar layout correct
- Safe area insets respected
- No layout breaks on resize

---

### 5. Bug Documentation Best Practices

**Bug Title**:
- Clear and concise
- Describes the issue
- Includes app and screen

**Severity Classification**:
- Class A: Blocking, RTL unreadability, critical accessibility
- Class B: Degrades quality, non-blocking
- Class C: Cosmetic, nice-to-have

**Reproduction Steps**:
- Step-by-step instructions
- Clear and concise
- Easy to follow
- Include any prerequisites

**Expected vs Actual Behavior**:
- Clear description of what should happen
- Clear description of what actually happens
- Include screenshots if possible
- Note any error messages

**Severity Reason**:
- Explain why bug is classified as X
- Reference success criteria
- Impact on user experience

**Status & Fix**:
- Current status (Open, Fixed, Deferred)
- Fix description if available
- Deadline for fix (for Class A bugs)

---

## Conclusion

All unknowns have been resolved and best practices researched. The hardening sprint can proceed with:

1. Systematic RTL verification using checklist per screen
2. Automated accessibility testing with manual verification for high-risk areas
3. Latest stable mobile versions with default settings
4. Clear Class A bug classification (blocking + RTL unreadability)
5. Equal effort across all 25 screens
6. Document-all-bugs, fix- only-Class-A approach
7. Bug documentation following best practices

This research provides the foundation for a comprehensive hardening sprint that ensures all three applications are production-ready before Phase 2 begins.
