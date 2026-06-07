# Sprint 1.5 Bugs

## Sprint 1.5 Bugs - Cross-App Consistency Issues

### Bug 1: StatusBadge Using Hardcoded Colors Instead of Design Tokens

**Severity**: Class A (Blocks visual consistency principle)
**App**: All Apps (Driver Web, Driver Mobile, Dashboard)
**Screen**: StatusBadge component usage across all apps
**Bug Type**: Design System Violation

**Description**:
StatusBadge web component in `packages/ui/src/components/StatusBadge/StatusBadge.tsx` and native component in `packages/ui/src/components/StatusBadge/StatusBadge.native.tsx` are using hardcoded colors instead of design tokens for background colors, text colors, and dot colors.

**Current Implementation**:
```typescript
// dotColors correctly uses tokens
const dotColors: Record<StatusBadgeVariant, string> = {
  available: success,      // ✅ Uses token
  'in-use': warning,       // ✅ Uses token
  maintenance: errorColor, // ✅ Uses token
  offline: neutral400,     // ✅ Uses token
}

// bgColors uses hardcoded values ❌
const bgColors: Record<StatusBadgeVariant, string> = {
  available: '#d1fae5',    // Should use token
  'in-use': '#fef3c7',     // Should use token
  maintenance: '#fee2e2',  // Should use token
  offline: '#f1f5f9',      // Should use token
}

// textColors uses hardcoded values ❌
const textColors: Record<StatusBadgeVariant, string> = {
  available: '#065f46',    // Should use token
  'in-use': '#92400e',     // Should use token
  maintenance: '#991b1b',  // Should use token
  offline: '#475569',      // Should use token
}
```

**Native Component Additional Issue**:
```typescript
// StyleSheet also has hardcoded backgroundColor ❌
const styles = StyleSheet.create({
  container: {
    // ...
    backgroundColor: '#d1fae5',  // Should use bgColors[variant]
    borderRadius: 6,            // Should use radiusSm token
    gap: 4,                     // Should use spacing1 token
    paddingHorizontal: 8,        // Should use spacing2 token
    paddingVertical: 4,         // Should use spacing1 token
  },
})
```

**Token File Values** (`packages/ui/src/tokens/colors.ts`):
```typescript
export const success = '#10b981'
export const warning = '#f59e0b'
export const error = '#ef4444'
export const neutral400 = '#94a3b8'

// Added for StatusBadge consistency
export const bgSuccess = '#d1fae5'
export const bgWarning = '#fef3c7'
export const bgError = '#fee2e2'
export const bgNeutral400 = '#f1f5f9'

export const textSuccess = '#065f46'
export const textWarning = '#92400e'
export const textError = '#991b1b'
export const textNeutral400 = '#475569'
```

**Steps to Reproduce**:
1. Run any app using StatusBadge
2. Observe the background and text colors
3. Compare with token values in `packages/ui/src/tokens/colors.ts`

**Expected Behavior**:
- All StatusBadge colors should be derived from design tokens
- Use `bgSuccess`, `bgWarning`, `bgError`, `bgNeutral400` tokens for background colors
- Use `textSuccess`, `textWarning`, `textError`, `textNeutral400` tokens for text colors
- Native component StyleSheet should use tokens for spacing, radius, and dynamic background colors

**Actual Behavior**:
- Background colors are hardcoded hex values
- Text colors are hardcoded hex values
- Dot colors correctly use tokens
- Native component StyleSheet has hardcoded values
- Inconsistency between web and native implementations expected

**Severity Reason**:
- Violates core constitution principle: "All visual values from `packages/ui` design tokens"
- Makes it impossible to maintain consistent styling across apps
- Would require changing all component files that use StatusBadge to update hardcoded values
- Creates maintenance burden and reduces design system effectiveness
- Native component also missing StyleSheet import

**Status**: ✅ FIXED
**Fix Applied**:
- Added missing color tokens to `packages/ui/src/tokens/colors.ts` (bgSuccess, bgWarning, bgError, bgNeutral400, textSuccess, textWarning, textError, textNeutral400)
- Updated StatusBadge.tsx to import and use new tokens
- Updated StatusBadge.native.tsx to import StyleSheet and use new tokens
- Fixed native component StyleSheet to use tokens for spacing, radius, and dynamic background colors
- Changed `aria-hidden` to `accessibilityElementsHidden` for React Native compatibility

**Affected Files**:
- `packages/ui/src/components/StatusBadge/StatusBadge.tsx` ✅ Fixed
- `packages/ui/src/components/StatusBadge/StatusBadge.native.tsx` ✅ Fixed
- `packages/ui/src/tokens/colors.ts` ✅ Updated

**Deadline**: Before Phase 2 begins

---

### Bug 3: Dashboard Components Missing Focus Indicators and Keyboard Support

**Severity**: Class A (Accessibility violation - blocks keyboard navigation)
**App**: Dashboard
**Screen**: All Dashboard screens
**Bug Type**: Accessibility

**Description**:
Dashboard components in `apps/dashboard/src/components/` are missing focus indicators, keyboard event handlers, and ARIA attributes for interactive elements. Specifically:
- DataTable: Sortable column headers lack `tabIndex`, `role="button"`, `onKeyDown`, and focus ring styles
- DataTable: Action buttons lack focus ring styles
- NavigationItem: Missing focus ring styles
- TopBar: Notification button missing focus ring styles

**Steps to Reproduce**:
1. Open Dashboard
2. Tab through interactive elements
3. Observe that some elements don't show focus indicators
4. Try to sort DataTable columns using keyboard - doesn't work

**Expected Behavior**:
- All interactive elements have visible focus indicators
- All interactive elements support keyboard activation (Enter/Space)
- All sortable elements are keyboard accessible

**Actual Behavior**:
- Sortable column headers are not keyboard accessible
- Focus indicators are missing on interactive elements

**Severity Reason**:
- Violates WCAG 2.1 AA keyboard navigation requirements
- Violates constitution principle: "Accessibility: WCAG 2.1 AA minimum for all web applications"

**Status**: ✅ FIXED
**Fix Applied**:
- Added tabIndex, role="button", onKeyDown, and aria-label to DataTable column headers
- Added focus:ring-2 focus:ring-brand-primary to DataTable buttons and headers
- Added focus:ring-2 focus:ring-brand-primary to NavigationItem
- Added focus:outline-none focus:ring-2 focus:ring-brand-primary to TopBar notification button

**Affected Files**:
- `apps/dashboard/src/components/DataTable/DataTable.tsx` ✅ Fixed
- `apps/dashboard/src/components/AppShell/Sidebar/NavigationItem.tsx` ✅ Fixed
- `apps/dashboard/src/components/AppShell/TopBar.tsx` ✅ Fixed

**Deadline**: Before Phase 2 begins

---

## Bug Classification Summary

**Class A Bugs** (Must fix before Phase 2): 0
- Bug 1: StatusBadge using hardcoded colors instead of tokens ✅ FIXED
- Bug 2: Driver Mobile StationCard missing token imports ✅ FIXED
- Bug 3: Dashboard components missing focus indicators and keyboard support ✅ FIXED

**Class B Bugs** (Can defer to Phase 2): 0

**Class C Bugs** (Can defer to Phase 2): 0

**Total Bugs Found**: 3 (All Fixed)

**Next Steps**:
1. ✅ Fix Bug 1 (StatusBadge hardcoded colors) - COMPLETED
2. ✅ Fix Bug 2 (StationCard missing token imports) - COMPLETED
3. ✅ Fix Bug 3 (Dashboard focus indicators) - COMPLETED
4. Continue with Phase 5 accessibility tasks
5. Continue with remaining phases (6-10)
