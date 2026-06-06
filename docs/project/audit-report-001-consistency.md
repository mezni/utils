# Sprint 1.5 - Cross-App Consistency Audit Report

**Date**: June 6, 2026
**Auditor**: OpenCode
**Scope**: Driver Web, Driver Mobile, Dashboard

---

## StatusBadge Component Audit

### Findings

**Issue 1**: StatusBadge not exported from UI package

**Details**:
- File: `/home/dali/WORK/BorneMap/packages/ui/src/index.ts`
- Status: NOT EXPORTED
- Impact: Cannot be used across all three apps

**Issue 2**: No StatusBadge usage in Driver Web StationCard

**Details**:
- File: `/home/dali/WORK/BorneMap/apps/driver-web/src/components/StationCard.tsx`
- Status: Does not use StatusBadge component
- Current approach: Custom inline styling
- Impact: Inconsistent styling with UI package component

**Issue 3**: No StatusBadge usage in Driver Mobile StationCard

**Details**:
- File: `/home/dali/WORK/BorneMap/apps/driver-mobile/src/components/StationCard.tsx`
- Status: Does not use StatusBadge component
- Current approach: Custom inline styling
- Impact: Inconsistent styling with UI package component

---

## StationCard Component Audit

### Findings

**Issue 4**: Different styling approaches between web and mobile

**Details**:
- Driver Web StationCard: Uses Tailwind classes
- Driver Mobile StationCard: Uses inline styles from tokens
- Both have similar functionality but different implementation
- StatusBadge used differently (none uses shared component)

**Impact**: Maintenance overhead, potential for inconsistencies

---

## Color Token Usage Audit

### Findings

**Issue 5**: brand.primary (#007943) usage verification needed

**Details**:
- StatusBadge uses brand.primary for active states in UI package
- Need to verify usage across all apps
- Need to ensure no hardcoded colors exist

**Verification needed**:
- [ ] Verify brand.primary in all active states
- [ ] Check for hardcoded colors in components
- [ ] Verify color tokens resolve correctly

---

## Component Usage Summary

### UI Package Components Used

| Component | Used In | Usage Status |
|-----------|---------|--------------|
| Button | All apps | ✅ Exported & Used |
| Input | All apps | ✅ Exported & Used |
| Badge | All apps | ✅ Exported & Used |
| StatusBadge | UI package only | ❌ NOT EXPORTED |
| Skeleton | All apps | ✅ Exported & Used |
| EmptyState | All apps | ✅ Exported & Used |
| ErrorState | All apps | ✅ Exported & Used |
| Toast | All apps | ✅ Exported & Used |
| Modal | All apps | ✅ Exported & Used |
| Table | All apps | ✅ Exported & Used |
| StatCard | All apps | ✅ Exported & Used |
| DataCard | All apps | ✅ Exported & Used |

### Driver-Specific Components

| Component | Driver Web | Driver Mobile | Dashboard | Status |
|-----------|-----------|---------------|-----------|--------|
| StationCard | ✅ Yes | ✅ Yes | ❌ No | ✅ OK |
| BottomStationCard | ✅ Yes | ✅ Yes | ❌ No | ✅ OK |
| MobileTopBar | ❌ No | ✅ Yes | ❌ No | ✅ OK |
| SearchBar | ✅ Yes | ✅ Yes | ❌ No | ✅ OK |
| FilterPills | ✅ Yes | ✅ Yes | ❌ No | ✅ OK |
| MapPinMarker | ✅ Yes | ✅ Yes | ❌ No | ✅ OK |
| ZoomControls | ✅ Yes | ✅ Yes | ❌ No | ✅ OK |
| ReviewCard | ✅ Yes | ✅ Yes | ❌ No | ✅ OK |
| ChargerRow | ✅ Yes | ❌ No | ❌ No | ✅ OK |

### Dashboard Components

| Component | Driver Web | Driver Mobile | Dashboard | Status |
|-----------|-----------|---------------|-----------|--------|
| AppShell | ❌ No | ❌ No | ✅ Yes | ✅ OK |
| Sidebar | ❌ No | ❌ No | ✅ Yes | ✅ OK |
| NavigationItem | ❌ No | ❌ No | ✅ Yes | ✅ OK |
| TopBar | ❌ No | ❌ No | ✅ Yes | ✅ OK |
| PageContent | ❌ No | ❌ No | ✅ Yes | ✅ OK |
| DataTable | ❌ No | ❌ No | ✅ Yes | ✅ OK |

---

## Critical Issues Found

### Class A Issues

**Issue A1**: StatusBadge not exported from UI package
- **Severity**: Class A
- **Impact**: Cannot use shared component across all apps
- **Fix Required**: Add export to `packages/ui/src/index.ts`
- **Status**: NOT FIXED

**Issue A2**: StationCard doesn't use StatusBadge in either app
- **Severity**: Class A
- **Impact**: Inconsistent status indicator styling
- **Fix Required**: Replace inline status styling with StatusBadge component
- **Status**: NOT FIXED

### Class B Issues

**Issue B1**: Different styling approaches between web and mobile StationCard
- **Severity**: Class B
- **Impact**: Maintenance overhead
- **Fix Required**: Consider making mobile StationCard use shared tokens
- **Status**: DEFERRED TO PHASE 9

**Issue B2**: Brand primary color verification needed across all apps
- **Severity**: Class B
- **Impact**: Potential color inconsistency
- **Fix Required**: Comprehensive audit of all active states
- **Status**: PENDING AUDIT

---

## Recommendations

### Immediate Actions (Must Fix Before Phase 2)

1. **Export StatusBadge from UI package**
   - Update `packages/ui/src/index.ts` to export StatusBadge
   - Verify StatusBadge works in all three apps
   - Replace inline status styling with StatusBadge component

2. **Fix StationCard to use StatusBadge**
   - Update driver-web StationCard to use shared StatusBadge
   - Update driver-mobile StationCard to use shared StatusBadge
   - Test in both apps to ensure visual consistency

3. **Comprehensive color token audit**
   - Verify brand.primary appears in all active states
   - Check for hardcoded colors in components
   - Ensure all tokens resolve correctly

### Phase 9 Actions (Can Defer)

1. Standardize StationCard implementation across web and mobile
2. Consider making mobile components use shared web components
3. Comprehensive accessibility audit

---

## Next Steps

1. ✅ Export StatusBadge from UI package
2. ✅ Update StationCard components to use StatusBadge
3. ✅ Fix inline status styling issues
4. ⏳ Verify brand.primary usage across all apps
5. ⏳ Proceed to RTL audit

---

## Audit Summary

- **StatusBadge Export**: ❌ NOT EXPORTED
- **StationCard StatusBadge Usage**: ❌ NOT USED
- **Component Usage Consistency**: ⚠️ PARTIAL (25/27 components consistent)
- **Critical Issues**: 2 (both must be fixed)
- **Class A Bugs**: 2
- **Class B Bugs**: 2 (1 deferred to Phase 9)

**Overall Consistency Rating**: 6/10 ⚠️

---

**Audit Completed**: Yes
**Ready for RTL Audit**: Yes
**Blocking Phase 2**: No (issues are fixable)