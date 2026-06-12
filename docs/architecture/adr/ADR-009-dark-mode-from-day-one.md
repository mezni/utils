# ADR-009: Dark Mode Support from Day One

**Date:** 2026-06-11
**Status:** Accepted
**Decision:** Dark mode and light mode both supported from day one.

---

## Context

Dark mode is increasingly expected from mobile apps. Implementing it after the fact requires:
1. Color refactoring across all components
2. Theme context updates
3. Backward compatibility with existing designs
4. Testing on both themes

Doing it from day one eliminates refactoring and ensures consistent UX from launch.

---

## Decision

**Both dark and light themes must be supported from day one.**

### Design System

```typescript
// tokens.ts
export const tokens = {
  colors: {
    light: {
      background: '#ffffff',
      surface: '#f3f4f6',
      primary: '#3b82f6',
      text: '#000000',
      border: '#e5e7eb'
    },
    dark: {
      background: '#000000',
      surface: '#1a1a1a',
      primary: '#60a5fa',
      text: '#ffffff',
      border: '#262626'
    }
  },
  // ... more tokens
};
```

### Theme Provider

```typescript
// theme.ts
import { tokens } from './tokens';

export const theme = {
  light: tokens.colors.light,
  dark: tokens.colors.dark,
  current: tokens.colors.light // default
};

export const useTheme = () => {
  // hook to access current theme
  return theme.current;
};

export const toggleTheme = () => {
  theme.current = theme.current === tokens.colors.light
    ? tokens.colors.dark
    : tokens.colors.light;
};
```

### Usage in Components

```typescript
// ✗ WRONG - inline variant
<View style={{ backgroundColor: darkMode ? '#1a1a1a' : '#ffffff' }} />

// ✓ CORRECT - uses tokens
<View style={{ backgroundColor: tokens.colors.background }}>
```

---

## Consequences

### Positive
- Better UX for users preferring dark mode
- Future-proof design system
- No refactoring needed later
- Consistent tokens across components

### Negative
- Slightly more initial token definitions
- Need to test all screens in both themes

---

## Alternatives Considered

### Alternative 1: Light Mode Only
**Rejected:** Dark mode expected, refactoring later causes technical debt.

### Alternative 2: Theme Toggle Later
**Rejected:** Color refactoring, component testing burden, inconsistent UX.

### Alternative 3: System Preference Only
**Rejected:** Can't force dark mode, inconsistent user experience.

---

## Implementation

1. Define color tokens in both light and dark variants
2. Create theme provider
3. Wrap app with theme provider
4. Create theme toggle component
5. Test all screens in both themes
6. Ensure no hardcoded colors

---

## Testing Checklist

- [ ] All screens render in light mode
- [ ] All screens render in dark mode
- [ ] Theme toggle works correctly
- [ ] Colors accessible (WCAG AA)
- [ ] No hardcoded colors in components
- [ ] Smooth theme transitions
- [ ] Icons/illustrations adapted for both themes

---

## References

- **Constitution:** Section 7.7 — Dark Mode on Every Screen
- **Design System:** UI Pro Max Skill
- **Tokens:** `source/front/mobile-driver/design/tokens.ts`
