# Design System

## Version: 1.0
## Status: Active
## Core Philosophy: Documentation is the system. Code is just its execution.

---

## 🎯 PURPOSE

**Single source of truth for UI tokens and styling rules.**

**No component can define its own visual system.**

**Everything comes from @bm/design-tokens.**

---

## 🎨 TOKENS OVERVIEW

### Color System

**Roles, not raw colors:**

```typescript
// Design tokens (src/front/packages/@bm/design-tokens/)
colors: {
  primary: '#007AFF',      // Brand primary
  primaryDark: '#0062cc',  // Primary dark
  background: '#FFFFFF',   // Surface background
  surface: '#F5F5F5',      // Card surface
  text: '#1A1A1A',         // Primary text
  textSecondary: '#888888',// Secondary text
  error: '#FF3B30',        // Error states
  success: '#34C759',      // Success states
  warning: '#FF9500',      // Warning states
}
```

**Dark Mode Support:**
```typescript
darkMode: {
  colors: {
    background: '#000000',
    surface: '#1A1A1A',
    text: '#FFFFFF',
    textSecondary: '#888888',
  }
}
```

---

### Typography Scale

```typescript
typography: {
  heading1: {
    fontSize: 32,
    fontWeight: 'bold',
    lineHeight: 1.2,
  },
  heading2: {
    fontSize: 24,
    fontWeight: 'semibold',
    lineHeight: 1.3,
  },
  heading3: {
    fontSize: 20,
    fontWeight: 'semibold',
    lineHeight: 1.4,
  },
  body1: {
    fontSize: 16,
    fontWeight: 'regular',
    lineHeight: 1.5,
  },
  body2: {
    fontSize: 14,
    fontWeight: 'regular',
    lineHeight: 1.5,
  },
  caption: {
    fontSize: 12,
    fontWeight: 'regular',
    lineHeight: 1.4,
  },
}
```

---

### Spacing Scale

```typescript
spacing: {
  xs: 4,    // Extra small
  sm: 8,    // Small
  md: 16,   // Medium (base)
  lg: 24,   // Large
  xl: 32,   // Extra large
  xxl: 48,  // Double extra large
}
```

**Usage Rules:**
- Use `md` (16) as base spacing
- Always use even multiples
- No odd spacing values

---

### Radius System

```typescript
radius: {
  sm: 4,    // Small
  md: 8,    // Medium (base)
  lg: 16,   // Large
  xl: 24,   // Extra large
  full: 9999, // Full (pill shape)
}
```

**Usage Rules:**
- Use `md` (8) as base radius
- Component-specific overrides allowed
- Consistent within component

---

## 📦 DESIGN TOKENS IMPLEMENTATION

### @bm/design-tokens Package

**Location:** `source/front/packages/@bm/design-tokens/`

**Package Structure:**
```
@bm/design-tokens/
├── package.json
├── index.ts          # Exports all tokens
├── colors.ts         # Color roles
├── typography.ts     # Typography scale
├── spacing.ts        # Spacing scale
├── radius.ts         # Radius scale
├── shadows.ts        # Shadow definitions
├── z-index.ts        # Z-index hierarchy
└── darkMode.ts       # Dark mode tokens
```

**Example Index:**
```typescript
export * from './colors';
export * from './typography';
export * from './spacing';
export * from './radius';
export * from './shadows';
export * from './z-index';
export * from './darkMode';
```

---

## 🎨 IMPLEMENTATION RULES

### 1. Component Styling

**❌ WRONG - Component defines its own tokens:**

```typescript
// Don't do this
function StationMarker({ station }: { station: Station }) {
  const color = station.status === 'active' ? '#007AFF' : '#FF3B30';
  return (
    <View style={{ padding: 8, backgroundColor: color }}>
      <Text style={{ fontSize: 12, color: '#FFFFFF' }}>
        {station.name}
      </Text>
    </View>
  );
}
```

---

**✅ CORRECT - Component uses design tokens:**

```typescript
// Do this
import { colors, spacing, typography, radius } from '@bm/design-tokens';

function StationMarker({ station }: { station: Station }) {
  return (
    <View
      style={{
        padding: spacing.sm,
        backgroundColor: station.status === 'active'
          ? colors.primary
          : colors.error,
        borderRadius: radius.md,
      }}
    >
      <Text style={typography.caption}>
        {station.name}
      </Text>
    </View>
  );
}
```

---

### 2. Global Styling

**❌ WRONG - Inline styles:**

```typescript
// Don't do this
<View style={{ flex: 1, backgroundColor: 'white', marginTop: 20 }}>
```

---

**✅ CORRECT - Style components:**

```typescript
// Do this
import { colors, spacing, darkMode } from '@bm/design-tokens';

const styles = StyleSheet.create({
  container: {
    flex: 1,
    backgroundColor: colors.background,
    paddingTop: spacing.md,
  },
});

// Dark mode support
const styles = StyleSheet.create({
  container: {
    flex: 1,
    backgroundColor: colors.background,
    paddingTop: spacing.md,
  },
  darkContainer: {
    backgroundColor: darkMode.colors.background,
  },
});
```

---

### 3. Responsive Design

**Web only:**

```css
/* Don't do this */
.some-component {
  width: 500px;
}

/* Do this */
.some-component {
  width: 100%;
  max-width: 500px;
}
```

**Mobile:**
- Always mobile-first
- Use responsive breakpoints

---

## 🎯 USAGE REQUIREMENTS

### Required for MVP-1

**Components must use:**
- [ ] Colors from design tokens
- [ ] Spacing from design tokens
- [ ] Typography from design tokens
- [ ] Radius from design tokens
- [ ] Shadows from design tokens
- [ ] Z-index from design tokens

### Forbidden Patterns

**❌ Never:**
- Hardcoded colors (except layout glue)
- Hardcoded spacing values
- Inline styles (except layout glue)
- Custom radius calculations
- Custom typography
- Custom z-index values

---

## 🌗 DARK MODE SUPPORT

### Dark Mode Requirements

**MVP-1:**
- [ ] Dark mode toggle (optional)
- [ ] Design tokens for dark mode
- [ ] Adaptive components
- [ ] Theme-aware styling

**Dark Mode Architecture:**
```typescript
import { darkMode } from '@bm/design-tokens';

function MyComponent() {
  const isDark = useDarkMode();

  return (
    <View
      style={{
        backgroundColor: isDark
          ? darkMode.colors.background
          : colors.background,
      }}
    >
      {/* Content */}
    </View>
  );
}
```

---

## 🧱 COMPONENT BASELINE

### Basic Component Template

**Mobile:**
```typescript
import { colors, spacing, typography, radius, darkMode } from '@bm/design-tokens';

const styles = StyleSheet.create({
  container: {
    padding: spacing.md,
    backgroundColor: colors.surface,
    borderRadius: radius.lg,
  },
  title: {
    ...typography.heading2,
    color: colors.text,
  },
  subtitle: {
    ...typography.body2,
    color: colors.textSecondary,
  },
  darkContainer: {
    backgroundColor: darkMode.colors.surface,
  },
  darkTitle: {
    color: darkMode.colors.text,
  },
});
```

---

**Web:**
```tsx
import { colors, spacing, typography, radius, darkMode } from '@bm/design-tokens';

const useStyles = makeStyles({
  container: {
    padding: spacing.md,
    backgroundColor: colors.surface,
    borderRadius: radius.lg,
  },
  title: {
    ...typography.heading2,
    color: colors.text,
  },
  subtitle: {
    ...typography.body2,
    color: colors.textSecondary,
  },
  darkContainer: {
    backgroundColor: darkMode.colors.surface,
  },
  darkTitle: {
    color: darkMode.colors.text,
  },
});
```

---

## 🔄 DESIGN TOKENS MAINTENANCE

### Update Process

1. **Identify inconsistency**
2. **Update design tokens**
3. **Update all components**
4. **Run tests**
5. **Verify visual consistency**

### Versioning

- Major version for breaking changes
- Minor version for additions
- Patch version for fixes

---

## 🧠 CORE PRINCIPLE

**Design tokens are the single source of truth. No component defines its own visual system.**

---

*This document ensures all styling is consistent, maintainable, and theme-aware.*