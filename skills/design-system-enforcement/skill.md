# Design System Enforcement Skill — BorneMap

## Purpose
Prevent design system drift through strict token usage and pattern enforcement.

---

## 🎯 Core Philosophy

**Design is not appearance. Design is systematic behavior under interaction.**

All design is controlled through tokens, patterns, and strict rules.

---

## 🚫 The Problem

**Design system drift happens when:**
- Hardcoded colors and spacing
- Duplicate UI patterns
- Inconsistent styling
- Ad-hoc component creation

---

## 🔒 Core Rules

### 1. No Styling Outside Tokens

**All styling MUST go through design tokens:**

```typescript
// ❌ WRONG - Hardcoded styling
function StationMarker({ station }) {
  return (
    <View style={{
      padding: 16,  // ❌ Hardcoded spacing
      backgroundColor: '#007AFF',  // ❌ Hardcoded color
      borderRadius: 8,  // ❌ Hardcoded radius
    }}>
      <Text style={{
        fontSize: 14,  // ❌ Hardcoded typography
        color: '#FFFFFF',  // ❌ Hardcoded color
      }}>
        {station.name}
      </Text>
    </View>
  );
}

// ✅ CORRECT - Using design tokens
import { colors, spacing, radius, typography } from '@bm/design-tokens';

function StationMarker({ station }) {
  return (
    <View style={{
      padding: spacing.md,
      backgroundColor: colors.primary,
      borderRadius: radius.md,
    }}>
      <Text style={typography.body2}>
        {station.name}
      </Text>
    </View>
  );
}
```

---

### 2. No Duplicated UI Patterns

**Single pattern library:**

```typescript
// ❌ WRONG - Duplicated patterns
// StationMarker in mobile
function StationMarker({ station }) {
  return (
    <View style={{
      padding: 16,
      backgroundColor: '#007AFF',
      borderRadius: 8,
    }}>
      <Text style={{ fontSize: 14, color: '#FFFFFF' }}>
        {station.name}
      </Text>
    </View>
  );
}

// StationMarker in web
function StationMarker({ station }) {
  return (
    <div style={{
      padding: '16px',
      backgroundColor: '#007AFF',
      borderRadius: '8px',
    }}>
      <div style={{ fontSize: '14px', color: '#FFFFFF' }}>
        {station.name}
      </div>
    </div>
  );
}

// ✅ CORRECT - Single pattern library
import { colors, spacing, radius, typography } from '@bm/design-tokens';
import { Button } from '@bm/components';

function StationMarker({ station }) {
  return (
    <Button
      variant="station"
      onPress={() => onSelect(station)}
    >
      <Text style={typography.body2}>
        {station.name}
      </Text>
    </Button>
  );
}
```

**UI Pattern Library:**

1. **Buttons:** StationMarker, Navigation, Primary, Secondary
2. **Cards:** StationCard, Card, CardContent
3. **Inputs:** TextInput, Select, SearchBar
4. **Lists:** StationList, List, ListItem
5. **Feedback:** Skeleton, Loading, Error

---

### 3. Consistent Spacing/Typography Usage

**Always use design tokens:**

```typescript
// ❌ WRONG - Hardcoded spacing
<View style={{ padding: 20, marginTop: 10, marginBottom: 10, spacing: 16 }}>
  <Text style={{ fontSize: 16, lineHeight: 1.5, fontWeight: 'bold' }}>
    {title}
  </Text>
</View>

// ✅ CORRECT - Using tokens
import { spacing, typography } from '@bm/design-tokens';

const styles = StyleSheet.create({
  container: {
    padding: spacing.xl,
    marginTop: spacing.md,
    marginBottom: spacing.md,
    spacing: spacing.md,  // ✅ Only use spacing token once
  },
  title: {
    ...typography.heading2,
    color: colors.text,
  },
});

// ✅ CORRECT - Using tokens
<View style={styles.container}>
  <Text style={styles.title}>{title}</Text>
</View>
```

**Spacing Rules:**
- Use `spacing.md` (16) as base
- Always use even multiples
- Never use odd spacing values
- Use spacing only for layout, not content

**Typography Rules:**
- Use `typography.heading1`, `typography.body2`, etc.
- Never use `fontSize` or `fontWeight` directly
- Never use `lineHeight` directly
- Use typography scale for all text

---

### 4. Platform Consistency Rules

**Same patterns across platforms:**

```typescript
// ❌ WRONG - Different patterns
// Mobile
function StationMarker({ station }) {
  return (
    <TouchableOpacity
      style={{
        padding: 16,
        backgroundColor: colors.primary,
        borderRadius: 8,
      }}
      onPress={() => onSelect(station)}
    >
      <Text style={{ fontSize: 14, color: colors.text }}>
        {station.name}
      </Text>
    </TouchableOpacity>
  );
}

// Web
function StationMarker({ station }) {
  return (
    <button
      style={{
        padding: '16px',
        backgroundColor: colors.primary,
        borderRadius: '8px',
      }}
      onClick={() => onSelect(station)}
    >
      <span style={{ fontSize: '14px', color: colors.text }}>
        {station.name}
      </span>
    </button>
  );
}

// ✅ CORRECT - Consistent patterns
import { colors, spacing, radius, typography } from '@bm/design-tokens';
import { Button } from '@bm/components';

function StationMarker({ station }) {
  return (
    <Button
      variant="station"
      onPress={() => onSelect(station)}
    >
      <Text style={typography.body2}>{station.name}</Text>
    </Button>
  );
}
```

**Platform Consistency:**
- Same components across platforms
- Same variants and patterns
- Same token usage
- Same behavior

---

## 📋 Design Token Usage Checklist

**Before implementing ANY component:**

- [ ] Uses design tokens for all styling
- [ ] No hardcoded colors
- [ ] No hardcoded spacing
- [ ] No hardcoded typography
- [ ] No hardcoded radius
- [ ] No custom colors
- [ ] No custom spacing
- [ ] No custom typography
- [ ] No custom radius

**After implementing ANY component:**

- [ ] All styling uses tokens
- [ ] No hardcoded values
- [ ] Consistent with patterns
- [ ] Platform consistency verified

---

## 🚫 Forbidden Patterns

### 1. Hardcoded Colors

```typescript
// ❌ WRONG
<View style={{ backgroundColor: '#007AFF' }}>

// ✅ CORRECT
import { colors } from '@bm/design-tokens';
<View style={{ backgroundColor: colors.primary }}>
```

### 2. Hardcoded Spacing

```typescript
// ❌ WRONG
<View style={{ padding: 16 }}>

// ✅ CORRECT
import { spacing } from '@bm/design-tokens';
<View style={{ padding: spacing.md }}>
```

### 3. Hardcoded Typography

```typescript
// ❌ WRONG
<Text style={{ fontSize: 14, fontWeight: 'bold' }}>

// ✅ CORRECT
import { typography } from '@bm/design-tokens';
<Text style={typography.body2}>
```

### 4. Custom Colors

```typescript
// ❌ WRONG
const customColor = '#FF6B6B';
<View style={{ backgroundColor: customColor }}>

// ✅ CORRECT
import { colors } from '@bm/design-tokens';
// Use only from colors.roles, not custom colors
```

---

## 🎯 Design Token Validation

### Pre-Implementation Check

```
1. Identify Component Style
   ↓
2. Check Styling Requirements
   - Colors needed?
   - Spacing needed?
   - Typography needed?
   - Radius needed?
   ↓
3. Select Design Tokens
   - Use colors.roles
   - Use spacing scale
   - Use typography scale
   - Use radius scale
   ↓
4. Apply Tokens
   - Replace all hardcoded values
   - Use style objects
   - Use inline styles only for layout glue
```

---

## 📊 Design System Compliance

### Current Compliance

**Design Token Usage:**
- ✅ All components use design tokens
- ✅ No hardcoded colors
- ✅ No hardcoded spacing
- ✅ No hardcoded typography
- ✅ No hardcoded radius
- ✅ Consistent patterns across platforms
- ✅ Platform consistency verified

**Platform Consistency:**
- ✅ Mobile patterns match web patterns
- ✅ Same components across platforms
- ✅ Same behavior across platforms
- ✅ Same design token usage

---

## 🚦 Design System Enforcement

### Violation Detection

**If violation detected:**

1. **Hardcoded values:**
   - ❌ STOP implementation
   - ✅ Identify all hardcoded values
   - ✅ Find matching design tokens
   - ✅ Replace all occurrences

2. **Duplicate patterns:**
   - ❌ STOP implementation
   - ✅ Identify duplicate patterns
   - ✅ Create shared pattern
   - ✅ Refactor to use pattern

3. **Platform inconsistencies:**
   - ❌ STOP implementation
   - ✅ Identify platform differences
   - ✅ Create platform adapters
   - ✅ Enforce platform consistency

---

## 🔄 Design System Evolution

### Adding New Design Tokens

**Only when necessary:**

1. **Identify need:**
   - Is there a missing token?
   - Is there a duplicated pattern?

2. **Create token:**
   - Add to design token file
   - Document the token
   - Update documentation

3. **Update all usage:**
   - Find all occurrences
   - Replace hardcoded values
   - Update components

---

## 🧹 Design System Compliance Checklist

**Before Implementing ANY Component:**

- [ ] Identify styling requirements
- [ ] Select design tokens
- [ ] Use tokens for all styling
- [ ] No hardcoded values
- [ ] Consistent patterns
- [ ] Platform consistency

**After Implementing ANY Component:**

- [ ] All styling uses tokens
- [ ] No hardcoded values
- [ ] Consistent with patterns
- [ ] Platform consistency verified
- [ ] Documentation updated

---

*This skill prevents design system drift through strict token usage and pattern enforcement.*