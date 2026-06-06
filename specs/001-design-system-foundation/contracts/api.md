# Component API Contracts

## Overview

These contracts define the public API that consumers of the `@borne-map/ui` package must follow. These contracts ensure consistency, type safety, and interoperability across all BorneMap applications.

---

## Token Package API

### Import Path

```typescript
import * as tokens from '@borne-map/ui/tokens'
```

### Available Exports

#### Colors

```typescript
import { brandPrimary, success, error, neutral500 } from '@borne-map/ui/tokens/colors'
```

**Valid tokens**: All keys from `colors.ts`

**Usage**: `const color = brandPrimary`

**Type**: `string` (hex color)

---

#### Typography

```typescript
import { fontSizeLg, fontWeightBold, lineHeightNormal } from '@borne-map/ui/tokens/typography'
```

**Valid tokens**: All keys from `typography.ts`

**Usage**: `const size = fontSizeLg`

**Types**:
- `fontSize`: `number` (px)
- `fontWeight`: `400` | `500` | `600` | `700`
- `lineHeight`: `1.25` | `1.5` | `1.75`

---

#### Spacing

```typescript
import { spacing4, spacing8 } from '@borne-map/ui/tokens/spacing'
```

**Valid tokens**: All keys from `spacing.ts`

**Usage**: `const margin = spacing4`

**Type**: `number` (px, multiples of 4)

---

#### Radius

```typescript
import { radiusMd, radiusFull } from '@borne-map/ui/tokens/radius'
```

**Valid tokens**: All keys from `radius.ts`

**Usage**: `const borderRadius = radiusMd`

**Type**: `number` (px, integers)

---

#### Shadows

```typescript
import { shadowCard, shadowFloat } from '@borne-map/ui/tokens/shadows'
```

**Valid tokens**: All keys from `shadows.ts`

**Usage**: `const boxShadow = shadowCard`

**Types**: Object with `elevation`, `shadowColor`, `shadowOffset`, `shadowOpacity`, `shadowRadius`, `androidElevation` (web), `elevation` (RN)

---

#### Central Index

```typescript
import * as tokens from '@borne-map/ui/tokens'
```

**All tokens**: All exports from `colors.ts`, `typography.ts`, `spacing.ts`, `radius.ts`, `shadows.ts`

---

#### React Native Compatibility

```typescript
import * as tokens from '@borne-map/ui/tokens/native'
```

**All tokens**: Same names as main tokens, but values in React Native StyleSheet format

**Types**:
- Colors: `string`
- Spacing: `number` (px)
- Shadows: Object with `elevation`, `shadowColor`, `shadowOffset`, `shadowOpacity`, `shadowRadius`, `androidElevation`

---

### Token Usage Contracts

**Contract 1**: All visual values MUST come from tokens

```typescript
// ❌ WRONG: Hardcoded color
<div style={{ color: '#007943' }} />

// ✅ CORRECT: From token
import { brandPrimary } from '@borne-map/ui/tokens/colors'
<div style={{ color: brandPrimary }} />
```

**Contract 2**: Spacing values MUST be from tokens

```typescript
// ❌ WRONG: Hardcoded spacing
<div style={{ padding: '16px' }} />

// ✅ CORRECT: From token
import { spacing4 } from '@borne-map/ui/tokens/spacing'
<div style={{ padding: spacing4 }} />
```

**Contract 3**: No hardcoded shadow values

```typescript
// ❌ WRONG: Hardcoded shadow
<div style={{ boxShadow: '0 2px 4px rgba(0,0,0,0.1)' }} />

// ✅ CORRECT: From token
import { shadowCard } from '@borne-map/ui/tokens/shadows'
<div style={{ boxShadow: shadowCard }} />
```

---

## Component Package API

### Import Path

```typescript
import { Button, Input, Badge, StatusBadge } from '@borne-map/ui'
```

### Available Components

#### Button

```typescript
import { Button } from '@borne-map/ui'

<Button variant="primary" size="md" onClick={handleClick()}>
  Click me
</Button>
```

**Props**:
- `variant`?: `'primary' | 'secondary' | 'ghost' | 'danger'`
- `size`?: `'sm' | 'md' | 'lg'`
- `state`?: `'default' | 'hover' | 'active' | 'disabled' | 'loading'`
- `disabled`?: `boolean`
- `loading`?: `boolean`
- `children`: `React.ReactNode`
- `onClick`?: `() => void`

**Valid states**:
- `state="disabled"`: Button disabled, cannot click
- `state="loading"`: Show loading indicator, disable button, no other states
- `state="hover"`: Visual test only, not for end-user

**Accessibility**: Keyboard accessible, ARIA labels for screen readers, focus indicators

---

#### Input

```typescript
import { Input } from '@borne-map/ui'

<Input
  variant="search"
  state="error"
  error="Email is required"
  onChange={handleChange}
  value={value}
/>
```

**Props**:
- `variant`?: `'default' | 'error' | 'search'`
- `size`?: `'sm' | 'md' | 'lg'`
- `state`?: `'default' | 'focused' | 'error' | 'disabled'`
- `disabled`?: `boolean`
- `error`?: `string`
- `placeholder`?: `string`
- `value`?: `string`
- `onChange`?: `(value: string) => void`
- `type`?: `'text' | 'password' | 'search'`

**Valid states**:
- If `error` provided, `state` defaults to 'error'
- If `state="disabled"`, `disabled` prop takes precedence

---

#### StatusBadge

```typescript
import { StatusBadge } from '@borne-map/ui'

<StatusBadge variant="available" showDot={true}>
  Available
</StatusBadge>

<StatusBadge variant="in-use">
  In use
</StatusBadge>
```

**Props**:
- `variant`: `'available' | 'in-use' | 'maintenance' | 'offline'` (REQUIRED)
- `state`?: `'default' | 'animating'`
- `showDot`?: `boolean`
- `children`?: `React.ReactNode`

**Valid variants**:
- `available`: Green dot, "Available"
- `in-use`: Amber dot, "In use"
- `maintenance`: Red dot, "Maintenance"
- `offline`: Gray dot, "Offline"

**Accessibility**: Must include non-color indicator (dot + text)

---

#### Badge

```typescript
import { Badge } from '@borne-map/ui'

<Badge variant="success">Active</Badge>
<Badge variant="error">Invalid</Badge>
```

**Props**:
- `variant`?: `'default' | 'success' | 'warning' | 'error' | 'info'`
- `children`: `React.ReactNode`

---

#### Skeleton

```typescript
import { Skeleton } from '@borne-map/ui'

<Skeleton type="block" width="200px" height="40px" animated={true} />
<Skeleton type="text" width="300px" />
```

**Props**:
- `type`: `'block' | 'text' | 'circular'` (REQUIRED)
- `width`?: `number | string`
- `height`?: `number | string`
- `animated`?: `boolean`

---

#### EmptyState

```typescript
import { EmptyState } from '@borne-map/ui'

<EmptyState
  icon={<SearchIcon />}
  title="No results found"
  description="Try adjusting your search criteria"
  action={{
    label: "Clear filters",
    onClick: handleClear
  }}
/>
```

**Props**:
- `icon`?: `React.ReactNode`
- `title`: `string` (REQUIRED)
- `description`?: `string`
- `action`?: `{ label: string; onClick: () => void }`

---

#### ErrorState

```typescript
import { ErrorState } from '@borne-map/ui'

<ErrorState
  title="Something went wrong"
  description="Please try again or contact support"
  retry={() => handleRetry()}
/>
```

**Props**:
- `icon`?: `React.ReactNode`
- `title`: `string` (REQUIRED)
- `description`?: `string`
- `retry`?: `() => void`

---

#### Toast

```typescript
import { Toast } from '@borne-map/ui'

<Toast
  variant="success"
  title="Success!"
  message="Your changes have been saved."
  onClose={handleClose}
/>
```

**Props**:
- `variant`?: `'success' | 'error' | 'warning' | 'info'`
- `title`: `string` (REQUIRED)
- `message`?: `string`
- `duration`?: `number` (milliseconds, default 5000)
- `onClose`?: `() => void`
- `showCloseButton`?: `boolean`

---

#### Modal

```typescript
import { Modal } from '@borne-map/ui'

<Modal
  size="md"
  title="Confirm Action"
  isOpen={isOpen}
  onClose={handleClose}
>
  <p>Are you sure you want to proceed?</p>
</Modal>
```

**Props**:
- `size`?: `'sm' | 'md' | 'lg'`
- `title`?: `string`
- `isOpen`: `boolean` (REQUIRED)
- `onClose`: `() => void` (REQUIRED)
- `children`: `React.ReactNode`

**Note**: Controlled component pattern - must use `isOpen` and `onClose` props

---

#### Table

```typescript
import { Table } from '@borne-map/ui'

<Table
  columns={[
    { key: 'name', label: 'Name', sortable: true },
    { key: 'email', label: 'Email' },
    { key: 'status', label: 'Status' }
  ]}
  data={[
    { name: 'John Doe', email: 'john@example.com', status: 'active' },
    { name: 'Jane Smith', email: 'jane@example.com', status: 'inactive' }
  ]}
  rowActions={[
    { label: 'Edit', icon: <EditIcon /> }
  ]}
  onRowAction={handleRowAction}
/>
```

**Props**:
- `columns`: `Array<{ key: string; label: string; sortable?: boolean; width?: string | number }>` (REQUIRED)
- `data`: `Array<Record<string, any>>` (REQUIRED)
- `onRowAction`?: `(action: string, rowData: any) => void`
- `rowActions`?: `Array<{ label: string; icon: React.ReactNode }>`

---

#### StatCard

```typescript
import { StatCard } from '@borne-map/ui'

<StatCard
  label="Total Stations"
  value="124"
  trend={{ value: 12, positive: true }}
  icon={<StationIcon />}
/>
```

**Props**:
- `label`: `string` (REQUIRED)
- `value`: `string | number` (REQUIRED)
- `trend`?: `{ value: number; positive?: boolean }`
- `icon`?: `React.ReactNode`

---

#### DataCard

```typescript
import { DataCard } from '@borne-map/ui'

<DataCard
  title="Station Details"
  action={{
    label: "Edit",
    onClick: handleEdit
  }}
>
  <p>Content goes here</p>
</DataCard>
```

**Props**:
- `title`?: `string`
- `action`?: `{ label: string; onClick: () => void }`
- `children`: `React.ReactNode` (REQUIRED)

---

## Component Testing Contract

**Contract 1**: Each component MUST be unit tested for each variant/state combination

```typescript
// Example test structure
describe('Button', () => {
  it('renders with primary variant', () => {
    render(<Button variant="primary">Click</Button>)
    expect(screen.getByText('Click')).toBeInTheDocument()
  })

  it('renders in loading state', () => {
    render(<Button loading={true}>Loading</Button>)
    expect(screen.getByRole('button')).toBeDisabled()
    expect(screen.getByText('Loading')).toBeInTheDocument()
  })

  // Test all variants
  it('renders with secondary variant', () => { /* ... */ })
  it('renders with ghost variant', () => { /* ... */ })
  it('renders with danger variant', () => { /* ... */ })

  // Test all sizes
  it('renders with sm size', () => { /* ... */ })
  it('renders with md size', () => { /* ... */ })
  it('renders with lg size', () => { /* ... */ })

  // Test all states
  it('renders in disabled state', () => { /* ... */ })
  it('renders in hover state', () => { /* ... */ })
  it('renders in active state', () => { /* ... */ })
  it('renders in loading state', () => { /* ... */ })
})
```

**Contract 2**: Accessibility testing required for all interactive components

```typescript
// Must test
- Keyboard navigation (Tab key)
- Focus indicators visible
- ARIA labels for screen readers
- Color contrast ratios
```

---

## Export Contract

**Central Index**:

```typescript
import { Button, Input, Badge, StatusBadge, Skeleton, EmptyState, ErrorState, Toast, Modal, Table, StatCard, DataCard } from '@borne-map/ui'
```

**All components**: All 12 components must be exported from `components/index.ts`

---

## Tailwind Integration Contract

### Tailwind Config Extension

The package MUST include `tailwind.config.base.js` that extends Tailwind with token values.

**Example**:

```javascript
// tailwind.config.base.js
import { colors, spacing, borderRadius } from './src/tokens/index'

export default {
  theme: {
    extend: {
      colors: colors,
      spacing: spacing,
      borderRadius: borderRadius,
      // Additional Tailwind config...
    }
  }
}
```

**Contract**: Tailwind config must correctly resolve all token values without errors (FR-016)

---

## React Native Integration Contract

### Native Token Import

```typescript
import * as tokens from '@borne-map/ui/tokens/native'
```

**Contract**: Native tokens must be in StyleSheet-compatible format

```typescript
// React Native example
import { spacing4, shadowCard } from '@borne-map/ui/tokens/native'

const styles = StyleSheet.create({
  container: {
    padding: spacing4,
    shadow: shadowCard
  }
})
```

---

**Document Version**: 1.0
**Status**: Active
**Last Updated**: 2026-06-05
