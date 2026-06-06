# Quickstart: Design System Foundation

This guide shows you how to use the `@borne-map/ui` design system package in your BorneMap applications.

---

## Installation

### 1. Ensure Dependencies are Installed

```bash
# Install Node.js (v18+ recommended)
node --version

# Install pnpm
npm install -g pnpm
```

### 2. Verify Monorepo Setup

```bash
# Navigate to project root
cd /home/dali/WORK/BorneMap

# Verify workspace is configured
cat pnpm-workspace.yaml
```

Expected output includes `packages/*` in the workspaces list.

---

## Using Design Tokens

### Basic Token Import

```typescript
import * as tokens from '@borne-map/ui/tokens'

// Use token values
const primaryColor = tokens.brandPrimary // '#007943'
const fontSize = tokens.fontSizeLg // 16
const spacing = tokens.spacing4 // 16
const radius = tokens.radiusMd // 8
const shadow = tokens.shadowCard // { elevation: 2 }
```

### Import by Category

```typescript
// Colors
import { brandPrimary, success, error } from '@borne-map/ui/tokens/colors'

// Typography
import { fontSizeLg, fontWeightBold, lineHeightNormal } from '@borne-map/ui/tokens/typography'

// Spacing
import { spacing4, spacing8 } from '@borne-map/ui/tokens/spacing'

// Radius
import { radiusMd, radiusFull } from '@borne-map/ui/tokens/radius'

// Shadows
import { shadowCard, shadowFloat } from '@borne-map/ui/tokens/shadows'
```

### React Native Compatibility

```typescript
// React Native uses different format for spacing and shadows
import * as tokens from '@borne-map/ui/tokens/native'

const styles = StyleSheet.create({
  container: {
    padding: tokens.spacing4, // number, not string
    shadow: tokens.shadowCard // object with elevation, not boxShadow string
  }
})
```

---

## Using Shared Components

### Import Components

```typescript
import { Button, Input, Badge, StatusBadge } from '@borne-map/ui'
```

### Button Component

```typescript
import { Button } from '@borne-map/ui'

function App() {
  return (
    <div>
      <Button variant="primary" onClick={() => handleClick()}>
        Click me
      </Button>

      <Button variant="secondary" size="lg" disabled={true}>
        Disabled button
      </Button>

      <Button variant="ghost" loading={true}>
        Loading...
      </Button>
    </div>
  )
}
```

**Variants**: `primary`, `secondary`, `ghost`, `danger`

**Sizes**: `sm`, `md`, `lg`

**States**: `default`, `hover`, `active`, `disabled`, `loading`

---

### Input Component

```typescript
import { Input } from '@borne-map/ui'

function App() {
  const [value, setValue] = useState('')

  return (
    <div>
      <Input
        variant="search"
        value={value}
        onChange={setValue}
        placeholder="Search stations..."
      />

      <Input
        variant="error"
        error="Email is required"
        type="password"
      />
    </div>
  )
}
```

**Variants**: `default`, `error`, `search`

**States**: `default`, `focused`, `error`, `disabled`

---

### StatusBadge Component

```typescript
import { StatusBadge } from '@borne-map/ui'

function App() {
  return (
    <div>
      <StatusBadge variant="available" showDot={true}>
        Available
      </StatusBadge>

      <StatusBadge variant="in-use">
        In use
      </StatusBadge>

      <StatusBadge variant="maintenance">
        Maintenance
      </StatusBadge>

      <StatusBadge variant="offline">
        Offline
      </StatusBadge>
    </div>
  )
}
```

**Variants**: `available`, `in-use`, `maintenance`, `offline`

---

### Using Components with Token-Based Styles

```typescript
import * as tokens from '@borne-map/ui/tokens'
import { Button } from '@borne-map/ui'

function App() {
  return (
    <Button
      style={{
        backgroundColor: tokens.brandPrimary,
        color: tokens.neutral100,
        padding: `${tokens.spacing3} ${tokens.spacing5}`,
        borderRadius: tokens.radiusMd,
        border: `1px solid ${tokens.neutral300}`,
        fontSize: tokens.fontSizeMd,
        fontWeight: tokens.fontWeightSemibold
      }}
    >
      Click me
    </Button>
  )
}
```

**Note**: Use tokens for ALL visual values (colors, spacing, typography, shadows, radius).

---

## Accessibility (WCAG 2.1 AA)

All web components meet WCAG 2.1 AA compliance:

### Keyboard Navigation

- All interactive elements (Button, Input) are keyboard accessible
- Tab to focus, Enter/Space to activate
- Focus indicators visible (ring around element)

### Color Contrast

- All text must have ≥ 4.5:1 contrast ratio on background
- Status badges include non-color indicators (dot + text label)
- Success/error/warning colors have sufficient contrast

### ARIA Labels

- All interactive elements have appropriate ARIA labels
- Status badges announce state to screen readers
- Loading states indicate progress

### Example: Accessible Button

```typescript
<Button
  variant="primary"
  onClick={handleClick}
  aria-label="Save changes"
>
  Save
</Button>
```

---

## RTL (Right-to-Left) Support

Components support RTL automatically based on context:

```typescript
import * as i18n from 'i18n'

function App() {
  const { locale } = i18n.useLocale()

  return (
    <div dir={locale === 'ar' ? 'rtl' : 'ltr'}>
      {/* All components inside automatically adapt to RTL */}
      <Button variant="primary">Save</Button>
      <Input placeholder="Search..." />
    </div>
  )
}
```

**Important**:
- Components use `dir="rtl"` context for RTL layout
- No manual RTL handling required
- Arabic language requires RTL layout

---

## Testing Components

### Unit Tests

Each component must have unit tests for each variant/state combination.

```typescript
import { render, screen } from '@testing-library/react'
import { Button } from '@borne-map/ui'

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
})
```

### Running Tests

```bash
# Run all tests
pnpm test

# Run tests in watch mode
pnpm test:watch

# Run tests with coverage
pnpm test:coverage
```

---

## Building the Package

### Build for Production

```bash
# Build the token package
cd packages/ui
pnpm build
```

### Build Output

- `dist/` directory contains built files
- TypeScript compilation complete
- No build warnings allowed (SC-004)

### Verify Build

```bash
# Check for errors
pnpm build 2>&1 | grep -i error

# Check for warnings
pnpm build 2>&1 | grep -i warning
```

---

## Development Workflow

### 1. Add New Token

Edit the appropriate token file in `packages/ui/src/tokens/`:

```typescript
// packages/ui/src/tokens/colors.ts
export const brandAccent = '#00d4ff' // New accent color
```

### 2. Update Tailwind Config

```typescript
// packages/ui/tailwind.config.base.js
import { colors } from './src/tokens/colors'

export default {
  theme: {
    extend: {
      colors: {
        ...colors,
        brandAccent // Add to Tailwind config
      }
    }
  }
}
```

### 3. Test the Change

```bash
# Build the package
cd packages/ui
pnpm build

# Run tests
pnpm test
```

### 4. Document the Change

Update `docs/ui/tokens.md` with the new token:

```markdown
## New Colors

### brandAccent
- **Value**: `#00d4ff`
- **Usage**: Call to action buttons, highlights
- **Contrast**: ≥ 4.5:1 on neutral100
```

---

## Common Patterns

### Pattern 1: Button with Loading State

```typescript
import { useState } from 'react'
import { Button } from '@borne-map/ui'

function SubmitButton({ onSubmit }) {
  const [loading, setLoading] = useState(false)

  const handleClick = async () => {
    setLoading(true)
    try {
      await onSubmit()
    } finally {
      setLoading(false)
    }
  }

  return (
    <Button variant="primary" loading={loading} onClick={handleClick}>
      Submit
    </Button>
  )
}
```

### Pattern 2: Error Input with Message

```typescript
import { useState } from 'react'
import { Input } from '@borne-map/ui'

function EmailInput() {
  const [value, setValue] = useState('')
  const [error, setError] = useState(null)

  const handleChange = (e) => {
    setValue(e.target.value)
    setError(null)
  }

  const handleSubmit = () => {
    if (!value.includes('@')) {
      setError('Invalid email format')
      return
    }
    // Handle submission
  }

  return (
    <Input
      variant="error"
      value={value}
      onChange={handleChange}
      error={error}
      placeholder="Enter email"
    />
  )
}
```

### Pattern 3: Status Badge with Icon

```typescript
import { StatusBadge } from '@borne-map/ui'
import { StationIcon } from './StationIcon'

function StationStatus({ status }) {
  return (
    <StatusBadge variant={status} showDot={true}>
      <StationIcon />
      {status}
    </StatusBadge>
  )
}
```

---

## Troubleshooting

### Problem: Build Fails with TypeScript Errors

**Solution**: Check for typos in token names or value types

```bash
# Run build with TypeScript error output
pnpm build 2>&1 | grep TypeScript
```

### Problem: Token Value is Undefined

**Solution**: Verify token is exported in correct file

```typescript
// Check exports in tokens/index.ts
export * from './colors'
export * from './typography'
// ... ensure token file is listed
```

### Problem: Tailwind Doesn't Resolve Tokens

**Solution**: Verify Tailwind config imports tokens correctly

```javascript
// tailwind.config.base.js
import { colors, spacing, borderRadius } from './src/tokens/index'
```

### Problem: Components Don't Support RTL

**Solution**: Ensure container has `dir="rtl"` attribute

```typescript
<div dir={locale === 'ar' ? 'rtl' : 'ltr'}>
  {/* Components inside */}
</div>
```

---

## Next Steps

1. **Read Component Documentation**: See `docs/ui/components.md` for detailed component usage
2. **Review Token Documentation**: See `docs/ui/tokens.md` for complete token reference
3. **Explore Design System**: View examples in existing BorneMap applications
4. **Contribute**: Add new tokens or components following the same patterns

---

## Additional Resources

- **Component Documentation**: [docs/ui/components.md](../ui/components.md)
- **Token Documentation**: [docs/ui/tokens.md](../ui/tokens.md)
- **Design Tokens**: [packages/ui/src/tokens/](../../packages/ui/src/tokens/)
- **Components Source**: [packages/ui/src/components/](../../packages/ui/src/components/)

---

**Document Version**: 1.0
**Status**: Active
**Last Updated**: 2026-06-05
