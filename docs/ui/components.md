# Components

All 12 shared components in the `@borne-map/ui` package.

## Button

```tsx
import { Button } from '@borne-map/ui'

<Button variant="primary" size="md" onClick={handleClick}>
  Click me
</Button>
```

**Props**:
| Prop | Type | Default | Description |
|------|------|---------|-------------|
| `variant` | `'primary' \| 'secondary' \| 'ghost' \| 'danger'` | `'primary'` | Visual style |
| `size` | `'sm' \| 'md' \| 'lg'` | `'md'` | Size |
| `state` | `'default' \| 'hover' \| 'active' \| 'disabled' \| 'loading'` | — | Explicit state override |
| `disabled` | `boolean` | `false` | Disables the button |
| `loading` | `boolean` | `false` | Shows spinner, disables |
| `children` | `React.ReactNode` | required | Button content |
| `onClick` | `() => void` | — | Click handler |

**Accessibility**: Keyboard navigable (Enter/Space), focus indicator, ARIA labels, disabled/loading ARIA states.

## Input

```tsx
import { Input } from '@borne-map/ui'

<Input variant="default" placeholder="Search..." onChange={(v) => setValue(v)} />
<Input variant="error" error="This field is required" />
```

**Props**:
| Prop | Type | Default | Description |
|------|------|---------|-------------|
| `variant` | `'default' \| 'error' \| 'search'` | `'default'` | Visual style |
| `size` | `'sm' \| 'md' \| 'lg'` | `'md'` | Size |
| `disabled` | `boolean` | `false` | Disables the input |
| `error` | `string` | — | Error message (switches to error variant) |
| `placeholder` | `string` | — | Placeholder text |
| `value` | `string` | — | Controlled value |
| `type` | `'text' \| 'password' \| 'search'` | `'text'` | Input type |
| `onChange` | `(value: string) => void` | — | Change handler |

**Accessibility**: aria-invalid, aria-describedby for errors, focus indicator, aria-label support.

## Badge

```tsx
import { Badge } from '@borne-map/ui'

<Badge variant="success">Active</Badge>
<Badge variant="error">Invalid</Badge>
```

**Props**:
| Prop | Type | Default | Description |
|------|------|---------|-------------|
| `variant` | `'default' \| 'success' \| 'warning' \| 'error' \| 'info'` | `'default'` | Color variant |
| `children` | `React.ReactNode` | required | Badge content |

## StatusBadge

```tsx
import { StatusBadge } from '@borne-map/ui'

<StatusBadge variant="available" showDot>Available</StatusBadge>
```

**Props**:
| Prop | Type | Default | Description |
|------|------|---------|-------------|
| `variant` | `'available' \| 'in-use' \| 'maintenance' \| 'offline'` | required | Status variant |
| `state` | `'default' \| 'animating'` | `'default'` | Animation state |
| `showDot` | `boolean` | `true` | Show color dot indicator |
| `children` | `React.ReactNode` | — | Label text |

**Accessibility**: role="status", non-color dot indicator.

## Skeleton

```tsx
import { Skeleton } from '@borne-map/ui'

<Skeleton type="text" width="300px" />
<Skeleton type="block" width={200} height={40} animated />
```

**Props**:
| Prop | Type | Default | Description |
|------|------|---------|-------------|
| `type` | `'block' \| 'text' \| 'circular'` | required | Shape |
| `width` | `number \| string` | `'100%'` | Width |
| `height` | `number \| string` | auto | Height |
| `animated` | `boolean` | `true` | Shimmer animation |

**Accessibility**: aria-busy="true", aria-label="Loading".

## EmptyState

```tsx
import { EmptyState } from '@borne-map/ui'

<EmptyState
  title="No results"
  description="Try adjusting your filters"
  action={{ label: "Clear", onClick: handleClear }}
/>
```

**Props**:
| Prop | Type | Default | Description |
|------|------|---------|-------------|
| `icon` | `React.ReactNode` | — | Optional icon |
| `title` | `string` | required | Heading |
| `description` | `string` | — | Subtext |
| `action` | `{ label: string; onClick: () => void }` | — | Call-to-action |

## ErrorState

```tsx
import { ErrorState } from '@borne-map/ui'

<ErrorState
  title="Something went wrong"
  retry={handleRetry}
/>
```

**Props**:
| Prop | Type | Default | Description |
|------|------|---------|-------------|
| `icon` | `React.ReactNode` | — | Optional icon |
| `title` | `string` | required | Error heading |
| `description` | `string` | — | Error details |
| `retry` | `() => void` | — | Retry handler |

**Accessibility**: role="alert".

## Toast

```tsx
import { Toast } from '@borne-map/ui'

<Toast
  variant="success"
  title="Saved!"
  message="Your changes are saved"
  onClose={handleClose}
/>
```

**Props**:
| Prop | Type | Default | Description |
|------|------|---------|-------------|
| `variant` | `'success' \| 'error' \| 'warning' \| 'info'` | `'info'` | Type |
| `title` | `string` | required | Heading |
| `message` | `string` | — | Body text |
| `duration` | `number` | `5000` | Auto-dismiss ms (0=no auto) |
| `onClose` | `() => void` | — | Close callback |
| `showCloseButton` | `boolean` | `true` | Show close × |

**Accessibility**: role="alert".

## Modal

```tsx
import { Modal } from '@borne-map/ui'

<Modal isOpen={isOpen} onClose={() => setIsOpen(false)} size="md" title="Confirm">
  <p>Are you sure?</p>
</Modal>
```

**Props**:
| Prop | Type | Default | Description |
|------|------|---------|-------------|
| `size` | `'sm' \| 'md' \| 'lg'` | `'md'` | Width |
| `title` | `string` | — | Optional heading |
| `isOpen` | `boolean` | required | Visibility |
| `onClose` | `() => void` | required | Close handler |
| `children` | `React.ReactNode` | required | Content |

**Accessibility**: role="dialog", aria-modal, focus trap, Escape to close, overlay click to close.

## Table

```tsx
import { Table } from '@borne-map/ui'

<Table
  columns={[{ key: 'name', label: 'Name', sortable: true }]}
  data={[{ name: 'John' }]}
  rowActions={[{ label: 'Edit', icon: <EditIcon /> }]}
  onRowAction={handleAction}
/>
```

**Props**:
| Prop | Type | Default | Description |
|------|------|---------|-------------|
| `columns` | `Array<{ key: string; label: string; sortable?: boolean; width?: string\|number }>` | required | Column definitions |
| `data` | `Record<string, any>[]` | required | Row data |
| `onRowAction` | `(action: string, rowData: any) => void` | — | Action click |
| `rowActions` | `Array<{ label: string; icon: React.ReactNode }>` | — | Row action buttons |

**Accessibility**: role="table", aria-sort for sortable columns.

## StatCard

```tsx
import { StatCard } from '@borne-map/ui'

<StatCard label="Total" value="124" trend={{ value: 12, positive: true }} icon={<Icon />} />
```

**Props**:
| Prop | Type | Default | Description |
|------|------|---------|-------------|
| `label` | `string` | required | Metric name |
| `value` | `string \| number` | required | Metric value |
| `trend` | `{ value: number; positive?: boolean }` | — | Trend indicator |
| `icon` | `React.ReactNode` | — | Icon |

## DataCard

```tsx
import { DataCard } from '@borne-map/ui'

<DataCard title="Details" action={{ label: "Edit", onClick: handleEdit }}>
  <p>Content</p>
</DataCard>
```

**Props**:
| Prop | Type | Default | Description |
|------|------|---------|-------------|
| `title` | `string` | — | Card title |
| `action` | `{ label: string; onClick: () => void }` | — | Header action |
| `children` | `React.ReactNode` | required | Card body |
