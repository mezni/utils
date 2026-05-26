# BorneMap — Defensive UX Guardrails

## 1. Horizontal Grid Protection

To safeguard data column tables containing deep relational keys and coordinates from squishing or horizontal word-breaking, all data matrices must be wrapped inside a custom horizontal scrolling element.

### Component: `<ScrollableTable/>`

- **Location**: `sources/frontend/packages/ui/src/components/ui/scrollable-table.tsx`
- **Enforced minimum content width**: `800px`
- **Behavior**: When viewport width is less than content width, horizontal scrollbar appears. Content never wraps or breaks.

### Rationale

Relational key columns (e.g., `STN-4f7d2a8b9c02`, `PRT-z5x3n1v9p4q7`) and coordinate pairs must remain fully readable without truncation. Standard responsive table patterns that compress columns make these identifiers unreadable and break user workflows.

## 2. Destructive Safety Verification

Destructive operations are **barred from running on simple click events**. The action must load a confirmation modal overlay requiring the operator to manually type the matching resource prefix code string before unlocking the execution wrapper button.

### Confirmation Flow

```
1. User clicks "Delete" action on a resource (e.g., station STN-4f7d2a8b9c02)
2. Modal overlay appears with:
   - Warning message describing the destructive action
   - Input field requiring the user to type: STN-4f7d2a8b9c02
   - Disabled "Confirm Delete" button
3. User types the resource ID into the input field
4. If input matches the resource ID exactly → "Confirm Delete" button becomes enabled
5. If input does not match → button remains disabled
6. User clicks "Confirm Delete" → action executes
```

### Match Rule

The typed string must **exactly match** the full prefixed identifier (e.g., `STN-4f7d2a8b9c02`), not just the prefix or a substring.

### Applicable Operations

- Delete station
- Delete charger
- Delete partner profile
- Delete connector type
- Any other irreversible data removal action
