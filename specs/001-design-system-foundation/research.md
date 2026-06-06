# Research: Design System Foundation

## Research Questions & Decisions

### 1. TypeScript vs. JavaScript for Design Tokens

**Decision**: Use TypeScript with strict mode for design tokens

**Rationale**:
- Type safety prevents runtime errors from typos in token names or values
- TypeScript provides autocomplete and IDE support for developers consuming the tokens
- Enforces compile-time validation of design token exports
- Type definitions help generate documentation automatically

**Alternatives Considered**:
- JavaScript (no type safety, prone to typos)
- JavaScript with JSDoc (less type-safe, more verbose)

**Best Practices**: Use exported constants for token values with explicit types (strings for colors/spacing, numbers for numeric values).

---

### 2. Design Token Organization & Export Strategy

**Decision**: Separate token files by category (colors, typography, spacing, radius, shadows) with a central index for re-exports and a React Native specific file.

**Rationale**:
- Organized files make tokens easy to find and modify
- Central index provides single import path for all tokens
- Separate native.ts handles React Native StyleSheet compatibility (different value types: strings vs. numbers for spacing/shadows)

**Alternatives Considered**:
- Single file with all tokens (harder to navigate, less maintainable)
- Object-based tokens (loses TypeScript type inference, harder to validate)

**Best Practices**:
- Export tokens as constants: `export const brandPrimary = '#007943'`
- Use 4px base unit for spacing scale (0.25rem in Tailwind terms)
- Document token naming conventions in README
- Include both hex values and Tailwind classes in comments

---

### 3. Component Library Patterns & Testing Approach

**Decision**: Use component composition pattern with TypeScript interfaces for props. Unit test each component variant/state combination using Vitest + @testing-library/react.

**Rationale**:
- Composition enables flexible component building from primitives
- TypeScript interfaces provide type safety for props and variants
- Unit tests per variant/state ensure all states are tested (meeting FR-011)
- Vitest is fast, integrates with Vite, and provides good TypeScript support

**Alternatives Considered**:
- Class components (legacy, less flexible for composition)
- No testing (violates FR-011, risks regressions)
- Integration tests only (slower, harder to isolate failures)

**Best Practices**:
- Use functional components with React Hooks
- Extract reusable hooks for common logic (e.g., `useTokenStyles`)
- Document all props with TypeScript interfaces
- Test each variant/state combination (e.g., Button: primary/default variants, sm/md/lg sizes, all states)

---

### 4. Tailwind CSS Integration with Design Tokens

**Decision**: Extend Tailwind config to use token values as theme extensions, creating Tailwind classes that reference token values.

**Rationale**:
- Allows developers to use token-based styling in React components
- Consistent usage of tokens across web applications
- Tailwind utilities provide rapid styling without writing custom CSS

**Alternatives Considered**:
- No Tailwind integration (more CSS needed, harder to maintain)
- Direct CSS variables (complex, less predictable)

**Best Practices**:
- Create Tailwind config that exports token values as theme extensions
- Provide mapping from tokens to Tailwind classes (e.g., `text-paragraph` → uses typography token)
- Document Tailwind + token usage patterns in quickstart guide

---

### 5. React Native Compatibility for Design Tokens

**Decision**: Create a separate `native.ts` file that exports token values in React Native StyleSheet-compatible format (strings for colors, numbers for spacing/shadows).

**Rationale**:
- React Native requires different value types than web (numbers for spacing, not strings)
- Keeps token definitions clean without runtime conversion logic
- Native file is explicitly marked as React Native specific

**Alternatives Considered**:
- Runtime conversion in imports (pollutes imports with conversion logic)
- Single file with conditional exports (harder to maintain, confusing)

**Best Practices**:
- Export numbers for spacing: `export const spacing16 = 4` (4px)
- Export numbers for shadows: `export const shadowFloat = { elevation: 4 }`
- Export strings for colors: `export const brandPrimary = '#007943'`
- Use StyleSheet.flatten() to combine token values in components

---

### 6. Component Variant & State Management

**Decision**: Use TypeScript `variant` prop (union type) and explicit `state` prop (optional) to handle variants and states.

**Rationale**:
- TypeScript union types provide compile-time validation
- Clear separation between intentional variants (Button primary, Input error) and dynamic states (disabled, loading)
- Enables easy extensibility (add new variants without breaking existing code)

**Alternatives Considered**:
- Props-based variants (e.g., `variant="primary"`, `variant="secondary"` - less type-safe)
- Enum-based states (harder to extend, less flexible)

**Best Practices**:
- Define variant unions in types.ts: `type ButtonVariant = 'primary' | 'secondary' | 'ghost' | 'danger'`
- Define state unions in component files: `type ButtonState = 'default' | 'hover' | 'active' | 'disabled' | 'loading'`
- Use component-level types for specific props (e.g., InputVariant includes 'error', 'default', 'search')

---

### 7. Accessibility (WCAG 2.1 AA) for Components

**Decision**: Implement WCAG 2.1 AA compliance for all web components using proper ARIA labels, keyboard navigation, and color contrast.

**Rationale**:
- Constitution requires WCAG 2.1 AA for all web applications
- Accessibility is not optional - must work correctly in all states
- Proper ARIA labels and keyboard navigation are best practices

**Alternatives Considered**:
- Skip accessibility (violates constitution, Class A bug if broken)
- Only basic accessibility (insufficient, violates WCAG 2.1 AA)

**Best Practices**:
- All interactive elements must have keyboard focus indicators
- Color contrast ≥ 4.5:1 for normal text, ≥ 3:1 for large text
- Status badges must include non-color indicators (dot + text label)
- Use semantic HTML elements where possible
- Test with keyboard-only navigation

---

### 8. Documentation Strategy

**Decision**: Create comprehensive Markdown documentation with examples for tokens and components in `docs/ui/`. Use clear headings, prop tables, and code examples.

**Rationale**:
- Developers need to understand what's available and how to use it
- Documentation reduces support burden and accelerates onboarding
- Markdown is platform-agnostic, easy to maintain

**Alternatives Considered**:
- Inline code comments (harder to navigate, not searchable)
- No documentation (developer confusion, slower velocity)

**Best Practices**:
- Each component entry: description, props table, usage examples, accessibility notes
- Each token file: token list, values, usage patterns, conversion to Tailwind/classes
- Keep examples copy-pasteable and runnable
- Link to related components and tokens

---

### 9. Build Tooling & Quality Checks

**Decision**: Use pnpm for package management, TypeScript for type checking, ESLint for linting, Prettier for formatting. Implement pre-commit hooks if possible.

**Rationale**:
- pnpm is fast, disk-efficient, works well with monorepos
- TypeScript catches errors early (design token typos, component prop mismatches)
- ESLint + Prettier enforce consistent code style
- Build must pass with zero warnings (SC-004)

**Alternatives Considered**:
- npm (slower, less efficient for monorepos)
- No linting/formatting (code quality suffers, harder to maintain)
- CI/CD pre-commit hooks (deferred to implementation phase)

**Best Practices**:
- All token files must pass TypeScript strict mode
- All component tests must pass
- Build output must have zero warnings
- No hardcoded visual values (all from tokens)

---

### 10. Monorepo Integration

**Decision**: Place `packages/ui` as a separate package in the monorepo using pnpm workspaces. Apps consume from `packages/ui` via relative imports.

**Rationale**:
- Monorepo structure allows sharing code across all three applications
- pnpm workspaces handle dependencies and hoisting efficiently
- Consistent tooling and config across all packages

**Alternatives Considered**:
- Copy-paste components into each app (violates DRY, harder to maintain)
- Separate package registry (complex, unnecessary for internal library)

**Best Practices**:
- Define shared TypeScript config in root
- Use consistent ESLint/Prettier configs
- Document consuming app setup in quickstart.md

---

## Research Summary

**Key Technologies**:
- TypeScript 5.x (strict mode)
- React 18+
- Tailwind CSS
- React Native
- Vitest + @testing-library/react
- pnpm
- ESLint + Prettier

**Architecture Pattern**:
- Token package with separate files by category
- Central index for re-exports
- React Native specific file for StyleSheet compatibility
- Shared components using TypeScript interfaces for props
- Unit tests per variant/state combination

**Integration Strategy**:
- Apps import from `packages/ui` (e.g., `import { Button } from '@borne-map/ui'`)
- Tailwind config extends token values for web usage
- React Native imports from `native.ts` for mobile usage
- All visual values from tokens (hardcoding prohibited)

**Success Criteria Mapping**:
- SC-004 (zero build warnings) → pnpm build strict mode + ESLint
- SC-005 (100% test pass rate) → Vitest coverage of all variant/state combinations
- FR-014 (pnpm build passes) → Strict TypeScript + ESLint configuration
- FR-015 (pnpm test passes) → All component tests passing
- FR-016 (Tailwind resolves tokens) → Proper Tailwind config with token extensions
