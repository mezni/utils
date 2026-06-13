# Design System Overview

## Version: 1.0
## Status: Active
## Core Philosophy: Documentation is the system. Code is just its execution.

---

## 🎯 DESIGN PURPOSE

**BorneMap Design is not appearance. It is system behavior under interaction.**

---

## 📘 DEFINITION OF DESIGN

In BorneMap, "design" means:

### 1. **UX is Behavior, Not Visuals**

- **UX** = How the system responds to user actions
- **Design** = The interaction patterns and behaviors
- **Visuals** = The presentation layer (handled by design tokens)

### 2. **Map-First Interaction Model**

- **Map is the primary UI element**
- All discovery is map-based
- All actions are geospatial
- Map controls all UI flows

### 3. **Mobile-First Prioritization**

- Primary UX experience on mobile
- Desktop web as secondary platform
- Touch interactions optimized for mobile
- Responsive web design supports desktop

### 4. **Performance as UX Constraint**

- **Performance is part of UX design**
- Map interactions must feel instant
- No UI freezing during data loading
- Smooth animations (60fps target)

---

## 🏗️ DESIGN PHILOSOPHY

### Core Principles

1. **Design is systematic behavior**
   - Every interaction is designed
   - Every state transition is designed
   - Every error is designed

2. **Map is the control center**
   - Map controls discovery
   - Map controls interaction
   - Map controls navigation

3. **Consistency over variety**
   - Same patterns across platforms
   - Same behaviors across screens
   - Same feedback across states

4. **Progressive disclosure**
   - Show essentials first
   - Reveal details on demand
   - Keep map always visible

---

## 📁 DOCUMENTATION STRUCTURE

The design system is organized into 11 files:

1. **[Design System](./01_design-system.md)** - UI tokens and styling rules
2. **[UX Principles](./02_ux-principles.md)** - Interaction principles
3. **[Mobile Patterns](./03_mobile-patterns.md)** - Mobile-specific behaviors
4. **[Web Patterns](./04_web-patterns.md)** - Web-specific behaviors
5. **[Map Interactions](./05_map-interactions.md)** - Map behavior contract (CRITICAL)
6. **[Motion System](./06_motion-system.md)** - Animation rules
7. **[States](./07_states.md)** - State architecture
8. **[Empty/Error States](./08_empty-error-states.md)** - Failure UX
9. **[Component Guidelines](./09_component-guidelines.md)** - Component building rules
10. **[Accessibility](./10_accessibility.md)** - Accessibility requirements

---

## 🎨 DESIGN LAYER ORGANIZATION

```
Design System
├── Design Tokens (@bm/design-tokens)
│   ├── Colors
│   ├── Typography
│   ├── Spacing
│   └── Components
│
├── Interaction Design
│   ├── UX Principles
│   ├── Map Interactions
│   └── Pattern Library
│
└── Implementation Guidelines
    ├── Component Guidelines
    ├── State Architecture
    └── Accessibility
```

---

## 🚫 NON-NEGOTIABLE RULES

### Hard Constraints

1. **No hardcoded styling**
   - All colors, spacing, typography from design tokens
   - No inline styles (except layout glue)
   - No custom CSS/SCSS in components

2. **No UI logic inside API layer**
   - API layer only handles data
   - UI logic in React components or hooks
   - Separation of concerns

3. **No map logic outside MapContainer**
   - All map rendering through MapContainer abstraction
   - No platform-specific map logic in UI components
   - No duplicated map behavior

4. **No inconsistent interaction patterns across apps**
   - Same behaviors in mobile and web
   - Same feedback patterns
   - Same state transitions

---

## 🧠 CORE DESIGN PRINCIPLE

**Design is not appearance. It is system behavior under interaction.**

---

## 🎯 DESIGN OBJECTIVES

### For MVP-1 (Discovery Core)

**Primary Goals:**
- [ ] Seamless map-based discovery
- [ ] Instant station selection
- [ ] Smooth location-based updates
- [ ] Professional failure handling

**Secondary Goals:**
- [ ] Consistent across platforms
- [ ] Mobile-first performance
- [ ] Accessible to all users
- [ ] Extensible for future features

---

## 📊 DESIGN QUALITY METRICS

### Performance Metrics

- [ ] Map interactions < 100ms perceived
- [ ] Transitions 150-300ms smooth
- [ ] No UI blocking during operations
- [ ] 60fps during animations

### UX Metrics

- [ ] No confusion in interactions
- [ ] Clear feedback for all actions
- [ ] No dead ends in flows
- [ ] Smooth state transitions

### Accessibility Metrics

- [ ] Keyboard accessible on web
- [ ] Color contrast WCAG AA
- [ ] Touch targets ≥ 44px
- [ ] Screen reader support

---

## 🔄 DESIGN EVOLUTION

### MVP-1 Foundation

**Design System:**
- Design tokens established
- Basic interaction patterns defined
- Map behavior contract set
- Error handling patterns defined

### Future Extensions

**MVP-2+:**
- Admin UI patterns
- Data management patterns
- Partner dashboard patterns

**MVP-3+:**
- Auth flow patterns
- User management patterns
- Profile customization patterns

---

*This overview establishes design as systematic behavior, not just appearance.*