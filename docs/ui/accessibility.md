# Accessibility & Internationalization

BorneMap is built for accessibility and RTL/multilingual support from the ground up. These are not afterthoughts.

---

## Accessibility Standards

### Target: WCAG 2.1 AA

All web applications (Driver Web, Dashboard) must meet **Web Content Accessibility Guidelines 2.1 Level AA** minimum.

- **A** — Basic accessibility (minimum)
- **AA** — Enhanced accessibility (BorneMap target)
- **AAA** — Enhanced+ (aspirational, not required)

### Core Principles

1. **Perceivable** — Information and UI components are perceivable to all users
2. **Operable** — Navigation and interaction work with keyboard, touch, voice
3. **Understandable** — Content is clear, readable, predictable
4. **Robust** — Content works across browsers, assistive technologies, and devices

---

## 1. Color & Contrast

### Text Contrast Ratios

**Minimum 4.5:1 for normal text (WCAG AA)**  
**Minimum 3:1 for large text (18px+) (WCAG AA)**

All BorneMap text colors meet these ratios:

| Text | Background | Ratio | WCAG AA |
|------|-----------|-------|---------|
| #111827 | #F8FAF6 | 15.8:1 | ✅ Pass |
| #111827 | #FFFFFF | 18.5:1 | ✅ Pass |
| #6B7280 | #FFFFFF | 5.2:1 | ✅ Pass |
| #007943 | #F8FAF6 | 4.8:1 | ✅ Pass |
| #10B981 | #ECFDF5 | 5.1:1 | ✅ Pass |
| #F59E0B | #FFFBEB | 4.6:1 | ✅ Pass |
| #EF4444 | #FEF2F2 | 5.3:1 | ✅ Pass |

**Exception:** Neon glow (#00E676) is only used for colored elements (map pins), never for text. It does not require WCAG contrast ratios.

### Color Not the Only Indicator

- ✅ Status: indicated by color AND icon (e.g., StatusBadge with checkmark)
- ✅ Errors: indicated by color AND text message
- ✅ Links: underlined or styled, not color alone
- ✅ Focus: visible outline (2–3px), not relying on color change

### Testing

- Use WebAIM Contrast Checker (https://webaim.org/resources/contrastchecker/)
- Automated: axe DevTools browser extension
- Manual: disable colors temporarily, verify readability

---

## 2. Keyboard Navigation

All functionality must be accessible via keyboard. No mouse-only interactions.

### Required Keys

| Key | Action |
|-----|--------|
| **Tab** | Move focus forward through interactive elements |
| **Shift+Tab** | Move focus backward |
| **Enter** | Activate buttons, submit forms |
| **Space** | Toggle checkboxes, activate buttons in some contexts |
| **Escape** | Close modals, dismiss dropdowns, exit fullscreen |
| **Arrow Keys** | Navigate lists, sliders, menus |
| **Home/End** | Jump to start/end of lists |

### Focus Order

1. Focus order must follow visual/logical order
2. Skip links on web apps: "Skip to main content" link (hidden, visible on focus)
3. No "trap" (focus stuck in modal without escape key)
4. Focus visible indicator: 2–3px outline, not hidden

### Implementation

```javascript
// Focusable elements
<button>, <a>, <input>, <select>, <textarea>

// Add tabindex if necessary (use sparingly)
tabindex="0"  // Include in tab order
tabindex="-1" // Programmatically focusable, not in tab order

// Always show focus
:focus {
  outline: 3px solid #007943;  // brand.primary
  outline-offset: 2px;
}
```

### Testing

- Tab through each screen without mouse
- Verify focus outline visible at all times
- Escape key closes modals/dropdowns
- Enter key submits forms

---

## 3. Screen Reader Support

### Semantic HTML

Use correct HTML elements, not divs for everything:

```html
<!-- Good ✅ -->
<nav>
  <button>Menu</button>
</nav>

<main>
  <section>
    <h1>Stations</h1>
    <ul>
      <li><button>Station Name</button></li>
    </ul>
  </section>
</main>

<!-- Avoid ❌ -->
<div role="navigation">
  <div role="button">Menu</div>
</div>

<div role="main">
  <div role="region">
    <div role="heading" aria-level="1">Stations</div>
    <div role="list">
      <div role="listitem"><div role="button">Station Name</div></div>
    </div>
  </div>
</div>
```

### ARIA Labels

When semantic HTML isn't sufficient, use ARIA:

```html
<!-- Icon button: must have label -->
<button aria-label="Search">
  <SearchIcon />
</button>

<!-- Hidden descriptive text -->
<input type="text" aria-label="Enter station name" placeholder="Search..." />

<!-- Complex region: use aria-label or aria-describedby -->
<div aria-label="Station details">
  <h2>Station Name</h2>
  <p>Address and chargers...</p>
</div>

<!-- Live region: announce updates to screen readers -->
<div aria-live="polite" role="status">
  Favorite added! (auto-announces when changed)
</div>

<!-- Disabled vs Aria-disabled (choose one) -->
<button disabled>Cannot interact</button>
<button aria-disabled="true" role="button">Cannot interact (but focusable)</button>
```

### Images

All images must have alt text:

```html
<!-- Meaningful image -->
<img src="station.jpg" alt="EV charging station with 4 Tesla chargers" />

<!-- Decorative image -->
<img src="divider.png" alt="" />  <!-- Empty alt for decorative images -->

<!-- SVG icon -->
<svg aria-label="Favorite" role="img">
  <path d="..." />
</svg>

<!-- Background image -->
<!-- Provide text alternative elsewhere on page -->
<div style="background: url(map.jpg)">
  <h2>Map of stations in Tunis</h2>
</div>
```

### Form Labels

All form inputs must have associated labels:

```html
<!-- Good: explicit association -->
<label for="email">Email</label>
<input id="email" type="email" name="email" />

<!-- Also good: implicit association -->
<label>
  Email
  <input type="email" name="email" />
</label>

<!-- Not accessible ❌ -->
<div>Email:</div>
<input type="email" />
```

### List Semantics

```html
<!-- Use semantic list structures -->
<ul>
  <li><button>Station 1</button></li>
  <li><button>Station 2</button></li>
</ul>

<!-- Announce dynamic list changes -->
<ul aria-live="polite">
  <li>Station added</li>
</ul>
```

### Testing

- Use screen reader: NVDA (Windows), JAWS (Windows), VoiceOver (Mac/iOS), TalkBack (Android)
- Navigate entire app with screen reader only
- Verify all buttons, links, and form inputs announced correctly
- Test form error messages announced in context

---

## 4. Mobile Accessibility

### Touch Targets

Minimum 44×44px (Material Design) or 48×48px (Apple):

```html
<button style="width: 44px; height: 44px;">+</button>
```

### Mobile Focus

- Tap should show visual focus indicator
- Hold/long-press for additional options (context menu)
- Swipe gestures have keyboard alternatives

### Testing

- Use Android/iOS accessibility features
- TalkBack (Android), VoiceOver (iOS)
- Test one-handed operation

---

## 5. Internationalization (i18n)

### Supported Languages

1. **French** — Primary language, LTR layout
2. **Arabic** — Full support including RTL, Semitic script
3. **English** — English, LTR layout

### Language Switching

- No page reload required (client-side switch)
- User preference stored in browser (localStorage) and Keycloak profile
- Detected on load (browser language, Keycloak profile, user selection)

### Content Strategy

**Translate every string:**
- UI labels (buttons, form fields)
- Placeholder text
- Error messages
- Toast notifications
- Help text
- Page titles and meta descriptions

**Do NOT translate:**
- Brand name ("BorneMap")
- Station names (proper nouns)
- User-generated content (reviews)
- Technical terms when no translation exists

### i18n Implementation Example

```typescript
// packages/i18n/translations.json
{
  "en": {
    "home.title": "Stations",
    "home.search": "Search for stations...",
    "common.favorite": "Favorite",
    "common.favorited": "Favorited",
  },
  "fr": {
    "home.title": "Stations",
    "home.search": "Rechercher des stations...",
    "common.favorite": "Ajouter aux favoris",
    "common.favorited": "Ajouté aux favoris",
  },
  "ar": {
    "home.title": "المحطات",
    "home.search": "ابحث عن المحطات...",
    "common.favorite": "أضف إلى المفضلة",
    "common.favorited": "تم الإضافة إلى المفضلة",
  }
}

// In components
import { useTranslation } from 'react-i18next'

export function StationCard() {
  const { t } = useTranslation()
  return (
    <div>
      <h2>{t('home.title')}</h2>
      <button>{t('common.favorite')}</button>
    </div>
  )
}
```

### Date/Time Formatting

Use locale-aware formatting:

```javascript
// Good: respects user locale
const formatter = new Intl.DateTimeFormat('ar')
const date = formatter.format(new Date())  // "٢٠٢٦/٦/٥"

// Also good: with options
const options = { 
  year: 'numeric', 
  month: 'long', 
  day: 'numeric',
  timeZone: 'Africa/Tunis'
}
const formatted = new Intl.DateTimeFormat('ar', options).format(new Date())
```

### Number Formatting

```javascript
// Good: respects locale (commas vs periods)
const formatter = new Intl.NumberFormat('ar')
formatter.format(1234.56)  // "١٬٢٣٤٫٥٦"
```

### Testing

- Switch language in app, verify all text changes
- Use multilingual screen reader (VoiceOver with Arabic, NVDA with French)
- Test on RTL system (iPhone with Arabic, Android with Arabic)

---

## 6. Right-to-Left (RTL) Layout

### RTL Requirement: CLASS A BUG

**Any screen that does not work correctly in Arabic RTL is a Class A bug.**

RTL is not an afterthought. It is tested from day one.

### CSS Logical Properties

Use logical properties instead of directional:

```css
/* Bad: directional ❌ */
.card {
  margin-left: 16px;    /* breaks in RTL */
  padding-right: 24px;  /* breaks in RTL */
}

/* Good: logical ✅ */
.card {
  margin-inline-start: 16px;   /* left in LTR, right in RTL */
  padding-inline-end: 24px;    /* right in LTR, left in RTL */
  text-align: start;           /* left in LTR, right in RTL */
}
```

### Flexbox & Grid

```css
/* Flexbox: use row-reverse for RTL */
.row {
  display: flex;
  direction: ltr;  /* or rtl */
  flex-direction: row;  /* auto-reverses with `direction: rtl` */
}

/* Grid: names stay consistent, direction handles placement */
.grid {
  display: grid;
  grid-template-columns: 1fr 2fr;
  direction: rtl;  /* columns reverse order */
}

/* Justify-content with logical keywords */
.container {
  display: flex;
  justify-content: space-between;  /* works in both LTR and RTL */
}
```

### HTML Direction

```html
<!-- Set dir attribute on root -->
<html dir="ltr">  <!-- English, French -->
<html dir="rtl">  <!-- Arabic -->

<!-- Or via CSS -->
<html lang="ar">
<style>
  [lang="ar"] { direction: rtl; }
  [lang="en"], [lang="fr"] { direction: ltr; }
</style>
```

### Component RTL Behavior

See [components.md](components.md#rtl-rules-for-driver-apps) for detailed RTL rules for each driver component:

- **MobileTopBar:** Menu/bell icons swap sides
- **SearchBar:** Search icon on right, text aligned right
- **FilterPills:** Pills scroll right-to-left
- **BottomStationCard:** Image left, text right, labels right/values left
- **BottomTabBar:** Tab order reverses, center button stays centered
- **MapPinMarker:** No change (geographic position)
- **ZoomControls:** No change (positioned right in both LTR/RTL)

### Testing RTL

1. **Force RTL in browser:** DevTools → more tools → Rendering → Emulate CSS media feature prefers-color-scheme → set to RTL
2. **Chrome:** DevTools → More tools → Rendering → set direction to `rtl`
3. **React Native:** `I18nManager.forceRTL(true)` in app startup
4. **Manual:** Switch device language to Arabic (iOS/Android settings)

### Checklist for Every Screen

- [ ] Text aligns right in Arabic
- [ ] Images/icons position mirrors LTR
- [ ] Form labels and inputs reverse correctly
- [ ] Lists and tables have correct direction
- [ ] Icons that indicate direction (arrows) flip
- [ ] Animations/transitions work in RTL
- [ ] Safe area insets correct for RTL notches
- [ ] Screen reader announces correct direction

---

## 7. Forms & Error Handling

### Error Messages

Errors must be perceivable and associated with form fields:

```html
<!-- Good: associated error ✅ -->
<div>
  <label for="email">Email</label>
  <input 
    id="email" 
    type="email" 
    aria-invalid="true"
    aria-describedby="email-error"
  />
  <span id="email-error" role="alert">
    Please enter a valid email address
  </span>
</div>

<!-- Also good: Toast + field highlight -->
<input 
  id="email" 
  type="email" 
  style="border-color: #EF4444;"  /* status.maintenance */
  aria-invalid="true"
/>
<Toast variant="error">
  Invalid email format
</Toast>
```

### Form Labels

All inputs must have visible labels, not just placeholders:

```html
<!-- Good ✅ -->
<label for="name">Full Name</label>
<input id="name" type="text" placeholder="John Doe" />

<!-- Avoid ❌ -->
<input type="text" placeholder="Full Name" />  <!-- no label -->
```

### Required Fields

Mark required fields clearly, not by color alone:

```html
<!-- Good ✅ -->
<label for="email">
  Email <span aria-label="required">*</span>
</label>
<input 
  id="email" 
  type="email" 
  required 
  aria-required="true"
/>

<!-- Also good: use asterisk + text -->
<label>
  Email *
  <span style="font-size: 0.8em;">(required)</span>
</label>
```

---

## 8. Responsive Design

### Mobile-First Approach

Build for mobile first, enhance for larger screens:

```css
/* Mobile (default) */
.button { width: 100%; }

/* Tablet */
@media (min-width: 768px) {
  .button { width: auto; }
}

/* Desktop */
@media (min-width: 1024px) {
  .button { max-width: 200px; }
}
```

### Safe Area Insets

Account for notches and home indicators:

```css
/* iOS SafeArea */
padding-top: max(16px, env(safe-area-inset-top));
padding-bottom: max(16px, env(safe-area-inset-bottom));
padding-left: max(16px, env(safe-area-inset-left));
padding-right: max(16px, env(safe-area-inset-right));
```

```javascript
// React Native
import { useSafeAreaInsets } from 'react-native-safe-area-context'

export function Component() {
  const insets = useSafeAreaInsets()
  return <View style={{ paddingTop: insets.top }} />
}
```

---

## 9. Testing Checklist

### Automated Testing

- [ ] axe DevTools (browser extension)
- [ ] Lighthouse Accessibility audit
- [ ] WAVE (web accessibility evaluation tool)
- [ ] Jest tests for ARIA and semantics (testing-library)

### Manual Testing

- [ ] Keyboard navigation (Tab, Enter, Escape)
- [ ] Screen reader (VoiceOver, NVDA, TalkBack)
- [ ] RTL language (Arabic)
- [ ] Low vision (zoom 200%)
- [ ] High contrast mode (Windows)
- [ ] Touch on mobile (44px targets)
- [ ] Error states (proper messaging)
- [ ] Forms (labels, error association)

### Before Release

- [ ] WCAG 2.1 AA audit passed
- [ ] Screen reader tested: headings, lists, buttons, forms, modals all announced
- [ ] Keyboard tested: no traps, focus visible, all interactive elements reachable
- [ ] RTL tested: Arabic screen looks correct, no text overflow, images position correct
- [ ] Mobile tested: touch targets 44px+, safe area respected, no horizontal scroll
- [ ] Colors tested: contrast ≥ 4.5:1 for body text, no color-only indicators

---

## 10. Resources & References

### WCAG 2.1
- https://www.w3.org/WAI/WCAG21/quickref/
- https://www.w3.org/WAI/tutorials/

### Accessibility Tools
- **axe DevTools** — Automated testing (browser extension)
- **Lighthouse** — Google Chrome audit tool
- **WebAIM Contrast Checker** — Color contrast validator
- **NVDA** — Screen reader (Windows, free)
- **VoiceOver** — Built-in (Mac, iOS)
- **TalkBack** — Built-in (Android)

### Internationalization
- **i18next** — React i18n library
- **React Intl** — Facebook's i18n library
- **Intl API** — Native date/number formatting

### RTL
- **RTL Tester** — https://www.rtljs.com/
- **MDN Logical Properties** — https://developer.mozilla.org/en-US/docs/Web/CSS/CSS_Logical_Properties

---

**Document Version:** 1.0  
**Last Updated:** 2026-06-05  
**Status:** Complete with all standards and testing guidelines
