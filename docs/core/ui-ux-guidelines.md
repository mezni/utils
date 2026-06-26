# UI/UX Pro Max Design Guide

This document defines the interface standards, interaction behaviors, and accessibility rules for the project. Every component built must align with these "Pro Max" guidelines.

## 1. Design Tokens & Visual Hierarchy
* **Color Palette:** Maintain a strict semantic color system (Primary, Secondary, Success, Warning, Danger, Info). Ensure consistency across dark and light modes.
* **Typography:** Use a scale-based type hierarchy to establish a clear reading order. Maintain a vertical grid for text layout.
* **Spacing & Grid:** Utilize a consistent layout system (e.g., 4px/8px grid) for margins, padding, and alignment to keep the UI perfectly balanced.

## 2. Component Guidelines & Layout
* **Responsiveness:** Build mobile-first. UI must scale elegantly from smart devices (320px) to ultra-wide desktops.
* **State Consistency:** Interactive elements (buttons, inputs, links) must feature distinct, smooth transitions for:
  * Default / Idle
  * `:hover`
  * `:focus` (highly visible focus rings for keyboard navigation)
  * `:disabled` (clear visual lock, changing cursor to `not-allowed`)

## 3. Asynchronous Feedback Loops
User actions must never leave the interface hanging. Every asynchronous request requires visual feedback:
* **Initial Load:** Use **Skeleton Loaders** matching the structural layout of incoming data rather than generic spinning wheels.
* **Action Submission:** Show a loading state directly on the button (e.g., spinner replaces text, button is disabled) to prevent double-submissions.
* **Success Feedback:** Confirm successful state changes with contextual UI updates or automated **Success Toasts**.
* **Error Transitions:** Gracefully handle failures using local **Error Boundaries**. Never crash the page. Inform the user what went wrong and provide a recovery action (e.g., a "Retry" button).

## 4. Accessibility (WCAG 2.1 AA Compliance)
* **Contrast Ratios:** Text and interactive elements must maintain a minimum contrast ratio of 4.5:1 against their background (3:1 for large text).
* **Keyboard Navigation:** The entire application must be fully navigable using the `Tab`, `Enter`, `Space`, and Arrow keys. Never trap keyboard focus.
* **Semantic HTML & ARIA:** Use meaningful tags (`<main>`, `<nav>`, `<button>`, `<article>`). Provide explicit `aria-label`, `aria-expanded`, and `aria-live` attributes where standard HTML elements fall short.

## 5. Form UX & Validation
* **Inline Validation:** Validate input fields as the user types or leaves the field (`onBlur`), rather than waiting for them to hit submit.
* **Clear Error Messaging:** Error text must explain exactly *how* to fix the issue, placed directly beneath the offending input box.
* **Preserve Input Data:** Never wipe out user form inputs upon a failed submission attempt.