# ADR-011: React + Vite for web applications

**Status:** Accepted
**Date:** 2026-06-09

## Context

Web applications (Driver Web, Dashboard) need a modern frontend framework and build tool. The framework must support TypeScript, Tailwind CSS, fast development iteration, and shared design tokens.

## Decision

Use React with Vite for all web applications. TypeScript is required. Tailwind CSS is the styling approach. Vite provides fast HMR and optimized production builds.

## Consequences

- Fast development iteration with Vite HMR
- TypeScript provides type safety
- Tailwind integrates naturally with the design token system
- Shared token configuration via Tailwind presets
