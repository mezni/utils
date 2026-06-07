# ADR-011: React + Vite for Web Applications

**Status**: Accepted
**Date**: 2026-06-07

## Context

Web frontend applications (Driver Web and Dashboard) need a framework and build tool. Options: Next.js, Remix, Create React App, Vite + React, SvelteKit, etc.

## Decision

Use React 18 with Vite 5 for web applications.

## Rationale

- Vite provides instant dev server startup and fast HMR
- React is the team's preferred frontend framework
- Tailwind CSS integration is straightforward with Vite
- No server-side rendering needed (static SPA is sufficient for this use case)
- Lighter than Next.js — no Node.js server in production
- Vite's proxy configuration simplifies development (proxy /api to backend services)

## Consequences

- Static files served via Traefik or a simple static file server
- No SSR — initial load may be slower than server-rendered alternatives
- Client-side routing with react-router-dom

## Compliance

- Both Driver Web and Dashboard use the same framework
- Design tokens consumed via Tailwind CSS configuration from packages/ui
- No hardcoded visual values
