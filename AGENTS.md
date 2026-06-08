# AGENTS.md

Guide for OpenCode agents working in BorneMap.

## Project Status

**Fresh project** – no production code, architecture, or established conventions yet. This file documents discovered patterns as the project grows.

## Installed Skills

- **impeccable**: Design, UI, frontend, or interface work. Use sub-commands like `craft`, `shape`, `audit`, or `polish`.
- **frontend-design**: Build production-grade web components, pages, and layouts.
- **rust-best-practices**: Guidance for idiomatic Rust code, ownership patterns, error handling, and performance.
- **git-guardrails-claude-code**: Set up safety hooks to block dangerous git operations.
- **find-skills**: Discover and install additional agent skills.

## OpenCode Best Practices

- Use `TodoWrite` to track multi-step tasks (breaking them into smaller steps as needed).
- Mark todos `in_progress` before working and `completed` immediately after finishing each step.
- Prefer reading existing files with `Read` over guessing with `Bash` commands.
- Use `Task` agents for open-ended codebase exploration (`explore` agent) or complex multi-step research (`general` agent).
- Verify solutions with executable commands (tests, type checking, builds) rather than prose claims.
- When in doubt, inspect config files and scripts (sources of truth) before guessing at conventions.
