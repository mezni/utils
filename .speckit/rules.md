# BorneMap SpecKit Architectural Guardrails

## Core Mandates
1. You MUST read and adhere to `docs/constitution.md` before generating code.
2. Any introduction of event streaming (Kafka/RabbitMQ), payment gateways, or unauthorized microservices will fail compliance automatically.
3. Every Rust database query to `platform_db` MUST use compile-time type-checked `sqlx` macros. No raw unvalidated string concatenation.
4. TypeScript strict mode is non-negotiable. The `any` keyword is strictly prohibited.

## UX/UI Pro Max Compliance
- Every frontend UI component must include explicit states for: Loading (Skeletons), Error Handling (Fallback boundaries), and Empty Data States.
- Responsive tailwind layouts and aria-labels for accessibility are required for all interactive items.

## Documentation & State Compliance
- Before completing a task, you MUST update `docs/roadmap_status.md`, `docs/sprint_backlog.md`, and `docs/system_state.md`.
