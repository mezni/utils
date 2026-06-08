# Out-of-Scope Registry

Features explicitly deferred indefinitely. These will never be added without a full constitution revision and new implementation plan.

## Deferred Features

### OCPP and Charging Sessions

**Feature**: Open Charge Point Protocol support. Real-time charging state, session management, power delivery control.

**Why deferred**: Requires integration with physical charging hardware, complex state machines, and real-time communication. MVP-1 through MVP-6 focus on station discovery and availability management only. Charging sessions are out of scope until the core platform is proven.

**Cost of adding later**: Breaking change to the database schema, addition of new services (OCPP gateway), new UI screens for session management. New implementation plan required.

### Payments and Billing

**Feature**: Charge users for electricity, subscription models, invoicing.

**Why deferred**: Introduces PCI compliance, payment processor integration, tax calculation, and regulatory complexity. Revenue model is not yet validated. MVP-1 focuses on discovery, not transactions.

**Cost of adding later**: New schema (transactions, subscriptions, invoicing), new service (billing engine), integration with payment gateway, new Dashboard screens, new legal framework. New implementation plan required.

### Routing and Navigation

**Feature**: Turn-by-turn directions to selected station, integration with native maps.

**Why deferred**: Complex algorithm, licensing costs for routing engines, offline data requirements. Driver app already provides map and station discovery. Navigation beyond "directions to this address" is not core to MVP-1.

**Cost of adding later**: New service for route optimization, licensing agreement with mapping provider, new frontend screens with real-time tracking, offline data management. New implementation plan required.

### Real-Time Availability

**Feature**: Immediate, live charger availability status via OCPP polling.

**Why deferred**: Requires OCPP implementation, polling infrastructure, and real-time messaging. MVP-1 supports manual availability updates. Real-time availability is dependent on OCPP, which is itself deferred.

**Cost of adding later**: Full OCPP integration, WebSocket server, real-time frontend state management, subscription to charging hardware feeds. New implementation plan required.

### Push Notifications

**Feature**: Notify users about station availability changes, favorites updates, review replies.

**Why deferred**: Requires push gateway (APNs, FCM), notification scheduling, opt-in management. Not essential for MVP-1 validation. User engagement can be validated through direct app usage.

**Cost of adding later**: Push notification service, frontend subscription management, analytics for notification effectiveness, infrastructure for scheduling. New implementation plan required.

## How to Propose a Deferred Feature

If a feature not in the above list needs to be deferred:

1. Document it in this file with the same template: Feature name, Why deferred, Cost of adding later.
2. Record a decision in `docs/project/decisions.md` with the sprint and rationale.
3. If the decision affects the overall roadmap, create an ADR.

## How to Activate a Deferred Feature

If a deferred feature becomes essential:

1. Create a new ADR in `docs/adr/` explaining why the feature is no longer deferred.
2. Write a new implementation plan amendment with sprints and done criteria.
3. Review with the team and update the constitution if needed.
4. Update this registry to reflect the new status.

**No deferred feature is ever added incrementally. It requires a full plan revision.**
