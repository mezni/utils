# Out of Scope Registry — BorneMap

**Version:** 1.0
**Last updated:** 2026-06-09

---

The following features are permanently deferred. They are not included in any current or planned MVP. Introducing any of these requires an approved ADR and a formal plan revision.

## OCPP and Charging Sessions

- OCPP 1.6 / 2.0 integration
- Charging session management (start, stop, meter values)
- Real-time charger status via OCPP WebSocket
- Firmware updates for charging hardware
- Transaction data (kWh delivered, session duration, cost)

## Payments and Billing

- Credit/debit card payment processing
- Mobile money (e.g., Orange Money, Flooz)
- Subscription or membership plans
- Per-session or per-kWh billing
- Invoice generation
- Payment gateway integration of any kind
- Pricing management

## Routing and Navigation

- Turn-by-turn navigation to stations
- Route optimization
- Multi-stop trip planning
- Real-time traffic integration
- EV range-based routing
- Integration with Google Maps, Waze, or similar navigation SDKs

## Real-Time Availability (OCPP-driven)

- Live charger status via OCPP
- Real-time connector availability
- Occupancy detection
- WebSocket-based status updates

## Push Notifications

- Mobile push notifications (FCM/APNs)
- Email notifications
- SMS notifications
- In-app notification center
- Station status change alerts

---

## Rationale

These features are excluded because:

1. **OCPP and real-time availability** require hardware integration and certification, adding significant complexity and operational burden incompatible with MVP-first delivery.

2. **Payments and billing** introduce financial regulation, PCI compliance, and fraud prevention requirements that are disproportionate for the discovery-focused platform.

3. **Routing and navigation** are well-served by mature third-party applications (Google Maps, Apple Maps, Waze). A dedicated implementation would provide marginal value.

4. **Push notifications** require persistent infrastructure (FCM/APNs), notification token management, and user opt-in flows that add complexity before the core discovery loop is validated.

## Reconsideration Process

To reintroduce any of these features:

1. File an ADR describing the scope, cost, and rationale
2. Update the implementation plan with the new MVP or sprint
3. Obtain approval per the standard project governance process

The ADR must explicitly address why the feature was deferred and what has changed to warrant reconsideration.
