# Failure Simulation

## Fake E-Commerce Server

> **Status:** Draft
> **Authority:** `.specify/memory/constitution.md` v1.1.1, *Explicit Business Rules* and
> *Determinism and Reproducibility*
> **Feature:** `specs/009-failure-simulation`

Failure simulation is a **first-class capability**, not a side effect. Its purpose is to let clients
and tests reproduce real-world failure modes on demand, deterministically, without patching code.

## Divergences from other documents

Reconciled against the constitution, `architecture.md`, `API.md`, `database.md`, `configuration.md`,
`generator.md`, and `testing.md`. Every conflict is recorded and resolved explicitly (constitution
§44).

| # | Divergence in the incoming draft | Resolution | Now in |
|---|---|---|---|
| S1 | §9, §16, §29, §41 use `FAKE_LATENCY_ENABLED` as a separate switch. | **Rejected** — not adopted (`G10`). `FAKE_FAILURE_ENABLED` is the single master switch for latency and failure alike. | §9, §16, §29, §41 |
| S2 | §9, §11, §36, §41, §43 use `FAKE_FAILURE_SEED`. | **Rejected** — not adopted (`G11`). `RANDOM_SEED` is the only seed. | §11, §31, §36, §41, §43 |
| S3 | §37, §46 use `POST /api/v1/carts/1/checkout` and `POST /carts/{id}/checkout`. | **Rejected for now** — the interim contract is `POST /api/v1/checkout` with `cart_id` in the body (A6, deferred to `specs/005-orders`). | §19, §37, §46 |
| S4 | §19 uses `POST /api/v1/payments/{id}/authorize` as a worked example. | **Flagged** — that route is an E19 proposal requiring sign-off in `specs/007-payments`. Kept as an example of *where* injection happens, not as a published endpoint. | §19 |
| S5 | §37 expects `error.code = PAYMENT_TIMEOUT`. | **Rejected** — a third name for a condition the taxonomy already names. Resolved in S6. | §37 |
| S6 | Scenario error codes: §27 implies `SIMULATED_FAILURE` for all injected failures, while `API.md` §12.1 publishes `PAYMENT_PROVIDER_ERROR` (502) and `PAYMENT_PROVIDER_TIMEOUT` (504). | **Resolved** — a scenario whose condition exists in the taxonomy uses the **specific** code; `SIMULATED_FAILURE` is the generic fallback for scenarios with no specific code. | §2, §5, §37 |
| S7 | §21 maps "provider unavailable" to `503`, but the published scenario table maps provider error to `502`. | **Resolved** — `502` is authoritative for a fake-provider error (`API.md` §13). `503` is a valid status in the taxonomy but **no current scenario uses it**; a scenario is not needed to justify a status. | §21 |
| S8 | §23 says the order "becomes FAILED **or** remains PENDING" and §24 says the implementation "must select one consistent behavior and test it". | **Rejected as indecisive** — the contract is fixed: order → `failed` with a `failure_reason`, inventory released, payment terminal. | §23, §24 |
| S9 | §30 offers "reject the configuration, **or** require an explicit override". | **Rejected as indecisive** — the application **refuses to start**. | §30 |
| S10 | §18 implies an "inventory service unavailable" condition distinct from insufficient inventory. | **Rejected** — no inventory service exists. Inventory is a local transaction against SQLite, so there is no provider boundary to fail. | §18 |
| S11 | §26 treats `Idempotency-Key` as an active mechanism. | **Deferred** — idempotency columns are reserved, not yet designed (`database.md` D16). | §26 |
| S12 | §42 proposes a future `python -m ecommerce.failure` CLI. | **Recorded as a future proposal** — not a current interface. The one CLI entrypoint the constitution mandates is `python -m ecommerce.seed` (`src/ecommerce/seed/__main__.py`, A2). | §42 |
| S13 | §35 introduces a `Sleeper` port for latency testing. | **Recorded as a proposal** — genuinely needed, and the natural fourth sibling of the three published determinism ports, but it is not yet in `architecture.md` §68. Requires sign-off in `specs/009-failure-simulation`. See §35. | §35 |
| S14 | §47 asserts `inventory.reserved <= inventory.quantity`. | **Rejected** — pre-D17 naming. Corrected to `quantity_reserved <= quantity_on_hand`. | §47 |
| S15 | §23, §24, §13 use uppercase statuses in **prose** (`Order becomes FAILED`). | **Rejected** — uppercase survives only in state-machine diagrams (`database.md` D6). | §13, §23, §24, §47 |
| S16 | §6, §7, §8 invent a `FailurePolicy` type and a `FailureSimulator` class API. | **Downgraded to conceptual** — the published mechanism is configuration plus a request header, not a policy object. Sketched, not specified. | §6, §7, §8 |
| S17 | §31 logs `failure_type=TIMEOUT` and `seed=42`. | **Resolved** — log field values use the published **scenario names** (`payment_timeout`), and the seed is logged as `RANDOM_SEED`. | §31 |
| S18 | §27 lists "Failure rate 1 → every eligible operation fails" but §33 says probability-based failure is for experiments, not tests. | **Clarified** — `FAKE_FAILURE_RATE=1.0` is a deliberate soak-test setting, never a test-suite default. | §27, §33 |
| S19 | §41's four profiles use `FAKE_FAILURE_SEED`. | **Rejected** — profiles rewritten to use `RANDOM_SEED`. | §41 |
| S20 | §46 is cited by `architecture.md` §60.1 as holding the checkout route contract (previously §5). | **Resolved** — this document keeps the draft's numbering; the inbound reference was repointed here. | §46 |

**This draft's genuinely new material** — the non-goals (§3), failure-category taxonomy (§5),
component sketch (§6), operation-specific targeting (§12), the decline/provider-error/timeout
distinction (§13–15), latency-plus-failure composition (§17), the HTTP-level injection point (§19),
idempotency interaction (§26), failure precedence (§28), the prohibition on debug endpoints (§34),
the injectable-`Sleeper` idea (§35), boundary guidance (§39), the domain-error distinction (§40),
the four configuration profiles (§41), the provider integration sketch (§44), security (§48),
performance-when-disabled (§49), implementation order (§50), and acceptance criteria (§51) — is
retained, in most cases near-verbatim.

---

# 1. Purpose

The Fake E-Commerce Server provides controlled failure simulation for development, integration
testing, resilience testing, and AI-agent experiments.

Failure simulation allows developers to reproduce conditions that are difficult to reproduce reliably
with real external systems.

Examples include:

* Payment declines
* Payment provider errors
* Payment timeouts
* Inventory unavailable
* Artificial latency
* HTTP 500 responses
* Temporary provider failures
* Deterministic failure sequences

Failure simulation must be:

* Disabled by default
* Explicitly configurable
* Deterministic when requested
* Isolated from domain logic
* Safe for local and test environments
* Observable through logs
* Easy to enable and disable

Failure simulation must never silently affect normal application behavior.

---

# 2. Principles

1. **Off by default.** A developer must never hit a simulated failure unexpectedly.
2. **Deterministic first.** The same failure request produces the same failure behavior, every time.
3. **Randomized is separate.** Resilience testing MAY use random injection, but it is independently
   controllable and cannot contaminate the deterministic path.
4. **Realistic.** Simulated failures follow the same compensation and transaction rules as real ones —
   otherwise they test nothing.
5. **Contained.** Failure logic lives in infrastructure (fake providers, middleware), never in the
   domain.
6. **Config-driven, not a policy object.** Behavior comes from environment variables plus the
   `X-Fake-Failure` header. There is no runtime policy type to construct and keep in sync (S16).

---

# 3. Non-Goals

Failure simulation is not intended to provide:

* Production chaos engineering
* Distributed-system chaos orchestration
* Real network packet manipulation
* Operating-system fault injection
* Database corruption
* Random destructive behavior
* Security testing infrastructure
* An administrative control plane

The subsystem operates at the application/provider boundary. The last item is why §34 rules out
`POST /debug/fail-everything`: a failure-injection API is a chaos-orchestration surface, and this
project deliberately does not have one.

---

# 4. Architectural Position

Failure simulation belongs primarily in the infrastructure layer.

```text
                    API
                     │
                     ▼
               Application
                     │
                     ▼
                  Domain
                     │
                     ▼
              Infrastructure
                     │
          ┌──────────┴──────────┐
          ▼                     ▼
    Fake Providers        Failure Simulator
          │                     │
          └──────────┬──────────┘
                     ▼
                  SQLite
```

The domain layer must not know that failure simulation exists.

For example, the domain should not contain:

```python
if failure_rate > 0:
    ...
```

Instead, infrastructure components may use the failure simulator before performing a simulated
external operation.

The domain never asks "am I in failure mode?". The fake provider simply behaves as a provider that
declines or times out, and the application layer applies the same compensation rules it would for a
real provider. That is what makes the simulation meaningful.

---

# 5. Failure Categories

The initial system supports the following failure categories:

| Category               | Example                                  | Scenario name         |
| ---------------------- | ---------------------------------------- | --------------------- |
| Business failure       | Payment declined                         | `payment_declined`    |
| Provider failure       | Payment provider returned a bad response  | `payment_provider_error` |
| Timeout                | Payment provider exceeds timeout         | `payment_timeout`     |
| Availability failure   | Inventory unavailable                    | `inventory_unavailable` |
| Latency                | Provider responds after artificial delay | `FAKE_LATENCY_MS`     |
| HTTP failure           | Simulated HTTP 500                       | `internal_server_error` |
| Middleware timeout     | Request delayed past the client's limit  | `service_timeout`     |
| Deterministic failure  | Specific configured request fails        | any of the above by name |

The **scenario name** is the contract — it is what a client sends in `X-Fake-Failure` and what appears
in logs. The category is descriptive metadata. Keeping the two distinct is what lets
`payment_timeout` (provider boundary) and `service_timeout` (middleware) share a category without
sharing a meaning.

The published scenario table, with codes per S6:

| Scenario | Fires at | HTTP | Error code |
|---|---|---|---|
| `payment_declined` | Fake payment provider | `402` | `PAYMENT_DECLINED` |
| `payment_timeout` | Fake payment provider | `504` | `PAYMENT_PROVIDER_TIMEOUT` |
| `payment_provider_error` | Fake payment provider | `502` | `PAYMENT_PROVIDER_ERROR` |
| `inventory_unavailable` | Inventory reservation | `409` | `INSUFFICIENT_INVENTORY` |
| `service_timeout` | Middleware | `504` | `SIMULATED_FAILURE` |
| `internal_server_error` | Middleware | `500` | `SIMULATED_FAILURE` |

---

# 6. Failure Simulation Components

Consistent with the published project tree (`architecture.md` §14), failure simulation lives under
infrastructure:

```text
src/ecommerce/infrastructure/
└── external/
    ├── failure/
    │   ├── __init__.py
    │   ├── models.py        # FailureType, FailureDecision
    │   └── simulator.py     # the seeded decision point
    │
    └── payments/
        ├── provider.py      # PaymentProvider port
        └── fake_provider.py # consumes FailureSimulator
```

Two notes on the draft's version of this tree:

* `policies.py` is **not adopted** (S16). A `FailurePolicy` type implies runtime-constructed policy
  objects; the published design is environment variables plus a request header. Keeping
  `models.py` + `simulator.py` avoids a layer that would have nothing to do.
* `fake_provider.py` belongs under a `payments/` subpackage, mirroring the domain structure, so the
  provider and its port are not siblings in a flat `external/` directory.

The exact module names may evolve during implementation.

---

# 7. Failure Simulator

The central component is the failure simulator.

Conceptually:

```python
class FailureSimulator:
    def should_fail(self, operation: str) -> bool: ...
    def apply_latency(self, operation: str) -> None: ...
    def failure_for(self, operation: str): ...
```

This is a **sketch**, not a specified interface (S16). What is specified is the dependency it pulls
in, because the simulator cannot be deterministic without it: the simulator takes the injected
`RandomSource` (`architecture.md` §68), never a module-level `random`. Given the same `RANDOM_SEED`
and the same operation sequence, it MUST return the same decisions. The three determinism ports —
`Clock`, `IdGenerator`, `RandomSource` — are what make that true, and the same ports are what
`testing.md` §5.2 substitutes in unit tests.

The simulator should not contain business logic. Its only question is:

> Should this infrastructure operation experience a simulated failure?

It does not know what a payment is, and it cannot decide compensation.

---

# 8. Failure Policy

A failure policy describes how a failure should be injected.

Conceptually:

```text
FailurePolicy
├── enabled
├── operation
├── failure_type
├── probability
├── latency_ms
├── status_code
├── error_code
└── message
```

> **Not adopted as a type** (S16). Each field above maps to something that already exists, and none of
> them is a runtime object:
>
> | Field | Published mechanism |
> |---|---|
> | `enabled` | `FAKE_FAILURE_ENABLED` |
> | `probability` | `FAKE_FAILURE_RATE` |
> | `latency_ms` | `FAKE_LATENCY_MS` |
> | `failure_type` | `X-Fake-Failure: <scenario>` |
> | `operation` | the provider call site |
> | `status_code` / `error_code` | derived per scenario (§5) |
> | `message` | fixed per scenario; never client-supplied |
>
> A `FailurePolicy` class would be a second source of truth that has to be kept in sync with the
> settings object. The draft's `FailureType` values also disagree with the published scenario names:
>
> ```text
> draft:   DECLINED  TIMEOUT  PROVIDER_ERROR  INVENTORY_UNAVAILABLE  HTTP_500  LATENCY
> published: payment_declined  payment_timeout  payment_provider_error
>           inventory_unavailable  internal_server_error
> ```
>
> The published lowercase names win, because they are the wire contract in `X-Fake-Failure`.

---

# 9. Configuration

Failure simulation is controlled through configuration:

```dotenv
FAKE_FAILURE_ENABLED=false
FAKE_FAILURE_RATE=0.0
FAKE_LATENCY_MS=0
RANDOM_SEED=42
```

**`FAKE_LATENCY_ENABLED` is not adopted** (S1), and **`FAKE_FAILURE_SEED` is not adopted** (S2).

`FAKE_FAILURE_ENABLED` is the single master switch. Latency is not a separate subsystem with its
own on/off state: it is one of the things the master switch governs. A second switch would permit
`FAKE_FAILURE_ENABLED=false` with latency running, which is a system that delays requests and reports
no failures — a very slow test suite with nothing to show for it.

`RANDOM_SEED` is the only seed. It governs data generation *and* randomized failure injection, so one
value pins a whole reproducible run.

The default configuration must produce normal behavior:

```dotenv
FAKE_FAILURE_ENABLED=false
FAKE_FAILURE_RATE=0.0
FAKE_LATENCY_MS=0
```

means no simulated failures and no artificial latency.

### 9.1 The three-way interaction

| `FAKE_FAILURE_ENABLED` | `FAKE_FAILURE_RATE` | Behavior |
|---|---|---|
| `false` | any | All simulation off. `X-Fake-Failure` is **ignored**. |
| `true` | `0.0` | Only **deterministic**, explicitly requested scenarios. Recommended even when enabled. |
| `true` | `> 0.0` | Adds **randomized** injection for resilience testing. |

Authoritative source: `configuration.md` §23.1.

---

# 10. Failure Rate

A global failure rate may be configured:

```dotenv
FAKE_FAILURE_RATE=0.25
```

This represents a configured probability for eligible simulated operations.

For example:

```text
0.00 → no failures
0.10 → approximately 10%
0.25 → approximately 25%
0.50 → approximately 50%
1.00 → every eligible operation
```

The value must be constrained to:

```text
0.0 <= rate <= 1.0
```

Invalid values must fail configuration validation. See §43.

---

# 11. Deterministic Failure Simulation

Random failure behavior must be reproducible when a seed is configured.

```dotenv
RANDOM_SEED=42
```

**`FAKE_FAILURE_SEED` is not adopted** (S2). The same `seed`, `configuration`, and `operation
sequence` MUST produce the same failure decisions.

This is important for:

* Automated tests
* Debugging
* Reproducing agent behavior
* Regression tests
* CI

The payoff is visible in practice. An AI-agent run that hit a payment timeout can be replayed exactly
by setting one variable:

```bash
APP_ENV=test RANDOM_SEED=42 FAKE_CLOCK=2026-01-01T00:00:00Z \
  FAKE_FAILURE_ENABLED=true FAKE_FAILURE_RATE=0.0
```

then naming the scenario per request. With two competing seeds, a failure that reproduces locally and
not in CI has no defined cause.

---

# 12. Operation-Specific Failures

Global failure rates are useful but insufficient for targeted testing.

The simulator supports operation-specific behavior, so a test can fail exactly one provider call:

```text
payment.authorize
payment.capture
payment.refund

inventory.reserve
inventory.release

checkout.execute
```

This allows tests to say:

```text
payment.authorize → payment_timeout
```

without causing unrelated operations to fail.

The two targeting mechanisms are distinct and do not overlap:

| Mechanism | Scope | Set by |
|---|---|---|
| Scenario name | One request | `X-Fake-Failure` header |
| Operation targeting | One provider call within a request | Scenario definition |

---

# 13. Payment Failure Types

The fake payment provider supports at least:

```text
SUCCESS
DECLINED
TIMEOUT
PROVIDER_ERROR
```

These are internal provider outcomes. The **status they produce** is lowercase, per `API.md` §24 and
`database.md` D6.

### 13.1 Successful Payment

Normal operation:

```text
authorize()
    ↓
authorized
```

### 13.2 Payment Declined

A decline represents a **business-level** payment failure — the provider understood the request and
said no.

```text
authorize()
    ↓
declined
```

This is different from a provider outage. The application distinguishes:

```text
Payment declined              → 402 PAYMENT_DECLINED      → retriable with a new method
Payment provider unavailable   → 502 PAYMENT_PROVIDER_ERROR → retriable, same method
Payment provider timeout       → 504 PAYMENT_PROVIDER_TIMEOUT → retriable, ambiguous
```

Collapsing these into one "payment failed" state would make the retry semantics untestable, which is
the entire reason `captured` exists as a separate state (A4).

---

# 14. Payment Provider Error

A provider error represents an **infrastructure-level** failure.

```text
authorize()
    ↓
ProviderError
```

The application must not treat this as equivalent to a normal payment decline. The distinction allows
the application to implement appropriate retry behavior.

---

# 15. Payment Timeout

A timeout simulates a provider that does not respond within the configured time.

```text
authorize()
    ↓
ProviderTimeout
```

A timeout MUST be represented by an explicit infrastructure exception or equivalent error type. It
MUST NOT be represented as a generic Python exception with no context, because "the provider was
slow" and "the provider returned garbage" lead to different compensation.

> The draft's `PAYMENT_TIMEOUT_MS=5000` is **not adopted.** That is a *real* client-side timeout
> budget, not a simulation switch, and configuring it in a fake server means tuning how fake the fake
> is. The published timeout signal is the `payment_timeout` scenario itself, which needs no duration
> configuration — it never responds at all. Recorded in `configuration.md` G17 as deferred to
> `specs/009-failure-simulation`.

---

# 16. Artificial Latency

The system may introduce artificial latency.

```dotenv
FAKE_FAILURE_ENABLED=true
FAKE_LATENCY_MS=250
```

**`FAKE_LATENCY_ENABLED` is not adopted** (S1) — the master switch is `FAKE_FAILURE_ENABLED`.

An eligible operation will delay approximately the configured amount before returning.

Latency simulation should be applied primarily to external provider boundaries. The system should
avoid introducing unnecessary latency into domain operations, which are local function calls and have
no realistic reason to be slow.

---

# 17. Latency and Failure Combination

Latency and failure can be combined:

```text
request
   │
   ▼
250 ms latency
   │
   ▼
provider failure
```

This allows realistic scenarios such as a slow provider that then errors. Tests should verify that the
application handles both dimensions correctly — specifically that the latency is applied even when the
operation ultimately fails, since a client that times out during a slow failure never sees the error.

---

# 18. Inventory Failure Simulation

Inventory availability is a **business condition**, not an external provider failure.

```text
requested quantity = 10
available to sell = 4
```

The reservation must fail with `409 INSUFFICIENT_INVENTORY`.

> The draft's "inventory service unavailable" condition is **not adopted** (S10). There is no
> inventory service. Inventory is a conditional `UPDATE` against local SQLite inside the checkout
> transaction, so there is no provider boundary that could be unavailable. Inventing a second failure
> mode for a component that cannot fail that way would add a test with nothing behind it.

This is also why the arithmetic here is stated as *available to sell* rather than as a column
comparison. `quantity_on_hand` is total stock and `quantity_reserved` is committed to carts; the
reservation check is `quantity_on_hand - quantity_reserved >= n` (`database.md` §24, D17).

---

# 19. HTTP 500 Simulation

For API-level failure testing, selected operations may produce a controlled HTTP 500 response.

This should be implemented at the API/infrastructure boundary rather than by corrupting domain state:

```text
any API request
        │
        ▼
  simulated provider error
        │
        ▼
     HTTP 500
```

The response must still use the project's standard error structure (§21).

> The draft's example used `POST /api/v1/payments/{id}/authorize`. That route is an **E19 proposal**
> awaiting sign-off in `specs/007-payments` (S4), so the example is left route-agnostic here. The
> point being made is the injection point, not the endpoint.

---

# 20. Failure Error Model

Internal failures have explicit types, matching the published taxonomy (`API.md` §33):

```text
DomainError            INVALID_QUANTITY, INVALID_ORDER_STATE,
                       INVALID_PAYMENT_STATE, INSUFFICIENT_INVENTORY,
                       PRODUCT_INACTIVE, EMPTY_CART

ApplicationError       DUPLICATE_SKU, DUPLICATE_EMAIL,
                       CART_NOT_CHECKOUTABLE, NOT_FOUND

InfrastructureError    DATABASE_ERROR, PAYMENT_PROVIDER_ERROR,
                       PAYMENT_PROVIDER_TIMEOUT
```

Payment-specific failures (`PaymentDeclined`, `PaymentProviderError`, `PaymentTimeout`) are
**exception types**, distinct from the **error codes** they map to. The exception hierarchy is
finalized during implementation; the code taxonomy above is already published and MUST NOT drift.

---

# 21. Mapping Failures to HTTP

The API layer maps internal failures to HTTP responses. This is the published mapping
(`API.md` §13, §12.1) — it is not open to per-scenario interpretation:

| Internal condition      |                                 HTTP | Code                         |
| ----------------------- | -----------------------------------: | ---------------------------- |
| Validation error        |                                  422 | `VALIDATION_ERROR`           |
| Resource not found      |                                  404 | `NOT_FOUND`                  |
| Business rule violation |                                  409 | category-specific            |
| Payment declined        |                                  402 | `PAYMENT_DECLINED`           |
| Provider error          |                                  502 | `PAYMENT_PROVIDER_ERROR`     |
| Service unavailable     |                                  503 | — (no scenario uses it)      |
| Provider timeout        |                                  504 | `PAYMENT_PROVIDER_TIMEOUT`   |
| Unexpected error        |                                  500 | `INTERNAL_ERROR`             |

Two clarifications on the draft's version of this table:

* **"Payment declined: 402 or project-defined payment error"** (S8-adjacent) — the project *has*
  defined it: `402` / `PAYMENT_DECLINED`. Offering an either/or here is how a client ends up
  handling two codes that only one of which is ever sent.
* **"Provider unavailable: 503"** (S7) — `503` is a legitimate status in `API.md` §13, but the
  published scenario table maps a fake-provider error to `502`. `502` is authoritative, and no current
  scenario produces `503`. Reserving a status for a scenario that does not exist is harmless;
  documenting it as a live mapping is misleading.

---

# 22. Failure Simulation and Transactions

Failure simulation must not bypass transaction management.

For checkout:

```text
BEGIN
   reserve inventory
   create order
   create payment

   payment provider fails

ROLLBACK / COMPENSATE
```

The checkout implementation must maintain its defined consistency guarantees. Failure simulation
exists to *test* those guarantees, so a simulator that short-circuited the transaction path would
invalidate the very thing it is meant to exercise.

---

# 23. Payment Failure During Checkout

```text
Cart
 │
 ▼
Inventory reservation
 │
 ▼
Order creation
 │
 ▼
Payment authorization
 │
 ▼
declined
```

Required end state — **fixed, not implementation-defined** (S8):

```text
payment declined
       ↓
order → failed, with failure_reason
       ↓
inventory reservation released
       ↓
cart remains active
```

The draft's "order becomes FAILED **or** remains PENDING" is rejected. Leaving an order at `pending`
after payment failed creates a row that looks like an in-flight order and never resolves, and it is
the state the checkout compensation path exists to prevent. `failed` is in the order lifecycle
precisely for this (A3, the constitution's *Explicit Business Rules*, III).

The resulting states follow the checkout specification, not the failure simulator. The simulator
produces a decline; the application decides what that means.

---

# 24. Timeout During Checkout

```text
Cart
 │
 ▼
Reserve inventory
 │
 ▼
Create order
 │
 ▼
Payment timeout
```

Required end state — identical to a decline for state purposes (S8):

```text
payment → failed
order   → failed, with failure_reason
inventory reservation → released
response → 504 PAYMENT_PROVIDER_TIMEOUT
```

The draft's "the implementation must select one consistent behavior and test it" defers a contract
that consumers depend on. Selecting it later means a client written against the interim behavior
breaks. The system cannot wait forever for a provider that never responds, so it compensates exactly
as it would for a decline.

---

# 25. Retry Behavior

Retry behavior belongs to the application layer. The failure simulator should not automatically retry
operations:

```text
Application
    │
    ├── attempt 1
    │       ↓
    │    timeout
    │
    └── attempt 2
            ↓
         success
```

The provider adapter reports the timeout. The application decides whether retrying is valid. This
separation matters because not every failure is safe to retry: retrying after a `captured` payment is
`409 INVALID_PAYMENT_TRANSITION` (`API.md` E12).

---

# 26. Idempotency

Failure simulation must be compatible with API idempotency.

For operations supporting:

```text
Idempotency-Key
```

a client retry should not unintentionally create duplicate business operations:

```text
Request
  │
  ▼
Payment authorization
  │
  ▼
Timeout
  │
  ▼
Client retries
```

> **Deferred** (S11). The draft treats `Idempotency-Key` as an active mechanism. It is not:
> `database.md` D16 reserves the columns without designing them, and `API.md` §32 carries the contract
> as a requirement. The scenario is worth testing once the mechanism exists — a timeout is the
> canonical case where a client retries something that may or may not have committed — but the test
> cannot be written against an undesigned mechanism.

---

# 27. Failure Scenarios

The failure matrix:

| Scenario | Expected behavior | HTTP |
|---|---|---|
| Payment succeeds | Payment `authorized` | 201 |
| Payment declined | Payment `declined`; order `failed`; inventory released | 402 |
| Payment provider error | Order `failed`; inventory released | 502 |
| Payment timeout | Order `failed`; inventory released | 504 |
| Inventory insufficient | Reservation rejected; no order created | 409 |
| Provider latency | Delayed response | as normal |
| HTTP 500 simulation | Controlled server error, clean envelope | 500 |
| Failure disabled | Normal operation; `X-Fake-Failure` ignored | as normal |
| Failure rate 0 | Deterministic scenarios only, on explicit request | as normal |
| Failure rate 1 | Every eligible operation fails — **soak testing only** | varies |

The `Failure rate 1` row is a deliberate resilience setting, not a test-suite default (S18). With
`FAKE_FAILURE_RATE=1.0`, no test that expects success can pass, so a suite using it is measuring
nothing except its own error handling.

---

# 28. Failure Precedence

When multiple failure rules apply, precedence MUST be deterministic:

```text
1. Explicit operation-specific failure (X-Fake-Failure)
2. Explicit deterministic scenario
3. Timeout configuration
4. Global failure probability
5. Normal operation
```

An explicit request always wins over a probability, so a test that asks for a decline gets a decline
regardless of `FAKE_FAILURE_RATE`. Were it the other way round, a rate of `0.5` would make half the
declines silent — a test suite that fails intermittently and blames the seed.

---

# 29. Disabled-by-Default Rule

Failure simulation must be disabled by default.

A normal developer environment behaves like:

```dotenv
FAKE_FAILURE_ENABLED=false
FAKE_LATENCY_MS=0
```

(`FAKE_LATENCY_ENABLED` is not adopted — S1.)

This prevents accidental failure behavior during normal development. It is also enforced, not merely
documented: `APP_ENV=production` refuses to start with simulation enabled (§30).

---

# 30. Production Safety

Failure simulation must not be accidentally enabled in a "production" environment.

The application validates configuration and **refuses to start**:

```text
APP_ENV=production
FAKE_FAILURE_ENABLED=true    → startup error
FAKE_FAILURE_RATE > 0        → startup error
```

The draft offers "reject the configuration, **or** require an explicit override" (S9). There is no
override. An escape hatch here defeats the purpose: the failure mode is someone enabling fault
injection in a client-facing environment and not noticing, and an explicit-override flag makes that a
supported configuration rather than a mistake.

---

# 31. Observability

Every simulated failure MUST be observable. Logs include:

```text
event=failure_simulated
scenario=payment_timeout
request_id=abc123
random_seed=42
```

Two corrections to the draft (S17):

* The field is `scenario`, and its value is a **published scenario name** (`payment_timeout`), not
  the draft's `failure_type=TIMEOUT`. Logs and the `X-Fake-Failure` header must use the same
  vocabulary, or grepping a log for a scenario a client sent finds nothing.
* The seed is logged as `random_seed`, and it is `RANDOM_SEED` — the value the operator actually set.

Do not log:

* secrets
* credentials
* payment credentials
* authentication tokens
* sensitive customer information

---

# 32. Request Correlation

Failure logs MUST include the request ID when available:

```text
request_id=abc123
scenario=payment_provider_error
```

This allows a failure to be traced across:

```text
HTTP request
    ↓
application service
    ↓
payment provider
    ↓
failure simulator
```

The error envelope carries the same `request_id`, so a client reporting a failure and an operator
reading the logs are looking at the same identifier (`API.md` §8.1).

---

# 33. Deterministic Scenario Mode

For automated tests, explicit scenarios are preferable to probability-based failures:

```http
X-Fake-Failure: payment_timeout
```

This allows a test to say "this operation must fail with a timeout" instead of relying on random
chance.

Probability-based failures remain useful for broader resilience experiments and soak testing (§27),
where the absence of failures is the signal.

---

# 34. Test API for Failure Simulation

The implementation is **configuration-driven** plus a request header. It deliberately does NOT expose
unrestricted administrative endpoints such as:

```text
POST /debug/fail-everything
```

Configuration and dependency injection provide better control and reduce accidental exposure. An
unauthenticated failure-injection endpoint is a denial-of-service primitive: anyone who can reach the
server could make every checkout fail.

Administrative failure controls, if ever introduced, MUST require authentication and authorization
(§48).

---

# 35. Failure Simulation in Unit Tests

Unit tests MUST NOT require real delays.

For latency behavior, use an injectable sleeper or a mocked delay mechanism:

```text
Sleeper
   │
   ├── RealSleeper
   └── TestSleeper
```

This allows tests to verify that latency *would* be applied without making the test suite slow.

> **Recorded as a proposal** (S13). The `Sleeper` port is not in `architecture.md` §68, which
> publishes exactly three determinism ports: `Clock`, `IdGenerator`, and `RandomSource`. `Sleeper` is
> the natural fourth — it is the same class of problem (a non-determinism source behind a port) and
> the same solution. It is **not** added unilaterally here, because adding a port changes the
> architecture contract; it needs sign-off in `specs/009-failure-simulation`.
>
> Note that `FAKE_LATENCY_MS` alone is insufficient for testing: a test asserting a 250 ms delay
> either sleeps 250 ms or mocks the clock, and `Clock` is the wrong port for it — `Clock` controls
> *what time is*, not *how long we wait*.

---

# 36. Failure Simulation in Integration Tests

Integration tests MUST use deterministic failure configuration:

```dotenv
APP_ENV=test
FAKE_FAILURE_ENABLED=true
FAKE_FAILURE_RATE=0.0
RANDOM_SEED=42
```

(`FAKE_FAILURE_SEED` is not adopted — S2. `FAKE_FAILURE_RATE=0.0` rather than a non-zero value, per
`testing.md` §25: a rate above zero makes an assertion that a request fails into a coin flip.)

Tests should verify:

* failure occurs
* transaction behavior is correct
* state remains consistent
* retry behavior is correct
* error mapping is correct

---

# 37. Failure Simulation in API Tests

API tests verify the externally observable result.

```http
POST /api/v1/checkout
Authorization: Bearer <fake-token>
Content-Type: application/json
X-Fake-Failure: payment_timeout

{ "cart_id": "…", "payment_method": "fake_card" }
```

(The draft used `POST /api/v1/carts/1/checkout` — rejected, S3. The interim contract is
`POST /api/v1/checkout` with `cart_id` in the body, per A6.)

With a simulated payment timeout, verify:

```text
HTTP status = 504
error.code = PAYMENT_PROVIDER_TIMEOUT
request_id exists and matches the X-Request-ID response header
database state is consistent: order failed, inventory released
cart remains active
```

The draft expected `error.code = PAYMENT_TIMEOUT` (S5), a name that appears nowhere in the taxonomy.
The published code is `PAYMENT_PROVIDER_TIMEOUT`; `PAYMENT_DECLINED` / `PAYMENT_PROVIDER_ERROR` /
`PAYMENT_PROVIDER_TIMEOUT` are specific, and `SIMULATED_FAILURE` is the generic fallback for
scenarios with no specific code.

---

# 38. Failure Simulation Matrix

| Operation         | Scenario                | API result              | State check                        |
| ----------------- | ----------------------- | ----------------------- | ---------------------------------- |
| Payment authorize | `payment_declined`      | `402`                   | Payment `declined`; order `failed` |
| Payment authorize | `payment_timeout`       | `504`                   | No inconsistent state              |
| Payment authorize | `payment_provider_error`| `502`                   | No inconsistent state              |
| Inventory reserve | `inventory_unavailable` | `409`                   | Inventory unchanged; no order      |
| Checkout          | `payment_declined`      | `402`                   | Reservation compensated           |
| Checkout          | `payment_timeout`       | `504`                   | Reservation compensated           |
| Provider request  | `FAKE_LATENCY_MS`       | Delayed response        | State unchanged                    |
| Any request       | `internal_server_error` | `500`                   | State unchanged; no detail leaked  |
| Any request       | `service_timeout`       | `504`                   | No partial writes                  |

Every row asserts **both** the response and the resulting state. See `testing.md` §25.

---

# 39. Failure Injection Boundaries

Failure injection occurs at boundaries where failures are realistic.

Good locations:

```text
Payment provider
External provider adapter
HTTP client boundary
API middleware (for service_timeout / internal_server_error)
```

Poor locations:

```text
Domain entities
Value objects
Pure calculations
Business invariants
```

The domain should fail because a business rule was violated, not because a random failure simulator
was injected into it. An inventory reservation that "fails" because of a coin flip tests the
simulator, not the rule.

---

# 40. Relationship to Domain Errors

Failure simulation remains separate from domain validation.

```text
Product price < 0
       ↓
Domain validation error        (not a simulated failure)

Payment provider unavailable
       ↓
Infrastructure failure         (a simulated failure)
```

A domain invariant that can be broken by a failure simulator is not an invariant. This is the concrete
form of the layer rule in `architecture.md` §26.

---

# 41. Failure Configuration Profiles

Development and test profiles:

### Normal

```dotenv
FAKE_FAILURE_ENABLED=false
FAKE_FAILURE_RATE=0.0
FAKE_LATENCY_MS=0
```

### Occasional Failures

```dotenv
FAKE_FAILURE_ENABLED=true
FAKE_FAILURE_RATE=0.10
RANDOM_SEED=42
```

### Aggressive Failure Testing

```dotenv
FAKE_FAILURE_ENABLED=true
FAKE_FAILURE_RATE=0.50
RANDOM_SEED=42
```

### Guaranteed Failure

```dotenv
FAKE_FAILURE_ENABLED=true
FAKE_FAILURE_RATE=1.0
RANDOM_SEED=42
```

(`FAKE_FAILURE_SEED` and `FAKE_LATENCY_ENABLED` are not adopted — S1, S2, S19.)

These profiles are for development and testing only. The last three set `FAKE_FAILURE_RATE > 0`, so
`APP_ENV=production` refuses to start with any of them (§30).

---

# 42. Failure Simulation CLI

A future development CLI may expose controlled scenarios:

```bash
uv run python -m ecommerce.failure --scenario payment-timeout
```

or:

```bash
uv run python -m ecommerce.failure --operation payment.authorize --failure timeout
```

**This is optional and not a current interface** (S12). The only CLI entrypoint the constitution
mandates is `python -m ecommerce.seed` (`src/ecommerce/seed/__main__.py`, A2). A second entrypoint
means a second packaging and distribution surface to maintain, and the header-based mechanism already
covers scripted use. If adopted, it takes the same `RANDOM_SEED` and the same scenario names.

The initial implementation prioritizes configuration-based control.

---

# 43. Configuration Validation

Pydantic Settings MUST validate:

```text
FAKE_FAILURE_RATE >= 0
FAKE_FAILURE_RATE <= 1
FAKE_LATENCY_MS >= 0
RANDOM_SEED is a valid integer
```

Invalid configuration MUST fail at application startup.

```dotenv
FAKE_FAILURE_RATE=2.0
```

must not be accepted.

(`FAKE_FAILURE_SEED is valid integer` from the draft is **not adopted** — S2. `RANDOM_SEED` is already
validated, and it is the seed that is actually used.)

Plus the two hard guards from `configuration.md` §29:

```text
APP_ENV=production + FAKE_FAILURE_ENABLED=true   → refuse to start
APP_ENV=production + FAKE_FAILURE_RATE > 0       → refuse to start
```

---

# 44. Failure Simulation and Fake Providers

The fake payment provider consumes the failure simulator:

```text
FakePaymentProvider
        │
        ▼
FailureSimulator
        │
   ┌────┼────┐
   ▼    ▼    ▼
Success Decline Timeout
```

The provider remains responsible for translating the simulated result into provider-specific behavior
— that translation is what makes a simulated decline travel the same code path as a real one.

---

# 45. Example Payment Flow

Normal:

```text
Authorize Payment → Failure Simulator → No failure → authorized
```

Failure:

```text
Authorize Payment
       │
       ▼
Failure Simulator
       │
       ▼
payment_timeout
       │
       ▼
PaymentTimeout
       │
       ▼
Application Error Handling
       │
       ▼
HTTP 504 PAYMENT_PROVIDER_TIMEOUT
```

---

# 46. Example Checkout Flow

```text
POST /api/v1/checkout
{ "cart_id": "…", "payment_method": "fake_card" }
            │
            ▼
      Checkout Service
            │
            ├── Validate cart
            │
            ├── Reserve inventory
            │
            ├── Create order
            │
            └── Authorize payment
                       │
                       ▼
                Failure Simulator
                       │
                       ▼
                payment_timeout
                       │
                       ▼
              Checkout compensation
                       │
                       ├── release inventory
                       └── order → failed, payment → failed
```

> The draft used `POST /carts/{id}/checkout` — rejected (S3). The interim contract is
> `POST /api/v1/checkout` with `cart_id` in the body (A6, deferred to `specs/005-orders`). If the spec
> selects the path-based form, this section, `API.md` §21, `README.md`, and `architecture.md` §60.1
> must be updated together.
>
> `architecture.md` §60.1 cites this section as the checkout-route contract; the citation was
> repointed here from the previous §5 (S20).

The test suite must verify the complete state transition.

---

# 47. State Consistency Requirements

Failure simulation MUST never intentionally leave invalid state.

After any simulated failure:

```text
quantity_on_hand >= 0
quantity_reserved >= 0
quantity_reserved <= quantity_on_hand     (D17)
order status is valid
payment status is valid
cart status is valid
foreign-key relationships remain valid
```

> The draft asserted `inventory.reserved <= inventory.quantity` — rejected (S14). Under the D17
> resolution the columns are `quantity_on_hand` (total stock) and `quantity_reserved` (committed to
> carts), and a reservation may never exceed on-hand.

Failure simulation tests MUST explicitly validate these invariants.

---

# 48. Security Considerations

Failure simulation MUST NOT expose sensitive operational controls to untrusted clients.

Configuration is controlled by:

* Environment configuration
* Local development configuration
* Test configuration
* CI configuration

Administrative failure controls, if introduced later, MUST require authentication and authorization
(§34).

---

# 49. Performance Considerations

Failure simulation MUST have negligible overhead when disabled.

```text
FAKE_FAILURE_ENABLED=false
        │
        ▼
one boolean check
        │
        ▼
normal provider operation
```

Artificial latency MUST only be introduced when explicitly enabled, and only at provider boundaries.

---

# 50. Implementation Order

Incremental implementation:

### Step 1 — Failure models

Define `FailureType` and `FailureDecision` using the published scenario names (§5).

### Step 2 — Simulator

Implement the simulator taking the injected `RandomSource` (§7).

### Step 3 — Latency

Add injectable latency behavior, behind the `Sleeper` proposal (§35).

### Step 4 — Payment integration

Integrate with `FakePaymentProvider`.

### Step 5 — Application error mapping

Map provider failures to the published error codes (§20, §21).

### Step 6 — API mapping

Map application errors to the published HTTP statuses (§21).

### Step 7 — Checkout compensation

Verify transaction/compensation behavior (§22–24).

### Step 8 — Tests

Add unit, integration, and API tests (`testing.md` §25).

### Step 9 — Observability

Add structured logging and request correlation (§31, §32).

---

# 51. Acceptance Criteria

Failure simulation is complete when:

* Failure simulation is disabled by default.
* Failure rates are configurable and validated.
* Randomized failures are deterministic under `RANDOM_SEED`.
* Operation-specific scenarios are supported via `X-Fake-Failure`.
* Payment decline, timeout, and provider error are distinct scenarios with distinct codes.
* Artificial latency is supported and bounded.
* `APP_ENV=production` **refuses to start** with simulation enabled.
* Inventory failure scenarios are testable.
* Error mapping matches `API.md` §12.1 and §13.
* Checkout compensation is tested for every payment scenario.
* Failures do not violate domain invariants (§47).
* Failures are observable through structured logs.
* Request IDs are preserved into logs and the error envelope.
* Unit tests do not require real delays.
* Integration tests use deterministic scenarios with `FAKE_FAILURE_RATE=0.0`.
* API tests verify externally observable behavior.
* No administrative failure-injection endpoint exists.
* Documentation matches the implementation.

---

# 52. Related

* `docs/API.md` — error envelope, error codes (§12.1), status codes (§13)
* `docs/configuration.md` — the three failure variables and their interaction (§23.1)
* `docs/testing.md` §25 — failure scenario test matrix
* `docs/architecture.md` §26 — failure simulation architecture
* `docs/architecture.md` §68 — determinism ports
* `docs/database.md` D16 — idempotency (deferred), D17 — inventory quantities
* `specs/009-failure-simulation` — the feature specification

---

## 53. Final Architecture

```text
                         API
                          │
                          ▼
                   Application
                          │
                          ▼
                       Domain
                          │
                          ▼
                   Infrastructure
                          │
             ┌────────────┴────────────┐
             │                         │
             ▼                         ▼
       Fake Providers           Failure Simulator
             │                         │
             └────────────┬────────────┘
                          │
                          ▼
                       SQLite
```

The key architectural rule is:

```text
Failure simulation belongs at infrastructure
boundaries, not inside domain business logic.
```

This keeps the domain deterministic and framework-independent while allowing the fake server to
reproduce realistic infrastructure failures for testing, resilience experiments, and AI-agent
workflows.
