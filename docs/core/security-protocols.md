# Security Protocols, Auth Flow, and Data Compliance

This document outlines the mandatory security architecture, authorization protocols, and data compliance standards for this project. All code implementations must strictly adhere to these guardrails.

## 1. Authentication & Authorization (Auth Flow)
* **Protocol:** [e.g., OAuth 2.0 / OIDC / JWT / Session-based]
* **Token Management:**
  * Access tokens must be short-lived and never stored in `localStorage` (use `httpOnly, Secure, SameSite=Strict` cookies or secure memory state).
  * Refresh tokens must use rotation and automatic reuse detection.
* **Access Control:** Implement Role-Based Access Control (RBAC) or Attribute-Based Access Control (ABAC). Secure both frontend routes and backend endpoints natively.

## 2. Data Protection & Encryption
* **In Transit:** All data transmission must require HTTPS with TLS 1.3. Reject non-secure traffic.
* **At Rest:** Sensitive user data (passwords, PII) must be hashed using a strong cryptographic algorithm (e.g., Argon2id or bcrypt).
* **PII Handling:** Mask or redact Personally Identifiable Information (PII) in application logs. Never log passwords, tokens, or credit card details.

## 3. Threat Mitigation & Input Validation
* **Injection Prevention:** Use parameterized queries or ORM/ODM features to prevent SQL/NoSQL Injection.
* **XSS (Cross-Site Scripting):** Sanitize and escape all user-generated content before rendering it in the DOM. Use a strict Content Security Policy (CSP).
* **CSRF (Cross-Site Request Forgery):** Implement anti-CSRF tokens for all state-changing state requests (POST, PUT, DELETE) if using cookie-based authentication.
* **Rate Limiting:** Protect all public API endpoints (especially auth endpoints) with rate-limiting to prevent brute-force and DDoS attacks.

## 4. Secret & Dependency Management
* **Zero Hardcoding:** All API keys, database credentials, and private keys must be loaded via environment variables (`.env`).
* **Dependency Scanning:** Regularly audit dependencies for known vulnerabilities (e.g., `npm audit`, `Snyk`, or GitHub Dependabot). Lock down dependency versions using lockfiles (`package-lock.json`, `yarn.lock`, etc.).

## 5. Compliance & Audit Logs
* **Audit Trail:** Log all critical security events (login failures, password changes, privilege escalations) with timestamps and anonymized user identifiers.
* **Regulatory Compliance:** Adhere to [e.g., GDPR / CCPA / HIPAA] guidelines regarding the "Right to be Forgotten" and data portability.