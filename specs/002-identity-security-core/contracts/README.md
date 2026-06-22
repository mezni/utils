# Contracts: Identity & Security Core

This directory defines the interface contracts for the identity and security layer. Contracts follow the contract-first approach: DTOs in domain-types, then backend implementation, then frontend.

## Contracts

| Contract | File | Description |
|----------|------|-------------|
| JWT Claims | [jwt-claims.md](./jwt-claims.md) | Structure of JWT payload claims |
| Audit Event | [audit-event.md](./audit-event.md) | Event schema for audit logging |
| RBAC | [rbac.md](./rbac.md) | Role definitions and endpoint protection rules |
| JIT Provisioning | [jit-provisioning.md](./jit-provisioning.md) | User profile sync contract |
