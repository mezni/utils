# ADR-006: Bare Metal + Docker Compose over Kubernetes

**Status**: Accepted
**Date**: 2026-06-07

## Context

The platform needs to be deployed for production. Options: Kubernetes (K8s), Docker Swarm, bare metal with Docker Compose, or PaaS.

## Decision

Use a single bare metal host with Docker Compose (docker-compose.prod.yml) for production deployment.

## Rationale

- Principle 3 (Simple operations): one person must be able to operate everything
- Docker Compose is dramatically simpler than Kubernetes for small deployments
- The stack has exactly 6 containers — Kubernetes is designed for hundreds
- No orchestration overhead: no etcd, no kubelet, no ingress controller configuration
- Backup, restore, and log access are straightforward SSH commands
- Single host eliminates distributed systems complexity

## Consequences

- Single point of failure: the host goes down, the platform goes down
- No built-in zero-downtime deployment (manual `docker compose up -d`)
- Scaling is vertical only (upgrade the host)
- Host maintenance requires downtime or careful planning

## Compliance

- No Kubernetes-related infrastructure without an approved ADR
- Every operational task must have a documented runbook
- Deployment is always manual via SSH following the runbook
