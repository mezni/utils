#!/usr/bin/env bash
set -e

echo "=== Connectivity Verification ==="

echo -n "Traefik (port 80) ......... "
if curl -sf -o /dev/null http://traefik:80/; then
  echo "OK"
else
  echo "FAIL"
fi

echo -n "PostgreSQL (port 5432) .... "
if pg_isready -h postgis -U "$POSTGRES_USER" -d "$POSTGRES_DB" >/dev/null 2>&1; then
  echo "OK"
else
  echo "FAIL"
fi

echo -n "RabbitMQ (port 5672) ...... "
if rabbitmq-diagnostics -q check_running >/dev/null 2>&1; then
  echo "OK"
else
  echo "FAIL"
fi

echo -n "Keycloak (/auth) .......... "
if curl -sf -o /dev/null http://keycloak:8080/health; then
  echo "OK"
else
  echo "FAIL"
fi

echo "=== Done ==="
