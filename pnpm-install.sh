#!/bin/bash
# Helper script to run pnpm commands without supply-chain policy check

# Run pnpm install without frozen lockfile
pnpm install --no-frozen-lockfile "$@"
