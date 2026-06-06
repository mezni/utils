#!/bin/bash
# Wrapper script for pnpm dev commands to bypass supply-chain policy

# Ensure dependencies are installed
pnpm install --no-frozen-lockfile

# Run the dev command with provided arguments
exec pnpm "$@"
