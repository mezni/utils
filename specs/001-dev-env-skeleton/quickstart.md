# Quickstart: Dev Environment

## Prerequisites

- Rust (stable toolchain): `curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`
- Node.js 18+: via nvm or system package manager
- pnpm: `npm install -g pnpm`
- Expo Go app on iOS/Android device or simulator
- Docker (optional, for CI verification)

## Clone & Setup

```bash
git clone <repo-url> bornemap
cd bornemap
```

## Run Backend

```bash
cargo run -p core-service
```

Expected output: service starts on `0.0.0.0:8080`.

Verify:
```bash
curl http://localhost:8080/api/v1/health/live
# {"status":"alive","service":"core-service"}

curl http://localhost:8080/api/v1/health/ready
# {"status":"ready","service":"core-service"}
```

## Run Mobile App

In a separate terminal:
```bash
pnpm install
pnpm --filter mobile-driver start
```

Open Expo Go on your device and scan the QR code. The app should display:
`Core Service: alive`

If the backend is not running, the app will show:
`Connection Error` with a retry prompt.

## Verify CI

Push a branch and open a pull request. GitHub Actions will run:
1. Rust clippy lint
2. Frontend eslint lint
3. `cargo test`
4. Docker build for core-service

All checks must pass before merge.

## Troubleshooting

| Problem | Likely Cause | Fix |
|---------|-------------|-----|
| Backend won't start | Missing Rust toolchain | Run `rustup install stable` |
| Mobile can't reach backend | Backend not running | Start backend first |
| CI fails on lint | Clippy/eslint violations | Run locally: `cargo clippy` |
| Port conflict | Another service on 8080 | Kill existing process |
