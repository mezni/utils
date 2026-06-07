# Guide d'onboarding — BorneMap

**Dernière mise à jour**: 2026-06-07
**Phase**: 1 — Foundation

---

## Prérequis

- **Rust**: 1.78+ (installer via rustup)
- **Node.js**: 20+ (installer via nvm)
- **pnpm**: 9+ (`npm install -g pnpm`)
- **Docker**: 24+ avec Docker Compose
- **PostgreSQL**: 16 (optionnel, Docker fournit la base)
- **sqlx-cli**: `cargo install sqlx-cli --no-default-features --features postgres`

---

## Cloner et installer

```bash
git clone git@github.com:mezni/BorneMap.git
cd BorneMap

# Installer les dépendances Rust
cargo build --all

# Installer les dépendances JS/TS
pnpm install
```

---

## Lancer la base de données

```bash
docker compose -f infra/compose/docker-compose.yml up -d postgres
```

Vérifier que PostgreSQL est prêt :

```bash
docker compose -f infra/compose/docker-compose.yml ps
```

---

## Exécuter les migrations

```bash
sqlx migrate run --source db/migrations --database-url postgres://postgres:postgres@localhost:5432/ev_platform
```

Ou utiliser le script :

```bash
chmod +x db/migrate.sh
DATABASE_URL=postgres://postgres:postgres@localhost:5432/ev_platform ./db/migrate.sh
```

---

## Lancer les services Rust

### Driver Service

```bash
cd services/driver-service
DATABASE_URL=postgres://postgres:postgres@localhost:5432/ev_platform \
SERVICE_PORT=8080 \
RUST_LOG=info \
cargo run
```

### Admin Service

```bash
cd services/admin-service
DATABASE_URL=postgres://postgres:postgres@localhost:5432/ev_platform \
SERVICE_PORT=8081 \
RUST_LOG=info \
cargo run
```

---

## Lancer les applications frontend

### Driver Web

```bash
cd apps/driver-web
pnpm dev
# Disponible sur http://localhost:5173
```

### Dashboard

```bash
cd apps/dashboard
pnpm dev
# Disponible sur http://localhost:5174
```

### Driver Mobile

```bash
cd apps/driver-mobile
pnpm start
# Scanner le QR code avec Expo Go
```

---

## Docker Compose (tout l'environnement)

```bash
docker compose -f infra/compose/docker-compose.yml up -d
```

Cela lance : postgres, pgadmin, driver-service, admin-service.

Vérifier les health checks :

```bash
curl http://localhost:8080/api/health
curl http://localhost:8081/api/health
```

---

## Tests

```bash
# Tous les tests Rust
cargo test --all

# Test d'un service spécifique
cargo test -p driver-service
cargo test -p admin-service

# Lint Rust
cargo clippy --all-targets -- -D warnings

# Format Rust
cargo fmt --all -- --check

# Tests frontend
pnpm test

# Lint frontend
pnpm lint
```

---

## Structure du projet

```
ev-platform/
├── apps/                    # Applications frontend
│   ├── driver-web/          # React + Vite (Leaflet)
│   ├── driver-mobile/       # React Native + Expo
│   └── dashboard/           # React + Vite
├── services/                # Backend Rust
│   ├── driver-service/      # API publique
│   └── admin-service/       # API admin
├── crates/                  # Crates Rust partagés
│   ├── ev-core/             # NanoIDs, types
│   └── ev-db/               # Pool, pagination
├── packages/                # Packages JS/TS partagés
│   └── ui/                  # Tokens, composants
├── db/                      # Migrations et seeds
│   ├── migrations/
│   └── seeds/
├── infra/                   # Infrastructure
│   ├── compose/
│   └── env/
└── docs/                    # Documentation
```

---

## Commandes utiles

```bash
# Voir les logs d'un service
docker compose logs -f driver-service

# Connexion PostgreSQL
psql -U postgres -d ev_platform

# Voir les schémas
\dn

# Voir les migrations appliquées
SELECT * FROM _sqlx_migrations;
```
