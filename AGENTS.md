<!-- SPECKIT START -->
For additional context about technologies to be used, project structure,
shell commands, and other important information, read the current plan
at specs/001-infra-database-setup/plan.md

Implementation tasks are defined in specs/001-infra-database-setup/tasks.md

## Project Structure

source/           ← ALL runtime code
├── shared/        ← Shared Rust crates
│   ├── ev-core/   ← Core domain types, traits
│   ├── ev-auth/   ← Authentication helpers
│   └── ev-db/     ← Database access layer
├── services/      ← Rust microservices
│   ├── driver-service/ ← Rust/Actix :8080
│   └── admin-service/  ← Rust/Actix :8081
├── front/         ← Mobile and web apps
│   ├── mobile-driver/ ← Expo SDK 54 app
│   ├── web-driver/    ← React + Leaflet
│   └── dashboard/     ← React + shadcn/ui

docs/  ← Documentation only
infra/ ← Docker, migrations, configs
scripts/ ← Build tools, seed scripts
<!-- SPECKIT END -->
