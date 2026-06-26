# BorneMap

**Location-Based Discovery & Navigation Platform**

## Project Overview

BorneMap is a modern location-based discovery and navigation platform that helps users explore, discover, and navigate to points of interest with precision and ease.

## Quick Start

### Prerequisites
- Rust 1.70+ (for backend development)
- Node.js 18+ (for frontend development)
- Cargo and npm installed

### Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd BorneMap
   ```

2. **Set up environment variables**
   ```bash
   cp .env.example .env
   ```

3. **Install dependencies**
   ```bash
   # Backend
   cd backend
   cargo install --path .
   
   # Frontend
   cd ../frontend
   npm install
   ```

4. **Configure your application**
   - Edit `.env` file with your configuration
   - Set up database credentials
   - Configure authentication providers
   - Set up environment-specific settings

5. **Run the development servers**
   ```bash
   # Backend (Rust)
   cd backend
   cargo run --release
   
   # Frontend (Node.js)
   cd frontend
   npm run dev
   ```

## Project Structure

```
BorneMap/
├── backend/              # Rust backend with Actix-web
│   ├── src/
│   │   ├── api/         # REST API endpoints
│   │   ├── models/      # Data models
│   │   ├── services/    # Business logic
│   │   └── main.rs
│   └── Cargo.toml
├── frontend/            # React/Next.js frontend
│   ├── src/
│   │   ├── components/  # Reusable UI components
│   │   ├── pages/       # Page components
│   │   ├── services/    # API clients
│   │   └── App.tsx
│   └── package.json
├── docs/                # Project documentation
│   ├── core/            # Core principles and guidelines
│   ├── architecture/    # System architecture
│   ├── quality/         # Quality assurance
│   └── sprints/         # Sprint documentation
├── tests/               # Integration and end-to-end tests
├── .env.example         # Environment variables template
├── .gitignore
└── README.md
```

## Development Guidelines

### Security First
- All code must adhere to the [Security Protocols](docs/core/security-protocols.md)
- Never hardcode credentials or secrets
- Implement proper input validation and sanitization
- Follow principle of least privilege

### UI/UX Standards
- All interfaces must follow the [UI/UX Pro Max Guide](docs/core/ui-ux-guidelines.md)
- Ensure WCAG 2.1 AA compliance
- Build mobile-first, responsive interfaces
- Include comprehensive feedback mechanisms

### Quality Assurance
- Write tests for all new features (unit, integration, E2E)
- Ensure all code passes `cargo clippy --all-targets -- -D warnings`
- Follow Rust idiomatic patterns and error handling
- Maintain strict type safety

### Project Workflow
- Follow the [Speckit Framework](docs/core/constitution.md) for all sprint work
- Each sprint operates in an isolated branch: `sprint-[X]-[feature-name]`
- Document all changes in `/docs/sprints/sprint-[X].md`
- Track known issues in `/docs/quality/issue-tracker.md`

## API Documentation

API endpoints are documented in `/docs/architecture/api-specification.md`

## Contributing

1. Create a new branch for your work: `git checkout -b sprint-[X]-[feature-name]`
2. Follow the Speckit lifecycle (Specify → Plan → Tasks → Implement)
3. Write comprehensive tests
4. Ensure all code meets quality standards
5. Submit a pull request with clear description

## Testing

### Running Tests
```bash
# Backend unit tests
cd backend
cargo test

# Integration tests
cd backend
cargo test --test integration

# Frontend tests
cd frontend
npm test
```

### Test Coverage
- Target >80% code coverage for new features
- Include both happy and unhappy path tests
- Document test cases in inline comments

## Deployment

The project supports multiple deployment targets:
- **Development**: Local development servers
- **Staging**: Test environment with production-like configuration
- **Production**: Full-scale deployment with all features

See `/docs/architecture/deployment.md` for detailed deployment instructions.

## Support

For questions or issues:
- Open an issue on the project repository
- Review the documentation in `/docs`
- Check the [AI Agent Constitution](docs/core/constitution.md) for development guidelines

## License

[Your License Here]

## Version History

### [0.2.0] - Core Identity + Token Foundation (Sprint 01)
- Complete authentication system with user registration and login
- Secure password handling with Argon2id
- JWT token management with Ed25519 signing
- Refresh token rotation with Redis revocation
- Complete audit logging system
- Docker Compose development environment
- Comprehensive test suite

### [0.1.0] - Project Initialization (Sprint 0)
- Project structure created
- Documentation framework established
- Development guidelines defined
- Security protocols implemented
- UI/UX Pro Max guidelines defined

See [CHANGELOG.md](CHANGELOG.md) for detailed version history and release notes.