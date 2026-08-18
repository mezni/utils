# Project Name

> Short description of the project.

## Overview

This project is a Python application designed to provide:

* Core application functionality
* A structured and maintainable codebase
* Automated testing
* Code quality and linting
* Reproducible dependency management with `uv`

## Requirements

* Python 3.12+
* [`uv`](https://docs.astral.sh/uv/)

## Project Structure

```text
.
├── app/
│   └── ...
├── tests/
│   └── ...
├── docs/
│   └── ...
├── .env.example
├── .gitignore
├── pyproject.toml
├── README.md
└── uv.lock
```

## Getting Started

### 1. Clone the repository

```bash
git clone <repository-url>
cd <project-directory>
```

### 2. Install dependencies

Using `uv`:

```bash
uv sync
```

This creates the project's virtual environment and installs the dependencies defined in `pyproject.toml` and `uv.lock`.

### 3. Activate the virtual environment

Linux/macOS:

```bash
source .venv/bin/activate
```

Windows PowerShell:

```powershell
.venv\Scripts\Activate.ps1
```

You can also run commands directly through `uv` without activating the environment:

```bash
uv run python --version
```

## Development

Run the application:

```bash
uv run python -m app
```

Run tests:

```bash
uv run pytest
```

Run the linter:

```bash
uv run ruff check .
```

Format the code:

```bash
uv run ruff format .
```

## Configuration

Create a local `.env` file based on `.env.example`:

```bash
cp .env.example .env
```

Environment-specific configuration should not be committed to Git.

## Testing

Tests are located in the `tests/` directory.

Run the complete test suite:

```bash
uv run pytest
```

For verbose output:

```bash
uv run pytest -v
```

## Code Quality

This project uses:

* **Ruff** — linting and formatting
* **Pytest** — testing
* **uv** — dependency and environment management

Before committing changes:

```bash
uv run ruff check .
uv run ruff format .
uv run pytest
```

## Git Workflow

Create a feature branch:

```bash
git checkout -b feature/my-feature
```

Commit your changes:

```bash
git add .
git commit -m "feat: add my feature"
```

Push the branch:

```bash
git push origin feature/my-feature
```

## License

Specify the project license here.

## Author

Project maintained by the project team.
