# CLAUDE.md - AI Assistant Guide for Flowfile

This document provides AI assistants with essential context for working in the Flowfile codebase.

## Project Overview

Flowfile is a visual ETL (Extract, Transform, Load) platform with three deployment models:
- **Desktop** - Electron app with embedded Python backend services
- **Server** - Docker Compose with separate frontend (Nginx), core (FastAPI), and worker (FastAPI) containers
- **Browser** - Standalone WebAssembly version using Pyodide + Polars (no backend required)

**Version**: 0.6.1 | **License**: MIT | **Python**: 3.10 - 3.13

## Repository Structure

```
Flowfile/
├── flowfile_core/          # ETL engine - FastAPI backend (port 63578)
├── flowfile_worker/        # Computation service - FastAPI (port 63579)
├── flowfile_frame/         # Python DataFrame API (Polars-like interface)
├── flowfile/               # CLI entry point & unified Python package
├── flowfile_frontend/      # Electron + Vue 3 desktop application
├── flowfile_wasm/          # Browser-based WASM version (Pyodide)
├── shared/                 # Shared storage configuration
├── test_utils/             # Test infrastructure (Postgres, S3 containers)
├── tools/                  # Database migration tools
├── build_backends/         # Python service build scripts (PyInstaller)
├── docs/                   # MkDocs documentation site
├── pyproject.toml          # Poetry config for all Python packages
├── Makefile                # Cross-platform build automation
└── docker-compose.yml      # Multi-service Docker setup
```

### Package Dependency Graph

```
flowfile (CLI) ──────────┐
  ├── flowfile_core       │  flowfile_frontend (Electron UI)
  │     └── polars        │    ├── Vue 3 + TypeScript
  ├── flowfile_worker     │    └── HTTP calls to core & worker APIs
  │     └── fastapi       │
  └── flowfile_frame      │  flowfile_wasm (Browser WASM)
        └── flowfile_core │    ├── Vue 3 + Pyodide
                          │    └── Polars (via WASM)
```

## Build System & Commands

### Python (Poetry)

All Python packages are managed by a single `pyproject.toml` at the root.

```bash
# Install dependencies
poetry install                    # Base dependencies
poetry install --with dev         # Include dev tools (pytest, ruff, mkdocs)
poetry install --with build       # Include PyInstaller for packaging

# Run services
poetry run flowfile_core          # Start core API on port 63578
poetry run flowfile_worker        # Start worker API on port 63579
poetry run flowfile               # Start full application (CLI)

# Run tests
poetry run pytest flowfile_core/tests --disable-warnings
poetry run pytest flowfile_worker/tests --disable-warnings
poetry run pytest flowfile_frame/tests --disable-warnings
poetry run pytest flowfile/tests --disable-warnings

# Run tests with coverage (core + worker only)
poetry run pytest flowfile_core/tests --cov --cov-report= --disable-warnings
poetry run pytest flowfile_worker/tests --cov --cov-append --cov-report= --disable-warnings
poetry run coverage report --show-missing

# Test infrastructure (Docker-based mock services)
poetry run start_postgres         # Start mock PostgreSQL with sample data
poetry run stop_postgres
poetry run start_minio            # Start mock S3 bucket with sample data
poetry run stop_minio

# Linting
poetry run ruff check .           # Lint Python code
poetry run ruff format .          # Format Python code
```

### Frontend (npm)

Frontend is in `flowfile_frontend/`. Uses Node.js 20+/22+ and npm.

```bash
cd flowfile_frontend

npm install                       # Install dependencies
npm run dev                       # Electron + Hot reload development
npm run dev:web                   # Web-only dev server (Vite)
npm run build                     # Full build (lint + typecheck + Electron package)
npm run build:web                 # Web-only production build
npm run lint                      # ESLint with auto-fix
vue-tsc --noEmit                  # TypeScript type checking

# E2E tests (Playwright)
npm run test                      # Electron app tests
npm run test:web                  # Web flow tests
npm run test:all                  # All Playwright tests
```

### WASM (npm)

Browser WASM version is in `flowfile_wasm/`.

```bash
cd flowfile_wasm

npm install
npm run dev                       # Vite dev server
npm run build                     # Production build
npx vitest                        # Run unit + integration tests
```

### Makefile Targets

The root `Makefile` provides cross-platform (Windows/Linux/macOS) build automation:

```bash
make all                          # Full build: deps + services + Electron + key
make install_python_deps          # Poetry install with build group
make build_python_services        # Build backend services (PyInstaller)
make build_electron_app           # Build Electron desktop app
make test_coverage                # Run Python tests with coverage
make test_e2e                     # Run E2E tests (web version)
make test_e2e_electron            # Run Electron E2E tests
make clean                        # Remove all build artifacts
make generate_key                 # Generate master encryption key
```

### Docker

```bash
docker compose up --build         # Build and start all services
docker compose up -d              # Detached mode
```

Services: `flowfile-frontend` (:8080), `flowfile-core` (:63578), `flowfile-worker` (:63579)

## Testing

### Python Tests (pytest)

- **Framework**: pytest with markers `core` and `worker`
- **Coverage**: pytest-cov targeting `flowfile_core/flowfile_core` and `flowfile_worker/flowfile_worker`
- **Test locations**:
  - `flowfile_core/tests/` - Core ETL engine tests
  - `flowfile_worker/tests/` - Worker computation tests
  - `flowfile_frame/tests/` - DataFrame API tests
  - `flowfile/tests/` - CLI and API tests
  - `tools/migrate/tests/` - Migration tool tests
- **Infrastructure**: Tests use `testcontainers` for Docker-based Postgres and MinIO (S3)
- **Important**: Run core and worker tests separately to avoid import collisions:
  ```bash
  poetry run pytest flowfile_core/tests --disable-warnings
  poetry run pytest flowfile_worker/tests --disable-warnings
  ```

### Frontend Tests (Playwright)

- **Config**: `flowfile_frontend/playwright.config.ts`
- **Test files**:
  - `tests/app.spec.ts` - Electron desktop app integration tests
  - `tests/complex-flow.spec.ts` - Complex flow scenarios
  - `tests/web-flow.spec.ts` - Web version E2E tests
- **Requires**: Backend running on port 63578 for web tests

### WASM Tests (Vitest)

- **Config**: `flowfile_wasm/vitest.config.ts`
- **Test directories**: `tests/unit/`, `tests/integration/`, `tests/components/`

## Code Style & Conventions

### Python

- **Linter/Formatter**: Ruff (configured in `pyproject.toml`)
- **Line length**: 120 characters
- **Target**: Python 3.10+
- **Quote style**: Double quotes
- **Import ordering**: isort via Ruff (standard lib, third-party, local)
- **Lint rules**: F (pyflakes), E (pycodestyle errors), W (warnings), I (isort), UP (pyupgrade), B (flake8-bugbear)
- **First-party packages**: `flowfile`, `flowfile_core`, `flowfile_worker`, `flowfile_frame`, `shared`, `test_utils`, `tools`, `build_backends`
- **FastAPI patterns**: Uses `fastapi.Depends`, `fastapi.Query`, etc. (configured as immutable calls in bugbear)
- **Data processing**: Polars is the primary DataFrame library (not pandas)
- **Models**: Pydantic v2 for data validation
- **Async**: FastAPI with uvicorn, asyncio event loop

### TypeScript / Vue

- **Framework**: Vue 3 with Composition API + TypeScript
- **Build tool**: Vite (ESM)
- **State management**: Pinia stores
- **UI components**: Element Plus, AG Grid
- **Flow visualization**: Vue Flow
- **Code editor**: CodeMirror 6
- **Linter**: ESLint with Vue 3 + TypeScript rules
- **Formatter**: Prettier (100 char width, double quotes, semicolons, trailing commas, LF line endings)
- **Path aliases**: `@/*` maps to `src/renderer/app/*`
- **Linebreak style**: Unix (LF) enforced by ESLint
- **TypeScript**: Strict mode enabled, ESNext target
- **Rules relaxed**: `@typescript-eslint/no-explicit-any` and `@typescript-eslint/no-non-null-assertion` are allowed

### File Naming

- Python: `snake_case.py` for modules
- TypeScript/Vue: `camelCase.ts` for utilities, `PascalCase.vue` for components
- Test files: `test_*.py` (Python), `*.spec.ts` (TypeScript)

## Architecture Details

### Service Communication

- **flowfile_core** (port 63578): Central orchestration API - manages flow graphs, node execution, auth, secrets
- **flowfile_worker** (port 63579): Computation offloading - handles heavy data transforms, caching, streaming
- **Frontend**: Communicates with both services via HTTP (axios)
- **Electron**: Main process manages Python service lifecycle, IPC bridges to renderer

### Key Source Files

| File | Description |
|------|-------------|
| `flowfile_core/flowfile_core/main.py` | Core FastAPI app entry point |
| `flowfile_core/flowfile_core/flowfile/flow_graph.py` | DAG flow graph management (large: ~116KB) |
| `flowfile_core/flowfile_core/flowfile/flow_node/` | Node implementations and execution |
| `flowfile_core/flowfile_core/routes/` | API route handlers (auth, routes, secrets, logs, cloud) |
| `flowfile_worker/flowfile_worker/main.py` | Worker FastAPI app entry point |
| `flowfile_worker/flowfile_worker/funcs.py` | Core transform functions |
| `flowfile_worker/flowfile_worker/routes.py` | Worker API endpoints |
| `flowfile_frame/flowfile_frame/flowfile_frame.py` | Main DataFrame class (~101KB) |
| `flowfile_frame/flowfile_frame/expr.py` | Expression API (~59KB) |
| `flowfile_frontend/src/main/main.ts` | Electron main process entry |
| `flowfile_frontend/src/main/services.ts` | Python service lifecycle management |
| `flowfile_frontend/src/renderer/app/` | Vue application root |
| `flowfile/flowfile/__main__.py` | CLI entry point |
| `flowfile/flowfile/api.py` | Server startup and management |

### Environment Variables

Key environment variables (see `.env.example`):
- `FLOWFILE_MODE` - `electron` (default) or `docker`
- `FLOWFILE_MASTER_KEY` - Fernet encryption key for secrets
- `FLOWFILE_ADMIN_USER` / `FLOWFILE_ADMIN_PASSWORD` - Default admin credentials
- `JWT_SECRET_KEY` - JWT token signing key
- `WORKER_HOST` / `CORE_HOST` - Service discovery in Docker
- `FLOWFILE_STORAGE_DIR` / `FLOWFILE_USER_DATA_DIR` - Storage paths

## CI/CD Pipelines

All workflows are in `.github/workflows/`:

| Workflow | File | Trigger | Purpose |
|----------|------|---------|---------|
| Run Tests | `test.yaml` | Push/PR to main | Backend (Python 3.10-3.13, multi-OS), frontend build, Electron tests, docs |
| E2E Tests | `e2e-tests.yml` | Push/PR to main (frontend/core changes) | Playwright web E2E tests |
| Docker Publish | `docker-publish.yml` | Push to main, releases | Multi-arch Docker images (amd64 + arm64) |
| WASM Build | `flowfile-wasm-build.yml` | WASM changes | Build and deploy browser version |
| PyPI Release | `pypi-release.yml` | Releases | Publish to PyPI |
| Release | `release.yaml` | Tags | GitHub release creation |
| Docs | `documentation.yml` | Doc changes | MkDocs deployment |
| CodeQL | `codeql.yaml` | Scheduled/PR | Code quality analysis |

**Change detection**: CI uses `dorny/paths-filter` to run only relevant tests based on which files changed. Backend, frontend, docs, and shared code changes are detected independently.

## Common Development Workflows

### Adding a New Python Feature

1. Write code in the appropriate package (`flowfile_core`, `flowfile_worker`, or `flowfile_frame`)
2. Add tests in the package's `tests/` directory
3. Run `poetry run ruff check .` and `poetry run ruff format .`
4. Run relevant tests: `poetry run pytest <package>/tests --disable-warnings`

### Adding a New Frontend Feature

1. Work in `flowfile_frontend/src/renderer/app/`
2. Components go in `components/`, stores in `stores/`, types in `types/`
3. Run `npm run lint` from `flowfile_frontend/`
4. Run `npm run build:web` to verify the build succeeds

### Adding a New Node Type

- Node implementations: `flowfile_core/flowfile_core/flowfile/flow_node/`
- Node designer/registration: `flowfile_core/flowfile_core/flowfile/node_designer/`
- Frontend node settings: `flowfile_frontend/src/renderer/app/components/nodes/`
- See `docs/for-developers/creating-custom-nodes.md` for the full guide

### Running the Full Stack Locally

```bash
# Terminal 1: Start core
poetry run flowfile_core

# Terminal 2: Start worker
poetry run flowfile_worker

# Terminal 3: Start frontend dev server
cd flowfile_frontend && npm run dev:web
```

Or use the Electron dev mode: `cd flowfile_frontend && npm run dev`

## Important Notes

- **Polars, not pandas**: This project uses Polars as its primary data processing library. Polars has different syntax from pandas.
- **Large files**: `flow_graph.py` (~116KB) and `flowfile_frame.py` (~101KB) are very large. Read targeted sections rather than loading them entirely.
- **Stub generation**: `flowfile_frame` includes stub generators (`expr_stub_generator.py`, `flow_frame_stub_generator.py`) that auto-generate `.pyi` type stubs.
- **Cross-platform**: The Makefile and CI support Windows, macOS, and Linux. Watch for OS-specific path handling.
- **Test isolation**: Core and worker tests must run in separate pytest invocations to avoid import collisions.
- **Docker mode**: When `FLOWFILE_MODE=docker`, services discover each other via container names (`flowfile-core`, `flowfile-worker`) instead of localhost.
- **Security**: Secrets are encrypted with Fernet. The master key is stored in `master_key.txt` (generated via `make generate_key`). Never commit `.env` or `master_key.txt`.
