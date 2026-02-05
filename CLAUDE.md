# CLAUDE.md - Flowfile Development Guide

## Project Overview

Flowfile is a visual ETL (Extract, Transform, Load) platform built with a Python backend and Vue.js/Electron frontend. It provides both a visual flow designer and a programmatic Python API for building data pipelines powered by Polars.

**Version:** 0.6.3 | **License:** MIT | **Python:** >=3.10, <3.14 | **Node.js:** 20+

## Repository Structure

This is a **monorepo** managed by Poetry (Python) and npm (frontend):

```
flowfile_core/       # FastAPI backend - ETL engine, flow execution, auth, catalog (port 63578)
flowfile_worker/     # FastAPI compute worker - heavy data processing offload (port 63579)
flowfile_frame/      # Python API library - Polars-like interface for programmatic flow building
flowfile_frontend/   # Electron + Vue 3 desktop/web UI with VueFlow graph editor
flowfile_wasm/       # Browser-only WASM version using Pyodide (lightweight, 14 nodes)
flowfile/            # CLI entry point and web UI launcher
kernel_runtime/      # Docker-based isolated Python code execution environment
shared/              # Shared storage configuration utilities
build_backends/      # PyInstaller build scripts
test_utils/          # Test helpers (PostgreSQL via testcontainers, MinIO/S3)
tools/               # Migration utilities
docs/                # MkDocs documentation site (Material theme)
```

## Architecture

```
Frontend (Electron/Web/WASM) → flowfile_core (port 63578) → flowfile_worker (port 63579)
                                                           → kernel_runtime (Docker, port 9999)
```

- **flowfile_core**: Central FastAPI app managing flows as DAGs, auth (JWT), catalog, secrets, cloud connections
- **flowfile_worker**: Separate FastAPI service for CPU-intensive data operations, process isolation
- **kernel_runtime**: Docker containers for sandboxed user Python code execution
- **flowfile_frame**: Standalone Python library with lazy evaluation, column expressions, DB/cloud connectors
- **Flow graph engine**: `flowfile_core/flowfile_core/flowfile/flow_graph.py` (main DAG execution logic)

## Development Setup

### Python Backend

```bash
# Install all Python dependencies (uses Poetry)
poetry install

# Install with build tools (PyInstaller)
poetry install --with build

# Start core backend
poetry run flowfile_core

# Start worker service
poetry run flowfile_worker
```

### Frontend

```bash
cd flowfile_frontend
npm install

# Development server (web mode, hot reload)
npm run dev:web

# Full Electron dev mode
npm run dev
```

### Full Stack via Docker

```bash
# Copy .env.example to .env and configure
docker compose up -d
# Frontend: http://localhost:8080, Core: :63578, Worker: :63579
```

## Build Commands

| Command | Description |
|---------|-------------|
| `make all` | Full build: Python deps + services + Electron app + master key |
| `make build_python_services` | Build Python backend with PyInstaller |
| `make build_electron_app` | Build Electron desktop app |
| `make build_electron_win/mac/linux` | Platform-specific Electron builds |
| `make generate_key` | Generate Fernet encryption master key |
| `make clean` | Remove all build artifacts |
| `npm run build:web` (in flowfile_frontend/) | Build web-only frontend |

## Testing

### Python Tests (pytest)

```bash
# Run core tests
poetry run pytest flowfile_core/tests

# Run worker tests
poetry run pytest flowfile_worker/tests

# Run frame tests
poetry run pytest flowfile_frame/tests

# Run with coverage (core + worker)
make test_coverage

# Tests requiring Docker (kernel integration)
poetry run pytest -m kernel
```

**Markers:** `worker`, `core`, `kernel` (Docker required)

**Coverage source:** `flowfile_core/flowfile_core`, `flowfile_worker/flowfile_worker`

### Frontend E2E Tests (Playwright)

```bash
cd flowfile_frontend

# Install Playwright browsers
npx playwright install --with-deps chromium

# Web E2E tests (requires backend + preview server running)
npm run test:web

# Electron E2E tests (requires built app)
npm run test:electron

# All tests
npm run test:all
```

**E2E via Makefile:**
```bash
make test_e2e          # Build frontend, start servers, run web tests
make test_e2e_electron  # Full Electron E2E (builds everything first)
```

### WASM Tests (Vitest)

```bash
cd flowfile_wasm
npm run test
npm run test:coverage
```

## Code Style & Linting

### Python (Ruff)

- **Line length:** 120
- **Target:** Python 3.10
- **Rules:** Pyflakes (F), pycodestyle errors/warnings (E/W), isort (I), pyupgrade (UP), flake8-bugbear (B)
- **Format:** Double quotes, space indentation, auto line endings
- **Excluded from linting:** tests/, test_utils/, .pyi files

```bash
# Check
poetry run ruff check .

# Fix
poetry run ruff check --fix .

# Format
poetry run ruff format .
```

### Frontend (ESLint + Prettier)

- **Prettier:** semicolons, 2-space tabs, double quotes, 100 char width, trailing commas, LF line endings
- **ESLint:** Vue 3 recommended + TypeScript + Prettier integration

```bash
cd flowfile_frontend
npm run lint          # ESLint with auto-fix
```

## Key Conventions

### Python

- **Framework:** FastAPI with Pydantic v2 models for request/response validation
- **Data processing:** Polars (not pandas) for all dataframe operations
- **Async:** FastAPI endpoints; heavy work offloaded to worker service
- **Import ordering:** stdlib, third-party, then first-party (`flowfile`, `flowfile_core`, `flowfile_worker`, `flowfile_frame`, `shared`, `test_utils`, `tools`, `build_backends`)
- **FastAPI patterns:** `fastapi.Depends`, `fastapi.Query`, etc. are treated as immutable in bugbear checks
- **Secrets:** Fernet encryption with master key; never commit `master_key.txt`

### Frontend

- **Framework:** Vue 3 Composition API with TypeScript
- **State management:** Pinia stores
- **UI library:** Element Plus
- **Data grids:** AG Grid Community
- **Flow visualization:** VueFlow (@vue-flow/core)
- **HTTP client:** Axios
- **Code editing:** CodeMirror 6 (Python + SQL syntax)
- **Path aliases:** `@` → `src/renderer/app/`, plus `@/api`, `@/types`, `@/stores`, `@/composables`

### File Naming

- Python: snake_case for modules and files
- Vue: PascalCase for components, kebab-case for route paths
- Tests: `test_*.py` for Python, `*.spec.ts` for Playwright

## CI/CD Workflows

| Workflow | Trigger | Description |
|----------|---------|-------------|
| `e2e-tests.yml` | Push/PR to main (frontend/core changes) | Build frontend, start backend, run Playwright web tests |
| `docker-publish.yml` | Push to main, releases | Multi-arch Docker builds (amd64/arm64) → Docker Hub |
| `pypi-release.yml` | Git tags (v*) | Build frontend into static, Poetry build, publish to PyPI |
| `documentation.yml` | Docs changes | Build and deploy MkDocs site |
| `flowfile-wasm-build.yml` | WASM changes | Build WASM version |

## Environment Variables

Key variables (see `.env.example`):

- `FLOWFILE_MODE` - `docker` or unset for local
- `FLOWFILE_ADMIN_USER` / `FLOWFILE_ADMIN_PASSWORD` - Initial admin credentials
- `JWT_SECRET_KEY` - JWT signing secret
- `FLOWFILE_MASTER_KEY` - Fernet key for secrets encryption
- `WORKER_HOST` / `CORE_HOST` - Service discovery between core and worker
- `FLOWFILE_STORAGE_DIR` / `FLOWFILE_USER_DATA_DIR` - Storage paths

## Default Ports

- **63578** - flowfile_core (backend API)
- **63579** - flowfile_worker (compute worker)
- **8080** - Frontend (production/Docker)
- **5173** - Frontend dev server (Vite)
- **4173** - Frontend preview server
- **5174** - WASM dev server
- **9999** - kernel_runtime (Docker execution kernel)

## Important Files

- `flowfile_core/flowfile_core/flowfile/flow_graph.py` - Core DAG execution engine (~127KB)
- `flowfile_frame/flowfile_frame/flow_frame.py` - FlowFrame API (~101KB)
- `flowfile_frame/flowfile_frame/expr.py` - Column expression system (~59KB)
- `flowfile_core/flowfile_core/main.py` - Core FastAPI app with all routers
- `flowfile_worker/flowfile_worker/main.py` - Worker FastAPI app
- `flowfile/flowfile/__main__.py` - CLI entry point (run flows, launch web UI)
- `flowfile_frontend/src/main/main.ts` - Electron main process
- `flowfile_frontend/src/renderer/app/App.vue` - Vue root component

## Things to Avoid

- Do not commit `master_key.txt`, `.env`, or credential files
- Do not use pandas for data operations; this project uses Polars throughout
- Polars has a Windows version ceiling (`<=1.25.2` on Windows due to build issues)
- Tests and test_utils are excluded from Ruff linting (except specific per-file rules)
- The `kernel` pytest marker requires Docker to be available
- Never force-push to `main`; CI builds Docker images and PyPI releases from it
