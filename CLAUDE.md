# CLAUDE.md - AI Assistant Guide for Flowfile

This document provides essential context for AI assistants working with the Flowfile codebase.

## Project Overview

**Flowfile** is a visual ETL (Extract, Transform, Load) tool and Python library that combines drag-and-drop workflow building with the speed of Polars dataframes. Users can:

- Build data pipelines visually using a node-based graph editor
- Transform data programmatically using the `flowfile_frame` API (Polars-like syntax)
- Export visual flows as standalone Python/Polars code for production deployment
- Run flows in the browser via the WASM version (no server required)

**Live Demo**: https://demo.flowfile.org
**Documentation**: https://edwardvaneechoud.github.io/Flowfile/

## Repository Structure

```
Flowfile/
├── flowfile_core/          # FastAPI backend - ETL engine (port 63578)
│   ├── flowfile_core/
│   │   ├── flowfile/       # Core DAG execution engine
│   │   │   ├── flow_graph.py           # Main graph execution (~3900 lines)
│   │   │   ├── flow_node/              # Node type implementations
│   │   │   ├── flow_data_engine/       # Data processing operations
│   │   │   ├── code_generator/         # Python/Polars code export
│   │   │   └── sources/                # Data source handlers
│   │   ├── routes/         # FastAPI endpoints
│   │   ├── schemas/        # Pydantic models (input_schema.py, transform_schema.py)
│   │   ├── database/       # SQLAlchemy models & DB init
│   │   ├── auth/           # JWT authentication
│   │   └── configs/        # Settings and node definitions
│   └── tests/              # pytest tests for core
│
├── flowfile_worker/        # FastAPI worker service (port 63579)
│   └── flowfile_worker/
│       ├── main.py         # FastAPI application
│       ├── routes.py       # Worker API endpoints
│       ├── funcs.py        # Data operation functions
│       └── external_sources/   # S3, SQL source handlers
│
├── flowfile_frontend/      # Electron + Vue 3 desktop application
│   ├── src/renderer/app/
│   │   ├── components/     # Vue components
│   │   │   ├── nodes/      # Node UI components (30+ types)
│   │   │   └── common/     # Reusable widgets
│   │   ├── stores/         # Pinia state management
│   │   ├── api/            # API client layer
│   │   ├── services/       # Business logic
│   │   └── types/          # TypeScript definitions
│   ├── main/               # Electron main process
│   └── tests/              # Playwright E2E tests
│
├── flowfile_frame/         # Python library with Polars-like API
│   ├── flow_frame.py       # Main FlowFrame class (~3400 lines)
│   ├── expr.py             # Expression system (~2000 lines)
│   └── lazy.py             # Lazy evaluation
│
├── flowfile_wasm/          # Browser-based WASM version (Pyodide)
│   └── src/                # Vue components for lite version
│
├── flowfile/               # Main package entry point & CLI
├── shared/                 # Shared utilities (storage config)
├── test_utils/             # Testing infrastructure & fixtures
├── tools/                  # Migration tools
├── docs/                   # MkDocs documentation
└── .github/workflows/      # CI/CD pipelines
```

## Technology Stack

### Backend (Python)
- **Framework**: FastAPI (async)
- **Data Processing**: Polars (high-performance DataFrames)
- **Database**: SQLAlchemy + databases (async)
- **Auth**: JWT (python-jose), bcrypt
- **Server**: Uvicorn (ASGI)
- **Python Version**: 3.10 - 3.13

### Frontend (TypeScript)
- **Framework**: Vue 3 + TypeScript
- **Desktop**: Electron 36
- **UI Components**: Element Plus
- **Flow Editor**: Vue Flow (DAG visualization)
- **State**: Pinia
- **Build**: Vite, electron-builder

### DevOps
- **Containerization**: Docker, Docker Compose
- **Testing**: pytest (unit), Playwright (E2E)
- **Linting**: ruff (Python), ESLint + Prettier (TypeScript)
- **CI/CD**: GitHub Actions
- **Docs**: MkDocs Material

## Quick Commands

### Development Setup
```bash
# Install Python dependencies
poetry install

# Start backend services (separate terminals)
poetry run flowfile_worker   # Worker on :63579
poetry run flowfile_core     # Core on :63578

# Start frontend
cd flowfile_frontend
npm install && npm run dev:web   # Web on :8080
```

### Build Commands
```bash
make                         # Full build (Python + Electron)
make install_python_deps     # Install Python with Poetry
make build_python_services   # Compile backends
make build_electron_app      # Build Electron app
make build_electron_linux    # Platform-specific builds
make build_electron_mac
make build_electron_win
```

### Testing
```bash
# Python tests
poetry run pytest flowfile_core/tests -v
poetry run pytest flowfile_worker/tests -v
make test_coverage           # Run with coverage report

# Frontend E2E tests
make test_e2e                # Full E2E suite
cd flowfile_frontend && npm run test:web   # Web E2E only
cd flowfile_frontend && npm run test:electron   # Electron E2E
```

### Docker
```bash
docker compose up -d         # Start all services
# Access at http://localhost:8080
```

### Linting
```bash
poetry run ruff check .      # Python linting
poetry run ruff format .     # Python formatting
cd flowfile_frontend && npm run lint   # TypeScript/Vue linting
```

## Key Architecture Patterns

### 1. DAG-Based Execution
The core engine (`flow_graph.py`) represents workflows as Directed Acyclic Graphs:
- **Nodes** = data operations (Read, Filter, Join, Group By, etc.)
- **Edges** = data flow between operations
- Execution respects topological ordering

### 2. Node Registration System
Nodes are defined in `flowfile_core/configs/node_store/nodes.py` using `NodeTemplate`:
```python
NodeTemplate(
    name="Filter data",
    item="filter",              # Unique identifier
    input=1, output=1,          # Number of ports
    node_type="process",        # input/process/output
    transform_type="narrow",    # narrow/wide/other
    node_group="transform",     # UI grouping
)
```

### 3. Pydantic Schema Validation
All node settings use Pydantic models in `schemas/input_schema.py`:
```python
class NodeFilter(BaseModel):
    filter_condition: str
    # ...
```

### 4. Micro-Service Architecture
- **Core Service** (63578): Flow management, node config, auth, catalog
- **Worker Service** (63579): Heavy computation, caching, data I/O
- Single-file mode available for Python package installs

### 5. Frontend State Management
Pinia stores organized by concern:
- `flow-store.ts` - Flow graph state
- `node-store.ts` - Node data and validation
- `editor-store.ts` - UI state (drawer, code generator)
- `results-store.ts` - Execution results

## Code Style & Conventions

### Python
- **Line length**: 120 characters
- **Linter**: ruff with rules: F, E, W, I (isort), UP (pyupgrade), B (bugbear)
- **Import order**: stdlib → third-party → local (enforced by isort)
- **Type hints**: Use modern syntax (`list[str]` not `List[str]`)
- **Docstrings**: Google-style for public functions
- **Naming**: snake_case for functions/variables, PascalCase for classes

### TypeScript/Vue
- **Style**: ESLint + Prettier
- **Naming**: camelCase for functions/variables, PascalCase for components
- **State**: Use Pinia stores, avoid prop drilling
- **Deprecated patterns**: Legacy snake_case getters exist with `@deprecated` tags

### FastAPI Endpoints
- Use dependency injection with `Depends()`
- Router-level auth: `router = APIRouter(dependencies=[Depends(get_current_active_user)])`
- Return Pydantic models or use `response_model=`
- Handle errors with `HTTPException`

## Testing Patterns

### Python Tests
```python
# Use pytest fixtures from conftest.py
@pytest.fixture(scope="session", autouse=True)
def setup_test_db():
    """Setup test database"""
    init_db()
    yield
    # Cleanup

# Mark tests with markers
@pytest.mark.core
def test_flow_execution():
    pass

@pytest.mark.worker
def test_worker_computation():
    pass
```

### E2E Tests (Playwright)
Tests in `flowfile_frontend/tests/`:
- `web-flow.spec.ts` - Web interface tests
- `app.spec.ts` - Electron app tests
- `complex-flow.spec.ts` - Complex workflow tests

## Environment Configuration

### Required Environment Variables
```bash
# Authentication
FLOWFILE_ADMIN_USER=admin
FLOWFILE_ADMIN_PASSWORD=changeme
JWT_SECRET_KEY=your-secure-key

# Encryption (for secrets storage)
FLOWFILE_MASTER_KEY=  # Fernet key, generate with make generate_key

# Service configuration
FLOWFILE_MODE=electron|docker|package
FLOWFILE_STORAGE_DIR=/path/to/storage
WORKER_HOST=localhost
WORKER_PORT=63579
```

### Ports
- **63578**: Core service (FastAPI)
- **63579**: Worker service (FastAPI)
- **8080**: Frontend (web/Docker)

## Common Development Tasks

### Adding a New Node Type
1. Define `NodeTemplate` in `flowfile_core/configs/node_store/nodes.py`
2. Create Pydantic settings model in `schemas/input_schema.py`
3. Add entry to `NODE_TYPE_TO_SETTINGS_CLASS` in `schemas/schemas.py`
4. Implement node logic in `flowfile_core/flowfile/flow_node/`
5. Create Vue component in `flowfile_frontend/src/renderer/app/components/nodes/node-types/`
6. Add tests

### Adding a New API Endpoint
1. Add route in appropriate file under `flowfile_core/routes/`
2. Use Pydantic models for request/response
3. Add authentication if needed via dependencies
4. Write tests in `flowfile_core/tests/`

### Modifying Frontend State
1. Update/create store in `flowfile_frontend/src/renderer/app/stores/`
2. Use Pinia composition API or options API
3. Avoid direct store access in components - use composables
4. Mark legacy patterns with `@deprecated`

## Important Files

| File | Purpose |
|------|---------|
| `flowfile_core/flowfile_core/flowfile/flow_graph.py` | Core DAG execution engine |
| `flowfile_frame/flow_frame.py` | Polars-like API implementation |
| `flowfile_core/configs/node_store/nodes.py` | Node type definitions |
| `flowfile_core/schemas/input_schema.py` | Node settings Pydantic models |
| `flowfile_core/routes/routes.py` | Main API endpoints |
| `flowfile_frontend/src/renderer/app/stores/flow-store.ts` | Frontend flow state |
| `pyproject.toml` | Python dependencies and config |
| `Makefile` | Build automation |

## CI/CD Workflows

| Workflow | Trigger | Purpose |
|----------|---------|---------|
| `e2e-tests.yml` | Push/PR to main | Run Playwright E2E tests |
| `docker-publish.yml` | Release | Build & publish Docker images |
| `pypi-release.yml` | Release | Publish to PyPI |
| `flowfile-wasm-build.yml` | Push to main | Build WASM version |
| `documentation.yml` | Push to main | Deploy MkDocs |

## Gotchas & Tips

1. **Polars Windows limitation**: Version capped at 1.25.2 on Windows due to compatibility issues
2. **Worker dependency**: Some tests require the worker service; use `SKIP_WORKER_TESTS=1` to skip
3. **Database testing**: Uses SQLite in-memory for tests; PostgreSQL via testcontainers for integration
4. **Vue Flow**: Custom node components must follow Vue Flow's node interface
5. **Secrets**: Never commit `.env` files; use `.env.example` as template
6. **WASM version**: Limited to 14 essential nodes; doesn't support all features
7. **Code generation**: Visual flows can be exported to Python via the "Generate Code" button
