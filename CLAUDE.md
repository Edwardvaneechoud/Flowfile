# CLAUDE.md - Flowfile Development Guide

## Project Overview

Flowfile is a visual ETL (Extract, Transform, Load) platform built with a Python backend and Vue 3/Tauri frontend. It pairs a visual flow designer with a programmatic, Polars-compatible Python API (`flowfile_frame`) for building data pipelines, and bundles a data catalog (Delta Lake storage), an embedded scheduler, Kafka ingestion, and sandboxed Python execution. The full stack ships as a single `pip install flowfile`.

**Version:** 0.11.0 | **License:** MIT | **Python:** >=3.10, <3.14 | **Node.js:** 20+ (CI runs 20 and 22; no `engines`/`.nvmrc` pin)

## Repository Structure

This is a **monorepo** managed by Poetry (Python) and npm (frontend):

```
flowfile_core/       # FastAPI backend - ETL engine, flow execution, auth, catalog, AI (port 63578)
flowfile_worker/     # FastAPI compute worker - heavy data processing offload (port 63579)
flowfile_frame/      # Python API library - Polars-like interface for programmatic flow building
flowfile_frontend/   # Tauri 2 (Rust shell) + Vue 3 desktop/web UI with VueFlow graph editor
flowfile_scheduler/  # Embedded scheduler for recurring flow runs
flowfile_wasm/       # Browser-only WASM version using Pyodide (lightweight, 16 nodes)
flowfile/            # CLI entry point and web UI launcher
kernel_runtime/      # Docker-based isolated Python code execution environment (port 9999)
shared/              # Cross-service package (storage_config, crypto, cloud_storage, kafka, ml, rest_api, google_analytics)
build_backends/      # PyInstaller build scripts
test_utils/          # Docker-backed test fixtures (postgres, mysql, s3/MinIO, gcs, azurite, kafka)
tools/               # Schema migration (migrate/) + Tauri sidecar staging/signing (rename_sidecar.py)
docs/                # MkDocs documentation site (Material theme)
docker-remote/       # Compose stack using published Docker Hub images (remote/server deploy)
```

Each Python package uses a nested layout: the importable code lives one level
down (e.g. `flowfile_core/flowfile_core/`) with a sibling `tests/` dir.

The 8 main packages each have their own `CLAUDE.md` with package-specific
architecture, conventions, and gotchas (`flowfile_core/`, `flowfile_worker/`,
`flowfile_frame/`, `flowfile_frontend/`, `flowfile_scheduler/`, `flowfile_wasm/`,
`kernel_runtime/`, `shared/`) — read the relevant one when working inside a
package. Paths in those docs are relative to the package's own directory.

## Architecture

```
Frontend (Tauri / Web)  ──HTTP──►  flowfile_core (:63578)  ──HTTP──►  flowfile_worker (:63579)
                                          │                            (spawned subprocesses hold dataset memory)
                                          └──Docker SDK──►  kernel_runtime containers (uvicorn :9999 in-container,
                                                                                       host-mapped to 19000-19999)
WASM frontend (Pyodide) runs fully in-browser — no core/worker/kernel.
```

- **flowfile_core** (`:63578`): Central FastAPI app. Manages flows as DAGs, auth (JWT), catalog, secrets, cloud + GA connections, the AI subsystem, and orchestrates kernel Docker containers (via the `docker` SDK). Routers are wired in `flowfile_core/flowfile_core/main.py`.
- **flowfile_worker** (`:63579`): Separate FastAPI service for CPU-intensive data ops. Each job runs in a **spawned subprocess** (`mp_context = get_context("spawn")` in `flowfile_worker/__init__.py`), so dataset memory lives in killable children, never the FastAPI process.
- **kernel_runtime**: Docker containers for sandboxed user Python code. Each serves `uvicorn ... --port 9999` inside the container (`EXPOSE 9999`); core maps that to a host port in `19000-19999` and the kernel calls back to core on `:63578`.
- **flowfile_frame**: Polars-like Python API (lazy evaluation, column expressions in `expr.py`, DB/cloud connectors). **Not standalone** — it imports `flowfile_core` directly to build in-process `FlowGraph` objects and ships in the same monorepo distribution.
- **Flow graph engine**: `flowfile_core/flowfile_core/flowfile/flow_graph.py` (main DAG execution logic, ~224KB).

**Core/worker contract:** core must **not** materialise LazyFrames (no `.collect()` on the hot path). With `FLOWFILE_OFFLOAD_TO_WORKER` (default on) core serializes the LazyFrame and POSTs it to the worker at `WORKER_HOST:FLOWFILE_WORKER_PORT`; the worker holds the resulting dataset in its spawned children. Core ships paths/JSON, not in-memory frames. The **scheduler** is embedded in core (no separate service/port) and only auto-starts when `FLOWFILE_SCHEDULER_ENABLED` is set.

## Subsystems & Cross-Package Contracts

- **AI** (`flowfile_core/flowfile_core/ai/`): LLM agent stack behind the `/ai/*` router. Surfaces in `ai/agents/` (`assist` single-shot, `copilot` next-step, `planner` multi-turn diff-staged graph edits). Providers in `ai/providers/registry.py` are **litellm-backed** (anthropic, openai, google, groq, openrouter, ollama, local); BYOK per-user encrypted keys in `ai/byok.py` + `ai/credentials.py`; on-demand local llama.cpp model in `ai/local_model/`. The whole router is gated by `FEATURE_FLAG_AI` (a runtime-flippable `MutableBool`, default on) → 503 when off. **Keep the package litellm-import-free except `ai/byok.py`** — importing `ai.credentials` / `ai.feature_flag` must do no provider I/O; lazy-contract tests enforce this. AI tests live in `flowfile_core/tests/ai/`.
- **Database & migrations**: one SQLite catalog DB (`flowfile_catalog.db`) shared by core, scheduler, and worker. URL resolved in `shared/storage_config.get_database_url()`. Schema changes use **Alembic** (`flowfile_core/flowfile_core/alembic/versions/NNN_*.py`, currently 001–010), run automatically at core startup via `flowfile_core/database/migration.py`. Add a migration when changing `flowfile_core/database/models.py`; keep the numeric `NNN_` prefix sequence.
- **`shared/` layer**: import-only-downward utilities for core/worker/scheduler/kernel. `shared/storage_config.py` is the single source of truth for on-disk paths via the `storage` singleton — two roots: **internal** (`base_directory`, `~/.flowfile` locally / `/app/internal_storage` in Docker) vs **user data** (flows, uploads, outputs). Kernel-exchange/artifact dirs must stay under the kernel shared volume so Docker kernels can read/write them — don't relocate them to `base_directory`.
- **Secrets & API keys**: user secrets use a Fernet master key → **HKDF per-user key**. Stored format `$ffsec$1$<user_id>$<token>` embeds the user_id so the **worker re-derives the key independently of core** (`flowfile_core/secret_manager/secret_manager.py`, `flowfile_worker/secrets.py`) — don't change the format without migrating both sides. API keys are hashed with **SHA-256** (`flowfile_core/auth/api_key.py`): intentional for 256-bit tokens; the CodeQL weak-hash alert is a known false positive, not a bug to "fix" with a KDF.
- **Scheduler**: `flowfile_scheduler` is deliberately **free of `flowfile_core` imports** — it polls the shared SQLite DB via reflected tables (`flowfile_scheduler/models.py`). Keep it dependency-light.
- **Polars version lock**: root `pyproject.toml`, `kernel_runtime`, and `flowfile_frame` must pin compatible Polars; kernel containers read their own `poetry.lock` at startup to surface versions and avoid drift. Bump them together; the kernel image version evolves independently of the app version.

## Development Setup

### Python Backend

```bash
# Install all Python dependencies (uses Poetry; pulls the default dev group)
poetry install

# Also install the optional build group (PyInstaller)
poetry install --with build

# Start core backend (FastAPI, port 63578)
poetry run flowfile_core

# Start worker service (FastAPI, port 63579)
poetry run flowfile_worker
```

### Frontend

```bash
cd flowfile_frontend
npm install

# Web dev server, hot reload — no desktop shell. Serves the renderer at
# http://localhost:8080 and proxies /api to flowfile_core (:63578), so start
# `poetry run flowfile_core` first.
npm run dev:web

# Full Tauri dev mode: compiles the Rust shell (src-tauri/) and boots the staged
# Python sidecars. Requires the Rust toolchain + the sidecars staged via
# `make services` (= `make build_python_services && make rename_sidecars`) from
# the repo root. Without the staged binaries the shell starts with no backend.
npm run dev
```

### Full Stack via Docker

```bash
# Copy .env.example to .env and configure (first run builds all three images)
docker compose up -d
# Frontend: http://localhost:8080, Core: :63578, Worker: :63579
```

## Build Commands

| Command | Description |
|---------|-------------|
| `make all` | Full build: deps → PyInstaller services → stage sidecars → sign sidecars → Tauri app → master key |
| `make install_python_deps` | `poetry install --with build` (auto-refreshes a stale lock first) |
| `make build_python_services` | Build Python backend with PyInstaller (`build_backends` entry point) |
| `make rename_sidecars` | Stage PyInstaller outputs into `flowfile_frontend/src-tauri/binaries/<name>-<triple>` |
| `make services` | Convenience: `build_python_services` + `rename_sidecars` |
| `make sign_sidecars` | Sign bundled sidecars for macOS notarization (no-op off macOS or when `APPLE_SIGNING_IDENTITY` unset) |
| `make build_tauri_app` | Build Tauri desktop app (current host target) |
| `make build_tauri_win` / `build_tauri_mac` / `build_tauri_linux` | Platform-specific Tauri builds |
| `make build_tauri_mac_arm` / `build_tauri_mac_intel` | macOS aarch64 / x86_64 Tauri builds |
| `make measure_bundle` | Print sizes of `services_dist/` and `src-tauri/binaries/` |
| `make test_built_services` | Smoke-test the PyInstaller binaries against `/docs` |
| `make stubs` | Regenerate flowfile_frame `.pyi` stubs (run after changing FlowFrame/Expr/public API) |
| `make check_stubs` | CI drift gate: regenerate stubs and fail if they differ from committed files |
| `make generate_key` / `make force_key` | Generate Fernet master key (no-op if present) / regenerate unconditionally |
| `make update_lock` / `make force_lock` | Refresh Poetry lock (`poetry lock` / `poetry lock --no-update`) |
| `make stop_servers` | Kill stray `flowfile_core` / Vite dev-server processes |
| `make clean` | Remove all build artifacts including `src-tauri/target` and `src-tauri/binaries` |
| `make clean_test` | Remove Playwright `test-results/` and `playwright-report/` |
| `npm run build:web` (in `flowfile_frontend/`) | Build web-only frontend (lint + `vue-tsc --noEmit` + `vite build`) |

## Testing

### Python Tests (pytest)

```bash
# Run core tests
poetry run pytest flowfile_core/tests

# Run worker tests
poetry run pytest flowfile_worker/tests

# Run frame tests
poetry run pytest flowfile_frame/tests

# Run scheduler tests
poetry run pytest flowfile_scheduler/tests

# Run with coverage (core + worker only, sequential with --cov-append)
make test_coverage

# Tests requiring Docker (kernel integration)
poetry run pytest -m kernel
```

**Markers** (registered in `pyproject.toml` `[tool.pytest.ini_options]`):

| Marker | Meaning |
|--------|---------|
| `worker` | flowfile_worker package tests |
| `core` | flowfile_core package tests |
| `kernel` | Integration tests requiring Docker kernel containers |
| `docker_integration` | Full Docker-based E2E tests (Docker required, slow) |
| `kafka` | Integration tests requiring a Kafka/Redpanda broker (Docker) |

**Coverage source:** `flowfile_core/flowfile_core` + `flowfile_worker/flowfile_worker` only (frame/scheduler excluded).

### Frontend Unit Tests (Vitest)

```bash
cd flowfile_frontend
npm run test:unit          # one-shot (vitest run, node env)
npm run test:unit:watch    # watch mode
```

Picks up `src/**/*.test.ts` (Pinia stores, AI features, cron-builder, etc.).

### Frontend E2E Tests (Playwright)

```bash
cd flowfile_frontend

# Install the Playwright browser
npx playwright install chromium

# Web E2E (runs only tests/web-flow.spec.ts)
npm run test:web

# Run every spec in tests/ (web-flow + canvas-overlays)
npm run test:all
```

`playwright.config.ts` has no `webServer` block, so flowfile_core and a Vite
preview/dev server must already be running before invoking these scripts.

**E2E via Makefile:**
```bash
make test_e2e          # build:web, start core + preview (:4173), run web-flow.spec.ts
make test_e2e_dev      # same but uses the dev server instead of preview
```

> Note: `make test_e2e` starts only flowfile_core (not the worker) and runs only
> `web-flow.spec.ts`. Tauri-shell E2E tests via `tauri-driver` are a follow-up;
> the Playwright suite currently covers renderer behavior in web mode, which is
> shared with the desktop shell.

### WASM Tests (Vitest)

```bash
cd flowfile_wasm
npm run test           # watch mode (vitest)
npm run test:run       # one-shot (CI)
npm run test:coverage  # one-shot with coverage
```

Tests live under `flowfile_wasm/tests/` (unit, integration, components; happy-dom env).

## Code Style & Linting

### Python (Ruff)

Config lives in the root `pyproject.toml` (`[tool.ruff]`); it is the only ruff config in the repo. Ruff is pinned via the dev group (`ruff = "^0.8.0"`).

- **Line length:** 120
- **Target:** Python 3.10 (`target-version = "py310"`)
- **Rules:** Pyflakes (F), pycodestyle errors/warnings (E/W), isort (I), pyupgrade (UP), flake8-bugbear (B)
- **Format:** Double quotes, space indentation, auto line endings, magic trailing comma respected
- **Excluded from linting:** `tests/`, `test_*.py`, `*_test.py`, `conftest.py`, `test_utils/`, `*.pyi` (plus build/venv dirs)
- **Per-file ignores:** `tests/**/*` → E501, S101; `test_utils/**/*` → E501
- **isort first-party:** `flowfile`, `flowfile_core`, `flowfile_worker`, `flowfile_frame`, `flowfile_scheduler`, `shared`, `test_utils`, `tools`, `build_backends`
- **bugbear immutables (no B008):** `fastapi.{Depends, Query, Body, Path, Header, Cookie, Form, File, Security}`

```bash
# Check
poetry run ruff check .

# Fix
poetry run ruff check --fix .

# Format
poetry run ruff format .
```

### Frontend (ESLint + Prettier)

Configs: `flowfile_frontend/.prettierrc.json` and `flowfile_frontend/.eslintrc.js` (legacy eslintrc, eslint 8).

- **Prettier:** semicolons, 2-space tabs, double quotes (`singleQuote: false`), 100 char width, trailing commas (`all`), LF line endings
- **ESLint:** extends `eslint:recommended`, `@typescript-eslint/recommended`, `plugin:vue/vue3-recommended`, `@vue/prettier`; `prettier/prettier` runs at warn level; enforces `linebreak-style: unix`

```bash
cd flowfile_frontend
npm run lint          # eslint --fix ./src/**/*.{ts,vue} (renderer TS/Vue only)
```

## Key Conventions

### Python

- **Framework:** FastAPI with Pydantic v2 models for request/response validation
- **Data processing:** Polars (not pandas) for all dataframe operations
- **Async:** FastAPI endpoints; heavy work offloaded to the worker service
- **Core never collects:** flowfile_core must not materialise LazyFrames (`.collect()`); it ships paths/JSON, the worker holds dataset memory
- **Worker compute is subprocess-bound:** heavy work runs in `mp_context.Process` (spawn) children managed by `ProcessManager`; don't expect dataset caches to live in the FastAPI process
- **Import ordering:** stdlib, third-party, then first-party (`flowfile`, `flowfile_core`, `flowfile_worker`, `flowfile_frame`, `flowfile_scheduler`, `shared`, `test_utils`, `tools`, `build_backends`)
- **FastAPI patterns:** `fastapi.Depends`, `fastapi.Query`, etc. are treated as immutable in bugbear checks
- **Secrets:** Fernet master key → HKDF per-user keys (see Subsystems); never commit `master_key.txt`

### Frontend

- **Framework:** Vue 3 (3.5) Composition API (`<script setup>`) with TypeScript
- **State management:** Pinia (v2) stores
- **UI library:** Element Plus (v2)
- **Data grids:** AG Grid Community — modular `@ag-grid-community/*` packages (v31), Vue binding `@ag-grid-community/vue3`
- **Flow visualization:** VueFlow (`@vue-flow/core` v1)
- **HTTP client:** Axios (v1)
- **Code editing:** CodeMirror 6 (`@codemirror/lang-python`, `@codemirror/lang-sql`) via `vue-codemirror`
- **Routing / i18n:** vue-router 4, vue-i18n 9
- **Path aliases:** `@` → `src/renderer/app/`, plus `@/api`, `@/types`, `@/stores`, `@/composables` (defined in both `vite.config.mjs` and `tsconfig.json`)

### File Naming

- Python: snake_case for modules and files
- Vue: PascalCase for components (predominant; some legacy files are camelCase), camelCase for route paths
- Tests: `test_*.py` (pytest), `*.spec.ts` (Playwright E2E, `flowfile_frontend/tests/`), `*.test.ts` (Vitest unit, colocated under `src/`)

## CI/CD Workflows

`.github/workflows/` holds 14 workflows (note the mix of `.yml` and `.yaml`). All path-filtered workflows also support `workflow_dispatch` (manual run).

| Workflow | Trigger | Description |
|----------|---------|-------------|
| `test.yaml` | Push/PR to `main` (no path filter) | **Primary CI**: backend tests (Linux+macOS matrix), Windows job, kernel tests, `check-stubs`, web tests, docs build, Codecov upload |
| `e2e-tests.yml` | Push/PR to `main` (`flowfile_frontend/**`, `flowfile_core/**`) | Build frontend, start backend, run Playwright web E2E |
| `test-docker-auth.yml` | Push/PR to `main` (auth code + core Dockerfile) | Docker-based auth E2E tests |
| `test-docker-kernel-e2e.yml` | Push/PR to `main` (kernel_runtime, kernel code, Dockerfiles, compose) | Docker kernel E2E tests |
| `test-kernel-integration.yml` | Push/PR to `main` (kernel_runtime, kernel/artifacts code + tests) | Kernel integration tests |
| `test-kafka-integration.yml` | Push/PR to `main` (kafka code + tests) | Kafka integration tests |
| `flowfile-wasm-build.yml` | Push/PR to `main` (`flowfile_wasm/**`) | Build WASM version and run its test suite |
| `docker-publish.yml` | Push to `main` (backend/frontend/worker/kernel/shared/tools paths), `release` published | Multi-arch Docker builds (amd64/arm64) → Docker Hub |
| `documentation.yml` | Push/PR to `main` (`docs/**`, `mkdocs.yml`, `flowfile_frame/**/*.py`) | Build and deploy MkDocs site |
| `codeql.yaml` | Weekly cron (Mon 06:00) | CodeQL security scan (Python, JS/TS) |
| `pypi-release.yml` | Git tags `v*` | Build frontend into static, Poetry build, publish to PyPI |
| `release.yaml` | Git tags `v*` | Build & sign Tauri desktop installers (macOS arm64/intel, Windows, Linux), publish GitHub release |
| `npm-publish-wasm.yml` | Git tags `wasm-v*` | Publish `flowfile-editor` WASM package to npm |

> Release tags: pushing a `v*` tag fires **both** `pypi-release.yml` (PyPI) and `release.yaml` (desktop installers); a `wasm-v*` tag fires the npm WASM publish.

## Environment Variables

Docker deployments are configured via `.env` (copy `.env.example`); a few vars
below live only in `docker-compose.yml` or are read directly from the
environment in local/desktop runs.

**Core deployment:**

| Variable | Purpose |
|----------|---------|
| `FLOWFILE_MODE` | Runtime: `electron` (default when unset, desktop), `package` (Python package), `docker` (container). Gates auth/secrets/JWT/storage behavior. Compose sets `docker`. |
| `FLOWFILE_ADMIN_USER` / `FLOWFILE_ADMIN_PASSWORD` | Initial admin account. |
| `JWT_SECRET_KEY` | JWT signing secret. Required in docker mode (startup fails if unset). |
| `FLOWFILE_MASTER_KEY` | Fernet key encrypting user secrets. Required in docker; in electron the UI setup prompts. May also be a `master_key.txt` Docker secret (env var wins). |
| `FLOWFILE_INTERNAL_TOKEN` | Shared secret for kernel → core service auth. Required in docker. |
| `WORKER_HOST` / `CORE_HOST` | Service discovery between core and worker (default `0.0.0.0`, `127.0.0.1` on Windows; `flowfile-worker`/`flowfile-core` in compose). |
| `FLOWFILE_STORAGE_DIR` | Internal storage path. Default `~/.flowfile` (local) / `/app/internal_storage` (docker). |
| `FLOWFILE_USER_DATA_DIR` | User data path. Default home dir (local) / `/app/user_data` (compose). |
| `FLOWFILE_SCHEDULER_ENABLED` | Start the embedded scheduler when truthy (`true`/`1`/`yes`); otherwise recurring flows never fire. |
| `FLOWFILE_KERNEL_IMAGE` / `_BASE` / `_ML` / `_LITE` | Override kernel container images; unset uses the registry default. |

**AI subsystem (see `.env.example`):**

- `FEATURE_FLAG_AI` — master switch for the `/ai/*` router. Default **on** (`true`/`1`/`yes`/`on`). With no BYOK rows saved, providers fall back to env keys (`ANTHROPIC_API_KEY`, `OPENAI_API_KEY`, `GEMINI_API_KEY`, …).
- `FLOWFILE_AI_<PROVIDER>_RPM` / `_RPD` — optional per-provider request budgets (enforced per worker process).
- `FLOWFILE_AI_LOG_PROMPTS` / `FLOWFILE_AI_LOG_PROMPTS_SCRUB` — prompt logging (see [Debugging the AI](#debugging-the-ai)).

**Local-dev tunables (read from env, not all in `.env.example`):**

- `FLOWFILE_WORKER_PORT` (default `63579`), `FLOWFILE_WORKER_URL` (full URL override).
- `FLOWFILE_OFFLOAD_TO_WORKER` (default `1`) — route heavy compute to the worker.
- `FLOWFILE_SINGLE_FILE_MODE` (default `0`) — co-host the worker on the core port; the bundled CLI sets this with `FLOWFILE_WORKER_PORT=63578`.

## Default Ports

- **63578** — flowfile_core (backend API). Also serves the bundled web UI in pip-installed unified mode
- **63579** — flowfile_worker (compute worker)
- **8080** — Frontend in Docker/production (nginx) **and** the Vite dev server `npm run dev:web` (`strictPort: true`; Tauri `devUrl`)
- **4173** — Vite preview server `npm run preview:web` (used by `make test_e2e`)
- **5174** — WASM dev server (`flowfile_wasm/vite.config.ts`)
- **9999** — kernel_runtime container-internal port (`EXPOSE 9999`); host-mapped into `19000-19999` by the kernel manager

> There is no dev server on 5173 — the only `5173` in the repo is a leftover entry in the core CORS allowlist; the actual Vite dev server is 8080.

## Important Files

- `flowfile_core/flowfile_core/flowfile/flow_graph.py` - Core DAG execution engine (~224KB, 5349 lines)
- `flowfile_core/flowfile_core/flowfile/flow_data_engine/flow_data_engine.py` - Polars data engine backing node execution (~120KB)
- `flowfile_core/flowfile_core/schemas/input_schema.py` - Pydantic node-config schemas (the node settings contract, ~64KB)
- `flowfile_frame/flowfile_frame/flow_frame.py` - FlowFrame API (~140KB, 3414 lines)
- `flowfile_frame/flowfile_frame/expr.py` - Column expression system (~68KB, 1722 lines)
- `flowfile_core/flowfile_core/main.py` - Core FastAPI app with all routers
- `flowfile_worker/flowfile_worker/main.py` - Worker FastAPI app
- `flowfile/flowfile/__main__.py` - CLI entry point (run flows, launch web UI)
- `flowfile_frontend/src-tauri/src/lib.rs` - Tauri shell entry (plugins, sidecar boot, menu, window lifecycle)
- `flowfile_frontend/src-tauri/src/sidecar/mod.rs` - Python sidecar spawn + readiness probe
- `flowfile_frontend/src-tauri/src/sidecar/shutdown.rs` - Graceful shutdown ladder (HTTP /shutdown → SIGTERM → SIGKILL)
- `flowfile_frontend/src-tauri/tauri.conf.json` - Tauri config (windows, CSP, bundle, updater endpoints)
- `flowfile_frontend/src-tauri/SIGNING.md` - Operator notes for updater keys + macOS/Windows code signing
- `flowfile_frontend/src/renderer/lib/desktop.ts` - Bridge between Vue renderer and Tauri runtime
- `tools/rename_sidecar.py` - Stages `services_dist/` into Tauri's per-triple sidecar layout
- `flowfile_frontend/src/renderer/app/App.vue` - Vue root component
- `Makefile` - Build/test orchestration (all `make` targets)
- `docker-compose.yml` - Full-stack service definitions (core 63578, worker 63579, frontend 8080)
- `CONTRIBUTING.md` - Contributor guide (dev setup, style, tests, PR process)
- `docs/community.md` - Community hub (Discussions, Issues, release feedback)

## Community & Contributions

- **Questions and discussion:** [GitHub Discussions](https://github.com/edwardvaneechoud/Flowfile/discussions) (Q&A, announcements, show-and-tell)
- **Bugs and feature requests:** [GitHub Issues](https://github.com/edwardvaneechoud/Flowfile/issues)
- **Contributing code:** see `CONTRIBUTING.md` at the repo root
- **Release feedback:** each release has a Discussion thread linked from [Releases](https://github.com/edwardvaneechoud/Flowfile/releases)

## Debugging the AI

When an agent run misbehaves (tool-name loops, planner self-loops, surprising
column references, etc.), the fastest way to inspect what the model actually
saw is the prompt log:

1. Set `FLOWFILE_AI_LOG_PROMPTS=true` in your env (accepts `true`/`1`/`yes`/`on`).
2. Re-run the failing flow / chat / agent session.
3. Tail the latest entries: `python -m flowfile_core.ai.prompt_log tail 20`
4. Or search them: `python -m flowfile_core.ai.prompt_log grep PATTERN [SURFACE]`
   (regex over the day's entries, optionally scoped to one AI surface).
5. Or open the file directly. It lives under the storage base dir:
   `<base>/ai_prompts/YYYY-MM-DD.jsonl` — `~/.flowfile/ai_prompts/...` by
   default, `$FLOWFILE_STORAGE_DIR/ai_prompts/...` when set, or
   `/app/internal_storage/ai_prompts/...` in Docker. One line per LLM call,
   parseable with `jq`. The file rolls on the **UTC** date.

Each line carries the full system prompt, message history, tool catalog, model
response, and timing. All vendor providers route through the shared
`LiteLLMProvider` seam, so every LLM call is captured. Runaway-loop lines over
256 KiB keep the system prompt plus the most-recent turns verbatim and stub
older message bodies with `[...truncated, len=N chars]` (and set `truncated:
true`) so each line stays `jq`-parseable.

When sharing transcripts externally, set `FLOWFILE_AI_LOG_PROMPTS_SCRUB=true` to
mask PII in user / tool messages (system + assistant content stays verbatim —
that's what you're debugging). Both flags default off; production runs stay silent.

## Things to Avoid

- Do not commit `master_key.txt`, `.env`, or credential files (`.gitignore` also blocks `*.key`, `*.pem`)
- Do not use pandas for data operations; this project uses Polars throughout (pandas is a dev/test-only dependency, never imported in package source)
- Do not call `.collect()` in flowfile_core — core ships paths/JSON; the worker holds dataset memory in spawned subprocesses
- Polars is pinned `>=1.8.2, <1.40` (one cross-platform pin — the old Windows `<=1.25.2` ceiling was removed); bumping past `<1.40` must be coordinated with the version-coupled `polars-*` plugin packages and `kernel_runtime`
- Tests and test_utils are excluded from Ruff linting (except specific per-file rules)
- The `kernel`, `docker_integration`, and `kafka` pytest markers all require Docker
- Do not "fix" the SHA-256 API-key hash (`flowfile_core/auth/api_key.py`) — it is deliberate for 256-bit tokens; the CodeQL weak-hash alert is a false positive
- Never force-push to `main`; CI builds Docker images from it (`docker-publish.yml` on push) and the test pipeline runs from it. PyPI/desktop releases run from `v*` tags
