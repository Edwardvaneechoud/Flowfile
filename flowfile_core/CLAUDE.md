# CLAUDE.md - flowfile_core

Central FastAPI backend and DAG execution engine for Flowfile: manages flows as DAGs, runs node compute on Polars, hosts auth/catalog/secrets/AI/kernel subsystems, and offloads heavy work to `flowfile_worker`. Package-specific notes only; see the root `/CLAUDE.md` for monorepo-wide setup, build, lint, ports, and cross-package contracts.

## Role
- The hub of the system. The frontend (Tauri/web) and `flowfile`/`flowfile_frame` clients hit its REST API on **port 63578**; it calls `flowfile_worker` (63579) for compute, the kernel containers (Docker SDK) for sandboxed user code, and `flowfile_scheduler` for cron runs.
- App is `flowfile_core/main.py:app`; entry `flowfile_core.main:run` (poetry script `flowfile_core`). Also runs a saved flow in-process via `python -m flowfile_core.main --run-flow <path> --run-id <id>` (PyInstaller / CLI path; forces `OFFLOAD_TO_WORKER.set(False)` and `execution_location="local"`, drops UI-only `explore_data` nodes). `--run-id` is required.
- **Core compute contract:** core never materialises full LazyFrames for dataset memory. With `OFFLOAD_TO_WORKER` on (default; `FLOWFILE_OFFLOAD_TO_WORKER=1`) and a node's `execution_location != "local"`, full-dataset compute/writes go to the worker (it owns dataset memory in spawned subprocesses); core ships paths/JSON. Bounded `.collect()` on previews (head/`pl.len()`/sampling) inside `flow_data_engine.py` is allowed — don't add full-frame collects in core.

## Layout
- `flowfile_core/main.py` — FastAPI app, lifespan (scheduler/kernel/local-model shutdown), CORS (Tauri origin regex + explicit dev/Docker origins), all router mounts, `--run-flow` CLI.
- `flowfile_core/routes/` — REST routers: `routes.py` (editor/transform, JWT-gated), `flow_api.py` (`data_router` API-key data + `management_router` JWT), `auth.py`, `secrets.py`, `catalog.py`, `cloud_connections.py`, `ga_connections.py`, `kafka.py`, `file_manager.py`, `api_consumers.py`, `user_defined_components.py`, `logs.py`, `public.py`. (More routers live under `ai/`, `kernel/`, `artifacts/`, `ml/`.)
- `flowfile_core/flowfile/flow_graph.py` — DAG execution engine (`FlowGraph`, node add/run, worker offload). `flowfile/handler.py` — `FlowfileHandler` in-memory flow registry.
- `flowfile_core/flowfile/flow_data_engine/flow_data_engine.py` — per-node Polars compute wrapper (lazy frames, previews; `join/`, `fuzzy_matching/`, `subprocess_operations/` subdirs).
- `flowfile_core/flowfile/sources/external_sources/` — SQL / REST API / Google Analytics / custom source connectors (`factory.py`).
- `flowfile_core/configs/node_store/nodes.py` — node template/default registry (`get_all_standard_nodes`).
- `flowfile_core/schemas/input_schema.py` — Pydantic node-config models (~90 classes); other request/response schemas alongside.
- `flowfile_core/ai/` — AI subsystem (see patterns); routers under `ai/*_routes.py`, plus `agents/`, `providers/`, `tools/` (incl. `tools/executor/`), `local_model/`, `context/`.
- `flowfile_core/auth/` — JWT (`jwt.py`), API keys (`api_key.py`), passwords (`password.py`).
- `flowfile_core/secret_manager/secret_manager.py` — Fernet+HKDF per-user secret encryption.
- `flowfile_core/kernel/` — Docker kernel orchestration: `manager.py`, `flavours.py`, `models.py`, `routes.py`.
- `flowfile_core/database/` + `flowfile_core/alembic/versions/NNN_*.py` — SQLAlchemy models + Alembic migrations.
- `flowfile_core/configs/settings.py` — env-driven runtime flags (`MutableBool`s), ports, OAuth config.
- `flowfile_core/scheduler/__init__.py` — re-exports `FlowScheduler` from `flowfile_scheduler.engine` + `get_scheduler`/`set_scheduler` module singleton.

## Key patterns & conventions
- **Router wiring is centralized:** add endpoints by including the router in `main.py` (order/prefixes there are load-bearing — e.g. `ai_admin_router` mounts on `/system`, not `/ai`, so admins can flip the gate). Most editor routes carry `Depends(get_current_active_user)`; `public_router` and `flow_api.data_router` are API-key-authenticated (`verify_api_key`) instead.
- **AI gate:** the whole `/ai/*` router is gated by `FEATURE_FLAG_AI` (live-mutable `MutableBool` in `settings.py`; `ai/feature_flag.py` `is_ai_enabled`/`require_ai_enabled`, returns 503 when off). The `/system` admin route flips it without restart.
- **Keep the `ai` package litellm-import-free at module level.** Every `import litellm` is lazy (inside functions): `ai/providers/_litellm_base.py` and `ai/scheduler.py` (`from litellm import exceptions`). `ai/__init__.py` `setdefault`s `LITELLM_LOCAL_MODEL_COST_MAP=True` before any lazy import. Re-adding an eager litellm import breaks `test_classification`/`test_dry_run` lazy-contract tests. BYOK key wiring lives in `ai/byok.py` (reads `ai/credentials.py`, sets provider env vars litellm picks up); providers registered in `ai/providers/registry.py`.
- **Secrets format:** `encrypt_secret(secret_value, user_id)` → `$ffsec$1$<user_id>$<fernet_token>`; key is `HKDF-SHA256(master_key, info="user-<id>")`. The embedded user_id lets the worker decrypt without user context; legacy raw-Fernet tokens still decrypt via the `user_id` param.
- **API-key hashing is deliberate SHA-256** (`auth/api_key.py:hash_api_key`) — 256-bit random tokens, not passwords; don't "upgrade" to a KDF (the CodeQL weak-hash alert here is a known false positive). Passwords use bcrypt (`settings.PWD_CONTEXT`).
- **Kernels:** `kernel/manager.py` allocates host ports from **19000–19999** (`_BASE_PORT=19000`, `_PORT_RANGE=1000`) per container; image flavours (`ImageFlavour.BASE/ML/LITE/CUSTOM`) resolve locked versions from `kernel_runtime/poetry.lock`, parsed in `kernel/flavours.py`. All containers stop in the app lifespan shutdown.
- **Migrations auto-run on import:** importing `flowfile_core.database.init_db` runs `database/migration.py:run_startup_migration` at module level (skipped when `FLOWFILE_SKIP_STARTUP_MIGRATION` set), handling fresh-install, legacy `flowfile.db` copy, and pending-migration cases. `init_db()` itself then seeds default users/catalog. Add schema changes as a new `alembic/versions/NNN_*.py`; never hand-edit existing migrations.
- **Settings are `MutableBool`** so they toggle live: `SINGLE_FILE_MODE`, `OFFLOAD_TO_WORKER`, `FEATURE_FLAG_AI`, `FLOWFILE_AI_LOG_PROMPTS[_SCRUB]`.
- **Project git-tracking** (`project/`, router `routes/project.py`, migrations 023–026, `FLOWFILE_ENABLE_PROJECTS`): mirrors an install's flows/connections/schedules/catalog into a deterministic, secret-free git folder. The **DB stays the runtime source of truth**: projection (`projection.py`, DB→files) runs automatically as fire-and-forget hooks at the tail of catalog/secret/connection/flow save paths, and import (`importer.py`, files→DB) runs only at explicit boundaries (open/restore/reload). **The projection hooks must never raise** — failures are swallowed (logged) so a sync error never breaks the originating save. In docker mode the router 404s unless `FLOWFILE_ENABLE_PROJECTS` is set, and projects are confined to `<user_data>/projects/<owner_id>`.

## Running / entry points
```bash
poetry run flowfile_core                                  # serve API on :63578
python -m flowfile_core.main --run-flow F --run-id N       # run a saved flow in-process (CLI/PyInstaller)
docker build -f flowfile_core/Dockerfile .                 # image: FLOWFILE_MODE=docker, CMD python -m flowfile_core.main
```
Scheduler only starts when `FLOWFILE_SCHEDULER_ENABLED` ∈ {true,1,yes}. `FLOWFILE_MODE` defaults to `electron` if unset (`docker` in the image).

## Testing
```bash
poetry run pytest flowfile_core/tests          # this package's suite (run from repo root)
poetry run pytest -m kernel                     # Docker-backed kernel tests only
```
Tests live in `flowfile_core/tests/` (package root, `test_*.py`); fixtures in `tests/conftest.py`, `tests/kernel_fixtures.py`, helpers in `tests/flowfile_core_test_utils.py` / `tests/utils.py`. Markers: `core`, `kernel`, `kafka`, `docker_integration` (defined in root `pyproject.toml`). `asyncio_mode = strict`.

## Gotchas
- `flowfile_core/__init__.py` runs `validate_setup()` then `init_db()` **on import** — importing the package has side effects (DB file creation, Alembic schema migration, default-user seeding).
- Don't bypass the worker offload for full datasets, and don't add eager litellm imports anywhere under `ai/` — both break invariants enforced by tests.
- There is no package-level `pyproject.toml`; deps, scripts, ruff, and pytest config all live in the monorepo-root `pyproject.toml`. The local `.env` is read by `settings.py` via Starlette `Config`.
- Kernel / AI-local-model / Docker features need Docker available; absence surfaces as runtime errors, not import errors.

## Key files
- `flowfile_core/main.py` — app + router registry + lifespan + `--run-flow` CLI.
- `flowfile_core/flowfile/flow_graph.py` — DAG engine and worker-offload logic.
- `flowfile_core/flowfile/flow_data_engine/flow_data_engine.py` — node Polars compute/preview wrapper.
- `flowfile_core/schemas/input_schema.py` — Pydantic node-config models.
- `flowfile_core/configs/settings.py` — env flags / `MutableBool`s / ports.
- `flowfile_core/secret_manager/secret_manager.py` — Fernet+HKDF per-user secrets (`$ffsec$1$…`).
- `flowfile_core/auth/api_key.py` — deliberate SHA-256 API-key hashing.
- `flowfile_core/kernel/manager.py` — Docker kernel lifecycle, host ports 19000–19999.
- `flowfile_core/database/migration.py` — startup Alembic migration orchestration.
- `flowfile_core/ai/feature_flag.py` — `FEATURE_FLAG_AI` gate for `/ai/*`.
- `flowfile_core/ai/providers/registry.py` — provider registry (litellm-backed, lazy imports).
- `flowfile_core/project/` — git project-tracking: `projection.py` (DB→files, never-raise hooks), `importer.py` (files→DB), `service.py` (lifecycle), `git_ops.py`.
