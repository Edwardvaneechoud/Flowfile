---
name: flowfile-run-and-operate
description: How to run and operate Flowfile — every `flowfile` CLI verb and flag, the three headless flow-execution paths (CLI/PyInstaller/scheduler), local-dev vs single-process vs Docker service startup, the on-disk storage map (flows, catalog Delta tables, DB, logs, secrets, master key), and the state-inspection runbook. Use when starting core/worker/UI, running a flow headlessly or on a schedule, asking "where does Flowfile store X on disk," debugging why a flow/schedule/run didn't produce output, choosing FLOWFILE_MODE or FLOWFILE_SINGLE_FILE_MODE, or importing `flowfile`/`flowfile_core` in a script and needing to avoid mutating the live catalog DB.
---

# Flowfile Run and Operate

## When NOT to use this skill

- Writing/registering a **new node type** → `flowfile-node-development`.
- Full `.env` / feature-flag catalog beyond the handful of ops-critical vars here → `flowfile-config-and-flags`.
- **Secret ciphertext format** (`$ffsec$1$<user_id>$<token>`), HKDF derivation, why core/worker must stay byte-identical → `flowfile-architecture-contract` (this skill only covers *where the master key file lives* and how to unblock a `RuntimeError` at startup).
- Triaging a **specific stack trace / reproduced bug** → `flowfile-debugging-playbook`.
- **Known historical incidents** (the "why" behind a design, postmortems) → `flowfile-failure-archaeology`.
- Local dev environment setup, `make` targets, PyInstaller/Tauri build pipeline → `flowfile-build-and-env`.
- Test-DB isolation (`FLOWFILE_DB_PATH` for **pytest** specifically, markers, coverage) → `flowfile-testing-and-validation` (this skill covers `FLOWFILE_DB_PATH` for **ad-hoc/CLI** isolation, which is the same mechanism but a different use case).

If your question is "how do I start/run/inspect a running Flowfile install" — keep reading.

---

## 1. The `flowfile` CLI — every verb and flag

Entry point: Poetry script `flowfile = "flowfile.__main__:main"` (root `pyproject.toml`). Argparse lives in `flowfile/flowfile/__main__.py`.

```
flowfile [command] [component] [file_path] [--host H] [--port P] [--no-browser]
         [--param KEY=VALUE ...] [--run-id N]
```

| positional | choices | meaning |
|---|---|---|
| `command` | `run`, `seed-demo`, `remove-demo`, `project` | top-level verb |
| `component` | `ui`, `core`, `worker`, `flow`, `init`, `open`, `save` | run target, or project sub-command |
| `file_path` | free | flow file path, project folder, or version message |

| flag | default | applies to | notes |
|---|---|---|---|
| `--host` | `127.0.0.1` | `run core` / `run worker` only | **ignored by `run ui`** |
| `--port` | `63578` | `run core` / `run worker` only | ignored by `run ui` |
| `--no-browser` | off | `run ui` | skips `webbrowser.open_new_tab` |
| `--param KEY=VALUE` | none, repeatable | `run flow` | overrides/creates a `FlowParameter` |
| `--run-id N` | `None` | `run flow` | pre-created `FlowRun` row id; results reported via `shared.run_completion.complete_run` |

### Verb behavior

- **`flowfile run ui [--no-browser]`** → `flowfile.web.start_server(...)`. **`--host`/`--port` are parsed but silently dropped** — `start_server` hard-rejects any host other than `127.0.0.1` or port other than `63578` (`raise NotImplementedError`). The CLI's own no-args usage text (`__main__.py:281`) shows `flowfile run ui --host 0.0.0.0 --port 8080` as an example — that example is dead code; don't follow it. Use `run core`/`run worker` directly if you need a custom host/port.
- **`flowfile run core`** → `flowfile_core.main.run(host, port)`, honors `--host`/`--port`.
- **`flowfile run worker`** → `flowfile_worker.main.run(host, port)`. When launched directly (`poetry run flowfile_worker`), the worker's own argparse additionally accepts `--core-host`/`--core-port` (`flowfile_worker/configs.py`).
- **`flowfile run flow <path> [--param k=v] [--run-id N]`** → headless execution; see §3.A.
- **`flowfile seed-demo`** / **`flowfile remove-demo`** → `flowfile_core.catalog.demo_seed.seed_demo_catalog()` / `remove_demo_catalog()`. Seeds a `Demo` catalog namespace tree (`sales_analytics`, `market`), 4 physical + up to 3 more Delta tables, 2 flow registrations, and 1 cron schedule; prints a summary dict, e.g. `{'tables_created': [...], 'sales_flow_registration_id': 1, 'fx_flow_registration_id': 2, 'schedule_id': 1, 'fx_populate': 'triggered'}`.
- **`flowfile project {init|open|save} <folder-or-message>`** — headless git-backed project tracking (mirrors flows/connections/schedules/catalog into a deterministic git folder; DB stays the source of truth). Owner is `get_local_user_id()`.
  - `init <folder>`: validates the path (electron: any local root; docker/package: confined to CWD ∪ storage roots, no `..`), runs `git init`, writes a manifest + `.gitignore`, commits "Initialize Flowfile project". Prints `Initialized project '<name>' at <root>`.
  - `open <folder>`: imports flows/connections/schedules from the folder into the DB; prints counts and `N value(s) need to be set (FLOWFILE_SECRET_<NAME> or update them in the app).` for placeholder secrets.
  - `save "<message>"`: projects the DB into the folder and `git commit`s; prints `Saved version <sha8>` or `(no changes)`.
- **No args** → prints `FlowFile v<version>` + a usage block.

Full multi-line usage/version output, verb list, and demo-seed summary shape are all worth re-reading directly from `flowfile/flowfile/__main__.py` — it is short (under 300 lines) and is the ground truth for every flag above.

---

## 2. CRITICAL: import side effects

`import flowfile` (`flowfile/flowfile/__init__.py`) unconditionally sets, at import time:

```python
os.environ["FLOWFILE_WORKER_PORT"] = "63578"
os.environ["FLOWFILE_SINGLE_FILE_MODE"] = "1"
```

So **any** `python -m flowfile ...` invocation — including a throwaway `python -c "import flowfile"` — silently forces single-file (co-hosted worker) mode for the rest of the process.

`import flowfile` also imports `flowfile_core`, and `flowfile_core/flowfile_core/__init__.py` runs, **at import time**:
1. `validate_setup()` — node-registry sanity check.
2. `init_db()` → `flowfile_core/flowfile_core/database/init_db.py`, which (unless `FLOWFILE_SKIP_STARTUP_MIGRATION` is set) calls `run_startup_migration()` — **runs Alembic migrations against the live catalog DB** — and then seeds the default `local_user`.

This means importing `flowfile` or `flowfile_core` from an ad-hoc script, a REPL, or a debugging session **mutates your real catalog database** unless you isolate it first.

**Isolation rule — use `FLOWFILE_DB_PATH` for any diagnostic import:**

```bash
FLOWFILE_DB_PATH=/tmp/scratch/cat.db FLOWFILE_STORAGE_DIR=/tmp/scratch/storage \
  poetry run python -c "import flowfile_core"
```

**Do not** combine `FLOWFILE_SKIP_STARTUP_MIGRATION=1` with a brand-new/nonexistent DB path — skipping migration means no tables get created at all, and the very next query 500s with "no such table." Only use `FLOWFILE_SKIP_STARTUP_MIGRATION=1` when you deliberately want to import core against an **already-migrated** DB without re-running Alembic (e.g. the Alembic CLI itself, which needs to import settings without triggering a second migration run).

Also: `flowfile/__init__.py` mutes the `PipelineHandler` logger to `WARNING` on import.

---

## 3. Headless flow execution — three equivalent paths

All three run the same logic: force `execution_location = "local"`, disable worker offload (`OFFLOAD_TO_WORKER.set(False)` — compute happens in-process, no worker service needed), delete any UI-only `explore_data` nodes (prints `Skipping N explore_data node(s) (UI-only)`), stamp catalog producer lineage, call `flow.run_graph()`, and report per-node results.

### A. CLI (dev / pip install)

```bash
flowfile run flow /abs/path/pipeline.yaml
# or, with parameter overrides and a pre-created run-id:
flowfile run flow flow.yaml --param input_dir=/data --param threshold=100 --run-id 42
# equivalently:
python -m flowfile run flow /abs/path/pipeline.yaml
```

Implemented in `flowfile/flowfile/__main__.py:run_flow()`. `--param key=value` (repeatable) replaces an existing `FlowParameter`'s `default_value` or appends a new one. Exit 0 + `Flow completed successfully in X.XXs` / `Nodes completed: n/m` on success; exit 1 + per-node errors on stderr on failure. Writes a per-flow log to `<storage>/logs/flow_<flow_id>.log` (see §9).

### B. PyInstaller / frozen-sidecar binary

```bash
python -m flowfile_core.main --run-flow <path> --run-id <id>   # --run-id is REQUIRED here
```

`flowfile_core/main.py` dispatches `--run-flow` (only reachable via `if __name__ == "__main__"`) to `_run_flow_cli`, which duplicates `run_flow()`'s logic *inside* `flowfile_core` because the top-level `flowfile` package isn't bundled into the PyInstaller binary. Missing `--run-id` prints `Error: --run-id is required` and exits 1 — unlike path A, there is no optional-run-id fallback here.

### C. Scheduler-spawned

`shared/subprocess_utils.py:spawn_flow_subprocess(flow_path, run_id)`:
- **frozen**: `[sys.executable, --run-flow, path, --run-id, N]`
- **dev**: `[sys.executable, -m, flowfile, run, flow, path, --run-id, N]`
- stdout+stderr redirect to `shared.run_logs.run_log_path(run_id)` = `storage.logs_directory / "scheduled_run_<run_id>.log"` (mode 0644, truncated each run) — resolved per call, so it **honors `FLOWFILE_STORAGE_DIR`**, docker mode, and `TESTING`. The `scheduled_run_` prefix is legacy: manual and on-demand runs use it too.
- `start_new_session=True` (fire-and-forget); returns the child PID or `None` on spawn failure.

The scheduler engine (`flowfile_scheduler/flowfile_scheduler/engine.py`) polls the shared SQLite catalog DB every `DEFAULT_POLL_INTERVAL = 30` seconds, supports cron/interval/table-trigger schedules, skips launching a schedule that already has an active run (`ended_at IS NULL`), creates the `FlowRun` row (`run_type="scheduled"`) **before** spawning, and records the child PID. Standalone: `poetry run flowfile_scheduler` (continuous) or `poetry run flowfile_scheduler --once` (single tick). Embedded inside core only when `FLOWFILE_SCHEDULER_ENABLED` is truthy (`true`/`1`/`yes`).

---

## 4. Service startup — order, terminals, co-hosting

### Local dev — three terminals

```bash
poetry run flowfile_core                     # :63578 — start FIRST
poetry run flowfile_worker                   # :63579 — needed for worker-offloaded / remote-execution runs
cd flowfile_frontend && npm run dev:web       # :8080, proxies /api -> :63578
```

Core must be up before the frontend — Vite proxies `/api` to core (and nginx does the same in Docker: `proxy_pass http://flowfile-core:63578/`). The worker is independent: core starts fine without it, but any worker-offloaded node run fails until the worker is up. The worker only calls back to core to ship logs (`POST /raw_logs`).

### Single-process ("unified") mode — the pip-install story

```bash
flowfile run ui [--no-browser]
```

Fixed at `127.0.0.1:63578` (see §1). `start_server` sets `FLOWFILE_MODE=electron` if unset, forces `OFFLOAD_TO_WORKER.value = True`, and extends the core FastAPI app:
- serves the built Vue SPA from `flowfile/web/static/` under `/ui` (missing build → `{"error": "Web UI not installed..."}`),
- adds `StripApiPrefixMiddleware` (strips a leading `/api` so the Docker-oriented frontend base URL still resolves),
- `GET /single_mode` reports whether `FLOWFILE_SINGLE_FILE_MODE == "1"`,
- **mounts the worker's own router at `/worker` on the same app** — one process serves UI, core API, and worker compute.

`FLOWFILE_SINGLE_FILE_MODE=1` + `FLOWFILE_WORKER_PORT=63578` is exactly what `import flowfile` sets automatically (§2) — that's *why* `get_default_worker_url()` appends `/worker` to the worker URL when single-file mode is on: core's own offload POSTs go to `http://127.0.0.1:63578/worker/...`, i.e. itself. If you need this co-hosted mode **without** the `flowfile` CLI wrapper (e.g. scripting it directly), set both env vars yourself before starting core.

Gotcha: the browser tab opens after `time.sleep(5)` but **before** uvicorn starts listening — the first load can 404/connection-refuse; just reload. `webbrowser.open_new_tab` targets `/ui`; API docs live at `/docs`.

### Core startup/shutdown side effects worth knowing

- Import-time: `storage.cleanup_directories()` runs (see §8 cleanup policy) — **starting core deletes cache files older than 1 hour**, every time.
- Lifespan startup: `logging.basicConfig(INFO, ...)` to stdout only (no file handler — Electron/Tauri pipes this); starts the embedded scheduler iff `FLOWFILE_SCHEDULER_ENABLED`.
- Lifespan startup also runs `shared.run_logs.cleanup_old_logs()` — age-based retention over `scheduled_run_*.log` and `flow_*.log` (`FLOWFILE_RUN_LOG_RETENTION_DAYS`, default 30, `0` disables).
- Lifespan shutdown: stops the scheduler, stops **every** Docker kernel container, and shuts down the optional local LLM. It **no longer deletes logs** — the old `clear_all_flow_logs()` call wiped every `*.log`, run logs included, on every restart. Logs now expire only by age.
- `POST /shutdown` triggers a graceful uvicorn exit (used by the Tauri shutdown ladder).
- CLI arg parsing (`--host`/`--port`/`--worker-port`) happens at **import** of `flowfile_core.configs.settings` via `parse_known_args()` against whatever `sys.argv` the importing process has — importing core inside a process with unrelated `--host`/`--port` flags on argv will silently repoint the server.

### Docker — build-from-source stack

First-time setup (`.env` bootstrapping, kernel-image build profiles + `FLOWFILE_KERNEL_IMAGE`, the Docker-socket mount and fixed-name `flowfile-network` rationale) → `flowfile-build-and-env` §2(d); this section only covers operating an already-built stack.

`docker compose up -d` starts frontend :8080, core :63578, worker :63579. Verified in `docker-compose.yml`: core and worker both get `shm_size: 2gb`, and `FLOWFILE_SCHEDULER_ENABLED=true` + `FLOWFILE_ENABLE_PROJECTS=true` by default. Core/worker Dockerfiles: `CMD python -m flowfile_core.main` / `python -m flowfile_worker.main`, healthcheck `curl -f http://localhost:6357{8,9}/docs`. Status/logs: `docker compose ps`, `docker compose logs -f flowfile-core|flowfile-worker` (§11 runbook).

### Published-images deployment ("docker-remote" — documented drift)

Root `CLAUDE.md` lists a `docker-remote/` directory as "Compose stack using published Docker Hub images." **That directory does not exist** in the working tree or in any commit (as of 2026-07-03). The actual published-images deployment story lives in **`docs/users/deployment/docker.md`**: a sample compose file pulling `edwardvaneechoud/flowfile-{frontend,core,worker}:latest` plus versioned kernel images (`edwardvaneechoud/flowfile-kernel-base:0.3.0`, `-ml:0.3.0`), same env vars/volumes as the source compose (the storage volume is just named `flowfile-storage` there instead of `flowfile-internal-storage`). Ops commands: `docker compose up -d | down | pull | logs -f`. Treat any reference to `docker-remote/` elsewhere in the docs as stale — point people at `docs/users/deployment/docker.md` instead.

---

## 5. Flow file format

**Format:** YAML (preferred) or JSON, root Pydantic model `FlowfileData`. Key shape:

```yaml
flowfile_version: 0.12.7        # stamped with the running app version at save time; informational only
flowfile_id: 424242
flowfile_name: my_flow
flowfile_settings:               # description, execution_mode (Development|Performance),
                                  # execution_location (local|remote|...), auto_save,
                                  # show_detailed_progress, max_parallel_workers, parameters[]
nodes:
  - id: 1
    type: manual_input           # node type == FlowGraph method suffix ("add_" + type)
    is_start_node: true
    x_position: 100
    y_position: 100
    outputs: [2]                 # downstream node ids; connections derive from this
    output_handles: [output-0]   # parallel to outputs; missing entries default to "output-0"
    setting_input: {...}         # node-type-specific settings
groups: []                       # visual FlowfileGroup boxes
```

**Save** (`FlowGraph.save_flow`): `.yaml`/`.yml` → `yaml.dump(..., default_flow_style=False, sort_keys=False, allow_unicode=True)`; `.json` → `json.dump(..., indent=2)`; **`.flowfile` raises `DeprecationWarning`** ("The .flowfile format is deprecated. Please use .yaml or .json formats."); unknown extension → warns and defaults to YAML. Save also re-derives the flow name from the filename stem, prunes empty visual groups, and records catalog read-links.

**Load** (`open_flow`): dispatches on suffix. `.yaml`/`.yml`/`.json` → straightforward Pydantic parse. `.flowfile` → **legacy pickle** load via a custom `LegacyUnpickler` that maps old dataclass names through `tools/migrate/legacy_schemas.py`'s `LEGACY_CLASS_MAP`, plus a compatibility pass that back-fills fields legacy pickles lack (`groups`, `output_handles`, `table_settings`). There is **no version-gated migration keyed on `flowfile_version`** — compatibility is purely structural (Pydantic defaults + the legacy-pickle path); the stamped version is cosmetic.

**Load overrides the flow's name from the filename stem** — `flow_settings.name = flow_path.stem`. Rename the YAML on disk and the flow's in-app name changes on next open; the `flowfile_name` field inside the file becomes stale/cosmetic.

**Path sandboxing at load**: allowed extensions `{.yaml, .yml, .json, .flowfile}`. In **docker mode**, the validator rejects a **relative** path that resolves outside `flows_directory`/`uploads_directory`/`temp_directory_for_flows` — but an **absolute** path outside those directories is *not* rejected by this check (the guard is `if not is_safe and not path.is_absolute(): raise`). Don't rely on this function alone as a security boundary for absolute paths; verify current behavior in `flowfile_core/flowfile_core/flowfile/manage/io_flowfile.py::_validate_flow_path` before treating it as a hard confinement guarantee. Local/electron/package modes accept any path.

### Where flows live on disk

| kind | path | notes |
|---|---|---|
| quick-created ("unnamed") | `<flows_directory>/unnamed_flows/YYYYMMDD_HH_MM_SS_flow.yaml` | persisted, not temp — survives cleanup sweeps. `FlowfileHandler.add_flow(persist=False)` keeps a scratch flow memory-only until first save/run, so abandoned blank canvases leave no orphan file |
| Python-API-built (FlowFrame) | `<flows_directory>/python_editor_flows/<stem>.yaml` | written by `flowfile_frame` flows registering into the catalog |
| `open_graph_in_editor(...)` (no explicit path) | `TemporaryDirectory(prefix="flowfile_graph_")/temp_flow_<hex8>.yaml` | forces `execution_location="local"` + `execution_mode="Development"` on the saved copy; auto-starts the unified server if `/docs` isn't responding (polls up to 60s), then opens `http://127.0.0.1:63578/ui/flow/<id>` in a browser when in electron/single-file mode |

---

## 6. Filesystem map — the `storage` singleton

`shared/storage_config.py`'s `storage = FlowfileStorage()` is instantiated **at import** and eagerly `mkdir -p`s every directory below (except the two marked "no" — opt-in only). Importing `shared` (transitively: importing `flowfile_core` or `flowfile_worker`) has filesystem side effects.

Two roots:
- **base (internal)**: `FLOWFILE_STORAGE_DIR` env, else `~/.flowfile` (local), else `/app/internal_storage` (docker mode, i.e. `FLOWFILE_MODE == "docker"` exactly).
- **user data**: local = `Path.home()`; docker = `FLOWFILE_USER_DATA_DIR` env, else `/data/user` (the compose file overrides this to `/app/user_data`).

| directory property | local path | docker path | eager mkdir | purpose |
|---|---|---|---|---|
| `cache_directory` | `<base>/cache` | same | yes | worker↔core IPC; `.arrow` results under `cache/<flow_id>/<task_id>.arrow`; **cleaned when >1h old at every core startup** |
| `database_directory` | `<base>/database` | same | yes | `flowfile_catalog.db` (+ legacy `flowfile.db`) |
| `logs_directory` | `<base>/logs` | same | yes | per-flow `flow_<flow_id>.log` + per-run `scheduled_run_<run_id>.log`; `FLOWFILE_RUN_LOG_RETENTION_DAYS` retention (default 30d). `TESTING=True` redirects to `<base>/temp/test_logs` |
| `system_logs_directory` | `<base>/system_logs` | same | yes | reserved — no writer currently ships to it |
| `temp_directory` | `<base>/temp` | same | yes | scratch; 24h cleanup |
| `temp_directory_for_flows` | `<base>/temp/flows` | same | yes | flow-scoped temp |
| `shared_directory` | `<base>/temp/kernel_shared` (or `$FLOWFILE_SHARED_DIR`) | same | yes | core↔worker↔kernel exchange — **must stay on the kernel-visible volume** |
| `artifact_staging_directory` | `<base>/temp/kernel_shared/artifact_staging` | same | yes | artifact upload staging |
| `global_artifacts_directory` | `<base>/temp/kernel_shared/global_artifacts` | same | yes | permanent artifacts (ML models, etc.) |
| `flows_directory` | `<base>/flows` | `<user_data>/flows` | yes | saved flow YAMLs |
| `unnamed_flows_directory` | `<flows>/unnamed_flows` | same | yes | quick-created flows |
| `python_editor_flows_directory` | `<flows>/python_editor_flows` | same | yes | FlowFrame/API-registered flows |
| `uploads_directory` | `<base>/uploads` | `<user_data>/uploads` | yes | user uploads |
| `outputs_directory` | `<base>/outputs` | `<user_data>/outputs` | yes | user outputs |
| `user_defined_nodes_directory` (+`/icons`) | `<base>/user_defined_nodes` | `<user_data>/user_defined_nodes` | yes | custom node code + icons |
| `catalog_tables_directory` | `<base>/catalog_tables` | `<user_data>/catalog_tables` | yes | Delta Lake tables — see §7 |
| `catalog_virtual_results_directory` | `<base>/catalog_virtual_results` | `<user_data>/catalog_virtual_results` | yes | worker IPC cache for materialized virtual tables |
| `notebooks_directory` | `<base>/notebooks` | `<user_data>/notebooks` | yes | catalog notebook cell content |
| `template_data_directory` | `<base>/template_data` | `<base>` (not user data) | yes | cached template CSVs |
| `local_model_directory` | `<base>/local_model` | same | **no** | opt-in llama.cpp binary + GGUF |
| `ai_sessions_directory` | `<base>/ai_sessions` | `<user_data>/ai_sessions` | **no** | persisted AI agent sessions |
| ai prompt log (not a `storage` property) | `<base>/ai_prompts/YYYY-MM-DD.jsonl` | same | on first log | gated by `FLOWFILE_AI_LOG_PROMPTS` |

**Cleanup policy** (`storage.cleanup_directories()`, runs at every core startup): `temp` > 24h, `cache` > 1h, `system_logs` > 168h, mtime-based. `logs` is deliberately **not** swept here — its retention is owned by `shared/run_logs.py` (`FLOWFILE_RUN_LOG_RETENTION_DAYS`); re-adding it would silently override the env var with a hardcoded 7 days.

**Catalog DB resolution order** (`get_database_url()`):
1. `FLOWFILE_DB_PATH` env → `sqlite:///<that path>` (always wins)
2. `TESTING=True` → `sqlite:///<base>/temp/test_flowfile_catalog.db` — **one shared file**; concurrent test sessions clobber each other, use `FLOWFILE_DB_PATH` per session instead
3. default → `sqlite:///<base>/database/flowfile_catalog.db`

Legacy one-time migration: if `<base>/database/flowfile.db` exists (and `FLOWFILE_DB_PATH` is unset), its data is copied into the new DB at startup.

As of 2026-07-03 (v0.12.7) the schema has 34 tables + `alembic_version`, migration head **028**. Fresh non-docker DB seeds exactly one user: `local_user`.

---

## 7. Catalog Delta Lake layout

- Each catalog table = one directory under `catalog_tables_directory`, a standard Delta table (`_delta_log/00000000000000000000.json` + `part-*.snappy.parquet`). Naming: demo seed uses `demo_<name>`, flow-written tables use `<table_name>_<8-hex>`, unnamed fallback is `catalog_<32-hex>`.
- The `catalog_tables` DB row's `file_path` holds the table dir as an **absolute path**; `storage_format='delta'`; lineage columns `source_registration_id`/`producer_registration_id` link back to the producing flow. Deleting an orphan Delta directory on disk requires also deleting/fixing its `catalog_tables` row — the two are not self-healing against each other.
- Namespace tree lives in `catalog_namespaces` (root `General` with children `default`, `Local Flows`, `Unnamed Flows`; `seed-demo` adds a `Demo` namespace with `sales_analytics`/`market` children).
- All reads/writes go through the **worker**, never core in-process — `flowfile_worker/catalog_reader.py` (`scan_delta` / `scan_ipc`), with every path validated against the two catalog roots.
- Optional per-namespace object-storage backend (S3, etc.) resolved via `flowfile_core/catalog/storage_backend.py`; `FLOWFILE_CATALOG_STORAGE_URI`/`_CONNECTION` env vars are creation-time defaults for *new* catalogs only, never a live override of existing local tables.
- Materialized virtual-table results land as Arrow IPC files under `catalog_virtual_results/`.

---

## 8. Secrets & master key — where they live (ops view)

Ciphertext format and HKDF derivation are owned by `flowfile-architecture-contract`; here's only what you need to unblock a broken install:

- **Docker mode**: key source is `FLOWFILE_MASTER_KEY` env, else the Docker secret file `/run/secrets/flowfile_master_key`, else a `RuntimeError: Master key not configured...` on first use. Core and worker **must** be given the same key (compose passes it to both).
- **Electron/local**: `SecureStorage` auto-generates a Fernet key at `$APPDATA/flowfile/.secret_key` (mode 0600) the first time it's needed, alongside an encrypted `flowfile.json.enc` store. Falls back to `~/.config/flowfile` if `APPDATA` is unset, or `$SECURE_STORAGE_PATH` (default `/tmp/.flowfile`) in non-electron/non-docker runs.
- **Repo-root `master_key.txt`** is a **build** artifact, not a runtime path: `make generate_key` writes one if missing, `make force_key` regenerates unconditionally (`Makefile`, `KEY_FILE := master_key.txt`). It's gitignored and used by `test-docker-auth.yml` CI / as a Docker secret source — never a live master key for a normal install.

---

## 9. Logs — who writes what, where

| log | location | writer |
|---|---|---|
| per-flow execution log | `<base>/logs/flow_<flow_id>.log` | `FlowLogger`, `FileHandler`, format `%(asctime)s - %(levelname)s - %(message)s`; node lines prefixed `Node ID: <n> - ` |
| scheduled/manual/on-demand run subprocess output | `<base>/logs/scheduled_run_<run_id>.log` (via `shared.run_logs.run_log_path`; honors `FLOWFILE_STORAGE_DIR`) | `shared/subprocess_utils.py` |
| core service log | stdout only, `%(asctime)s [%(levelname)s] %(name)s: %(message)s` | no file handler — Electron/Tauri captures stdout |
| worker service log | stdout only, `%(asctime)s: %(message)s`; worker subprocesses ship flow-scoped lines back to core via `POST /raw_logs` so they land in the same `flow_<id>.log` | no file |
| AI prompt log | `<base>/ai_prompts/YYYY-MM-DD.jsonl` (UTC-dated), only when `FLOWFILE_AI_LOG_PROMPTS` is truthy | `flowfile_core/ai/prompt_log.py` |
| docker logs | container stdout | `docker compose logs -f [service]` |

Access:
- Stream a flow's log live: `GET /logs/{flow_id}` (JWT via query param, `idle_timeout=300` default). Append: `POST /logs/{flow_id}`. Worker ingest: `POST /raw_logs`. Wipe all: `POST /clear-logs`.
- Prompt-log CLI: `python -m flowfile_core.ai.prompt_log tail [N]` (default 10), `... grep PATTERN [SURFACE]`.
- **Logs survive restarts and expire only by age** (`FLOWFILE_RUN_LOG_RETENTION_DAYS`, default 30d; swept at core startup and hourly on the scheduler tick). `POST /clear-logs` is scoped to `flow_*.log` and never touches run logs. Per-flow `flow_<id>.log` is still truncated at each run start, so it holds only the latest run.

---

## 10. Gotchas (quick reference)

1. `flowfile run ui --host/--port` is dead — flags parsed, never used; `start_server` throws on non-default values.
2. `import flowfile` mutates env and importing `flowfile_core` migrates + seeds the live DB (§2) — always isolate ad-hoc imports with `FLOWFILE_DB_PATH`.
3. Run logs live under `storage.logs_directory` (`shared/run_logs.py`), so `FLOWFILE_STORAGE_DIR` / docker / `TESTING` move them — don't assume the real home dir.
4. Flow logs survive core restarts (the shutdown wipe and hardcoded 7-day sweep were removed); both `flow_*.log` and `scheduled_run_*.log` are expired only by `shared.run_logs.cleanup_old_logs` (`FLOWFILE_RUN_LOG_RETENTION_DAYS`, default 30). Per-flow `flow_<id>.log` is still truncated at the start of each run, so only the run logs are true history.
5. Cache files older than 1h are deleted every time core starts — don't assume a `Status.file_ref` survives a restart.
6. Opening a flow renames it in-app to the file's stem; renaming the YAML on disk renames the flow.
7. Saving `.flowfile` raises `DeprecationWarning`; *loading* `.flowfile` still works via the legacy pickle path.
8. Docker-mode path sandboxing (§5) only rejects *relative* escapes — verify the current guard before treating it as absolute-path-safe.
9. `TESTING=True` uses one shared temp DB file — concurrent pytest sessions cross-drop tables; use per-session `FLOWFILE_DB_PATH` (see `flowfile-testing-and-validation`).
10. Kernel-exchange dirs (`shared_directory`, `global_artifacts_directory`, `artifact_staging_directory`) must stay under the kernel-mounted volume — don't relocate them via ad-hoc env overrides.
11. `flowfile_core.configs.settings` parses `sys.argv` at **import** time — importing core inside a process with unrelated `--host`/`--port`/`--worker-port` flags on argv silently repoints ports.
12. Root `CLAUDE.md` documentation drift found as of this writing: version says 0.11.0 (actual 0.12.7 — `shared/_version.py`); migrations "001–021" (actual head is 028); `docker-remote/` listed as a directory that doesn't exist (§4).
13. In zsh, `echo ===` breaks (`== not found`) if you paste separator lines from other shells — use `---` instead; unrelated to the app but easy to trip over when scripting diagnostics.

---

## 11. State-inspection runbook

Copy-paste, in order, when a flow/schedule/run silently didn't do what you expected.

```bash
# 1. Which mode? (electron is the default if unset; compose sets docker)
echo "${FLOWFILE_MODE:-electron (unset)}"

# 2. Catalog DB: tables, migration head, recent runs, schedules, registrations
sqlite3 ~/.flowfile/database/flowfile_catalog.db '.tables'
sqlite3 ~/.flowfile/database/flowfile_catalog.db 'select * from alembic_version;'      # expect 028 as of 2026-07-03
sqlite3 ~/.flowfile/database/flowfile_catalog.db \
  'select id,flow_name,started_at,ended_at,success,pid from flow_runs order by id desc limit 10;'
sqlite3 ~/.flowfile/database/flowfile_catalog.db 'select * from flow_schedules;'
sqlite3 ~/.flowfile/database/flowfile_catalog.db 'select id,flow_path from flow_registrations;'
# In docker mode the DB is inside the flowfile-internal-storage volume at
# /app/internal_storage/database/flowfile_catalog.db

# 3. Read the flow's own log (or stream it live via GET /logs/{flow_id})
tail -100 ~/.flowfile/logs/flow_<flow_id>.log
# run logs follow storage.logs_directory (<base>/logs; ~/.flowfile/logs by default,
# but FLOWFILE_STORAGE_DIR / docker / TESTING relocate them):
tail -100 ~/.flowfile/logs/scheduled_run_<run_id>.log

# 4. Worker offload / connectivity — core prints its resolved worker URL at startup
#    ("Worker configured at <WORKER_URL> (host: ..., port: ...)"); confirm co-hosting:
curl -s http://127.0.0.1:63578/single_mode

# 5. Stale worker-result cache (safe to delete; auto-cleans after 1h anyway)
ls ~/.flowfile/cache/<flow_id>/

# 6. Kernel containers (Python-script nodes) — host ports 19000-19999
docker ps --filter "name=flowfile-kernel"

# 7. Diagnose without touching live data — full isolation
FLOWFILE_DB_PATH=/tmp/ffdiag/cat.db \
FLOWFILE_STORAGE_DIR=/tmp/ffdiag/storage \
APPDATA=/tmp/ffdiag/appdata \
  poetry run python -m flowfile run flow /abs/path/to/flow.yaml
# add FLOWFILE_SKIP_STARTUP_MIGRATION=1 only if importing core against an
# ALREADY-migrated DB and you want to skip re-running Alembic (never on a fresh path)

# 8. Master-key problems in docker ("RuntimeError: Master key not configured...")
#    set FLOWFILE_MASTER_KEY or mount /run/secrets/flowfile_master_key; core and
#    worker must share the exact same key (compose passes it to both by default)

# 9. Docker stack logs / status
docker compose ps
docker compose logs -f flowfile-core
docker compose logs -f flowfile-worker
```

---

## Provenance and maintenance

Volatile facts above need periodic re-verification — commands are copy-pasteable, run from the repo root.

- **App version** (as of 2026-07-03: `0.12.7`): `cat shared/_version.py` and `grep -m1 '^version' pyproject.toml`
- **Alembic migration head** (as of 2026-07-03: `028`): `ls flowfile_core/flowfile_core/alembic/versions/ | sort | tail -3`
- **CLI verbs/flags** (§1): `sed -n '196,293p' flowfile/flowfile/__main__.py`
- **Import side effects** (§2): `sed -n '1,20p' flowfile/flowfile/__init__.py`; `sed -n '1,20p' flowfile_core/flowfile_core/__init__.py`; `sed -n '20,30p' flowfile_core/flowfile_core/database/init_db.py`
- **Headless run paths** (§3): `flowfile/flowfile/__main__.py:run_flow`, `flowfile_core/flowfile_core/main.py:_run_flow_cli`, `shared/subprocess_utils.py:spawn_flow_subprocess`
- **Scheduler poll interval / launch guard**: `grep -n "DEFAULT_POLL_INTERVAL\|_maybe_launch" flowfile_scheduler/flowfile_scheduler/engine.py`
- **`flowfile run ui` host/port rejection** (§1, §4): `grep -n "NotImplementedError" flowfile/flowfile/web/__init__.py`
- **Single-file mode env coupling**: `grep -n "SINGLE_FILE_MODE\|get_default_worker_url" flowfile_core/flowfile_core/configs/settings.py`
- **Core startup/shutdown side effects** (§4): `grep -n "cleanup_directories\|clear_all_flow_logs\|shutdown_handler" flowfile_core/flowfile_core/main.py`
- **`docker-remote/` non-existence** (§4): `ls docker-remote 2>&1; git log --all --oneline -- docker-remote` (both should be empty) — cross-check against `grep -n docker-remote CLAUDE.md` and re-read `docs/users/deployment/docker.md` for the current published-images story
- **Compose facts** (§4): `grep -n "shm_size\|FLOWFILE_SCHEDULER_ENABLED\|FLOWFILE_ENABLE_PROJECTS" docker-compose.yml`
- **Flow save/load format** (§5): `grep -n "def save_flow" -A 40 flowfile_core/flowfile_core/flowfile/flow_graph.py`; `sed -n '1,50p' flowfile_core/flowfile_core/flowfile/manage/io_flowfile.py` (look for `_validate_flow_path`, `open_flow`)
- **Storage directory table** (§6): `sed -n '1,280p' shared/storage_config.py` (every `@property` under `FlowfileStorage`)
- **DB URL resolution order + table count** (§6): `sed -n '395,420p' shared/storage_config.py`; `grep -c '__tablename__' flowfile_core/flowfile_core/database/models.py`
- **Catalog Delta layout** (§7): `grep -n "catalog_tables_directory\|file_path\|storage_format" flowfile_core/flowfile_core/database/models.py`
- **Master key resolution** (§8): `sed -n '1,50p' flowfile_core/flowfile_core/auth/secrets.py` (electron path), `sed -n '140,220p'` (docker path); `grep -n "generate_key\|force_key\|KEY_FILE" Makefile`
- **Log locations/formats** (§9): `grep -n "asctime" flowfile_core/flowfile_core/configs/flow_logger.py flowfile_worker/flowfile_worker/configs.py`; `grep -n '@router\.' flowfile_core/flowfile_core/routes/logs.py`
- **Kernel host port range**: `grep -n "_BASE_PORT\|_PORT_RANGE" flowfile_core/flowfile_core/kernel/manager.py`
