# Discovery Dossier — run-operate-data

Dimension: running, operating, and on-disk data conventions for the Flowfile monorepo.
Repo: `/Users/edwardvaneechoud/flowfile_backup/Flowfile` (branch at time of investigation: `feature/claude-skills`, HEAD `f6963c77`).
All claims verified by reading source or running read-only/isolated commands unless marked **inferred**.

**Version note:** actual runtime version is **0.12.7** (printed by `python -m flowfile`; from `shared/_version.get_version()`). Root CLAUDE.md says 0.11.0 — stale. Alembic head is **028** (`028_catalog_namespace_storage.py`), not 021 as root CLAUDE.md says.

---

## 1. The `flowfile` CLI — every verb and flag (flowfile/flowfile/__main__.py)

Entry point: poetry script `flowfile = "flowfile.__main__:main"` (root `pyproject.toml:89`). Argparse definition at `flowfile/flowfile/__main__.py:196-225`.

```
flowfile [command] [component] [file_path] [--host H] [--port P] [--no-browser]
         [--param KEY=VALUE ...] [--run-id N]
```

| positional | choices | meaning |
|---|---|---|
| `command` | `run`, `seed-demo`, `remove-demo`, `project` | top-level verb |
| `component` | `ui`, `core`, `worker`, `flow`, `init`, `open`, `save` | sub-verb (run target OR project sub-command) |
| `file_path` | free | flow file path, project folder, or version message |

| flag | default | applies to | notes |
|---|---|---|---|
| `--host` | `127.0.0.1` | `run core` / `run worker` ONLY | **ignored by `run ui`** (see gotcha below) |
| `--port` | `63578` | `run core` / `run worker` ONLY | ignored by `run ui` |
| `--no-browser` | off | `run ui` | skips `webbrowser.open_new_tab` |
| `--param KEY=VALUE` | none, repeatable | `run flow` | overrides/creates a `FlowParameter` (`__main__.py:52-69`) |
| `--run-id N` | None | `run flow` | pre-created `FlowRun` row id; results reported back via `shared.run_completion.complete_run` |

### Verb anatomy (verified against code)

- **`flowfile run ui`** (`__main__.py:228-234`) → `flowfile.web.start_server(open_browser=not args.no_browser)`. **`--host`/`--port` are NOT passed through**, and `start_server` (`flowfile/flowfile/web/__init__.py:141-171`) raises `NotImplementedError` for any host ≠ `127.0.0.1` or port ≠ `63578`. The no-args usage text advertising `flowfile run ui --host 0.0.0.0 --port 8080` is **misleading/dead** — those flags only affect `run core`/`run worker`.
- **`flowfile run core`** → `flowfile_core.main.run(host, port)` (direct core service, honors `--host/--port`).
- **`flowfile run worker`** → `flowfile_worker.main.run(host, port)`. Worker's own argparse (when launched via `poetry run flowfile_worker` / `python -m flowfile_worker.main`) additionally supports `--core-host` / `--core-port` (`flowfile_worker/configs.py:23-33`).
- **`flowfile run flow <path>`** → `run_flow()` (`__main__.py:7-126`). Details in §3.
- **`flowfile seed-demo` / `remove-demo`** → `flowfile_core.catalog.demo_seed.seed_demo_catalog()` / `remove_demo_catalog()`. Seeds a `Demo` catalog (namespaces `sales_analytics`, `market`), 4+3 Delta tables, 2 registered flows and 1 cron schedule. Verified live (isolated env): prints `Demo catalog seeded: {'tables_created': ['regions', 'products', 'customers', 'sales'], 'sales_flow_registration_id': 1, 'fx_flow_registration_id': 2, 'schedule_id': 1, 'fx_populate': 'triggered'}`.
- **`flowfile project {init|open|save} <folder-or-message>`** → `_run_project_command` (`__main__.py:151-193`):
  - `init <folder>`: validates via `validate_path_under_cwd` (fileExplorer/funcs.py:419; electron = any local root, docker/package = CWD ∪ `storage.base_directory` ∪ `storage.user_data_directory`; rejects any `..`), then `project_sync.init_project(folder, Path(folder).name, owner_id)` — creates `flowfile-project.yaml`-style manifest + `.gitignore`, `git init`, projects DB→files, commits "Initialize Flowfile project". Prints `Initialized project '<name>' at <root>`.
  - `open <folder>`: `project_sync.open_project` → prints imported flows/connections/schedules counts, then `N value(s) need to be set (FLOWFILE_SECRET_<NAME> or update them in the app).` for placeholder secrets.
  - `save "<message>"`: `project_sync.save_version(owner_id, message)` → projects DB→files, `git commit`, prints `Saved version <sha8>` or `(no changes)`.
  - Owner is `get_local_user_id()`. Multi-tenant confinement: docker/package mode confines projects to `<user_data>/projects/<owner_id>` (`project/service.py:41-49`); electron unconfined.
- **no args** → prints version + usage (verified live: `FlowFile v0.12.7 ...`).

### Import side effects (critical)

`import flowfile` (package `__init__.py:17-18`) **sets env vars**:
```python
os.environ["FLOWFILE_WORKER_PORT"] = "63578"
os.environ["FLOWFILE_SINGLE_FILE_MODE"] = "1"
```
So *any* `python -m flowfile ...` invocation runs in single-file mode. It also imports `flowfile_core`, whose `__init__.py` (flowfile_core/flowfile_core/__init__.py:8,15) runs `validate_setup()` (node-registry sanity check) and `init_db()` **at import time** — which runs Alembic migrations against the live catalog DB (`database/init_db.py:26: if not os.environ.get("FLOWFILE_SKIP_STARTUP_MIGRATION"): run_startup_migration()`) and seeds the default `local_user`. **Always isolate diagnostics with `FLOWFILE_DB_PATH` (and optionally `FLOWFILE_SKIP_STARTUP_MIGRATION=1`)**.

Also `flowfile/__init__.py:252` mutes `PipelineHandler` logger to WARNING.

---

## 2. Flow file format — save/load/serialization

### Format
YAML (preferred) or JSON. Root Pydantic model `schemas.FlowfileData` (`flowfile_core/flowfile_core/schemas/schemas.py:364-372`):

```yaml
flowfile_version: 0.12.7        # stamped with app __version__ at save (flow_graph.py:5470)
flowfile_id: 424242             # int
flowfile_name: my_flow
flowfile_settings:              # FlowfileSettings: description, execution_mode (Development|Performance),
                                # execution_location (local|remote|...), auto_save, show_detailed_progress,
                                # max_parallel_workers, source_registration_id, parameters[]
nodes:
  - id: 1
    type: manual_input          # node type == FlowGraph method suffix ("add_" + type)
    is_start_node: true
    description: ...
    node_reference: ...         # optional stable ref for codegen
    x_position: 100
    y_position: 100
    group_id: null              # visual group membership
    left_input_id / right_input_id / input_ids: []   # join-style vs main inputs
    outputs: [2]                # downstream node ids (connections derived from these)
    output_handles: [output-0]  # parallel to outputs; missing => "output-0" (schemas.py:289-292)
    input_connections: ...      # keyed edges for dynamic-input nodes only
    setting_input: {...}        # node-type-specific settings dict
groups: []                      # visual FlowfileGroup boxes (id, name, color, x/y, w/h, collapsed)
```

### Save — `FlowGraph.save_flow(flow_path)` (`flowfile/flow_graph.py:5529-5577`)
- `.yaml`/`.yml` → `yaml.dump(model_dump(mode="json"), default_flow_style=False, sort_keys=False, allow_unicode=True)`
- `.json` → `json.dump(..., indent=2, ensure_ascii=False)`
- `.flowfile` → **raises `DeprecationWarning`** ("The .flowfile format is deprecated. Please use .yaml or .json")
- unknown extension → warns and **defaults to YAML**.
- `setting_input` serialization (`schemas.py:297-323`): a `field_serializer` strips the runtime-injected fields `{flow_id, node_id, pos_x, pos_y, group_id, is_setup, description, node_reference, user_id, is_flow_output, is_user_defined, depending_on_id, depending_on_ids}` — those are re-injected at load. `NodePromise` settings serialize to `None`; objects with `to_yaml_dict()` use that.
- Save also: sets `modified_on`, syncs catalog read-links (`_sync_catalog_read_links`, records which catalog tables the flow reads, keyed by `source_registration_id`), `mark_as_saved()` (dirty-tracking baseline), and re-derives flow name from the filename stem (`_handle_flow_renaming`).
- Groups with no members are pruned at save (`flow_graph.py:5463-5468`).

### Load — `open_flow(flow_path, user_id=None)` (`flowfile/manage/io_flowfile.py:287-392`)
- Dispatch by suffix (`_load_flow_storage`, io_flowfile.py:250-284): `.flowfile` = **legacy pickle** via `load_flowfile_pickle` (a custom `LegacyUnpickler` mapping old dataclass names through `tools.migrate.legacy_schemas.LEGACY_CLASS_MAP`, `manage/compatibility_enhancements.py:17-49`) + `ensure_compatibility` migration; `.yaml`/`.yml` via `yaml.safe_load` → `FlowfileData.model_validate`; `.json` likewise.
- Path validation (`_validate_flow_path`, io_flowfile.py:19-46): allowed extensions `{.yaml,.yml,.json,.flowfile}`; in **docker mode** paths must live under `storage.flows_directory` / `uploads_directory` / `temp_directory_for_flows`; local modes accept anything.
- Settings class resolved per node type via `get_settings_class_for_node_type(node.type)`; unknown type ⇒ `ValueError("Unknown node type: ...")`.
- Backward-compat handling: missing `output_handles` → `DEFAULT_OUTPUT_HANDLE` ("output-0"); legacy pickles may lack `groups`/`output_handles` entirely (getattr fallbacks at io_flowfile.py:342,389); `depending_on_id`/`depending_on_ids` reconstructed from `input_ids`+left/right; output nodes get `table_settings` back-filled from `file_type` (io_flowfile.py:193-199).
- **Name is overridden by filename**: `open_flow` sets `flow_settings.name = flow_path.stem` (io_flowfile.py:306-307) — the stored `flowfile_name` is cosmetic after a rename on disk.
- Insertion order determined by graph walk (`determine_insertion_order`), nodes added as promises then configured via `getattr(new_flow, "add_" + node_info.type)(setting_input)`; user-defined nodes go through `CUSTOM_NODE_STORE` (skipped silently if the custom node type isn't installed, io_flowfile.py:330-331).
- There is **no version-gated migration on `flowfile_version`** — compat is structural (Pydantic defaults + the legacy pickle path). `flowfile_version` is informational.

### Where flows live on disk
- Quick-created ("unnamed") flows: `storage.flows_directory/unnamed_flows/` named `YYYYMMDD_HH_MM_SS_flow.yaml` (`flowfile/handler.py:14-27`). Persisted, not temp, so they survive cleanup. `FlowfileHandler.add_flow(persist=False)` keeps scratch flows memory-only until first save/run.
- Python-API-built flows (FlowFrame / demo flows): `storage.flows_directory/python_editor_flows/<stem>.yaml` (`flowfile/catalog_helpers.py:276`).
- `open_graph_in_editor(flow_graph, storage_location=None)` (`flowfile/flowfile/api.py:399-460`): saves to `storage_location` or a `TemporaryDirectory(prefix="flowfile_graph_")/temp_flow_<hex8>.yaml`, forces `execution_location="local"` + `execution_mode="Development"` for the saved copy, auto-starts the unified server if `/docs` isn't responding (waits up to 60 s), imports via API, opens `http://127.0.0.1:63578/ui/flow/<id>` in a browser when in electron/unified mode.

---

## 3. Headless flow execution — three equivalent paths (all verified)

### A. CLI (dev / pip install)
```bash
flowfile run flow /abs/path/pipeline.yaml [--param k=v ...] [--run-id N]
# or: python -m flowfile run flow ...
```
Behavior (`flowfile/__main__.py:7-126`):
1. `OFFLOAD_TO_WORKER.set(False)` — **no worker service needed**; compute runs in-process.
2. `open_flow(path, user_id=...)` — user resolved from `--run-id`'s FlowRun row (`get_run_user_id`), else `get_local_user_id()`.
3. `--param` overrides merge into `flow.flow_settings.parameters` (existing param → `default_value` replaced; unknown key → appended as new `FlowParameter`).
4. Forces `flow.execution_location = "local"`; **deletes all `explore_data` nodes** (UI-only, need a worker) and prints `Skipping N explore_data node(s) (UI-only)`.
5. Stamps catalog lineage: `resolve_source_registration_id(flow)` before running so catalog writes record the producer.
6. `flow.run_graph()`; prints ASCII graph + execution order (`flow.print_tree()`), per-node success, and either `Flow completed successfully in X.XXs` + `Nodes completed: n/m` (exit 0) or `Flow execution failed` + per-node errors on stderr (exit 1).
7. If `--run-id` given, updates the `flow_runs` row via `shared.run_completion.complete_run` (raw SQLAlchemy on the shared DB; sets ended_at/success/nodes_completed/duration_seconds).

Verified end-to-end with a 2-node flow (manual_input → record_count); output includes the "Flow Graph Visualization" tree and `Nodes completed: 2/2`, and a per-flow log landed at `<storage>/logs/flow_<flow_id>.log`.

### B. PyInstaller / sidecar binary path
```bash
python -m flowfile_core.main --run-flow <path> --run-id <id>   # --run-id REQUIRED here
```
`flowfile_core/main.py:386-402` → `_run_flow_cli` (main.py:262-361): same semantics as A (offload off, execution local, explore_data dropped) but implemented inside core because the top-level `flowfile` package isn't bundled in the frozen binary. Missing `--run-id` exits 1 with `Error: --run-id is required`.

### C. Scheduler-spawned
`shared/subprocess_utils.py:19-54` `spawn_flow_subprocess(flow_path, run_id)`:
- frozen: `[sys.executable, --run-flow, path, --run-id, N]`
- dev: `[sys.executable, -m, flowfile, run, flow, path, --run-id, N]`
- stdout+stderr redirected to **hardcoded** `Path.home()/.flowfile/logs/scheduled_run_<run_id>.log` (0644, truncated per run) — note this ignores `FLOWFILE_STORAGE_DIR`.
- `start_new_session=True` (fire-and-forget); returns child PID or None.

Scheduler (`flowfile_scheduler/flowfile_scheduler/engine.py`): polls the shared SQLite DB every `DEFAULT_POLL_INTERVAL = 30`s; supports cron, interval, and table-trigger schedules; `_maybe_launch` (engine.py:399-449) skips if the registration already has an active run (`ended_at IS NULL`), creates the `FlowRun` row **before** spawning (run_type="scheduled"), records `pid`, and marks failed if the spawn fails. Standalone run: `poetry run flowfile_scheduler` (continuous) or `--once` (single tick) (`flowfile_scheduler/__main__.py`). Embedded in core only when `FLOWFILE_SCHEDULER_ENABLED ∈ {true,1,yes}` (main.py:73-77).

---

## 4. Service startup — commands, order, co-hosting

### Local dev (three terminals)
```bash
poetry run flowfile_core     # :63578 — start FIRST
poetry run flowfile_worker   # :63579 — needed for remote-execution flows / UI compute offload
cd flowfile_frontend && npm run dev:web   # :8080, proxies /api → :63578
```
Startup order: core before frontend (Vite proxies `/api` to core; nginx in docker `proxy_pass http://flowfile-core:63578/`, `flowfile_frontend/nginx.conf:2,10`; compose `depends_on: [flowfile-core, flowfile-worker]`). Worker is independent — core starts without it, but any worker-offloaded node run will fail until it's up. Worker calls back to core at `http://<CORE_HOST>:63578` only to ship logs (`/raw_logs`).

### Unified single-process mode (pip install story)
```bash
flowfile run ui [--no-browser]
```
`flowfile/web/__init__.py:141-171 start_server`: fixed `127.0.0.1:63578`; sets `FLOWFILE_MODE=electron` if unset; `OFFLOAD_TO_WORKER.value = True`; `extend_app(core_app)`:
- mounts static Vue build from `flowfile/web/static/` at `/ui` (missing build ⇒ `{"error": "Web UI not installed..."}`),
- adds `StripApiPrefixMiddleware` (strips `/api` prefix so the docker-oriented frontend base URL works),
- `GET /single_mode` returns whether `FLOWFILE_SINGLE_FILE_MODE=1`,
- **mounts the worker router at `/worker` on the same app** (`include_worker_routes`, web/__init__.py:105-137) with shutdown cleanup of `mp_context.active_children()` + `CACHE_DIR.cleanup()`.

`FLOWFILE_SINGLE_FILE_MODE` mechanics: env read once into `SINGLE_FILE_MODE: MutableBool` (`flowfile_core/configs/settings.py:19`). `get_default_worker_url` (settings.py:95-117) appends `/worker` to the worker URL when set — so core's offload POSTs go to `http://…:63578/worker/...` on itself. The `flowfile` package import forces both `FLOWFILE_WORKER_PORT=63578` and `FLOWFILE_SINGLE_FILE_MODE=1`. Browser open happens after `time.sleep(5)` and *before* uvicorn starts (web/__init__.py:161-163) — the tab may load before the server listens.

- Quirk: `webbrowser.open_new_tab` targets `/ui`; API docs at `/docs`.

### Core service internals relevant to ops (`flowfile_core/main.py`)
- import-time: `storage.cleanup_directories()` (main.py:51) — ages out temp>24h, cache>1h, logs>168h, system_logs>168h (storage_config.py:335-340). So **starting core deletes cache files older than 1 hour**.
- lifespan (`shutdown_handler`, main.py:60-95): `logging.basicConfig(INFO, "%(asctime)s [%(levelname)s] %(name)s: %(message)s")` → stdout (Electron pipes it); starts scheduler iff `FLOWFILE_SCHEDULER_ENABLED`; on shutdown stops scheduler, `_shutdown_kernels()` (all Docker kernel containers), `_shutdown_local_model()`, `clear_all_flow_logs()` (deletes every `*.log` in the logs dir!).
- `POST /shutdown` endpoint triggers graceful uvicorn exit (used by the Tauri shutdown ladder); SIGTERM/SIGINT handled; `start_parent_death_watcher` exits if the Tauri parent dies.
- CLI arg parsing happens at **import** of `flowfile_core.configs.settings` (`parse_known_args`, settings.py:120) — `--host/--port/--worker-port` are read from `sys.argv` of whatever process imports settings.

### Docker (build-from-source stack, `docker-compose.yml`)
```bash
cp .env.example .env   # set JWT_SECRET_KEY, FLOWFILE_MASTER_KEY, FLOWFILE_INTERNAL_TOKEN, admin creds
docker compose up -d
# frontend http://localhost:8080, core :63578, worker :63579
```
Compose facts (verified in file): core gets `/var/run/docker.sock` (kernel management), `shm_size: 2gb` on core+worker, `FLOWFILE_SCHEDULER_ENABLED=true`, `FLOWFILE_ENABLE_PROJECTS` default true, network fixed-name `flowfile-network` (so API-created kernel containers can join). Kernel images are built via profiles only: `docker compose --profile kernel build flowfile-kernel` (base/ml/lite variants tagged `flowfile-kernel-*:local`; point core at them with `FLOWFILE_KERNEL_IMAGE=...`). Core/worker Dockerfiles: `CMD python -m flowfile_core.main` / `python -m flowfile_worker.main`, healthcheck `curl -f http://localhost:6357{8,9}/docs`.

### "docker-remote" deployment story
**`docker-remote/` does NOT exist** — not in the working tree and not in any commit (`git log --all -- docker-remote` is empty). Root CLAUDE.md's mention is stale/aspirational. The published-images deployment story lives in **`docs/users/deployment/docker.md`**: a sample compose using `edwardvaneechoud/flowfile-{frontend,core,worker}:latest` images (kernels `edwardvaneechoud/flowfile-kernel-base:0.3.0` / `-ml:0.3.0`), same env vars/volumes as the source compose (volume named `flowfile-storage` there instead of `flowfile-internal-storage`). Ops commands documented there: `docker compose up -d | down | pull | logs -f`.

---

## 5. Filesystem map — the `storage` singleton (shared/storage_config.py)

`storage = FlowfileStorage()` instantiated **at import** (line 343); `__init__` eagerly `mkdir -p`s all internal+user dirs (`_ensure_directories`, lines 248-277). Importing `shared` has filesystem side effects.

Two roots:
- **base (internal)**: `FLOWFILE_STORAGE_DIR` env, else `~/.flowfile` locally, else `/app/internal_storage` in docker mode (lines 40-52). Docker mode = `FLOWFILE_MODE == "docker"` exactly.
- **user data**: local = `Path.home()`; docker = `FLOWFILE_USER_DATA_DIR` env else `/data/user` (lines 55-64). Compose sets it to `/app/user_data` (host bind `./flowfile_data`).

### Directory table (all verified in code; live-verified locally by running the CLI with `FLOWFILE_STORAGE_DIR` pointed at scratch)

| property | local path | docker path | created eagerly | purpose |
|---|---|---|---|---|
| `cache_directory` | `<base>/cache` | same under `/app/internal_storage` | yes | worker↔core IPC; Arrow results at `cache/<flow_id>/<task_id>.arrow` (worker routes.py:84,128,295,…; `get_flow_cache_directory`) — **cleaned when >1 h old at core start** |
| `database_directory` | `<base>/database` | 〃 | yes | `flowfile_catalog.db` (and legacy `flowfile.db`) |
| `logs_directory` | `<base>/logs` | 〃 | yes | per-flow logs `flow_<flow_id>.log`; 168 h cleanup |
| `system_logs_directory` | `<base>/system_logs` | 〃 | yes | reserved; **no writer found in code today** (only referenced in prompt_log docstring) |
| `temp_directory` | `<base>/temp` | 〃 | yes | scratch; 24 h cleanup |
| `temp_directory_for_flows` | `<base>/temp/flows` | 〃 | yes | flow-scoped temp |
| `shared_directory` | `<base>/temp/kernel_shared` (or `$FLOWFILE_SHARED_DIR`) | 〃 | yes | core↔worker↔kernel exchange; **must stay on the kernel-visible volume** |
| `artifact_staging_directory` | `<base>/temp/kernel_shared/artifact_staging` (or `$FLOWFILE_SHARED_DIR/artifact_staging`) | 〃 | yes | artifact upload staging |
| `global_artifacts_directory` | `<base>/temp/kernel_shared/global_artifacts` (or `$FLOWFILE_SHARED_DIR/global_artifacts`) | 〃 | yes (listed under *user* dirs) | permanent artifacts (ML models etc.) |
| `flows_directory` | `<base>/flows` | `<user_data>/flows` | yes | saved flow YAMLs |
| `unnamed_flows_directory` | `<flows>/unnamed_flows` | 〃 | yes | quick-created flows `YYYYMMDD_HH_MM_SS_flow.yaml` |
| `python_editor_flows_directory` | `<flows>/python_editor_flows` | 〃 | yes | FlowFrame/API-registered flows |
| `uploads_directory` | `<base>/uploads` | `<user_data>/uploads` | yes | user uploads |
| `outputs_directory` | `<base>/outputs` | `<user_data>/outputs` | yes | user outputs |
| `user_defined_nodes_directory` (+`/icons`) | `<base>/user_defined_nodes` | `<user_data>/user_defined_nodes` | yes | custom node code + icons |
| `catalog_tables_directory` | `<base>/catalog_tables` | `<user_data>/catalog_tables` | yes | **Delta Lake tables** (see §6) |
| `catalog_virtual_results_directory` | `<base>/catalog_virtual_results` | `<user_data>/catalog_virtual_results` | yes | worker IPC cache (`.arrow`) for materialized virtual tables (worker funcs.py:1101) |
| `notebooks_directory` | `<base>/notebooks` | `<user_data>/notebooks` | yes | catalog notebook cells keyed by `notebook_uuid` (per-owner subdir resolved by notebook store) |
| `template_data_directory` | `<base>/template_data` | 〃 (base) | yes | cached template CSVs |
| `local_model_directory` | `<base>/local_model` | 〃 | **no** (opt-in) | llama.cpp binary + GGUF |
| `ai_sessions_directory` | `<base>/ai_sessions` | `<user_data>/ai_sessions` | **no** | persisted AI agent sessions |
| ai prompts (not a property; prompt_log.py `LOG_SUBDIR`) | `<base>/ai_prompts/YYYY-MM-DD.jsonl` | 〃 | on first log | LLM prompt log, UTC-dated, gated by `FLOWFILE_AI_LOG_PROMPTS` |

Live verification: after `python -m flowfile run flow …` with `FLOWFILE_STORAGE_DIR=<scratch>/storage`, the tree contained exactly: `cache, catalog_tables, catalog_virtual_results, database, flows{,/python_editor_flows,/unnamed_flows}, logs/flow_424242.log, notebooks, outputs, system_logs, temp{,/flows,/kernel_shared/{artifact_staging,global_artifacts}}, template_data, uploads, user_defined_nodes{,/icons}`.

### Cleanup policy (`storage.cleanup_directories`, storage_config.py:335-340) — run at core import
- `temp` > 24 h, `cache` > 1 h, `logs` > 168 h, `system_logs` > 168 h (mtime-based, per top-level entry).

### Catalog DB resolution (`get_database_url`, storage_config.py:402-417)
1. `FLOWFILE_DB_PATH` env → `sqlite:///<that path>`
2. `TESTING=True` → `sqlite:///<base>/temp/test_flowfile_catalog.db` (single shared test DB — concurrent pytest sessions clobber each other; use `FLOWFILE_DB_PATH` per session)
3. default → `sqlite:///<base>/database/flowfile_catalog.db`

Legacy migration: `get_legacy_database_path()` returns `<base>/database/flowfile.db` if it exists (one-time copy into the new DB at startup; skipped when `FLOWFILE_DB_PATH` set). Migration orchestration in `flowfile_core/database/migration.py` (fresh install / legacy copy / pending upgrades; re-stamps unknown revisions from branch switching — `_ensure_known_revision`). Skip with `FLOWFILE_SKIP_STARTUP_MIGRATION=1`.

DB tables (verified live on a freshly created DB, alembic head **028**): `ai_audit_events, ai_provider_credentials, alembic_version, api_consumer_endpoints, api_consumers, catalog_dashboards, catalog_namespaces, catalog_notebooks, catalog_table_read_links, catalog_tables, catalog_visualizations, cloud_storage_connections, cloud_storage_permissions, database_connections, db_info, flow_api_endpoints, flow_api_keys, flow_favorites, flow_follows, flow_registrations, flow_runs, flow_schedules, global_artifacts, google_analytics_connections, kafka_connections, kernels, resource_grants, schedule_trigger_tables, scheduler_lock, secrets, table_favorites, user_group_memberships, user_groups, users, workspace_projects`. Fresh non-docker DB seeds one user: `local_user`.

---

## 6. Catalog Delta Lake on-disk layout (live-verified via `seed-demo`)

- Each catalog table = one **directory** under `catalog_tables_directory`, a standard Delta table:
  ```
  catalog_tables/
    demo_sales/                          # seed-demo naming: demo_<name>
      _delta_log/00000000000000000000.json
      part-00000-<uuid>-c000.snappy.parquet
    sales_by_region_73919c20/            # flow-written naming: <table_name>_<8-hex>  (worker routes.py:544-548)
    catalog_<32-hex>/                    # unnamed fallback
  ```
- DB row (`catalog_tables`): `file_path` holds the **absolute** table dir, `storage_format='delta'`, `table_type` `physical|virtual…`, `schema_json`, `row_count`, `size_bytes`, lineage cols `source_registration_id`/`producer_registration_id`, `serialized_lazy_frame` BLOB + `sql_query` for virtual tables.
- Namespace tree in `catalog_namespaces` (verified: `General` root with children `default`, `Local Flows`, `Unnamed Flows`; seed adds `Demo` → `sales_analytics`, `market`).
- Reads/writes go through the worker only: `flowfile_worker/catalog_reader.py` (`open_catalog_table` = `scan_delta`, `open_virtual_result` = `scan_ipc`), with **every path validated** under the two catalog roots via `shared.delta_utils.validate_catalog_path` (worker funcs.py:33,58).
- Optional object-storage backend per catalog: level-0 namespace carries `storage_uri` + `storage_connection_name` (migration 028); resolved by `flowfile_core/catalog/storage_backend.py::resolve_for_namespace` — unset ⇒ local target rooted at `catalog_tables_directory` (line 90). Env pair `FLOWFILE_CATALOG_STORAGE_URI`/`_CONNECTION` is only a creation-time default for new catalogs (per `.env.example`), never a live override.
- Virtual-table materializations land as Arrow IPC in `catalog_virtual_results/` (worker `funcs.py:1101`; `ipc_path` is a bare filename in worker models.py:341).

---

## 7. Secrets & master key handling

- **Docker mode** (`flowfile_core/auth/secrets.py:140-217`): key = `FLOWFILE_MASTER_KEY` env (stripped of quotes, validated as Fernet) → else Docker secret file `/run/secrets/flowfile_master_key` → else `RuntimeError` at first use; `GET /public` reports `setup_required` via `is_master_key_configured()` and the UI setup wizard can generate one (`routes/public.py`).
- **Electron/local**: `SecureStorage` (auth/secrets.py:15-100) writes to `$APPDATA/flowfile` (or `~/.config/flowfile`): a Fernet key file `.secret_key` (0600) plus `flowfile.json.enc` (encrypted JSON store holding the `master_key` entry). Auto-generates the master key on first use. Non-electron non-docker: `SECURE_STORAGE_PATH` env, default `/tmp/.flowfile`. Verified live: running the CLI created `appdata/flowfile/.secret_key` (44 bytes, 0600).
- **Worker mirrors this read-only** (`flowfile_worker/secrets.py`): same `$ffsec$1$<user_id>$<token>` format, same HKDF salt `KEY_DERIVATION_VERSION = b"flowfile-secrets-v1"` — must stay byte-identical to core. `TEST_MODE` env returns a fixed key.
- **Repo-root `master_key.txt`** is a *build* artifact: `make generate_key` writes a fresh Fernet key there if absent; `make force_key` regenerates (Makefile:151-169; `KEY_FILE := master_key.txt`). Consumed by `test-docker-auth.yml` CI and available as a Docker secret source; gitignored; also in every project's `.gitignore` template (`project/manifest.py:47,63`).

---

## 8. Logs — who writes what, where

| log | location | writer / format |
|---|---|---|
| per-flow execution log | `<base>/logs/flow_<flow_id>.log` | `FlowLogger` (`flowfile_core/configs/flow_logger.py:333-335`), `FileHandler`, format `%(asctime)s - %(levelname)s - %(message)s`; node lines prefixed `Node ID: <n> - ` |
| scheduled-run subprocess output | **`~/.flowfile/logs/scheduled_run_<run_id>.log`** (hardcoded home, truncated each run) | `shared/subprocess_utils.py:37-40` |
| core service log | stdout only — `logging.basicConfig(INFO, "%(asctime)s [%(levelname)s] %(name)s: %(message)s")` in lifespan (main.py:68) and `init_db.py:19`; Electron/Tauri pipes it | no file handler |
| worker service log | stdout only — `logging.basicConfig(format="%(asctime)s: %(message)s")` (`flowfile_worker/configs.py:9`, `funcs.py:72`); worker subprocesses **ship flow logs to core** via `FlowfileLogHandler` POST `/raw_logs` (`routes/logs.py:45`) so they land in the same `flow_<id>.log` | no file |
| AI prompt log | `<base>/ai_prompts/YYYY-MM-DD.jsonl` (UTC-dated) when `FLOWFILE_AI_LOG_PROMPTS` truthy; optional scrub flag | `flowfile_core/ai/prompt_log.py` — one JSONL line per LLM call, 256 KiB soft cap w/ truncation |
| docker logs | `docker compose logs -f [service]` | containers log to stdout |

Access/ops:
- Stream a flow's log over SSE: `GET /logs/{flow_id}` (JWT via query param, idle timeout 300 s), append: `POST /logs/{flow_id}`, worker ingest: `POST /raw_logs`, wipe all: `POST /clear-logs` (`routes/logs.py`).
- Prompt-log CLI (verified `_cli_main` in prompt_log.py:444-467):
  `python -m flowfile_core.ai.prompt_log tail [N]` (default 10) and `... grep PATTERN [SURFACE]`.
- Retention: flow logs auto-deleted when older than 7 days (`cleanup_old_logs`) and by the 168 h sweep; **`clear_all_flow_logs()` runs at every core shutdown** (main.py:94) — flow logs do not survive a core restart.

---

## 9. Inspecting state when things break (runbook)

1. **Which mode am I in?** `FLOWFILE_MODE` (unset ⇒ `electron`; compose sets `docker`). Mode changes: storage roots, flow-path sandboxing, master-key source, auth behavior, sharing/catalog access.
2. **DB**: `sqlite3 ~/.flowfile/database/flowfile_catalog.db '.tables'`; `select * from alembic_version;` (expect `028`); runs: `select id,flow_name,started_at,ended_at,success,pid from flow_runs order by id desc limit 10;`; schedules: `flow_schedules`; registrations (flow_path lives here): `flow_registrations`. In docker the DB is inside the `flowfile-internal-storage` volume at `/app/internal_storage/database/`.
3. **Flow failed in UI**: read `~/.flowfile/logs/flow_<flow_id>.log` (or SSE endpoint). Scheduled runs: `~/.flowfile/logs/scheduled_run_<run_id>.log` (always under real home).
4. **Worker offload issues**: core prints `Worker configured at <WORKER_URL> ...` at startup (main.py:230). Single-file mode appends `/worker`; check `GET :63578/single_mode`. Windows defaults hosts to `127.0.0.1`, else `0.0.0.0` (settings.py:112-115,127).
5. **Stale caches**: worker results are `cache/<flow_id>/<task_id>.arrow` under base; safe to delete (1 h auto-clean at core start).
6. **Diagnostics without touching live data** (memory-verified pattern):
   ```bash
   FLOWFILE_DB_PATH=/tmp/x/cat.db FLOWFILE_STORAGE_DIR=/tmp/x/storage APPDATA=/tmp/x/appdata \
     poetry run python -m flowfile ...
   ```
   (add `FLOWFILE_SKIP_STARTUP_MIGRATION=1` if you must import core against the real DB without migrating).
7. **Kernel containers**: host ports 19000-19999; local mode bind-mounts `<base>/cache` → `/shared` and `catalog_tables` → `/catalog_tables` (kernel/manager.py:630-636); docker-in-docker mounts the same named volumes at identical paths (manager.py:599-624). `docker ps` + core lifespan stops them all on shutdown.
8. **Master-key problems in docker**: `RuntimeError: Master key not configured...` — set `FLOWFILE_MASTER_KEY` or mount `/run/secrets/flowfile_master_key`; core and worker MUST share the same key (compose passes it to both).

---

## 10. Verified commands (all run or line-verified in this session)

```bash
# CLI usage/version (isolated env) — printed "FlowFile v0.12.7" + usage block
FLOWFILE_DB_PATH=$S/cat.db FLOWFILE_STORAGE_DIR=$S/storage APPDATA=$S/appdata \
  poetry run python -m flowfile

# Headless flow run, end-to-end (exit 0, per-node progress, flow log written)
poetry run python -m flowfile run flow /abs/path/flow.yaml
# with params + scheduler-style run record:
poetry run python -m flowfile run flow flow.yaml --param input_dir=/data --run-id 42

# Demo catalog seed/teardown (writes Delta dirs + registers flows/schedule)
poetry run python -m flowfile seed-demo
poetry run python -m flowfile remove-demo

# Services
poetry run flowfile_core                     # :63578
poetry run flowfile_worker                   # :63579 (--host/--port/--core-host/--core-port)
poetry run flowfile_scheduler [--once]       # standalone schedule poller
flowfile run ui --no-browser                 # unified single-process on 127.0.0.1:63578 (fixed)
python -m flowfile_core.main --run-flow F --run-id N   # frozen-binary flow runner

# Projects (headless git tracking)
flowfile project init <folder> && flowfile project open <folder> && flowfile project save "msg"

# Prompt log
python -m flowfile_core.ai.prompt_log tail 20
python -m flowfile_core.ai.prompt_log grep PATTERN [SURFACE]

# State inspection
sqlite3 ~/.flowfile/database/flowfile_catalog.db '.tables'
sqlite3 ... 'select * from alembic_version;'          # → 028
```

---

## 11. Gotchas (each verified)

1. `flowfile run ui --host/--port` is a lie: flags parsed but never passed; `start_server` hard-rejects non-defaults (`web/__init__.py:153-156`). Root CLAUDE.md and the CLI's own usage text both repeat the misleading example.
2. `import flowfile` mutates env (`FLOWFILE_WORKER_PORT=63578`, `FLOWFILE_SINGLE_FILE_MODE=1`) and importing `flowfile_core` runs Alembic + seeds users. Never import either in an ad-hoc script against production state without `FLOWFILE_DB_PATH` isolation.
3. Scheduled-run logs ignore `FLOWFILE_STORAGE_DIR` — hardcoded `Path.home()/.flowfile/logs/scheduled_run_*.log` (`shared/subprocess_utils.py:37`).
4. Flow logs are wiped on core shutdown (`clear_all_flow_logs()` in lifespan) and by 7-day sweeps — don't rely on them as history; the `flow_runs` DB table is the durable record.
5. Cache purge on startup: files in `<base>/cache` older than 1 h die when core starts; results referenced by `Status.file_ref` can disappear between sessions.
6. `open_flow` renames the flow to the file stem; renaming the YAML renames the flow.
7. Saving to `.flowfile` raises; loading `.flowfile` still works via the legacy unpickler (`.flowfile` load requires the file extension check to pass in `_validate_flow_path`).
8. In docker mode `open_flow` only accepts paths under flows/uploads/temp-flows dirs — a bind-mounted flow elsewhere 403s/ValueErrors.
9. `TESTING=True` uses ONE shared temp DB file — parallel pytest sessions cross-drop tables; per-session `FLOWFILE_DB_PATH` is the fix (user memory + storage_config.py:414).
10. Kernel-exchange dirs (`shared_directory`, `global_artifacts`, `artifact_staging`) must remain under the kernel-mounted volume (`temp/kernel_shared` or `$FLOWFILE_SHARED_DIR`); relocating them breaks Docker kernels.
11. `settings.py` parses `sys.argv` at import (`parse_known_args`) — importing core in a process with unrelated argv flags named `--host/--port/--worker-port` will silently repoint the server/worker.
12. Root CLAUDE.md staleness found by this sweep: version says 0.11.0 (actual 0.12.7); migrations "001–021" (actual through 028); `docker-remote/` listed but absent; docker user-data default documented as compose's `/app/user_data` while the code default is `/data/user` when the env var is unset.
13. `sqlite3` CLI note: worker results/`.arrow` and Delta dirs are *files*, not DB rows — deleting a `catalog_tables/<dir>` orphan requires also fixing the `catalog_tables` DB row (`file_path` is absolute).
14. In zsh, `echo ===` style separators break (`== not found`) — irrelevant to the app but bit this investigation; use `---`.

## 12. Historical incidents / design-note archaeology (from code comments & docs)

- **`.flowfile` pickle → YAML migration**: legacy pickles carried dataclass-based `transform_schema` objects; `LegacyUnpickler` + `LEGACY_CLASS_MAP` (tools/migrate/legacy_schemas.py) keep them loadable; save path deprecated with a pointer to "stay on .1 if you still need .flowfile support" (flow_graph.py:5548-5551). Status: load-compat maintained, save removed.
- **`flowfile.db` → `flowfile_catalog.db` rename**: one-time data copy at startup handled by `database/migration.py` scenario 2; `get_legacy_database_path` returns None when `FLOWFILE_DB_PATH` is set. Status: shipped, self-healing.
- **Unknown alembic revision after branch switching**: DB stamped with a revision missing from local scripts crashed `command.upgrade`; fixed by `_ensure_known_revision` re-stamping to local head (migration.py:93-100). Status: shipped.
- **Unnamed flows used to land in temp and got auto-cleaned**: now persisted under `flows/unnamed_flows` "so quick-created flows survive auto-cleanup" (handler.py:16-19); ephemeral scratch flows use `persist=False` so abandoned blank canvases leave no orphan YAML (handler.py:204-206, routes.py:999). Status: shipped (branch `improvement/improve-naming-unnamed-flows` continues naming work).
- **Prompt-log location debate**: writing transcripts to `Path.home()` deemed intrusive; settled on `base_directory/ai_prompts` "same dir that owns master_key.txt / temp/ / system_logs/" (prompt_log.py:9-16). Status: shipped.
- **Spawn-fd race note**: `spawn_flow_subprocess` documents the `os.open`/`Popen`/`os.close` fd-duplication pattern to avoid child-fd races (subprocess_utils.py:22-27). Status: shipped.
