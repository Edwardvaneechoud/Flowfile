# CLAUDE.md - flowfile_worker

Standalone FastAPI compute service that offloads heavy Polars/data work from `flowfile_core`, running each job in a spawned subprocess. Package-specific notes; see the root `/CLAUDE.md` for monorepo-wide setup, build, lint, ports, and cross-package contracts.

## Role
- Separate FastAPI service on **port 63579** (`configs.DEFAULT_SERVICE_PORT`). Core (port 63578) is its only client; core's `subprocess_operations.py` POSTs serialized Polars LazyFrames to `WORKER_URL/submit_query/` and polls `/status/{id}` + `/fetch_results/{id}`, or streams over `/ws/submit`.
- The worker calls **back to core** at `FLOWFILE_CORE_URI` (`configs.get_core_url(CORE_HOST, CORE_PORT)`, default `http://<CORE_HOST>:63578`) only for log shipping: `flow_logger.FlowfileLogHandler` POSTs to `/raw_logs`.
- Core ships paths/JSON and never materialises LazyFrames; the worker holds dataset memory. Results land as Arrow IPC files under `CACHE_DIR/<flow_id>/<task_id>.arrow`; `fetch_results` returns `pl.scan_parquet(file_ref).serialize()`, not the data itself.
- Runtime contract: every heavy job runs in a `spawn`-context child process. The FastAPI process only spawns, monitors shared `Value`/`Array`/`Queue`, ships paths, and kills children. It must stay lean — no large dataset in the parent.

## Layout
- `flowfile_worker/main.py` — FastAPI app (`shutdown_handler` lifespan) + `run()` entrypoint; lifespan calls `viz_session_registry.shutdown()`, terminates `mp_context.active_children()`, runs `storage.cleanup_directories()`; `/shutdown` endpoint + SIGTERM/SIGINT handlers + `start_parent_death_watcher`.
- `flowfile_worker/__init__.py` — process-global state: `multiprocessing.set_start_method("spawn", force=True)`, `mp_context = get_context("spawn")`, `status_dict`, `process_dict`, locks, `CACHE_DIR`, `PROCESS_MEMORY_USAGE`.
- `flowfile_worker/routes.py` — REST endpoints (`/submit_query/`, store_sample/write/create_table, catalog materialize/sql_query/delta/visualize, train/apply ML, add_fuzzy_join, status/fetch_results/cancel_task/clear_task).
- `flowfile_worker/streaming.py` — `streaming_router` WebSocket `/ws/submit` (binary in/out, progress frames, disconnect hand-off to `handle_task`).
- `flowfile_worker/spawner.py` — `start_*` helpers (`start_process`, `start_generic_process`, `start_fuzzy_process`, `start_train_model_process`, `start_apply_model_process`) that build shared mem + spawn `mp_context.Process`, plus `handle_task` monitor loop and the `process_manager` singleton.
- `flowfile_worker/process_manager.py` — `ProcessManager`: lock-guarded `task_id -> Process` map with `cancel_process` (terminate + join).
- `flowfile_worker/funcs.py` — the actual subprocess targets (store, store_sample, fuzzy_join_task, write_*, merge_delta, train/apply model, resolve_virtual_table, execute_sql_query, catalog metadata, `generic_task`). Imports polars at module top.
- `flowfile_worker/models.py` — Pydantic request/response + `Status`, `OperationType` literal, `Base64Bytes` JSON-safe bytes type.
- `flowfile_worker/configs.py` — host/port arg parsing, `FLOWFILE_CORE_URI`, `TEST_MODE`, logger.
- `flowfile_worker/secrets.py` — independent Fernet/HKDF secret derivation (mirrors core).
- `flowfile_worker/catalog_reader.py` — the only two catalog-open primitives (`open_catalog_table`, `open_virtual_result`); polars-at-top, children-only.
- `flowfile_worker/viz_sessions.py` / `viz_session_worker.py` — `VizSessionRegistry` + spawned-child entrypoint for long-lived Graphic Walker (`polars_gw`) viz LazyFrames.
- `flowfile_worker/external_sources/` — `sql_source/`, `s3_source/`, `kafka_source/`, `rest_api_source/`, `google_analytics_source/` connectors run inside subprocesses.
- `flowfile_worker/create/` — `table_creator_factory_method(FileType)` + `create_from_path_*` builders for `csv`/`parquet`/`json`/`excel`.

## Key patterns & conventions
- **All compute is `spawn`-context.** Spawn only via `mp_context.Process(...)`. Children re-import the package fresh, so `configs.py` skips argparse for non-`MainProcess` workers and falls back to env-var defaults — don't rely on parent-set host/port in children.
- **Subprocess signalling protocol** (every `funcs.*` target): shared `Value("i")` progress (`0`→`100` on success, `-1` on error), `Array("c", 1024)` error message, and a `Queue(maxsize=1)` for the result. `handle_task` (sync) and `_monitor_progress` (WS) translate these into the `Status` object.
- **Result transport:** bytes results are b64-encoded only at the REST boundary (`status.results` in `handle_task`); the WS path (`_send_completion`) sends raw binary via `send_bytes`. IPC files are the canonical hand-off; `Status.file_ref` points at them.
- **Catalog reads** go exclusively through `catalog_reader.open_catalog_table` (`scan_delta`) / `open_virtual_result` (`scan_ipc`); both validate paths under `storage.catalog_tables_directory` / `catalog_virtual_results_directory` via `shared.delta_utils.validate_catalog_path`. Don't `scan_delta`/`scan_ipc` catalog paths inline.
- **Module-top polars imports** (`funcs.py`, `catalog_reader.py`, `viz_session_worker.py`) are fine because they're invoked in children; do not import them eagerly into the FastAPI request path.
- `secrets.py` re-derives user keys independently of core using the same `$ffsec$1${user_id}${token}` format (`SECRET_FORMAT_PREFIX`) and `KEY_DERIVATION_VERSION = b"flowfile-secrets-v1"` HKDF salt — keep these byte-for-byte in sync with core's secret module or decryption breaks.
- `TEST_MODE` (env `TEST_MODE` set) returns a fixed master key; tests set it in `conftest.py`.

## Running / entry points
- Poetry script: `poetry run flowfile_worker` → `flowfile_worker.main:run`.
- Module: `python -m flowfile_worker.main` (Docker `CMD`).
- Flags: `--host`, `--port` (default 63579), `--core-host`, `--core-port` (default 63578); also `CORE_HOST`/`CORE_PORT` env (Docker).
- Docker: `flowfile_worker/Dockerfile` (python:3.12-slim, `FLOWFILE_MODE=docker`, EXPOSE 63579, healthcheck on `/docs`).

## Testing
- `poetry run pytest flowfile_worker/tests` (coverage source `flowfile_worker/flowfile_worker`).
- `tests/conftest.py` forces `TEST_MODE=1` and provides a session-scoped Postgres fixture via `test_utils.postgres` (testcontainers; skipped if port 5433 busy or Docker absent).
- Markers (defined at root in `/pyproject.toml`): `worker`, `core`, `kernel`, `docker_integration`. Worker tests use `@pytest.mark.worker`; `@pytest.mark.slow` is also applied to viz tests (`test_catalog_visualize.py`) but is not declared in the markers list.
- Tests live in `tests/` with `tests/external_sources/` for connector tests (SQL/cloud/GA/REST).

## Gotchas
- Adding a new `OperationType`: it must be both a literal in `models.OperationType` AND a function name on `funcs` — both `spawner.start_process` and `streaming._spawn_subprocess` do `getattr(funcs, operation)`.
- `status_dict`, `PROCESS_MEMORY_USAGE`, and the `ProcessManager` map are plain in-memory dicts in the parent — task state does not survive a worker restart; `clear_task` is the only thing that removes the IPC file + state (and the Kafka `.offsets.json` sidecar).
- On WebSocket client disconnect mid-task the subprocess is **handed off** to a daemon `handle_task` thread (`_handoff_to_background`), not killed; `p`/`progress`/`error_message` are nulled so the `finally` block doesn't reap them.
- Connectors do blocking network I/O; they MUST run in the subprocess (via `start_generic_process`), never inline in the async endpoint, or they block the event loop.
- Viz sessions are long-lived spawned children with idle-TTL reaping (`VizSessionRegistry.IDLE_TTL_SECONDS = 300`, background reaper thread); shut them down via the lifespan, evict via `/catalog/visualize_evict`.

## Key files
- `flowfile_worker/__init__.py` — spawn context + shared process-global state.
- `flowfile_worker/main.py` — FastAPI app, `run()`, lifespan child cleanup, `/shutdown`.
- `flowfile_worker/routes.py` — REST endpoint surface.
- `flowfile_worker/streaming.py` — `/ws/submit` WebSocket protocol + disconnect hand-off.
- `flowfile_worker/spawner.py` — process spawn + `handle_task` monitor + `process_manager`.
- `flowfile_worker/funcs.py` — subprocess compute targets + progress/error/queue convention.
- `flowfile_worker/process_manager.py` — cancellable `task_id -> Process` registry.
- `flowfile_worker/models.py` — `Status`, `OperationType`, request/response models.
- `flowfile_worker/configs.py` — ports, `FLOWFILE_CORE_URI`, `TEST_MODE`.
- `flowfile_worker/secrets.py` — independent Fernet/HKDF `$ffsec$` secret derivation.
- `flowfile_worker/catalog_reader.py` — the only sanctioned catalog-open primitives.
- `flowfile_worker/flow_logger.py` — ships subprocess logs back to core `/raw_logs`.
