# CLAUDE.md - shared

Cross-service utility package: dependency-light helpers, wire models, and storage-path resolution importable by every other Flowfile Python package. Package-specific notes only; see the root `/CLAUDE.md` for monorepo-wide setup, build, lint, ports, and cross-package contracts.

## Role
`shared` is the bottom of the import graph. It is imported by `flowfile_core`, `flowfile_worker`, `flowfile_scheduler`, and the CLI (`flowfile`) — but it must NEVER import from any of them (verified: no `from flowfile_core/_worker/_scheduler/_frame` imports exist outside tests). `kernel_runtime` does NOT `import shared`; it only reads/writes the same on-disk shared volume at runtime. `shared` carries only stdlib + light third-party deps (sqlalchemy, pydantic, polars, httpx, optional boto3) so lightweight consumers (scheduler, CLI run-completion) can touch the DB and storage paths without pulling in FastAPI/core's heavy stack.

Two recurring contracts it enforces:
- **Wire-model split**: `rest_api.models`, `google_analytics.models`, `kafka.models` hold pure-data Pydantic models with NO decryption logic. `*_encrypted` fields carry Fernet tokens decrypted only worker-side. The worker *subclasses* `RestApiReadSettings` and `GoogleAnalyticsReadSettings` to attach decryption; `KafkaReadSettings` instead exposes `to_consumer_config(decrypt_fn=...)` and the caller injects a decrypt callable. Core constructs requests without importing worker code.
- **Independent SQLAlchemy models** (`models.py`): a minimal mirror of `flowfile_core.database.models` (only columns non-core consumers need), declared on its own `Base` so the scheduler/CLI talk to the catalog DB without importing core.

It is a Poetry package — `{ include = "shared" }` in root `pyproject.toml`, with no `from` dir, so the import path is `shared.*`.

## Layout
- `storage_config.py` — `FlowfileStorage` + the `storage` singleton; single source of truth for every on-disk path. `get_database_url()` resolves the SQLite catalog URL.
- `models.py` — standalone SQLAlchemy models (`FlowRun`, `FlowSchedule`, `FlowRegistration`, `CatalogTable`, `ScheduleTriggerTable`, `SchedulerLock`) on their own `Base`.
- `artifact_storage.py` — `ArtifactStorageBackend` ABC (`prepare_upload`/`prepare_download`/`delete`/`exists`) + `SharedFilesystemStorage` / `S3Storage` (presigned-URL) backends, returning `UploadTarget` / `DownloadSource`. Kernel moves blob bytes via presigned URLs; Core stays metadata-only.
- `delta_utils.py` / `delta_models.py` — dependency-light Delta-log helpers (`make_json_safe`, `format_delta_timestamp`, `get_delta_size_bytes`, `validate_catalog_path`, plus `write_delta` / `merge_into_delta`) + Pydantic `DeltaVersionCommit` / `SourceTableVersion`.
- `sql_utils.py` — `construct_sql_uri`, `get_sqlalchemy_uri`, `SQLALCHEMY_DRIVER_MAP` (caller passes an already-decrypted password).
- `cloud_storage/` — GCS/S3/ADLS helpers: `storage_options.py` (`build_*_storage_options`), `writers.py` (`write_to_cloud` + per-format parquet/csv/json/delta writers), `directory.py` (first-file listing per backend), `gcs.py`, `utils.py`.
- `kafka/` — `consumer.py` (`read_kafka_source`, `infer_topic_schema`, `commit_offsets`, `make_kafka_commit_callback`), `models.py`, `deserializers.py` (`get_deserializer`, JSON deserializer).
- `ml/` — `algorithms.py` (`get_algorithm_specs`, the hyperparam-spec registry served to the UI picker), `trainers.py` (`TRAINER_REGISTRY`, `get_trainer`), `metrics.py` (`compute_metrics`).
- `rest_api/` — `models.py` (wire), `fetch.py` (`fetch_rest_api`: pure httpx engine, JSON-only, flattened via `pl.json_normalize`).
- `google_analytics/models.py` — GA4 wire models.
- `subprocess_utils.py` — `spawn_flow_subprocess(flow_path, run_id)` (fire-and-forget `flowfile run flow`).
- `parent_watcher.py` — `start_parent_death_watcher` for Tauri sidecar reparent detection.
- `run_completion.py` — `complete_run` / `get_run_user_id` (CLI subprocess updates `FlowRun` via raw SQLAlchemy).
- `viz_protocol.py` — `HTTP_TIMEOUT_SECONDS` / `REQUEST_TIMEOUT_SECONDS` for catalog viz core↔worker calls.

## Key patterns & conventions
- **IMPORT-ONLY-DOWNWARD.** Never add an import from `flowfile_core`/`_worker`/`_scheduler`/`_frame` here. If a helper needs core types, it's in the wrong package. Keep modules dependency-light; gate optional heavy deps behind local imports (e.g. `boto3` is imported inside `S3Storage.__init__`).
- **`storage` is a singleton instantiated at import** (`storage = FlowfileStorage()` at module bottom), and its `__init__` eagerly `mkdir`s the internal + user directory lists. Importing `shared.storage_config` (or `shared`) has filesystem side effects.
- **Two roots**: `base_directory` (internal core↔worker volume; local `~/.flowfile`, Docker `/app/internal_storage`) vs `user_data_directory` (local `~`, Docker `/data/user`). Behavior branches on `FLOWFILE_MODE == "docker"`.
- **Kernel-exchange dirs MUST stay under the shared volume.** `shared_directory` defaults to `temp/kernel_shared`; `global_artifacts_directory` and `artifact_staging_directory` default under it (`temp/kernel_shared/...`, matching `KernelManager`'s mount) and all honor `FLOWFILE_SHARED_DIR`. Do not relocate these to a path Docker containers can't see.
- **Wire models stay logic-free.** Don't add secret-decryption to `rest_api`/`google_analytics`/`kafka` models — that belongs in the worker subclasses (rest_api/GA) or the injected `decrypt_fn` (kafka).
- `models.py` maps only columns non-core consumers need; if you add a column here, the canonical schema still lives in `flowfile_core.database.models`.

## Running / entry points
Not a service — no router, no CLI of its own. It's a library: consumers do `from shared.storage_config import storage`, `from shared.kafka.consumer import read_kafka_source`, etc. The package `__init__.py` re-exports the common surface (`storage`, cloud/delta/sql helpers).

## Testing
Tests live in `shared/tests/` and run from the repo root:
```bash
poetry run pytest shared/tests
```
- `tests/test_artifact_storage.py`, `tests/test_ml_metrics.py` — plain unit tests.
- `tests/kafka/` — needs a Redpanda container; `tests/kafka/conftest.py` auto-starts it via `test_utils.kafka.fixtures` and `pytest.skip`s when Docker is unavailable. No custom pytest marker is applied to shared tests (the `worker`/`core`/`kernel` markers in root `pyproject.toml` are not used here).

## Gotchas
- `crypto/` exists on disk but contains only stale `__pycache__/*.pyc` — there is NO tracked crypto source module. Don't import `shared.crypto`; envelope/master-key crypto lives elsewhere.
- `rest_api/__init__.py` and `google_analytics/__init__.py` are empty — import submodules directly (`from shared.rest_api.models import ...`), not the package.
- `fetch_rest_api` is driven by the worker's REST source; core's `/rest_api/sample` route and `flowfile_frame.read_api` reach it *through the worker*, not by calling it in-process.
- `get_database_url()` resolution order: `FLOWFILE_DB_PATH` env → `TESTING=True` (temp test DB) → default `database_directory/flowfile_catalog.db`. `get_legacy_database_path()` is one-time-migration only.
- `local_model_directory` and `ai_sessions_directory` are deliberately NOT created in `_ensure_directories` — don't add them to the eager mkdir list (local model is opt-in install).

## Key files
- `storage_config.py` — `storage` singleton, all path properties, `get_database_url()`.
- `models.py` — standalone SQLAlchemy models for non-core DB access.
- `artifact_storage.py` — blob-storage backend ABC + filesystem/S3 impls.
- `run_completion.py` — CLI-subprocess `FlowRun` completion without core.
- `subprocess_utils.py` — `spawn_flow_subprocess`.
- `parent_watcher.py` — sidecar parent-death watcher.
- `kafka/consumer.py` — Kafka read/commit engine.
- `ml/algorithms.py` — ML algorithm/hyperparam registry (core+worker source of truth, served to the UI picker).
- `rest_api/fetch.py` — `fetch_rest_api` httpx REST engine.
- `cloud_storage/writers.py` — unified cloud write helpers.
- `delta_utils.py` — dependency-light Delta/catalog helpers.
