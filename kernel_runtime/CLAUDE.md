# CLAUDE.md - kernel_runtime

FastAPI service that executes arbitrary user Python in an isolated Docker container, exposing a `flowfile_ctx` API for Polars data I/O, artifacts, catalog tables, and rich display. Package-specific notes; see the root `/CLAUDE.md` for monorepo-wide setup, build, lint, ports, and cross-package contracts.

## Role
A sandboxed code-execution kernel. `flowfile_core`'s `KernelManager` (`flowfile_core/flowfile_core/kernel/manager.py`) launches one container per kernel from a published image, serving uvicorn on container port **9999** (`EXPOSE 9999`). In local Docker mode core maps 9999 to a host port in the **19000-19999** range (`_BASE_PORT=19000`, `_PORT_RANGE=1000`); in Docker-in-Docker mode it reaches the kernel by service name on 9999 (no host map). Core POSTs `/execute` with code + input parquet paths; the kernel runs the code, writes output parquet to the shared volume, and calls **back to core** (`FLOWFILE_CORE_URL`, default `http://host.docker.internal:63578`) for global-artifact and catalog HTTP APIs. Core never materialises datasets — the kernel does the Polars/Delta work and ships paths/JSON.

Image flavours (selected by core, not by this package): **base** / **ml** / **lite**, defaulting to `edwardvaneechoud/flowfile-kernel-{base,ml,lite}:0.3.0`, overridable via `FLOWFILE_KERNEL_IMAGE` (legacy base alias) or `FLOWFILE_KERNEL_IMAGE_{BASE,ML,LITE}`. The Dockerfile builds base vs ml via `--build-arg EXTRAS=ml`; lite via `--build-arg SLIM_CONSTRAINTS=true`.

## Layout
- `kernel_runtime/main.py` — FastAPI app, `/execute` + clear/artifact/persistence/recovery/memory/display/health endpoints, per-flow namespace store, SIGUSR1 interrupt handling.
- `kernel_runtime/flowfile_client.py` — the injected `flowfile_ctx` module: `read_input`/`read_inputs`/`read_first`/`publish_output`, in-memory + global artifacts, catalog `TableRef`/`SchemaRef`/`CatalogRef`, logging, `display`/`explore` (Polars frames render as an interactive table / full Graphic Walker explorer via the `application/vnd.flowfile.{table,gwalker}+json` mimes), host→container path translation.
- `kernel_runtime/artifact_store.py` — thread-safe in-memory artifact store keyed by `(flow_id, name)`, with lazy/eager disk recovery.
- `kernel_runtime/artifact_persistence.py` — disk-backed cloudpickle persistence + `RecoveryMode` enum (`lazy`/`eager`/`clear`).
- `kernel_runtime/serialization.py` — `detect_format` + (de)serialise for global artifacts (parquet/joblib/json/pickle, all via cloudpickle for pickle).
- `kernel_runtime/schemas.py` — Pydantic models (`ArtifactInfo`, `GlobalArtifactInfo`).
- `Dockerfile` — multi-stage build (`EXTRAS`, `SLIM_CONSTRAINTS` args); exports `/opt/constraints.txt`.
- `entrypoint.sh` — installs `KERNEL_PACKAGES` (constraint-pinned) then `exec uvicorn kernel_runtime.main:app`.

## Key patterns & conventions
- **Injected globals:** `flowfile_ctx` is the canonical name; `flowfile` is a deprecation-warning alias (`_DeprecatedFlowfileAlias` in `main.py`). New code/docs/tests use `flowfile_ctx`.
- **Per-flow namespace:** `_namespace_store[flow_id]` persists variables across `/execute` cells (LRU, `_MAX_NAMESPACES` from `MAX_NAMESPACES`, default 20). `__name__` is set to `"__main__"` so user-defined classes cloudpickle correctly.
- **Execution context:** request context lives in `contextvars` (`flowfile_client._context`); `flowfile_ctx` APIs only work during `/execute` and raise `RuntimeError` otherwise.
- **Path translation:** core passes host paths; `_translate_host_path_to_container` rewrites them to container mounts (`FLOWFILE_HOST_CATALOG_TABLES_DIR`→`/catalog_tables` checked first, then `FLOWFILE_HOST_SHARED_DIR`→`/shared`). In DinD those env vars are unset and paths pass through unchanged.
- **Core callbacks auth:** `X-Internal-Token` (prefer per-request `internal_token`, fall back to `FLOWFILE_INTERNAL_TOKEN`) + `X-Kernel-Id`.
- **No flowfile_core/worker import:** the kernel re-implements Delta writes itself (`_perform_delta_write` mirrors `shared/delta_utils.py`) so it stays standalone.
- **Version contract:** image / `pyproject.toml` version (`0.3.1`) evolves independently of the app version (root `0.11.0`) and of the runtime API `__version__` (`kernel_runtime/__init__.py`, currently `0.2.2`, surfaced by `/health`). The app pins **one exact kernel tag per flavour** in `flowfile_core/flowfile_core/kernel/manager.py` — there is **no** compatibility range and **no** runtime version check (`kernel_version` is display-only). polars (`>=1.8.2,<1.40`) and pyarrow (`^18`) are aligned with core; core's `flavours.py` reads `kernel_runtime/poetry.lock` as the source of truth for baked versions. Full write-up: `docs/for-developers/kernel-architecture.md` → "Versioning & compatibility".

## Running / entry points
- Local (no Docker): `poetry run uvicorn kernel_runtime.main:app --host 0.0.0.0 --port 9999`
- Build images: `docker build -t flowfile-kernel-base:local kernel_runtime/` (add `--build-arg EXTRAS=ml` or `--build-arg SLIM_CONSTRAINTS=true`); or `docker compose --profile kernel build flowfile-kernel[-ml|-lite]`.
- Normally not run by hand — `flowfile_core`'s `KernelManager` spawns it.

## Testing
- `poetry run pytest tests/ -v` (run from `kernel_runtime/`). No Docker or kernel marker needed — tests drive the app via FastAPI `TestClient`.
- Fixtures in `tests/conftest.py`: `client` (sets `PERSISTENCE_PATH` to a tmp dir before lifespan), autouse `_clear_global_state` resets the module-global `artifact_store`/persistence/display state between tests, plus `store`, `persistence`, `store_with_persistence`.

## Gotchas
- `main.artifact_store` and persistence state are **module-level singletons**; tests must reset them (the autouse fixture does). Don't rely on isolation between requests for them.
- Persistence reads env at lifespan-start, not import (`_setup_persistence`): `PERSISTENCE_ENABLED` (default true), `PERSISTENCE_PATH` (default `/shared/artifacts`), `KERNEL_ID`, `RECOVERY_MODE` (`lazy`/`eager`/`clear`; `none` is a deprecated alias for `clear` and is **destructive**), `PERSISTENCE_CLEANUP_HOURS` (default 24).
- The README says "Two images" — there are actually **three** flavours (base/ml/lite); trust core's `manager.py`/`flavours.py` and the Dockerfile build args.
- `publish_global` needs a `source_registration_id`; core normally injects a scratch one, but if it's still `None` (older core / interactive cell mode) it no-ops, prints a warning, and returns `-1`.
- No `VOLUME` directive in the Dockerfile by design — `/shared` is bind-mounted at runtime so an anonymous volume can't shadow the named mount in DinD.
- Deserialization uses pickle/cloudpickle (RCE vector); acceptable only because the trust boundary is the user's own code already running in the kernel.

## Key files
- `kernel_runtime/main.py` — FastAPI app + `/execute` orchestration, namespace/LRU, SIGUSR1 interrupt.
- `kernel_runtime/flowfile_client.py` — the `flowfile_ctx` user-facing API surface.
- `kernel_runtime/artifact_store.py` — `(flow_id, name)`-keyed thread-safe store with lazy recovery.
- `kernel_runtime/artifact_persistence.py` — cloudpickle disk persistence + `RecoveryMode`.
- `kernel_runtime/serialization.py` — global-artifact format detection / (de)serialise.
- `Dockerfile` — multi-stage build, `EXTRAS`/`SLIM_CONSTRAINTS` args, exports `/opt/constraints.txt`.
- `entrypoint.sh` — `KERNEL_PACKAGES` install then launch uvicorn on 9999.
- `pyproject.toml` — pinned deps aligned with core, `ml` extras group, image version `0.3.0`.
- `tests/conftest.py` — `TestClient` fixtures + global-state reset.
