# Flowfile Test Infrastructure — Discovery Dossier (KEY=test-infra)

All paths relative to repo root `/Users/edwardvaneechoud/flowfile_backup/Flowfile` unless absolute.
Everything below was verified by reading files or running read-only commands on 2026-07-03 (branch `improvement/improve-naming-unnamed-flows`). Items I could not fully verify are marked **inferred**.

---

## 1. Pytest configuration (single source of truth)

`pyproject.toml:129-139` — the ONLY pytest config for the monorepo (exception: `flowfile_wasm/pytest.ini`, see §9):

```toml
[tool.pytest.ini_options]
asyncio_mode = "strict"
asyncio_default_fixture_loop_scope = "function"
markers = [
    "worker: Tests for the flowfile_worker package",
    "core: Tests for the flowfile_core package",
    "kernel: Integration tests requiring Docker kernel containers",
    "docker_integration: Full Docker-based E2E tests (require Docker, slow)",
    "kafka: Integration tests requiring a Kafka/Redpanda broker (Docker)",
    "lsp: Tests for the notebook LSP (Jedi) code-intelligence surface",
]
```

- **No `addopts`, no `testpaths`, no `norecursedirs`** (verified by grep). Consequence: a bare `pytest` from repo root collects EVERYTHING, including `tests/integration` (docker_integration, slow compose builds) and `tests/kafka`. The claim in `tests/integration/README.md` that "pytest excludes these tests by default" is wrong at the config level — they are only "excluded" because everyone targets a directory. **Always pass an explicit directory** (`poetry run pytest flowfile_core/tests`, etc.).
- Undeclared markers in use: `@pytest.mark.slow` in `flowfile_worker/tests/test_catalog_visualize.py:164,216` (not in the markers list — pytest warns, doesn't fail); `tools/migrate/tests/conftest.py:14-21` registers its own `slow` and `requires_yaml` markers via `pytest_configure`.
- `lsp` marker used in `flowfile_core/tests/flowfile/test_lsp_kernel_integration.py:16` (`pytestmark = [pytest.mark.kernel, pytest.mark.lsp]`); `flowfile_core/tests/lsp/test_lsp_routes.py` is the hermetic (no-Docker) LSP suite.

## 2. Coverage configuration

`pyproject.toml:141-160`:
- `[tool.coverage.run] source = ["flowfile_core/flowfile_core", "flowfile_worker/flowfile_worker"]` — **frame/scheduler/shared are excluded from coverage on purpose**.
- omit: `*/tests/*`, `*/test_*`, `*/__pycache__/*`, `*/conftest.py`. `fail_under = 0` (coverage never gates). XML output: `coverage.xml`.
- **No `branch=true`, no `dynamic_context`** — deliberate, because CI uses `COVERAGE_CORE=sysmon` (PEP-669 sys.monitoring tracer) which doesn't support those (`.github/workflows/test.yaml:260-267`, comment at 216-219).

Local coverage: `make test_coverage` (Makefile:243-250) =
```bash
poetry run pytest flowfile_core/tests --cov --cov-report= --disable-warnings
poetry run pytest flowfile_worker/tests --cov --cov-append --cov-report= --disable-warnings
poetry run coverage report --show-missing
```
Core and worker run **sequentially with `--cov-append`** — the Makefile comment says "to avoid import collisions" (both packages get imported into one process otherwise).

Historical note (from user memory `project_ci_test_speed.md`): the 3.12 CI job was ~56 min because coverage's C-tracer ~doubled runtime and all core tests run serially. Fix shipped 2026-06-20: `COVERAGE_CORE=sysmon`, coverage split into its own `coverage` CI job, `concurrency: cancel-in-progress` for PRs. pytest-xdist was evaluated and **deferred** (would need per-xdist-worker `FLOWFILE_DB_PATH` isolation set BEFORE the first flowfile_core import, plus `parallel=true` + `coverage combine`; without those it produces the "no such table" cascade / empty coverage.xml). Don't re-litigate: worker poll interval, catalog `time.sleep(1.05)` (SQLite 1s `updated_at` granularity) and 2s cancel sleeps are load-bearing.

## 3. All conftest.py files (14)

| Path | Purpose |
|---|---|
| `flowfile_core/tests/conftest.py` | The big one — see §4 |
| `flowfile_core/tests/flowfile/conftest.py` | `sqlite_db` sample DB fixture; **catalog helpers**: `catalog_cleanup()` (wipes CatalogTableReadLink/FlowSchedule/CatalogTable/FlowRegistration/CatalogNamespace rows AND worker `catalog_virtual_results` `.arrow` cache files, lines 76-97), `create_test_namespace`, `create_test_graph`, `run_test_graph`, `catalog_clean_state` fixture |
| `flowfile_core/tests/sharing/conftest.py` | In-process docker-mode: `monkeypatch.setenv("FLOWFILE_MODE","docker")` + JWT_SECRET_KEY + fresh Fernet FLOWFILE_MASTER_KEY + `FLOWFILE_USER_DATA_DIR=tmp_path` (lines 29-41). Warning in docstring: "Module-level TestClients elsewhere in the suite mint electron tokens at import time — never flip the mode process-wide." Fixtures: `users` (admin/alice/bob/carol, full teardown of grants+groups+resources), `group_factory`, `grant_factory`, `resource_factory`, `client_for` |
| `flowfile_core/tests/project/conftest.py` | Autouse `_seed_default_catalog_namespaces` — re-seeds local_user + the 'General' catalog namespace before every project test because other modules wipe all CatalogNamespace rows and the init_db seed is session-once |
| `flowfile_core/tests/ai/conftest.py` | Autouse `_ai_feature_enabled` — saves/forces/restores `FEATURE_FLAG_AI.set(True)` per test |
| `flowfile_core/tests/templates/conftest.py` | Parametrized fixture over `data/templates/flows/*.yaml` |
| `flowfile_worker/tests/conftest.py` | Sets `os.environ['TEST_MODE']='1'` at import (line 8); session-autouse `postgres_db` (reuses port 5433 if busy, else test_utils.postgres, `pytest.fail` if container can't start when Docker present) |
| `flowfile_frame/tests/conftest.py` | Sets `os.environ['TESTING']='True'` (line 3); at module import creates a `minio-flowframe-test` CloudStorageConnection pointing at `http://localhost:9000` minioadmin/minioadmin (lines 15-32) — registration only, does not require MinIO to be up at import |
| `tests/integration/conftest.py` | docker_integration E2E: module-scoped `compose_services` (pre-flight docker/compose/ports 63578+63579 free → build core+worker+kernel images → temp `.env` with one-time JWT_SECRET_KEY/FLOWFILE_INTERNAL_TOKEN/admin creds/`FLOWFILE_KERNEL_IMAGE=flowfile-kernel-base:local` → `docker compose up -d` → health-wait → teardown `compose down -v --remove-orphans`), `auth_client`, `kernel_ready` |
| `tests/kafka/conftest.py` | session-autouse `redpanda_broker` (reuse running container else start, skip if Docker absent, stop only if we started it); `kafka_topic` (UUID-suffixed unique topic per test, 2 partitions); `produce_messages` |
| `shared/tests/kafka/conftest.py` | Mirror of tests/kafka conftest (1 partition) |
| `kernel_runtime/tests/conftest.py` | No Docker: FastAPI `TestClient`; autouse `_clear_global_state` resets module-global `artifact_store`/persistence/`_namespace` singletons before AND after each test; `client` fixture points `PERSISTENCE_PATH` at tmp |
| `flowfile_wasm/tests/python/conftest.py` | `sys.path.insert` to `src/pyodide`; imports the real shipped `engine` package; autouse `engine.clear_all()` before/after each test |
| `tools/migrate/tests/conftest.py` | sys.path bootstrap + registers `slow`, `requires_yaml` markers |

There is **no repo-root conftest.py**.

## 4. flowfile_core/tests/conftest.py in depth (the load-bearing one)

File: `flowfile_core/tests/conftest.py` (379 lines).

- Line 11-18: **monkey-patches `bcrypt.hashpw`** to truncate >72-byte passwords (passlib 1.7.4 / bcrypt 5.x compat).
- Line 23: `os.environ['TESTING'] = 'True'` — set at conftest import, BEFORE flowfile_core imports, so `shared/storage_config.get_database_url()` resolves the test DB.
- Line 25: `sys.path.insert` two levels up so `test_utils` and `tests.*` import.
- **`setup_test_db`** (session, autouse, lines 61-82): `init_db()` on setup; on teardown, if `TESTING=True` and sqlite URL → `Base.metadata.drop_all(engine)` then **deletes the DB file**. This teardown is the source of the cross-session clobbering incident (§12).
- **`flowfile_worker`** (session, autouse, lines 218-230): if `SKIP_WORKER_TESTS=1` → no-op; else `managed_worker()` — reuses an already-running worker (probes `http://0.0.0.0:63579/docs`, env `FLOWFILE_WORKER_HOST`/`FLOWFILE_WORKER_PORT`), otherwise spawns `poetry run flowfile_worker` as a subprocess (own process group; SIGTERM→SIGKILL ladder, `FLOWFILE_STARTUP_TIMEOUT` default 30s, `FLOWFILE_SHUTDOWN_TIMEOUT` default 15s). If it can't start: **`pytest.skip` for the whole session**. The spawned worker inherits the pytest env (`TESTING`, `FLOWFILE_DB_PATH` propagate).
- **`postgres_db`** (session, autouse, lines 233-252): reuse if port 5433 in use / can connect; skip if Docker unavailable; else `test_utils.postgres.managed_postgres()`; `pytest.fail` if start fails.
- **`mysql_db`** (session, autouse, lines 255-276): same pattern on port 3307 but **soft-fails** ("MySQL tests will be skipped") instead of pytest.fail.
- **`kernel_manager`** / **`kernel_manager_with_core`** (session, NOT autouse, lines 279-343): skip without Docker locally; **fail loudly in CI** (`CI==true` or `TEST_MODE==1`). Delegate to `tests/kernel_fixtures.py::managed_kernel` (see below).
- **`execution_location`** (params `["local","remote"]`, lines 346-356): parametrizes tests across local/worker execution; `remote` auto-skips when no worker is running. Used by test_catalog.py, test_flow_api*.py, test_run_flow_node.py, etc.
- **`cleanup_global_artifacts`** (function, opt-in): deletes all `GlobalArtifact` rows before/after.

`flowfile_core/tests/kernel_fixtures.py` (`managed_kernel`, lines 147-321): creates temp `FLOWFILE_SHARED_DIR`, **rebuilds the `shared.storage_config.storage` singleton in-place** (`storage_module.storage = FlowfileStorage()`) + `flowfile_core.artifacts.reset_storage_backend()`; optionally starts a real uvicorn Core server on **port 63578 in a background thread** (`_start_core_server`) with a fresh `FLOWFILE_INTERNAL_TOKEN` and `FLOWFILE_CORE_URL=http://host.docker.internal:63578`; builds the kernel image with tag `flowfile-kernel` **unless `FLOWFILE_KERNEL_IMAGE` is already set** (CI presets it to skip the ~30s build); creates/starts kernel id `integration-test` (or `integration-test-core`), force-removes stale containers, and restores every env var + rebuilds the storage singleton again on teardown. `flowfile_core/tests/README.md` documents the two-fixture design and the cleanup command: `docker rm -f flowfile-kernel-integration-test flowfile-kernel-integration-test-core`.

`flowfile_core/tests/flowfile_core_test_utils.py`: `is_docker_available()` (returns **False on Windows always**, line 13-14), `ensure_password_is_available()` (seeds secret `test_database_pw` = `testpass` for user 1), `ensure_db_connection_is_available()` (seeds `test_connection_endpoint` → postgres localhost:5433 testuser/testpass/testdb).

## 5. test_utils/ — the Docker service fixtures

Package layout: `test_utils/{postgres,mysql,s3,gcs,azurite,kafka}/` each with `fixtures.py` + `commands.py` (CLI entry). Poetry scripts (`pyproject.toml:82-101`):

| Command | Effect |
|---|---|
| `poetry run start_postgres` / `stop_postgres` | Postgres w/ sample data |
| `poetry run start_mysql` / `stop_mysql` | MySQL 8 w/ inline sample data |
| `poetry run start_minio` / `stop_minio` | MinIO (S3) |
| `poetry run start_azurite` / `stop_azurite` | Azurite (Azure Blob) |
| `poetry run start_gcs` / `stop_gcs` | fake-gcs-server |
| `poetry run start_redpanda` / `stop_redpanda` | Redpanda (Kafka) |

All `start_*` CLI commands **return exit code 0 when Docker is missing** ("Return success to allow pipeline to continue") — the tests then skip.

**Shared skip logic** — every `is_docker_available()` in test_utils (e.g. `test_utils/postgres/fixtures.py:46-78`):
1. On macOS or Windows **when `CI` env is truthy** → returns False (Docker skipped on non-Linux CI runners).
2. `shutil.which("docker")` must exist; `docker info` must exit 0 (5s timeout).

Per-service specifics (defaults; all overridable via `TEST_*` env vars in each fixtures.py):

| Service | Container name | Image | Host port(s) | Data seeded | Notes |
|---|---|---|---|---|---|
| Postgres | `test-postgres-sample` | locally-built `test-sample-db` | **5433**→5432 | clones https://github.com/zseta/postgres-docker-samples into `test_utils/postgres/postgres-docker-samples/`, builds image with `movies` schema (or `stocks`); creds testuser/testpass/testdb | `--rm -d`; readiness = psycopg2 connect loop (30s) |
| MySQL | `test-mysql-sample` | `mysql:8` | **3307**→3306 | inline SQL in `fixtures.py:43-80`: `movies` (exotic types: ENUM/SET/JSON/YEAR) + `actors`; testuser/testpass/testdb, root `rootpass` | pulls image first (300s timeout); 60s startup; `_init_sample_data()` after ready |
| MinIO (S3) | `test-minio-s3` | `minio/minio` | **9000** (API), **9001** (console) | buckets `test-bucket`, `flowfile-test`, `sample-data`, `worker-test-bucket`, `demo-bucket`; `data_generator.populate_test_data` writes single/multi CSV/JSON/parquet, a Delta table (`delta-lake-table`), and an Iceberg warehouse; `demo_data_generator` fills demo-bucket | minioadmin/minioadmin; named volume `test-minio-s3-data` removed on stop; `KEEP_MINIO_RUNNING=true` keeps it up after `managed_minio` |
| GCS | `test-fake-gcs` | `fsouza/fake-gcs-server` | **4443** | buckets `test-bucket`, `flowfile-test`, `sample-data`, `worker-test-bucket` + parquet/csv test data (`data_generator.populate_test_data`) | anonymous creds, `-scheme http`; `is_gcs_available()` also checks the `single-file-parquet/data.parquet` blob exists and re-populates a running-but-empty container; `KEEP_GCS_RUNNING` |
| Azurite | `test-azurite` | `mcr.microsoft.com/azure-storage/azurite` | **10000** (blob) | containers `test-container`, `flowfile-test`, `sample-data`, `worker-test-container` + data | well-known devstoreaccount1 account/key hardcoded (`test_utils/azurite/fixtures.py:15-18`); `--skipApiVersionCheck`; `KEEP_AZURITE_RUNNING` |
| Redpanda | `test-redpanda-kafka` | `docker.redpanda.com/redpandadata/redpanda:v25.3.11` | **19092**→9092 | none (topics per-test, UUID-suffixed) | `--smp 1 --memory 256M --mode dev-container`; readiness = AdminClient list_topics; dumps container logs on failure; `KEEP_REDPANDA_RUNNING` |

Skip-if-unavailable pattern in test modules: `@pytest.mark.skipif(not is_docker_available(), ...)` (42 uses in core tests, 10 in worker tests), plus service-reachability guards `_minio_available()` (test_catalog_cloud_virtual.py:71, test_catalog_namespace_storage.py:39), `_mysql_reachable()` (worker), `is_gcs_available()` / `is_azurite_available()` (worker `tests/external_sources/test_cloud_source_{gcs,adls}.py`, core `tests/flowfile/flowfile_table/test_flow_data_engine_{gcs,adls}.py`).

## 6. State-altering env vars (verified in code)

| Var | Where read | Effect |
|---|---|---|
| `TESTING` | `shared/storage_config.py:414,430` (must be exactly the string `"True"`) | `get_database_url()` → `sqlite:///<base>/temp/test_flowfile_catalog.db` instead of `<base>/database/flowfile_catalog.db`. `<base>` = `~/.flowfile` locally (`storage_config.py:40-52`) or `$FLOWFILE_STORAGE_DIR`. Set by core conftest (line 23) and frame conftest (line 3). Also gates conftest teardown DB deletion (`flowfile_core/tests/conftest.py:72`). |
| `FLOWFILE_DB_PATH` | `shared/storage_config.py:410-412` | Highest-priority DB override: `sqlite:///$FLOWFILE_DB_PATH`. THE isolation lever for concurrent pytest sessions (propagates to the spawned worker subprocess). Used by `flowfile_core/tests/test_migration.py:53` and `tests/project/test_legacy_uuid_backfill.py:39`. |
| `FLOWFILE_SKIP_STARTUP_MIGRATION` | `flowfile_core/flowfile_core/database/init_db.py:26-27` | Any truthy value skips `run_startup_migration()` which otherwise runs **at import of flowfile_core** (module scope). Use for diagnostics; **never set it when running flowfile_core/tests** — `setup_test_db` relies on the migration to create the schema, else every test errors `no such table: users`. |
| `TEST_MODE` | `flowfile_worker/flowfile_worker/configs.py:18` (presence check: `"TEST_MODE" in os.environ`) | Worker returns a **fixed master key** `06t640eu3AG2FmglZS0n0zrEdqadoT7lYDwgSmKyxE4=` (`flowfile_worker/secrets.py:128-129`) instead of keychain/Docker lookup. Set by worker conftest (`tests/conftest.py:8`) and kernel CI (`test-kernel-integration.yml`). Also doubles as an "in CI" signal for kernel fixtures (`flowfile_core/tests/conftest.py:292,329`). Core has NO equivalent — core tests use the real SecureStorage key (`flowfile_core/auth/secrets.py:197-223`, file-based Fernet store under `~/.config/flowfile` in electron mode). |
| `SKIP_WORKER_TESTS` | `flowfile_core/tests/conftest.py:226` (`== "1"`) | Skips starting/spawning the worker; `execution_location` fixture then auto-skips `remote` params (`conftest.py:354-355`). |
| `CI` | test_utils `is_docker_available()` (all six) + kernel fixtures | truthy on macOS/Windows → Docker treated as unavailable; in kernel fixtures → skip becomes `pytest.fail`. |
| `FLOWFILE_WORKER_HOST` / `FLOWFILE_WORKER_PORT` | `flowfile_core/tests/conftest.py:53-54` | Where conftest probes for an existing worker (defaults 0.0.0.0:63579). |
| `FLOWFILE_STARTUP_TIMEOUT` / `FLOWFILE_SHUTDOWN_TIMEOUT` | `conftest.py:56-58` | Worker spawn/stop timeouts (30/15s). |
| `FLOWFILE_SHARED_DIR`, `FLOWFILE_INTERNAL_TOKEN`, `FLOWFILE_CORE_URL`, `FLOWFILE_KERNEL_IMAGE` | `flowfile_core/tests/kernel_fixtures.py` | Kernel test wiring; `FLOWFILE_KERNEL_IMAGE` preset skips the image build. |
| `KEEP_MINIO_RUNNING` / `KEEP_GCS_RUNNING` / `KEEP_AZURITE_RUNNING` / `KEEP_REDPANDA_RUNNING` | test_utils fixtures | `=true` keeps the container alive after the managed context exits (debugging). |
| `TEST_URL` / `API_URL` | `flowfile_frontend/tests/web-flow.spec.ts:24-25` | Playwright targets (default `http://localhost:8080` / `http://localhost:63578`). |
| `COVERAGE_CORE=sysmon` | CI coverage job env (`test.yaml:263-264`) | PEP-669 tracer; near-zero overhead on 3.12. |
| `FLOWFILE_MODE` | read per-call by sharing gates; sharing conftest monkeypatches to `docker` per test | Never flip process-wide in core tests (electron tokens minted at import). |

## 7. How to run each suite (verified commands + prerequisites)

All Python suites run **from the repo root** through the single Poetry env.

| Suite | Command | Prereqs / notes |
|---|---|---|
| core | `poetry run pytest flowfile_core/tests` | Autouse fixtures spawn worker + postgres + mysql. Docker-less: everything Docker-dependent skips. Worker-less: `SKIP_WORKER_TESTS=1 poetry run pytest flowfile_core/tests`. CI runs `-m "not kernel"`. ~5,079 tests collected (verified `--collect-only -q`: "5079 tests collected in 17.06s"), 76 of them `-m kernel`. |
| worker | `poetry run pytest flowfile_worker/tests` | conftest sets TEST_MODE=1; postgres 5433 autouse (fails hard if Docker present but container won't start). 311 collected. Cloud tests need MinIO/GCS/Azurite up (start via `poetry run start_minio` etc.), else skip. |
| frame | `poetry run pytest flowfile_frame/tests` | conftest registers a MinIO connection; cloud tests need MinIO on :9000. 620 collected. |
| scheduler | `poetry run pytest flowfile_scheduler/tests` | Fully hermetic (tmp SQLite per test, `_spawn_flow` stubbed, `_utcnow` pinned). 13 collected. |
| shared | `poetry run pytest shared/tests --ignore=shared/tests/kafka` | 89 collected (non-kafka). Kafka subdir needs Redpanda. |
| shared kafka | `poetry run pytest shared/tests/kafka/ -v` | session-autouse conftest starts/reuses Redpanda, skips without Docker. |
| kafka integration | `poetry run pytest tests/kafka -m kafka` | `pytestmark = pytest.mark.kafka`; Redpanda auto-managed; CI: `test-kafka-integration.yml:112`. |
| kernel (core-side, Docker) | `poetry run pytest flowfile_core/tests -m kernel -v` | Builds `flowfile-kernel` image unless `FLOWFILE_KERNEL_IMAGE` preset; needs Docker; skips locally / fails in CI when Docker absent. 76 tests. |
| kernel_runtime unit | `poetry run pytest kernel_runtime/tests` (or CI style: `pip install -e "kernel_runtime/[test]" && python -m pytest kernel_runtime/tests`) | No Docker — TestClient only. 327 collected. |
| docker E2E | `poetry run pytest tests/integration -m docker_integration -v` | Docker + compose v2; ports 63578/63579 must be FREE (skips otherwise); builds all images (~minutes); CI: `test-docker-kernel-e2e.yml:71`. |
| auth Docker E2E | `poetry run pytest flowfile_core/tests/test_auth_e2e.py -v -s` | Builds the real core image via docker SDK; skips without Docker. CI: `test-docker-auth.yml:92` (runs from `flowfile_core/` cwd; Poetry resolves the root pyproject upward). |
| migrate tool | `poetry run pytest tools/migrate/tests` | 64 collected; hermetic. |
| flowfile CLI | `poetry run pytest flowfile/tests` | test_api.py starts a real flowfile server via `start_flowfile_server_process()`; 19 test fns. CI runs it AFTER stopping the mock DB containers (test.yaml:192-214). |
| coverage | `make test_coverage` | core+worker sequential `--cov-append` (see §2). |
| frontend unit | `cd flowfile_frontend && npm run test:unit` | Vitest, `vitest.config.ts`: `include: src/**/*.test.ts`, env **node**, `globals:false`, alias only `@`→`src/renderer/app`. 30 test files / ~409 `it()` cases (stores, composables, types, features). Watch: `npm run test:unit:watch`. |
| frontend E2E | `cd flowfile_frontend && npm run test:web` (web-flow only) or `npm run test:all` (adds canvas-overlays.spec.ts) | **No `webServer` block in playwright.config.ts** — core (:63578) and a Vite server must already run. Config: `testDir ./tests`, timeout 120s, `workers: 1`, retries 2 in CI, trace/video on-first-retry. Browser: `npx playwright install chromium`. |
| frontend E2E orchestrated | `make test_e2e` (build:web → core → `preview:web` :4173 → `TEST_URL=http://localhost:4173 npx playwright test tests/web-flow.spec.ts` → `make stop_servers`) / `make test_e2e_dev` (dev server :8080) | Makefile:184-225. Note `|| true` after playwright → the make target itself never fails on test failure; cleanup: `make clean_test` removes test-results/ + playwright-report/. `stop_servers` does `pkill -f flowfile_core` / `pkill -f vite`. |
| wasm JS | `cd flowfile_wasm && npm run test:run` (`test` = watch, `test:coverage` = v8) | Vitest `happy-dom`, `globals:true`, setup `tests/setup.ts` (fake-indexeddb + sessionStorage/localStorage/Blob mocks), include `src/**` + `tests/**` `.test/.spec`. ~348 cases in tests/ (unit 22 files, components 4, integration 1). |
| wasm Python engine | `cd flowfile_wasm && pip install -r tests/python/requirements.txt && python -m pytest tests/python` (own `pytest.ini` with `testpaths = tests/python`) | **Pins polars==1.18.0, pydantic==2.10.5, openpyxl==3.1.5, XlsxWriter==3.2.0, polars-expr-transformer==0.5.6 — the exact Pyodide v0.27.7 versions.** Running via the monorepo Poetry env resolves a different Polars — use the pinned env for parity. 85 test fns. |
| wasm Pyodide smoke | `cd flowfile_wasm && npm install --no-save pyodide@0.27.7 parquet-wasm@0.7.1 && node tests/pyodide-smoke/smoke.cjs` | The only guard for browser-namespace/bootstrap breakage (CPython tests can't catch it). CI job `pyodide-smoke`. |

Verified collect counts (all run with `FLOWFILE_DB_PATH=<scratch>` isolation): core 5079 (76 kernel-marked), worker 311, frame 620, scheduler 13, shared non-kafka 89, kernel_runtime 327, tools/migrate 64. Static `grep -c "def test_"` counts: core 4064 fns/187 files, worker 292/26, frame 435/13, shared 127/13, kernel_runtime 327/11, root tests/ 41/3, migrate 64/3, wasm-python 85/8, flowfile CLI 19/2 (collected counts > fn counts because of parametrization, e.g. `execution_location`).

## 8. Suites needing Docker

- **Hard Docker**: `-m kernel` (core), `-m docker_integration` (tests/integration), `-m kafka` (tests/kafka, shared/tests/kafka), `test_auth_e2e.py`.
- **Soft Docker (skip/degrade without it)**: core+worker postgres/mysql autouse fixtures; MinIO/GCS/Azurite-guarded cloud tests in core, worker, frame.
- **Never Docker**: scheduler, kernel_runtime unit, tools/migrate, wasm (all), frontend vitest, most of core's route/schema/AI tests (AI suite is mocked & cheap per memory note).
- On macOS/Windows **with `CI` set**, all test_utils fixtures report Docker unavailable by design; `flowfile_core_test_utils.is_docker_available()` returns False on Windows unconditionally.

## 9. Frontend & WASM test infra details

- `flowfile_frontend/vitest.config.ts` — deliberately narrow: "Frontend integration coverage lives in Playwright... full DOM-bound testing (jsdom/happy-dom) is out of scope until something needs it" (header comment). Unit tests co-locate next to modules as `*.test.ts`.
- Playwright specs: `tests/web-flow.spec.ts` (auth token via `POST /auth/token`, imports `tests/fixtures/complex-flow.yaml` with 21 node types, runs flow through the API+UI) and `tests/canvas-overlays.spec.ts`. Top-of-file TODO (web-flow.spec.ts:1-9): desktop/Tauri E2E was deleted in the Tauri migration and never replaced — sidecar startup/port allocation/shutdown is **untested**; the intended fix is a tauri-driver smoke test.
- CI `e2e-tests.yml`: builds `npm run build:web`, starts `poetry run flowfile_core` + `npm run preview:web` (:4173) in background with curl-wait loops, then `npx playwright test tests/web-flow.spec.ts --reporter=html` with `TEST_URL`/`API_URL` env.
- `flowfile_wasm/pytest.ini` — the only other pytest config; scoped so `cd flowfile_wasm && pytest` runs only `tests/python` independent of root config.
- WASM CI (`flowfile-wasm-build.yml`) = 3 jobs: build+`npm run test:run` (plus a grep gate that `dist/flowfile-editor.js` has no literal `import("https://` — breaks webpack5/esbuild embedders), `python-engine-tests` (pinned pip env), `pyodide-smoke`.

## 10. CI test matrix (test.yaml, primary gate)

- `detect-changes` (dorny/paths-filter) gates every job; `workflow_dispatch` input `run_all_tests` forces all.
- `backend-tests` matrix: ubuntu × py3.10/3.11/3.12/3.13 + macos-latest × 3.11. Steps: poetry install → start postgres/mysql/minio/azurite/gcs via the poetry scripts → per-package pytest (shared w/o kafka → frame → core `-m "not kernel"` → worker) → stop containers → flowfile CLI tests last (after container stop). On macOS the `start_*` steps no-op (CI + macOS → Docker "unavailable") and Docker tests skip.
- `coverage`: separate ubuntu 3.12 job, `COVERAGE_CORE=sysmon`, core+worker with `--cov --cov-append --cov-report=`, then `coverage report`/`coverage xml` → Codecov (flag `backend`, `fail_ci_if_error: false`).
- `backend-tests-windows`: windows-latest py3.11, same shape (Docker steps effectively no-op).
- `kernel-tests`: ubuntu 3.11, 15-min timeout; builds `flowfile-kernel` image; runs kernel_runtime unit tests via `pip install -e "kernel_runtime/[test]"`; then `poetry run pytest flowfile_core/tests -m kernel`.
- `check-stubs` (`make check_stubs` = regenerate + `git diff --exit-code` on `.pyi`), `check-formula-docs` (`make check_formula_docs`), `test-web` (vitest + build:web + preview :4173 curl 200 check), `docs-test` (mkdocs build), `test-summary` (fails if any non-skipped job failed).
- Separate workflows: `e2e-tests.yml` (Playwright), `test-docker-auth.yml` (test_auth.py unit + test_auth_e2e.py Docker), `test-kernel-integration.yml` (`-m kernel` with `TEST_MODE=1`, `FLOWFILE_INTERNAL_TOKEN`, `FLOWFILE_KERNEL_IMAGE=flowfile-kernel-base:test` preset; also builds the ml flavour), `test-docker-kernel-e2e.yml` (`pytest tests/integration -m docker_integration`), `test-kafka-integration.yml` (shared/tests/kafka + `tests/kafka -m kafka`; dumps `docker logs test-redpanda-kafka` on failure).

## 11. State isolation model (and where it's fragile)

**DB**: One SQLite file per "mode": live `~/.flowfile/database/flowfile_catalog.db`; test `~/.flowfile/temp/test_flowfile_catalog.db` (fixed path via `TESTING=True`); explicit override `FLOWFILE_DB_PATH`. Schema built by Alembic at import of `flowfile_core.database.init_db` → the session-autouse `setup_test_db` calls `init_db()` and drops+deletes the file at teardown.

**Fragile points (all observed/verified):**
1. **Fixed shared test-DB path** — two concurrent pytest sessions (any two envs on the same machine) share `test_flowfile_catalog.db`; the first teardown drops the other's tables mid-run → non-deterministic `sqlite3.OperationalError: no such table: catalog_table_read_links` cascades. Mitigation: `FLOWFILE_DB_PATH=/tmp/ff_isolated_$$.db poetry run pytest ...` and `ps aux | grep pytest` before blaming your change. (Incident: user memory `feedback_isolated_test_db_path.md` — "phantom 'my change broke 35 tests'" was a concurrent `ff_311` session.)
2. **Import-time migration** — `import flowfile_core` (even transitively) runs `run_startup_migration()` against whatever DB resolves (init_db.py:26-27). Incident 2026-05-04 (memory `feedback_skip_startup_migration.md`): a diagnostic import through a *stale* Poetry env whose migrations only went to 010 saw the live DB stamped 013, downgraded the stamp without reverting schema, and corrupted the migration state (manual re-stamp + table recreation needed). Rules: diagnostics → `FLOWFILE_SKIP_STARTUP_MIGRATION=1` (+ `FLOWFILE_DB_PATH=/tmp/throwaway.db` to exercise DB paths); never set the skip flag for the core test suite. Proposed-but-unshipped fix: move migration into the FastAPI lifespan.
3. **Catalog rows + seed erosion** — many core test modules call `catalog_cleanup()` which wipes ALL CatalogNamespace rows including the init_db-seeded 'General' catalog; the project tests depend on that seed, hence the autouse re-seed in `tests/project/conftest.py` (module ordering bug, papered over per-directory). New suites that depend on seeded rows need the same treatment.
4. **Worker virtual-result cache** — `catalog_cleanup()` also deletes `storage.catalog_virtual_results_directory` `.arrow` files because table-id recycling lets a stale `fvt-{table_id}-{hash}.arrow` satisfy the next test's resolve (comment at flowfile/conftest.py:78-81).
5. **Session-global services** — worker (:63579), postgres (:5433), mysql (:3307) are session singletons that are REUSED if already listening; a dirty long-running worker/db from a previous session can leak state into a new run. Same for Redpanda (topics are UUID-suffixed precisely to survive container reuse — tests/kafka/conftest.py:44-53).
6. **Process-wide env mutation** — sharing tests flip `FLOWFILE_MODE=docker` via monkeypatch per-test only; kernel fixtures mutate and painstakingly restore `FLOWFILE_SHARED_DIR`/`FLOWFILE_INTERNAL_TOKEN`/`FLOWFILE_CORE_URL`/`FLOWFILE_KERNEL_IMAGE` and rebuild the `storage` singleton twice. Any new fixture touching these must restore them or later tests inherit corrupted paths.
7. **Module-level TestClients** — several core test modules create `TestClient(main.app)` and mint tokens at import time under electron mode; anything that changes global auth mode before those imports breaks them (sharing conftest docstring).
8. **kernel_runtime module singletons** — `main.artifact_store`, `_persistence`, `_namespace` state are module globals; only the autouse `_clear_global_state` fixture keeps tests independent.
9. **bcrypt patch** — core conftest monkey-patches `bcrypt.hashpw` at import (72-byte truncation); tests touching password hashing behave differently from prod bcrypt >5.0 without it.

## 12. Slow / flaky areas

- `backend-tests (ubuntu, 3.12)` was ~56 min pre-2026-06-20 (coverage C-tracer + serial core suite); now split, ~28-31 min expected. Core suite is the long pole (5k tests, serial; xdist deferred as high-risk).
- Docker image builds dominate `-m kernel` (~30s+ saved by presetting `FLOWFILE_KERNEL_IMAGE`) and `-m docker_integration` (builds core+worker+kernel; 600s build timeouts in conftest).
- MySQL container start: up to 60s (`STARTUP_TIMEOUT`), image pull up to 300s first time.
- Worker viz tests are `@pytest.mark.slow` (`flowfile_worker/tests/test_catalog_visualize.py`).
- Load-bearing sleeps (do NOT remove): catalog 1.05s sleeps (SQLite 1-second `updated_at` granularity) and 2s cancel sleeps (per memory `project_ci_test_speed.md`).
- Playwright: retries=2 in CI + trace/video on-first-retry — the retry budget masks flakes; single worker to avoid port conflicts.
- Trailing-slash axios/FastAPI 307s historically caused silent failures only in Docker (memory `feedback_match_axios_slash_to_route.md`) — Vite proxy and pytest TestClient both mask it; verify in core logs.

## 13. What "validated" means for a change here (evidence bar)

1. **Backend change (core/worker/shared/frame)**: run the owning package suite from repo root with an isolated DB, e.g. `FLOWFILE_DB_PATH=/tmp/ff_$$.db poetry run pytest flowfile_core/tests -m "not kernel"`. Docker-dependent coverage requires the relevant `start_*` container(s) up, else those tests silently skip — a green run without Docker is weaker evidence. Check that skip counts didn't balloon.
2. **Cross-boundary (core↔worker) change**: must run WITHOUT `SKIP_WORKER_TESTS` so the real worker subprocess is exercised; `execution_location` parametrized tests need the worker for their `remote` half.
3. **DB schema change**: new `flowfile_core/flowfile_core/alembic/versions/NNN_*.py` + `poetry run pytest flowfile_core/tests/test_migration.py` (uses `FLOWFILE_DB_PATH` to build DBs from scratch).
4. **Kernel-touching change**: `poetry run pytest flowfile_core/tests -m kernel` locally with Docker (CI runs it in two workflows).
5. **flowfile_frame public API change**: `make stubs` + commit the `.pyi` diff — `make check_stubs` is a hard CI gate.
6. **Kafka path change**: `poetry run pytest tests/kafka -m kafka` + `shared/tests/kafka`.
7. **Frontend renderer change**: `npm run test:unit` + `npm run build:web` (lint + vue-tsc are part of the build script) — that's what `test-web` CI enforces; behavior changes touching the canvas/flow need `make test_e2e` (but remember its `|| true` — read the Playwright output, not the make exit code).
8. **WASM engine change**: pinned-env pytest AND the Pyodide smoke test (`node tests/pyodide-smoke/smoke.cjs`) — CPython green does not prove the browser namespace still works.
9. **Full-stack/deploy change**: `poetry run pytest tests/integration -m docker_integration -v` with ports 63578/63579 free.

## 14. Misc verified facts a skill author needs

- `poetry run pytest ...` is the invocation everywhere; there is no tox/nox.
- `pytest -m kernel` (bare, from root, as the root CLAUDE.md suggests) collects across ALL test dirs; the precise form used by CI is `poetry run pytest flowfile_core/tests -m kernel`.
- The worker started by core's conftest inherits the pytest process env — so `TESTING`/`FLOWFILE_DB_PATH` propagate to it (this is why FLOWFILE_DB_PATH isolation is complete).
- `tests/` (repo root) contains ONLY `integration/` and `kafka/` — the packages' suites live in `<pkg>/tests/`.
- Ruff excludes `tests/`, `test_*.py`, `*_test.py`, `conftest.py`, `test_utils/` from linting (root pyproject); per-file-ignores E501/S101 for tests.
- `test_utils/postgres/postgres-docker-samples/` is a git-cloned working dir created on first postgres start; it writes a `.env` inside itself and runs `build.sh` — network access to GitHub needed on first run.
- Frame tests conftest side effect: registers/replaces `minio-flowframe-test` cloud connection **in the (test) catalog DB at import**.
- flowfile CLI tests (`flowfile/tests/test_api.py`) boot a real server via `start_flowfile_server_process()` — they need free ports and are ordered after mock-DB shutdown in CI.
- Docker-auth CI runs pytest from `flowfile_core/` cwd; Poetry resolves the root pyproject by walking up (no package-level pyproject exists).
- The auth E2E suite (`test_auth_e2e.py`) uses the `docker` Python SDK directly (not compose) and expects to build image `flowfile-core:e2e-test`.
