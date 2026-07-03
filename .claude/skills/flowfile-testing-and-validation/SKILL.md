---
name: flowfile-testing-and-validation
description: Exact per-package pytest/vitest/playwright commands, the 6 pytest markers and which need Docker, the test_utils Docker fixture matrix, the shared-test-DB isolation model and its failure modes, xfail/skip discipline, and coverage/CI test-matrix mechanics for the Flowfile monorepo. Use when running or writing tests, diagnosing "no such table" or phantom test failures, deciding whether a change is "validated," seeing XPASS in test output, wiring a new Docker-backed fixture, or asking "which suite proves this change works."
---

# Flowfile Testing and Validation

## When NOT to use this skill

- Writing a **new backend node** or its tests → `flowfile-node-development`.
- Debugging a **specific failure** you already reproduced (stack trace triage, log reading) → `flowfile-debugging-playbook`.
- **CI workflow authoring/release mechanics** beyond test.yaml's test jobs (tagging, PyPI/Tauri release, Docker publish, branch protection) → `flowfile-change-control` (release/tag mechanics, branch protection, publish pipelines).
- **Env var reference** beyond the handful that control test isolation (full `.env` catalog, feature flags) → `flowfile-config-and-flags`.
- **Known bugs / historical incidents** not related to test infra itself → `flowfile-failure-archaeology`.
- Local dev server startup, ports, Docker Compose for *running* the app (not testing it) → `flowfile-run-and-operate` / `flowfile-build-and-env`.

If you just need "how do I run the core tests" — that's this skill, keep reading.

---

## 1. The single pytest config, and why bare `pytest` is dangerous

`pyproject.toml` `[tool.pytest.ini_options]` is the **only** pytest config for the monorepo (exception: `flowfile_wasm/pytest.ini`, scoped so `cd flowfile_wasm && pytest` runs only `tests/python`). There is **no `addopts`, no `testpaths`, no `norecursedirs`**.

Consequence: running bare `pytest` from repo root collects **everything**, including slow Docker-compose E2E tests (`tests/integration`) and Kafka tests (`tests/kafka`). Any doc claiming "pytest excludes these by default" is wrong at the config level — they're only skipped in practice because everyone targets a specific directory.

**Rule: always pass an explicit directory.** `poetry run pytest flowfile_core/tests`, never bare `poetry run pytest`.

### The 6 registered markers (root `CLAUDE.md`'s marker table is stale — it omits `lsp`)

```toml
markers = [
    "worker: Tests for the flowfile_worker package",
    "core: Tests for the flowfile_core package",
    "kernel: Integration tests requiring Docker kernel containers",
    "docker_integration: Full Docker-based E2E tests (require Docker, slow)",
    "kafka: Integration tests requiring a Kafka/Redpanda broker (Docker)",
    "lsp: Tests for the notebook LSP (Jedi) code-intelligence surface",
]
```

| Marker | Needs Docker? | Where used |
|---|---|---|
| `worker` | No (marker only) | flowfile_worker package tests |
| `core` | No (marker only) | flowfile_core package tests |
| `kernel` | **Yes** | `flowfile_core/tests -m kernel` (76 tests); builds/runs kernel containers |
| `docker_integration` | **Yes** | `tests/integration` — full docker-compose E2E |
| `kafka` | **Yes** | `tests/kafka`, `shared/tests/kafka` — needs Redpanda |
| `lsp` | No | `flowfile_core/tests/lsp/test_lsp_routes.py` (hermetic); `test_lsp_kernel_integration.py` also carries `@pytest.mark.kernel` |

Two more markers are used but **not registered** (pytest warns, doesn't fail): `@pytest.mark.slow` (`flowfile_worker/tests/test_catalog_visualize.py`), and `slow`/`requires_yaml` registered locally by `tools/migrate/tests/conftest.py`.

---

## 2. Run each suite — exact commands and prerequisites

All commands run from the repo root through the single Poetry env; there is **no tox/nox**.

| Suite | Command | Prerequisites / notes |
|---|---|---|
| core | `poetry run pytest flowfile_core/tests` | Autouse fixtures spawn worker + Postgres + MySQL. CI form: `-m "not kernel"`. Worker-less: prefix `SKIP_WORKER_TESTS=1`. ~5,079 tests collected as of 2026-07-03 (v0.12.7), 76 of them kernel-marked. |
| worker | `poetry run pytest flowfile_worker/tests` | conftest sets `TEST_MODE=1`; Postgres on :5433 autouse (hard-fails if Docker is present but the container won't start). Cloud tests need MinIO/GCS/Azurite up. ~311 collected. |
| frame | `poetry run pytest flowfile_frame/tests` | conftest registers a `minio-flowframe-test` cloud connection at import; cloud tests need MinIO on :9000. ~620 collected. |
| scheduler | `poetry run pytest flowfile_scheduler/tests` | Fully hermetic — tmp SQLite per test, spawn stubbed, clock pinned. ~13 collected. |
| shared (no kafka) | `poetry run pytest shared/tests --ignore=shared/tests/kafka` | ~89 collected. |
| shared kafka | `poetry run pytest shared/tests/kafka/ -v` | session-autouse conftest starts/reuses Redpanda; skips without Docker. |
| kafka integration | `poetry run pytest tests/kafka -m kafka` | Needs Redpanda (auto-managed) + a running worker. |
| kernel (core-side) | `poetry run pytest flowfile_core/tests -m kernel -v` | Builds `flowfile-kernel` image unless `FLOWFILE_KERNEL_IMAGE` is preset; needs Docker. ~76 tests. |
| kernel_runtime unit | `poetry run pytest kernel_runtime/tests` | No Docker — `TestClient` only. ~327 collected. |
| docker E2E | `poetry run pytest tests/integration -m docker_integration -v` | Docker + compose v2; ports 63578/63579 must be **free** or tests skip; builds core+worker+kernel images (minutes). |
| auth Docker E2E | `poetry run pytest flowfile_core/tests/test_auth_e2e.py -v -s` | Builds real core image via the `docker` SDK directly (not compose); skips without Docker. |
| migrate tool | `poetry run pytest tools/migrate/tests` | ~64 collected; hermetic. |
| flowfile CLI | `poetry run pytest flowfile/tests` | `test_api.py` boots a real server via `start_flowfile_server_process()`; needs free ports. |
| coverage | `make test_coverage` | Core+worker sequential, `--cov-append` (see §5). |
| frontend unit | `cd flowfile_frontend && npm run test:unit` | Vitest, node env, no jsdom/happy-dom. ~30 test files. Watch mode: `npm run test:unit:watch`. |
| frontend E2E | `cd flowfile_frontend && npm run test:web` (web-flow only) or `npm run test:all` (adds canvas-overlays) | **No `webServer` block in `playwright.config.ts`** — core (:63578) and a Vite server must already be running. `npx playwright install chromium` first. Single worker, 2 retries in CI. |
| frontend E2E orchestrated | `make test_e2e` / `make test_e2e_dev` | Builds/starts core + preview(:4173) or dev(:8080), runs `web-flow.spec.ts`, then `make stop_servers`. **The make target itself never fails on test failure** (`|| true` after the playwright call) — read the Playwright output, not the make exit code. |
| wasm JS | `cd flowfile_wasm && npm run test:run` | Vitest, happy-dom, globals on. ~348 cases. |
| wasm Python engine | `cd flowfile_wasm && pip install -r tests/python/requirements.txt && python -m pytest tests/python` | Own `pytest.ini`. **Pins polars==1.18.0 / pydantic==2.10.5 / polars-expr-transformer==0.5.6 — the exact Pyodide 0.27.7 versions.** Running through the monorepo Poetry env resolves a different Polars; use the pinned env for parity. ~85 test fns. |
| wasm Pyodide smoke | `cd flowfile_wasm && npm install --no-save pyodide@0.27.7 parquet-wasm@0.7.1 && node tests/pyodide-smoke/smoke.cjs` | The only guard for browser-namespace/bootstrap breakage — CPython tests can't catch it. |

Worked example — run only core tests, isolated from any other pytest session, without needing a worker:

```bash
FLOWFILE_DB_PATH=/tmp/ff_core_$$.db SKIP_WORKER_TESTS=1 \
  poetry run pytest flowfile_core/tests -m "not kernel" -q
```

---

## 3. test_utils/ — the Docker fixture matrix

Package layout: `test_utils/{postgres,mysql,s3,gcs,azurite,kafka}/`, each with `fixtures.py` + `commands.py`. Every `start_*`/`stop_*` command is a Poetry script (`poetry run start_postgres`, `poetry run stop_postgres`, etc.) and **all `start_*` commands exit 0 when Docker is missing** — "return success to allow pipeline to continue" — so downstream tests just skip rather than the CI step failing.

| Service | Container name | Host port(s) | Started by | Skip behavior |
|---|---|---|---|---|
| Postgres | `test-postgres-sample` | **5433**→5432 | `poetry run start_postgres`; core+worker conftest autouse (reuses if already listening) | `is_docker_available()` False → tests `skipif`; if Docker present but start fails → `pytest.fail` (core), soft-skip (worker uses same pattern but less strict) |
| MySQL | `test-mysql-sample` | **3307**→3306 | `poetry run start_mysql`; core conftest autouse | Core: soft-fail with a log message, tests skip; image pull can take up to 300s first time |
| MinIO (S3) | `test-minio-s3` | **9000** API, **9001** console | `poetry run start_minio` | `_minio_available()` / `requires_minio` guards; frame conftest assumes :9000 |
| GCS | `test-fake-gcs` | **4443** | `poetry run start_gcs` | `is_gcs_available()` guard; also re-populates data if container is up but empty |
| Azurite | `test-azurite` | **10000** (blob) | `poetry run start_azurite` | `is_azurite_available()` guard; well-known devstoreaccount1 creds hardcoded |
| Redpanda (Kafka) | `test-redpanda-kafka` | **19092**→9092 | `poetry run start_redpanda`; `tests/kafka` + `shared/tests/kafka` conftest autouse | Skips without Docker; topics are UUID-suffixed per test so container reuse is safe |

Shared skip logic (every service's `is_docker_available()`):
1. On macOS or Windows **when `CI` env is truthy** → returns False (Docker treated as unavailable on non-Linux CI runners).
2. Otherwise: `shutil.which("docker")` must exist and `docker info` must exit 0 within 5s.

`KEEP_MINIO_RUNNING` / `KEEP_GCS_RUNNING` / `KEEP_AZURITE_RUNNING` / `KEEP_REDPANDA_RUNNING` (=`true`) keep a container alive after the managed context exits, for debugging.

`flowfile_core/tests/flowfile_core_test_utils.py::is_docker_available()` additionally returns **False on Windows unconditionally** (not just in CI).

---

## 4. State isolation model — and where it breaks

**One SQLite DB per mode, resolved by `shared/storage_config.py::get_database_url()`** (priority order, verified in code):

1. `FLOWFILE_DB_PATH` env var (explicit override) → `sqlite:///$FLOWFILE_DB_PATH`
2. `TESTING=True` env var (exact string) → `sqlite:///<storage.temp_directory>/test_flowfile_catalog.db` — a **fixed shared path**
3. Default → `sqlite:///<storage.database_directory>/flowfile_catalog.db` (the live DB)

Core conftest sets `os.environ['TESTING'] = 'True'` at import (`flowfile_core/tests/conftest.py:23`), before any `flowfile_core` import, so the test DB resolves correctly. Frame conftest does the same (`flowfile_frame/tests/conftest.py:3`).

### Fragile point #1 (the one that will burn you): fixed shared test-DB path

Because step 2's path is **fixed** (not per-process), **two concurrent pytest sessions on the same machine share `test_flowfile_catalog.db`**. The session-scoped autouse `setup_test_db` fixture (`flowfile_core/tests/conftest.py`) calls `init_db()` on setup and, on teardown, does `Base.metadata.drop_all(engine)` **then deletes the DB file**. If session A tears down while session B is mid-run, B's tables vanish out from under it.

**Symptom:** a cascade of `sqlite3.OperationalError: no such table: <catalog_table_read_links|users|...>` that looks like your change broke 35 unrelated tests. It didn't — a second concurrent pytest process (yours from an earlier terminal, a background CI-simulation run, anything) tore down the shared DB mid-test.

**The fix — highest-priority override, use it for every isolated run:**

```bash
FLOWFILE_DB_PATH=/tmp/ff_$$.db poetry run pytest flowfile_core/tests -m "not kernel"
```

`FLOWFILE_DB_PATH` wins over `TESTING` in `get_database_url()`, so this fully isolates the DB per invocation. It also **propagates to the worker subprocess** the core conftest spawns (the worker inherits the pytest process env), so cross-boundary core↔worker tests stay isolated too — this is why the override is "complete," not partial.

Before blaming a code change for a wall of table-not-found errors, run `ps aux | grep pytest` and check for a second session.

### Fragile point #2: never skip the startup migration for the test suite

`flowfile_core/flowfile_core/database/init_db.py` runs `run_startup_migration()` at **import time** unless `FLOWFILE_SKIP_STARTUP_MIGRATION` is set:

```python
if not os.environ.get("FLOWFILE_SKIP_STARTUP_MIGRATION"):
    run_startup_migration()
```

The core test suite's `setup_test_db` fixture depends on that import-time migration to create the schema in the first place. **If you set `FLOWFILE_SKIP_STARTUP_MIGRATION=1` while running `flowfile_core/tests` against a fresh/isolated `FLOWFILE_DB_PATH`, every test errors with `no such table: users`** — there was never a migration run to create the tables.

`FLOWFILE_SKIP_STARTUP_MIGRATION=1` is for **diagnostics only** — e.g. importing `flowfile_core` in a scratch script to inspect something without touching a real DB's migration stamp. Pair it with its own throwaway `FLOWFILE_DB_PATH` in that case, and never use it for `pytest flowfile_core/tests`.

```bash
# WRONG — will error "no such table: users" on every test
FLOWFILE_DB_PATH=/tmp/fresh.db FLOWFILE_SKIP_STARTUP_MIGRATION=1 poetry run pytest flowfile_core/tests

# RIGHT — isolated DB, migration allowed to run and build the schema
FLOWFILE_DB_PATH=/tmp/fresh.db poetry run pytest flowfile_core/tests
```

### `SKIP_WORKER_TESTS=1`

`flowfile_core/tests/conftest.py`'s session-autouse `flowfile_worker` fixture checks `os.environ.get("SKIP_WORKER_TESTS") == "1"` and no-ops if set (no worker spawned, no reuse-probe). The `execution_location` fixture (parametrized `["local", "remote"]`, used by catalog/flow-API/run-node tests) then auto-skips its `remote` half. Use this to run core tests fast without a worker; do **not** use it when validating a core↔worker contract change (§6 below).

### Other fragile points (verified in code, worth knowing)

- **Catalog seed erosion**: many core test modules call a `catalog_cleanup()` helper that wipes *all* `CatalogNamespace` rows, including the init_db-seeded `'General'` namespace. `flowfile_core/tests/project/conftest.py` has an autouse re-seed specifically to paper over this — a new suite depending on seeded catalog rows needs the same treatment.
- **Worker virtual-result cache**: `catalog_cleanup()` also deletes `.arrow` files under the worker's virtual-results directory, because table-id recycling would otherwise let a stale cached file satisfy the next test's resolve.
- **Session-global services**: worker (:63579), Postgres (:5433), MySQL (:3307), Redpanda are reused if already listening — a dirty long-running instance from a previous session can leak state into a new run.
- **Process-wide env mutation**: the sharing test suite flips `FLOWFILE_MODE=docker` via `monkeypatch.setenv` **per test only** — several core test modules construct a `TestClient` and mint auth tokens at **import time** under electron mode; flipping the mode process-wide before those imports breaks them.
- **bcrypt monkeypatch**: core conftest patches `bcrypt.hashpw` at import to truncate >72-byte passwords (passlib/bcrypt compat) — password-hashing tests behave differently from a prod bcrypt install without it.

---

## 5. Coverage

- **Source is core + worker only**: `[tool.coverage.run] source = ["flowfile_core/flowfile_core", "flowfile_worker/flowfile_worker"]` — frame, scheduler, and shared are deliberately excluded from coverage.
- `fail_under = 0` — coverage never gates a build by threshold.
- `omit`: `*/tests/*`, `*/test_*`, `*/__pycache__/*`, `*/conftest.py`.
- **No `branch=true`, no `dynamic_context`** — deliberate, because CI sets `COVERAGE_CORE=sysmon` (PEP-669 `sys.monitoring` tracer), which doesn't support those options.

Local:
```bash
make test_coverage
# = poetry run pytest flowfile_core/tests --cov --cov-report= --disable-warnings
#   poetry run pytest flowfile_worker/tests --cov --cov-append --cov-report= --disable-warnings
#   poetry run coverage report --show-missing
```
Core and worker run **sequentially with `--cov-append`** — the Makefile comment explains this avoids import collisions from both packages loading into one coverage-tracked process.

CI's dedicated `coverage` job (ubuntu, Python 3.12) sets `COVERAGE_CORE: sysmon`, runs core `-m "not kernel"` then worker, uploads `coverage.xml`/`.coverage` as an artifact, and pushes to Codecov (`flags: backend`, `fail_ci_if_error: false` — Codecov never blocks CI either). There is **no `codecov.yml`** in the repo; behavior is Codecov defaults.

---

## 6. Evidence bar — what "validated" means per change class

A green run is only as strong as what actually executed. Docker-gated suites **silently skip** without Docker/MinIO/etc — always ask "did the Postgres/MinIO/kernel tests actually run, or did they skip?" (check the pytest summary line for skip counts, not just "passed").

| Change class | Minimum bar |
|---|---|
| Core/worker/shared/frame backend change | Run the owning package's suite with an isolated DB: `FLOWFILE_DB_PATH=/tmp/ff_$$.db poetry run pytest <pkg>/tests`. Check skip counts didn't balloon vs. a baseline run. |
| Core ↔ worker contract change | Run **without** `SKIP_WORKER_TESTS` — the real worker subprocess must be exercised; `execution_location`-parametrized tests need it for their `remote` half. |
| DB schema change | Add `flowfile_core/flowfile_core/alembic/versions/NNN_*.py`, then `poetry run pytest flowfile_core/tests/test_migration.py` (builds DBs from scratch via `FLOWFILE_DB_PATH`). |
| Kernel-touching change | `poetry run pytest flowfile_core/tests -m kernel` locally with Docker running. |
| `flowfile_frame` public API change | `make stubs` and stage the `.pyi` diff (do not commit it yourself — hand off per `flowfile-change-control`'s no-agent-commit policy) — `make check_stubs` is a hard CI gate (regenerates then `git diff --exit-code`). |
| Kafka path change | `poetry run pytest tests/kafka -m kafka` plus `shared/tests/kafka`. |
| Frontend renderer change | `npm run test:unit` + `npm run build:web` (lint + `vue-tsc --noEmit` run inside the build script) — that's what CI's `test-web` job enforces. Canvas/flow behavior changes additionally need `make test_e2e` — **read its Playwright output, the make exit code is unreliable** (see §2). |
| WASM engine change | Pinned-env pytest (`tests/python`) **and** the Pyodide smoke test — a green CPython run does not prove the browser namespace still works. |
| Full-stack / deploy-shaped change | `poetry run pytest tests/integration -m docker_integration -v` with ports 63578/63579 free. |

---

## 7. xfail / XPASS discipline

Current inventory (verified live 2026-07-03, v0.12.7 — **re-run before trusting**, see §9):

| Location | What it claims | Live status |
|---|---|---|
| `flowfile_core/tests/flowfile/test_code_generator_edge_cases.py:201` (`test_in_operator_numeric`) | Flow's IN filter mishandles numeric-string quoting vs. codegen | **STALE — XPASS.** The bug was fixed elsewhere (filter logic moved to `filter_expressions.py`); the marker is dead weight. |
| `test_code_generator_edge_cases.py:459` (`test_unique_without_columns`) | Flow's `unique(columns=[])` errors via an internal `group_by` needing ≥1 key; generated code is correct | Still **xfail** — real bug, flow is the buggy side. |
| `test_code_generator_edge_cases.py:766` (`test_groupby_with_concat_aggregation`) | Delimiter mismatch: generated `str.concat()` defaults to `-`, flow uses `,` | Still **xfail** — real bug, codegen side. |
| `flowfile_core/tests/flowfile/node_designer/test_node_designer.py:516` (`TestNumericStringAliasBug`) | `data_types="numeric"` string alias maps to Decimal only | **STALE — XPASS.** Already fixed; the test class + its "workaround" sibling test now document a bug that no longer exists. |

None of these four markers use `strict=True`, so CI shows XPASS silently and nobody notices.

**Rule for this repo: XPASS means the xfail is stale. Delete the marker (and the outdated bug description) — never leave it, never "celebrate" the pass.** When you add a new xfail for a real known bug, prefer `@pytest.mark.xfail(reason=..., strict=True)` so a future fix turns into a hard CI failure demanding the marker's removal, instead of a silent XPASS nobody notices.

Verification recipe (isolated DB, do **not** pair with `FLOWFILE_SKIP_STARTUP_MIGRATION=1` — see §4):
```bash
FLOWFILE_DB_PATH=/tmp/xfail_probe.db poetry run pytest \
  "flowfile_core/tests/flowfile/test_code_generator_edge_cases.py::TestBasicFilterOperators::test_in_operator_numeric" \
  -q -p no:cacheprovider -rX
# → "1 xpassed" confirms staleness
```

Other skip inventory:
- `flowfile_core/tests/flowfile/test_basic_filter.py:751,761` — unconditional `pytest.mark.skip`, "Manual input converts None to string; test requires actual null values from file sources." Product limitation, not test debt: `is_null`/`is_not_null` are effectively untested via manual-input at integration level.
- `flowfile_worker/tests/test_train_apply_model.py:159` — benign parametrize carve-out (logistic_regression/knn_classifier need 0/1 targets; covered by a dedicated round-trip test elsewhere).
- No `.skip`/`.todo`/`.fixme` exist in any Playwright or Vitest spec (frontend or wasm) — the TS suites are clean of this pattern.
- The dominant "skip" pattern by volume is environment `skipif(not is_docker_available())` across core/worker/frame — dozens of uses. This is a coverage cliff, not debt: on a laptop without Docker+MinIO+emulators running, a large slice of integration surface silently skips, and a green local run is weak evidence of anything Docker-touching.

---

## 8. CI test matrix summary (`test.yaml`)

- **Concurrency**: group `${{ github.workflow }}-${{ github.ref }}`, `cancel-in-progress: ${{ github.event_name == 'pull_request' }}` — PR runs cancel their own superseded runs; **main-branch runs are never cancelled** (docker-publish/release pipelines key off completed main builds). This is the only workflow file in the repo with a `concurrency` block.
- **`detect-changes`** (`dorny/paths-filter`) gates every downstream job by which paths changed; `workflow_dispatch` input `run_all_tests: true` forces everything regardless.
- **`backend-tests` matrix**: `fail-fast: false`; `ubuntu-latest` × Python 3.10/3.11/3.12/3.13, plus `macos-latest` × 3.11. Starts Postgres/MySQL/MinIO/Azurite/GCS via the `poetry run start_*` scripts (no-op on macOS CI runners — Docker reports unavailable there). Core runs `-m "not kernel"`.
- **`coverage`**: separate ubuntu/3.12 job, `COVERAGE_CORE=sysmon` (see §5).
- **`backend-tests-windows`**: windows-latest, Python 3.11 only, pwsh shell.
- **`kernel-tests`**: ubuntu/3.11, 15-min timeout, builds the kernel image, runs `kernel_runtime` unit tests then `flowfile_core/tests -m kernel`.
- **`check-stubs`** / **`check-formula-docs`**: drift gates — regenerate `.pyi` stubs / `functions.md` and fail on `git diff`.
- **`test-web`**: Vitest unit + `build:web` + preview-server curl check.
- **`docs-test`**: `mkdocs build`.
- **`test-summary`**: `if: always()`, aggregates all jobs *except* `version-sync`, fails if any non-skipped job failed. Treat this as the real pass/fail signal for the whole run — but note it is **not** wired into required branch-protection checks (a CI-mechanics fact, not this skill's territory beyond flagging it).

Separate, path-filtered workflows cover what `test.yaml` doesn't: `e2e-tests.yml` (Playwright web E2E), `test-docker-auth.yml`, `test-kernel-integration.yml`, `test-docker-kernel-e2e.yml`, `test-kafka-integration.yml`, `flowfile-wasm-build.yml`.

---

## 9. History — read this before "fixing" CI test speed again

The `backend-tests (ubuntu, 3.12)` job was **~56 minutes** before 2026-06-20, caused by two compounding factors: coverage's default C-tracer roughly doubling runtime, and the core suite (~5k tests) running fully serially. Shipped fix (merged, do not re-litigate the same options without reading this first):

1. **`COVERAGE_CORE=sysmon`** — switched the coverage job to the PEP-669 `sys.monitoring` tracer, near-zero overhead on 3.12 vs. the old C-tracer's ~2x tax.
2. **Coverage split into its own dedicated job** — off the critical path of the functional matrix; the plain `backend-tests` matrix jobs run without `--cov` at all.
3. **Dropped redundant frontend builds from backend jobs.**
4. **`concurrency: cancel-in-progress` for PR runs.**

**`pytest-xdist` was evaluated and explicitly deferred** — do not casually re-propose it. Hard prerequisites that would need to be solved first, all rooted in the same fixed-DB-path problem as §4:
- Each xdist worker needs its **own** `FLOWFILE_DB_PATH`, derived from `PYTEST_XDIST_WORKER`, and that derivation must happen **before the first `flowfile_core` import** — the DB engine binds to a URL at import time, so setting the env var after import is a no-op.
- If used on the coverage job: `parallel = true` in `[tool.coverage.run]` plus a `coverage combine` step, neither of which exist today.
- Expected speedup is **~1.4–1.8x, not 2–4x**, because several fixtures are shared-service singletons (the worker on :63579, Postgres on :5433, MySQL on :3307) that don't parallelize cleanly across workers without further isolation work.

Without those prerequisites, naive `-n auto` reproduces exactly the "no such table" cascade from §4 fragile point #1, at coverage-job scale — an empty or garbaged `coverage.xml` is the typical failure mode.

**Load-bearing sleeps — do not remove these thinking they're dead time:**
- Catalog test `time.sleep(1.05)` calls exist because SQLite's `updated_at` column has 1-second granularity; a faster sleep produces flaky ordering assertions.
- 2-second sleeps around cancel-flow tests are similarly timing-load-bearing.

---

## 10. Slow / flaky areas

- Core suite is the long pole (~5k tests, serial) even post-fix; expect the 3.12 matrix job around 28–31 minutes as of the 2026-06-20 fixes.
- Docker image builds dominate `-m kernel` (mitigate by presetting `FLOWFILE_KERNEL_IMAGE` to skip the ~30s build) and `-m docker_integration` (builds core+worker+kernel; conftest uses 600s build timeouts).
- MySQL container start can take up to 60s; first-time image pull up to 300s.
- Worker viz tests are tagged `@pytest.mark.slow` (`flowfile_worker/tests/test_catalog_visualize.py`) — not registered as a marker, so it just warns.
- Playwright: `retries: 2` in CI plus trace/video on first retry — this retry budget can mask genuine flakes; `workers: 1` avoids port conflicts, not a performance choice.
- A trailing-slash axios/FastAPI mismatch has historically caused **silent** failures only in Docker (Vite's dev proxy and pytest's `TestClient` both mask a 307 redirect that Docker's real network path doesn't) — if a route "works locally but not in Docker," check for a slash mismatch in core logs, not test logic.

---

## Provenance and maintenance

Volatile facts below need periodic re-verification — commands are copy-pasteable.

- **Pytest markers list** (as of 2026-07-03, v0.12.7): `grep -A8 '\[tool.pytest.ini_options\]' pyproject.toml`
- **Coverage config**: `grep -A20 '\[tool.coverage.run\]' pyproject.toml`
- **`FLOWFILE_DB_PATH` / `TESTING` priority order**: `sed -n '400,420p' shared/storage_config.py` (look for `get_database_url`)
- **Startup-migration skip gate**: `sed -n '1,30p' flowfile_core/flowfile_core/database/init_db.py`
- **`SKIP_WORKER_TESTS` wiring**: `grep -n "SKIP_WORKER_TESTS\|def flowfile_worker" flowfile_core/tests/conftest.py`
- **Docker fixture ports**: `grep -n "_PORT = int(os.environ.get" test_utils/*/fixtures.py`
- **Test-utils Poetry scripts**: `grep -n '^start_\|^stop_' pyproject.toml`
- **xfail inventory + live status** (re-run periodically — bugs get fixed and markers go stale silently, that's the whole point of §7):
  ```bash
  FLOWFILE_DB_PATH=/tmp/xfail_probe.db poetry run pytest \
    "flowfile_core/tests/flowfile/test_code_generator_edge_cases.py::TestBasicFilterOperators::test_in_operator_numeric" \
    "flowfile_core/tests/flowfile/test_code_generator_edge_cases.py::TestUniqueOperationVariations::test_unique_without_columns" \
    "flowfile_core/tests/flowfile/test_code_generator_edge_cases.py::TestGroupByEdgeCases::test_groupby_with_concat_aggregation" \
    "flowfile_core/tests/flowfile/node_designer/test_node_designer.py::TestNumericStringAliasBug" \
    -q -p no:cacheprovider -rX
  ```
- **Collect counts** (~5,079 core / ~311 worker / ~620 frame / ~13 scheduler as of 2026-07-03): `poetry run pytest flowfile_core/tests --collect-only -q | tail -3` (repeat per package)
- **CI job list, concurrency block, matrix versions**: `sed -n '1,120p' .github/workflows/test.yaml` and `grep -n "python-version:" .github/workflows/test.yaml`
- **Coverage job env/steps**: `sed -n '216,296p' .github/workflows/test.yaml`
- **56-min → ~28-31-min history and xdist deferral**: no single file encodes this — cross-check against `.github/workflows/test.yaml` coverage-job comments (`sed -n '216,220p'`) which corroborate the sysmon rationale; the xdist-deferral reasoning is institutional knowledge captured here, verify by searching `git log --oneline --all -- .github/workflows/test.yaml` for the speed-fix commit if it needs re-confirming.
- **Required branch-protection checks / whether `test-summary` gates merges**: out of this skill's scope — verify via `gh api repos/<org>/Flowfile/branches/main/protection` if needed for a CI-mechanics task.
