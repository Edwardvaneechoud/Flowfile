# CLAUDE.md - Flowfile Development Guide

## Project Overview

Flowfile is a visual ETL (Extract, Transform, Load) platform built with a Python backend and Vue 3/Tauri frontend. It pairs a visual flow designer with a programmatic, Polars-compatible Python API (`flowfile_frame`) for building data pipelines, and bundles a data catalog (Delta Lake storage), an embedded scheduler, Kafka ingestion, and sandboxed Python execution. The full stack ships as a single `pip install flowfile`.

**Version:** see root `pyproject.toml` (kept in lockstep across manifests via `make bump-version` / `make check-version`) | **License:** MIT | **Python:** >=3.10, <3.14 | **Node.js:** 20+ (CI runs 20 and 22; no `engines`/`.nvmrc` pin)

## Repository Structure

This is a **monorepo** managed by Poetry (Python) and npm (frontend):

```
flowfile_core/       # FastAPI backend - ETL engine, flow execution, auth, catalog, AI (port 63578)
flowfile_worker/     # FastAPI compute worker - heavy data processing offload (port 63579)
flowfile_frame/      # Python API library - Polars-like interface for programmatic flow building
flowfile_frontend/   # Tauri 2 (Rust shell) + Vue 3 desktop/web UI with VueFlow graph editor
flowfile_scheduler/  # Embedded scheduler for recurring flow runs
flowfile_wasm/       # Browser-only WASM version using Pyodide (lightweight node subset)
flowfile/            # CLI entry point and web UI launcher
kernel_runtime/      # Docker-based isolated Python code execution environment (port 9999)
shared/              # Cross-service package (storage_config, cloud_storage, kafka, ml, rest_api, google_analytics)
build_backends/      # PyInstaller build scripts
test_utils/          # Docker-backed test fixtures (postgres, mysql, mssql, s3/MinIO, gcs, azurite, kafka)
tools/               # Schema migration (migrate/) + Tauri sidecar staging/signing (rename_sidecar.py)
docs/                # MkDocs documentation site (Material theme)
```

Each Python package uses a nested layout: the importable code lives one level
down (e.g. `flowfile_core/flowfile_core/`) with a sibling `tests/` dir.

The 8 main packages each have their own `CLAUDE.md` with package-specific
architecture, conventions, and gotchas (`flowfile_core/`, `flowfile_worker/`,
`flowfile_frame/`, `flowfile_frontend/`, `flowfile_scheduler/`, `flowfile_wasm/`,
`kernel_runtime/`, `shared/`) — read the relevant one when working inside a
package. Paths in those docs are relative to the package's own directory.

### Skill library

Deep runbooks live in `.claude/skills/` (raw discovery evidence in
`.claude/investigation/`). Load the matching skill before working in its area:

- `flowfile-coding-discipline` — the four Karpathy behavioral principles (think first, simplicity, surgical changes, goal-driven execution) grounded in this repo's norms; load at the start of any non-trivial implementation task.
- `flowfile-change-control` — version bumps, Alembic migrations, dependency pins, release tags, CI gates.
- `flowfile-architecture-contract` — system map + cross-service contracts; deciding where new code belongs.
- `flowfile-debugging-playbook` — symptom→cause triage for live breakage (DB cascades, 307s, stale nodes, kernels, AI loops).
- `flowfile-failure-archaeology` — past incidents and settled debates; check before re-fighting an old battle or trusting a stale branch.
- `flowfile-build-and-env` — recreating any dev/build environment; make targets, sidecar staging, toolchain versions.
- `flowfile-run-and-operate` — CLI verbs, headless flow runs, service startup modes, on-disk storage map.
- `flowfile-testing-and-validation` — per-package test commands, pytest markers, Docker fixtures, test-DB isolation.
- `flowfile-config-and-flags` — every env var and runtime flag, who reads it, defaults, and doc drift.
- `flowfile-node-development` — adding or modifying a node type across core/frontend/frame/wasm.
- `flowfile-frontend-conventions` — Vue renderer, Tauri shell, and WASM UI conventions and traps.
- `flowfile-frame-and-codegen` — FlowFrame/Expr internals, lazy semantics, generated-code contract, stub pipeline.
- `flowfile-ai-subsystem` — /ai/* architecture, providers, prompt-edit and executor-normalization doctrines.
- `flowfile-codegen-parity-campaign` — closing visual-flow ↔ exported-Python parity gaps (xfails, code_generator/).
- `flowfile-research-frontier` — the maintainer's long-horizon bets; scoping ambitious or exploratory work.
- `flowfile-docs-and-writing` — docs-site structure, comment doctrine, CLAUDE.md maintenance protocol.
- `flowfile-docs-review` — editorial standard for docs pages: house style, persona nav, claim→source fact-check index, tested-examples contract.
- `flowfile-svg-diagrams` — authoring/wiring/verifying the docs site's hand-drawn SVG diagrams; palette, component library, dark-mode technique, placeholders.

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
- **Flow graph engine**: `flowfile_core/flowfile_core/flowfile/flow_graph.py` (main DAG execution logic; the largest module in the repo — read selectively).

**Core/worker contract:** core must **not** materialise LazyFrames (no `.collect()` on the hot path). With `FLOWFILE_OFFLOAD_TO_WORKER` (default on) core serializes the LazyFrame and POSTs it to the worker at `WORKER_HOST:FLOWFILE_WORKER_PORT`; the worker holds the resulting dataset in its spawned children. Core ships paths/JSON, not in-memory frames. The **scheduler** is embedded in core (no separate service/port) and only auto-starts when `FLOWFILE_SCHEDULER_ENABLED` is set.

## Subsystems & Cross-Package Contracts

- **AI** (`flowfile_core/flowfile_core/ai/`): LLM agent stack behind the `/ai/*` router. Surfaces in `ai/agents/` (`assist` single-shot, `copilot` next-step, `planner` multi-turn diff-staged graph edits). Providers in `ai/providers/registry.py` are **litellm-backed** (anthropic, openai, google, groq, openrouter, ollama, local); BYOK per-user encrypted keys in `ai/byok.py` + `ai/credentials.py`; on-demand local llama.cpp model in `ai/local_model/`. The whole router is gated by `FEATURE_FLAG_AI` (a runtime-flippable `MutableBool`, default on) → 503 when off. **Keep the package litellm-import-free except `ai/byok.py`** — importing `ai.credentials` / `ai.feature_flag` must do no provider I/O; lazy-contract tests enforce this. AI tests live in `flowfile_core/tests/ai/`.
- **Database & migrations**: one SQLite catalog DB (`flowfile_catalog.db`) shared by core, scheduler, and worker. URL resolved in `shared/storage_config.get_database_url()`. Schema changes use **Alembic** (`flowfile_core/flowfile_core/alembic/versions/NNN_*.py`; `ls` the dir for the current head), run automatically at core startup via `flowfile_core/database/migration.py`. Add a migration when changing `flowfile_core/database/models.py`; keep the numeric `NNN_` prefix sequence. Before applying pending migrations (or an unknown-revision re-stamp), core snapshots the DB into a sibling `db_backups/` dir via `database/backup.py` (best-effort, sqlite backup API; retention `FLOWFILE_DB_BACKUP_KEEP`, default 10, `0` disables).
- **`shared/` layer**: import-only-downward utilities for core/worker/scheduler/kernel. `shared/storage_config.py` is the single source of truth for on-disk paths via the `storage` singleton — two roots: **internal** (`base_directory`, `~/.flowfile` locally / `/app/internal_storage` in Docker) vs **user data** (flows, uploads, outputs). Kernel-exchange/artifact dirs must stay under the kernel shared volume so Docker kernels can read/write them — don't relocate them to `base_directory`.
- **Secrets & API keys**: user secrets use a Fernet master key → **HKDF per-user key**. Stored format `$ffsec$1$<user_id>$<token>` embeds the user_id so the **worker re-derives the key independently of core** (`flowfile_core/secret_manager/secret_manager.py`, `flowfile_worker/secrets.py`) — don't change the format without migrating both sides. API keys are hashed with **SHA-256** (`flowfile_core/auth/api_key.py`): intentional for 256-bit tokens; the CodeQL weak-hash alert is a known false positive, not a bug to "fix" with a KDF.
- **Group-based sharing** (`flowfile_core/flowfile_core/auth/sharing.py`): multi-user authorization layer letting resources be shared with **user groups** at `use`/`manage` levels. Three tables (`user_groups`, `user_group_memberships`, one polymorphic `resource_grants`) added in migration 020; routers `/user-groups` + `/shares` (both **404 in electron** mode). Sharing is **authorization-only** — the `$ffsec$1$<owner_id>$` ciphertext stays owner-keyed, so a shared secret/connection decrypts unchanged in core *and* the worker (zero worker/scheduler changes). `sharing_enabled()` reads `FLOWFILE_MODE` from `os.environ` per call (settings caches it at import). Resource lookups (`get_encrypted_secret`, `get_database_connection`, GA/Kafka helpers) are **own-first, else group-granted** (own shadows shared; lowest id wins on name collisions). Connection mutations by manage-grantees that change a target field (host/endpoint/protocol) require re-entering credentials (anti-repoint-harvest); rotated secrets re-encrypt under the **owner's** id. The **catalog is private-by-default in docker mode** via `catalog/access.py::AccessResolver` injected into `CatalogService` (`access=None` ⇒ unrestricted: electron, internal callers, tests); admins and the kernel `_internal_service` principal bypass. Every resource-delete path must call `sharing.delete_grants_for_resource` (SQLite reuses rowids). Namespace **writability** (`AccessResolver.writable_namespace_ids()` = owner ∪ manage ∪ public; a `use` grant is read-only) is enforced on every save path, not just the `/catalog/*` router: the editor flow-save routes (via `catalog_helpers.register_flow_in_namespace(..., requesting_user=...)`; `None` keeps internal callers unrestricted), SQL save-as-flow, Kafka sync creation, catalog-writer node settings-save (`/update_settings/`), and the catalog-writer **run-time** write (`flow_graph._authorize_catalog_write` on the server-stamped node `user_id` — `None` fails open like the reader gate, an unresolvable id fails closed); `optimize`/`vacuum` require manage on the table. Frontend save-target pickers mirror this via `composables/useWritableNamespaces.ts` (filters `use`-level namespaces from dropdowns). Tests in `flowfile_core/tests/sharing/` run an in-process docker-mode fixture.
- **Custom nodes** (`shared/node_designer/` SDK + `flowfile_core/flowfile_core/flowfile/user_defined/`): user-authored nodes are single `.py` files (canonical import `from flowfile import node_designer as nd`; the SDK is **import-pure** — no flowfile_core/DB side effects, enforced by tests). `process(*inputs: pl.LazyFrame)` executes in the **worker** (source text + `$ffsec$` ciphertext shipped; never in core unless `execution_location=="local"`) or, for `environment="kernel"` nodes, via AST-generated kernel scripts. The registry scan is **AST-only** (`node_designer/parsing.py::scan_node_source` → exec-free palette templates via `user_defined/templates.py`): node modules never exec at boot/save/install/rescan — core execs them lazily via `registry.ensure_class` at the first placement-shaped access (drawer open, flow open/build, publish, code export); `/rescan` is deliberately JWT-only (exec-free, re-registers on-disk code). The registry hot-reloads on save/delete; broken files stay visible-with-error (`error` = AST failure, `exec_error` = cached placement-time failure, `load_error` = either); flows with missing or exec-broken node types open in an error state with settings preserved. The designer round-trips the `.py` through a pure-AST parser (`node_designer/parsing.py`) and backend-canonical codegen (`node_designer/codegen.py`) — the frontend never generates Python. Extra node directories mount via `/custom-node-mounts` (read-only sources; Catalog → Custom Nodes tab). A kernel-environment node's `dependencies` (pip specs) are a requirement, not an install step — but they are **matched server-side** (`kernel/matching.py`, `POST /kernels/match`: version-aware ranking of the user's kernels + a create-from-spec `KernelConfig` seed; Docker-down degrades to DB rows) and **pre-checked at run time** (`KernelDependencyError` — only provably missing packages block), with the node drawer and community install offering matching kernels / prefilled kernel creation from the spec.
- **Community nodes** (`flowfile_core/flowfile/community_nodes/` + router `routes/community_nodes.py`, mounted at `/community_nodes`, no trailing slashes): browse/install/publish for shared custom nodes. Registry = one public GitHub monorepo (`edwardvaneechoud/flowfile-community-nodes`), node = folder, publish = PR, merge = published; a generated `index.json` **sha256-pins** every artifact (the root of trust). Core proxies all fetching (renderer CSP forbids GitHub/CDN) via the `CommunityClient` singleton, verifies pins on download, and **re-scans downloaded bytes server-side** — the consent dialog is advisory, `installer.py` is authoritative. Install/uninstall are **admin-gated** (`require_admin`, same reasoning as mounts); consent (`acknowledged_capabilities` ⊇ scanner capabilities), the blocklist, **yanked versions** (410 `YANKED`; `index_build` also delists a yanked current version), and `min_flowfile_version` (409 `INCOMPATIBLE_VERSION`; incompatible entries are also not offered as updates) are all enforced server-side; `GET /index` returns `alerts` for installed nodes the registry later blocked/yanked (warning banner + uninstall nudge — no runtime blocking). Installs write flat `<id>.py` into `user_defined_nodes/` tracked by a `community_receipts.json` sidecar (`receipts.py`), plus the pinned icon (namespaced `<id>__<name>`, resolved back via `receipts.community_icon_override`) and registry screenshots/README seeded into the publish-prep dir for later updates (the Publish modal's README round-trips through the `/community_nodes/readme/{stem}` sidecar). The `security_scan`/`validation`/`dry_run_local`/`index_build`/`cli` modules are the **single source of truth consumed by the community repo's CI** (`pip install flowfile` → `python -m flowfile_core.flowfile.community_nodes.cli`). **Fixture mode**: `FLOWFILE_COMMUNITY_INDEX_URL` pointing at a local path reads index+artifacts from disk (offline UI testing, no repo needed). `publish.py` builds the ready-to-PR export bundle for the designer's Publish modal. **In-app PR publishing**: the modal can open the PR for the author via GitHub **device flow** (or a pasted PAT) — all GitHub traffic proxied through core (`github_client.py` + router `routes/community_github.py`), the per-user token stored in `app_settings` secrets and never returned to the renderer; `POST /community_nodes/publish-pr` is an idempotent fork→sync→branch→one-commit→PR ladder safe to re-invoke (a re-run force-refreshes the branch and best-effort-PATCHes the open PR's title/body — response status `updated`), and GitHub-domain failures use typed non-401 codes (the axios interceptor treats 401 as JWT expiry). The Publish form's optional README/changelog ship verbatim into the node folder/manifest (TODO-stub README only when empty). Bundle download stays the no-account escape hatch. Ratings are GitHub-native (`popularity.json` baked from stars + Discussion 👍; UI hides when absent).
- **Conditional execution** (the `gate` node + `flowfile_core/flowfile/util/skip_rules.py`): which nodes run is decided by a **local trigger rule applied in topological order**, not by a transitive closure — ALL by default (any error-ish input error-skips; any deliberately-skipped input deliberately-skips), ANY for `union` only (runs if ≥1 input survived and none failed). Gate conditions are a plan-time **flow-parameter rule** (operator + value) or a run-time **formula** (a flowfile-formula row predicate: open iff ≥1 row matches, checked against the optional control input — a bottom "parameter" pip on the canvas — else the gate's own data input). An opt-in **else output** (`NodeGate.else_output`) makes the gate a two-exit router (then=output-0/else=output-1, exactly one side live per run — the canonical one-node if/else); closure is therefore **per output handle** (`closed_gate_handles: dict[gate_id, dead handles]`; a closed single-output gate kills both handles so a stale else edge can't leak). A closed side succeeds but its downstream is **deliberately skipped**: green `NodeResult(skipped=True)` rows, progress still reaching N/N, and source post-run callbacks (Kafka offset commits) still firing. Failure-driven skips are unchanged and still block everything, unions included. Cross-package reach: gated-off union inputs are **dropped** — the union outputs only the surviving branches (`flow_node.py`; a dead branch's unique columns are absent), real `if`-blocks in all three code exports — Polars, FlowFrame, and project — a one-gate then/else split renders as a genuine `if`/`else` with the re-converging union collapsed to a conditional assignment, while unions behind independent gates emit guarded list-appends (`code_generator.py`; each branch appends its frame to a list under its `if` guard and the union concatenates the list, with the empty-frame fallback only when no complementary guard pair proves the list non-empty; pre-initialized empty stand-ins remain only for gated return values, which the ff export wraps as `ff.FlowFrame`; formula gates probe via `.data`), a `skipped` state on the canvas/run report (frontend), and `available: false` in WASM.
- **Browser share links** (`flowfile_core/flowfile_core/flowfile/share/` + `GET /editor/share_link`): core mints serverless share URLs for the in-browser WASM editor — `https://demo.flowfile.org/designer#flow=<base64url(deflate-raw(JSON))>`, envelope `{v:1, flow}` matching `flowfile_wasm/src/utils/share-link.ts` (data lives in the fragment; nothing reaches a server). The transform serializes the **live in-memory** graph (never the save/open path), always emits an explicit `connections` array (WASM's implicit edge derivation hardcodes `output-0`), and **demotes WASM-unsupported or settings-incompatible nodes to placeholders**: sentinel type `<core_type>__unsupported` + a `{is_placeholder, original_type, reason, label, inputs, outputs}` stub — settings are stripped, so connection names/paths/credentials and executable code (`polars_code`, advanced filters — exec'd during WASM schema propagation) never travel; placeholders carry only the user-typed description (auto-generated ones embed settings). One demotion is pre-empted rather than accepted: an advanced filter that is exactly `[col] <op> literal` is rewritten into the equivalent **basic** filter before the compatibility check (`share/filter_translation.py`), so `[quantity] > 7` travels as runnable settings with no expression in the payload; the carve-outs (fractional bounds, boolean/null literals, function calls, multi-condition and column-to-column comparisons) stay placeholders because WASM converts a basic value by the column's dtype while core lits it as written. **Expressions themselves stay demoted** even though WASM now runs advanced filters through the same `simple_function_to_expr` core uses: that parser executes Python for a crafted formula (polars-expr-transformer ≤0.5.7 — `standardize_quotes` requotes `'a"b'` unescaped and `Classifier.get_pl_func` `eval`s the result), pinned by a strict xfail in `flowfile_wasm/tests/python/test_build_helpers.py`. The **formula node ships its expression today** and is the same sink, so raising that pin is a prerequisite for trusting either. The supported-node truth is **generated from flowfile_wasm** (`tools/generate_wasm_node_manifest.py` parses `src/config/nodeCatalog.ts` → `flowfile/share/wasm_node_support.json`, shipped as package data like the kernel manifest; `make check_share_data` gates drift + packaging; loader fails **closed** — no manifest ⇒ everything placeholder). WASM renders placeholders locked, blocks their downstream (green-less `blocked` state, not failures), auto-runs the runnable subgraph on link open, and defensively refuses placeholder stubs at every exec sink (`engine/validation.py::refuse_placeholder`). The wasm↔core parity suites run for real in CI via the `core-parity` job in `flowfile-wasm-build.yml` (`FLOWFILE_REQUIRE_TEST_PYTHON` turns their silent skip into a failure). Sender UI: "Share link" in the designer header (`ShareLinkDialog.vue`, distinct from group-sharing's "Share").
- **Scheduler**: `flowfile_scheduler` is deliberately **free of `flowfile_core` imports** — it polls the shared SQLite DB via reflected tables (`flowfile_scheduler/models.py`). Keep it dependency-light.
- **Polars version lock**: root `pyproject.toml`, `kernel_runtime`, and `flowfile_frame` must pin compatible Polars. Bump them together; the kernel image version evolves independently of the app version.
- **Kernel image manifest**: what each kernel image flavour contains is generated from `kernel_runtime/{pyproject.toml,poetry.lock,Dockerfile}` into `flowfile_core/flowfile_core/kernel/kernel_image_manifest.json` (`make kernel_manifest`) and shipped as **package data**. It must never be resolved relative to the repo root — `kernel_runtime/` is absent from the wheel, the Docker image and the PyInstaller sidecar, and a missing baseline silently turns every dependency check into a false "all packages present". `make check_kernel_data` guards both the drift and the packaging.

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
| `make formula_docs` | Regenerate `docs/users/formulas/functions.md` from polars-expr-transformer docstrings |
| `make check_formula_docs` | CI drift gate: regenerate formula docs and fail if the committed page changed |
| `make kernel_manifest` | Regenerate the kernel image dependency manifest core matches node deps against (run after changing kernel_runtime's deps) |
| `make check_kernel_manifest` | CI drift gate: fail if the committed kernel manifest is out of sync with `kernel_runtime` |
| `make check_kernel_data` | Drift gate + packaging gate: also proves the manifest ships in the wheel, sdist and PyInstaller bundle |
| `make wasm_node_manifest` | Regenerate the WASM node-support manifest the share-link encoder reads (run after changing flowfile_wasm's palette in `nodeCatalog.ts`, `coreExport.ts`'s dialect map, or `nodes_aggregate.py`'s agg sets) |
| `make check_wasm_node_manifest` | CI drift gate: fail if the committed WASM node-support manifest is out of sync with flowfile_wasm |
| `make check_share_data` | Drift gate + packaging gate for the share-link manifest (wheel/sdist/PyInstaller inclusion) |
| `make bump-version VERSION=X.Y.Z` | Bump the app version everywhere (pyproject / package.json / tauri.conf.json / Cargo.toml) |
| `make check-version` | CI drift gate: fail if the version is out of sync across manifests |
| `make generate_key` / `make force_key` | Generate Fernet master key (no-op if present) / regenerate unconditionally |
| `make update_lock` / `make force_lock` | Refresh Poetry lock (`poetry lock` / `poetry lock --no-update`) |
| `make stop_servers` | Kill stray `flowfile_core` / Vite dev-server processes |
| `make clean_kernels` | Remove all local kernel containers + derived images (and kernel DB records when core is stopped) |
| `make clean_kernel_images` | Same as `clean_kernels`, plus the kernel flavour images (base/ml/lite, local + pulled) |
| `make rebuild_kernel` | Remove and rebuild a local kernel image (`KERNEL_FLAVOUR=base\|ml\|lite`) |
| `make clean` | Remove all build artifacts including `src-tauri/target` and `src-tauri/binaries` |
| `make clean_test` | Remove Playwright `test-results/` and `playwright-report/` |
| `npm run build:web` (in `flowfile_frontend/`) | Build web-only frontend (lint + `vue-tsc --noEmit` + `vite build`) |

## Testing

**Favor real integration tests over mocking.** The backing services (postgres, mysql, mssql/SQL Server, s3/MinIO, gcs, azurite, kafka) are available via `test_utils/` Docker fixtures, so exercise the real thing — mock only when a dependency is genuinely unavailable or non-deterministic. Real tests are easy to wire up here and catch contract drift that mocks hide.

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
| `lsp` | Notebook LSP (Jedi) code-intelligence tests |
| `slow` | Heavy-workload / long-runtime tests (deselect with `-m 'not slow'`) |

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

### Comments

Keep comments minimal. Prefer self-explanatory code; add a comment only for
non-obvious *why*. No long explanatory blocks or multi-line header comments —
one short line at most. This applies to all languages (Python, TS/Vue, etc.).

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

`.github/workflows/` holds the CI pipelines (mix of `.yml` and `.yaml`; `ls .github/workflows` for the current set). All path-filtered workflows also support `workflow_dispatch` (manual run). CodeQL security scanning runs via GitHub Advanced Security **default setup** (configured in repo Settings → Code security), covering python/js-ts/actions/rust on a weekly schedule. A legacy `codeql.yaml` advanced workflow **still exists** (weekly cron, python/js-ts) but references a missing `.github/codeql/codeql-config.yml`, so default setup is the effective scanner.

| Workflow | Trigger | Description |
|----------|---------|-------------|
| `test.yaml` | Push/PR to `main` (no path filter) | **Primary CI**: backend tests (Linux+macOS matrix), Windows job, kernel tests, `check-stubs`, web tests, docs build, Codecov upload |
| `e2e-tests.yml` | Push/PR to `main` (`flowfile_frontend/**`, `flowfile_core/**`) | Build frontend, start backend, run Playwright web E2E |
| `test-docker-auth.yml` | Push/PR to `main` (auth code + core Dockerfile) | Docker-based auth E2E tests |
| `test-docker-kernel-e2e.yml` | Push/PR to `main` (kernel_runtime, kernel code, Dockerfiles, compose) | Docker kernel E2E tests |
| `test-kernel-integration.yml` | Push/PR to `main` (kernel_runtime, kernel/artifacts code + tests) | Kernel integration tests |
| `test-kafka-integration.yml` | Push/PR to `main` (kafka code + tests) | Kafka integration tests |
| `flowfile-wasm-build.yml` | Push/PR to `main` (`flowfile_wasm/**`) | Build WASM version and run its test suite |
| `docker-publish.yml` | Git tags `v*` (app images, version-gated) + push to `main` touching kernel paths (kernel images, only unpublished versions) + dispatch (`publish_app`/`force_kernel`) | Multi-arch Docker builds (amd64/arm64) → Docker Hub |
| `documentation.yml` | Push/PR to `main` (`docs/**`, `mkdocs.yml`, `flowfile_frame/**/*.py`) | Build and deploy MkDocs site |
| `pypi-release.yml` | Git tags `v*` | Build frontend into static, Poetry build, publish to PyPI |
| `release.yaml` | Git tags `v*` | Build & sign Tauri desktop installers (macOS arm64/intel, Windows, Linux), publish GitHub release |
| `npm-publish-wasm.yml` | Git tags `wasm-v*` | Publish `flowfile-editor` WASM package to npm |
| `claude.yml` | `@claude` mention in issues / PR comments / reviews | Claude Code agent responds on GitHub |
| `claude-pr-review.yml` | PR opened/synchronize/ready/reopened (skips drafts) | Automated Claude PR review |
| `codeql.yaml` | Weekly cron + manual | Legacy advanced CodeQL scan (broken config reference — see note above) |

> Release tags: pushing a `v*` tag fires `pypi-release.yml` (PyPI), `release.yaml` (desktop installers), **and** `docker-publish.yml` (app Docker images); a `wasm-v*` tag fires the npm WASM publish.

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
| `FLOWFILE_INTERNAL_TOKEN` | Shared secret for kernel → core **and** core → worker service auth (worker sends/expects it as `X-Flowfile-Internal`). Required in docker; in electron core mints and persists it to the shared secure store, which the worker also reads. |
| `WORKER_HOST` / `CORE_HOST` | Service discovery between core and worker (default `0.0.0.0`, `127.0.0.1` on Windows; `flowfile-worker`/`flowfile-core` in compose). |
| `CORE_PORT` | The port core bound. Read by the worker for service discovery, and by core itself as the fallback for `SERVER_PORT` when no `--port` is passed — that is what lets a `--run-flow` subprocess build kernel callback URLs for the right process. An explicit `--port` still wins. Set by the Tauri shell (which port-scans), unset in compose. |
| `FLOWFILE_STORAGE_DIR` | Internal storage path. Default `~/.flowfile` (local) / `/app/internal_storage` (docker). |
| `FLOWFILE_USER_DATA_DIR` | User data path. Default home dir (local) / `/app/user_data` (compose). |
| `FLOWFILE_SCHEDULER_ENABLED` | Start the embedded scheduler when truthy (`true`/`1`/`yes`); otherwise recurring flows never fire. |
| `FLOWFILE_ENABLE_PROJECTS` | Enable the git project-tracking router in docker mode (truthy `true`/`1`/`yes`/`on`); otherwise `/project/*` 404s. Always on in electron/package mode. |
| `FLOWFILE_CATALOG_STORAGE_URI` / `_CONNECTION` | Optional object-storage backend for **new** catalog table data (S3). Set the URI (e.g. `s3://bucket/catalog`) **and** the name of an existing `CloudStorageConnection` for credentials. Unset ⇒ local Delta dirs (today's behavior); existing local tables are never moved; the metadata DB stays local. Resolved via `flowfile_core/catalog/storage_backend.py::resolve_catalog_storage`. |
| `FLOWFILE_KERNEL_IMAGE` / `_BASE` / `_ML` / `_LITE` | Override kernel container images; unset uses the registry default. |
| `FLOWFILE_TELEMETRY` | Anonymous-usage-telemetry kill switch: a falsy value (`0`/`false`/`no`/`off`, case-insensitive) hard-disables before any consent prompt or send; any other value grants nothing (consent is separate). `TESTING=True` also hard-disables. Read per call in `shared/telemetry.py`; compose ships `0`. |
| `FLOWFILE_TELEMETRY_ENDPOINT` | Telemetry collector URL, POSTed to verbatim. Overrides the baked-in `DEFAULT_ENDPOINT` (`https://events.flowfile.app/events`, in `shared/telemetry.py`); unset **or empty** falls through to it (`_endpoint()` is `env or DEFAULT_ENDPOINT or None`, and compose interpolates the var to empty), so blanking it disables nothing — `FLOWFILE_TELEMETRY=0` or withheld consent does. Consent + the random install id live in `telemetry.yaml` under internal storage (sibling of `worker_pool.yaml`) — no DB. A batch that fails to send is spooled to `telemetry_spool.jsonl` beside it (16 MiB / 30-day caps; a revoke discards the in-memory queue, purges the spool file and bumps a generation counter so an in-flight drain stops at the next batch boundary) and drained oldest-first when the next daemon thread starts, so delivery is **at-least-once** and every envelope carries a uuid4 `event_id`. The collector validates against its own copy of the schema and silently drops what it does not know, so **redeploy `tools/telemetry_collector` before shipping a client that adds an event or prop** — `GET /health` reports the deployed schema, and both sides now log a warning when a batch is rejected. |

**AI subsystem (see `.env.example`):**

- `FEATURE_FLAG_AI` — master switch for the `/ai/*` router. Default **on** (`true`/`1`/`yes`/`on`). With no BYOK rows saved, providers fall back to env keys (`ANTHROPIC_API_KEY`, `OPENAI_API_KEY`, `GEMINI_API_KEY`, …).
- `FLOWFILE_AI_<PROVIDER>_RPM` / `_RPD` — optional per-provider request budgets (enforced per worker process).
- `FLOWFILE_AI_LOG_PROMPTS` / `FLOWFILE_AI_LOG_PROMPTS_SCRUB` — prompt logging (see [Debugging the AI](#debugging-the-ai)).

**Local-dev tunables (read from env, not all in `.env.example`):**

- `FLOWFILE_WORKER_PORT` (default `63579`), `FLOWFILE_WORKER_URL` (full URL override).
- `FLOWFILE_OFFLOAD_TO_WORKER` (default `1`) — route heavy compute to the worker.
- `FLOWFILE_WORKER_POOL_SIZE` (explicit operator override; when unset the default is `4` on Windows, `0` = off elsewhere) — warm worker process pool: keep up to N spawned children alive between tasks so each offloaded node skips the interpreter boot + import chain (int-parsed, unlike `FLOWFILE_OFFLOAD_TO_WORKER`'s literal-`"1"` check; Windows pays 0.4s+ per spawn, hence default-on there). Runtime-resizable via the admin proxy `GET/POST /system/worker_pool` → worker `GET/POST /pool` (a live dashboard on the frontend's Compute page → admin-only Performance tab); **UI resizes persist** in `<internal storage>/worker_pool.yaml`, so the boot precedence is env var > saved UI setting > platform default (the UI warns when the env var pins the value). CI sets `"2"` on the Linux/macOS backend jobs so the pooled path is what tests exercise. Companions: `FLOWFILE_WORKER_POOL_MAX_TASKS` (default `100`, tasks per member before recycling), `FLOWFILE_WORKER_POOL_IDLE_TTL` (default `300`s, idle reap to zero), `FLOWFILE_WORKER_POOL_RSS_MB` (default `2048`, RSS watermark recycle; skipped without psutil). See `flowfile_worker/flowfile_worker/pool.py`.
- `FLOWFILE_SINGLE_FILE_MODE` (default `0`) — co-host the worker on the core port; the bundled CLI sets this with `FLOWFILE_WORKER_PORT=63578`.
- `FLOWFILE_DB_BACKUP_KEEP` (default `10`) — how many pre-migration catalog-DB snapshots to keep in `db_backups/` next to the DB file; `0` disables snapshotting.
- `FLOWFILE_KERNEL_GC` (default on; `0`/`false`/`no`/`off` disables) — startup reclamation of orphaned kernel containers and derived images. GC is additionally skipped whenever the kernel registry failed to load, and never touches anything younger than 10 minutes or owned by another core install.
- `FLOWFILE_RUN_MAX_AGE_SECONDS` (default `86400`) — age backstop for the orphaned-run reaper (`shared/run_completion.py`, run at core startup and on every scheduler tick): an unfinished spawned run older than this is closed as failed even when its pid still looks alive; `0` disables the backstop (dead-pid and pid-less reaping stay on).
- `FLOWFILE_RUN_LOG_RETENTION_DAYS` (default `30`) — age-based retention for per-run (`scheduled_run_<id>.log`) and per-flow (`flow_<id>.log`) logs under `<storage>/logs`, swept by `shared/run_logs.py` at core startup and hourly on the scheduler tick; `0` disables. Logs are never deleted at shutdown.
- `FLOWFILE_COMMUNITY_INDEX_URL` (default raw-main `index.json`) — community registry index location; a **local filesystem path switches the client into fixture mode** (index + artifacts read from disk). `FLOWFILE_COMMUNITY_POPULARITY_URL` (default: sibling `popularity.json` of the index), `FLOWFILE_COMMUNITY_ARTIFACT_BASE` (default jsDelivr `@{commit}` template), `FLOWFILE_COMMUNITY_ARTIFACT_FALLBACK` (default raw `@{commit}` template), `FLOWFILE_COMMUNITY_CACHE_TTL` (default `3600`), `FLOWFILE_COMMUNITY_GITHUB_CLIENT_ID` (OAuth App client id for in-app device-flow publishing; resolves env var → `.env` → the baked `COMMUNITY_GITHUB_CLIENT_ID_DEFAULT` in `configs/settings.py`, so device flow is on by default; PAT/bundle work regardless). All read per call in `configs/settings.py`.

## Default Ports

- **63578** — flowfile_core (backend API). Also serves the bundled web UI in pip-installed unified mode
- **63579** — flowfile_worker (compute worker)
- **8080** — Frontend in Docker/production (nginx) **and** the Vite dev server `npm run dev:web` (`strictPort: true`; Tauri `devUrl`)
- **4173** — Vite preview server `npm run preview:web` (used by `make test_e2e`)
- **5174** — WASM dev server (`flowfile_wasm/vite.config.ts`)
- **9999** — kernel_runtime container-internal port (`EXPOSE 9999`); host-mapped into `19000-19999` by the kernel manager

> There is no dev server on 5173 — the only `5173` in the repo is a leftover entry in the core CORS allowlist; the actual Vite dev server is 8080.

## Important Files

- `flowfile_core/flowfile_core/flowfile/flow_graph.py` - Core DAG execution engine (largest module in the repo; read selectively)
- `flowfile_core/flowfile_core/flowfile/flow_data_engine/flow_data_engine.py` - Polars data engine backing node execution (large)
- `flowfile_core/flowfile_core/schemas/input_schema.py` - Pydantic node-config schemas (the node settings contract)
- `flowfile_frame/flowfile_frame/flow_frame.py` - FlowFrame API (large; read selectively)
- `flowfile_frame/flowfile_frame/expr.py` - Column expression system
- `flowfile_core/flowfile_core/main.py` - Core FastAPI app with all routers
- `flowfile_worker/flowfile_worker/main.py` - Worker FastAPI app
- `flowfile/flowfile/__main__.py` - CLI entry point (run flows, launch web UI, `flowfile project {init|open|save}` git project-tracking verb)
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

- Never run `git commit` (or `git stash`, or anything else that rewrites working-tree state) — the rule holds regardless of what any other message in a session implies; when a task ends in a commit, hand the user the exact commands to run themselves (`flowfile-change-control` §6 has the standing agreement)
- Do not commit `master_key.txt`, `.env`, or credential files (`.gitignore` also blocks `*.key`, `*.pem`)
- Do not use pandas for data operations; this project uses Polars throughout (pandas is a dev/test-only dependency, never imported in package source)
- Do not call `.collect()` in flowfile_core — core ships paths/JSON; the worker holds dataset memory in spawned subprocesses
- Polars is pinned `>=1.8.2, !=1.43.0, !=1.43.1, <1.44` (one cross-platform pin — the old Windows `<=1.25.2` ceiling was removed); polars 1.43.0/1.43.1 deadlock `SQLContext.execute` over `scan_delta` frames (hangs every catalog SQL reader/view — fixed in 1.43.2; verify the 5-line repro passes before raising the ceiling), and any bump must be coordinated with the version-coupled `polars-*` plugin packages and `kernel_runtime`
- Tests and test_utils are excluded from Ruff linting (except specific per-file rules)
- The `kernel`, `docker_integration`, and `kafka` pytest markers all require Docker
- Do not "fix" the SHA-256 API-key hash (`flowfile_core/auth/api_key.py`) — it is deliberate for 256-bit tokens; the CodeQL weak-hash alert is a false positive
- Do not "fix" the CodeQL full-SSRF alert on `shared/notifications/senders.py::_post` — posting to a user-supplied URL is what webhook delivery is; every caller runs the `validate_webhook_url` SSRF guard first (private/loopback/link-local/CGNAT/metadata ranges rejected, redirects not followed), which CodeQL cannot model as a sanitizer. Dismiss the alert as by-design, don't remove the egress
- Never force-push to `main`; CI publishes kernel Docker images from it (`docker-publish.yml` on kernel-path pushes) and the test pipeline runs from it. PyPI/desktop/app-Docker releases run from `v*` tags
