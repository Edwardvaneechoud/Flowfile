---
name: flowfile-architecture-contract
description: The system map and load-bearing design contracts of Flowfile's core/worker/frontend/kernel/scheduler/shared architecture — who talks to whom, why core never materializes LazyFrames, the worker offload wire protocol, the $ffsec$ secrets format, dual node-execution state, node-hash caching discipline, and the pending/vision directions for execution and catalog decentralization. Use when onboarding to the codebase, deciding which service a new code path belongs in, debugging cross-service contract breaks (secrets, worker offload, kernel containers, scheduler), or evaluating whether a design change would foreclose the maintainer's decentralization vision.
---

# Flowfile architecture contract

This skill is the map of the system and the *why* behind its cross-service
contracts. It does not teach you how to build a node, write a test, or run
the app — it teaches you where the load-bearing walls are so you don't put a
door through one by accident.

## When NOT to use this skill

- Adding/changing a node type (settings class, template, `add_<type>` method,
  frontend component) → `flowfile-node-development`.
- Running/building/deploying the stack, ports, Docker Compose → `flowfile-build-and-env` / `flowfile-run-and-operate`.
- Env vars and feature flags in detail (their full table, defaults, gotchas) → `flowfile-config-and-flags`.
- Writing or running tests → `flowfile-testing-and-validation`.
- The AI subsystem's internals (agents, providers, BYOK, prompt log) → `flowfile-ai-subsystem`.
- Frontend/Vue/Tauri conventions → `flowfile-frontend-conventions`.
- `flowfile_frame` Python API and code generation → `flowfile-frame-and-codegen`.
- Debugging a live symptom step-by-step → `flowfile-debugging-playbook` (this skill explains *why* the failure mode exists; that one walks you through diagnosing *this* incident).
- Known historical bugs and their fixes as a searchable archive → `flowfile-failure-archaeology`.
- Change-control process (tests, drift gates, review requirements) → `flowfile-change-control` — nothing here overrides it.

---

## 1. The system map

```
Frontend (Tauri desktop / Vite web, :8080) ──HTTP(Axios)──► flowfile_core (:63578)
  FastAPI: JWT auth, catalog, secrets, AI, DAG engine
      │ HTTP(query-plan bytes) + WebSocket(streaming)   │ Docker SDK
      ▼                                                 ▼
  flowfile_worker (:63579)                    kernel_runtime containers
    spawned subprocess per job                  uvicorn :9999 in-container,
    (dataset memory lives here,                 host-mapped to 19000-19999,
     never in the FastAPI process)              call back to core on :63578

flowfile_scheduler — polls the shared catalog DB via its own models; embedded in
  core's process when FLOWFILE_SCHEDULER_ENABLED is set, or run standalone.
shared/ — cross-package utilities (storage paths, crypto, cloud_storage, kafka,
  ml, rest_api) used by core/worker/scheduler/kernel_runtime. Import direction
  is strictly downward: shared never imports core/worker/scheduler/frame.
flowfile_frame — Python API; imports flowfile_core in-process to build FlowGraph
  objects directly. Not a separate service.
WASM frontend (Pyodide) — runs fully in-browser, no core/worker/kernel at all.
```

One SQLite catalog DB (`flowfile_catalog.db`) is the metadata source of truth,
resolved via `shared/storage_config.get_database_url()`. **Correction to root
CLAUDE.md:** the worker does **not** open this DB — verified, no
engine/session in `flowfile_worker/` (only `shared.sql_utils` for *user*
database connection URIs). Core has the full ORM (`database/models.py`);
scheduler and the CLI run-completion path use a lightweight declarative
mirror in `shared/models.py` (§5) — a hand-maintained copy, not core's own
tables and not SQLAlchemy reflection.

---

## 2. Load-bearing decision: core never materializes LazyFrames

**The rule:** `flowfile_core` must never call `.collect()` on a real dataset's
`pl.LazyFrame` on the hot path. Core ships *paths and query plans*; the
**worker holds dataset memory**, and only inside subprocesses it spawns
(`mp_context = get_context("spawn")` in `flowfile_worker/__init__.py`) —
never in the worker's own FastAPI process. That's *why* the worker exists as
a separate service at all: a crashed/OOM-killed job takes down a killable
child process, never the API that's holding open connections for every other
user.

**Enforcement status — read this before assuming a lint catches violations:**
it is a **convention, not a mechanically enforced rule**. No lint or test
forbids `.collect()` in core. Guardrails that do exist: bounded/preview
collects are corralled inside `FlowDataEngine` itself (`collect(n)` →
`head(n).collect(engine="streaming")`, `collect_schema()`, `pl.len()` for
counts); a load-bearing comment in `flow_node.py`'s `_do_execute_remote` warns
to use `is not None`, never truthiness, on a `FlowDataEngine` because
`__len__` collects; `NodeTemplate.laziness` metadata plus
`FlowNode.check_upstream_laziness` / `FlowGraph.check_flow_laziness`
(`GET /editor/laziness_check`) report eager nodes upstream of a catalog
*virtual table* — but that only gates one optimization path, not a general
rule. Accepted, deliberate exceptions where core **does** collect a full
frame: writing a Delta table for the catalog locally, the explore-data/Graphic
Walker preview path, seeding a brand-new manual-input `datasource` node, and
`FlowDataEngine.to_arrow()/to_pylist()/to_dict()` for previews.

**Violation symptom:** a `.collect()` on a real (non-preview,
non-accepted-exception) LazyFrame inside `flowfile_core` moves a potentially
multi-GB materialization into the FastAPI process every user's request goes
through — the whole point of the worker split is defeated, and a bad file
hangs or OOM-kills the API server itself instead of one job's subprocess.

---

## 3. The worker offload seam and wire protocol

Config (`configs/settings.py`): `OFFLOAD_TO_WORKER` is a live-mutable
`MutableBool` read from `FLOWFILE_OFFLOAD_TO_WORKER`, default on. **Gotcha:**
the check is `os.environ.get("FLOWFILE_OFFLOAD_TO_WORKER", "1") == "1"` —
only the exact string `"1"` is truthy; `"true"`/`"yes"`/`"on"` all evaluate
**false**, silently disabling offload, unlike the AI feature flags which
accept the full `true/1/yes/on` set — don't assume symmetry. `WORKER_URL`
resolves from `FLOWFILE_WORKER_URL` if set, else `WORKER_HOST` env, else
`127.0.0.1` (Windows) / `0.0.0.0` (else), port `FLOWFILE_WORKER_PORT`
(default 63579); appends `/worker` when `FLOWFILE_SINGLE_FILE_MODE` co-hosts
the worker on the core port.

Wire protocol (`flow_data_engine/subprocess_operations/subprocess_operations.py`):
serialization is **`pl.LazyFrame.serialize()` raw bytes — the query plan, not
data.** `POST {WORKER_URL}/submit_query/`, `Content-Type:
application/octet-stream`, headers `X-Task-Id` (= the node's hash, see §7),
`X-Operation-Type` (`store` | `calculate_number_of_records` | …), `X-Flow-Id`,
`X-Node-Id`. Sampling uses `POST /store_sample/` with `X-Sample-Size`; fuzzy
join uses `POST /add_fuzzy_join`. Preferred transport is a **WebSocket
stream**, falling back to REST + polling on any connect/send error. `GET
/status/{file_ref}` returns job status; a polars-op result is a **base64
serialized LazyFrame** — the worker hands back a lazy *scan over its own
cached parquet*, still not materialized data, so core stays clean reading the
result back too. `results_exists(hash)` (`GET /status/{hash}`) returns
**False on connection error** (worker down), not an exception — a known weak
point, see §11.

Where core calls the seam: `FlowNode._do_execute_remote` builds the lazy
result, ships it via `ExternalDfFetcher(lf=…, file_ref=self.hash)`, and reads
back a lazy scan plus a row count from a second fetcher call.
`_do_execute_local_with_sampling` computes narrow transforms lazily in core
and only ships a 100-row `ExternalSampler` job to the worker for the preview.
Source nodes doing network I/O (database, Kafka, Google Analytics, REST API)
never fetch in core — they use an `External*Fetcher`/`External*Writer`
(`add_google_analytics_reader` is the canonical template): the worker does
all fetching. Sinks (`output`, `api_response`, `flow_output`, `run_flow`) stay
in-core even in remote mode; the output node's own function routes the actual
write through `ExternalOutputWriter` when not local. Every fetcher-based node
also keeps an in-core `execution_location == "local"` branch (e.g. the
database reader builds a local `SqlSource`) for CLI `--run-flow` / package
mode where no worker process exists — that local branch is not a violation of
"worker does all fetching," it only fires when there is no worker.

---

## 4. Secrets: `$ffsec$1$<user_id>$<token>` — never change one side alone

Format: `$ffsec$1$<user_id>$<fernet_token>` — the leading `$1$` is the format
version, `<user_id>` is embedded in plaintext, `<fernet_token>` is the
Fernet-encrypted secret. Key derivation:
`HKDF-SHA256(master_key, length=32, salt=KEY_DERIVATION_VERSION, info=f"user-{user_id}")`
→ base64-urlsafe → Fernet key, `KEY_DERIVATION_VERSION = b"flowfile-secrets-v1"`.

**Why the user_id is embedded:** the **worker re-derives the per-user key with
zero context from core** — no HTTP call back, no session, nothing but the
shared master key and the id parsed out of the ciphertext prefix. That's what
lets a spawned worker subprocess decrypt a DB/cloud/Kafka credential
completely independently.

`secret_manager/secret_manager.py` (core) and `secrets.py` (worker) are
**byte-for-byte parallel implementations** of this format (same
`SECRET_FORMAT_PREFIX`, same `KEY_DERIVATION_VERSION`). **Changing the
prefix, salt, or HKDF `info` string on one side without the other is the
single easiest way to break every DB/cloud/Kafka/GA connector in the app** —
and it's a confusing split-brain failure: core still works (it never
re-derives), every worker job throws `InvalidToken`.

Legacy fallback: ciphertext not starting with the prefix is a raw Fernet
token decrypted with the master key directly (pre-per-user-key secrets).
Group sharing is **authorization-only** (§10), so this format needed zero
changes when sharing shipped.

---

## 5. Scheduler: no `flowfile_core` imports, ever

`flowfile_scheduler` is a separate, dependency-light package. Verified:
`grep -rn "flowfile_core|flowfile_worker|flowfile_frame"
flowfile_scheduler/flowfile_scheduler/` matches only two docstring sentences
declaring the rule — zero actual imports. The scheduler's `models.py` is a
**backward-compat shim re-exporting from `shared/models.py`** — independent
declarative SQLAlchemy models on their own `Base`, a minimal hand-maintained
mirror of `flowfile_core/database/models.py`, **not** SQLAlchemy table
reflection. (Root CLAUDE.md's "polls the shared SQLite DB via reflected
tables" is loose phrasing — treat "reflected" as informal, not literal
`Table(..., autoload_with=engine)` reflection.) Dependency arrow is core →
scheduler, never reverse.

**Why it matters:** a scheduler feature needing a new column means editing
`shared/models.py` to keep the mirror in sync, *and* a real Alembic migration
against `flowfile_core/database/models.py` (the canonical schema) — touching
only one side either breaks the scheduler's queries or leaves core's
migration history incomplete.

Worth knowing: single-leader via a `SchedulerLock` row (90s stale threshold);
cron uses a **naive local wall-clock cursor** advanced to "now" (not the
missed slot) on catch-up so a downed scheduler fires exactly once rather than
backfilling; runs launch as a **detached subprocess**
(`python -m flowfile run flow <path> --run-id <id>`), not an in-process call
— this is why the scheduler can be embedded in core without core's own crash
taking a running scheduled flow down with it.

---

## 6. Dual node-execution state — a classic bug source

Every `FlowNode` carries **two** parallel state representations:
- `node_stats: NodeStepStats` — legacy. Still read by `needs_run` and
  `get_predicted_resulting_data` (schema prediction).
- `_execution_state: NodeExecutionState` — new. Governs actual run/skip
  decisions via `NodeExecutor._decide_execution`.

They are synced **one-way**: `NodeExecutor._sync_state_to_legacy` copies the
new state into the legacy one after every decision/run. **There is no path
that updates only `node_stats` and expects `_execution_state` to follow** —
if you add a new run-affecting flag, write it to `_execution_state` and let
the sync propagate it; writing only `node_stats` is invisible to
`_decide_execution` and writing only `_execution_state` without going through
`_sync_state_to_legacy` leaves schema prediction (`needs_run`) looking at
stale data. **Symptom of getting this wrong:** the UI's "needs run" badge and
the actual run/skip behavior disagree — one lags the other by exactly one
sync call.

`NodeExecutor._decide_execution` is the single source of truth for **both**
whether to run and how (`executor.py`). Order matters — evaluated top to
bottom, first match wins:

| # | Condition | Result | Reason tag |
|---|---|---|---|
| 1 | `node_template.node_group == "output"` | RUN | `OUTPUT_NODE` — sinks always run |
| 2 | `node_type == "run_flow"` | RUN | subflow file can change with no parent-settings change, never "up to date" |
| 3 | `force_refresh` (reset_cache) | RUN | `FORCED_REFRESH` |
| 4 | `cache_results=True`, `results_exists(hash)` | **SKIP** | cache hit — checked *before* performance mode so a cached result survives even when upstream had no new data |
| 4b| `cache_results=True`, not `results_exists` | RUN | `CACHE_MISSING` |
| 5 | `performance_mode` | RUN | `PERFORMANCE_MODE` |
| 6 | `not has_run_with_current_setup` | RUN | `NEVER_RAN` |
| 7 | read node, source file changed on disk | RUN | `SOURCE_FILE_CHANGED` |
| 8 | else | **SKIP** | results already in memory from a previous run |

Strategy (`_determine_strategy`, once RUN is decided): `run_location ==
"local"` → `FULL_LOCAL`; else `cache_results` → `REMOTE` (caching needs a
fully materialized result); else `transform_type == "narrow"` →
`LOCAL_WITH_SAMPLING` (compute lazily in core, ship only a 100-row sample
job); else → `REMOTE`.

---

## 7. Node-hash caching, the `_hash` save/restore dance, and parameterized runs

`FlowNode.calculate_hash` folds: input-node hashes + `hash(setting_input)` +
`parent_uuid` (a `uuid1` stamped once per `FlowGraph` instance at
construction) + `_cache_epoch`. **The hash is the worker's cache key**
(`file_ref` in the wire protocol above) — `results_exists(hash)` and
`get_external_df_result(hash)` both key off it.

Because `parent_uuid` is per-graph-*instance*, worker cache keys **never
survive a graph reload** — restarting core or reopening a flow always
recomputes from scratch even if nothing changed. This follows directly from
§8's in-memory-only invariant but is easy to forget if you expect cache hits
across sessions.

**The `_hash` save/restore discipline** (`FlowGraph._execute_single_node`):
when a flow has `${param}` placeholders, execution substitutes resolved
parameter values into `setting_input` *in place* before running the node,
then restores the original `${...}` text afterward — **and explicitly saves
and restores `node._hash` around that substitution.** Why: mutating
`setting_input` in place, even temporarily, changes what `calculate_hash`
would compute; without the explicit restore, the next real settings write
after a parameterized run sees a hash mismatch, treats it as an external
change, calls `reset()`, and silently loses `example_data_generator` /
`has_completed_last_run` — a real bug this exact comment documents having
fixed once. **If you touch parameter substitution, preserve this save/restore
or you reintroduce that regression.**

---

## 8. Flows live in process memory only

`FlowfileHandler` keeps every open flow in an in-memory `dict` (process
memory, not the DB — the DB only stores catalog *registrations* of flows, not
live graph state). **A core restart, or a "Save As" that changes the
in-memory flow's identity, invalidates the frontend's `flow_id`.** The run
route has an explicit guard for this: hitting it with a stale id returns a
targeted 404 telling the user to reload the flow, rather than a generic
crash. If you add a new endpoint keyed by `flow_id`, expect the same failure
mode and handle it the same way — don't assume a `flow_id` a client sends is
still resolvable.

---

## 9. Kernel container contract (sandboxed user Python)

Each kernel is a Docker container running `uvicorn kernel_runtime.main:app
--host 0.0.0.0 --port 9999` **inside** the container (`EXPOSE 9999`,
healthcheck `curl -f http://localhost:9999/health`). **Local (electron/
package) topology:** core maps that container port to a host port from
`19000-19999` (`_BASE_PORT = 19000`, `_PORT_RANGE = 1000`); kernel URL is
`http://localhost:{allocated_port}`. **Docker-in-Docker (compose)
topology:** no host port mapping — kernels are reached by container name
(`http://flowfile-kernel-{id}:9999`) on the shared Docker network, because
core discovers the named volume covering the shared path and mounts the
*same* volume at the *same* path in the kernel container, so paths line up
identically across core/worker/kernel without translation.

**Kernel → core auth:** every kernel gets `FLOWFILE_INTERNAL_TOKEN` injected
(via `auth.jwt.get_internal_token()`) plus `FLOWFILE_CORE_URL`; the kernel
sends the token per-request, core verifies with `secrets.compare_digest`.
**Electron auto-generates** the token if unset; **docker mode hard-errors**
on a missing token — deliberate, electron is single-user and docker is
multi-tenant. A valid token with no kernel id becomes a synthetic
`_internal_service` principal that bypasses catalog access restriction (§10)
but must never be treated as a real user for ownership — sharing code keys
the synthetic check on `username == "_internal_service"`, never on id,
because that principal's `id` defaults to `1`, a real user id.

**Known asymmetry (gotcha):** `storage.shared_directory` and the derived
artifact/global-artifact dirs honor `FLOWFILE_SHARED_DIR` if set, but
production's `get_kernel_manager()` **hardcodes**
`storage.temp_directory / "kernel_shared"` and ignores that env var — setting
it to a non-default path in a real deployment splits artifact staging away
from the volume kernels actually mount. Tests dodge this by constructing
`KernelManager` with the path passed explicitly.

---

## 10. Group sharing is authorization-only

Group-based resource sharing (12 shareable resource types: secrets,
DB/cloud/Kafka/GA connections, catalog namespaces/tables/notebooks, flows,
visualizations, dashboards, global artifacts) is a pure **authorization**
layer bolted on top of existing ownership. **The load-bearing property:** a
shared secret's ciphertext stays encrypted under the *owner's* `user_id`
forever — sharing never re-encrypts it under the grantee's key. That's why
`flowfile_worker` (§4) and `flowfile_scheduler` (§5) needed **zero code
changes** to support sharing: a group-granted secret decrypts through the
exact same owner-keyed-derivation path as an owned one.

Consequences of "authorization-only, never re-key": rotating a secret on a
shared connection re-encrypts it under the **owner's** id, never the
manage-grantee's who triggered the rotation. Changing a shared connection's
*target* fields (host/endpoint/protocol) while it carries a bundled secret
and no new credentials were supplied is rejected with 422 — an
anti-repoint-harvest guard against a manage-grantee silently repointing the
connection at a server they control to harvest the owner's credentials on the
next connect. The catalog is **private-by-default in docker mode**
(electron/package stay fully open); resolution is own-first (own rows, then
group-granted, lowest row id wins on name collisions). `/user-groups` and
`/shares` **404 in electron mode** (`require_sharing_enabled` dependency,
`routes/user_groups.py`), same status as a genuinely missing resource — no
enumeration oracle. Every resource-delete path must call
`sharing.delete_grants_for_resource(...)`: SQLite reuses row ids, so a
leftover grant on a deleted resource's old id would silently reattach to
whatever unrelated resource claims that id next. An ORM `after_delete`
backstop covers single-row deletes, but **bulk `query.delete()` bypasses ORM
events** and must call the cleanup explicitly.

---

## 11. Known weak points (state these plainly; they are not secrets)

| Weak point | What it means in practice |
|---|---|
| **`flow_graph.py` god file** (~5977 lines) | Holds the DAG engine, ~46 `add_*` node builders, catalog Delta write helpers, ML train/apply plumbing, kernel execution, YAML serialization, groups, layout, history, and codegen entry all in one file. Navigate it with `grep -n "def add_" flowfile_core/flowfile_core/flowfile/flow_graph.py` — that's the practical index; don't try to read it top to bottom. |
| **Skip-list shallowness** | The pre-run skip pass (`util/node_skipper.py`) only expands one transitive level of "leads to" from incorrectly-configured nodes. Correctness for deeper chains relies on `_execute_stages` re-checking dependents of every failed/skipped node at run time — a genuine belt-and-suspenders design, not a hole, but don't assume the skip pre-pass alone is a complete skip set. |
| **`results_exists` swallows worker downtime** | Returns `False` on an HTTP connection error, same as a genuine cache miss. In Development mode this silently degrades "should skip, cached" decisions into full re-runs whenever the worker is briefly unreachable — no error surfaces, just unexpectedly slower runs. |
| **HTTP 419 on add-node failure** | `POST /update_settings/` raises the non-standard status code `419` (not `422`/`500`) when the node's `add_<type>` function itself raises — handle it explicitly in frontend/AI tool wrappers. Full dispatch-trap mechanics: `flowfile-node-development` §1.6. |
| **`FLOWFILE_OFFLOAD_TO_WORKER` truthy parsing** | Only the literal string `"1"` enables offload; `"true"` silently disables it. See §3. |

---

## 12. PENDING DIRECTION — unmerged, not yet true of `main`

As of 2026-07-03 there is an **unmerged** branch,
`claude/core-abstraction-flowgraph-001hrn`, carrying 7 commits (5 substantive
+ a docs commit + a merge of `main` — matching `flowfile-failure-archaeology`'s
branch inventory) that begin
decomposing the seams in §2, §3, and §6. **None of this is on `main` or any
release — treat it as a candidate design, not current behavior**, until it
lands:

- `WorkerTransport` (`flowfile/execution/transport.py`) — proposed sole owner
  of worker URLs/HTTP/WS, with typed exceptions instead of §3's ad-hoc
  error-code scheme.
- `ExecutionBackend` ABC with `LocalBackend`/`RemoteWorkerBackend`
  (`flowfile/execution/backends/`) — proposes replacing inline
  `if execution_location == "local"` branches (§3) with one
  `backend.run_lazyframe/sample/count_records` call per node. Adds a
  **ratchet test** pinning the count of remaining inline branches in
  `flow_graph.py`, failing if it goes up — worth reusing regardless of
  whether this branch lands.
- `NodeSpec` registry (`flowfile/node_registry/`) — proposed single source of
  truth for built-in node types, unifying four independently-maintained
  catalogs (`get_all_standard_nodes()`, `NODE_TYPE_TO_SETTINGS_CLASS`,
  `nodes_with_defaults`, `ai/tools/classification._NODE_CLASS_MAP` — see
  `flowfile-node-development` for why keeping these in sync today is a known
  parity hazard) as derived views over one `NodeSpec` per type.

**If asked to work near these seams**, check whether the branch has merged
(`git log --oneline main..claude/core-abstraction-flowgraph-001hrn` — empty
means merged or superseded) before assuming either the old inline-branch
shape or the new backend/registry shape is current truth.

---

## 13. VISION (maintainer direction, 2026-07-03) — not implemented, do not foreclose

The maintainer's stated direction: **decentralized execution on a
centralized catalog.** Global/shared *components* (catalog schema,
definitions, sharing model), catalog *metadata* in PostgreSQL, catalog *data*
in object storage (S3), and independent local Flowfile instances that each
bring their own compute and mix local files with catalog-hosted remote files
in the same flow.

**This is a direction, not a spec, and not implemented anywhere in the repo
today.** Nothing above contradicts it — but two current seams are exactly
what a future implementation would generalize, so **do not add code that
tightens their coupling to "local + SQLite" further**:
- `shared/storage_config.get_database_url()` (§1) is the **only** function
  deciding where the catalog metadata DB lives — a local SQLite file today
  (`FLOWFILE_DB_PATH` env, else `TESTING` temp path, else
  `<base>/database/flowfile_catalog.db`), no PostgreSQL branch yet. Route new
  catalog code through this seam (or `storage`/`CatalogService` above it)
  rather than hard-coding a SQLite path assumption inline.
- `catalog/storage_backend.py` already demonstrates the pattern the vision
  needs for *data*: storage resolves **per catalog** (a level-0 namespace
  carries `storage_uri` + `storage_connection_name`; unset ⇒ local
  filesystem, set ⇒ object storage, credentials resolved as the catalog
  owner). This supports "data in S3" already; it does not yet support
  "metadata in Postgres" or "multiple compute instances sharing one
  catalog." Extend along this per-namespace-target pattern, not a single
  global env-var switch — the module already demoted the old
  `FLOWFILE_CATALOG_STORAGE_URI`/`_CONNECTION` env vars from "the mechanism"
  to "just a creation-time default," which is this vision's direction.

If asked to design a feature touching catalog storage, catalog identity, or
cross-instance execution, treat "would this make a future
Postgres-metadata/multi-instance-compute design harder" as a review question
— not a blocker, but flag it rather than silently baking in a local-SQLite
assumption.

---

## Provenance and maintenance

All facts above were verified by reading source or running the listed
read-only command against the repo at commit `f6963c77` (branch
`feature/claude-skills`), app version **0.12.7**, dated **2026-07-03**. Every
volatile fact below should be re-checked with its command if this skill feels
stale — architecture facts like these drift slower than config defaults, but
line numbers and the pending-branch status will drift fastest.

```bash
# system map / "worker doesn't touch the catalog DB" claim
grep -rn "create_engine|Session|shared.models" flowfile_worker/flowfile_worker/ | grep -v __pycache__

# core-never-collects: god-file sizing
wc -l flowfile_core/flowfile_core/flowfile/flow_graph.py \
      flowfile_core/flowfile_core/flowfile/flow_node/flow_node.py \
      flowfile_core/flowfile_core/flowfile/flow_data_engine/flow_data_engine.py

# worker offload truthy parsing ("1" only)
grep -n "FLOWFILE_OFFLOAD_TO_WORKER" flowfile_core/flowfile_core/configs/settings.py

# worker wire-protocol endpoints
grep -n "@router" flowfile_worker/flowfile_worker/routes.py

# secrets format parity between core and worker
grep -n "SECRET_FORMAT_PREFIX\|KEY_DERIVATION_VERSION" \
  flowfile_core/flowfile_core/secret_manager/secret_manager.py \
  flowfile_worker/flowfile_worker/secrets.py

# scheduler zero-core-imports invariant
grep -rn "flowfile_core|flowfile_worker|flowfile_frame" flowfile_scheduler/flowfile_scheduler/ | grep -v __pycache__

# NodeExecutor decision table (still ordered the same way?)
sed -n '/_decide_execution/,/_determine_strategy/p' flowfile_core/flowfile_core/flowfile/flow_node/executor.py

# kernel port range + internal token wiring
grep -n "_BASE_PORT\|_PORT_RANGE" flowfile_core/flowfile_core/kernel/manager.py
grep -n "get_internal_token\|verify_internal_token" flowfile_core/flowfile_core/auth/jwt.py

# group sharing 404-in-electron gate
grep -n "require_sharing_enabled" flowfile_core/flowfile_core/routes/user_groups.py

# HTTP 419 on add-node failure
grep -n "419" flowfile_core/flowfile_core/routes/routes.py

# catalog storage per-namespace resolution (vision seam)
sed -n '1,40p' flowfile_core/flowfile_core/catalog/storage_backend.py

# pending-branch status: has it merged / is it still distinct?
git log --oneline main..claude/core-abstraction-flowgraph-001hrn 2>/dev/null \
  || git log --oneline f6963c77..claude/core-abstraction-flowgraph-001hrn

# app version
grep -n '^version' pyproject.toml
```
