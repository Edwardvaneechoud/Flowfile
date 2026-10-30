---
name: flowfile-architecture-contract
description: The system map and load-bearing design contracts of Flowfile's core/worker/frontend/kernel/scheduler/shared architecture — who talks to whom, why core never materializes LazyFrames, the worker offload wire protocol, the $ffsec$ secrets format, dual node-execution state, node-hash caching discipline, the compute protocol behind which the worker is the only compute, and the vision direction for catalog decentralization. Use when onboarding to the codebase, deciding which service a new code path belongs in, debugging cross-service contract breaks (secrets, worker offload, kernel containers, scheduler), or evaluating whether a design change would foreclose the maintainer's decentralization vision.
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

One catalog DB (SQLite `flowfile_catalog.db`, or PostgreSQL via
`FLOWFILE_DATABASE_URL`) is the metadata source of truth, resolved via
`shared/storage_config.get_database_url()`. The worker has no ORM but reads
it for one thing: a plan's `CredentialRef` (§4), read by id with no access
check (`shared/database.py::cloud_connection_record`). So a worker on another
host needs core's PostgreSQL catalog and master key. Core has the full ORM
(`database/models.py`); the scheduler and CLI run-completion path use a
hand-maintained declarative mirror in `shared/models.py` (§5), not
SQLAlchemy reflection.

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
it is **partly mechanical**. Every node's plan goes through `backend.submit`
(§3); `tests/flowfile/execution/test_serving_app_ratchet.py` statically holds
at zero every `.collect(` over a `plan_for(...)` result in `flowfile_core` and
`flowfile/web`; bounded reads go through `flow_data_engine/bounded.py`
(`count_rows`, `head_rows`, `single_row`, `unique_values`) on a backend. No
lint forbids any other `.collect()` in core, and `FlowDataEngine` still has
`collect(n)` / `to_arrow()` / `__len__`: use `is not None`, never truthiness,
on a `FlowDataEngine`, because `__len__` collects (a load-bearing comment in
`flow_node.py` says so). `NodeTemplate.laziness` plus
`FlowGraph.check_flow_laziness` (`GET /editor/laziness_check`) only report
eager nodes upstream of a catalog *virtual table*. The one accepted
full-frame collect in core: seeding a brand-new manual-input `datasource`
node (`builders/io.py::add_datasource`). Catalog Delta writes and the Graphic
Walker explorer (`viz_session`) are operations on the backend.

**Violation symptom:** a `.collect()` on a real (non-preview,
non-accepted-exception) LazyFrame inside `flowfile_core` moves a potentially
multi-GB materialization into the FastAPI process every user's request goes
through — the whole point of the worker split is defeated, and a bad file
hangs or OOM-kills the API server itself instead of one job's subprocess.

---

## 3. The worker offload seam and wire protocol

Config: the default backend is `execution/registry.py::default_backend_id`:
a headless run's override, else `FLOWFILE_COMPUTE_BACKEND` when non-empty, else the legacy
`FLOWFILE_OFFLOAD_TO_WORKER` (on: `worker`; off: `local`, the worker embedded
in this process); a serving core turns `local` into `worker`.
**Gotcha:** the legacy check is
`os.environ.get("FLOWFILE_OFFLOAD_TO_WORKER", "1") == "1"` — only the exact
string `"1"` is on; `"true"`/`"yes"`/`"on"` turn it **off** (default backend
`local` in a non-serving process), unlike the AI flags' `true/1/yes/on` —
don't assume symmetry. `WORKER_URL` resolves from `FLOWFILE_WORKER_URL`, else
`WORKER_HOST` (else `127.0.0.1` on Windows / `0.0.0.0`), port
`FLOWFILE_WORKER_PORT` (default 63579); the pip server's `start_server`
(`flowfile/web/__init__.py`) points it at `/worker` on the core port.

Wire protocol: with the protocol on (the default), `WorkerBackend`
(`execution/backends/worker.py`) speaks the
worker's versioned `/v1` routes (`flowfile_worker/protocol_routes.py`). The
plan crosses as **`pl.LazyFrame.serialize()` bytes — the query plan, not
data** — and previews come back inline as Arrow IPC, so core never opens a
worker's file. A protocol-version mismatch is a 426 `VersionMismatch`, no
fallback. Route table, auth headers and the task routes core still calls
(Delta writes, `viz_session`, catalog services, custom-node dry run):
`docs/for-developers/compute-protocol.md`. The old path (task routes,
`ExternalDfFetcher`, results read over a shared disk) remains with
`FLOWFILE_COMPUTE_PROTOCOL=0`.

Where core calls the seam: `FlowNode._do_execute` calls
`backend.submit(plan, inputs, hints, result_id=self.hash)` (hints §6) and
keeps the returned `ResultRef` plus an inline sample of at most
`preview_rows`. No node computes in core. Network sources (`database_read`,
`kafka_read`, `google_analytics_read`, `rest_api_read`) and writes
(`file_write`, `database_write`, `cloud_write`, `delta_*`) are declared
operations through `FlowNode.run_op` on the backend; the GA reader is pinned
to `worker`. Headless runs and Flowfile in Python compute on the embedded
worker by default (§12).

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

**Serialized plans are a transport too.** Polars inlines `storage_options`
into a serialized plan, so decrypted keys in a core-built cloud scan would
ship in plaintext. With the protocol on, a stored connection's scan carries a
`shared/cloud_credential_provider.py::CredentialRefProvider` (connection id,
user id, per-plan nonce, no secret); the process running the plan resolves it
from its own catalog DB and master key (`resolve_credential_ref`), turning
Azure service-principal and GCS service-account secrets into bearer tokens.
With the flag off, or for a read with no saved connection, the credentials
ride in an `EncryptedCredentialProvider` holding only a `$ffsec$` ciphertext;
there ADLS service-principal secrets and GCS service-account keys still
travel in the options.

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

`NodeExecutor._decide_execution` (`executor.py`) is the single source of
truth for whether to run. Order matters — top to bottom, first match wins:

| # | Condition | Result | Reason tag |
|---|---|---|---|
| 1 | `node_template.node_group == "output"` | RUN | `OUTPUT_NODE` — sinks always run |
| 2 | `run_flow` | RUN | `OUTPUT_NODE` — subflow file can change with no settings change |
| 3 | formula `gate` | RUN | `OUTPUT_NODE` — its data can change with no hash change |
| 4 | `force_refresh` | RUN | `FORCED_REFRESH` |
| 5 | read node's source file changed | RUN | `SOURCE_FILE_CHANGED` — outranks every cache |
| 6 | ran with current setup, but `results.artifact_is_missing()` | RUN | `CACHE_MISSING` |
| 7 | `cache_results`, held materialised result still `exists`, no last error | **SKIP** | cache hit — before performance mode, so it survives when upstream had no new data |
| 7b| `cache_results`, no held result, or one gone or after an error | RUN | `CACHE_MISSING`; a `ComputeError` from `exists` fails the node; a deferred held result falls through |
| 8 | `performance_mode` | RUN | `PERFORMANCE_MODE` |
| 9 | `not has_run_with_current_setup` | RUN | `NEVER_RAN` |
| 10 | else | **SKIP** | result already held |

How it runs: no strategy step. `hints_for` sets each submit's `SubmitHints`
(`must_materialise`, `wide`, `fanout`, `preview_rows`: default 100, `None` in
Performance mode unless `cache_results`); sinks (`IN_CORE_NODE_TYPES`) are
never wide, ask no preview and materialise only for a boundary input. The
worker stores a result when `must_materialise`, or when `wide` and either
previewed or `fanout > 1`
(`flowfile_worker/service.py::_materialise`); otherwise the plan stays
deferred.

---

## 7. Node-hash caching, the `_hash` save/restore dance, and parameterized runs

`FlowNode.calculate_hash` folds: input-node hashes + `hash(setting_input)` +
`parent_uuid` (a `uuid1` stamped once per `FlowGraph` instance at
construction) + `_cache_epoch`. **The hash is the result's id on the
backend** (`result_id=self.hash`): the `ResultRef`, job id and cache check
(`backend.exists`, §6) key off it; with the protocol off, the task routes'
`file_ref`.

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
| **`flow_graph` was a god file** (split 2026-10-09) | Now the `flowfile/flow_graph/` package: `graph.py` plus concern mixins and plain helper modules; `__init__.py` is the facade and the bind surface (collaborators tests patch are read through `_root.root()`). |
| **Skip-list shallowness** | The pre-run skip pass (`util/node_skipper.py`) only expands one transitive level of "leads to" from incorrectly-configured nodes. Correctness for deeper chains relies on `_execute_stages` re-checking dependents of every failed/skipped node at run time — a genuine belt-and-suspenders design, not a hole, but don't assume the skip pre-pass alone is a complete skip set. |
| **HTTP 419 on add-node failure** | `POST /update_settings/` raises the non-standard status code `419` (not `422`/`500`) when the node's `add_<type>` function itself raises — handle it explicitly in frontend/AI tool wrappers. Full dispatch-trap mechanics: `flowfile-node-development` §1.6. |
| **`FLOWFILE_OFFLOAD_TO_WORKER` truthy parsing** | Only the literal `"1"` keeps the legacy flag on; `"true"` turns it off, so a non-serving process with `FLOWFILE_COMPUTE_BACKEND` unset defaults to `local`. See §3. |

---

## 12. Directions that shipped: the compute protocol and the notebook kernel

### Compute protocol: one `ComputeBackend` seam, the worker the only compute

All compute sits behind `ComputeBackend` (`flowfile/execution/protocol.py`;
design: `docs/for-developers/compute-protocol.md`). A plan goes in, a
`ResultRef` (schema, row count, opaque location) comes back, so core holds
references and never rows beyond `preview_rows`; anything else is a declared
operation (`run_op`); errors are typed (`execution/errors.py`). With the
protocol on, a flow's `execution_location` is `"auto"` or a backend id
(`"remote"` an alias of `"auto"`). Two backends, both the worker:

- `EmbeddedWorkerBackend` (`execution/backends/embedded.py`), id `local`: the
  worker embedded in this process, its `/v1` route bodies called directly (no
  port, token or socket). Headless runs (`flowfile run flow`, `--run-flow`,
  scheduled) default to it, each job in a child of the runner
  (`registry.headless_run`); Flowfile in Python runs jobs inline; the pip
  server's `worker` is this backend too, its jobs in the worker's pool.
- `WorkerBackend` (`execution/backends/worker.py`): the client of a
  `flowfile_worker` on any host, checked once per process by a
  `capabilities()` handshake. Ids: `worker` (built in) and `worker:<name>` per
  `compute_backends` row (Settings → Execution → Backends); other kinds
  register through the `flowfile.compute_backends` entry-point group.

A serving core (`registry.mark_serving()`) never defaults to `local`, and
with the protocol on runs no node there (`this core serves; compute runs on
its worker`): with an exchange store such a node falls back to the default
backend, otherwise the run is refused before any node starts. Placement
(`execution/placement.py`) refuses a node its backend cannot run before the
run starts. With
`FLOWFILE_EXCHANGE_URI` set, each node goes to the backend its own
`execution_location` names and boundary edges cross through the exchange
store (`export` / `import_uri`); without it, every node goes to the run's
backend. No inline `execution_location == "local"` branch remains in the
flow graph, builders, flow node or data engine
(`tests/flowfile/execution/test_inline_location_branches.py` holds it at
zero); `api_runner.py::_materialize` keeps one, unreachable in a serving core.

**If asked to work near these seams**, read `compute-protocol.md` first;
every backend passes `tests/flowfile/execution/conformance/`.

### Notebook kernel: canvas fallback everywhere, no database copy (planned 2026-10-02, landed 2026-10-03)

A separate execution mode (`flowfile_frame/notebook_kernel.py`, driven by
`flowfile_core/notebook/kernel_runner.py`): node settings stay the contract
that round-trips to the canvas on Push, the kernel runs natively what it can,
and core runs the rest. The full contract is in the root `CLAUDE.md`
("Notebook kernel mode") and `flowfile_core/CLAUDE.md` (`notebook/`).

Invariants:
- **No catalog database in the kernel.** `_notebook_env` sets no
  `FLOWFILE_DB_PATH`, and every `notebook_kernel.handle()` installs
  `_refuse_database` on the cached engine, so a cell that opens the database
  stops with `NO_DATABASE`, naming where it was opened. An uncached
  `create_catalog_engine()` called by hand is not refused.
- **Metadata comes from core.** The `notebook/lookup.py::metadata_lookup`
  hook (a `ContextVar`) routes every catalog, flow, kernel and connection
  lookup to `POST /notebook/session/lookup`: a closed `lookup.KINDS`,
  answered as the kernel's owner, metadata only (no plan, credential,
  password or ciphertext).
- **Core runs what the kernel cannot.** A gate, a deferred node outside
  `COMPUTED_HERE_TYPES` (pivot and Polars Code compute in the kernel), every
  catalog reader (local tables too), a secret-bearing node (cloud readers,
  custom nodes with a `SecretSelector`) and every local file a cell names (a
  URL is read in the kernel) get their rows from core: from the canvas twin
  through `POST /notebook/session/node_result` (`kernel_runner._run_lineage`),
  else from `notebook/held_run.py` through `POST /notebook/session/node_run`
  (under `KernelHold`, `commit_sources=False`) for the closed
  `HELD_NODE_TYPES` plus installed non-output custom nodes; writers and other
  types say "Push".
- **The kernel mounts exactly `/shared` and `/catalog_tables`.** A path in a
  cell is a path on the user's machine that only core opens
  (`native._kernel_hidden_path`); a folder test is the `is_directory`
  lookup; installed custom node files are mirrored in through
  `notebook_kernel._mirror_custom_nodes`; a push stores paths as written.
- A lineage run with no output node passes `run_graph(commit_sources=False)`
  (`kernel_runner.lineage_commits`), so showing rows moves no cursor.

Do not add SCD2, change-feed or SQL logic to `kernel_runtime/flowfile_client.py`,
forward raw SQL to core, bring a database back into the kernel, or mount a
host folder into it.

Proof: `tests/notebook/test_kernel_database_census.py` (no kernel op opens the
database; every lookup kind answers without `$ffsec$` or a decrypt),
`tests/notebook/test_kernel_session.py::test_a_notebook_kernel_refuses_to_open_a_catalog_database`
(the only non-Docker test that fires `_refuse_database`; the census sim never
sets `FLOWFILE_KERNEL_ID`), `tests/kernel/test_notebook_kernel_env.py`
(exactly the two binds), `tests/notebook/test_kernel_canvas_rows.py`, and the real-kernel
`tests/notebook/test_kernel_notebook_docker.py`, run by
`test-notebook-kernel.yml` (`FLOWFILE_REQUIRE_NOTEBOOK_KERNEL` makes a skip a
failure).

Known gaps: a catalog table a cell reads is handed over whole as parquet
through `/shared`, even for `display()`; a seeded canvas node below one that
just ran keeps its seeded columns until it runs or the session is reset;
`FlowFrame.collect()` on a deferred frame in a script still commits sources.
Each has its own plan, as do a Python Script on the notebook's own kernel
(`KernelHold` refuses it), a push that keeps the session's variables,
session artifacts visible to the canvas, Postgres catalogs (the gate,
`not sharing_enabled()`, already admits an electron app on one, untested),
and docker- and package-mode sessions: widen `gate.kernel_sessions_allowed`
only after the lookups check per-user access and `node_run` is admin-gated.

---

## 13. VISION (maintainer direction, 2026-07-03) — partly shipped, do not foreclose

The maintainer's stated direction: **decentralized execution on a
centralized catalog.** Global/shared *components* (catalog schema,
definitions, sharing model), catalog *metadata* in PostgreSQL, catalog *data*
in object storage (S3), and independent local Flowfile instances that each
bring their own compute and mix local files with catalog-hosted remote files
in the same flow.

**Parts have shipped; the whole is a direction, not a spec.** Shipped:
catalog metadata in PostgreSQL (`FLOWFILE_DATABASE_URL`), catalog data in
object storage (below), compute on other hosts (§12). Not shipped: several
independent Flowfile instances, each with its own core, on one catalog. Two
current seams are what that would generalize, so **do not add code that
tightens their coupling to "local + SQLite"**:
- `shared/storage_config.get_database_url()` (§1) is the **only** function
  deciding where the catalog metadata DB lives: `FLOWFILE_DATABASE_URL`, else
  `FLOWFILE_DB_PATH`, else the `TESTING` temp path, else
  `<base>/database/flowfile_catalog.db`. Route new catalog code through this
  seam (or `storage`/`CatalogService` above it), never an inline SQLite path.
- `catalog/storage_backend.py` already demonstrates the pattern the vision
  needs for *data*: storage resolves **per catalog** (a level-0 namespace
  carries `storage_uri` + `storage_connection_name`; unset ⇒ local
  filesystem, set ⇒ object storage, credentials resolved as the catalog
  owner). Extend along this per-namespace-target pattern, not a single
  global env-var switch — the module already demoted the old
  `FLOWFILE_CATALOG_STORAGE_URI`/`_CONNECTION` env vars from "the mechanism"
  to "just a creation-time default," which is this vision's direction.

If asked to design a feature touching catalog storage, catalog identity, or
cross-instance execution, treat "would this make a multi-instance design
on one catalog harder" as a review question
— not a blocker, but flag it rather than silently baking in a local-SQLite
assumption.

---

## Provenance and maintenance

All facts above were verified by reading source or running the listed
read-only command against the repo at commit `f6963c77` (branch
`feature/claude-skills`), app version **0.12.7**, dated **2026-07-03**. Every
volatile fact below should be re-checked with its command if this skill feels
stale — architecture facts like these drift slower than config defaults, but
line numbers will drift fastest.

```bash
# system map / the worker reads core's catalog DB to resolve a plan's CredentialRef
grep -rn "def cloud_connection_record\|def resolve_credential_ref\|credential_ref_kwargs(" shared/database.py shared/cloud_credential_provider.py flowfile_worker/flowfile_worker/ | grep -v __pycache__

# core-never-collects: god-file sizing
wc -l flowfile_core/flowfile_core/flowfile/flow_graph/*.py flowfile_core/flowfile_core/flowfile/flow_graph/builders/*.py \
      flowfile_core/flowfile_core/flowfile/flow_node/flow_node.py \
      flowfile_core/flowfile_core/flowfile/flow_data_engine/flow_data_engine.py

# default backend + legacy offload truthy parsing ("1" only)
grep -n "def default_backend_id" -A12 flowfile_core/flowfile_core/flowfile/execution/registry.py
grep -n "FLOWFILE_OFFLOAD_TO_WORKER" flowfile_core/flowfile_core/configs/settings.py

# worker endpoints: /v1 routes, sockets, task routes (deprecated ones marked)
grep -n "@router\|add_api_route\|websocket(\|Depends(deprecated_route)" flowfile_worker/flowfile_worker/{protocol_routes,routes,streaming}.py

# the inline local branches left (published-API runner)
grep -n 'execution_location == "local"' flowfile_core/flowfile_core/flowfile/api_runner.py

# secrets format parity between core and worker
grep -n "SECRET_FORMAT_PREFIX\|KEY_DERIVATION_VERSION" \
  flowfile_core/flowfile_core/secret_manager/secret_manager.py \
  flowfile_worker/flowfile_worker/secrets.py

# scheduler zero-core-imports invariant
grep -rn "flowfile_core|flowfile_worker|flowfile_frame" flowfile_scheduler/flowfile_scheduler/ | grep -v __pycache__

# NodeExecutor decision table (still ordered the same way?) + submit hints
sed -n '/^def hints_for/,/^class StateProvider/p;/def _decide_execution/,/def _gate_routes_on_formula/p' flowfile_core/flowfile_core/flowfile/flow_node/executor.py

# kernel port range + internal token wiring
grep -n "_BASE_PORT\|_PORT_RANGE" flowfile_core/flowfile_core/kernel/manager.py
grep -n "get_internal_token\|verify_internal_token" flowfile_core/flowfile_core/auth/jwt.py

# group sharing 404-in-electron gate
grep -n "require_sharing_enabled" flowfile_core/flowfile_core/routes/user_groups.py

# HTTP 419 on add-node failure
grep -n "419" flowfile_core/flowfile_core/routes/routes.py

# catalog storage per-namespace resolution (vision seam)
sed -n '1,40p' flowfile_core/flowfile_core/catalog/storage_backend.py

# compute protocol: the seam, its two backends, the serving core's refusal of `local`
grep -n "class ComputeBackend" flowfile_core/flowfile_core/flowfile/execution/protocol.py
grep -n "^class " flowfile_core/flowfile_core/flowfile/execution/backends/*.py
grep -n "this core serves" flowfile_core/flowfile_core/flowfile/execution/placement.py

# app version
grep -n '^version' pyproject.toml
```
