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

One catalog DB (SQLite `flowfile_catalog.db` by default, PostgreSQL when
`FLOWFILE_DATABASE_URL` holds a server URL) is the metadata source of truth,
resolved via `shared/storage_config.get_database_url()`. **The worker reads
it too:** with `FLOWFILE_COMPUTE_PROTOCOL` on, a plan's cloud scan or sink
carries a `CredentialRefProvider` (connection id + user id,
`shared/cloud_credential_provider.py`), and the process that runs the plan
reads that connection's row and its secrets' ciphertext from the catalog
(`shared/database.py::cloud_connection_record`, by id, no access check) and
decrypts them with its own master key (`resolve_credential_ref`; worker side
`credential_ref_kwargs` in `flowfile_worker/service.py` / `funcs.py`). So a
worker must use core's catalog DB and master key: a SQLite catalog serves
only a worker on core's host, a worker on another host needs the shared
PostgreSQL catalog (`docs/users/deployment/remote-worker.md`). The worker has
no ORM or session of its own — that one raw read, plus `shared.sql_utils` for
*user* database connection URIs. Core has the full ORM (`database/models.py`);
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
it is **partly mechanical**. `FlowNode._do_execute` submits every node's plan
through `backend.submit` (§3), and
`tests/flowfile/execution/test_serving_app_ratchet.py` statically holds at
zero every `.collect(` over a `plan_for(...)` result in `flowfile_core` and
`flowfile/web` (the serving app reads a held result's rows only through the
backend's bounded `sample`). Bounded reads go through
`flow_data_engine/bounded.py` (`count_rows`, `head_rows`, `single_row`,
`unique_values`): each submits the plan deferred to the backend it is given and
reads back only the count or the rows asked for, never computing in core on
failure. No lint forbids a `.collect()` elsewhere in core, and
`FlowDataEngine` still has `collect(n)` / `to_arrow()` / `__len__`: a
load-bearing comment in `FlowNode.get_resulting_data` warns to use
`is not None`, never truthiness, on a `FlowDataEngine` because `__len__`
collects; `NodeTemplate.laziness` metadata plus
`FlowNode.check_upstream_laziness` / `FlowGraph.check_flow_laziness`
(`GET /editor/laziness_check`) report eager nodes upstream of a catalog
*virtual table* — but that only gates one optimization path, not a general
rule. The one accepted exception where core holds a full frame: seeding a
brand-new manual-input/`datasource` node (`builders/io.py::add_datasource`'s
`input_data.collect()`, for a manual input the rows the user typed). Catalog
Delta writes (`delta_write`/`delta_merge`/`delta_scd2`) and the Graphic Walker
explorer (`viz_session`) are operations on the backend; every other read of
rows is a bounded read or a sample through a backend.

**Violation symptom:** a `.collect()` on a real (non-preview,
non-accepted-exception) LazyFrame inside `flowfile_core` moves a potentially
multi-GB materialization into the FastAPI process every user's request goes
through — the whole point of the worker split is defeated, and a bad file
hangs or OOM-kills the API server itself instead of one job's subprocess.

---

## 3. The worker offload seam and wire protocol

Config: the default backend is `execution/registry.py::default_backend_id` —
the override a headless run sets (`set_default_override`), else
`FLOWFILE_COMPUTE_BACKEND` when non-empty, else the legacy
`OFFLOAD_TO_WORKER` (`configs/settings.py`, a live-mutable `MutableBool` read
from `FLOWFILE_OFFLOAD_TO_WORKER`, default on): `worker` while on, `local`
(the worker embedded in this process) while off. A serving core
(`mark_serving`) turns `local` into `worker`, so there the flag has no
effect. Outside `configs/settings.py` the registry is the only module that
reads it (`test_inline_location_branches.py` counts readers). **Gotcha:** the check is
`os.environ.get("FLOWFILE_OFFLOAD_TO_WORKER", "1") == "1"` — only the exact
string `"1"` is truthy; `"true"`/`"yes"`/`"on"` all evaluate **false**, making
a non-serving process's default backend `local`, unlike the AI feature flags
which accept the full `true/1/yes/on` set — don't assume symmetry. `WORKER_URL`
resolves from `FLOWFILE_WORKER_URL` if set, else `WORKER_HOST` env, else
`127.0.0.1` (Windows) / `0.0.0.0` (else), port `FLOWFILE_WORKER_PORT`
(default 63579); in single-file mode `flowfile/web/__init__.py::start_server`
sets it to `/worker` on the core port, where it mounts the worker's routes.

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
result back too. `results_exists(hash)` (`GET /status/{hash}`, protocol off
only) raises `ComputeUnavailable` when the worker cannot be reached, never a
cache miss; with the protocol on the cache check is `FlowNode.held_result` +
`backend.exists` (§6).

Where core calls the seam: `FlowNode._do_execute` builds the node's plan
(`get_resulting_data`) and calls `backend.submit(plan, inputs, hints,
result_id=self.hash)` on the context's backend, with the hints from
`flow_node/executor.py::hints_for` (§6); `_take_result` keeps the returned
`ResultRef` (`results.result_ref`), a `FlowDataEngine` over
`backend.plan_for(ref)`, and the backend's inline sample (at most
`preview_rows`) as the preview, else outside Performance mode a sample read
when first opened. No node is computed in core; `ExternalDfFetcher` /
`ExternalSampler` remain only for `FLOWFILE_COMPUTE_PROTOCOL=0`. Source nodes
doing network I/O never fetch in core: each runs its declared operation
(`database_read`, `kafka_read`, `google_analytics_read`, `rest_api_read`)
through `FlowNode.run_op` on the context's backend (`builders/connectors.py`),
the Google Analytics reader pinned to `registry.backend("worker")` since core
has no reader of its own. Writes are operations too: the output node's
`file_write` (`builders/io.py::add_output`), the database writer's
`database_write`, the cloud writer's `cloud_write` (or `delta_write` /
`delta_merge`), the catalog writer's `delta_*`. Sinks (`IN_CORE_NODE_TYPES` in
`executor.py`: `output`, `api_response`, `flow_output`, `run_flow`) are still
submitted, but their hints are never `wide`, ask no preview and materialise
only for a boundary input, so no backend computes or samples their result
(the input a sink wrote, a `run_flow` node's child-flow output) during the
run. No inline `execution_location == "local"` branch remains in the flow
graph, its builders, the flow node or the data engine
(`test_inline_location_branches.py` holds that at zero; `api_runner.py`'s
`_materialize` still collects in process for a flow set to `local`): headless
runs (`flowfile run flow`, `--run-flow`, scheduled runs) and Flowfile in
Python compute on the
worker embedded in their own process (`EmbeddedWorkerBackend`, id `local`),
which runs the same operation. Since `a0c45bd6` a headless run
(`registry.headless_run`) runs each job in a child process of the runner (one
kept warm, `HEADLESS_POOL_SIZE`; time limit `FLOWFILE_WORKER_TASK_TIMEOUT`);
only Flowfile in Python runs jobs inline (`spawner.in_process`).

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
into a LazyFrame's serialized plan, so a cloud scan built in core with
decrypted keys would ship them to the worker (and into logs/caches) in
plaintext. Core-built cloud scans therefore pass
`CloudStorageReader.get_secure_scan_kwargs(...)`: the credentials ride in a
`shared/cloud_credential_provider.py::EncryptedCredentialProvider` (a Polars
`credential_provider` whose only state is a `$ffsec$` ciphertext under the
node's user), decrypted by whichever process executes the plan through the
decryptor it registered (core: `flow_data_engine/cloud_storage_reader.py`;
worker: `flowfile_worker/utils.py`). ADLS service-principal secrets and GCS
service-account keys cannot go through a Polars provider and still travel in
the options.

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

`NodeExecutor._decide_execution` is the single source of truth for whether
to run (`executor.py`); how it runs is the `hints_for` paragraph below. Order
matters — evaluated top to bottom, first match wins:

| # | Condition | Result | Reason tag |
|---|---|---|---|
| 1 | `node_template.node_group == "output"` | RUN | `OUTPUT_NODE` — sinks always run |
| 2 | `node_type == "run_flow"` | RUN | `OUTPUT_NODE` — subflow file can change with no parent-settings change, never "up to date" |
| 3 | `gate` with a formula condition (`_gate_routes_on_formula`) | RUN | `OUTPUT_NODE` — the formula's data can change with no hash change |
| 4 | `force_refresh` (reset_cache) | RUN | `FORCED_REFRESH` |
| 5 | read node, source file changed on disk (`_source_file_changed`) | RUN | `SOURCE_FILE_CHANGED` — outranks every cache, the hash does not rotate when a file changes |
| 6 | ran with current setup, `results.artifact_is_missing()` | RUN | `CACHE_MISSING` — skipping would hand downstream a dangling plan |
| 7 | `cache_results=True`, last run had no error, and the run's backend still `exists` the materialised result `FlowNode.held_result(backend.id)` returns | **SKIP** | cache hit — checked *before* performance mode so a cached result survives even when upstream had no new data |
| 7b| `cache_results=True`, no held result, or a materialised one not held any more or after an error | RUN | `CACHE_MISSING`; a `ComputeError` from `backend.exists` fails the node, never a cache miss. A deferred held result is no cache and falls through to 8 |
| 8 | `performance_mode` | RUN | `PERFORMANCE_MODE` |
| 9 | `not has_run_with_current_setup` | RUN | `NEVER_RAN` |
| 10 | else | **SKIP** | result already held from a previous run |

How it runs (`hints_for`, once RUN is decided): there is no strategy step.
Every running node submits its plan through `FlowNode._do_execute` (§3) with
`SubmitHints`: `must_materialise` (`cache_results` outside
`IN_CORE_NODE_TYPES`, or a boundary input), `wide` (not
`is_narrow_transform`, not in `IN_CORE_NODE_TYPES`), `fanout` (distinct
downstream nodes this run executes) and `preview_rows` (the context's, else
`DEFAULT_PREVIEW_ROWS` = 100; `None` in Performance mode unless
`cache_results`, and for `IN_CORE_NODE_TYPES`). The backend stores the result
when `must_materialise`, or when it is `wide` and has a preview or
`fanout > 1` (`flowfile_worker/service.py::_materialise`); otherwise it keeps
the plan deferred.

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
| **`flow_graph` was a god file** (split 2026-10-09) | Now the `flowfile/flow_graph/` package: `graph.py` plus concern mixins and plain helper modules; `__init__.py` is the facade and the bind surface (collaborators tests patch are read through `_root.root()`). |
| **Skip-list shallowness** | The pre-run skip pass (`util/node_skipper.py`) only expands one transitive level of "leads to" from incorrectly-configured nodes. Correctness for deeper chains relies on `_execute_stages` re-checking dependents of every failed/skipped node at run time — a genuine belt-and-suspenders design, not a hole, but don't assume the skip pre-pass alone is a complete skip set. |
| **HTTP 419 on add-node failure** | `POST /update_settings/` raises the non-standard status code `419` (not `422`/`500`) when the node's `add_<type>` function itself raises — handle it explicitly in frontend/AI tool wrappers. Full dispatch-trap mechanics: `flowfile-node-development` §1.6. |
| **`FLOWFILE_OFFLOAD_TO_WORKER` truthy parsing** | Only the literal string `"1"` enables offload; `"true"` silently disables it. See §3. |

---

## 12. Directions that shipped: the compute protocol and the notebook kernel

### Compute protocol: one `ComputeBackend` seam, the worker the only compute

All compute sits behind one interface, `ComputeBackend`
(`flowfile_core/flowfile_core/flowfile/execution/protocol.py`, a
`runtime_checkable` Protocol; the design is
`docs/for-developers/compute-protocol.md`). A node's plan crosses it as a
`pl.LazyFrame` and comes back as a `ResultRef` (schema, row count, an opaque
location), so core holds references and never rows beyond `preview_rows`;
anything that is not a plain plan is a declared operation (`run_op`), and
errors are typed (`execution/errors.py`). Two backends, both the worker:

- `EmbeddedWorkerBackend` (`execution/backends/embedded.py`), id `local`: the
  worker embedded in this process, reached by calling the bodies of its `/v1`
  routes directly, with no port, token or socket. A scheduled run,
  `flowfile run flow` and Flowfile in Python compute on it, a headless run's
  jobs each in a child of the runner (§3), Flowfile in Python's in the
  process itself; so does the pip server, its jobs in the worker's pool.
- `WorkerBackend` (`execution/backends/worker.py`): the HTTP and WebSocket
  client to a `flowfile_worker` on any host, one method to one `/v1` route,
  checked once per process through its own `capabilities()` handshake. Ids
  are `worker` (the built-in default) and `worker:<name>` for each row of the
  backends table (`compute_backends`, Settings → Execution → Backends); other
  kinds register through the `flowfile.compute_backends` entry-point group.

A core that serves (the desktop sidecar, the Docker image, the pip server;
`registry.mark_serving()`) never defaults to `local` and refuses a run placed
on it before any node starts: `cannot run on backend "local": this core
serves; compute runs on its worker`. Placement (`execution/placement.py`)
compares each node's requirements with the backends' capabilities and
refuses before the run, with the reason in words. With `FLOWFILE_EXCHANGE_URI`
set, the mixed policy places each node on the backend its own
`execution_location` names, and the data of every boundary edge crosses
through the exchange store (`execution/exchange.py`) by `export` and
`import_uri`; without one, every node of a run goes to the run's backend. No
inline `execution_location == "local"` branch remains in the flow graph, its
builders, the flow node or the data engine
(`tests/flowfile/execution/test_inline_location_branches.py` holds the count
at zero; `api_runner.py` keeps one, §3).

`FLOWFILE_COMPUTE_PROTOCOL` (default on) still gates the path: with it off,
core uses the worker's task routes of §3, which answer `Deprecation: true`
where a `/v1` route replaced them. The flag is removed later. **If asked to
work near these seams**, read `compute-protocol.md` first; the conformance
suite (`tests/flowfile/execution/conformance/`) is what every backend passes.

### Notebook kernel: canvas fallback everywhere, no database copy (planned 2026-10-02, landed 2026-10-03)

Agreed direction for the notebook kernel (`flowfile_frame/notebook_kernel.py`,
`flowfile_core/notebook/kernel_runner.py`), built in phases 0–4a below. The
kernel is a separate execution mode: node settings stay the contract that
round-trips to the canvas on Push, the kernel runs natively what it can, and
everything else goes to the canvas. The end state, reached with phase 3, is a
kernel that opens **no** catalog database connection: the per-kernel SQLite
copy (`kernel/notebook_db.py`, there because WAL did not cross the Docker
Desktop VM) and the SQLite requirement of the gate (`notebook/gate.py`) are
gone. Do not add SCD2, change-feed or SQL logic to
`kernel_runtime/flowfile_client.py`, forward raw SQL to core, bring a
database back into the kernel, or mount a host folder into it.

Phase 0 landed with the plan: a lineage run that holds no output node
passes `run_graph(commit_sources=False)` (`kernel_runner.lineage_commits`),
and a session takes the schemas of canvas nodes that ran
(`_Session.refresh`).

Phase 1 landed 2026-10-03: **held nodes run in core from their settings.**
Where the kernel's `_Session._resolve` finds a gate or deferred node without
a canvas twin (or a read of a file it cannot see), it hands core the node as
`FlowfileData` lists it plus its inputs as parquet under the session's
results folder: `notebook/held_run.py`, `POST /notebook/session/node_run`,
bound like `node_result`. Core builds the inputs as `flow_input` nodes fed
those files (not `add_dependency_on_polars_lazy_frame`: a `NodePromise` node
is never `is_correct`, so the planner skips it) plus the one node through
`populate_graph_from_flow_information`, on a `FlowGraph` that lives for the
call (`_system_run`, Performance mode, the canvas flow's
`source_registration_id` and the session's parameters), runs it under
`KernelHold` with `commit_sources=False`, answers every live output's path
(a gate's dead handles as `closed`) or, for `schema_only`, its columns, and
releases the graph's `FlowLogger`, log file and exchange folder. Closed
allowlist `HELD_NODE_TYPES` plus installed non-output custom nodes; writers
and other types still say "Push". A cell-built deferred node whose seed has
no columns asks core for them at build (`NotebookMode.schema_resolver`,
`native.resolved_seed`). A Python Script on another kernel runs this way
too, with its artifacts on the call's graph only.

Phase 2 landed 2026-10-03: **metadata lookups go through core.** One hook,
`notebook/lookup.py::metadata_lookup` (a `ContextVar`, the `placement_check`
pattern), consulted by `flow_graph._resolve_catalog_table_info` /
`_resolve_catalog_sql_tables`, `storage_backend.resolve_for_namespace`,
`subflow._registration` (behind `stamp_flow_reference` and
`resolve_subflow_path`), `prechecks.placement_refusal` and the frame's
`flowfile_frame/_metadata.py` (behind `catalog_reference.py`, `run_flow.py`,
`kernels.py` and the connection listings; every public function asks the hook
when set, else runs its `local_*` twin on the database). The kernel installs a
`_metadata.SessionLookup` for every op (`_metadata.installed`), which posts to
`POST /notebook/session/lookup`, a closed `lookup.KINDS` each answered by the
function the call site runs without the hook, as the kernel's owner, bound by
`kernel_runner._bound_kernel` (the flow need not be open), metadata only: no
plan, no storage credential, no password, no ciphertext. Secret-bearing nodes
are held in a kernel session and take phase 1's path: every
`cloud_storage_reader` (`NOTEBOOK_DEFERRED_NODE_TYPES`), a catalog reader of a
cloud-backed table (`_metadata.is_cloud_table`, from the `catalog_table`
answer), a custom node with a `SecretSelector` (`custom_node._selects_secret`);
`native._predicts_in_core` keeps `CONNECTION_SOURCE_TYPES` from running their
schema callback while a `schema_resolver` is set. The census is
`tests/notebook/test_kernel_database_census.py`, at zero: a pool `checkout`
listener under the sim's `IN_KERNEL_OP` marker over the whole corpus
(`conftest.kernel_db_opens`), plus every kind answered without a `$ffsec$` or
a decrypt.

Phase 3 landed 2026-10-03: **the copy is gone.** A notebook kernel holds no
catalog database: `_notebook_env` sets no `FLOWFILE_DB_PATH` (the kernel's
`get_database_url()` falls to the never-mounted `<storage>/database/`), and
every `notebook_kernel.handle()` first installs `_refuse_database`, a
`do_connect` listener on the one cached engine behind `connection.engine`,
`SessionLocal` and `get_db_context` (gated on `FLOWFILE_KERNEL_ID`, which only
a kernel container has), so a cell that opens the database itself stops with
`NO_DATABASE`, naming where it was opened (`_opened_from`: `a cell`, else the
frame module of a regression) and the `ff` functions to use, before pysqlite
could create a file. Deleted: `kernel/notebook_db.py`,
`POST /notebook/session/database`, `kernel_runner.refresh_database`, `_rearm`
and the refresh listeners, the `hello` schema-head handshake
(`migration.package_head`, `kernel_runner._schema_revision`; the flowfile
version check stays) and the gate's SQLite requirement
(`kernel_sessions_allowed` is now `not sharing_enabled()`, which admits an
electron app on a Postgres catalog without proving it). An uncached
`shared.database.create_catalog_engine()` a cell calls by hand is not
refused and creates an empty file in the container layer, as in any kernel
with `flowfile` installed. Proof: `test_kernel_database_census.py` (no op
connects), `test_kernel_session.py::test_a_notebook_kernel_refuses_to_open_a_catalog_database`
(scratch engine) and the real-kernel
`test_kernel_notebook_docker.py` (the default database file never appears in
the container; a `get_db_context()` cell is refused; a namespace core creates
shows in the next cell), which `.github/workflows/test-notebook-kernel.yml`
now runs on every frame, notebook or kernel change
(`FLOWFILE_REQUIRE_NOTEBOOK_KERNEL` makes a skip a failure).

Phase 4a landed 2026-10-03: **no host folder is mounted.** A notebook
kernel's container mounts what every kernel mounts, `/shared` and
`/catalog_tables`, and nothing else: not the Flowfile folders (flows, custom
nodes and their mount directories, catalog tables) and not a folder its owner
lists. Deleted: `kernel/notebook_mounts.py`, `mounted_folders`
(`models.MountedFolder`, the kernel form's folders field, migration 034's
column, dropped by 035; a pydantic model ignores it from an older client),
`FLOWFILE_NOTEBOOK_MOUNTS`, `notebook.kernel_path`,
`input_schema.kernel_file_path`, `KernelManager.host_folders`, the
`/host/<drive>/` Windows scheme, the key-store tmpfs, `FLOWFILE_STORAGE_DIR`
in `_notebook_env` (the kernel's storage is its own `~/.flowfile`,
`/root/.flowfile` in the image) and `_metadata.is_cloud_table` /
`SessionLookup.cloud_tables`. What replaced each use: a file a cell names is
never opened in the kernel (`notebook.paths_as_written` sets only
`keep_paths_as_written`, under which `native._kernel_hidden_path` hides every
local `read`/`list_files` path, `${param}` ones included) and core reads it
(the canvas twin's rows, else a held run); whether a bare path is a folder is
the `is_directory` lookup (`flow_frame_methods._resolve_scan_mode`); every
catalog reader in a kernel session is deferred (`native.notebook_defers`) and
its schema and rows come from core (`native._predicts_in_core`,
`CORE_PREDICTED_TYPES = CONNECTION_SOURCE_TYPES | {"catalog_reader",
"run_flow"}`); a `run_flow` reference's interface is the `flow_interface`
lookup (`_metadata.flow_interface`, core-side `resolve_subflow_path` +
`get_subflow_interface`); installed custom node files come through the
`custom_node_sources` lookup, which `notebook_kernel._mirror_custom_nodes`
writes as `<node_key>.py` into the kernel's own nodes folder before
`open`/`reset`/`execute`/`clean_run` (it removes only files it wrote and
rescans the registry once when anything changed; the kernel still runs a
local node's `process()` itself); a push stores paths as written and
`notebook/validate.host_file_paths` only recomputes `abs_file_path` on the
host. Invariant: **a kernel mounts exactly `/shared` and `/catalog_tables`; a
file path in a cell is a path on the user's machine that only core opens.**
Plain Polars or `open()` on a host path in a cell finds no file, and a cell
writes no file (an `ff` writer adds a writer node). Known cost: a catalog
table a cell reads is handed over whole as parquet through `/shared`, even
for `display()`. Proof: `tests/kernel/test_notebook_kernel_env.py` (exactly
the two binds, no `mounts` or `tmpfs`, none of `FLOWFILE_STORAGE_DIR`,
`FLOWFILE_NOTEBOOK_MOUNTS`, `FLOWFILE_DB_PATH`; an old client's
`mounted_folders` ignored), the sim tests in
`tests/notebook/test_kernel_canvas_rows.py` (a read is a held `read` run, a
bare folder asks `is_directory`) and `test_kernel_database_census.py` (a
catalog table, a flow file and a custom node reach the kernel through core;
the three new kinds answer metadata only), and the real-kernel
`test_kernel_notebook_docker.py::test_an_installed_custom_node_is_mirrored_into_the_kernel`
(with `test_a_file_a_cell_names_is_read_by_core`, where `pl.read_csv` on the
same path has no such file).

Remaining, each with its own plan: a Python Script on the notebook's own
kernel (`KernelHold` refuses it during a fallback), a push that keeps the
session's variables, a row-limited fallback for `display()` (which now also
bounds a catalog table a cell reads, until then handed over whole), Postgres
catalogs and docker-mode sessions (per-user access in the lookups, admin
gating on `node_run`), session artifacts visible to the canvas.

Known gaps this leaves until then: a seeded canvas node below one that just
ran keeps its seeded columns until it runs or the session is reset, and
`FlowFrame.collect()` on a deferred frame in a script still commits sources
(`flow_frame.py` calls `run_graph(node_ids=...)` with the default).

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
line numbers will drift fastest.

```bash
# system map / the worker reads core's catalog DB to resolve a plan's CredentialRef
grep -n "def cloud_connection_record\|get_catalog_engine()" shared/database.py
grep -n "class CredentialRefProvider\|def resolve_credential_ref\|def credential_ref_kwargs" shared/cloud_credential_provider.py
grep -rn "credential_ref_kwargs" flowfile_worker/flowfile_worker/ | grep -v __pycache__

# core-never-collects: god-file sizing
wc -l flowfile_core/flowfile_core/flowfile/flow_graph/*.py flowfile_core/flowfile_core/flowfile/flow_graph/builders/*.py \
      flowfile_core/flowfile_core/flowfile/flow_node/flow_node.py \
      flowfile_core/flowfile_core/flowfile/flow_data_engine/flow_data_engine.py

# default backend + legacy offload truthy parsing ("1" only)
grep -n "def default_backend_id" -A12 flowfile_core/flowfile_core/flowfile/execution/registry.py
grep -n "FLOWFILE_OFFLOAD_TO_WORKER" flowfile_core/flowfile_core/configs/settings.py

# worker wire-protocol endpoints
grep -n "@router" flowfile_worker/flowfile_worker/routes.py

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
grep -n "this core serves" flowfile_core/flowfile_core/flowfile/flow_graph/execution.py

# app version
grep -n '^version' pyproject.toml
```
