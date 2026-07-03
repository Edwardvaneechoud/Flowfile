---
name: flowfile-research-frontier
description: The maintainer's three long-horizon research bets for Flowfile (AI-native flow building, visual↔code round-trip, decentralized execution on a centralized catalog) with the verified existing assets each builds on, PR-sized first steps, falsifiable milestones, and the research discipline that turns a hunch into a merged change. Use when scoping ambitious/exploratory work, writing a design doc or spike, proposing a "big idea," choosing what to prototype next, evaluating whether an experiment is worth shipping, or asked "what is Flowfile trying to become" / "where is this going" / "research direction" / "north star."
---

# Flowfile Research Frontier

This is a **map of unbuilt territory**, not a description of shipped features. It names the
maintainer's three standing research bets (as of 2026-07-03, v0.12.7), the concrete repo assets
each one can stand on, and the discipline for taking a hunch to a merged change. Everything here
labeled *open* or *candidate* is deliberately not built yet — do not describe it as if it ships.

Use this skill to **scope and de-risk** exploratory work. It tells you the falsifiable milestone
to aim at and the first three PR-sized steps, so a spike produces a measurable result instead of a
demo. It does not replace the how-to skills — it points at them.

## When NOT to use this skill

- Actually shipping a new node → **flowfile-node-development**.
- Changing the AI agent/planner/providers → **flowfile-ai-subsystem** (owns all planner internals).
- Fixing a codegen ↔ graph-execution mismatch → **flowfile-codegen-parity-campaign**.
- The FlowFrame emission paths / stub pipeline → **flowfile-frame-and-codegen**.
- Env vars, feature flags, the `MutableBool` experiment-flag mechanism → **flowfile-config-and-flags**.
- How to prove a change is correct (test markers, isolation, the evidence bar) → **flowfile-testing-and-validation**.
- Getting a change merged (PRs, drift gates, review) → **flowfile-change-control**.
- Why a past idea was abandoned → **flowfile-failure-archaeology**.

If your task is to *build* one specific thing, you probably want a sibling above. Come here to
decide *what is worth building* and *how you will know it worked*.

## The three-word compass

The maintainer's frontier, verbatim intent:

- **A — AI-native flow building.** Agents that *reliably* build and edit pipelines, not demos.
- **B — Visual ↔ code round-trip.** Full two-way parity between visual flows, exported Python
  (raw Polars), and the FlowFrame API.
- **C — Decentralized execution on a centralized catalog.** Global components, catalog *metadata*
  in PostgreSQL, table *data* in S3 — and every local Flowfile instance brings its **own** compute,
  mixing local files and remote tables in complete harmony.

Each pillar below follows the same shape: **why the state of the art fails → the specific asset
Flowfile already has (verified) → three PR-sized first steps → a falsifiable milestone.**

---

## Pillar A — AI-native flow building

### Why current SOTA fails

AI flow-builders are demos. They generate a plausible pipeline once, on a clean canvas, and fall
over on the second turn: they hallucinate column names, loop on tool-name mismatches, and mutate a
live graph mid-run so a user's concurrent edit silently clobbers the model's work (or vice-versa).
There is no *staging*, no *drift detection*, no *atomic accept/reject* — so "reliable editing of an
existing pipeline" (the actual job) is unsolved.

### Flowfile's specific existing asset (VERIFIED, as of 2026-07-03 v0.12.7)

Flowfile already has the machinery those demos lack — the **diff-staged planner**:

- A real multi-turn agent package: `flowfile_core/flowfile_core/ai/agents/planner/`
  (`loop.py`, `insertion.py`, `recovery.py`, `staged_schemas.py`, …). Not a stub.
- Every LLM tool call goes through `execute_tool_call(...)` in
  `flowfile_core/flowfile_core/ai/tools/executor/dispatch.py:37`, which takes
  `mode: ExecutionMode = "apply"`. The planner dispatches with **`mode="stage"`** so the **live
  graph is never mutated mid-run** — steps accumulate as staged entries.
- On completion the staged steps bundle into one reviewable `GraphDiff` via
  `bundle_staged_results(...)` at `flowfile_core/flowfile_core/ai/diff.py:318`, accepted/rejected
  **atomically** in the UI.
- Drift is handled: the session keeps a graph snapshot (`sessions.capture_graph_snapshot`, re-taken
  on every resume path in `agents/planner/loop.py`); a user canvas edit mid-run emits a
  `drift_detected` event and parks the session in `paused_drift` status until an explicit resume.
- Rejected steps are retried by feeding the executor's `refusal_detail` back as a `role="tool"`
  message (`ai/tools/executor/refusals.py`), and there is a per-session dry-run cache
  (`ai/tools/dry_run.py`).

So Flowfile is *past* the demo layer. The frontier is **measured reliability** on top of it.
(Planner internals are owned by **flowfile-ai-subsystem** — read it before touching this code.)

### First three concrete steps (each PR-sized)

1. **Build a held-out benchmark harness.** Add a fixture corpus of `(natural_language_task,
   reference_flow)` pairs under `flowfile_core/tests/ai/`, driving `agents/planner/loop.py`
   end-to-end against a **recorded/replayed** provider (the shared `LiteLLMProvider` seam makes
   every call interceptable — see flowfile-ai-subsystem). Assert: staged `GraphDiff` applies
   cleanly **and** the applied flow executes to a frame equal to the reference. Report success rate
   and retry count. This makes "reliable" a number *before* you touch the agent.
2. **Instrument the refusal path.** Emit structured refusal-reason counters from
   `ai/tools/executor/refusals.py` across the benchmark, so "the agent is flaky" becomes a ranked
   table of *which* tool calls fail and *why*. Failure is free — do not make refusals raise.
3. **Add one executor-seam coercion for the top refusal reason**, behind an experiment flag (see
   flowfile-config-and-flags for the `MutableBool` pattern). Doctrine: when the model consistently
   emits a recoverable wrong shape, **normalize at the executor seam** (mirror the existing
   `_coerce_*` handlers in `ai/tools/executor/coercions.py`) — do **not** keep tightening prompts.
   Measure the benchmark success delta the coercion buys.

### Falsifiable milestone — you have a result when…

Over an N-task held-out benchmark, an agent given a task and a blank-or-partial graph produces a
staged diff that **(a)** applies cleanly and **(b)** executes to a frame equal to the human
reference (`assert_frame_equal`, order-insensitive), at a **measured success rate ≥ your
pre-registered threshold**, with **zero human tool-name corrections** and a mean retry count below a
pre-registered bound. If the harness cannot report those three numbers, you have a demo, not a
result.

---

## Pillar B — visual ↔ code round-trip

### Why current SOTA fails

Every visual ETL tool is a one-way street. It might *export* a script, but the export is a
lossy, un-reimportable artifact — edit the code and you can never get back to the diagram; edit the
diagram and the code silently drifts. There is no tool where the visual graph, hand-written code,
and a fluent API are **three renderings of one truth** that survive a round-trip.

### Flowfile's specific existing asset (VERIFIED, as of 2026-07-03 v0.12.7)

Flowfile has both directions of the bridge and one canonical DAG behind them:

- **Graph → code**: `flowfile_core/flowfile_core/flowfile/code_generator/code_generator.py` exposes
  `export_flow_to_polars(flow_graph)` (line 1602) and `export_flow_to_flowframe(flow_graph)`
  (line 1607), via `FlowGraphToPolarsConverter` / `FlowGraphToFlowFrameConverter`.
- **Code → graph**: `flowfile_frame` builds the **same** in-process `FlowGraph` as a side effect of
  every method call — native nodes where a schema exists, generated Polars-code nodes otherwise
  (`FlowFrame._add_polars_code`, `flowfile_frame/flowfile_frame/flow_frame.py:588`). The graph a
  frame builds is byte-identical in kind to what the Designer edits.
- **Graph → visual editor**: `open_graph_in_editor(flow_graph)` at
  `flowfile/flowfile/api.py:399` saves the frame's graph and opens it in the running Designer.
- **The parity discipline already exists**: the Descending-sort codegen bug (#544) was root-caused
  by *two* executors carrying separate direction parsers; the fix centralized it into
  `transform_schema.is_descending(...)` (`flowfile_core/flowfile_core/schemas/transform_schema.py:952`),
  used by **both** the code generator and the execution engine. And there is a real equivalence
  test: `flowfile_core/tests/flowfile/test_code_generator.py` parametrizes over
  `[export_flow_to_polars, export_flow_to_flowframe]` and asserts the generated code, when executed,
  yields a frame equal to `flow.get_node(N).get_resulting_data().data_frame`.

So the pieces exist; what is *open* is **full round-trip idempotence with zero lossy fallbacks**.
(Emission paths and the codegen bug family are owned by **flowfile-frame-and-codegen** and
**flowfile-codegen-parity-campaign** — this pillar is the parity campaign's north star.)

### The two lossy escape hatches that break round-trip (know your enemy)

1. **`UnsupportedNodeError`** in the code generator — a node with no codegen handler.
2. **The base64-serialized-LazyFrame fallback** in `flow_frame.py` `_add_polars_code`: when an
   expression is not convertible to source (e.g. an unresolvable lambda), the frame serializes a
   LazyFrame blob into the node and logs a warning ending *"a breaking graph when using the the
   ui"* (sic — the doubled "the" is in the source; grep `breaking graph when using`,
   `flow_frame.py` ~line 655). Such a node does not round-trip and the visual graph no longer
   matches the computation.

Any corpus flow that produces either is, by definition, not round-trippable.

### First three concrete steps (each PR-sized)

1. **Add a round-trip idempotence test.** Extend
   `flowfile_core/tests/flowfile/test_code_generator.py`: for a flow corpus, assert
   `export_flow_to_flowframe(g)` → re-imported through `flowfile_frame` → re-exported is
   **idempotent** (graph-shape or normalized-string stable), *and* both executions frame-equal.
   This surfaces the exact nodes that don't survive the trip.
2. **Fail the build on a lossy fallback.** Add a test/lint that fails when a corpus flow yields a
   base64-serialized-LazyFrame node or raises `UnsupportedNodeError` — converting "sometimes lossy"
   into a hard, counted gate.
3. **Add a native handler for the single most common `UnsupportedNodeError` node type** (in the
   converter + `code_generator/transform_handlers.py`), following the **one-shared-parser** rule
   (the `is_descending` pattern) so graph-exec and codegen can never diverge. Ship it with an
   equivalence test. Cross-ref flowfile-codegen-parity-campaign for the full bug-family playbook.

### Falsifiable milestone — you have a result when…

For every flow in a fixed corpus: `export_flow_to_flowframe(g)` re-imported rebuilds `g'` such that
`export_flow_to_flowframe(g') == export_flow_to_flowframe(g)` (idempotent), **both** `g` and `g'`
execute to `assert_frame_equal` results, and the corpus produces **zero `UnsupportedNodeError` and
zero base64-serialized-LazyFrame nodes**. Measurable check: the extended parametrized test is green
with those three counters at zero.

---

## Pillar C — decentralized execution on a centralized catalog

### Why current SOTA fails

Every catalog/lakehouse today assumes **central compute**: the catalog service *owns* the engine
that reads and writes the data. "Decentralized" offerings still funnel queries through a shared
cluster. Nobody offers: shared metadata + shared object-store data, where each participant brings
**their own local compute** and treats a local Parquet file and a remote S3 Delta table as
first-class peers in one pipeline, with credentials that never leak between participants.

### Flowfile's specific existing assets (VERIFIED, as of 2026-07-03 v0.12.7)

Three seams are already in the right shape:

- **Data can already live in object storage, per-catalog.** Migration **028**
  (`028_catalog_namespace_storage.py`) added `storage_uri` + `storage_connection_name` to level-0
  (catalog-root) namespaces. `flowfile_core/flowfile_core/catalog/storage_backend.py`
  `resolve_for_namespace(...)` (line 123) resolves a cloud `CatalogStorageTarget` when a URI is set,
  resolving the `CloudStorageConnection` **as the catalog owner** (`owner_id = root.owner_id`,
  line 107) — never the calling user. `FLOWFILE_CATALOG_STORAGE_URI` / `_CONNECTION` remain a
  **creation-time default** for new catalogs (`catalog/services/namespaces.py:_env_default_storage`,
  line 148); `resolve_catalog_storage(_user_id, ...)` (line 137) is now a shim that **ignores its
  user_id**. (Env-var mechanics are owned by flowfile-config-and-flags.)
- **The worker reads cloud tables with its own compute.** `flowfile_worker/catalog_reader.py`
  `open_catalog_table(...)` (line 29) does `pl.scan_delta(validate_catalog_uri(...), storage_options=...)`
  for cloud and local roots alike. Local compute reading remote data is already the code path.
- **The catalog-DB seam is a single function.** `get_database_url()` at
  `shared/storage_config.py:402` is the one place every catalog-DB consumer resolves its URL
  (core `database/connection.py`, `database/migration.py`, `alembic/env.py`, the scheduler's
  `engine.py`, `shared/run_completion.py`). Today it only ever emits `sqlite:///…`.
- **Secrets are already instance-independent.** A stored secret is
  `$ffsec$1$<user_id>$<fernet_token>` — the worker (and any peer) re-derives the per-user key from
  the shared master key **with zero core round-trip** (`secret_manager.py` ↔ `flowfile_worker/secrets.py`,
  byte-for-byte parallel). Sharing/decentralizing does **not** require re-encryption. (This contract
  is owned by flowfile-architecture-contract.)

What is **open**: `get_database_url` only speaks SQLite, and there is no two-instance harmony test.
The metadata DB is deliberately always-local today; migration 028 is forward-only with no backfill.
Also open: **global components** — user-defined components today are per-instance `.py` files under
`<user_data>/user_defined_nodes/` (`shared/storage_config.py:109-114`, loaded into
`CUSTOM_NODE_STORE`); sharing them through the central catalog is a candidate follow-on to steps
1–3 below, with no design yet.

### What PostgreSQL actually needs (probed empirically, 2026-07-03)

The full Alembic chain 001→028 **was run against a disposable PostgreSQL 16** (monkeypatching
`shared.storage_config.get_database_url` to a `postgresql+psycopg2://` URL before importing
`flowfile_core`, with `FLOWFILE_DB_PATH` pointed at a fresh scratch SQLite so the import side stays
isolated — never set `FLOWFILE_SKIP_STARTUP_MIGRATION` for this). Results, so step 1 is honest:

- **Migration 001 applies clean on Postgres.** The chain's verified first failure is **002**:
  `ALTER TABLE catalog_tables ADD COLUMN is_optimized BOOLEAN DEFAULT 0` →
  `psycopg2.errors.DatatypeMismatch: column "is_optimized" is of type boolean but default
  expression is of type integer` (`002_virtual_flow_tables.py:26`, `server_default=sa.text("0")`).
- **Exactly four migration files carry SQLite-isms**; with those shimmed, the chain reaches 028 and
  `init_db()` even seeds the default user on Postgres (35 tables, `alembic_version` = `028`):
  1. `002_virtual_flow_tables.py:26` — Boolean column with integer `server_default=sa.text("0")`.
  2. `016_flow_api_endpoints.py:41,64` — same class, `server_default=sa.text("1")` on `enabled`.
  3. `026_project_uniqueness_and_uuid_not_null.py` — **three** sites: the dedup SELECT/UPDATE
     using integer booleans (`is_active = 1/0`, lines 46–51), a **hand-written SQLite
     `CREATE TABLE _wp_new` rebuild** using `DATETIME` and `BOOLEAN DEFAULT 0/1` (lines 81–97 —
     invisible to any type-level grep; Postgres fails with `type "datetime" does not exist`), and
     the partial index `WHERE is_active = 1` (line 130).
  4. `020_user_groups_sharing.py:45-48` — `SET is_public = 1` backfill. **Dormant on a fresh DB**
     (it only runs when a `local_user` row and a 'General' namespace already exist), so it never
     fires when bootstrapping Postgres from empty — fix it anyway for data-bearing paths.
- **Everything else is already dialect-clean**, verified by the same run: every
  `op.batch_alter_table` (degrades to plain ALTER off SQLite — do not rewrite them),
  `render_as_batch=True` in `alembic/env.py:27,47` (harmless on Postgres — keep it), the raw
  backfills in 006/010/012/018/024, and all model column types. Every downstream engine creator
  already gates `connect_args={"check_same_thread": False}` on `"sqlite" in url`
  (`flowfile_core/database/connection.py:25`, `flowfile_scheduler/flowfile_scheduler/engine.py:50-52`,
  `shared/run_completion.py:29-30,49-50`), so consumers need no change.
- **Seam changes beyond the migrations:**
  - `get_database_url()` (`shared/storage_config.py:402`) wraps `FLOWFILE_DB_PATH` in `sqlite:///`
    unconditionally — minimal change: pass values containing `://` through as full URLs.
  - `database/migration.py:_catalog_db_exists()` (line 84) is a **file-existence** check that
    returns `False` for any non-SQLite URL, so every Postgres boot takes the fresh/legacy branch.
    Safe today only because `get_legacy_database_path()` returns `None` when `FLOWFILE_DB_PATH` is
    set; if the legacy branch ever fired on Postgres, `migrate_data_from_legacy_db()` executes
    `PRAGMA foreign_keys = OFF/ON` (lines 265, 280) → hard error. Make the existence check
    `alembic_version`-based for server URLs.
  - `psycopg2-binary` is **dev-group only** (`pyproject.toml:112`) — shipping Postgres support
    needs a runtime dependency (or an optional extra); today no driver ships.
- **The hand-edit tension:** flowfile_core doctrine forbids editing shipped migrations. A
  dialect-portability edit is the narrow exception, and only with proof SQLite behavior is
  unchanged — runnable check: run the chain on a fresh SQLite before and after the edit
  (`FLOWFILE_DB_PATH=$(mktemp -d)/probe.db poetry run python -c "import flowfile_core"`) and diff
  the two `sqlite3 <db> .schema` dumps; they must be byte-identical.
- **Test assets already provisioned:** `test_utils/postgres/` fixture with poetry scripts
  `start_postgres` / `stop_postgres`, and `testcontainers ^4.10.0` in the dev group.

### The one security wrinkle you must respect

Polars serializes `storage_options` (i.e. **decrypted credentials**) *inline* into a serialized
LazyFrame blob. `serialized_frame_uses_cloud(blob)` (`storage_backend.py:31`) exists precisely to
catch this: a cloud-scan blob **must never be replayed** — re-run the producer instead. A
decentralized catalog makes this sharper: a replayed blob would ship one instance's credentials to
another. Any cross-instance data path must route through `resolve_for_namespace` (owner-keyed
credentials, worker re-derives), never through a shipped blob.

### First three concrete steps (each PR-sized)

1. **Generalize the metadata seam.** The exact worklist is known (see the probe above): (a) teach
   `get_database_url()` (`shared/storage_config.py:402`) to pass `://`-bearing values through as
   full URLs; (b) make `_catalog_db_exists()` (`database/migration.py:84`) `alembic_version`-based
   for server URLs; (c) fix the four migration files (002, 016, 026, 020) with SQLite-identical
   replacements (`sa.false()`/`sa.true()` for the defaults, `TRUE/FALSE` literals or boolean-typed
   bindparams for the raw SQL, portable types in 026's `_wp_new` DDL); (d) add a
   `docker_integration`-marked test that boots Postgres (testcontainers or `test_utils/postgres/`)
   and runs `run_alembic_upgrade()` against it. Expected observations: before the fixes the chain
   dies at 002 with `psycopg2.errors.DatatypeMismatch` on `is_optimized`; after, `SELECT
   version_num FROM alembic_version` returns `028` and the SQLite before/after `.schema` diff is
   empty. Keep `render_as_batch=True` — it is a no-op off SQLite. (Migration mechanics:
   flowfile-testing-and-validation + flowfile-change-control.)
2. **Prove two-instance data sharing on the already-built cloud path.** Add an integration test:
   two core processes sharing one catalog DB (via step 1's Postgres URL) and one MinIO bucket via a
   per-namespace `storage_uri`/`storage_connection_name` (migration 028). Assert instance B **sees**
   and **reads** a table instance A wrote, using instance B's **own** worker. (MinIO is already a
   `test_utils/` fixture.)
3. **Mixed local + cloud join, credential-blob guard hardened.** Add a flow that joins a local
   Parquet file with a remote S3 Delta catalog table in one graph, and add an assertion that no
   cloud-backed catalog read is ever served from a replayed blob across instances
   (`serialized_frame_uses_cloud` must gate it). Cross-ref flowfile-architecture-contract for the
   worker-re-derives-secrets contract.

### Falsifiable milestone — you have a result when…

Two independent local Flowfile instances (each with its own worker/compute), pointed at one
PostgreSQL catalog and one S3 data root, can both **list, read, and write** catalog tables; a single
flow **joins a local file with a remote S3 table** and produces a correct frame; and **neither
instance's worker ever holds the other's credentials in a replayable form**. Measurable check: an
integration test with two core processes sharing a Postgres `get_database_url` + one MinIO bucket,
asserting cross-instance visibility, a mixed local+cloud join result, and zero cloud-blob replays.

---

## Research methodology — hunch → merged change (or documented retirement)

The frontier is speculative; the *discipline* is not. A change earns its way in here the same way
every merged change does. Follow this or your spike stays a spike.

### 1. Predict numbers before you run

State the hypothesis as a **quantity with a direction and a threshold**, written down *before* the
experiment: "the executor coercion in Pillar A/step 3 lifts benchmark success from X% to ≥Y%," or
"the native handler in Pillar B/step 3 drops corpus `UnsupportedNodeError` count from N to 0." A
hypothesis that can only be evaluated *after* seeing the output is not a hypothesis. Each pillar's
milestone above is already written this way — copy that shape. Worked example: Pillar C's Postgres
probe *predicted* (from a static grep) that the chain's first failure would be migration 002's
boolean default; the live run against Postgres 16 confirmed exactly that failure, then surfaced
026's hand-written DDL that no grep predicted — the prediction earned trust, the surprise got
recorded.

### 2. One mechanism must explain ALL observations — including the negatives

A result is not "it got better on the cases I hoped for." The mechanism you propose must account for
the **regressions and the no-ops too**. If your coercion helps 8 tasks and breaks 2, the *same*
causal story must explain the 2 — otherwise you have two mechanisms and understand neither. Flowfile
history rewards this: the VizSessionRegistry 504s were only truly fixed once one mechanism
(response-queue theft between two parents sharing a key) explained *every* symptom, not just the
common one.

### 3. Survive an assigned adversarial-refutation pass

Before proposing the change, have someone (or yourself, in a separate pass) **try to kill it**:
find the input that breaks the number, the confound that explains the delta without your mechanism,
the negative case you didn't run. This repo has a documented failure mode for skipping this —
Windows E2E tests that were green while testing nothing (OR-instead-of-AND on startup signals,
`test.skip()` on launch failure, error-swallowing `.catch(() => false)`). A result that hasn't
survived an attempt to refute it is that green-but-empty test.

### 4. The idea lifecycle (where the gates live)

```
hunch
  └─► experiment flag        (MutableBool / env gate — see flowfile-config-and-flags)
        default OFF; ship the plumbing dark so it can't regress the default path
  └─► validation             (the evidence bar — see flowfile-testing-and-validation)
        real integration tests over mocks; the pre-registered number must clear its threshold;
        survives the adversarial pass
  └─► EITHER  adopted change  (through flowfile-change-control: PR, drift gates, review)
      OR       documented retirement (write it up in flowfile-failure-archaeology so the next
               person doesn't re-run the dead experiment)
```

A retired idea is a *deliverable*, not a failure — the repo's abandoned directions (Airbyte
connector, in-house fuzzy matching extracted to an external package, the walked-back FastAPI upgrade,
the whole Electron toolchain) are load-bearing knowledge. Retire loudly.

### 5. Where good ideas have actually come from here

Not from greenfield brainstorming — from **user pain and production bugs**. The entire Alembic
migration system was born from a production incident: a run-type mismatch between local and Docker
databases (fixed by migration `006_normalize_run_type.py`) forced the maintainer to "implement a
mechanism to downgrade the db," which became versioned migrations. The uuid columns
(`flow_uuid`, viz/dashboard uuids) trace to a concrete SQLite rowid-reuse bug pulling one flow's
runs into another's history. The DST-correct cron cursor traces to a real double-fire risk. **The
best frontier work starts from an observed failure, then generalizes** — when scoping a pillar, look
first for the smallest real pain it removes, and make *that* your first PR.

---

## Provenance and maintenance

Re-verify volatile facts before relying on them (all commands read-only, run from repo root). Facts
are stamped as of **2026-07-03 (v0.12.7)**.

| Claim | One-line re-verification |
|---|---|
| Planner package + `mode="stage"` staging exists | `ls flowfile_core/flowfile_core/ai/agents/planner/ && grep -n 'mode: ExecutionMode' flowfile_core/flowfile_core/ai/tools/executor/dispatch.py` |
| `bundle_staged_results` → `GraphDiff` | `grep -n 'def bundle_staged_results' flowfile_core/flowfile_core/ai/diff.py` |
| Executor coercion + refusal seams | `ls flowfile_core/flowfile_core/ai/tools/executor/ ` (expect `coercions.py`, `refusals.py`, `dispatch.py`) |
| Graph→code exporters | `grep -n 'def export_flow_to_polars\|def export_flow_to_flowframe' flowfile_core/flowfile_core/flowfile/code_generator/code_generator.py` |
| Code→graph frame bridge | `grep -n 'def _add_polars_code' flowfile_frame/flowfile_frame/flow_frame.py` |
| Graph→editor entry point | `grep -n 'def open_graph_in_editor' flowfile/flowfile/api.py` |
| One shared sort-direction parser (parity discipline) | `grep -n 'def is_descending' flowfile_core/flowfile_core/schemas/transform_schema.py` |
| Codegen equivalence test parametrized over both exporters | `grep -n 'export_flow_to_polars, export_flow_to_flowframe' flowfile_core/tests/flowfile/test_code_generator.py` |
| Per-catalog object storage (migration 028) | `ls flowfile_core/flowfile_core/alembic/versions/ | grep 028` |
| `resolve_for_namespace` / owner-keyed creds / `resolve_catalog_storage` shim | `grep -n 'def resolve_for_namespace\|owner_id = root.owner_id\|def resolve_catalog_storage' flowfile_core/flowfile_core/catalog/storage_backend.py` |
| Env vars are creation-time default only | `grep -n 'FLOWFILE_CATALOG_STORAGE_URI\|FLOWFILE_CATALOG_STORAGE_CONNECTION' flowfile_core/flowfile_core/configs/settings.py` |
| Worker reads cloud catalog tables with its own compute | `grep -n 'def open_catalog_table' flowfile_worker/flowfile_worker/catalog_reader.py` |
| `get_database_url` is the single catalog-DB seam (SQLite-only today; wraps `FLOWFILE_DB_PATH` in `sqlite:///`) | `sed -n '402,420p' shared/storage_config.py` |
| The four Postgres-blocking migrations (002/016/026/020) | `grep -n 'server_default=sa.text' flowfile_core/flowfile_core/alembic/versions/0{02,16}_*.py; grep -n 'DATETIME\|is_active = 1' flowfile_core/flowfile_core/alembic/versions/026_*.py; grep -n 'is_public = 1' flowfile_core/flowfile_core/alembic/versions/020_*.py` |
| `_catalog_db_exists` is file-based; legacy copy uses PRAGMAs | `grep -n 'def _catalog_db_exists\|PRAGMA foreign_keys' flowfile_core/flowfile_core/database/migration.py` |
| Engine creators already dialect-gate `check_same_thread` | `grep -rn 'check_same_thread' flowfile_core/flowfile_core/database/connection.py flowfile_scheduler/flowfile_scheduler/engine.py shared/run_completion.py` |
| Postgres driver is dev-group only; test fixture exists | `grep -n 'psycopg2-binary\|start_postgres' pyproject.toml && ls test_utils/postgres/` |
| Components are per-instance files (global components open) | `grep -n 'def user_defined_nodes_directory' shared/storage_config.py` |
| Secrets worker-re-derivable format | `grep -n 'SECRET_FORMAT_PREFIX\|KEY_DERIVATION_VERSION' flowfile_core/flowfile_core/secret_manager/secret_manager.py flowfile_worker/flowfile_worker/secrets.py` |
| Cloud-blob replay guard | `grep -n 'def serialized_frame_uses_cloud' flowfile_core/flowfile_core/catalog/storage_backend.py` |
| Migration born from a production bug (methodology §5) | `head -20 flowfile_core/flowfile_core/alembic/versions/006_normalize_run_type.py` |

**Candidate / pending direction (do not treat as current truth):** an unmerged architecture branch
introduces an `ExecutionBackend` seam (local vs worker compute), a `WorkerTransport` owner of worker
URLs, and a `NodeSpec` registry with a declarative `_add_from_spec` node path. As of 2026-07-03
these names return **nothing** on `main`
(`grep -rl 'class ExecutionBackend\|class WorkerTransport\|class NodeSpec' flowfile_core/flowfile_core` is empty);
re-run that grep before citing them, and check whether the branch has merged — if it has, it reshapes
how nodes and execution are added and Pillars A/C should be re-scoped against it.

**When the frontier moves:** if a pillar ships, demote it out of this skill into the owning sibling
(A→flowfile-ai-subsystem, B→flowfile-codegen-parity-campaign, C→flowfile-architecture-contract) and
replace it here with the *next* open problem. Keep this skill about what is unbuilt.
