# Discovery Dossier — KEY=subsystems-contracts

> **FROZEN EVIDENCE** — snapshot at commit `f6963c77` (2026-07-03, v0.12.7); deliberately unmaintained and expected to drift.
> Authority order: **live repo → `.claude/skills/` → this file (leads only — re-verify before citing).** See [`README.md`](./README.md).

Cross-cutting subsystem contracts of the Flowfile monorepo, verified against source at
`Flowfile` (branch `improvement/improve-naming-unnamed-flows`,
HEAD `fa23a297`). All paths below are repo-relative unless absolute. Every claim was verified by
reading the file or running a read-only grep/ls; items I could not fully verify are marked
**(inferred)**.

---

## 1. AI subsystem (`flowfile_core/flowfile_core/ai/`)

### Surface map
- `ai/__init__.py` exposes exactly one public symbol: `router` (mounted in `main.py`). Before any
  import of routes it does `os.environ.setdefault("LITELLM_LOCAL_MODEL_COST_MAP", "True")`
  (`ai/__init__.py:31`) so litellm's first import uses the bundled cost map and never dials
  raw.githubusercontent.com. This module-level set is guaranteed to run before the first
  `import litellm` **because all litellm imports are lazy**.
- Package is large: ~30 route modules (`*_routes.py`: admin, agent, autocomplete, byok, chat,
  command_palette, cron, diff, docgen, generate, inline_action, intent_router, lineage,
  local_model, node_codegen, run_failure, suggest_next_node) plus `agents/`, `providers/`,
  `tools/`, `context/`, `prompts/`, `local_model/`.

### Feature flag gate
- `ai/feature_flag.py` — two functions:
  - `is_ai_enabled()` (`feature_flag.py:39-49`) reads `settings.FEATURE_FLAG_AI` (a live
    `MutableBool`) on every call.
  - `require_ai_enabled()` (`feature_flag.py:52-71`) is a FastAPI dependency wired at router level
    (`APIRouter(dependencies=[Depends(require_ai_enabled)])` in `ai/routes.py`); raises
    `HTTPException(503, detail="AI features are disabled. Set FEATURE_FLAG_AI=true to enable.")`
    (`DISABLED_DETAIL`, `feature_flag.py:36`).
- Default is **on**: `configs/settings.py:25-26` —
  `FEATURE_FLAG_AI: MutableBool = MutableBool(os.environ.get("FEATURE_FLAG_AI", "1").strip().lower() in ("true","1","yes","on"))`.
- The flag is runtime-flippable via `settings.FEATURE_FLAG_AI.set(...)`; an admin endpoint mounts
  under `/system` (not `/ai`) precisely so it works while the AI router is 503ing.

### Agents
- `agents/assist.py` (9 lines) and `agents/copilot.py` (10 lines) are **docstring-only stubs**
  ("Stub."); the real single-shot/next-step behavior lives in the route modules
  (`chat_routes.py`, `suggest_next_node.py`, `autocomplete.py`, `command_palette.py`, etc.).
  Don't send anyone to `agents/assist.py` looking for implementation.
- `agents/planner/` is the real multi-turn agent — a package (`__init__.py`, `_internal.py`,
  `catalog.py`, `coercions.py`, `insertion.py`, `llm_replies.py`, `loop.py`, `messages.py`,
  `rationale.py`, `recovery.py`, `staged_schemas.py`). Key contract from the package docstring
  (`agents/planner/__init__.py:1-46`):
  - session opened via `POST /ai/agent/start`; tool catalog narrowed per surface
    (`surface="agent"` = two-stage pick_category; `surface="agent_complex"` = full catalog).
  - every LLM tool call dispatched through `execute_tool_call` with **`mode="stage"`** — the live
    graph is never mutated mid-run.
  - graph snapshot before every dispatch; user canvas edits mid-run ⇒ `drift_detected` + `paused`
    events, session waits for `POST /ai/agent/{session_id}/resume`.
  - rejected steps retried up to `max_retries_per_step` by feeding executor `refusal_detail` back
    as a `role="tool"` message.
  - on completion staged steps bundle into one `GraphDiff`
    (`flowfile_core.ai.diff.bundle_staged_results`) reviewed/accepted atomically in the UI.
  - the planner generator **never raises** — every failure becomes a `PlannerEvent`
    (`"error"` / `"tool_call_rejected"` / `"drift_detected"` / `"abort"`); SSE `id:` headers are
    `f"{session_id}.{step_count}"` for `Last-Event-ID` replay.
  - The old `planner.py` public API (including underscored helpers) is re-exported verbatim from
    the package `__init__.py` — tests import them directly.

### Providers / litellm lazy-import contract
- `providers/registry.py:33-40` — `PROVIDERS = {"anthropic", "openai", "google", "groq",
  "openrouter", "ollama"}` mapping to per-vendor subclasses of `LiteLLMProvider`. Vendor classes
  are config-only (name, `default_model`, `model_prefix`, capability flags, `surface_models`,
  `default_api_base`).
- The **local** llama.cpp pseudo-provider (`providers/local.py`, `LOCAL_PROVIDER_ID`) is
  deliberately **not** in `PROVIDERS` (so it never appears in BYOK credential CRUD);
  `is_resolvable_provider()` (`registry.py:84-94`) = `PROVIDERS ∪ {local}` and is what read-only
  routes use; the tool-calling agent route rejects local explicitly.
- **The lazy contract (the real rule):** *no module in `ai/` imports litellm at module level.*
  Verified by grep — exactly two lazy imports exist in package source:
  - `providers/_litellm_base.py:46` — `import litellm` inside `_lazy_litellm()` (also sets
    `litellm.suppress_debug_info = True` idempotently on each call, `_litellm_base.py:34-49`).
  - `ai/scheduler.py:169` — `from litellm import exceptions as lle  # lazy` inside a function.
  - **Correction to root CLAUDE.md:** it says "Keep the package litellm-import-free except
    `ai/byok.py`" — `byok.py` contains **no** litellm import at all (verified). The core-level
    CLAUDE.md has it right. `byok.py`'s separation is about keeping `ai/credentials.py`
    provider-class-import-free (its own docstring, `byok.py:1-9`).
- **Enforcing tests** (`flowfile_core/tests/ai/`):
  - `test_classification.py:79-86` `test_classify_lazy_litellm` — asserts no `litellm*` module in
    `sys.modules` after importing classification.
  - `test_dry_run.py:180-184` `test_dry_run_lazy_litellm_contract` — same pattern for dry_run.
  - `ai/scheduler.py` docstring also states "Tests verify `flowfile_core.ai.scheduler` import does
    not pull litellm into sys.modules."
  Re-adding any eager `import litellm` under `ai/` breaks these tests.
- `LiteLLMProvider.chat()`/`stream()` (`_litellm_base.py:142-249`) are the single dispatch seam:
  they translate Pydantic `Message`/`ToolSpec` ⇄ litellm dict shapes, aggregate streamed tool-call
  fragments in `_PartialToolCall` buffers (a tool call is surfaced only when id+name present and
  arguments parse as JSON), and in `finally` blocks run two never-crash hooks:
  `_record_call_telemetry` (always-on counter `flowfile_ai_provider_call_total`) and
  `_record_chat_log`/`_record_stream_log` (prompt log, only when enabled). All hook imports are
  function-local; failures are swallowed with a warning (`_litellm_base.py:423-546`).

### BYOK (`ai/byok.py` + `ai/credentials.py`)
- `credentials.py` = DB CRUD + Pydantic schemas, **provider-import-free** (its docstring:
  "verified by a snapshot test"). API keys are stored as `Secret` rows via the normal
  `secret_manager` (`encrypt_secret`/`decrypt_secret`), secret name convention
  `ai:{provider}:api_key:{user_id}:{credential_id}`; rotation mutates
  `Secret.encrypted_value` in place (secret id stable). Curated `models` list is JSON-in-Text.
  Explicit `clear_api_key` / `clear_models` flags are mutually exclusive with providing a value
  (422).
- `byok.get_configured_provider(db, user_id, provider, *, surface, model)`
  (`byok.py:88-153`) — model resolution order (documented `byok.py:10-31`):
  1. explicit `model=` arg; 2. credential row `default_model`; 3. provider class
  `surface_models[surface]` **if** in the user's curated list; 4. first curated model;
  5. class `surface_models[surface]`; 6. class `default_model`.
- Env fallback: `_PROVIDER_ENV_VARS` (`byok.py:56-63`) — anthropic→`ANTHROPIC_API_KEY`,
  openai→`OPENAI_API_KEY`, google→`GEMINI_API_KEY`/`GOOGLE_API_KEY`, groq→`GROQ_API_KEY`,
  openrouter→`OPENROUTER_API_KEY`, ollama→() (never needs a key).
  `ProviderNotConfiguredError` raised only when: no cred row AND no env var AND no class
  `default_api_base` AND provider != "ollama" (`byok.py:140-141`).

### Rate limiting (`ai/scheduler.py`)
- Per-provider sliding-window RPM/RPD via `FLOWFILE_AI_<PROVIDER>_RPM` / `_RPD` env vars (unset ⇒
  no enforcement); honors `Retry-After` on 429; exponential backoff 2/4/8/16s, max 4 retries,
  ±25% jitter. **State is per-process** — under multi-worker deploys effective limit ≈ N × config.
  Opt-in primitive (`with_provider_retry(provider, ...)`); `byok.get_configured_provider` is NOT
  auto-wrapped. No audit writes from retries ("failure is free").

### Prompt log (`ai/prompt_log.py`)
- Gated by `FLOWFILE_AI_LOG_PROMPTS` (MutableBool, default off; `settings.py:37-38`);
  scrub gated by `FLOWFILE_AI_LOG_PROMPTS_SCRUB` (`settings.py:42-43`).
- Writes `{storage.base_directory}/ai_prompts/YYYY-MM-DD.jsonl` (`LOG_SUBDIR = "ai_prompts"`),
  one line per LLM round-trip, rolls on **UTC** date.
- `MAX_MESSAGES_BYTES = 256 * 1024`; truncation keeps system prompt + last
  `KEEP_RECENT_TURNS = 5` messages, stubs older bodies with `[...truncated, len=N chars]`.
- Scrub mode masks user/tool message bodies only; system + assistant stay verbatim.
- CLI: `python -m flowfile_core.ai.prompt_log tail 20` / `grep PATTERN [SURFACE]`.

### Local model (`ai/local_model/manager.py`)
- On-demand llama.cpp `llama-server` + GGUF (default Qwen2.5-Coder-3B, ~2 GB), pinned build
  `LLAMACPP_BUILD = "b9305"` from `ggml-org/llama.cpp`. Downloads into
  `storage.local_model_directory` (`~/.flowfile/local_model`) **only when user opts in** via
  `/ai/local-model/*` routes — the dir is deliberately NOT in `_ensure_directories`.
  Module-level singleton, one server at a time; `LocalProvider` resolves the live port lazily on
  first `stream` call. Shut down in core's lifespan (`main.py` `_shutdown_local_model`).

### How to extend safely
- New vendor: add a config-only subclass under `providers/`, register in `registry.PROVIDERS`,
  add its env var(s) to `byok._PROVIDER_ENV_VARS`. Never import litellm at module scope.
- New AI route: include it under the `/ai` router so it inherits `require_ai_enabled`; add auth
  per-route (`Depends(get_current_active_user)`) — the flag gate is orthogonal to auth.

---

## 2. Secrets (`flowfile_core/.../secret_manager/secret_manager.py` + `flowfile_worker/.../secrets.py`)

### The contract
- Wire format: `$ffsec$1$<user_id>$<fernet_token>` — `SECRET_FORMAT_PREFIX = "$ffsec$1$"`
  (core `secret_manager.py:22`, worker `secrets.py:24`).
- Key derivation: `HKDF-SHA256(master_key, length=32, salt=KEY_DERIVATION_VERSION, info=f"user-{user_id}")`,
  base64-urlsafe → Fernet key. `KEY_DERIVATION_VERSION = b"flowfile-secrets-v1"`
  (core `secret_manager.py:18`, worker `secrets.py:21`). The salt doubles as the scheme version
  tag; `$1$` in the prefix is the format version.
- The embedded user_id is the whole point: **the worker re-derives the key with zero user
  context** — `flowfile_worker/secrets.py:decrypt_secret` (172-203) parses the prefix, extracts
  `embedded_user_id`, derives, decrypts. No HTTP call to core; only the shared master key.
- These two files are **byte-for-byte parallel implementations**. Worker CLAUDE.md:
  "keep these byte-for-byte in sync with core's secret module or decryption breaks."
  Changing prefix/salt/HKDF-info format requires migrating both sides simultaneously.
- Legacy fallback: values not starting with the prefix are raw Fernet tokens. Core's
  `decrypt_secret(value, user_id=None)` (87-123): tries per-user key if `user_id` given, else
  master key directly. Worker's legacy path always uses the master key directly.

### Master key resolution
- Core: `flowfile_core/auth/secrets.get_master_key` (not read here in full — **(inferred)** same
  logic family as worker's). Worker (`secrets.py:113-143`):
  1. `TEST_MODE` ⇒ fixed test key `06t640eu3AG2FmglZS0n0zrEdqadoT7lYDwgSmKyxE4=` (line 129).
  2. `FLOWFILE_MODE=docker` ⇒ `FLOWFILE_MASTER_KEY` env (validated as Fernet key) or Docker secret
     file `/run/secrets/flowfile_master_key`; missing ⇒ RuntimeError.
  3. Else (electron/local): platform secure storage — `$APPDATA|~/.config/flowfile/.secret_key`
     encrypting `flowfile.json.enc`, `get_password("flowfile", "master_key")`.

### Own-first, group-shared fallback resolution
- `get_encrypted_secret(current_user_id, secret_name)` (`secret_manager.py:126-152`):
  1. own row `(user_id, name)`, `ORDER BY id ASC` (name is NOT unique at DB level — lowest id is
     the deterministic winner; an own secret always shadows a granted one);
  2. else group-granted rows by name via `sharing.granted_resource_ids(db, user_id, "secret")`,
     again lowest id wins. The returned ciphertext stays owner-keyed — no re-encryption for
     sharing, which is exactly why worker/scheduler needed zero changes for the sharing feature.
- `upsert_secret` (170-187) updates the **lowest-id own row** (the one resolution returns) rather
  than inserting a duplicate — this is how placeholder secrets get filled.
- `delete_secret` (190-203) calls `sharing.delete_grants_for_resource(db, "secret", id)` before
  `db.delete` — mandatory pattern (see §3).

### What breaks when violated
- Prefix/salt drift between core and worker ⇒ worker throws `InvalidToken` on every secret-using
  job (DB/cloud/Kafka/GA connectors) while core still works — a confusing split-brain failure.
- Re-encrypting a shared secret under the grantee's key would break the owner and the worker;
  rotation paths therefore always re-encrypt under the **owner's** user_id
  (`db_connections.py:369-370` comment: routes authorize manage-grantees then pass the OWNER's
  user_id).

---

## 3. Group-based sharing (`auth/sharing.py`, `catalog/access.py`, migration 020)

### Data model (migration `020_user_groups_sharing.py`)
- Three tables: `user_groups` (unique name), `user_group_memberships` (unique
  `(group_id, user_id)`, role ∈ owner/manager/member), `resource_grants`
  (polymorphic: `resource_type` string + `resource_id` int + `group_id` + `permission` +
  `granted_by`). Plus `catalog_namespaces.is_public` column.
- 020 backfills `is_public=1` for the seeded system namespaces (General catalog and its
  `default` / `Unnamed Flows` / `Local Flows` schemas, owned by `local_user`) so they remain
  visible once the catalog becomes private-by-default in docker mode. Migration is idempotent
  (`_has_table`/`_has_column` guards).

### Authorization semantics (`auth/sharing.py`, 393 lines)
- `sharing_enabled()` (`sharing.py:73-76`) = `os.environ.get("FLOWFILE_MODE","electron") != "electron"`,
  read **per call** (comment: `configs.settings` caches FLOWFILE_MODE at import, which would make
  docker-mode behavior untestable in-process). Everything degenerates to owner-only when False.
- Permissions: `use` < `manage` (`sharing.py:24-26`). `MANAGE_DISALLOWED_TYPES = {"secret"}`
  (`sharing.py:65`) — a manage grant on a credential collapses into "give me the plaintext".
- `RESOURCE_REGISTRY` (`sharing.py:46-61`) — 12 resource types with per-model owner attribute
  (naming is split: `user_id` for secret/connections, `owner_id` for catalog entities,
  `created_by` for visualization/dashboard which is NULLABLE — NULL-owner rows reachable only by
  admin or explicit grant):
  `secret, database_connection, cloud_connection, ga_connection, kafka_connection,
  catalog_namespace, catalog_table, flow, visualization, dashboard, catalog_notebook,
  global_artifact`.
- Namespace cascade: `_NAMESPACE_SCOPED_TYPES` (`sharing.py:68-70`) = table/flow/viz/dashboard/
  notebook/global_artifact. A grant on a namespace expands to its **direct children only**
  (namespaces are hard-capped at two levels: catalog → schema; one `parent_id` expansion covers
  the whole subtree, `sharing.py:114-144`).
- `_has_access` (`sharing.py:268-279`): owner wins (unless synthetic principal) → admin wins →
  sharing off or synthetic principal ⇒ deny → else grant lookup. The synthetic
  `_internal_service` principal (minted by `get_user_or_internal_service` for internal-token
  calls without a kernel id) has **id defaulting to 1 — a real user** — so synthetic detection
  MUST key on `username == "_internal_service"`, never id (`sharing.py:33-36`).
- `user_id_can_use` (290-303): unrestricted when sharing off or user_id is None; a non-None id
  with no `User` row is **denied** ("a stale id must never widen access").
- Own-first shadowing + lowest-id-wins collision rule implemented at each resolver
  (e.g. `get_encrypted_secret`, `db_connections.py:97-148` — own query `ORDER BY id ASC`, then
  granted-ids query `ORDER BY id ASC`).

### Grant cleanup — the load-bearing delete rule
- `delete_grants_for_resource(db, resource_type, resource_id)` (`sharing.py:346-351`):
  "Must be called from every resource-delete path (no FK cascades; **rowids get reused**)." SQLite
  reuses rowids, so a stale grant would silently re-attach to whatever unrelated resource next
  claims the id.
- Backstop: `_register_grant_cleanup_backstop()` (`sharing.py:372-392`, executed at import)
  registers an SQLAlchemy `after_delete` ORM event for every registered model that deletes its
  grants in the same flush. **Bulk `query.delete()` bypasses ORM events** and must still call
  `delete_grants_for_resource` explicitly. Call sites today (grep, 9 files): artifacts/service.py,
  catalog/repository.py, secret_manager.py, db_connections.py, ga_connections.py,
  kafka/connection_manager.py, project/importer.py, database/models.py, auth/sharing.py.

### Catalog privacy (`catalog/access.py`, 128 lines)
- `AccessResolver(db, user)` built per-request in `routes/catalog.py`, attached to
  `CatalogService`. `access=None` ⇒ unrestricted (electron, internal callers, tests).
- `restricted` (`access.py:28-32`) = `sharing_enabled() AND not user.is_admin AND not synthetic`.
- Per-request memoization: `group_ids()` and `namespace_permissions()` computed once per request —
  before the memo the catalog tree re-ran `user_group_ids` ~14x and namespace expansion ~8x per
  load (`access.py:37-43` comment).
- Visibility vs writability: `visible_namespace_ids` = own ∪ granted(+children) ∪ **public**;
  `writable_namespace_ids` = own ∪ **manage**-granted ∪ public — "a use-level grant makes a
  namespace visible (read) but never a write target" (`access.py:110-127`).

### Connection-mutation hardening (`routes/_connection_sharing.py`, 45 lines)
- `authorize_connection_mutation` — owner mutates freely; non-owner requires `can_manage`;
  unauthorized callers get **404, same as missing** (no enumeration oracle). Returns True for
  non-owners so callers apply the target-change rule.
- `require_credentials_on_target_change` — changing TARGET fields (host/endpoint/protocol,
  enumerated per connection type at each route, e.g. `routes/cloud_connections.py:67-91`,
  `routes/routes.py:692`) while a bundled secret exists and no new credentials were provided ⇒
  422 "requires re-entering the credentials". Rationale in the module docstring: a manage-grantee
  repointing the connection at a server they control could harvest the owner's credential.
- Rotated secrets on shared connections are re-encrypted under the **owner's** id
  (`db_connections.py:369-370`).

### Tests
- `flowfile_core/tests/sharing/` (10 files: authz, catalog, connections, engagement,
  flow_execution, secrets, user_groups, shares API, project tenant isolation/admin gate).
- `conftest.py` fixture `multi_user_mode` monkeypatches `FLOWFILE_MODE=docker`,
  `JWT_SECRET_KEY`, a fresh Fernet `FLOWFILE_MASTER_KEY`, and points
  `FLOWFILE_USER_DATA_DIR` + `storage._user_data_dir` at tmp_path — in-process, no Docker. The
  docstring warns: never flip the mode process-wide (module-level TestClients elsewhere mint
  electron tokens at import time).
- Routers `/user-groups` + `/shares` **404 in electron mode** (root CLAUDE.md; router mounting in
  main.py) **(inferred — did not read the router-gating lines directly)**.

---

## 4. Catalog storage (`flowfile_core/flowfile_core/catalog/`)

### Architecture
- Layered: `routes/catalog.py` → `CatalogService` (`service.py`, 1499 lines — domain rules, never
  raises HTTPException, only `catalog.exceptions`) → sub-services in `catalog/services/`
  (namespaces, tables, flows, runs, schedules, sql, virtual_tables, visualizations, dashboards via
  visualizations, notebooks + notebook_store, previews, stats, engagement, `_resolve`) →
  `SQLAlchemyCatalogRepository` (`repository.py`, 1243 lines) for persistence.
- Table data = **Delta Lake directories**; metadata = rows in the SQLite catalog DB
  (`CatalogTable` et al). Legacy `.parquet` single-file tables still detected
  (`catalog/delta_utils.py: is_delta_table / is_legacy_parquet`); one-time manual converter:
  `python -m flowfile_core.catalog.migrate_parquet_to_delta [--dry-run]` — explicitly NOT auto-run
  at startup.
- Table directory naming: `f"{table_name}_{uuid4().hex[:8]}"`
  (`catalog/services/tables.py:872`, in `resolve_write_destination`).
- Delta I/O helpers live in **shared** (`shared/delta_utils.py`): `write_delta`,
  `merge_into_delta`, `vacuum_delta`, `optimize_delta`, `get_delta_size_bytes`,
  `get_delta_partition_columns`, and the security chokepoints `validate_catalog_path(table_name,
  catalog_dir)` (line 332 — only a bare name: no separators, no `..`, no NUL; path constructed
  from the trusted dir) and `validate_catalog_uri` (cloud analogue).
- Worker-side reads go **exclusively** through `flowfile_worker/catalog_reader.py`
  (`open_catalog_table` = `scan_delta`, `open_virtual_result` = `scan_ipc`), both validating under
  `storage.catalog_tables_directory` / `catalog_virtual_results_directory`. Heavy catalog compute
  (materialize/sql_query/delta merge/visualize) is offloaded to the worker
  (`tables.py:_should_offload`, `_materialize_table_with_worker`).

### Per-catalog object storage (`catalog/storage_backend.py`, 139 lines) — CURRENT shape
- Storage resolves **per catalog (level-0 namespace)**: `catalog_namespaces.storage_uri` +
  `storage_connection_name` (added by migration **028**); schemas/tables inherit from their root
  catalog. Unset URI (or `namespace_id is None`) ⇒ local filesystem rooted at
  `storage.catalog_tables_directory` (`_local_target`, line 88-90).
- `resolve_for_namespace(namespace_id, *, db=None)` (123-134): loads the root namespace; if
  `storage_uri` set, requires `storage_connection_name`, resolves the `CloudStorageConnection`
  **as the catalog owner** (`owner_id = root.owner_id`) — never the calling user — and returns a
  `CatalogStorageTarget(is_cloud=True, base, storage_options, connection_name, worker_interface)`.
- `resolve_catalog_storage(_user_id, *, namespace_id=None)` (137-139) is now a **shim** that
  ignores `_user_id`. (Root CLAUDE.md describes the older env-var-driven behavior.)
- Env vars `FLOWFILE_CATALOG_STORAGE_URI` / `_CONNECTION` still exist
  (`configs/settings.py:60,68`) but are now only a **creation-time default** applied when creating
  a new level-0 catalog (`catalog/services/namespaces.py:_env_default_storage`, ~line 148-168) —
  best-effort: if the connection isn't usable for the owner it logs a warning and falls back to
  local.
- Worker hand-off keeps secrets encrypted: `CatalogStorageTarget.to_worker_payload()` (77-85)
  ships `{"base_uri", "connection": FullCloudStorageConnectionWorkerInterface}` — the worker joins
  `base_uri` + bare dir name and decrypts the connection itself (via `$ffsec$` re-derivation).
- Security wrinkle: `serialized_frame_uses_cloud(blob)` (31-39) — Polars serializes
  `storage_options` (i.e. **decrypted credentials**) inline into LazyFrame blobs; a blob that
  embeds a cloud scan must never be replayed — re-run the producer instead. Cloud URI schemes
  checked: `s3:// s3a:// az:// abfs:// abfss:// adl:// gs:// gcs://` (line 22).
- Numeric limits: `catalog/constants.py` — preview 100/max 1000, viz rows 100k/max 500k, thumbnail
  500 KB, SQL max rows 10k, min schedule interval 60s, virtual-table recursion limit 5.

### Metadata DB
- Always local SQLite (`flowfile_catalog.db`); object storage moves only **table data**. Existing
  local tables are never moved (migration 028 is forward-only, no backfill).

---

## 5. Scheduler (`flowfile_scheduler/`)

### Zero-core-imports invariant — VERIFIED
- `grep -rn "flowfile_core|flowfile_worker|flowfile_frame" flowfile_scheduler/flowfile_scheduler/`
  matches only two docstring mentions (engine.py:3, __init__.py:3). Imports are: stdlib,
  `croniter`, `sqlalchemy`, `zoneinfo`, and `shared.*`. Dependency arrow is core → scheduler,
  never reverse. Core re-exports via `flowfile_core/scheduler/__init__.py`
  (also owns the `get_scheduler`/`set_scheduler` singleton).
- Models: `flowfile_scheduler/models.py` is a **backward-compat shim** re-exporting from
  `shared.models` (Base, CatalogTable, FlowRegistration, FlowRun, FlowSchedule, SchedulerLock,
  ScheduleTriggerTable). NOT "reflected tables" in the SQLAlchemy-reflection sense — they are
  independent declarative models on their own `Base`, a minimal mirror of
  `flowfile_core.database.models`. Add columns in `shared/models.py`, canonical schema stays in
  core's models + an Alembic migration.

### Engine (`engine.py`, 457 lines)
- `DEFAULT_POLL_INTERVAL = 30` s; `STALE_THRESHOLD = 90` s (engine.py:32,34).
- On init: `create_engine(get_database_url())` + `Base.metadata.create_all(checkfirst=True)`
  (safe no-op; ensures scheduler tables exist even standalone).
- Single-leader: `SchedulerLock` row id=1, `holder_id = uuid4().hex[:12]`; foreign lock taken over
  after 90s without heartbeat; tick skipped unless lock held.
- `_tick` (offloaded via `asyncio.to_thread`) processes 4 schedule types in order: `interval`,
  `cron`, `table_trigger`, `table_set_trigger`.
- Cron: naive **local wall-clock** cursor `last_cron_slot` in `cron_timezone` (default UTC),
  croniter-advanced; DST fall-back hour fires once; on fire the cursor advances to **now** (not
  the slot) so a downed scheduler catches up with exactly one fire, never backfills.
  `last_triggered_at` records real UTC. (Migration 015 docstring documents the same rationale.)
- Double-launch guard: skip if a `FlowRun` with `ended_at IS NULL` exists for the registration;
  the `FlowRun` row (`run_type="scheduled"`) is created **before** spawning.
- Launch = `shared.subprocess_utils.spawn_flow_subprocess` → detached
  `python -m flowfile run flow <path> --run-id <id>` (or frozen-exe variant), logs to
  `~/.flowfile/logs/scheduled_run_<run_id>.log`.
- `table_trigger` polling is a **safety net**; the fast path is push-based from
  `CatalogService._fire_table_trigger_schedules` on catalog writes.
- Embedded start: core `main.py:73-77` — only when
  `os.environ.get("FLOWFILE_SCHEDULER_ENABLED","").lower() in ("true","1","yes")`; stopped +
  `set_scheduler(None)` in lifespan shutdown (main.py:84-89). Standalone:
  `poetry run flowfile_scheduler [--once]`.
- DB datetimes are stored naive-UTC; engine re-attaches `tzinfo=timezone.utc` on read — preserve
  this or you get aware/naive arithmetic errors.

---

## 6. Kernel (Docker sandboxed Python) — `flowfile_core/flowfile_core/kernel/` + `kernel_runtime/`

### Container lifecycle (`kernel/manager.py`, 2053 lines)
- Singleton via `kernel/__init__.py:get_kernel_manager()` which constructs
  `KernelManager(shared_volume_path=str(storage.temp_directory / "kernel_shared"))`
  (`kernel/__init__.py:45-56`). Note the constructor's own fallback default is
  `storage.cache_directory` (`manager.py:384`) but production always passes the kernel_shared
  path.
- Ports: `_BASE_PORT = 19000`, `_PORT_RANGE = 1000` (# 19000-19999) (`manager.py:317-318`);
  `_allocate_port` scans for a free localhost port (`manager.py:942-945`, RuntimeError when
  exhausted). Health: `_HEALTH_TIMEOUT = 120` s, poll every 2 s.
- Two topologies, decided by `_is_docker_mode()` + volume discovery (`manager.py:393-443`):
  - **Local (electron/package):** bind-mounts `shared_volume → /shared` and
    `catalog_tables_dir → /catalog_tables`; maps container port `9999/tcp` → allocated host port
    (`_build_run_kwargs`, `manager.py:628-635`); adds `extra_hosts host.docker.internal`.
    Kernel URL = `http://localhost:{kernel.port}` (`_kernel_url`, 504-512).
  - **Docker-in-Docker (compose):** discovers the named volume covering the shared path
    (e.g. `flowfile-internal-storage` at `/app/internal_storage`) and mounts the *same volume at
    the same path* in kernel containers so paths are identical across core/worker/kernel; no host
    port mapping (`port=None` on create); kernels reached by container name
    `http://flowfile-kernel-{id}:9999` on the shared docker network (auto-detected or
    `FLOWFILE_DOCKER_NETWORK`). A **separate catalog volume** is mounted when catalog_tables lives
    under user_data (different volume).
- Path translation: `to_kernel_path()` (514-535) — identity in DinD; in local mode rebases host
  prefixes onto `/catalog_tables` / `/shared`, always POSIX (Windows backslash-safe).
- Kernel env (`_build_kernel_env`, 1200-1258): `FLOWFILE_CORE_URL` (DinD default
  `http://flowfile-core:63578`, local default `http://host.docker.internal:63578`),
  `FLOWFILE_INTERNAL_TOKEN` (via `auth.jwt.get_internal_token()`), `FLOWFILE_KERNEL_ID`,
  `FLOWFILE_HOST_SHARED_DIR` (local mode only), `FLOWFILE_KERNEL_SHARED_DIR`,
  `FLOWFILE_HOST_CATALOG_TABLES_DIR` (local only) / `FLOWFILE_KERNEL_CATALOG_TABLES_DIR`,
  `KERNEL_PACKAGES=""` (packages are pre-baked into a derived image, so the entrypoint pip loop is
  a no-op), `PERSISTENCE_ENABLED/PATH` (artifacts under `<shared>/artifacts`), `RECOVERY_MODE`.
- Node I/O convention: inputs/outputs at `<shared>/{flow_id}/{node_id}/inputs|outputs`, parquet
  files named `{name}_{index}.parquet`; `"main"` is the backward-compatible alias for all inputs
  (`resolve_node_paths`, 537-582).
- Registry concurrency: `_kernels_lock` (RLock) serialises reconcile/restore/create/launch/delete;
  never held across the scratch-flow DB path or long Docker I/O (`manager.py:361-378`). Each
  kernel auto-provisions a **scratch FlowRegistration** so interactive-cell artifact publishes
  have a valid producer (migration 014).
- Startup reconciliation: `_restore_kernels_from_db()` + `_reclaim_running_containers()` +
  `_remove_orphan_derived_images()` (orphan GC keyed by a stamped per-install
  `_core_instance_id`). All containers stopped in core's lifespan shutdown
  (`main.py:_shutdown_kernels`).
- Image flavours: `ImageFlavour.BASE/ML/LITE/CUSTOM`; locked versions parsed from
  `kernel_runtime/poetry.lock` at `repo_root/kernel_runtime/poetry.lock`
  (`kernel/flavours.py:17-19` — the same lockfile the Docker build installs from; missing file ⇒
  versions display as "—"). Custom images must carry an explicit tag or `@sha256:` digest
  (`manager.py:300-314`). Env overrides: `FLOWFILE_KERNEL_IMAGE` / `_BASE` / `_ML` / `_LITE`.

### kernel_runtime side
- `kernel_runtime/entrypoint.sh`: optional `KERNEL_PACKAGES` pip install constrained by
  `/opt/constraints.txt`, then `exec uvicorn kernel_runtime.main:app --host 0.0.0.0 --port 9999`.
- `Dockerfile`: `EXPOSE 9999`, healthcheck `curl -f http://localhost:9999/health`.
- kernel → core auth: `kernel_runtime/flowfile_client.py:219` `_CORE_URL =
  os.environ.get("FLOWFILE_CORE_URL", "http://host.docker.internal:63578")`; token from the
  per-request `internal_token` field (core passes it in `ExecuteRequest`,
  `kernel_runtime/main.py:353`) falling back to the `FLOWFILE_INTERNAL_TOKEN` env
  (`flowfile_client.py:243`).
- core-side verification: `auth/jwt.py:31-57` — `get_internal_token()` reads
  `FLOWFILE_INTERNAL_TOKEN`; in **electron** mode auto-generates (`secrets.token_hex(32)`) and
  writes it back into the env; in docker mode missing ⇒ ValueError at first use.
  `verify_internal_token` uses `secrets.compare_digest`. Requests with the internal token and no
  kernel id become the synthetic `_internal_service` principal
  (`auth/jwt.py:268-320`) — which bypasses catalog restriction (§3) but is denied owner-shortcut
  matches in sharing.
- `kernel_runtime` does **not** import `shared`; it only reads/writes the shared volume paths
  given via env (shared/CLAUDE.md, confirmed by its dependency surface).
- Tests: `poetry run pytest -m kernel` (Docker required). Fixture
  (`tests/kernel_fixtures.py:190-239`) sets `FLOWFILE_SHARED_DIR` to a tempdir, **rebuilds the
  `storage` singleton**, and constructs `KernelManager(shared_volume_path=shared_dir)` explicitly.

### Gotcha found (asymmetry) — **(inferred severity)**
`storage.shared_directory` / `global_artifacts_directory` / `artifact_staging_directory` all honor
`FLOWFILE_SHARED_DIR` (`storage_config.py:150-175,236-246`), but production
`get_kernel_manager()` hardcodes `storage.temp_directory / "kernel_shared"` and ignores
`FLOWFILE_SHARED_DIR` (`kernel/__init__.py:50-55`). Setting `FLOWFILE_SHARED_DIR` to a path
other than `<base>/temp/kernel_shared` in a real deployment would relocate artifact staging away
from the volume kernels actually mount. Tests avoid this by passing the path explicitly.

---

## 7. Database & migrations

### Migration inventory (`flowfile_core/flowfile_core/alembic/versions/`) — 28 revisions, linear chain 001→028
| Rev | File | Summary (from docstring) |
|-----|------|--------------------------|
| 001 | initial_schema | Baseline schema. |
| 002 | virtual_flow_tables | Virtual flow table support. |
| 003 | query_virtual_tables | `sql_query` column for query-based virtual tables. |
| 004 | polars_plan | `polars_plan` column (optimized virtual-table query plans). |
| 005 | source_table_versions | Staleness detection for virtual tables. |
| 006 | normalize_run_type | Data fix: remap leaked engine run kinds (`fetch_one`/`full_run`/`init`) in `flow_runs.run_type` → `in_designer_run` (they failed Pydantic validation on `GET /catalog/runs`). |
| 007 | analytics_data | `google_analytics_connections` + schema-drift cleanup. |
| 008 | catalog_visualizations | Saved Graphic Walker chart specs (table- or SQL-sourced, thumbnail data URL). |
| 009 | catalog_dashboards | Dashboard = 2D canvas in `layout_json`; deliberately **no FK** to visualizations (deleted viz ⇒ placeholder, not cascade). |
| 010 | flow_uuid | Stable `flow_uuid` on registrations+runs — SQLite id reuse must never pull another flow's runs into a new flow's history. |
| 011 | ai_audit_and_credentials | `ai_audit_events` + `ai_provider_credentials` (BYOK); `flow_id` is a plain int (draft flows aren't registered yet). |
| 012 | kernel_image_flavour | `image_flavour` + `custom_image`; backfill `'base'`. |
| 013 | kernel_resolved_packages | JSON column with pip-resolved versions. |
| 014 | kernel_scratch_flow | `scratch_flow_registration_id` FK (artifact producer for interactive cells). |
| 015 | cron_schedules | `cron_expression`/`cron_timezone`/`last_cron_slot`; naive-local wall-clock cursor for DST-correct once-per-slot firing. |
| 016 | flow_api_endpoints | Publish flows as HTTP APIs + hashed `flow_api_keys`. |
| 017 | flow_registration_api_compatible | `is_api_compatible` flag (exactly one `api_response` node), recomputed on save. |
| 018 | api_consumers | Reusable service-account consumers + per-endpoint grants; backfills one implicit consumer per existing key. |
| 019 | ga_service_account_auth | GA4 `auth_method`: `oauth` vs `service_account`; backfill `oauth`. |
| 020 | user_groups_sharing | Groups/memberships/resource_grants + `catalog_namespaces.is_public`; backfills public seeds (see §3). |
| 021 | catalog_partition_columns | Delta partition columns (JSON list) on `catalog_tables`; NULL = unpartitioned. |
| 022 | catalog_notebooks | Notebooks with `cells_json` Text; unique `(name, namespace_id)`. |
| 023 | workspace_projects | Git project-tracking export/import layer; DB stays runtime source of truth. |
| 024 | visualization_dashboard_uuids | Stable uuids for viz/dashboards; dashboards reference `viz_uuid` portably. |
| 025 | project_track_data_artifacts | Per-project toggle mirroring project.yaml; default True. |
| 026 | project_uniqueness_and_uuid_not_null | `(owner_id, folder_path)` unique; partial unique index one-active-project-per-owner; viz/dashboard uuid NOT NULL. Idempotent with dedup guards. |
| 027 | notebook_files | Notebook cells moved to disk as YAML keyed by `notebook_uuid` (see `catalog/services/notebook_store.py`); table recreated (drop `cells_json`). |
| 028 | catalog_namespace_storage | `storage_uri` + `storage_connection_name` on level-0 namespaces (per-catalog object storage); nullable, forward-only, NULL ⇒ local. |

**Root CLAUDE.md says "currently 001–021" — stale; actual head is 028.**

### Startup migration (`database/migration.py`, 318 lines)
- Trigger: `database/init_db.py:26-27` runs `run_startup_migration()` **at module import** unless
  `FLOWFILE_SKIP_STARTUP_MIGRATION` is set (and `flowfile_core/__init__.py` imports it — so
  *importing flowfile_core migrates the live DB*; the skip var exists so the alembic CLI can
  import metadata without recursion, and diagnostics can import safely).
- `run_startup_migration()` (289-317), three scenarios:
  1. `flowfile_catalog.db` exists ⇒ `run_alembic_upgrade()` to head.
  2. legacy `flowfile.db` exists ⇒ create schema, then `migrate_data_from_legacy_db()` —
     dynamic column-mapped copy (common columns copied; old-only skipped; new-only defaults or
     per-table generators in `_NEW_COLUMN_GENERATORS`: flow_registrations.flow_uuid,
     flow_runs.flow_uuid (looked up from registrations), viz/dashboard uuids), Kahn-topo-sorted
     FK order, `BATCH_SIZE = 500`, `PRAGMA foreign_keys OFF/ON`, per-table failures logged and
     skipped, **old DB never modified**.
  3. fresh install ⇒ create schema.
- `_ensure_known_revision` (93-128): if the DB is stamped at a revision missing from local
  scripts (e.g. after switching to an older branch), it re-stamps to the local head
  (`command.stamp(cfg, head, purge=True)`) — schema artifacts from the unknown revision are NOT
  reverted; startup proceeds with a warning.
- PyInstaller-aware alembic file resolution via `sys._MEIPASS` (`_get_base_dir`, 64-72).
- `alembic/env.py` targets `flowfile_core.database.models.Base.metadata` and
  `shared.storage_config.get_database_url()`; configured with `render_as_batch=True`
  (SQLite ALTER support).
- Rule from core CLAUDE.md: schema change ⇒ edit `database/models.py` (+ mirror needed columns in
  `shared/models.py` if scheduler/CLI need them) ⇒ add new `alembic/versions/NNN_*.py` keeping the
  numeric prefix sequence; never hand-edit an existing migration.

### Who touches the DB
- Core: full ORM (`flowfile_core/database/models.py` is canonical).
- Scheduler + CLI run-completion: `shared/models.py` lightweight mirror on its own Base.
- **Worker: does NOT open the catalog DB at all.** Verified:
  `grep -rn "create_engine|Session|shared.models" flowfile_worker/flowfile_worker/` finds no DB
  engine/session usage (only `shared.sql_utils` for building user database-connection URIs). The
  root CLAUDE.md sentence "one SQLite catalog DB shared by core, scheduler, and worker" overstates
  the worker's involvement — the worker shares storage *paths* (via `shared.storage_config`), not
  the DB.
- Test isolation (from user memory + `get_database_url`): `TESTING=True` ⇒ shared fixed
  `temp/test_flowfile_catalog.db` — concurrent pytest sessions can drop each other's DB; use
  `FLOWFILE_DB_PATH` for isolation.

---

## 8. `shared/storage_config.py` — the `storage` singleton

### Contract
- `storage = FlowfileStorage()` instantiated **at import**, and `__init__` eagerly `mkdir -p`s all
  listed directories (`_ensure_directories`, 248-277). Importing `shared.storage_config` (or
  `shared`, or anything that pulls them) has filesystem side effects.
- Two roots, branching on `FLOWFILE_MODE == "docker"` (`_is_docker_mode`, 26-28):
  - `base_directory` (internal, core↔worker volume): `FLOWFILE_STORAGE_DIR` env, else
    `~/.flowfile` locally / `/app/internal_storage` in docker (39-52).
  - `user_data_directory`: docker ⇒ `FLOWFILE_USER_DATA_DIR` (default `/data/user`); local ⇒
    `Path.home()` (54-64). Note compose maps it to `/app/user_data` per root CLAUDE.md env table;
    code default is `/data/user`.

### Directory inventory (property → location; eager-mkdir list at 248-277)
**Internal (under base_directory), eagerly created:**
- `cache` (worker↔core Arrow IPC handoff; per-flow subdirs via `get_flow_cache_directory`)
- `database` (holds `flowfile_catalog.db`)
- `logs`, `system_logs`
- `temp`, `temp/flows`
- `shared_directory` = `$FLOWFILE_SHARED_DIR` or `temp/kernel_shared` (core/worker/kernel exchange)
- `artifact_staging_directory` = `$FLOWFILE_SHARED_DIR/artifact_staging` or
  `temp/kernel_shared/artifact_staging` (must stay under the kernel shared volume)

**User-facing (base_directory locally, user_data_directory in docker), eagerly created:**
- `flows` (+ `flows/unnamed_flows`, `flows/python_editor_flows`)
- `uploads`, `outputs`
- `user_defined_nodes` (+ `user_defined_nodes/icons`)
- `global_artifacts_directory` = `$FLOWFILE_SHARED_DIR/global_artifacts` or
  `temp/kernel_shared/global_artifacts` (kernel shared-volume constraint — listed in the *user*
  list but actually resolved under the shared volume)
- `catalog_tables` (Delta dirs), `catalog_virtual_results` (worker IPC cache)
- `template_data`, `notebooks` (per-owner subdir resolved by notebook_store, not here)

**Deliberately NOT eagerly created:**
- `local_model_directory` (`base/local_model`) — opt-in local LLM install pays nothing otherwise
  (docstring 183-192).
- `ai_sessions_directory` — docker ⇒ `user_data/ai_sessions` (per-user separation); local ⇒
  `base/ai_sessions` (never `Path.home()/ai_sessions`, deemed intrusive) (195-208).
- (Also `ai_prompts/` is created lazily by prompt_log.)

### DB URL resolution (`get_database_url`, 402-417)
1. `FLOWFILE_DB_PATH` env ⇒ `sqlite:///<path>` (also disables legacy-migration detection);
2. `TESTING=True` ⇒ `sqlite:///<temp>/test_flowfile_catalog.db`;
3. default ⇒ `sqlite:///<base>/database/flowfile_catalog.db`.
`get_legacy_database_path` (420-435) returns `<base>/database/flowfile.db` if it exists (test
variant under TESTING), None when `FLOWFILE_DB_PATH` set.

### Cleanup policy (`cleanup_directories`, 335-340; worker lifespan calls it)
temp 24h, cache 1h, logs 168h, system_logs 168h — mtime-based recursive delete.

### Invariants / how to extend
- IMPORT-ONLY-DOWNWARD: `shared` never imports core/worker/scheduler/frame (verified per
  shared/CLAUDE.md; consistent with everything read).
- Kernel-exchange dirs (`shared_directory`, `global_artifacts`, `artifact_staging`) MUST stay
  under the kernel shared volume — Docker kernels can't see `base_directory` proper.
- Adding a directory: add the property + (if it should exist eagerly) append to the right list in
  `_ensure_directories`; respect the docker/local branch pattern; do NOT add opt-in dirs to the
  eager list.
- `shared/crypto/` on disk contains only stale `.pyc` — no importable source; don't import
  `shared.crypto` (shared/CLAUDE.md).

---

## Cross-cutting corrections & open problems (for the skills library)

1. **Stale root-CLAUDE.md facts (verified against source):**
   - Alembic migrations are 001–**028**, not 001–021 (six newer feature families: notebooks,
     workspace projects, notebook files-on-disk, per-catalog object storage).
   - The litellm rule is "no module-level litellm anywhere under `ai/`; lazy imports live in
     `providers/_litellm_base.py` and `ai/scheduler.py`" — NOT "except `ai/byok.py`".
   - `FLOWFILE_CATALOG_STORAGE_URI/_CONNECTION` are now only a creation-time default for new
     catalogs; the live mechanism is per-namespace columns from migration 028;
     `resolve_catalog_storage` ignores its user_id arg.
   - Worker does not open the catalog SQLite DB (no sqlalchemy engine/session in
     `flowfile_worker`); "shared by core, scheduler, and worker" is wrong about the worker.
2. **FLOWFILE_SHARED_DIR asymmetry:** honored by storage_config's artifact dirs but ignored by
   production `get_kernel_manager()` (hardcodes `temp/kernel_shared`). Setting it in production to
   a foreign path would split artifact staging from the kernel mount. (Tests pass the path to the
   manager explicitly, masking this.)
3. **Historical incidents encoded in migrations:** 006 (run_type enum leak → Pydantic 500s on
   /catalog/runs → data remap), 010/024/026 (SQLite rowid-reuse identity bugs → uuid columns),
   015 (DST double-fire risk → naive-local cron cursor), 020 backfill (catalog going
   private-by-default would have hidden seeded namespaces), `_ensure_known_revision` (branch
   switching used to crash startup on unknown alembic revision → auto re-stamp).
4. **rowid-reuse is a recurring theme:** grants must be deleted on resource delete
   (`delete_grants_for_resource` + after_delete backstop; bulk deletes bypass the backstop), and
   flow runs/viz/dashboards all grew uuids for the same reason.
5. **Never-raise seams:** planner event generator, prompt-log/telemetry hooks, project projection
   hooks — all deliberately swallow errors; don't "fix" them to raise.
