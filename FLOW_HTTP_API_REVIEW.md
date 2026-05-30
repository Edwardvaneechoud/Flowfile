# Branch review — Host flows as HTTP data APIs

**Branch:** `claude/nifty-cray-5PBpi` · **Base:** `main` branch (merge-base `d9c8f578`)
**Scope:** 12 commits / 36 files / ~2,580 insertions. Feature: host saved flows as authenticated HTTP data APIs (`GET /api/data/{slug}`), a new `api_response` node, a flow-page/catalog frontend redesign, and AI node-doc updates.

> Note: a stale local git **tag** named `main` (`f6cb5f27`) hijacks `git diff main` and shows a bogus 1,235-file diff. Always diff against the branch: `git diff d9c8f578 HEAD`.

Reviewed by 6 subsystem agents → 28 findings → each adversarially verified (28/28 confirmed, 0 false positives), then deduped to 23 distinct issues.

---

## Findings catalog

### 🔴 HIGH
| # | Issue | File:loc | Fix |
|---|-------|----------|-----|
| 1 | **Untrusted query params substituted verbatim into `exec()`'d Polars code.** Raw query strings → `flow.parameters[i].default_value` (no escaping) → `${name}` regex substitution into all node settings incl. `polars_code` → `exec()` with full `pl` exposed. Key holder can bypass filters, `pl.read_csv('/etc/passwd')`, hit DBs, OOM. Sandbox blocks dunders/imports (no OS-RCE) but not data access. | `api_runner.py:30-34,147-150` → `parameter_resolver.py`, `polars_code_parser.py:312` | Don't interpolate raw strings into code. Prefer real Polars values (`pl.lit`/bound params); or, scoped to the API seam, validate/reject string params containing quotes/parens/backslashes when targeting code nodes. Add a regression test proving an injected expression can't bypass a filter or read a file. |

### 🟠 MEDIUM
| # | Issue | File:loc | Fix |
|---|-------|----------|-----|
| 2 | Flow errors returned verbatim to callers (`detail=str(exc)` leaks paths, SQL, columns). | `flow_api.py:147-148` | Generic 500 to caller; log details server-side. Keep verbose errors for owner-only test path. |
| 3 | No rate limiting / concurrency cap on public endpoint; each request = full sync graph run + DB commit. Authenticated DoS. | `flow_api.py:123-150` | Bounded concurrency (module-level `asyncio.Semaphore`) + per-key rate limit, or document "front with a gateway". *(Complements #4, different layer.)* |
| 4 | Concurrent requests to the same flow corrupt shared `flow_id`-keyed state: singleton `FlowLogger` (truncated mid-run), kernel I/O dirs `shared_volume/{flow_id}/{node_id}`, cache dir. Interactive path uses `get_flow_run_lock`; API path takes no lock. Docstring "no shared mutable state" is false. | `api_runner.py:118-162` | Serialize per `flow_id` around `run_graph()` (use `get_flow_run_lock`), or assign each per-request graph a unique `flow_id`. Fix the docstring. |
| 5 | `columns` orientation collects the whole result then slices `[:max_rows]` (no push-down); `records` pushes down. OOM risk. *(Found 3×.)* | `api_runner.py:86-97` | `df = data.collect(n_records=max_rows); df.to_dict(as_series=False)`. |
| 6 | **Deleting a flow orphans its endpoint + API keys.** `delete_flow` doesn't clean new tables; SQLite FK enforcement is off → orphaned key stays `enabled` (revocation gap) + slug stays occupied (can't republish). | `catalog/repository.py` (concrete `delete_flow`, ~line 393) | Delete `FlowApiKey` rows for the registration's endpoints, then the `FlowApiEndpoint` rows, before deleting the registration (mirror the favorite/follow cleanup). |
| 7 | Enable/disable switch not reverted on save failure → UI shows a state the server never persisted, for a live public endpoint. | `ApiEndpointPanel.vue:51,368-385` | Restore `enabled.value`/`slug.value` from server state in the catch block. |
| 8 | Endpoint stays `enabled` after the flow loses its `api_response` node (compat flag only gates the Publish button, not already-published endpoints). Public route then 500s. | `catalog_helpers.py:135-155` | When compatibility flips to false, disable the registration's `FlowApiEndpoint` rows. |

### 🟡 LOW
| # | Issue | File:loc | Fix |
|---|-------|----------|-----|
| 9 | No route to disable a key without deleting it (`FlowApiKey.enabled` honored but unmanageable). | `flow_api.py:296-346` | Add owner-scoped `PATCH /endpoints/{id}/keys/{key_id}` toggling `enabled`. |
| 10 | Endpoint `default` values bypass `_coerce` → invalid typed/enum defaults injected silently. | `api_runner.py:73` | Run defaults through `_coerce`. |
| 11 | Declared (`ApiEndpointOut.parameters`) vs enforced (`_effective_specs`) params diverge; stored `required` for a removed param silently dropped. | `flow_api.py` `_endpoint_out` vs `api_runner._effective_specs` | Return effective specs (import `_effective_specs`, read-only) in the single-endpoint GET, or document inheritance clearly. |
| 12 | Owner "test" endpoint has no execution timeout (unlike public route) → hung flow ties up a threadpool worker. *(Found 2×.)* | `flow_api.py:258-281` | Apply the same `asyncio.wait_for(anyio.to_thread.run_sync(...), timeout=_API_RUN_TIMEOUT)` + 504. |
| 13 | Model/migration drift: `enabled` columns have `server_default` in migration 016 but not in the model. *(Postgres concern is NOT live — catalog DB is always SQLite.)* | `database/models.py:258,284` | Add matching `server_default` to the model columns (mirror `is_api_compatible`). |
| 14 | The Enabled toggle silently persists unsaved param-table edits (shared `saveEndpoint` always sends `parameters`). | `ApiEndpointPanel.vue:51,368-385` | Give the toggle a `parameters`-free update payload. |
| 15 | "Try it" preview URL iterates live `params.value`; the actual run iterates saved `endpoint.value.parameters` → they diverge. *(Found 2×, = #20.)* | `ApiEndpointPanel.vue:268-276` vs `454-471` | Drive both from one source (`params.value`). |
| 16 | `testValues` not reset on flow switch → prior flow's "Try it" inputs leak into same-named params. | `ApiEndpointPanel.vue:320-346` | Add `testValues.value = {}` to `load()`'s reset block. |
| 19 | `response_node_id` resolved+stored at publish but never read by the runner (re-scans live); can go stale. | `flow_api.py:173-179` | Keep the validation, stop persisting/returning the id (drop from `ApiEndpointOut`) — or use it in the runner. |
| 21 | Concurrent publish/slug-change → unhandled `IntegrityError` (500 instead of 409). | `flow_api.py:165-185,240-255` | Wrap commit in `try/except IntegrityError` → 409. |

### ⚪ NIT
| # | Issue | File:loc | Fix |
|---|-------|----------|-----|
| 23/25 | Redundant `secrets.compare_digest` after exact-hash DB lookup (always True; misleading intent). | `auth/api_key.py:70` | Drop it (keep None/expiry checks) or add a clarifying comment. |
| 24 | N+1 `FlowRegistration` lookup per endpoint in `list_endpoints`. | `flow_api.py` via `_endpoint_out` | Batch-fetch registrations; pass a map into `_endpoint_out`. |
| 26 | Stale "21+1 static set" count in docstring (now 24 after `api_response`). | `ai/tools/classification.py:20-21` | Update the count (or make it non-numeric). |
| 27 | `FLOWFILE_API_RUN_TIMEOUT_SECONDS` (new env var, default 120) undocumented. | `flow_api.py:44` | Document in `.env.example` + `CLAUDE.md`. |
| 28 | Dead CSS `.param-delete-btn` (control removed). | `ApiEndpointPanel.vue:543-548` | Remove the rule. |

---

## Parallel fix plan (file ownership — zero overlap)

Each prompt owns a **disjoint** set of files, so all five can run at the same time (separate sessions or `git worktree`s) without merge conflicts. Each also writes to its **own** test file.

| Prompt | Owns (edits) | Findings | Priority |
|--------|--------------|----------|----------|
| **P1 – Runner security & correctness** | `flowfile/api_runner.py`, `flowfile/parameter_resolver.py`*, `flowfile/flow_data_engine/polars_code_parser.py`*, `tests/test_api_runner_security.py` (new) | 1, 4, 5, 10 | 🔴 highest |
| **P2 – Public-endpoint hardening** | `routes/flow_api.py`, `auth/api_key.py`, `schemas/flow_api_schema.py`, `tests/test_flow_api.py` | 2, 3, 9, 11, 12, 19, 21, 23/25, 24 | 🟠 high |
| **P3 – Lifecycle & data integrity** | `catalog/repository.py`, `flowfile/catalog_helpers.py`, `database/models.py`, `tests/test_flow_api_lifecycle.py` (new) | 6, 8, 13 | 🟠 high |
| **P4 – Frontend** | `views/CatalogView/ApiEndpointPanel.vue` | 7, 14, 15/20, 16, 28 | 🟡 medium |
| **P5 – Docs & nits** | `.env.example`, `CLAUDE.md`, `ai/tools/classification.py` | 26, 27 | ⚪ low |

\* P1 may only need to touch `api_runner.py`; the substitution files are listed in case the chosen #1 fix reaches them. No other prompt touches them.

All paths are under `flowfile_core/flowfile_core/` unless noted; tests under `flowfile_core/tests/`.

---

## The prompts

Copy each block into a separate session. Each is self-contained.

### P1 — Runner security & correctness 🔴

```
Repo: /Users/edwardvaneechoud/flowfile_backup/Flowfile (branch claude/nifty-cray-5PBpi).
You are fixing review findings on the "host flows as HTTP data APIs" feature. Diff the branch with: git diff d9c8f578 HEAD -- <file>.

CONSTRAINTS:
- ONLY edit these files: flowfile_core/flowfile_core/flowfile/api_runner.py (and, only if your #1 fix requires it, flowfile_core/flowfile_core/flowfile/parameter_resolver.py and flowfile_core/flowfile_core/flowfile/flow_data_engine/polars_code_parser.py). Put new tests in a NEW file flowfile_core/tests/test_api_runner_security.py. Do NOT touch any other file (parallel sessions own them).
- Do NOT commit, push, or amend git history — make file changes only.

FIX:
[#1 HIGH security] In api_runner.py the public endpoint writes raw query strings onto flow parameter default_value with no escaping (_coerce returns string params unchanged, lines 30-34; assignment lines 147-150). These ${name} refs are regex-substituted (parameter_resolver.resolve_parameters) into ALL node settings including a polars_code node's source, which is exec()'d (polars_code_parser.py:312) with full `pl` exposed. A key holder can inject Polars expressions to bypass row filters, read local files (pl.read_csv/scan_csv), hit DBs, or OOM. Implement a defensible mitigation AT THE API SEAM (api_runner) so designer/editor flows are unaffected: validate string-typed param values and reject ones containing characters that can break out of a string literal or inject code (e.g. quotes ' " , parentheses, backslash, semicolon, ${ ) — raise ApiParamError. If a fully safe fix requires substituting params as real values (pl.lit/bound) rather than string interpolation, evaluate it but keep blast radius small and call out any cross-file impact in your summary. Add tests proving (a) an injected `region=') | True | ('`-style value is rejected, and (b) a value attempting pl.read_csv cannot exfiltrate.
[#4 MEDIUM correctness] run_flow_as_api claims "no shared mutable state" but open_flow reuses the saved flow_id, which keys a singleton FlowLogger, kernel I/O dirs shared_volume/{flow_id}/{node_id}, and the cache dir; concurrent same-flow requests collide. Serialize runs of the same flow: acquire the existing get_flow_run_lock(flow.flow_id) around flow.run_graph() (search the codebase for get_flow_run_lock — it's used in routes.py). Fix the inaccurate docstring.
[#5 MEDIUM efficiency] In _serialize (lines 86-97) the 'columns' orientation calls data.to_dict() (full collect) then slices [:max_rows]; the 'records' path pushes the limit down. Change columns to: df = data.collect(n_records=max_rows); columns = df.to_dict(as_series=False); row_count = df.height.
[#10 LOW correctness] In resolve_params (line 73) endpoint defaults are used verbatim, bypassing _coerce. Run defaults through _coerce(spec, spec.default) so they obey the same type/enum validation as request values.

VERIFY: poetry run ruff check flowfile_core/flowfile_core/flowfile/api_runner.py && poetry run pytest flowfile_core/tests/test_api_runner_security.py flowfile_core/tests/test_flow_api.py
Report a short summary of each fix and any cross-file impact you deliberately avoided.
```

### P2 — Public-endpoint hardening 🟠

```
Repo: /Users/edwardvaneechoud/flowfile_backup/Flowfile (branch claude/nifty-cray-5PBpi).
You are fixing review findings on the "host flows as HTTP data APIs" feature. Diff the branch with: git diff d9c8f578 HEAD -- <file>.

CONSTRAINTS:
- ONLY edit: flowfile_core/flowfile_core/routes/flow_api.py, flowfile_core/flowfile_core/auth/api_key.py, flowfile_core/flowfile_core/schemas/flow_api_schema.py, and the EXISTING test file flowfile_core/tests/test_flow_api.py (add/extend tests). You may READ (import, do not edit) flowfile_core/flowfile_core/flowfile/api_runner.py. Do NOT touch any other file (parallel sessions own them).
- Do NOT commit, push, or amend git history — make file changes only.

FIX (all in flowfile_core/flowfile_core/):
[#2 MEDIUM security] run_published_flow (flow_api.py:147-148) returns detail=str(exc) for ApiExecutionError/ApiConfigError on the PUBLIC route, leaking node errors (paths, SQL, columns). Return a generic message ("Flow execution failed") to the public caller and log the detailed error server-side. Keep verbose errors on the owner-only test_endpoint path.
[#3 MEDIUM security] No concurrency cap on GET /api/data/{slug}; each request runs a full graph. Add a bounded concurrency guard (module-level asyncio.Semaphore, size from an env var e.g. FLOWFILE_API_MAX_CONCURRENT_RUNS default a small number) around the public run, returning 503 when saturated. (This is the global cap; per-flow serialization is handled elsewhere — don't add a flow lock here.)
[#9 LOW] Add owner-scoped PATCH (or PUT) /flow-api/endpoints/{endpoint_id}/keys/{key_id} that toggles FlowApiKey.enabled (verify ownership via _get_owned_endpoint). Add ApiKeyUpdate schema in flow_api_schema.py.
[#11 LOW] _endpoint_out returns stored param_schema_json, which can diverge from runtime-enforced specs (api_runner._effective_specs). In the single-endpoint GET (get_endpoint), import and use _effective_specs (read-only import from api_runner) so the advertised parameters match enforcement; leave list_endpoints as-is for perf, or document the inheritance behavior in the schema docstring.
[#12 LOW] test_endpoint runs with no timeout (public route uses asyncio.wait_for(..., _API_RUN_TIMEOUT)). Make test_endpoint async and wrap run_flow_as_api in anyio.to_thread.run_sync + asyncio.wait_for, mapping timeout to 504, mirroring run_published_flow.
[#19 LOW] response_node_id is resolved+stored at publish but never read (the runner re-scans live). Keep the publish-time validation (exactly one api_response node) but stop persisting/returning response_node_id: remove it from ApiEndpointOut and from the FlowApiEndpoint(...) construction. (Do NOT add a migration; leaving the nullable DB column unused is fine.)
[#21 LOW] publish_endpoint / update_endpoint slug-change pre-checks aren't atomic with the UNIQUE constraints; a race raises IntegrityError -> 500. Wrap the commit in try/except sqlalchemy.exc.IntegrityError and raise HTTPException(409).
[#23/#25 NIT] In auth/api_key.py:70 the secrets.compare_digest after the exact-hash WHERE lookup is always True and misleading. Drop the compare_digest term (keep the None and expiry checks) and add a one-line comment that security rests on the high-entropy token + hashed-column lookup.
[#24 NIT] list_endpoints does an N+1 db.get(FlowRegistration) per endpoint via _endpoint_out. Batch-fetch the registrations in one query and pass a {id: registration} map into _endpoint_out.

VERIFY: poetry run ruff check flowfile_core/flowfile_core/routes/flow_api.py flowfile_core/flowfile_core/auth/api_key.py flowfile_core/flowfile_core/schemas/flow_api_schema.py && poetry run pytest flowfile_core/tests/test_flow_api.py
Add tests for: public route hides internal error detail; disabled-key is rejected; concurrent-publish 409; key enable/disable toggle. Report a short summary.
```

### P3 — Lifecycle & data integrity 🟠

```
Repo: /Users/edwardvaneechoud/flowfile_backup/Flowfile (branch claude/nifty-cray-5PBpi).
You are fixing review findings on the "host flows as HTTP data APIs" feature. Diff the branch with: git diff d9c8f578 HEAD -- <file>.

CONSTRAINTS:
- ONLY edit: flowfile_core/flowfile_core/catalog/repository.py, flowfile_core/flowfile_core/flowfile/catalog_helpers.py, flowfile_core/flowfile_core/database/models.py, and a NEW test file flowfile_core/tests/test_flow_api_lifecycle.py. Do NOT touch any other file (parallel sessions own them).
- Do NOT commit, push, or amend git history — make file changes only.

FIX:
[#6 MEDIUM data-loss] The concrete delete_flow in catalog/repository.py (around line 393) cleans up FlowFavorite/FlowFollow/artifacts/FlowRun but NOT the new flow_api_endpoints / flow_api_keys tables. Because SQLite FK enforcement is off, deleting a flow silently orphans them: the orphaned API key stays enabled (a real revocation gap) and the slug stays occupied so the flow can't be republished. Before deleting the FlowRegistration: collect its FlowApiEndpoint ids for this registration_id, delete FlowApiKey rows whose endpoint_id is in that set, then delete the FlowApiEndpoint rows — mirroring the existing favorite/follow cleanup. (Import FlowApiEndpoint/FlowApiKey from the models module.)
[#8 MEDIUM correctness] sync_api_compatibility (catalog_helpers.py:135-155) recomputes reg.is_api_compatible but nothing re-checks an ALREADY-published endpoint. If a published flow loses/duplicates its api_response node, the FlowApiEndpoint stays enabled and the public route 500s. When is_api_compatible flips to False, also set enabled=False on any FlowApiEndpoint for that registration (in the same session/commit). Keep the existing flag update.
[#13 LOW] Model/migration drift: migration 016 creates the enabled columns with server_default but the model (database/models.py:258 FlowApiEndpoint.enabled, :284 FlowApiKey.enabled) declares no server_default. Add a matching server_default to those two model columns (mirror how is_api_compatible aligns model+migration). Cosmetic — no data migration.

VERIFY: poetry run ruff check flowfile_core/flowfile_core/catalog/repository.py flowfile_core/flowfile_core/flowfile/catalog_helpers.py flowfile_core/flowfile_core/database/models.py && poetry run pytest flowfile_core/tests/test_flow_api_lifecycle.py
Add tests: deleting a published flow removes its endpoint+keys (and a key by that hash no longer authenticates); making a published flow API-incompatible disables its endpoint. Report a short summary.
```

### P4 — Frontend 🟡

```
Repo: /Users/edwardvaneechoud/flowfile_backup/Flowfile (branch claude/nifty-cray-5PBpi).
You are fixing review findings on the catalog "API endpoint" panel. Diff with: git diff d9c8f578 HEAD -- <file>.

CONSTRAINTS:
- ONLY edit: flowfile_frontend/src/renderer/app/views/CatalogView/ApiEndpointPanel.vue. Do NOT touch any other file (parallel sessions own them).
- Do NOT commit, push, or amend git history — make file changes only.

FIX (all in ApiEndpointPanel.vue):
[#7 MEDIUM] The Enabled el-switch (line ~51) is v-model bound and saved via @change=saveEndpoint; Element Plus updates the model before @change, so a failed PUT leaves the toggle showing a state the server never persisted. In saveEndpoint's catch block, restore enabled.value/slug.value from the last known server endpoint (or re-run load()).
[#14 LOW] saveEndpoint always sends parameters: fromRows(params.value), so flipping the Enabled switch silently persists unsaved param-table edits. Give the toggle its own minimal update that sends only { enabled } (ApiEndpointUpdate treats all fields optional), leaving parameter persistence to the explicit "Save changes" button.
[#15/#20 LOW] testRequestUrl (lines ~268-276) builds the preview from params.value, but runTest (lines ~454-471) sends endpoint.value.parameters — they diverge when params are edited but unsaved. Drive both from the same source: have runTest iterate params.value (filtering empty values) so the executed test matches the displayed URL.
[#16 LOW] load() (lines ~320-346) resets newKey/testResult/testError but not testValues, so "Try it" inputs leak across flows (same-named params pre-fill with the previous flow's value). Add testValues.value = {} to the reset block.
[#28 NIT] Remove the dead scoped CSS rule .param-delete-btn.el-button--small (lines ~543-548) and its comment — the per-row delete control no longer exists.

VERIFY: cd flowfile_frontend && npm run lint
Report a short summary of each fix.
```

### P5 — Docs & nits ⚪

```
Repo: /Users/edwardvaneechoud/flowfile_backup/Flowfile (branch claude/nifty-cray-5PBpi).
You are fixing two doc/nit review findings.

CONSTRAINTS:
- ONLY edit: .env.example, CLAUDE.md, and flowfile_core/flowfile_core/ai/tools/classification.py. Do NOT touch any other file (parallel sessions own them).
- Do NOT commit, push, or amend git history — make file changes only.

FIX:
[#27 NIT] The branch added a new env var in routes/flow_api.py:44 — FLOWFILE_API_RUN_TIMEOUT_SECONDS (default 120), controlling the published-flow request timeout. Document it in .env.example and in CLAUDE.md's "Environment Variables" list alongside the other FLOWFILE_* vars. (If P2 added FLOWFILE_API_MAX_CONCURRENT_RUNS, document that too if present.)
[#26 NIT] In ai/tools/classification.py the module docstring (lines ~20-21) says "The 21+1 explicit static set covers everything...". The branch added "api_response": "static" so the real count of "static" entries is now 24 and drifting. Update the count to match (or replace the hardcoded number with a non-numeric phrasing so it stops going stale). Do NOT alter the classification logic or any LLM-facing prompt text — docstring only.

VERIFY: poetry run ruff check flowfile_core/flowfile_core/ai/tools/classification.py
Report a short summary.
```

---

## After the parallel run

- Re-run the full suite: `poetry run pytest flowfile_core/tests/test_flow_api.py flowfile_core/tests/test_api_runner_security.py flowfile_core/tests/test_flow_api_lifecycle.py`
- The only cross-prompt coupling is conceptual: **#3** (global concurrency cap, P2) and **#4** (per-flow lock, P1) are complementary layers — both should land.
- Deferred (cross-cutting, handle solo if desired): none required; all 23 are covered above.
