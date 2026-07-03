# Discovery Dossier — KEY=todos-debt
## Debt & Live Pain Inventory — Flowfile monorepo

Investigated at commit `f6963c77` (branch `feature/claude-skills`), 2026-07-03.
Working dir: `/Users/edwardvaneechoud/flowfile_backup/Flowfile`.
Everything below was verified by reading files or running read-only commands unless marked **inferred**.

---

## 0. Headline numbers

| Metric | Value | How verified |
|---|---|---|
| `TODO|FIXME|HACK|XXX` word-boundary hits in source (py/ts/vue/rs/js, excluding node_modules/target/dist/.venv/services_dist/locks/stubs) | **54** | grep (command in §12) |
| Of which `FIXME` / `HACK` | **0 / 0** | same grep |
| `TODO(refactor)` in `flowfile_frontend/src` | **19** | `grep -rn 'TODO(refactor)' flowfile_frontend/src | wc -l` |
| `TODO(ux)` in `flowfile_frontend/src` | **11** | `grep -rn 'TODO(ux)' flowfile_frontend/src | wc -l` |
| Hard `pytest.mark.skip` (non-environment) | **3** (2 filter-null, 1 trainer param carve-out) | grep + read |
| `pytest.mark.xfail` markers | **4** — **2 of them are STALE (now XPASS)** | ran the tests (§4) |
| Skipped/`.todo` Vitest/Playwright TS tests | **0** | precise grep `\b(it|test|describe)\.(skip|todo|fixme|fails)\(` → no hits |
| NOTES/PLAN/ROADMAP/BACKLOG/CHANGELOG/TODO files | **none exist** | `find` (only pyarrow headers + AI "planner" module matched) |
| Commented-out feature blocks (routers, Vue components) | **none found** | grep `include_router` in main.py; `<!-- <` in Vue → no hits |

The repo had a deliberate comment purge (2026-06-04, per user memory "Lean comments, no AI narration"), so the surviving markers are curated, load-bearing debt notes — most carry an explicit plan. Hedge-comment sweeps ("for now", "temporary", "workaround") come back almost empty: the only "for now" is `flowfile_core/flowfile_core/kernel/models.py:32` ("Mutable fields on an existing kernel (packages-only for now)") and the only WORKAROUND is a test docstring.

---

## 1. The ~25 most consequential debt items (ranked)

### Tier 1 — correctness / durability risks in shipped paths

1. **Kernel artifact store holds whole objects in memory → OOM risk for large ML models.**
   `kernel_runtime/kernel_runtime/artifact_store.py:25-37`. Docstring: stores entire object via `self._artifacts`; ">1GB models cause memory pressure and potential OOM". Enumerated future fixes: spill-to-disk, external object store, streaming blob uploads. Ends with a **placeholder issue link**: `See: https://github.com/Edwardvaneechoud/Flowfile/issues/XXX (placeholder)` (`artifact_store.py:37`) — the only literal `XXX` in the codebase; no real issue was ever filed.

2. **Artifacts stuck in "pending" forever if a kernel crashes mid-upload.**
   `flowfile_core/flowfile_core/artifacts/service.py:64-70`: "TODO: Add a periodic cleanup task or TTL-based reaper... If a kernel crashes between prepare_upload and finalize_upload, the DB row stays in 'pending' forever." Options listed: background task, startup check, TTL column.

3. **S3 artifact upload integrity is not verified.**
   `shared/artifact_storage.py:266-275` (`finalize_upload`): "WARNING: This currently only checks existence, not integrity. We trust the kernel's SHA-256 hash without verification." TODO lists S3 ChecksumSHA256, download-and-verify, or Object Lock.

4. **Tauri startup-phase exit race → orphaned sidecar processes.**
   `flowfile_frontend/src-tauri/src/lib.rs:115-121` `TODO(D)`: quitting while the setup task is inside `start_services` can run shutdown before sidecar PIDs are stored, "so the spawn that lands after shutdown is never reaped". Full fix: `start_services` must check `is_shutting_down` before each spawn.

5. **Stale sidecar PID during shutdown can killpg a recycled PID.**
   `flowfile_frontend/src-tauri/src/sidecar/mod.rs:316-318` `TODO(C)`: in `handle_termination`, when a sidecar dies *during* shutdown the code returns without clearing the pid — "leaving it stale lets shutdown.rs's `.take()` + killpg target a process/group whose PID has been recycled."

6. **Desktop (Tauri) E2E coverage gap — the exact area that regressed in the migration is untested.**
   `flowfile_frontend/tests/web-flow.spec.ts:1-8` `TODO(F)`: "The Electron suites tests/app.spec.ts and tests/complex-flow.spec.ts (service startup, window lifecycle, multi-node flows) were deleted in the Tauri migration and not replaced... The area that regressed in the migration (sidecar startup / port allocation / SHUTDOWN) is now untested." Asks for a tauri-driver smoke test asserting no core/worker processes survive quit. Cross-referenced in root `CLAUDE.md:206-209`, `CONTRIBUTING.md:127`, `flowfile_frontend/CLAUDE.md:47,63`. Items 4+5+6 compound: the two known shutdown races have no automated test that would catch them.

### Tier 2 — code-generator correctness (flow → Python export)

7. **FlowFrame export: right joins emit `.collect().lazy()`, breaking the FlowFrame chain.**
   `flowfile_core/flowfile_core/flowfile/code_generator/join_handlers.py:477-480` `TODO(FlowFrame)`: the pattern returns a `pl.LazyFrame`; "The FlowFrame converter may need to override join handling or use framework-aware collect/lazy."

8. **FlowFrame export: formula nodes emit `pl.col`/`pl.lit` without `import polars as pl` when `framework == "ff"`.**
   `transform_handlers.py:54-58` `TODO(FlowFrame)` with three resolution options (add import / rewrite prefix / parameterize `to_polars_code`). Sibling issue at `transform_handlers.py:326-329`: `{framework}.col()` expressions unverified after `.collect().lazy()` chains.

9. **Fuzzy-match export serializes Polars `Expr` objects via `repr` → generated code is invalid Python.**
   `transform_handlers.py:259-262` `TODO(FlowFrame)`: "produces invalid code like `pl.lit(<Expr ['len()'] at 0x...>)`."

10. **Polars-code nodes exported as FlowFrame reference `ff.LazyFrame`, which flowfile doesn't export.**
    `code_generator.py:567-572` `TODO(FlowFrame)` with three options (always `pl.LazyFrame` in signatures / converter override / export LazyFrame).

11. **Two REAL xfail bugs in flow-vs-generated-code parity** (`flowfile_core/tests/flowfile/test_code_generator_edge_cases.py`, verified still failing on 2026-07-03):
    - `:459` `test_unique_without_columns`: flow's unique with `columns=[]` uses `group_by` internally → "at least one key is required in a group_by operation"; generated `.unique(keep='first')` is correct. **Flow is the buggy side.**
    - `:766` `test_groupby_with_concat_aggregation`: delimiter mismatch — generated `str.concat()` uses default `-`, flow uses `,` → `['x-y']` vs `['x,y']`.
    Plus two documented deprecated-API emissions (same file `:1002`, `:1054`): codegen still uses `with_row_count` (should be `with_row_index`) and `str.concat` (should be `str.join`); tests assert the *deprecated* form and carry "TODO: When fixed, change to: assert uses_modern and not uses_deprecated".

12. **Two STALE xfail markers — bugs already fixed, tests now XPASS silently** (verified by running them, §4):
    - `test_code_generator_edge_cases.py:201` `test_in_operator_numeric` — reason cites `flow_graph.py:1055-1056`, but the IN filter now lives in `flowfile_core/flowfile_core/flowfile/filter_expressions.py:144-167` and correctly splits before per-value numeric checks. **XPASS.**
    - `flowfile_core/tests/flowfile/node_designer/test_node_designer.py:516` `TestNumericStringAliasBug` — "'numeric' string alias maps to Decimal only". **XPASS** (fixed). The class docstring + the `Types.Numeric` "workaround" test (`:541`) now document a bug that no longer exists.
    Since neither marker is `strict=True`, CI shows XPASS and nobody notices. Skill-library lesson: **prefer `xfail(strict=True)` here, and treat XPASS in this repo as "delete the marker".**

13. **Param codegen accepts Python keywords as parameter names → invalid generated function signature.**
    `flowfile_core/flowfile_core/flowfile/code_generator/param_codegen.py:38`: "TODO: reject Python keywords too (keyword.iskeyword); a param named class/return emits an invalid signature."

14. **Typed flow-parameter defaults: empty default exports `""` for int/float params.**
    `flowfile_core/flowfile_core/flowfile/param_types.py:96`: "empty default returns '' for any type; an int/float param then exports a wrong-typed default."

### Tier 3 — engine / API behavior gaps

15. **Virtual-table laziness check punts on "conditional" nodes.**
    Same TODO three times in `flowfile_core/flowfile_core/configs/node_store/nodes.py:51,431,530` and once in `flowfile_core/flowfile_core/flowfile/flow_node/flow_node.py:623`: "resolve conditional nodes (read_data, polars_code, cloud_storage_reader) via isinstance checks like catalog_reader, then raise ValueError here instead". Today any conditional node upstream defaults the virtual table to non-optimized (flow_node.py:616-628) → the whole producer flow re-executes on every read (cost documented in `docs/users/visual-editor/catalog/virtual-tables.md:190-193`).

16. **Catalog-reader schema prediction does redundant round-trips.**
    `flowfile_core/flowfile_core/flowfile/flow_graph.py:3749`: "todo: There are quite some round-trips happening here because the Flowgraph tries to predict the schema." Perf debt on the catalog hot path.

17. **AI rate-limit scheduler is per-process and per-provider only.**
    `flowfile_core/flowfile_core/ai/scheduler.py:25-37` module docstring, verbatim: "State is per-process, not shared across workers — under gunicorn -w N ... the effective aggregate is ≈ N × the configured RPM / RPD. Pin to a single worker or scale the configured limit down by the worker count." and "Per-provider granularity (not per-(provider, model)) — surface → model fanout is a known limitation." No persistence across restarts (in-memory deques).

18. **AI `Provider` Protocol doesn't declare `default_model`** though `LiteLLMProvider` subclasses expose it → static analyzers warn at call sites. `flowfile_core/flowfile_core/ai/providers/base.py:124-130` with two resolution options.

19. **Manual-input node converts `None` to the string — nulls can't enter via manual input.**
    Product limitation surfaced as 2 skipped tests: `flowfile_core/tests/flowfile/test_basic_filter.py:751,761` — `@pytest.mark.skip(reason="Manual input converts None to string; test requires actual null values from file sources")`. is_null/is_not_null are effectively untested at integration level.

20. **Cloud-storage `append` write mode not implemented for non-delta formats.**
    `shared/cloud_storage/writers.py:222` raises `NotImplementedError("The 'append' write mode is not yet supported for this destination.")`; mirrored in `flowfile_worker/flowfile_worker/external_sources/s3_source/main.py:18-27` and `flow_data_engine.py:451-452`. Also: `flow_data_engine.py:555` (`File format {x} not yet implemented`), `:637` — **Iceberg read from cloud storage: "Not yet implemented"**.

### Tier 4 — frontend architecture debt (the big rock)

21. **19 god components / oversized files with written extraction plans (`TODO(refactor)`)** — full inventory §2. Worst: `CatalogView.vue` ~1700 LOC ("God component"), `Canvas.vue` ~1170 ("bundles 7+ concerns"), `DraggableItem.vue` ~1027 ("god component"), `fileBrowser.vue` ~1020, `PythonScript.vue` ~995, `GoogleAnalyticsReader.vue` ~990, `useDragAndDrop.ts` ~867 ("doing 5 jobs").

22. **Deprecated-store façade layer still live.**
    `flowfile_frontend/src/renderer/app/stores/node-store.ts` carries ~20 `@deprecated` backward-compat getters/actions proxying to flow-store/editor-store/results-store (lines 38-100, 557-580); `stores/column-store.ts:2` re-exports the split stores ("DEPRECATED: The monolithic useNodeStore has been split..."); path shims `features/designer/types.ts:1`, `features/designer/composables/useFlowExecution.ts:1`, `features/designer/editor/types.ts:1` all say "DEPRECATED: Import from '@/types'/'@/composables' instead". `ai-store-persistence.ts:51,208`: `autoPromote` deprecated, kept as read-only migration shim (`ai-store.ts:122-127`).

23. **Group-sharing frontend rollout is incomplete.**
    `ShareDialog.vue` is mounted in: SecretsView, DatabaseView, DashboardLibraryPanel, and CatalogView panels (TableDetailPanel, FlowDetailPanel, ArtifactDetailPanel, VisualizationsTab, VisualizationViewer, CatalogView) — verified via `grep -rln ShareDialog`. It is **NOT** mounted in `CloudConnectionView/`, `KafkaConnectionView/`, `GoogleAnalyticsConnectionView/` (grep for `shar|Share` in those dirs hits only an unrelated comment). Backend grants for those resource types exist (sharing.py, migration 020), so shared cloud/Kafka/GA connections work via API but can't be *granted* from their management UIs. Matches user memory "residual frontend mounts pending".

24. **`TODO(H)` — dev-mode CORS for the Tauri shell is unverified.**
    `flowfile_frontend/src/renderer/config/constants.ts:36-41`: under `tauri dev` the origin is `http://localhost:8080` but baseURL targets `http://127.0.0.1:<port>/` — cross-origin; "Confirm flowfile_core's CORS allows the Tauri origin (and tauri://localhost in a packaged build). Likely fine since the app runs, but it's unverified." Related: `flowfile_core/flowfile_core/main.py:133` still allowlists `http://localhost:5173` — a leftover; no dev server runs on 5173 (root CLAUDE.md confirms).

### Tier 5 — docs drift & housekeeping

25. **Stale docs.** `docs/quickstart.md:225`: "**Schedule it** to run automatically (coming soon)" — but the embedded scheduler + `docs/users/visual-editor/catalog/schedules.md` ship today. Same page (~:222) says saving "creates a `.flowfile`" — that format is **deprecated**: `flow_graph.py:5548-5551` raises `DeprecationWarning("The .flowfile format is deprecated. Please use .yaml or .json formats...")`; `tools/migrate/` exists to convert pickle-era `.flowfile` (≤ v0.4.1) to YAML. Also `docs/users/visual-editor/kernels.md:466` claims "no UI to browse or inspect the contents of stored artifacts... no visual artifact explorer yet" — partially stale: `dataPreview.vue:15-104` has a per-node Artifacts tab and CatalogView has `ArtifactDetailPanel.vue` for global artifacts (contents inspection may still be missing — **inferred**).

Honorable mentions: `artifacts/exceptions.py:71` backwards-compat alias `ArtifactNotActiveError = ArtifactStateError` ("TODO: Remove after deprecation period"); `routes.py:1594-1602` deprecated `GET /save_flow` kept for old clients (sends `Deprecation: true` header, warns in logs); `test_custom_component_integration.py:701` "TODO: Check if behaviour is correct in local run" (empty-input custom node semantics uncertain); `dataPreview.vue:162` refactor plan; subflow hard caps (`input_schema.py:1558-1560`: >9 data inputs / >10 outputs rejected).

---

## 2. Frontend `TODO(refactor)` inventory (all 19, with stated LOC and plans)

| File | LOC (per comment) | Plan summary |
|---|---|---|
| `views/CatalogView/CatalogView.vue:497` | ~1700 "God component" | extract useModalState (8 dialog refs), move 19 async handlers into catalog-store, useRouteSync composable |
| `views/DesignerView/Canvas.vue:2` | ~1170, "7+ concerns" | extract 6 draggable panel wrappers, useFlowClipboard, useContextMenu, useFlowHotkeys |
| `components/common/DraggableItem/DraggableItem.vue:132` | ~1027 "god component" | extract (plan in file) |
| `components/common/FileBrowser/fileBrowser.vue:227` | ~1020 | extract (plan in file) |
| `elements/pythonScript/PythonScript.vue:319` | ~995 | "Cohesive but long; defer unless touched" |
| `elements/googleAnalyticsReader/GoogleAnalyticsReader.vue:481` | ~990 | extract (plan in file) |
| `views/CatalogView/TableDetailPanel.vue:373` | ~905 | "cohesive. Defer unless touched" |
| `composables/useDragAndDrop.ts:3` | ~867, "doing 5 jobs" | split into useNodeComponentLoader, useDragMechanics, useNodeCopy, useClipboardPaste, useEdgeInsertion; keep file as thin façade |
| `pages/NodeDesigner.vue:275` | ~768 | "mostly modular; lower priority" |
| `views/CatalogView/FlowDetailPanel.vue:317` | ~750 after ScheduleTable extraction | remaining target listed |
| `pages/nodeDesigner/NodeDesignerHelpModal.vue:364` | ~724 | extract plan |
| `views/AdminView/AdminView.vue:348` | ~713 | extract plan |
| `components/nodes/NodeWrapper.vue:234` | ~709 | extract plan |
| `components/layout/Header/HeaderButtons.vue:242` | ~692 | extract plan |
| `elements/databaseReader/DatabaseReader.vue:176` | ~655 (paired w/ Writer) | extract plan |
| `pages/nodeDesigner/PropertyEditor.vue:378` | ~588 | extract plan |
| `elements/databaseWriter/DatabaseWriter.vue:157` | ~516 (paired w/ Reader) | extract plan |
| `views/DashboardsView/DashboardTile.vue:181` | ~484 | extract plan |
| `features/designer/dataPreview.vue:162` | "large" | extract DataTabs, OutputSelector, ArtifactsPanel, useTableData |

`TODO(ux)` items (11): `OpenDialog.vue:2-8` (keyboard nav, Material icons instead of ✔/✖/…, drop redundant flow_path column, tablist ARIA), `SaveDialog.vue:2-5` (move Save trigger into footer, surface catalog namespace), `CatalogNamespacePicker.vue:2-5` (collapse branches by default, replace string-based `'General'/'Local'` default lookup), `NamespaceTreeItem.vue:2` (non-selectable group rows should differ visually), `CreateDialog.vue:2` (tablist ARIA), `FlowSelectorView.vue:348` (aria-live for dirty-state).

---

## 3. Letter-tagged TODO series (Tauri-migration review residue)

Only **C, D, F, H** survive (grep `TODO([A-Z])` — no A/B/E/G anywhere), implying the others were fixed:
- `TODO(C)` `src-tauri/src/sidecar/mod.rs:316` — stale pid on shutdown-time termination (item 5).
- `TODO(D)` `src-tauri/src/lib.rs:115` — startup-phase exit race (item 4).
- `TODO(F)` `tests/web-flow.spec.ts:1` — desktop E2E gap (item 6).
- `TODO(H)` `src/renderer/config/constants.ts:36` — dev-mode CORS unverified (item 24).

---

## 4. Skipped / xfail test audit

### xfail (4 markers, none strict)
| Location | Reason (abridged) | Status 2026-07-03 |
|---|---|---|
| `test_code_generator_edge_cases.py:201` | IN filter quotes numerics ("1, 3, 5" checked whole) | **STALE — XPASS** (fix lives in `filter_expressions.py:144-167`) |
| `test_code_generator_edge_cases.py:459` | unique with empty columns → group_by needs ≥1 key | **still xfail (real bug in flow)** |
| `test_code_generator_edge_cases.py:766` | str.concat delimiter `-` vs `,` mismatch | **still xfail (real bug in codegen)** |
| `node_designer/test_node_designer.py:516` | `data_types="numeric"` maps to Decimal only | **STALE — XPASS** |

Verification run (isolated DB per user-memory rules; **must not** set `FLOWFILE_SKIP_STARTUP_MIGRATION=1` with a fresh DB path or you get `no such table: users`):
```
FLOWFILE_DB_PATH=<scratch>/xfail_probe2.db poetry run pytest \
  "flowfile_core/tests/flowfile/test_code_generator_edge_cases.py::TestBasicFilterOperators::test_in_operator_numeric" \
  "flowfile_core/tests/flowfile/test_code_generator_edge_cases.py::TestUniqueOperationVariations::test_unique_without_columns" \
  "flowfile_core/tests/flowfile/test_code_generator_edge_cases.py::TestGroupByEdgeCases::test_groupby_with_concat_aggregation" \
  -q -p no:cacheprovider -rX
# → "2 xfailed, 1 xpassed" (XPASS = test_in_operator_numeric)
FLOWFILE_DB_PATH=<scratch>/xfail_probe2.db poetry run pytest \
  "flowfile_core/tests/flowfile/node_designer/test_node_designer.py::TestNumericStringAliasBug" -q -rX
# → "1 passed, 1 xpassed"
```

### Unconditional skips (3)
- `test_basic_filter.py:751,761` — "Manual input converts None to string" (product limitation, item 19).
- `flowfile_worker/tests/test_train_apply_model.py:159` — logistic_regression/knn_classifier need 0/1 targets; "covered by a dedicated round-trip test" (benign parametrize carve-out).

### Environment skipifs (the dominant pattern — not debt per se, but a coverage cliff)
Dozens of `@pytest.mark.skipif(not is_docker_available(), ...)` across `flowfile_core/tests` (test_endpoints.py:1590+, test_flowfile.py:1217+, test_code_generator.py:2945+, external_sources/test_sql_source.py, flowfile_table/*), `flowfile_worker/tests` (test_app.py, test_funcs.py, external_sources/*), `flowfile_frame/tests/test_flowfile_frame.py:56+`, `kernel_runtime/tests/test_serialization.py` (9×). Variants: `requires_minio` (`test_catalog_cloud.py:42`, `test_catalog_cloud_virtual.py:82`, `test_catalog_namespace_storage.py:50`), MySQL on :3307 (`test_sql_source.py:244`), ADLS/GCS emulators (`test_utils/azurite/fixtures.py:54`, `test_utils/gcs/fixtures.py:48`), worker-not-running skip (`flowfile_core/tests/conftest.py:211,355`), Redpanda (`tests/kafka/conftest.py:34`), one Windows skip (`flowfile_frame/tests/test_lazy_frame.py:76`). **Implication:** on a laptop without Docker+MinIO+emulators, a large slice of the integration surface silently skips — a green local run is weak evidence.

### TS tests
No `.skip`/`.todo`/`.fixme` in any Playwright or Vitest spec (frontend or wasm). One perf-flake self-skip in `flowfile_worker/tests/test_catalog_visualize.py:193` ("workload too fast to measure overlap").

---

## 5. By-design "not supported" boundaries (skill-relevant, all verified)

- **Standalone Polars codegen refuses:** external_source (`connector_handlers.py:13-21`), cloud storage reader/writer (`:27-45`), Kafka source (`:53-58`), catalog reader/writer (`code_generator.py:993-1010`) → all say "Use FlowFrame export".
- **Exported projects:** server-backed `flowfile_ctx` APIs (global artifacts, catalog) raise `NotImplementedError` in exports; flagged in manifest warnings (`project_exporter.py:66,340`; `project_shim.py:280`; documented at `docs/users/visual-editor/tutorials/code-generator.md:189`).
- **Kernel (Python Script) nodes are skipped by Export-to-Python** (`docs/users/visual-editor/kernels.md:465`).
- **flowfile_frame:** lambdas in expressions unsupported (`flow_frame.py:164-174` NotImplementedError); writable file-like objects unsupported with the Polars-Code fallback (`flow_frame.py:1702,1764,1840,2040,2128`); `join_asof`/`join_where` not supported (`docs/users/python-api/reference/joins.md:102`); map_elements serialization pitfall warned at `lazy.py:354,433`.
- **WASM:** JSON read unsupported in browser (`flowfile_wasm/src/stores/flow-store.ts:2026`), Arrow files unsupported (`src/utils/remote-file.ts:33`, `ReadFileSettings.vue:260`). The formula-node publish gate from user memory is **resolved**: `flow-store.ts:36` pins `polars-expr-transformer==0.5.6` and `src/pyodide/engine/nodes_formula.py` exists.
- **Subflows:** max 9 data inputs / 10 outputs (`input_schema.py:1558-1560`).
- **Custom nodes (node designer):** single input DataFrame only; multi-input "planned" (`docs/for-developers/creating-custom-nodes.md:154`). "Coming Soon" list at `:475-483`: node templates, custom icons (note: `tests/.../test_custom_icons.py` exists, so possibly landed — **inferred**), node categories, testing framework, publishing.
- **Kernels beta limitations** (`docs/users/visual-editor/kernels.md:461-469`): codegen skip, artifact-browse UI (partially stale, see §1 item 25), pip packages installed unpinned at container startup (no lockfile; pin manually `pkg==x.y.z`).
- **Virtual tables** (`docs/users/visual-editor/catalog/virtual-tables.md:190-196`): non-optimized tables re-execute full producer flow per read; require registered producer flow; no Delta versioning/time-travel; one virtual table per producer flow.
- **Kernel update surface:** packages-only (`flowfile_core/kernel/models.py:32`).
- **`flowfile` CLI web UI:** host must be localhost, port must be 63578 (`flowfile/flowfile/web/__init__.py:154-156` NotImplementedError).

---

## 6. Historical incidents (symptom → root cause → fix → status)

1. **Worker task hangs forever on large results** → both transports (`spawner.handle_task`, `streaming.ws_submit`) joined the child before draining its result queue; a child `put()`ing >~64KB blocks in the queue feeder thread until the parent reads → deadlock; wide-table `calculate_schema` and deep query plans were realistic triggers → fix commit `3857aced` (2026-07-02, PR #564) added `spawner.drain_result_queue` (poll + bail when child exits), broke monitor loops on progress==100, reordered `apply_model_task` to put-before-signal, added >64KB regression tests (`flowfile_worker/tests/test_result_queue_deadlock.py`) → **fixed, merged**.
2. **delta-rs can't handle `az://` scheme** → upstream bug (delta-io/delta-rs#3716) → workaround `normalize_delta_path` rewrites `az://` → `abfss://` (`shared/cloud_storage/utils.py:13-22`) → **live workaround; remove when upstream fixes**.
3. **`.flowfile` pickle format** (≤ v0.4.1) → replaced by YAML/JSON in v0.5; saving as `.flowfile` now raises `DeprecationWarning` (`flow_graph.py:5548-5551`); `tools/migrate` converts old files (`tools/migrate/README.md`) → **deprecated, migration tool shipped, quickstart doc still references it**.
4. **Old frontends called `GET /save_flow`** → kept as deprecated shim with `Deprecation: true` header + log warning (`routes.py:1594-1602`) → **pending removal**.
5. **Electron → Tauri migration** deleted desktop E2E suites; sidecar startup/shutdown regressed during migration and remains untested (`web-flow.spec.ts:1`) → **open** (items 4-6).
6. **Monolithic `useNodeStore` split** into flow/editor/results/node stores → backward-compat proxy getters still in `node-store.ts`, `column-store.ts` shim re-export → **half-finished migration** (grep `useNodeStore` from "column-store" to see remaining consumers, e.g. `dataPreview.vue:168`).
7. **AI `autoPromote` boolean → `mode` enum** migration: persistence keeps read-only legacy field + seed shim (`ai-store-persistence.ts:51,208`; `ai-store.ts:122-127`) → **shim live**.

---

## 7. Stalled / WIP work visible in git

- Branch `improvement/improve-naming-unnamed-flows` @ `fa23a297` "Create different naming for flows" — 1 commit ahead of `f6963c77`, unmerged (this was the checked-out branch in the session's git snapshot).
- ~60 local branches, many marked "behind" their origin (e.g. `feature/improve-params: ahead 4`, `feature/group-nodes`, `claude/*` experiment branches). Local `main` ref is **very stale** (`git log main` tops out at PR #185-era commits) — in this backup clone, do not trust local `main`; the freshest history is on feature branches. **Repo-hygiene observation, inferred as backup-clone artifact.**

---

## 8. Planning/roadmap files, issue templates, changelogs

- **No** NOTES/PLAN/ROADMAP/BACKLOG/CHANGELOG/TODO/KNOWN_ISSUES files anywhere (find across repo; only false positives: pyarrow `plan.h` headers in build outputs, the AI "planner" agent module).
- Roadmap lives implicitly in: docs "Coming Soon" sections (`docs/users/python-api/tutorials/index.md:21-30` — Data Pipeline Patterns, Performance Optimization, Integration Examples; `docs/for-developers/creating-custom-nodes.md:475`), the TODO(refactor) plans, and GitHub Discussions (per `docs/community.md`).
- **Issue templates:** exactly one — `.github/ISSUE_TEMPLATE/bug_report.md` — the unmodified GitHub default (still asks about "Smartphone (please complete the following information): Device: [e.g. iPhone6]"), untailored to a desktop ETL app; no feature-request template, no config.yml.
- Release notes happen via GitHub Releases + per-release Discussion threads (root `CLAUDE.md`), not a CHANGELOG file.

---

## 9. Deprecation & legacy-shim inventory (beyond §1)

| What | Where | Status |
|---|---|---|
| `ArtifactNotActiveError` alias | `artifacts/exceptions.py:71` | remove after deprecation period |
| `GET /save_flow` | `routes.py:1594-1602` | deprecated, warns + header |
| `.flowfile` save format | `flow_graph.py:5548` | raises DeprecationWarning |
| node-store compat getters (~20) | `stores/node-store.ts:38-100,557-580` | live |
| `column-store.ts` monolith shim | `stores/column-store.ts:2` | live (still imported, e.g. dataPreview.vue) |
| designer path shims (3 files) | `features/designer/{types,composables/useFlowExecution,editor/types}.ts:1` | live |
| `autoPromote` persistence field | `ai-store-persistence.ts:51,208` | read-only legacy |
| codegen emits `with_row_count` / `str.concat` | tests `:1002,:1054` document it | deprecated-Polars-API emissions, unfixed |

---

## 10. Ranked live-pain summary (what a skill library must warn about)

1. **Code generator ↔ flow parity is not guaranteed** — 2 live xfail divergences + 5 TODO(FlowFrame) export bugs + deprecated Polars API emissions. Any skill touching "Export to Python"/FlowFrame conversion must check `test_code_generator_edge_cases.py` first and know the FlowFrame framework-prefix trap.
2. **Tauri sidecar lifecycle has two known races and zero desktop E2E** — changes to `src-tauri/src/sidecar/*` or `lib.rs` are the riskiest edits in the repo; manual verification (quit both ways, `ps aux | grep flowfile`) is currently the only guardrail.
3. **Frontend god components** — 19 files with pre-written extraction plans; skills should say "follow the TODO(refactor) plan at the top of the file; don't invent a new decomposition", and "don't import from deprecated shim paths".
4. **Artifacts subsystem has three durability holes** (pending-reaper, unverified S3 integrity, in-memory OOM) — treat artifacts as best-effort; large-model workflows are at risk.
5. **Test-environment skips hide most integration coverage** — always ask "did Docker/MinIO tests actually run?"; use isolated `FLOWFILE_DB_PATH`; never `FLOWFILE_SKIP_STARTUP_MIGRATION=1` with a *fresh* DB (no tables); XPASS = stale xfail, delete the marker.
6. **Docs drift** — quickstart (scheduler "coming soon", `.flowfile` save), kernels.md artifact-UI claim; skills sourcing facts from docs must cross-check code.
7. **Sharing UI rollout unfinished** — Cloud/Kafka/GA connection views lack ShareDialog mounts; the pattern to copy is in `SecretsView.vue` / `DatabaseView.vue`.

---

## 11. Notable per-file quote bank (load-bearing exact text)

- `artifact_store.py:37`: `See: https://github.com/Edwardvaneechoud/Flowfile/issues/XXX (placeholder)`
- `shared/artifact_storage.py:268`: `WARNING: This currently only checks existence, not integrity.`
- `lib.rs:115`: `TODO(D): startup-phase exit race.`
- `mod.rs:316`: `TODO(C): clear the pid here too — leaving it stale lets shutdown.rs's `.take()` + killpg target a process/group whose PID has been recycled.`
- `web-flow.spec.ts:4`: `The area that regressed in the migration (sidecar startup / port allocation / SHUTDOWN) is now untested.`
- `ai/scheduler.py:31-33`: `State is **per-process**, not shared across workers — under gunicorn -w N ... ≈ N × the configured RPM / RPD.`
- `flow_graph.py:3749`: `todo: There are quite some round-trips happening here because the Flowgraph tries to predict the schema.`
- `useDragAndDrop.ts:3`: `TODO(refactor): ~867 LOC, doing 5 jobs.`
- `CatalogView.vue:497`: `TODO(refactor): God component (~1700 LOC).`

---

## 12. Verified commands (copy-pasteable)

```bash
# Master debt sweep (54 hits at f6963c77)
grep -rniE '\b(TODO|FIXME|HACK|XXX)\b' --include='*.py' --include='*.ts' --include='*.vue' \
  --include='*.rs' --include='*.js' --include='*.mjs' . 2>/dev/null \
  | grep -vE 'node_modules|/target/|/dist/|\.venv|site-packages|\.pyi:|package-lock|poetry\.lock|/build/|services_dist|test-results|playwright-report|\.min\.'

# Letter-tagged migration TODOs (C, D, F, H)
grep -rn 'TODO([A-Z])' --include='*.ts' --include='*.rs' --include='*.vue' --include='*.py' . \
  | grep -vE 'node_modules|/target/|/dist/|services_dist'

# Frontend refactor/ux backlogs
grep -rn 'TODO(refactor)' flowfile_frontend/src   # 19 hits
grep -rn 'TODO(ux)' flowfile_frontend/src         # 11 hits

# xfail/skip audit
grep -rn 'pytest.mark.skip\|pytest.mark.xfail\|pytest.skip' --include='*.py' . | grep -v '\.venv'

# Probe an xfail for staleness (isolated DB — do NOT combine a fresh DB with
# FLOWFILE_SKIP_STARTUP_MIGRATION=1, that yields "no such table: users")
FLOWFILE_DB_PATH=/tmp/probe.db poetry run pytest \
  "flowfile_core/tests/flowfile/test_code_generator_edge_cases.py::TestBasicFilterOperators::test_in_operator_numeric" \
  -q -p no:cacheprovider -rX   # → 1 xpassed (stale marker)

# Sharing-UI mount coverage
grep -rln 'ShareDialog' flowfile_frontend/src/renderer --include='*.vue'

# Planning-file sweep (returns nothing real)
find . \( -iname 'NOTES*' -o -iname 'PLAN*' -o -iname 'ROADMAP*' -o -iname 'BACKLOG*' \
  -o -iname 'CHANGELOG*' -o -iname 'TODO*' \) | grep -vE 'node_modules|services_dist|binaries'

# Deadlock-incident commit
git show 3857aced --stat --format='%H%n%an %ad%n%B'
```
(macOS note: `timeout` is not available in this zsh; and `echo ===` breaks zsh — quote separators.)
