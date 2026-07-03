# Discovery dossier — KEY=core-architecture (flowfile_core engine deep-read)

> **FROZEN EVIDENCE** — snapshot at commit `f6963c77` (2026-07-03, v0.12.7); deliberately unmaintained and expected to drift.
> Authority order: **live repo → `.claude/skills/` → this file (leads only — re-verify before citing).** See [`README.md`](./README.md).

All paths relative to repo root `Flowfile` unless absolute.
Verified on branch `improvement/improve-naming-unnamed-flows` (clean tree), 2026-07-03. Everything below was read from source; items I could not fully confirm are marked **inferred**.

Verified line counts (`wc -l`):

| File | Lines |
|---|---|
| `flowfile_core/flowfile_core/flowfile/flow_graph.py` | 5977 |
| `flowfile_core/flowfile_core/flowfile/flow_data_engine/flow_data_engine.py` | 2842 |
| `flowfile_core/flowfile_core/schemas/input_schema.py` | 1995 (101 `class` statements) |
| `flowfile_core/flowfile_core/schemas/schemas.py` | 848 |
| `flowfile_core/flowfile_core/main.py` | 402 |
| `flowfile_core/flowfile_core/flowfile/flow_node/flow_node.py` | 1930 |

---

## 1. The object model — who does what

```
FlowfileHandler (flowfile/handler.py)          in-memory registry of open flows (`flow_file_handler`)
  └── FlowGraph (flowfile/flow_graph.py:1039)  the DAG: nodes, groups, history, run orchestration
        └── FlowNode (flowfile/flow_node/flow_node.py:54)  one node: settings, hash, schema, results
              ├── NodeExecutor (flow_node/executor.py:64)  HOW/WHETHER to run (single source of truth)
              ├── NodeExecutionState (flow_node/state.py)  new-style state (mirrored to legacy node_stats)
              └── results: NodeResults → FlowDataEngine    the Polars LazyFrame wrapper
```

- `FlowGraph.__init__` (flow_graph.py:1072–1143): takes `FlowSettings|FlowGraphConfig`, creates `uuid` (uuid1), `_node_db: dict[node_id → FlowNode]`, `_flow_starts` (explicit source nodes), `_groups` (purely visual, "never read by the executor" — comment at 1111), `HistoryManager`, `ArtifactContext`, subflow recursion guards (`_subflow_ancestry` frozenset + `_subflow_depth`, deliberately plain attributes "because stages execute on ThreadPoolExecutor threads" — 1120-1123), `_owner_user_id` (last user_id stamped on any node settings; lets `restore_from_snapshot` re-stamp owner on undo of an empty graph — 1125-1128).
- `FlowfileHandler` (handler.py) — `import_flow` (calls `open_flow`), `register_flow`, `get_flow`, per-user session registry, `save_flow`/`save_as_flow`, `rekey_flow`. 253 lines.
- `FlowDataEngine` (flow_data_engine.py:216) — high-level wrapper over `pl.DataFrame | pl.LazyFrame`. Holds `_lazy` flag, `number_of_records` (−1 for lazy = unknown), `_streamable`, cached `_schema: list[FlowfileColumn]`. All transform verbs (`do_filter`, `join`, `do_group_by`, `do_pivot`, `make_unique`, `concat`, `do_select`, …) return new `FlowDataEngine`s built from lazy expressions. `execute_polars_code` / `execute_sql_query` module functions back the polars_code / sql_query nodes.

## 2. Node lifecycle — add / connect / update

### 2.1 Adding a node from the UI

1. `POST /editor/add_node/` (routes/routes.py:546) → `flow.add_node_promise(NodePromise)` — an unconfigured placeholder whose function returns an empty `FlowDataEngine` (flow_graph.py:1651–1696). If the type has default settings (`check_if_has_default_setting`), the route immediately constructs the settings model with defaults and calls `add_func = getattr(flow, "add_" + node_type)` (routes.py:594) — with history tracking temporarily disabled so "Add node" is one undo entry (routes.py:590–610).
2. `POST /update_settings/` (routes.py:1146 `add_generic_settings`) — the generic settings endpoint. Resolution is **string-convention driven**:
   - `node_type = camel_case_to_snake_case(node_type)`
   - settings class looked up by `get_node_model("node" + node_type.replace("_", ""))` (routes.py:111–118) which scans `input_schema.__dict__` for a class whose lowercase name matches. So node type `fuzzy_match` ⇒ class `NodeFuzzyMatch` (matched as `nodefuzzymatch`).
   - add function is `getattr(flow, "add_" + node_type)` (routes.py:1168).
   - `input_data["user_id"] = current_user.id` stamped before validation (routes.py:1158).
   - 422 for validation errors, **419** (non-standard) when the add function raises (routes.py:1189).
3. Connections: `POST /editor/connect_node/` → `add_connection(flow, node_connection)` (flow_graph.py:5842). Validation (`validate_connection`, 5824): rejects wiring **into** a source node (template `input == 0`; message built by `format_source_target_detail`, shared with the AI layer) and cycles (`_would_create_cycle`, DFS over `leads_to_nodes`, 5767). Dynamic-input nodes (only `run_flow`; `NodeTemplate.dynamic_inputs=True`) go through `_add_keyed_connection_validated` (5922): must be configured first (a `NodePromise` target ⇒ 422 "Configure the node before connecting inputs"), one edge per `input-N` handle, handle must be within `setting_input.input_slots` count. `input-0` is the **reserved parameter handle** (flow_node/input_handles.py:8).

### 2.2 `add_node_step` — the one true node factory

`FlowGraph.add_node_step` (flow_graph.py:3378–3495) is what every `add_<type>` method calls:
- If a node with the id exists **and its node_type differs**, the old node is deleted first (3438–3442). If it exists with the same type, `existing_node.update_node(...)` is called (preserving connections).
- `input_node_ids` binds inputs on first creation; for existing nodes the current `all_inputs` are kept.
- Nodes that may exist without inputs are special-cased by name/type: `function.__name__ in ("placeholder", "analysis_preparation")` or `node_type in ("cloud_storage_reader", "catalog_reader", "polars_lazy_frame", "input_data")` (3451–3455); everything else with no inputs raises `Exception("No data initialized")`.
- If `setting_input.output_field_config` exists, the schema callback is wrapped by `create_schema_callback_with_output_config` **even when disabled** (3420–3436).
- Each node gets `node._params_getter` — a closure returning the *live* flow parameters so lazy schema prediction can substitute `${param}` refs (3484–3494).

Source-style nodes (read, database_reader, datasource/manual_input) **don't** use `add_node_step`; they construct/patch the `FlowNode` directly and register via `self.add_node_to_starting_list(node)` + `self._node_db[...]`/`_node_ids` (see add_read at 4646, add_database_reader at 3950, add_datasource at 4735).

### 2.3 History capture

`@with_history_capture(HistoryActionType.UPDATE_SETTINGS)` decorates the add methods (46 occurrences counted). The decorator (flow_graph.py:173–223):
- records `_owner_user_id` from `settings_input.user_id`,
- snapshots `get_flowfile_data()` before, calls the method, then `HistoryManager.capture_if_changed` (diff-based; no-op settings updates don't create undo entries),
- honors `flow_settings.track_history`.

### 2.4 Two traced node types

**Simple — `filter`** (single input):
1. Settings: `input_schema.NodeFilter(NodeSingleInput)` (input_schema.py:542) → `filter_input: transform_schema.FilterInput` (+ `split_mode: bool` for pass/fail two-output mode) + `get_default_description()`.
2. Registered in `NODE_TYPE_TO_SETTINGS_CLASS["filter"]` (schemas.py:27–75) — used by `NodeInformation.validate_setting_input` when deserializing saved flows (schemas.py:647–664).
3. Template: `NodeTemplate(name="Filter data", item="filter", input=1, output=1, transform_type="narrow", laziness="lazy", …)` in `configs/node_store/nodes.py::get_all_standard_nodes` (list at nodes.py:10; `filter` entry ~line 172).
4. Graph method `add_filter` (flow_graph.py:2224, decorated 2263-context): `_func(fl: FlowDataEngine)` builds a polars-expr-transformer expression from either the advanced string or a `BasicFilter` (via `build_filter_expression`), materializes the advanced expression back onto the settings (`filter_settings.filter_input.advanced_filter = expression` — the node self-normalizes), returns `fl.filter_split(expression)` (a `NamedOutputs`) in split mode else `fl.do_filter(expression)`. Then `add_node_step(..., node_type="filter", renew_schema=False, input_node_ids=[filter_settings.depending_on_id])`.
5. Update route: `POST /update_settings/?node_type=filter` (generic).
6. Frontend contract (core side): `/node_list` (routes.py:1263) serves the `NodeTemplate`s (item/name/image/inputs/outputs/tags); `get_vue_flow_input` (flow_graph.py:5689) serializes nodes+edges (`NodeEdge` with `sourceHandle`/`targetHandle`, e.g. `output-0`→`input-0`).

**Complex — `read`** (source, add_read at flow_graph.py:4646) and **`join`** (two-input, add_join at 2537):
- `read`: settings `NodeRead(NodeBase)` with `received_file: ReceivedTable` (input_schema.py:891/201) carrying a discriminated union `InputTableSettings` (csv/json/parquet/excel/ipc/ndjson/avro; input_schema.py:189–198). `add_read`:
  - Excel: default sheet resolved eagerly via `fastexcel` (4652–4657).
  - `_func()` chooses at run time: `execution_location == "local"` or parquet/ipc/ndjson or utf-CSV ⇒ in-core lazy `FlowDataEngine.create_from_path`; otherwise (exotic encodings, excel) ⇒ `FlowDataEngine.create_from_path_worker` which asks the **worker** to convert to parquet (ExternalCreateFetcher, subprocess_operations.py:1143).
  - schema callback selected by file type: declared `fields` if present, else read-the-file for csv/json/parquet/ipc/ndjson, else `get_xlsx_schema_callback` (bounded 100-row sample; flow_graph.py:226–338). Set as **both** `node.schema_callback` and `node.user_provided_schema_callback` (survives resets).
  - `read` nodes are the only type with **source-file change detection**: executor `_source_file_changed` compares `SourceFileInfo` stat snapshots (executor.py:297–334); `reset()` intentionally preserves `source_file_info` (flow_node.py:1538).
- `join`: settings `NodeJoin(NodeMultiInput)` (input_schema.py:672) with `join_input: transform_schema.JoinInput`; `_func(main, right)` deep-copies join input and recomputes `is_available` per select field before `main.join(...)`; explicit `schema_callback` uses `calculate_join_schema(JoinInputManager, left_schema, right_schema, auto_generate_selection)` from `flowfile/schema_callbacks.py` — schema prediction without executing. Template: `input=2, transform_type="wide", laziness="lazy"` (nodes.py:64–77).

### 2.5 Add-a-node checklist (exact, derived from the above)

1. **Settings model** in `flowfile_core/schemas/input_schema.py`: subclass `NodeSingleInput` (`depending_on_id: int|None = -1`) / `NodeMultiInput` (`depending_on_ids: list[int]`) / `NodeBase` (sources). Class name MUST be `Node` + CamelCase of the node_type with underscores removed matching lowercase (`get_node_model` does `"node" + node_type.replace("_","")` lowercase scan — routes.py:111,1170). Implement `get_default_description()`; optionally `to_yaml_dict()` for curated YAML.
2. **Register** in `NODE_TYPE_TO_SETTINGS_CLASS` (schemas.py:27) — otherwise saved flows fail validation with "Unknown node type".
3. **Template** in `configs/node_store/nodes.py::get_all_standard_nodes`: `item` == node_type, `input`/`output` counts, `multi`, `node_group`, `node_type` (input/output/process), `transform_type` (**narrow** ⇒ eligible for LOCAL_WITH_SAMPLING, **wide** ⇒ forced remote when optimizing downstream), `laziness` (lazy/eager/conditional — drives `check_upstream_laziness` for catalog virtual tables), `can_be_start`, `tags` (palette search), image svg. Add a `node_defaults` entry (nodes.py:755) if the node should get default settings on drop.
4. **`add_<node_type>` method on FlowGraph**, decorated `@with_history_capture(HistoryActionType.UPDATE_SETTINGS)`, calling `self.add_node_step(node_id=…, function=_func, node_type="<node_type>", setting_input=…, input_node_ids=[…], schema_callback=…)`. `_func` receives one `FlowDataEngine` per input slot and returns a `FlowDataEngine` or `NamedOutputs` (multi-output). **`_func` must be safe to run on empty schema-only frames** — schema prediction executes it against `FlowDataEngine.create_from_schema(...)` inputs (`_predicted_data_getter`, flow_node.py:1060).
5. If it's a **source doing network I/O**: do NOT fetch in core — build worker settings and use an `External*Fetcher` (see §4); GA4 (`add_google_analytics_reader`, 4254) is the canonical template.
6. Frontend: node config component + mapping (outside core; the core-side contract is the `/node_list` template + generic `/update_settings/`).
7. Parity surfaces that otherwise silently lag: code generator (`flowfile/code_generator/`), `flowfile_frame` API method, WASM node set (16-node subset), AI tools' node catalog. (**inferred** from structure; each has its own registry.)
8. Tests under `flowfile_core/tests/`.

## 3. Execution: plan → stages → node executor

### 3.1 `run_graph` (flow_graph.py:5328–5377)

```
if is_running: raise
is_running=True; is_canceled=False; clear log
plan = compute_execution_plan(nodes, _flow_starts + implicit starters)   # util/execution_orderer.py:39
plan_skip_ids = {skip nodes}
_prepare_rerun_artifacts(plan_skip_ids)          # python_script/kernel artifact hygiene (5184)
latest_run_info = RunInformation(run_type="full_run")
performance_mode = flow_settings.execution_mode == "Performance"        # 5360
params = {p.name: p.typed_default() for p in flow_settings.parameters}
failed = _execute_stages(plan, performance_mode, params, plan_skip_ids)  # 5235
if not canceled: _run_post_execution_callbacks(failed, skips)            # e.g. Kafka offset commits (5298)
finally: is_running = False
```

- **Skip pre-pass** (`util/node_skipper.py`): `skip_nodes = [n for n in nodes if not n.is_correct]` then ONE extension pass adding their `leads_to_nodes` (the comprehension evaluates over the initial list — one transitive level only). Deeper dependents are still protected at run time because `_execute_stages` adds `get_all_dependent_nodes()` of every failed/skipped node to `skip_node_ids` (5289–5294). `is_correct` (flow_node.py:494): NodePromise ⇒ False; else input count matches template (or multi with ≥1 input, or multi+can_be_start, or dynamic-inputs).
- **Topological staging** (`util/execution_orderer.py`): Kahn-style level sort; each `ExecutionStage` = nodes with no mutual deps, run in parallel. Cycle ⇒ `Exception("Cycle detected in the graph…")` (execution_orderer.py:80). Note `compute_in_degrees_and_adjacency_list` re-adds skipped downstream nodes into `node_map` via `leads_to_nodes` (lines 119–126), so skipped nodes may appear in stages; they are filtered per-stage by `skip_node_ids` in `_execute_stages` (5259).
- **Parallelism** (5269–5287): `max_workers = 1 if execution_location == "local" else flow_settings.max_parallel_workers` (default 4, `FlowGraphConfig` schemas.py:153). ThreadPoolExecutor per stage; results gathered with `as_completed`. A `run_info_lock` (`threading.Lock`) guards `latest_run_info` mutations.
- **Per-node** `_execute_single_node` (5093–5182): creates `NodeResult`, appends under lock, then the **parameter substitution dance**: `saved_hash = node._hash`; `apply_parameters_in_place(node.setting_input, params)` mutates settings in place (so closures see resolved values); after `node.execute_node(...)` runs, `restore_parameters(restorations)` puts `${...}` back AND `node._hash = saved_hash` is restored — the long comment at 5125–5131 explains that without restoring the hash, the next `setting_input` write would see a hash mismatch → spurious `reset()` → loses `example_data_generator`/`has_completed_last_run`.
- **Single-node fetch**: `trigger_fetch_node` (4940) — used by `POST /node/trigger_fetch_data` (routes.py:266); runs one node with `reset_cache=True`, `run_type="fetch_one"`. `validate_if_node_can_be_fetched` (4904) recomputes the plan and refuses nodes in the skip set.
- **Run route** (`POST /flow/run/`, routes.py:413–442): per-flow `asyncio.Lock` from `run_lock.py`, 422 if already running, executes `_run_and_track` as a FastAPI BackgroundTask. `_run_and_track` (routes.py:304): two-phase catalog run record (start_run before, complete_run after), auto-registers unregistered flows into the catalog on first run, snapshots the flow YAML into the run record. Run status polled via `GET /flow/run_status/` — **202 while running, 200 when done** (routes.py:467–480).
- **Cancel**: `flow.cancel()` sets `is_canceled` and calls `node.cancel()` on every node (5497). `FlowNode.cancel` (1428) cancels, in priority order: worker fetcher (`_fetch_cached_df.cancel()` → worker `/cancel_task/{id}`), kernel execution, running subflow; always sets `is_canceled` state.

### 3.2 Execution modes and locations

- `ExecutionModeLiteral = Literal["Development", "Performance"]`, `ExecutionLocationsLiteral = Literal["local", "remote"]` (schemas.py:18–19). Default `execution_mode="Performance"` on `FlowGraphConfig` (schemas.py:151); unknown values coerced to Performance by validator.
- **Development mode** (`performance_mode=False`): every executed node generates 100-row example data (worker sampler or local sample) and marks `has_run_with_current_setup`, enabling skip-on-rerun. **Performance mode**: skips example-data generation AND `_decide_execution` treats it as always-run (`InvalidationReason.PERFORMANCE_MODE`) — except `cache_results` nodes, which force `effective_performance_mode=False` so their cache materializes (executor.py:138–142).
- **Location**: `execution_location` defaults from `get_global_execution_location()` — "remote" iff `OFFLOAD_TO_WORKER` (schemas.py:78–87). Validators clamp: if global is local, a requested "remote" is downgraded (`get_prio_execution_location`, schemas.py:114). Changing location or mode via the `flow_settings` setter triggers a full graph reset (flow_graph.py:1149–1165); same for the `execution_location` property setter (4893).
- Location `"local"` also forces `needs_run() == False` (flow_node.py:1225) and stage parallelism of 1.

### 3.3 NodeExecutor decision table (executor.py:162–241) — single source of truth

Order matters:
1. template `node_group == "output"` → RUN (`OUTPUT_NODE`) — sinks always run.
2. `node_type == "run_flow"` → RUN always ("the referenced flow file can change without any parent-settings change", 178–182).
3. `force_refresh` (reset_cache) → RUN (`FORCED_REFRESH`).
4. `node_settings.cache_results`: worker has result for `node.hash` (`results_exists`) → **SKIP**; else RUN (`CACHE_MISSING`). Checked before performance mode "so cached results are preserved even when upstream produces no new data".
5. `performance_mode` → RUN (`PERFORMANCE_MODE`).
6. `not state.has_run_with_current_setup` → RUN (`NEVER_RAN`).
7. read-node source file changed → RUN (`SOURCE_FILE_CHANGED`).
8. else **SKIP** (results still in memory from previous run).

Strategy (`_determine_strategy`, 213–241): local → `FULL_LOCAL`; `cache_results` → `REMOTE` ("caching needs full materialization"); template `transform_type == "narrow"` → `LOCAL_WITH_SAMPLING` (compute lazily in core, ship only the sample job to the worker); else `REMOTE`. Override in `execute()` (121–130): a **wide** transform that got `LOCAL_WITH_SAMPLING` is promoted to `REMOTE` when `optimize_for_downstream` and not local.

Error handling (`_handle_error`, 358–400): `"No such file or directory (os error"` ⇒ upstream worker cache evicted → re-run all inputs with `reset_cache=True`, then retry self once (`retry=False`). `"Connection refused" + "/submit_query/"` ⇒ logs "Ensure the worker process is running, or change settings to local execution".

Dual state: new `NodeExecutionState` is mirrored into legacy `node_stats` by `_sync_state_to_legacy` (351–356). Both exist; the legacy one still feeds `needs_run`/`get_predicted_resulting_data`.

### 3.4 Caching & invalidation

- **Node hash** (`calculate_hash`, flow_node.py:633–653): `get_hash(input-node-hashes + [hash(setting_input), parent_uuid, _cache_epoch])`. Dynamic-input nodes fold target-handle+source-handle into each input hash so re-wiring invalidates. `parent_uuid` is per-FlowGraph-instance (uuid1 at construction) ⇒ hashes (and therefore worker cache keys) never collide across flows/instances — and never survive a graph reload.
- **The hash is the worker cache key** (`file_ref`): `results_exists(hash)` asks `GET {WORKER_URL}/status/{hash}`; `cache_results` nodes read back via `get_external_df_result(hash)` (flow_node.py:1332–1338).
- `needs_reset()` = stored `_hash` != recomputed hash; the `setting_input` setter calls `reset()` (flow_node.py:424–448), which cascades downstream via `evaluate_nodes()` and clears run state/schemas; comment at 1540–1549: schema-callback prefetch after reset is deliberately restricted to start nodes "eagerly starting them races with the cascade of resets".
- `invalidate_cache()` (1553) bumps `_cache_epoch` — used when external state changes without settings changes (Kafka consumer-group offset reset).

### 3.5 Schema prediction (no-execution schemas)

- `node.schema_callback` is wrapped in `SingleExecutionFuture` (flow_node/schema_callback.py) — thread-safe run-at-most-once with cached result, background `ThreadPoolExecutor(max_workers=1)`, and a **generation counter** so an in-flight callback finishing after `reset()` can't poison the new state (comments 44–47, 126–128).
- If no explicit callback, `create_schema_callback_from_function` wraps the node function itself and runs it against **predicted** inputs (`get_predicted_resulting_data` → `FlowDataEngine.create_from_schema(...)` — schema-only empty frames). Multi-output functions populate `_named_schemas` per handle in the same call (flow_node.py:231–262).
- Preview UI: `get_table_example` (1716) reads the 100-row arrow sample written by the worker (`results.example_data_generator` = `get_read_top_n(file_ref)`), or `.head(100).collect()` per named output.

## 4. The worker offload seam

### 4.1 Config (configs/settings.py)

- `OFFLOAD_TO_WORKER: MutableBool = MutableBool(os.environ.get("FLOWFILE_OFFLOAD_TO_WORKER", "1") == "1")` (settings.py:22) — note **only the exact string "1"** enables; "true" would disable. Live-mutable (`--run-flow` CLI calls `OFFLOAD_TO_WORKER.set(False)`, main.py:275–277).
- `WORKER_URL = config("FLOWFILE_WORKER_URL", default=get_default_worker_url(WORKER_PORT))` (settings.py:133; Starlette `Config(".env")`). `get_default_worker_url` (95–117): `WORKER_HOST` env if set; else `127.0.0.1` on Windows, `0.0.0.0` elsewhere; port `FLOWFILE_WORKER_PORT` default 63579; appends `"/worker"` when `SINGLE_FILE_MODE` (co-hosted worker on core port).

### 4.2 Wire protocol (`flow_data_engine/subprocess_operations/subprocess_operations.py`)

- Serialization is **`pl.LazyFrame.serialize()` raw bytes** — the *query plan*, not data. `trigger_df_operation` (39–60): `POST {WORKER_URL}/submit_query/` with headers `Content-Type: application/octet-stream`, `X-Task-Id` (= node hash), `X-Operation-Type` (`store` | `calculate_number_of_records` | …), `X-Flow-Id`, `X-Node-Id`, optional `X-Kwargs` JSON. Sampling: `POST /store_sample/` with `X-Sample-Size` (63–78). Fuzzy join: `POST /add_fuzzy_join` with base64-in-JSON `PolarsOperation`s (81–103). Worker endpoints confirmed in `flowfile_worker/flowfile_worker/routes.py` (63 `/submit_query/`, 110 `/store_sample/`, 893 `/status/{task_id}`, 1048 `/add_fuzzy_join`).
- Preferred transport is **WebSocket streaming** (`_execute_streaming` → `streaming_submit`/`streaming_start` in `subprocess_operations/streaming.py`), falling back to REST + polling on any connect/send error (`ExternalDfFetcher.__init__`, 964–1006).
- Status/result: `GET /status/{file_ref}` → `Status` model; result for polars ops is a **base64 serialized LazyFrame** decoded by `get_df_result` (614–616) — i.e. the worker returns a lazy *scan over its cached parquet*, not materialized data. `results_exists` (573) returns False immediately when `OFFLOAD_TO_WORKER` is off. `DELETE /clear_task/{ref}`, `POST /cancel_task/{ref}`.
- `BaseFetcher` (659) — thread-safe poller: daemon thread polls `/status/` every 0.5s; `get_result()` blocks on a Condition; `cancel()` closes WS, posts `/cancel_task/`, sets stop event, joins with 5s timeout. Error codes: `1` worker-reported error, `2` HTTP/request failure, `-1` "unknown error … process got killed by the server" (OOM kill of the spawned child).

### 4.3 Where core calls the seam

- **REMOTE node execution**: `FlowNode._do_execute_remote` (flow_node.py:1318–1404). Sinks short-circuit: `node_type in ("output", "api_response", "flow_output", "run_flow")` stay in-core (1339–1344) — the output node's own `_func` already routes the actual write through `ExternalOutputWriter` when not local (add_output, flow_graph.py:3518–3543). Otherwise: build lazy result via `get_resulting_data()`, ship it with `ExternalDfFetcher(lf=…, file_ref=self.hash, wait_on_completion=False)`, `get_result()` returns the worker-cached LazyFrame; row count via a second `ExternalDfFetcher(operation_type="calculate_number_of_records")`; store the 100-row example generator. On error code `-1` (worker child OOM-killed), core **falls back to its own lazy frame** with a warning ("We cannot display example data…", 1385–1396).
- **LOCAL_WITH_SAMPLING**: `_do_execute_local_with_sampling` (1278) — narrow transforms compute lazily in core; only `ExternalSampler` (store_sample of 100 rows) goes to the worker for the preview.
- **ExternalFetcher pattern** (source I/O — "worker does all fetching, never core"): `ExternalDatabaseFetcher`, `ExternalKafkaFetcher`, `ExternalGoogleAnalyticsFetcher`, `ExternalRestApiFetcher`, and writers `ExternalDatabaseWriter`, `ExternalCloudWriter`, `ExternalOutputWriter` (subprocess_operations.py:1161–1260). All follow: `trigger_*` POST of a settings/credentials payload (secrets remain `$ffsec$…` encrypted; worker re-derives keys) → `Status.background_task_id` → poll. In graph `_func`s, the node stores the fetcher on `node._fetch_cached_df` so `cancel()` reaches it (e.g. add_database_reader flow_graph.py:4021–4025). ML: `MLTrainFetcher`/`MLApplyFetcher` (1074+) return metadata dicts, not frames.
- Each such node ALSO has an in-core `execution_location == "local"` branch (e.g. database reader builds `SqlSource` with decrypted URI locally, 3987–4008) — used by CLI/`--run-flow`/package mode where no worker exists. (The memory note "no in-core local fetch branch" refers to not fetching when remote; the local branch exists for local mode.)
- Credential resolution for DB nodes is **lazy + memoized + locked** (`_get_creds` with `threading.Lock`, 3961–3974) because the schema callback runs on a background thread while `_func` runs on the execution thread.

### 4.4 The "core never collects" invariant — enforcement status

- **It is a convention, not a mechanically enforced rule.** No lint/test forbids `.collect()` in core. What exists:
  - Bounded/preview collects live inside `FlowDataEngine` (`collect(n)` → `head(n).collect(engine="streaming")`, schema via `collect_schema()`, `pl.len()` counts — flow_data_engine.py:953–972, 907, 2229).
  - Comment in `_do_execute_remote`: "Use 'is not None' instead of truthiness … which calls .collect() on the LazyFrame" (flow_node.py:1348–1350) — `FlowDataEngine.__len__` triggers a count.
  - `NodeTemplate.laziness` metadata + `FlowNode.check_upstream_laziness` (flow_node.py:591–631) + `FlowGraph.check_flow_laziness` (4832, exposed at `GET /editor/laziness_check`) report eager/conditional nodes — but only for the catalog **virtual table** optimization path, not as a general gate.
  - Full-frame collects that DO exist in core (accepted exceptions): `_write_catalog_delta_local` collects to write Delta (flow_graph.py:602); explore-data/graphic-walker path collects (2155); `add_datasource` collects initial data for brand-new datasource nodes (4762); `FlowDataEngine.to_arrow()/to_pylist()/to_dict()` (used for previews/manual input).

## 5. Settings/schemas contract

- `NodeBase` (input_schema.py:428): `flow_id`, `node_id`, `cache_results=False`, `pos_x/pos_y`, `group_id` (visual only), `is_setup=True`, `description`, `node_reference` (validated: lowercase identifier, no spaces; used for codegen and kernel input naming), `user_id`, `is_flow_output`, `is_user_defined`, `output_field_config: OutputFieldConfig|None`. `NodeSingleInput.depending_on_id=-1`; `NodeMultiInput.depending_on_ids=[]`.
- `OutputFieldConfig` (96–107): declarative output-schema enforcement (`add_missing` / `add_missing_keep_extra` / `raise_on_missing` / `select_only` + type validation), applied post-`_func` by `apply_output_field_config` (flow_node.py:1040–1048) and folded into schema callbacks.
- Serialization model (schemas.py): runtime `FlowSettings` ⊃ `FlowGraphConfig`; on-disk YAML/JSON = `FlowfileData{flowfile_version, flowfile_id, flowfile_name, flowfile_settings, nodes:[FlowfileNode], groups}` — `FlowfileNode.setting_input` serialized via `to_yaml_dict()` if present else `model_dump(exclude=_setting_input_exclude)` (excludes node_id/pos/user_id/etc., schemas.py:297–323). Note `FlowfileSettings` (240–259) excludes runtime state (is_running/is_canceled/modified_on).
- Save formats: `.yaml`/`.yml` (default), `.json`; `.flowfile` pickle raises DeprecationWarning (save_flow, flow_graph.py:5529–5572). Custom YAML list representer keeps short scalar lists inline (163–170).
- Loading: `manage/io_flowfile.py::open_flow` (287–392) — two-pass: add all `NodePromise`s in dependency order (`determine_insertion_order`), then re-dispatch each stored `setting_input` through `getattr(new_flow, "add_" + node_info.type)` (line 338), then reconstruct edges (left/right/main inference from stored ids + `output_handles` positional list, "output-0" default for legacy files), keyed edges via `restore_dynamic_input_connections`, then `mark_as_saved()`. Flow display name is forced to the file stem (306–307).

## 6. main.py wiring (flowfile_core/main.py)

- Module import side effects: `storage.cleanup_directories()` (line 51); `FLOWFILE_MODE` defaulted to `electron` (53–54). NB: importing `flowfile_core/__init__` runs `validate_setup()` + `init_db()` (Alembic migration!) — set `FLOWFILE_SKIP_STARTUP_MIGRATION=1` for diagnostics (package CLAUDE.md + memory).
- Lifespan `shutdown_handler` (60–95): starts embedded scheduler only when `FLOWFILE_SCHEDULER_ENABLED` ∈ {true,1,yes}; on shutdown stops scheduler, all kernel containers, local LLM, clears flow logs.
- CORS (126–151): explicit dev origins + regex `^(tauri|http|https)://(tauri\.localhost|localhost(:\d+)?)$` for the Tauri shell.
- Routers (153–180): public, editor `router`, catalog, flow_api data(API-key)/management(JWT), api_consumers, artifacts, ml, logs, auth, user-groups+shares (404 in electron), secrets, project, cloud_connections, ga_connections, kafka, user_defined_components, kernel, lsp, file_manager, ai (`/ai`), ai_admin+lsp_admin on `/system` (deliberately NOT under the gate they flip).
- `POST /shutdown` sets `server.should_exit` after response (Tauri graceful-shutdown ladder step 1).
- `__main__` `--run-flow <path> --run-id <id>` CLI (262–400): forces `OFFLOAD_TO_WORKER=False`, `execution_location="local"`, **deletes explore_data nodes** (UI-only, need worker), resolves run user from the run record, reports completion via `shared.run_completion`.

## 7. Known weak points / hazards (for a skills library)

1. **God file**: `flow_graph.py` (5977 lines) holds the DAG engine + ~46 add_* methods + catalog Delta write helpers + ML train/apply plumbing + kernel execution + YAML serialization + groups + layout + history + codegen entry. `flow_node.py` (1930) and `flow_data_engine.py` (2842) are second-tier. Navigating by `grep -n "def add_"` is the practical index.
2. **Dual state**: legacy `node_stats: NodeStepStats` vs new `_execution_state: NodeExecutionState`, synced one-way by `_sync_state_to_legacy` (executor.py:351). `needs_run` (legacy) is still used by schema prediction; `_decide_execution` (new) governs actual runs. Writing to only one is a classic bug source.
3. **Hash discipline**: the `_hash` save/restore in `_execute_single_node` (5124–5159) exists because parameter substitution mutates `setting_input` in place; touching that flow without preserving the hash re-introduces the "spurious reset loses example data" bug the comment documents.
4. **Threading**: stage-parallel execution on ThreadPoolExecutor; `FlowNode._execution_lock` (RLock) serializes `get_resulting_data` AND is acquired on *each input node* while gathering inputs (flow_node.py:1011–1025) — lock ordering is input-before-self; sibling nodes sharing an upstream serialize on it. Schema callbacks run on their own single-thread executors. `BaseFetcher` uses Lock+Condition+Event. Subflow guards are plain attributes because contextvars don't cross threads (flow_graph.py:1120).
5. **String-convention dispatch**: `add_<node_type>` + `Node<TypeNoUnderscores>` class-name matching; a typo produces 404/AttributeError at runtime, nothing at import time. Endpoint returns HTTP **419** on add-function failure (nonstandard).
6. **`add_node_connection` main-input replacement**: for templates with `input <= 2`, connecting a new main input *replaces* `main_inputs` rather than appending (flow_node.py:689–693) — appending happens only for `input > 2`/multi templates. Union (multi=True) relies on `main_inputs is None` check.
7. **Skip-list shallowness**: `determine_nodes_to_skip` only expands one level; correctness relies on the runtime failed/skip propagation in `_execute_stages`.
8. **`results_exists` swallows worker downtime** (returns False on connection error, logs) — Development-mode skip decisions silently degrade to re-runs; conversely a *stale worker cache under an unchanged hash* returns SKIP with old data (cache key includes settings+inputs+parent_uuid, so this only bites within one session after out-of-band data changes; `invalidate_cache`/cache_epoch is the escape hatch).
9. **In-memory only**: flows live in `flow_file_handler` (process memory). Backend restart / Save As invalidates frontend flow_ids — run route returns a targeted 404 "no longer in memory. Reload the flow" (routes.py:428–435).
10. **CORS-masked 500s**: deleted-connection guards exist specifically because an AttributeError 500 "also drops CORS headers and shows up as a CORS error in the browser" (flow_graph.py:5951–5953). Same failure mode applies to any unhandled route exception.
11. **`OFFLOAD_TO_WORKER` parsing**: only `"1"` is truthy (settings.py:22), unlike the AI flags which accept true/yes/on. Easy operator foot-gun.
12. **Port binding default `0.0.0.0`** for non-Windows worker URL (and server host) — anything on the network can reach a dev instance (**inferred** risk; not a code bug).

## 8. Incident stories embedded in code comments (symptom → cause → fix)

- **Spurious node reset after parameterized runs** → executor's `node.reset()` recomputed `_hash` from resolved `${param}` paths; after restore the hash mismatched → next settings write reset the node, losing example data → fix: save/restore `_hash` around execution (flow_graph.py:5124–5159). Status: fixed, load-bearing comment.
- **Poisoned schema cache after reset** → in-flight schema callback (executor shut down `wait=False`) committed its result after `reset()` cleared state → fix: generation counter in `SingleExecutionFuture`; stale generations skip writes (schema_callback.py:44–47, 121–144). Status: fixed.
- **Worker child OOM-killed mid-store** → status "Unknown Error", fetcher error_code −1 → core continues with its own lazy frame + warning "cannot display example data" instead of failing the node (flow_node.py:1383–1396). Status: designed degradation.
- **Upstream cache eviction mid-session** → downstream node fails "No such file or directory (os error …)" → executor re-runs all inputs with reset_cache then retries once (executor.py:373–391). Status: fixed via retry.
- **Stale connection delete → browser shows CORS error** → missing node ⇒ AttributeError ⇒ 500 without CORS headers → explicit 422 guards in `delete_connection` (flow_graph.py:5949–5955). Status: fixed.
- **Eager schema prefetch racing reset cascade** → downstream callbacks read upstream state during `graph.reset()` → only start nodes prefetch on reset (flow_node.py:1540–1549). Status: fixed by restriction.
- **Frontend flow_id drift after Save As/restart** → run 500'd → explicit 404 with reload guidance (routes.py:428–435). Status: fixed.
- **Filename-stem display names leaking into run history** (`9_house_price`) → `_resolve_run_identity` prefers the catalog registration name (routes.py:284–301). Status: fixed (this branch's topic: better naming for unnamed flows, commit fa23a297).

## 9. Verified commands

```bash
# line counts of the core files
wc -l flowfile_core/flowfile_core/flowfile/flow_graph.py \
      flowfile_core/flowfile_core/flowfile/flow_data_engine/flow_data_engine.py \
      flowfile_core/flowfile_core/schemas/input_schema.py \
      flowfile_core/flowfile_core/schemas/schemas.py flowfile_core/flowfile_core/main.py

# index the graph API (all node-add methods & execution entry points)
grep -n "def add_\|def run_graph\|def _execute" flowfile_core/flowfile_core/flowfile/flow_graph.py

# find the offload seam
grep -rn "FLOWFILE_OFFLOAD_TO_WORKER\|OFFLOAD_TO_WORKER" flowfile_core/flowfile_core --include="*.py" | grep -v __pycache__

# worker endpoints core talks to
grep -n "@router" flowfile_worker/flowfile_worker/routes.py

# node-type → settings-class registry
grep -n "NODE_TYPE_TO_SETTINGS_CLASS" -A 50 flowfile_core/flowfile_core/schemas/schemas.py | head -60

# where routes dispatch to graph methods
grep -n 'getattr(flow, "add_"' flowfile_core/flowfile_core/routes/routes.py
```

(Read-only; no server was started. Test suite touching this area: `flowfile_core/tests/flowfile/test_flowfile.py` — 113 test functions; `tests/conftest.py` provides a `flowfile_worker` session fixture that spawns `poetry run flowfile_worker` for real-integration tests.)
