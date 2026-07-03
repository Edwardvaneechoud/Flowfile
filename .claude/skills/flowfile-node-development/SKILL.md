---
name: flowfile-node-development
description: End-to-end runbook for adding or modifying a Flowfile node type across all four layers (flowfile_core settings/graph/template, flowfile_frontend UI registry, flowfile_frame Python API, flowfile_wasm parity) including the string-convention dispatch trap, the worker-does-all-fetching rule for network sources, and the stub/parity gates each layer enforces. Use when a task says "add a node", "new node type", "add a transform/reader/writer node", "wire up a node in the UI", "expose this as a FlowFrame method", "add this node to WASM", or when debugging a 404/AttributeError/HTTP 419 from `/update_settings/`, a settings drawer that silently fails to load, a `make check_stubs` CI failure, or a WASM node missing from the palette.
---

# Flowfile node development

## When NOT to use this skill

- Editing `flow_graph.py` execution/caching/scheduling internals *without* adding a node type → `flowfile-architecture-contract` (DAG engine, executor decision table, hash/cache invariants).
- Debugging a failure whose cause is unclear (not "which file do I edit") → `flowfile-debugging-playbook` first, then come back here once you know it's a node-registration problem.
- General Vue/store/Tauri conventions unrelated to node settings components → `flowfile-frontend-conventions`.
- Deep dive on `flowfile_frame`'s expression system (`Expr`, `_repr_str`, `_ff_repr`, selectors) beyond "should this be a native node or polars-code" → `flowfile-frame-and-codegen`.
- Systematic flow-vs-generated-code parity bugs (Export to Python) → `flowfile-codegen-parity-campaign`.
- Running tests / CI markers in general → `flowfile-testing-and-validation`.
- Env vars / feature flags (`FLOWFILE_OFFLOAD_TO_WORKER`, `FEATURE_FLAG_AI`, etc.) → `flowfile-config-and-flags`.

All facts below verified in-repo as of **2026-07-03 (v0.12.7)**, branch `feature/claude-skills`. Re-verification commands are in the final section — run them before trusting a line number in a stale checkout.

---

## 0. The mental model — four layers, one node

A "node" is not one artifact. Adding `my_node` (fictional example used throughout) touches up to four independent registries that only agree by **naming convention**, not by a shared enum or generated code:

```
flowfile_core        settings schema (input_schema.py) + add_<type> method on FlowGraph
                      + NodeTemplate (configs/node_store/nodes.py) + NODE_TYPE_TO_SETTINGS_CLASS
                            │  served over HTTP as GET /node_list + POST /update_settings/?node_type=...
                            ▼
flowfile_frontend     settings Vue component at a convention path, resolved by import.meta.glob
                      (main desktop/web app only — NOT flowfile_wasm)
flowfile_frame        optional: a FlowFrame/Expr method that emits either a native add_<type> node
                      or a generated polars-code string
flowfile_wasm         optional, only if the node must run standalone-in-browser: Python engine fn +
                      TS settings component + palette entry (own registries, no HTTP, no flowfile_core)
```

Nothing here is enforced by a compiler or a single source of truth. **Grep is the index.** The canonical command to find every place a node type is wired:

```bash
grep -n "def add_" flowfile_core/flowfile_core/flowfile/flow_graph.py           # core graph methods
grep -n '"<node_type>"' flowfile_core/flowfile_core/configs/node_store/nodes.py  # template
grep -n "<node_type>" flowfile_core/flowfile_core/schemas/schemas.py             # settings-class registry
find flowfile_frontend/src/renderer/app/components/nodes/node-types/elements -iname '*<TitleCase>*'
```

---

## 1. Layer 1 — flowfile_core (the only layer that is ever mandatory)

### 1.1 The string-convention dispatch trap (read this before writing any code)

Adding a node from the UI/API goes through `POST /update_settings/?node_type=<node_type>` → `add_generic_settings` (`flowfile_core/flowfile_core/routes/routes.py:1146`). It resolves **everything by string manipulation, at request time, with no import-time check**:

```python
# routes.py:1158-1189 (verified)
input_data["user_id"] = current_user.id
node_type = camel_case_to_snake_case(node_type)                 # e.g. "fuzzyMatch" -> "fuzzy_match"
add_func = getattr(flow, "add_" + node_type)                    # AttributeError if no add_fuzzy_match method
setting_name_ref = "node" + node_type.replace("_", "")          # "fuzzy_match" -> "nodefuzzymatch"
...
ref = get_node_model(setting_name_ref)                          # scans input_schema.__dict__ for a
                                                                  # class whose *lowercased* name matches
...
try:
    add_func(parsed_input)
except Exception as e:
    raise HTTPException(419, str(f"error: {e}")) from e         # non-standard status code, verified
```

`get_node_model` (`routes.py:111-118`) literally does `for ref_name, ref in inspect.getmodule(input_schema).__dict__.items(): if ref_name.lower() == setting_name_ref: return ref`.

**Consequences you must design around:**
- Your settings class name MUST be exactly `Node` + the node_type with underscores stripped, case-insensitively: `text_to_rows` → class `NodeTextToRows` (lowercases to `nodetexttorows`, matches `"node" + "texttorows"`). Get one letter wrong and `get_node_model` silently returns `None` → the route responds `404 "could not find the interface"`.
- Your graph method MUST be named `add_<node_type>` exactly (snake_case, matching what the frontend sends as `node_type`). Get it wrong and you get `AttributeError` inside `add_generic_settings`, which is **not caught** — it propagates as an unhandled 500, or if it happens after `add_func` is fetched but the call itself raises inside your method, you get the **419** above.
- None of this is checked until the endpoint is actually hit. A typo ships clean through code review, ruff, and mypy... because none of them look inside a runtime `getattr`/`__dict__` scan.

### 1.2 Add-a-node checklist (core)

1. **Settings model** — `flowfile_core/flowfile_core/schemas/input_schema.py`. Subclass one of:
   - `NodeSingleInput` (`input_schema.py:473`) — adds `depending_on_id: int | None = -1`. Use for one-input transforms (filter, sort, select, formula...).
   - `NodeMultiInput` (`input_schema.py:479`) — adds `depending_on_ids: list[int]`. Use for join/union/concat-style nodes.
   - `NodeBase` (`input_schema.py:428`) directly — sources with no input (read, manual_input, database_reader...). `NodeBase` already carries `flow_id`, `node_id`, `cache_results`, `pos_x/pos_y`, `description`, `node_reference`, `user_id`, `is_setup`, `output_field_config`.
   Implement `get_default_description()`; add `to_yaml_dict()` only if you need curated YAML on save.
2. **Register** in `NODE_TYPE_TO_SETTINGS_CLASS` — `flowfile_core/flowfile_core/schemas/schemas.py:27`. It's a flat dict, `"<node_type>": input_schema.Node<CamelCase>`. Skip this and **saved flows fail to reload** with "Unknown node type" (used by `NodeInformation.validate_setting_input` when deserializing).
3. **Template** in `flowfile_core/flowfile_core/configs/node_store/nodes.py::get_all_standard_nodes()` — a `NodeTemplate(...)` entry in the big list. Required fields: `item` (== node_type, snake_case), `input`/`output` counts, `node_type` (`"input"|"process"|"output"`), `transform_type` (`"narrow"` → eligible for local-with-sampling execution; `"wide"` → forced remote/worker under `optimize_for_downstream`), `laziness` (`"lazy"|"eager"|"conditional"`), `node_group` (palette section), `image` (an `<name>.svg` filename), `drawer_title`/`drawer_intro`, `tags` (palette search). Add an entry to the `node_defaults` dict (`nodes.py:755`) only if the node should get pre-filled default settings on drop (checked via `check_if_has_default_setting` at add time, `routes.py:594`).
4. **`add_<node_type>` method on `FlowGraph`** (`flowfile_core/flowfile_core/flowfile/flow_graph.py`), decorated `@with_history_capture(HistoryActionType.UPDATE_SETTINGS)` (so "Add node" collapses to one undo entry), calling the one true factory `self.add_node_step(node_id=..., function=_func, node_type="<node_type>", setting_input=..., input_node_ids=[...], schema_callback=...)` (`add_node_step` itself lives at `flow_graph.py:3378`). `_func` receives one `FlowDataEngine` per input slot and returns a `FlowDataEngine` (or a `NamedOutputs` for multi-output nodes like `filter_split`).
   - **`_func` must tolerate empty schema-only frames.** Schema prediction runs your `_func` against `FlowDataEngine.create_from_schema(...)` placeholder inputs so the UI can show output columns without executing anything (`flow_node.py:1060`, `_predicted_data_getter` — surfaced via `get_predicted_resulting_data`). Don't assume real row data inside `_func` unless you gate on it.
   - Source-style nodes that need custom lifecycle (read/database_reader/manual_input) skip `add_node_step` and build/patch the `FlowNode` directly, registering via `self.add_node_to_starting_list(node)` + `self._node_db[...]` — see `add_read` (`flow_graph.py:4646`) as the template if you're building a source with special schema-callback logic.
5. **If it's a network-source node**, read §1.3 below before writing `_func` — do not fetch from core.
6. Frontend: node config component + path convention — §2.
7. **Parity surfaces that silently lag** (each has its own registry, none cross-check at build time): the code generator (`flowfile_core/flowfile_core/flowfile/code_generator/`, may need a new handler for Export-to-Python), the `flowfile_frame` API method (§3, optional), the WASM node set (§4, optional — only the 23-node runnable palette + `available:false` teaser entries), the AI tools' node catalog (context for the AI agents to know the node exists — search `flowfile_core/flowfile_core/ai/` for existing node references if the node should be AI-discoverable).
8. **Tests** — `flowfile_core/tests/flowfile/` (e.g. `test_basic_filter.py`, `test_filter_expressions.py` are the pattern for a simple transform node). Assert: settings validate, the graph method adds a node with the right template counts, `.collect()`/execution produces correct data, and (if the node has a native vs polars-code split anywhere downstream) round-trip through save/reload.

### 1.3 Worked example — simple, single-input: `filter`

Read this to see the whole chain end to end for the simplest realistic case.

1. Settings: `input_schema.NodeFilter(NodeSingleInput)` (`input_schema.py:542`) → holds `filter_input: transform_schema.FilterInput` plus `split_mode: bool` for the pass/fail two-output variant.
2. Registered: `NODE_TYPE_TO_SETTINGS_CLASS["filter"] = input_schema.NodeFilter` (`schemas.py:27` block).
3. Template: `NodeTemplate(name="Filter data", item="filter", input=1, output=1, transform_type="narrow", node_type="process", laziness="lazy", image="filter.svg", ...)` (`configs/node_store/nodes.py`, in the list built by `get_all_standard_nodes`).
4. Graph method `add_filter` (`flow_graph.py:2224`): `_func(fl: FlowDataEngine)` builds a polars-expr-transformer expression from either the advanced filter string or a `BasicFilter`, **writes the normalized expression back onto the settings** (`filter_settings.filter_input.advanced_filter = expression` — the node self-normalizes its own settings on every run), then returns `fl.filter_split(expression)` in split mode or `fl.do_filter(expression)` otherwise. Ends with `self.add_node_step(..., node_type="filter", renew_schema=False, input_node_ids=[filter_settings.depending_on_id])`.
5. Runtime dispatch: `POST /update_settings/?node_type=filter` → generic `add_generic_settings` (§1.1) — no filter-specific route exists.

### 1.4 Worked example — complex, source + two-input: `read` and `join`

- **`read`** (source, `add_read` at `flow_graph.py:4646`): settings `NodeRead(NodeBase)` carries a discriminated union `InputTableSettings` (csv/json/parquet/excel/ipc/ndjson/avro). `_func()` picks the execution path at run time: local/parquet/ipc/ndjson/utf-8-CSV stay in-core as a lazy scan (`FlowDataEngine.create_from_path`); exotic encodings and Excel go to the **worker** to convert to parquet first (`FlowDataEngine.create_from_path_worker`, an `ExternalCreateFetcher`). The schema callback is chosen by file type (declared `fields` win; else read-the-file per format; Excel uses a bounded 100-row sample) and is set on **both** `node.schema_callback` and `node.user_provided_schema_callback` so it survives a `reset()`. `read` is also the only node type with source-file change detection — the executor compares file-stat snapshots and reruns automatically if the underlying file changed on disk, and `reset()` deliberately preserves that tracking state.
- **`join`** (two-input, `add_join` at `flow_graph.py:2537`): settings `NodeJoin(NodeMultiInput)` → `join_input: transform_schema.JoinInput`. `_func(main, right)` deep-copies the join input, recomputes per-field `is_available` before calling `main.join(...)`. Uses an **explicit schema callback** (`calculate_join_schema` from `flowfile/schema_callbacks.py`) instead of running `_func` on placeholder frames — this predicts the output schema without executing the join. Template: `input=2, transform_type="wide"` (wide ⇒ eligible to be forced onto the worker when optimizing downstream execution).

### 1.5 Network-source nodes: the worker does ALL external fetching

**Rule, verified against three current implementations:** a node whose `_func` talks to the outside world (HTTP API, OAuth provider, Kafka broker, a database over the network) must not perform that I/O inside `flowfile_core`. Build a worker-settings payload and dispatch it through an `External*Fetcher` (`flowfile_core/flowfile_core/flowfile/flow_data_engine/subprocess_operations/subprocess_operations.py`) — the fetcher POSTs the settings to `flowfile_worker`, which does the actual network round-trip in a spawned subprocess and hands back a cached, lazy result.

**Canonical zero-local-branch template — `add_google_analytics_reader`** (`flow_graph.py:4254`): `_func()` unconditionally builds `ExternalGoogleAnalyticsFetcher(_build_worker_settings(), wait_on_completion=False)` and calls `.get_result()`. There is **no `execution_location == "local"` branch at all** — GA4 always round-trips through the worker, full stop. The schema callback is derived purely from the selected metrics/dimensions (`derive_schema`, pure Python, no network call), so schema prediction never has to touch the worker either.

**Default to the GA4 pattern for new network-source nodes.** It is the smallest, least error-prone shape: one fetch path, one place secrets get decrypted (worker-side, just-in-time), nothing to keep in sync.

**The nuance (verified, corrects a stricter-sounding rule of thumb):** two existing readers — `add_database_reader` (`flow_graph.py:3950`) and `add_rest_api_reader` (`flow_graph.py:4399`) — **do** carry an `execution_location == "local"` branch that fetches in-process instead of going through the worker. This exists specifically to support `--run-flow`/CLI execution and other package/local modes where **no worker process exists at all** (core forces `execution_location="local"` and `OFFLOAD_TO_WORKER=False` in that mode). The REST API reader's local branch was added later than its worker-only fetch path and is explicitly modeled on the database reader (comment in source: `# No worker service in local runs — fetch in-process (cf. add_database_reader).`) — it is a deliberate, kept pattern, not a one-off legacy wart to avoid copying.

**So the actual rule is:**
- Never fetch externally when `execution_location != "local"` — that's the worker's job, no exceptions.
- Only add a gated `if self.execution_location == "local":` in-process fetch branch if your node has a concrete need to work without a worker (CLI/`--run-flow`, scheduled local execution). If you add one, mirror `add_rest_api_reader`'s shape exactly: decrypt/resolve credentials the same way for both branches, and call the **same underlying fetch function** the worker would use (e.g. `shared/rest_api/fetch.py::fetch_rest_api`) so the two paths can't silently diverge.
- If you have no such requirement, don't add the branch — follow GA4, not the two exceptions.

### 1.6 Traps specific to this layer

- **HTTP 419** (nonstandard, not a typo) is the status code `add_generic_settings` returns when your `add_<type>` method raises for any reason (`routes.py:1189`). If a frontend network tab shows a bare "419" with no obvious cause, the bug is inside your node's `_func`/settings validation, not the transport.
- **`add_node_step` deletes-then-recreates** a node whose `node_type` changed but keeps it (via `update_node`) if the type is unchanged — don't assume "the node with this id" is stable across settings edits that change the node's own type.
- Nodes with `input > 2` or `multi=True` in their template use append-connection semantics; templates with `input <= 2` **replace** the main input on a new connection instead of appending (`flow_node.py:689-693`) — relevant if your node ever needs more than two ordered inputs.

---

## 2. Layer 2 — flowfile_frontend (main desktop/web UI)

**Not required** if the node is core-only (e.g. an internal/AI-orchestrated node with no user-facing settings). Required for anything a human drags onto the canvas.

### 2.1 The convention (no registry file, no build-time check)

There is **no static list of node components**. Two independent `import.meta.glob` calls, which must be kept pointing at the same tree, resolve a settings component purely from the node's `item` string:

1. `app/composables/useDragAndDrop.ts:88` — `import.meta.glob("../components/nodes/node-types/elements/**/*.vue")`. `getComponent()` (`useDragAndDrop.ts:158`) builds the path `elements/${camelCase(item)}/${TitleCase(item)}.vue` and loads it for the VueFlow node itself.
2. `app/components/nodes/GenericNode.vue:25` — `import.meta.glob("./node-types/elements/**/*.vue")`. `loadDrawerComponent()` (`GenericNode.vue:47`) applies the same convention for the settings-drawer component, via `defineAsyncComponent` with a 3s timeout and up to 3 retries.

`TitleCase` = each `_`-separated word of the node_type capitalized and joined: `text_to_rows` → dir `textToRows`, file `TextToRows.vue`. So `text_to_rows` resolves to exactly:

```
src/renderer/app/components/nodes/node-types/elements/textToRows/TextToRows.vue
```

**If you get the path wrong, there is no build error.** The glob still succeeds (it just doesn't find your file); `loadDrawerComponent` logs a `console.error` and the settings drawer silently never opens for that node. This is a pure-runtime failure mode — verify by actually opening the node's settings drawer in a running app, not just by compiling.

### 2.2 Add-a-node-UI checklist

1. Backend template already done (§1.2 step 3) — `item`, `image`, counts must already exist in `/node_list`.
2. Icon: add `<image>.svg` to `src/renderer/app/features/designer/assets/icons/`.
3. Settings component at exactly `src/renderer/app/components/nodes/node-types/elements/<camelCase(item)>/<TitleCase(item)>.vue`. Copy the `filter/Filter.vue` pattern:
   - Wrap the form in `<generic-node-settings v-model="nodeXxx" @update:model-value="handleGenericSettingsUpdate" @request-save="saveSettings">`.
   - Pull `const { saveSettings, pushNodeData, handleGenericSettingsUpdate } = useNodeSettings({ nodeRef, onBeforeSave })` from `app/composables/useNodeSettings.ts` — it auto-sets `is_setup = true` and POSTs through `nodeStore.updateSettings`.
   - Implement `loadNodeData(nodeId)` (fetch via `nodeStore.getNodeData(nodeId, false)`, hydrate `setting_input`) and end with `defineExpose({ loadNodeData, pushNodeData, saveSettings })` — `NodeSettingsDrawer.vue` requires both hooks to exist on every drawer-entry component ("Universal Apply" contract).
4. No registration step. Both globs auto-discover the new file by path.
5. Set `prod_ready: false` on the backend `NodeTemplate` while iterating — keeps it out of the production palette but it still loads fine in saved flows (useful for landing a half-finished node without exposing it).

### 2.3 Verify (don't just compile)

Drag the node from the palette, open its settings drawer, and confirm it loads — `vue-tsc` and `npm run build:web` will not catch a wrong component path. If you don't have a running desktop/web session available, at minimum grep for the exact expected path and confirm the file exists there with that exact casing (case-sensitive on Linux CI even if your local macOS filesystem is case-insensitive and would mask the bug).

---

## 3. Layer 3 — flowfile_frame (Python programmatic API)

**Not required** for most nodes — many core nodes (kernel/python_script, kafka_source, most ML nodes) have no `flowfile_frame` method and that's fine. Add one only when the node should be reachable from `import flowfile_frame as ff` code.

### 3.1 Decide the emission path first

`flowfile_frame` builds the *same* in-process `FlowGraph` the UI edits — every `FlowFrame` method call is really "mutate a graph, then re-read the resulting LazyFrame." Two paths exist for any new method:

1. **Native node** (preferred when a core node type already exists or you just added one in §1): build the Pydantic `input_schema.NodeXxx(...)`, call `self.flow_graph.add_xxx(settings)`, return `self._create_child_frame(new_node_id)`. This renders as a first-class editable node if the user opens the graph in the Designer.
2. **Polars-code node** (`FlowFrame._add_polars_code`, `flowfile_frame/flowfile_frame/flow_frame.py:588`): for cases with no dedicated node, or too many parameter combinations to model — generates a **Polars source string** (`output_df = input_df.<method>(...)`) that core re-executes in its sandboxed `PolarsCodeExecutor` (`flowfile_core/flowfile_core/flowfile/flow_data_engine/polars_code_parser.py`) at run time. The generated string is **load-bearing** — a wrong string breaks graph execution silently, only surfacing when the graph actually runs. Contract: no `import` statements, no `exec`/`eval`/`getattr`/`open`, must assign `output_df` (or be a single-line `pl.`/`col(`/`input_df`/`expr(`-prefixed expression), bare Polars dtype names resolve without a `pl.` prefix, 1 input → `input_df`, ≥2 inputs → `input_df_1..N`.

### 3.2 The method skeleton (every graph-building method follows this shape)

```python
new_node_id = generate_node_id()                 # utils.py:109 — process-global counter, not per-graph
# decide native vs polars-code (mirror an existing use_polars_code_path / can_use_native flag)
settings = input_schema.NodeXxx(
    flow_id=self.flow_graph.flow_id, node_id=new_node_id,
    depending_on_id=self.node_id, description=description, is_setup=True,
)
self.flow_graph.add_xxx(settings)
return self._create_child_frame(new_node_id)     # flow_frame.py:399 — wires the connection + re-reads data
```
Multi-input methods (`join`, `concat`, `fuzzy_join`) wire `"main"`/`"right"` connections manually instead of using `_create_child_frame`.

**Node ids are process-global, not per-graph** — `generate_node_id()` increments a module-level counter shared across every graph in the process, so ids can skip within a single graph (this is expected, not a bug). Every graph method should accept `description: str | None = None` — it becomes the frontend node label.

### 3.3 The stub-generation gate — do this or CI fails

Because `FlowFrame` and `Expr` inject most of their methods at runtime (`add_expr_methods`, `@add_lazyframe_methods`), static type checkers see nothing without committed `.pyi` stub files. Any change to the public surface of `flow_frame.py` or `expr.py` requires regenerating stubs:

```bash
make stubs          # regenerates flowfile_frame/flowfile_frame/*.pyi (expr.pyi, flow_frame.pyi, one per module)
make check_stubs    # CI drift gate: reruns `stubs`, then `git diff --exit-code` on the .pyi files
```

**Both commands import `flowfile_frame`, which imports `flowfile_core`, which runs Alembic migrations against the live catalog DB on import.** Isolate before running either locally:

```bash
FLOWFILE_DB_PATH=/tmp/stub_scratch.db make stubs
```

`make check_stubs` is wired as the `check-stubs` job in `.github/workflows/test.yaml`, triggered on any `flowfile_frame/**` change — a public-surface change without regenerated stubs **fails CI**, not just a lint warning. If you changed the passthrough-methods set (methods that delegate straight to the underlying LazyFrame with no node added, e.g. `collect`, `schema`, `describe`), update the duplicated copy of that set inside `flow_frame_stub_generator.py` too — it's hand-duplicated from `lazy_methods.py` and won't auto-sync.

### 3.4 Extend-the-API checklist

1. Pick native-node vs polars-code (§3.1); prefer native when a core node type exists.
2. Make the generated string sandbox-executable if using polars-code (§3.1 contract).
3. Follow the method skeleton (§3.2); always accept `description`.
4. Keep `LazyFrame = DataFrame = FlowFrame` aliases and the Polars dtype re-exports in `__init__.py` intact — generated code depends on them existing.
5. `make stubs` with an isolated `FLOWFILE_DB_PATH`, stage the `.pyi` diff (do not commit it yourself — hand off per `flowfile-change-control`'s no-agent-commit policy).
6. Test both the **result** (`.collect()` against a plain-Polars reference) and the **graph shape** (assert node type / generated code string), mirroring `flowfile_frame/tests/test_ff_repr.py`'s `_is_formula_node`/`_is_polars_code_node` helper pattern.
7. `FLOWFILE_DB_PATH=/tmp/frame_test.db poetry run pytest flowfile_frame/tests --disable-warnings`.

---

## 4. Layer 4 — flowfile_wasm (browser-only Pyodide build)

**Only needed if the node must run standalone in the browser with no `flowfile_core` at all.** WASM is a fully separate, from-scratch implementation — it does not import or call the core node code; every node it supports has its own Python engine function and its own TS settings component. Check first whether the node even belongs there: WASM currently ships a **23-node runnable palette** (read, manual_input, external_data, read_from_catalog, filter, select, sort, group_by, unique, formula, record_id, dynamic_rename, sample/head, pivot, unpivot, join, cross_join, union, explore_data, output, external_output, write_to_catalog, polars_code) plus locked/greyed-out teaser entries (database_reader, cloud_storage_reader, rest_api_reader, kafka_source, google_analytics_reader, window_functions, sql_query, python_script, fuzzy_match, graph_solver, train_model, apply_model, evaluate_model, database_writer, cloud_storage_writer) that link out to the full-app docs instead of running. Anything needing a backend connection, secrets, or a Docker kernel is a **locked placeholder in WASM by design** — don't try to make it runnable there.

### 4.1 Add-a-node checklist (WASM)

Touch, in order:
1. `src/pyodide/engine/nodes_*.py` — add an `execute_<type>(...)` function in the right module by responsibility (`nodes_io.py`, `nodes_transform.py`, `nodes_combine.py`, `nodes_aggregate.py`, `nodes_formula.py`, `nodes_explore.py`, `nodes_polars_code.py`); re-export it via `__init__.py`, which has **two** places that must both list the new name: the `from .module import (...)` block per submodule, and an explicit `__all__ = [...]` list further down (grouped by submodule with comments) — the browser runs `from engine import *`, which honors `__all__`, so a name missing from `__all__` silently never reaches the JS bridge even though the plain Python import works fine under pytest.
2. `src/stores/flow-store.ts` — add a `case` in `executeNode`'s dispatch to call the bridge string `execute_<type>(...)`.
3. `src/components/Canvas.vue` — add the node to `nodeCategories` (palette entry) and to the `getSettingsComponent(type)` map (explicit map here, unlike the main app's convention-glob — every WASM node is a named case).
4. `src/components/nodes/<X>Settings.vue` — new settings panel (flat file, not a per-node directory like the main app).
5. `src/types/index.ts` — add the key to `NODE_TYPES`.
6. `src/config/nodeDescriptions.ts` — add the palette description entry.
7. Usually also: `src/composables/useCodeGeneration.ts` (if the node needs its own Export-to-Python string) and `src/stores/schema-inference.ts` (pure-TS schema prediction; return `null` here for anything needing real execution to know its output schema, e.g. `polars_code`/`formula`/`pivot`/joins-without-a-right-schema — that forces the UI to fall back to lazy execution for schema).

### 4.2 The hard constraint: Rust-compiled Polars plugin packages have no WASM wheels

Any dependency that ships as a **compiled Rust extension for `polars`** (the class of package `polars-expr-transformer`'s optional heavy features, `polars_ds`, and similar plugin-style crates belong to) generally has **no wasm32 wheel**, because it's compiled per-target and nobody publishes a browser-Pyodide build. If a node's engine function needs such a dependency:

- Keep the import **lazy and optional** inside the function that needs it, gated so the rest of the module still imports cleanly without it (mirrors how `flowfile_frame` treats `polars_ds` as lazy/optional in the formula-upgrade path, and how the WASM formula node only pulls in the subset of `polars-expr-transformer` that has no `polars_ds` dependency).
- Never add an unconditional top-level `import` of such a package to a `nodes_*.py` module — that breaks `import.meta.glob`/Pyodide package loading for every node in the file, not just yours, because Pyodide's `micropip`/`loadPackage` has nothing to install.
- **CPython pytest cannot catch this class of bug.** `flowfile_wasm/tests/python/` runs under real CPython against a pinned Polars version — it will happily import a package that has a CPython wheel but no wasm32 wheel, and pass. The only real gate is the **pyodide-smoke job** (`tests/pyodide-smoke/smoke.cjs`, run in CI's `flowfile-wasm-build.yml` under the `pyodide-smoke` job): it installs actual Pyodide + the actual browser-target packages and replays the JS→Python bridge calls against them. If you add a new Python dependency to the engine, verify it under `pyodide-smoke` (or manually via `npm run dev` at `:5174` and executing the node in a real browser) before considering the change done — a green `npm run test:run` (happy-dom, CPython-backed pytest) is not sufficient evidence.

### 4.3 Other WASM-specific hard rules (verify still true before relying on them)

- **Execution is explicit-only.** Only Run flow / Run Now / Apply settings / Fetch data may trigger `execute_*`. Never wire a node's settings load, selection, or drag/drop to run data — regression-guarded by `tests/unit/no-auto-run.test.ts`.
- **No `.collect()` unless required.** Only `output`, `pivot`, `explore_data`, `polars_code` materialize; everything else should stay a lazy Polars plan until the user explicitly runs.
- Pyodide is pinned (verify current pin: `grep -n "pyodide.js" flowfile_wasm/src/stores/pyodide-store.ts`) as "the last Pyodide release with Polars support" — don't bump it casually.

---

## 5. Tests expected per layer

| Layer | Where | Command | Notes |
|---|---|---|---|
| core | `flowfile_core/tests/flowfile/` | `poetry run pytest flowfile_core/tests` | Follow `test_basic_filter.py` for a single-input transform pattern. |
| frontend | `flowfile_frontend/tests/` (Playwright), colocated `*.test.ts` (Vitest) | `npm run test:unit`, `npm run test:web` | No automated check exists for the glob-path convention itself — verify by driving the app (drag, open drawer). |
| flowfile_frame | `flowfile_frame/tests/` | `FLOWFILE_DB_PATH=<scratch> poetry run pytest flowfile_frame/tests --disable-warnings` | Assert both `.collect()` correctness and graph shape (node type / generated code), like `test_ff_repr.py`. |
| wasm | `flowfile_wasm/tests/{python,unit,integration}` + real-Pyodide smoke | `npm run test:run` (CPython-backed engine tests, happy-dom component tests) + pyodide-smoke job for anything touching Python deps | CPython pytest passing does **not** prove a wasm32-incompatible dependency is safe — see §4.2. |

---

## 6. Common failure signatures → cause (quick lookup)

| Symptom | Cause | Where to look |
|---|---|---|
| `POST /update_settings/` → HTTP 419 | Your `add_<type>` method raised an exception | `routes.py:1189`; add a try/except with a clearer message inside your `_func` or settings validator |
| `POST /update_settings/` → 404 "could not find the interface" | `get_node_model` couldn't match your settings class name | Class name must be exactly `"node" + node_type.replace("_","")` case-insensitively — §1.1 |
| `POST /update_settings/` → `AttributeError` / unhandled 500 | No `add_<node_type>` method on `FlowGraph`, or a typo in it | `getattr(flow, "add_" + node_type)` in `routes.py:1168` — §1.1 |
| Reloading a saved flow raises "Unknown node type" | Forgot `NODE_TYPE_TO_SETTINGS_CLASS` entry | `schemas.py:27` — §1.2 step 2 |
| Node settings drawer never opens, only a browser console error | Settings component path doesn't match `elements/<camelCase>/<TitleCase>.vue` | §2.1 — check both globs (`useDragAndDrop.ts`, `GenericNode.vue`) resolve the same path |
| `make check_stubs` fails in CI | Public `flow_frame.py`/`expr.py` surface changed without regenerating `.pyi` files | §3.3 |
| A WASM node works in `npm run test:run` but not in a real browser | A new Python dependency has no wasm32 wheel, imported eagerly | §4.2 — make the import lazy, verify under pyodide-smoke |
| Node ids "skip" inside one `flowfile_frame` graph | Not a bug — `generate_node_id()` is a process-global counter | §3.2 |

---

## 7. Provenance and maintenance

Facts below are dated **2026-07-03 (v0.12.7)**, branch `feature/claude-skills`. Re-run these before trusting a specific line number in a newer checkout — this codebase's node-related files churn fast (`flow_graph.py` alone is ~6k lines and gains add_* methods regularly).

```bash
# core: dispatch trap + settings-class lookup (routes.py) — confirm line numbers/behavior unchanged
grep -n "def get_node_model\|def add_generic_settings" -A 5 flowfile_core/flowfile_core/routes/routes.py

# core: settings-class registry (must include your new node_type)
grep -n "NODE_TYPE_TO_SETTINGS_CLASS" -A 5 flowfile_core/flowfile_core/schemas/schemas.py

# core: index every add_<type> method (grep is the practical index of the node API)
grep -n "def add_" flowfile_core/flowfile_core/flowfile/flow_graph.py

# core: confirm which network-source nodes have a local-execution branch (should stay GA4=none, DB/REST=yes)
grep -n 'execution_location == "local"' flowfile_core/flowfile_core/flowfile/flow_graph.py

# frontend: confirm both node-component globs still point at the same tree
grep -n "import.meta.glob" flowfile_frontend/src/renderer/app/composables/useDragAndDrop.ts \
  flowfile_frontend/src/renderer/app/components/nodes/GenericNode.vue

# flowfile_frame: stub pipeline still wired the same way
grep -n "^stubs:\|^check_stubs:" -A 8 Makefile

# flowfile_frame: isolate the DB before running stubs/tests (it imports flowfile_core -> runs Alembic)
FLOWFILE_DB_PATH=/tmp/ff_scratch.db poetry run pytest flowfile_frame/tests --disable-warnings -q

# wasm: current runnable palette + locked placeholders (counts/names drift — root CLAUDE.md is known-stale here)
grep -n "nodeCategories\s*=\|getSettingsComponent" flowfile_wasm/src/components/Canvas.vue
grep -n "pyodide.js\|loadPackage" flowfile_wasm/src/stores/pyodide-store.ts

# wasm: confirm the pyodide-smoke CI gate still exists (the only guard for wasm32-wheel gaps)
grep -n "pyodide-smoke" .github/workflows/flowfile-wasm-build.yml
```

Known drift already observed in this repo (don't propagate it further): the root `CLAUDE.md` describes WASM as "lightweight, 16 nodes" — stale; verified current runnable palette is 23 types. If you touch the WASM node count, update `flowfile_wasm/CLAUDE.md` and this file together rather than trusting either against the root doc.
