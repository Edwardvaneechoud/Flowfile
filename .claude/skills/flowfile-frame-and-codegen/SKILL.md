---
name: flowfile-frame-and-codegen
description: Deep dive into flowfile_frame — the Polars-LazyFrame-shaped Python API that builds an in-process flowfile_core FlowGraph as a side effect of every method call — covering the FlowFrame/Expr internals (_repr_str, _ff_repr, node-emission decision logic, method-injection decorators, lambda-source extraction), the generated-code contract with core's sandbox, exact lazy-vs-eager semantics, DB/cloud/catalog/Kafka/REST connectors, the native-vs-polars-code parity table, and the make-stubs/check-stubs .pyi pipeline. Use when writing or debugging flowfile_frame scripts, adding/changing a FlowFrame or Expr method, investigating why a `.collect()` triggered real I/O or a write happened before `.collect()`, deciding whether an operation will render as an editable node or an opaque code block in the Designer, chasing a `make check_stubs` CI failure, or asking "does read_api/read_database need flowfile_worker running".
---

# flowfile_frame and codegen

## When NOT to use this skill

- Adding a brand-new **core** node type (settings class, `NODE_TYPE_TO_SETTINGS_CLASS`, template, frontend component) — that four-layer registration process, including the `flowfile_frame` decision-to-add-a-method checklist at a shallower level, is `flowfile-node-development`. Come here only for the *internals* of how `flowfile_frame` itself is built (Expr repr machinery, injection decorators, lazy semantics, connectors).
- Systematically closing native-vs-polars-code parity gaps as a campaign/roadmap (which ops to convert next, tracking issues) — `flowfile-codegen-parity-campaign`. This skill documents *today's* parity table and the mechanics of extending it; it is not the battle plan.
- General core/worker/kernel system map, the worker-offload wire protocol, `$ffsec$` secrets format — `flowfile-architecture-contract`.
- Version bump rules, the stub gate as a *release* mechanic, Alembic migration discipline — `flowfile-change-control` (this skill covers the stub *generators*' mechanics; change-control covers when/why the gate blocks a release).
- Running the test suite in general, pytest markers, Docker fixture matrix — `flowfile-testing-and-validation`.
- CLI flow execution (`flowfile run`), on-disk storage layout, scheduler — `flowfile-run-and-operate`.
- Debugging a specific failure symptom you don't yet understand — `flowfile-debugging-playbook`.

All facts below verified in-repo as of **2026-07-03 (v0.12.7)**, branch `feature/claude-skills`. Re-verification commands are in the final section.

---

## 1. Mental model

`flowfile_frame` (`import flowfile_frame as ff`) gives a Polars-`LazyFrame`-shaped surface (`FlowFrame`, `Expr`/`Column`) where **every method call appends a node to an in-process `flowfile_core` `FlowGraph` as a side effect**, in addition to (usually) computing data.

It is **not a client of any backend HTTP API**. It imports `flowfile_core` directly — `flowfile_frame/flowfile_frame/flow_frame.py` imports `FlowDataEngine`, `FlowGraph`, `add_connection`, `combine_flow_graphs_with_mapping`, `FlowNode`, `input_schema`, `transform_schema` straight from `flowfile_core`. There is no port, no serialization boundary, no auth — it mutates Python objects in the calling process. The graph it builds is the **same kind of DAG** the Designer UI edits and core executes: it can be saved to a `.flowfile`, opened in the Designer (`open_graph_in_editor`, §7), or materialized with `.collect()`.

`utils.create_flow_graph()` seeds every new graph with `track_history=False` and, critically, forces `flow_graph.flow_settings.execution_location = "local"` — the comment in source is explicit: *"so that the run time does not attempt to use the flowfile_worker process."* This one setting is why several connector reads (§8) don't need a running worker, and why `write_*`/`pivot` behave eagerly (§6).

### The `FlowFrame` object

A `FlowFrame` is a thin handle around four fields, all set in `__new__` (`flow_frame.py:274`):

| field | meaning |
|---|---|
| `flow_graph` | the shared in-process `FlowGraph` |
| `node_id` | the node this frame currently points at |
| `parent_node_id` | upstream node id |
| `output_handle` | which source handle to read — `"output-0"` by default; `"output-1"` is e.g. the fail branch of `filter_split` |
| `data` | the node's resulting **Polars `LazyFrame`** — `flow_graph.get_node(node_id).get_resulting_data().data_frame` |

**All initialization happens in `__new__`; `__init__` is a deliberate no-op** (`flow_frame.py:370`, docstring: *"Python automatically calls `__init__` after `__new__`... this empty method catches that call and safely does nothing"*). `__new__` is a factory with three branches: `flow_graph`+`node_id` given → wrap an existing node; `flow_graph` given + non-`LazyFrame` data → `create_from_any_type` (new source node); otherwise → build a fresh graph, register a `NodeManualInput` (Python object) or a runtime dependency (`add_dependency_on_polars_lazy_frame`, for an existing `pl.LazyFrame`), then recurse into itself with the new graph/node.

---

## 2. The three node-emission paths

Every graph-mutating method picks one of three ways to represent the operation. Read this before writing or reviewing any `flowfile_frame` code.

1. **Native node.** When the arguments fit a dedicated core node schema, the method builds a Pydantic settings object (`flowfile_core.schemas.input_schema` / `transform_schema`) and calls the matching `flow_graph.add_*()` (`add_select`, `add_sort`, `add_join`, `add_group_by`, `add_formula`, `add_output`, …). Renders as a first-class **editable node** in the Designer.
2. **Polars-code node.** Complex cases call `FlowFrame._add_polars_code` (`flow_frame.py:588-684`), which creates a `NodePolarsCode` whose payload is a **generated Polars source string** (`transform_schema.PolarsCodeInput(polars_code=...)`). Core re-executes that string in a sandbox at run time (§6) — the string is load-bearing; a wrong one breaks graph execution silently, only surfacing when the graph actually runs.
3. **Serialization fallback** (inside `_add_polars_code`, edge case within path 2). If an expression can't be turned into code (`convertable_to_code=False` — e.g. a lambda whose source can't be recovered) or the generated code still contains a raw `<lambda> at 0x...>` repr, the operation runs directly on `self.data` (`getattr(self.data, method_name)(...)`) and the resulting `LazyFrame` is **base64-serialized into the node**. Source carries an explicit warning: *"This will result in a breaking graph when using the the ui."* If the result isn't a `LazyFrame` at all, the node degrades further to `output_df = input_df` and the real result is injected as a `precomputed_result` on the node's `results.resulting_data` (`_create_child_frame`, `flow_frame.py:399-418`) — **the visual graph now lies about what the node computes**, and only a `logger.error` hints at it.

### The general per-method skeleton

```python
new_node_id = generate_node_id()                        # utils.py — process-global counter
# decide native vs polars-code (mirror an existing use_polars_code_path / can_use_native flag)
settings = input_schema.NodeXxx(
    flow_id=self.flow_graph.flow_id, node_id=new_node_id,
    depending_on_id=self.node_id, description=description, is_setup=True,
)
self.flow_graph.add_xxx(settings)                        # or self._add_polars_code(...)
return self._create_child_frame(new_node_id)              # wires the connection, re-reads data
```

`_create_child_frame` (`flow_frame.py:399`) calls `_add_connection` — an `input_schema.NodeConnection.create_from_simple_input(...)` + `add_connection(self.flow_graph, ...)`, honoring `self.output_handle` — then **immediately** calls `get_resulting_data()` on the new node to obtain the next `LazyFrame`. Multi-input methods (`join`, `concat`, `fuzzy_join`) skip `_create_child_frame` and wire `"main"`/`"right"` connections manually before constructing the `FlowFrame(...)` directly.

**Node ids are process-global, not per-graph.** `generate_node_id()` (`utils.py`) increments a module-level dict `data = {"c": 0}`; `set_node_id(n)` resets it. Combining two graphs (e.g. via `join` across graphs, §3) bumps the counter by the combined node count. Consequence, verified live below: node ids inside a single graph can skip values — this is expected, not a bug.

Verified (isolated DB, `FLOWFILE_DB_PATH` set):

```python
import flowfile_frame as ff
df = ff.from_dict({"a": [1, 2, 3], "b": ["x", "y", "z"]})
out = (df.filter(ff.col("a") > 1)
         .with_columns((ff.col("a") * 2).alias("dbl"))
         .select("a", "dbl")
         .sort("a", descending=True))
for n in out.flow_graph.nodes:
    print(n.node_id, n.node_type, type(n.setting_input).__name__)
```
```
1 manual_input NodeManualInput
2 polars_code  NodePolarsCode      # filter — Expr predicates are never native (see 3d)
4 formula      NodeFormula         # with_columns via _ff_repr, one native node per expr
5 select       NodeSelect
6 sort         NodeSort
```
Node id `3` never appears — `with_columns` burned an id internally before delegating to the formula path. `out.collect()` returns the correct 2-row `pl.DataFrame`.

---

## 3. Worked examples — read these to internalize the pattern

### 3a. `sort` — dual-path with an explicit trigger list (`flow_frame.py:448`)
- Goes **native** (`NodeSort` / `transform_schema.SortByInput(column=…, how="asc"|"desc")`) only when *every* sort key is a plain string or an unaltered `Column`, **and** `nulls_last` is falsy, **and** `multithreaded=True`, **and** `maintain_order=False`.
- Any `Expr`, an altered `Column` (aliased/cast), `nulls_last`, `maintain_order`, or `multithreaded=False` flips to polars-code: `input_df.sort(<pure expr strs>, descending=…, …)`.
- Any collected function sources (from lambdas inside sort keys) get prepended as a definitions block, joined to the operation by the literal marker `\n#─────SPLIT─────\n\n`.

### 3b. `select` — native vs polars-code (`flow_frame.py:1022`)
- Strings and `Column` objects (including alias/cast via `Column.to_select_input()`) go **native**: builds a `transform_schema.SelectInput` list, adds unselected existing columns with a `keep` flag, emits `input_schema.NodeSelect`.
- Any general `Expr`, `Selector`, or expression with collected function sources → `input_df.select([<expr strs>])` polars-code.
- Special case: `select(len().alias("number_of_records"))` is pattern-matched by **string comparison of the repr** (`str(expr) == "pl.Expr(len()).alias('number_of_records')"`) and becomes a native `NodeRecordCount`.
- `rename(mapping)` is implemented as `select([col(old).alias(new), …], _keep_missing=True)` — its `strict` kwarg is accepted but **silently ignored**.

### 3c. `join` — native vs code, and cross-graph combining (`flow_frame.py:686`)
- `_should_use_polars_code_for_join` (`flow_frame.py:781`): any of `maintain_order`, `coalesce`, `nulls_equal=True`, `validate`, or a non-default `suffix` forces **polars-code**: `input_df_1.join(other=input_df_2, how=…, …)` with `depending_on_ids=[self.node_id, other.node_id]` and two `"main"` connections.
- Otherwise the native path builds `JoinMap`s and emits `NodeJoin` (or `NodeCrossJoin` for `how="cross"`), with `auto_generate_selection=True, verify_integrity=True`; right-side join keys get `keep=False`. Connections: left → `"main"`, right → `"right"`.
- `_ensure_same_graph` (`flow_frame.py:791`): if `self` and `other` live in **different** `FlowGraph`s, `combine_flow_graphs_with_mapping` merges them and **mutates both frames' `node_id`/`flow_graph` in place**. Any *third* `FlowFrame` still holding a reference into one of the old graphs is left stale — no re-mapping happens for frames the join doesn't touch directly. Keep frame references you plan to reuse after a cross-graph join to a minimum, or re-derive them from the merged graph.
- `fuzzy_join(other, fuzzy_mappings: list[FuzzyMapping])` (`flow_frame.py:3292`) is a Flowfile-only extension with no Polars equivalent → `NodeFuzzyMatch`.

### 3d. `group_by()` → `GroupByFrame` (`flow_frame.py:2361`, `flowfile_frame/flowfile_frame/group_frame.py`)
- `group_by()` itself creates **no node** — it returns a `GroupByFrame` carrying the future node id.
- `.agg(...)` tries to convert every aggregation to a native `transform_schema.AggColl` row (`agg="groupby"` for the grouping keys). Conversion requires `maintain_order=False`, no complex expressions, no `Selector`s, and every `agg_func` in the fixed set `_NATIVE_AGG_FUNCS = {sum, max, mean, median, min, count, n_unique, first, last, std, var, concat}` (`group_frame.py:20`). Named aggs also accept `name=("col", "aggstr")` tuples. Success → `NodeGroupBy`; anything outside the set → polars-code `input_df.group_by([...], maintain_order=…).agg(...)`.
- Direct `.sum()/.mean()/.median()` generate `…agg(cs.numeric().sum())` and aggregate **numeric columns only** — a deliberate deviation from Polars' `GroupBy.sum()`, which aggregates everything it can (`_NUMERIC_ONLY_METHODS`, `group_frame.py:235`). `.len()/.count()/.head()/.tail()/.first()/.last()/.min()/.max()` map straight to `.group_by(...).method(...)`.

Also note `filter` (`flow_frame.py:1129`): predicate-based filtering is **never native** — any `Expr` predicate always emits a polars-code `input_df.filter(...)` node. The **only** native filter path is the `flowfile_formula: str` kwarg, which builds a `NodeFilter(advanced)`. Verified by reading `filter`'s body: the predicate branch (len(predicates) > 0 or constraints) unconditionally calls `self._add_polars_code(...)`.

---

## 4. The expression system (`flowfile_frame/flowfile_frame/expr.py`, ~1.7k lines)

### Core state on every `Expr`
Every `Expr` carries **two parallel representations plus metadata** (`expr.py:423-493`):
- `expr: pl.Expr | None` — the live Polars expression, used for schema propagation and the serialization fallback; may be `None` for selector-aggregations.
- `_repr_str: str` — **the load-bearing generated Polars source**, e.g. `"(pl.col('a') * 2).alias('dbl')"`. Every method returns a *new* `Expr` with the repr extended (`_create_next_expr` appends `.method(args_repr)`; binary ops produce `(left op right)` and clear aggregation state).
- `_ff_repr: str | None` — an optional **flowfile-formula representation** (`[colname]`, `"str"`, `(a + b)`, `to_integer(...)`, `uppercase(...)`, …) maintained through operators, casts, and a subset of `.str`/`.dt` methods. When *every* expression in a `with_columns` call still has `_ff_repr` + a resolvable `column_name`, the whole call becomes a **chain of native `NodeFormula` nodes** instead of one polars-code node — this is the mechanism behind the `formula` node in the worked example above.
- `column_name` / `_initial_column_name` (rename tracking), `agg_func` (drives `GroupBy` `AggColl` conversion), `is_complex` (blocks native group-by conversion), `convertable_to_code` (False → serialization fallback, §2), `_function_sources: list[str]` (extracted function/lambda definitions to prepend to generated code).

### Subclasses & namespaces
- `Column(Expr)` — `pl.col(name)` plus a `transform_schema.SelectInput`; `.alias()`/`.cast()` return new `Column`s with `is_altered`/`data_type_change` flags so `select`/`sort`/`unique` can stay native when nothing structural changed.
- `When(Expr)` — `when().then()` mutate in place; `.otherwise()` returns a plain `Expr` carrying the full `pl.when(...).then(...).otherwise(...)` repr.
- `.str` → `StringMethods`, `.dt` → `DateTimeMethods` (hand-written), `.list` → `ExprListNameSpace`, `.name` → `ExprNameNameSpace`.
- `lit(value)` uses `pl.lit(value, allow_object=True)` and `repr(value)`; module-level `@agg_function`-decorated functions (`max/min/first/last/mean/count/implode/explode/sum/corr/cov`) render as `pl.sum('a')`-style calls and set `agg_func`.

### Method injection (two decorators — most of the surface is not hand-written)
- **`add_expr_methods(Expr)`**, invoked at `expr.py:1403` (module import time). For every callable on `pl.Expr` not already defined on `Expr` and not a dunder/property, it installs a wrapper (`adding_expr.py`) that calls the real Polars method for schema/error-checking (failures set `result_expr=None` and log at debug level, not raise), classifies the call via hard-coded `agg_methods`/`complex_methods` sets, and appends it to `_repr_str`. `PASSTHROUGH_METHODS = {"map_elements", "map_batches"}` (`adding_expr.py:16`) get callable-source extraction; an unresolvable callable sets `convertable_to_code=False` with a logged warning.
- **`@add_lazyframe_methods`** on `FlowFrame` (`lazy_methods.py:136`, applied at `flow_frame.py` class definition). Explicitly hand-defined methods always win over the injected ones. Injected `pl.LazyFrame` methods split in two:
  - `PASSTHROUGH_METHODS` (`lazy_methods.py:9-26`) — `collect, collect_async, profile, describe, explain, show_graph, fetch, collect_schema, columns, dtypes, schema, width, estimated_size, n_chunks, is_empty, chunk_lengths, get_meta` — delegate **straight to `self.data`, no node added**. Calling `describe()` really executes on the underlying `LazyFrame` right there.
  - Everything else → a generic wrapper emitting `output_df = input_df.<method>(<repr'd args>)` as a polars-code node (verified: `df.drop("y")` → `output_df = input_df.drop('y')`). Every injected method also gains an extra `description: str | None` kwarg.
  - `lazy_methods.py:108-110`: if any argument has `convertable_to_code=False`, the wrapper short-circuits into a *new source frame* built from the eagerly-computed result — graph lineage is severed at that point (a genuine edge case, not exercised in the test suite as of this writing).

### Lambda / function source extraction (`flowfile_frame/flowfile_frame/callable_utils.py`)
- Named functions: `inspect.getsource` (`_get_function_source`, `callable_utils.py:19`), dedented and stored verbatim.
- Lambdas: `_extract_lambda_source` (`callable_utils.py:52`) re-synthesizes a named `def _lambda_fn_<hash % 100000>(args): return <body>` via AST, matching by argument names, and **captures closure variables** as constant assignments or nested function defs; an unresolvable closure returns `(None, None)` → `convertable_to_code=False`.
- Verified live, run **from a file** (has retrievable source): `ff.col("n").map_elements(lambda x: x + 1, return_dtype=ff.Int64)` produced a polars-code node body:
  ```python
  def _lambda_fn_83562(x):
      return x + 1
  #─────SPLIT─────

  output_df = input_df.with_columns([pl.col('n').map_elements(_lambda_fn_83562, return_dtype=Int64).alias('m')])
  ```
  The **same lambda run from `stdin`/a REPL** (no retrievable source via `inspect.getsource`) falls into the base64-serialized-`LazyFrame` fallback (§2, path 3) instead — both variants `.collect()` to the same data, but only the file-based one produces a portable, UI-editable node. Note `return_dtype=Int64` has no `pl.` prefix in the generated code — it resolves only because core's sandbox injects bare dtype names into its namespace (§6).

### `fold` and the generic Polars-function wrapper
`lazy.py` provides `polars_function_wrapper` (`lazy.py:490`) — decorator machinery that turns arbitrary `pl.*` functions into `FlowFrame`/`Expr`-returning functions by deep-`repr`'ing their arguments. `fold` (`lazy.py:681-682`, `@polars_function_wrapper("fold", return_type="Expr")`) is the shipped example; use it as the template if you need to wrap another module-level Polars function the same way.

### Selectors (`selectors.py`)
`Selector` mirrors `polars.selectors`; its `repr_str` renders `pl.selectors.<name>()`-style strings. Selector aggregation methods (`.sum()`, `.mean()`, `.std(ddof)`, …) build an `Expr(expr=None, selector=…, agg_func=…)` whose repr is `<selector_repr>.<func>(…)`. **Selectors always force the polars-code path** in `select`/`group_by`/`unpivot` — there is no native-node route for a `Selector`.

---

## 5. Lazy semantics — exactly when does anything run?

**Graph building is schema-eager, data-lazy — with several genuinely eager exceptions worth memorizing.**

Baseline: every `_create_child_frame`/`FlowFrame(...)` call immediately invokes the new node's `get_resulting_data()` under a lock. For ordinary transforms this only chains `LazyFrame` operations (cheap, schema-resolution only) — confirmed above, every node logs "getting resulting data" at *build* time, not at `.collect()` time. `.collect(*args, **kwargs)` (`flow_frame.py:2446`) just collects the accumulated local `LazyFrame` and returns a `pl.DataFrame` — it does **not** invoke core's run machinery or talk to the worker.

Things that look lazy but are **not** (all verified live in this session):

| Operation | What actually happens | Evidence |
|---|---|---|
| `write_csv`/`sink_csv` and friends | `add_output`'s node function calls `df.output(..., execute_remote=False)` **at graph-build time** because `execution_location == "local"` always holds for frame graphs. `sink_csv → write_csv`, `sink_ipc → write_ipc`, etc. are literal aliases. | File exists on disk *before* `.collect()` is ever called. |
| `pivot` (native path) | Materializes its input during graph building — the node needs the distinct pivot-column values to compute the output schema. | Log line `Reading entire file: ~/.flowfile/cache/1/<uuid>.arrow … Got N unique values from external source`; leaves a cache file under `~/.flowfile/cache/`. |
| Passthrough methods (`describe`, `profile`, `fetch`, `collect_schema`, …) | Run immediately against `self.data`. | By construction — no node is added for these. |
| `from_dict(...)` / `FlowFrame(python_data)` | Rows are embedded as `NodeManualInput.raw_data_format` — the **full dataset gets serialized into the saved `.flowfile`**. | Direct read of `flow_frame.py` source. |
| Serialization fallback (§2, path 3) and the `lazy_methods` non-convertible short-circuit | Execute the real Polars operation at call time to capture a concrete result. | By construction. |

Readers (`read_csv`, `read_parquet`, `scan_*`) are genuinely lazy scans; `scan_csv`/`scan_parquet` are literal aliases of `read_csv`/`read_parquet`. `read_csv` uses a native `ReceivedTable`-based node when parameters fit a documented allowlist, else falls back to generated `pl.scan_csv` code. Passing a `BytesIO` as the CSV source degrades all the way to `from_dict(pl.read_csv(source))` — fully eager, because a `BytesIO` handle can't be re-scanned lazily by a saved graph.

`cache()` (`flow_frame.py:2616`) sets `cache_results=True` on the node settings (an execution hint the graph runner honors) **and** calls `self.data.cache()` immediately.

---

## 6. The generated-code contract with core's sandbox

Polars-code nodes execute inside `flowfile_core/flowfile_core/flowfile/flow_data_engine/polars_code_parser.py`'s `PolarsCodeExecutor`. When writing or reviewing a generated string, it must satisfy:

- **`safe_globals`** (`polars_code_parser.py:129`): `__builtins__ = {}`. Available names: `pl`, `cs` (= `pl.selectors`), `col`, `lit`, `expr`, every **bare Polars dtype name** (`Int64`, `String`, `Datetime`, `Struct`, …, no `pl.` prefix needed), a small basic-builtins set (`print, len, range, enumerate, zip, list, dict, set, str, int, float, bool, True, False, None`), plus `time`, `BytesIO`, `base64`, `datetime`.
- **`_validate_code`** (`polars_code_parser.py:191`): rejects any code containing an `import` statement, blocked calls (`exec`, `eval`, `getattr`, `open`, …), dunder attribute access, and string constants matching a `__word__` pattern.
- **`_wrap_in_function`** (`polars_code_parser.py:239`): code is wrapped in `_transform(input_df)` (or `_transform(input_df_1, …, input_df_N)` for multi-input nodes). A single-line expression starting with `pl.`/`col(`/`input_df`/`expr(` is returned as-is; any other body must assign the name **`output_df`**.
- **Input naming convention** (mirrored by frame codegen): 1 input → `input_df`; ≥2 inputs → `input_df_1..N`, in upstream-discovery order. `concat` also dedupes duplicate sources because `add_connection` is idempotent.
- The `#─────SPLIT─────` marker separating prepended function definitions from the operation is purely a **flowfile_frame-side cosmetic convention for the UI code editor** — comments are stripped before execution, and grep confirms no consumer in core parses that marker.

The rule that ties all of §2–§6 together (quoted verbatim from `flowfile_frame/CLAUDE.md`): **"Source string is load-bearing. … A wrong string breaks graph execution silently."**

---

## 7. `open_graph_in_editor` — frame graph to visual flow

Entry point: `from flowfile import open_graph_in_editor; open_graph_in_editor(frame.flow_graph)`. **`FlowFrame` itself has no `open_in_editor` method** — you pass its `.flow_graph`. Signature: `open_graph_in_editor(flow_graph, storage_location=None, module_name="flowfile", automatically_open_browser=True) -> bool` (`flowfile/flowfile/api.py`).

1. Copies the flow's settings, force-sets `execution_location="local"` and `execution_mode="Development"`, saves via `_save_flow_to_location` (a temp `temp_flow_<uuid>.yaml` if no `storage_location` given), then restores the original settings object — but the saved *file* still has `execution_location="local"` baked in, and `flow_settings.path` is left pointing at the saved file afterward.
2. `start_flowfile_server_process`: if `GET http://127.0.0.1:63578/docs` (host/port env-overridable via `FLOWFILE_HOST`/`FLOWFILE_PORT`) doesn't answer, spawns `flowfile run ui --no-browser` — via `poetry run` when a Poetry environment is detected, otherwise via the installed console script next to `sys.executable`. Waits up to 60s; registers an `atexit` kill hook.
3. `POST /auth/token` (empty body) → JWT; the flow is imported via the authenticated API to obtain a flow id.
4. Opens a browser tab at `http://HOST:PORT/ui/flow/{id}` **only** when the running server reports single-mode (`GET /single_mode`) **and** `FLOWFILE_MODE == "electron"`.

**Import side effect worth knowing about:** `import flowfile` (the top-level `flowfile` package, not `flowfile_frame`) sets `os.environ["FLOWFILE_WORKER_PORT"] = "63578"` and `os.environ["FLOWFILE_SINGLE_FILE_MODE"] = "1"` at import time, then re-exports the whole frame API (`col`/`lit`/`when`, readers, connections, selectors) plus core classes — this is why docs examples that write `import flowfile as ff` (rather than `import flowfile_frame as ff`) work unmodified.

---

## 8. DB, cloud, catalog, Kafka, REST connectors

All of these persist through `flowfile_core`'s storage layer (the shared SQLite catalog DB), not local files, and every helper hard-codes **`user_id = 1`** ("single-user mode": `database/connection_manager.py`, `cloud_storage/secret_manager.py`, `kafka.py`, `rest_api.py`, `catalog.py`).

- **Database** (`flowfile_frame/flowfile_frame/database/`): `create_database_connection(...)(connection_name, database_type=postgresql|mysql|sqlite|mssql|oracle, host/port/database/username/password, ssl_enabled, url)` stores a `FullDatabaseConnection`; `get_all_available_database_connections()` returns password-free interfaces. `read_database(connection_name, table_name=… | query=…, schema_name=…, flow_graph=…)` → `NodeDatabaseReader`; query wins if both `table_name` and `query` are given. `write_database(df, connection_name, table_name, schema_name=…, if_exists="append"|"replace"|"fail")` → `NodeDatabaseWriter`.
- **Cloud storage** (`flowfile_frame/flowfile_frame/cloud_storage/`): connection helpers in `secret_manager.py`. `read_from_cloud_storage(source, file_format=csv|parquet|json|delta, connection_name, scan_mode, …)` dispatches to `scan_{csv,parquet,json}_from_cloud_storage`/`scan_delta`; `write_to_cloud_storage(df, path, file_format, …, partition_by delta-only)` funnels through `add_write_ff_to_cloud_storage` → `NodeCloudStorageWriter`.
- **Catalog** (`catalog.py`, `catalog_reference.py`): `read_catalog_table`, `read_catalog_sql`, `write_catalog_table`, `register_flow_with_catalog`, plus navigable `CatalogReference`/`SchemaReference`/`list_catalogs()`/`default_schema()` that talk to `CatalogService` directly (in-process, same DB).
- **Kafka** (`kafka.py`, `read_kafka`) and **REST** (`rest_api.py`, `read_api`, with auth/pagination coercers `_coerce_auth`/`_coerce_pagination`) add `NodeKafkaSource`/`NodeRestApiReader` nodes.

**On "does this need a running `flowfile_worker`": no, for frame graphs — verified, corrects a natural assumption.** Core's general architecture rule is "the worker does all external fetching" (GA4 is the canonical zero-exception template — see `flowfile-node-development` §1.5 for the full rule and rationale). But `add_database_reader`, `add_kafka_source`, and `add_rest_api_reader` each carry an explicit `if self.execution_location == "local":` branch in `flow_graph.py` that fetches **in-process** instead of dispatching to the worker — added specifically so CLI/`--run-flow` and other no-worker execution modes still work. Since `create_flow_graph()` always sets `execution_location = "local"` for any graph `flowfile_frame` builds, **every DB/Kafka/REST read you do from a `flowfile_frame` script runs in-process, with no worker required for `.collect()`.** Verified live in this session: `ff.read_api(url="https://jsonplaceholder.typicode.com/todos/1").collect()` succeeded with fetching happening inline (`shared/rest_api/fetch.py::fetch_rest_api`, the same function the worker calls, just invoked directly) — no `flowfile_worker` process was running anywhere on the host. This only applies to `.collect()`-time execution of a frame-built graph; a flow later opened in the Designer and run in a non-local execution mode goes back through the worker like any other flow (`flowfile-architecture-contract`).

**Import side effect:** these connector modules import `flowfile_core`, whose package `__init__` runs `validate_setup()` + `init_db()` — **importing `flowfile_frame` creates and Alembic-migrates the catalog DB** if it doesn't already have the current schema. For any probing/scripting, isolate with `FLOWFILE_DB_PATH=<scratch path>` (per the standing project rule — do not point ad hoc scripts at the live catalog DB).

---

## 9. Parity vs Polars — native node / polars-code / missing

| Operation | Native-node condition | Fallback |
|---|---|---|
| `select` | strings + (aliased/cast) `Column`s only | polars-code `input_df.select([...])` |
| `filter(*predicates)` | **never native** for `Expr` predicates | `flowfile_formula=` kwarg → native `NodeFilter(advanced)` |
| `sort` | plain columns, no `nulls_last`/`maintain_order`, `multithreaded=True` | polars-code |
| `join` | equality joins, default suffix, no `validate`/`nulls_equal`/`coalesce`/`maintain_order`; `how="cross"` always native | polars-code join |
| `group_by().agg` | simple exprs, aggs in the fixed `_NATIVE_AGG_FUNCS` set, no `maintain_order`/`Selector` | polars-code |
| `pivot` | single `on` + single `values`, agg in `{first,last,min,max,sum,mean,median,count}`; `values` is **required** (raises `ValueError` if `None`) | polars-code — **multi-column fallback is currently broken**, see §10 |
| `unpivot` | plain columns, default variable/value names | polars-code |
| `unique` | string/unaltered-`Column` subset, no `maintain_order` | polars-code |
| `concat` | **only** `how="diagonal_relaxed"` + parallel + `!rechunk` + no duplicate sources → `NodeUnion(mode="relaxed")` | default `how="vertical"` goes to polars-code `pl.concat([...])` |
| `with_columns` | every expr `_ff_repr`-convertible → chain of native `NodeFormula` nodes (one per expr); `flowfile_formulas=` kwarg can also auto-upgrade via `polars_expr_transformer.to_flowframe_code` (≥0.5.4) | polars-code `input_df.with_columns([...])` |
| `with_row_index` | `name == "record_id"`, or (`offset == 1` and `name != "index"`) → `NodeRecordId`; also detects `cum_count().over(...)` patterns | polars-code |
| `head`/`limit` | `NodeSample`, always | — |
| `rename` | implemented via `select(..., _keep_missing=True)` | — |
| everything else on `pl.LazyFrame` (`drop`, `tail`, `slice`, `shift`, `reverse`, `fill_null`, `quantile`, …) | injected generic wrapper → polars-code | passthroughs (§5) add no node at all |

**Flowfile-only extensions with no Polars equivalent:** `filter_split` (→ `(pass, fail)` frames on output handles 0/1), `random_split(splits, seed)` (→ N frames), the ML verbs `train_model`/`apply_model`/`evaluate_model`/`wait_for`, `fuzzy_join`, `text_to_rows`, `solve_graph` (graph connected-components), `dynamic_rename` (prefix/suffix/formula/first-row renaming), the visual-grouping context manager `with df.group("name"):` + `set_group` (organizational only, no data effect), `write_catalog_table`, the cloud/DB writers, `to_graph`/`save_graph`.

**Known parity deviations, not bugs:**
- `GroupByFrame.sum/mean/median` aggregate `cs.numeric()` columns only (Polars aggregates everything it can).
- Multiple `filter` predicates are AND-joined into a single polars-code node, not chained node-by-node.
- `concat`'s default is graph-opaque polars-code, not the graph-friendly `NodeUnion`.
- `Series` is a 65-line stub; there is no eager `DataFrame` type — `LazyFrame = DataFrame = FlowFrame` aliases in `__init__.py` exist purely "for compatibility with generated code" and **must not be removed** (contract stated in `flowfile_frame/CLAUDE.md`).

For a plan to close specific gaps in this table, see `flowfile-codegen-parity-campaign` — this table is a snapshot, not a roadmap.

---

## 10. Stub generation — `make stubs` / `make check_stubs`

Because `FlowFrame` and `Expr` get most of their methods **injected at runtime** (§4), static type checkers see almost nothing without stubs. The package ships committed `.pyi` files plus `py.typed` (PEP 561) for every module — **27 `.pyi` files** as of this writing, including `database/` and `cloud_storage/` submodules.

```bash
# Makefile:259 — regenerate all stubs
stubs:
    poetry run python flowfile_frame/expr_stub_generator.py
    poetry run python flowfile_frame/flow_frame_stub_generator.py
    poetry run python flowfile_frame/submodule_stub_generator.py
    ruff check $(find flowfile_frame/flowfile_frame -name '*.pyi') --select F401 --fix --quiet

# Makefile:269 — CI drift gate: rerun stubs, then diff
check_stubs: stubs
    git diff --exit-code -- 'flowfile_frame/flowfile_frame/*.pyi' 'flowfile_frame/flowfile_frame/**/*.pyi'
```

- **`expr_stub_generator.py`** → `expr.pyi`. **Imports the live module** and introspects `Expr`/`Column`/`StringMethods`/`DateTimeMethods`/`When`/`ExprNameNameSpace`/`ExprListNameSpace` plus top-level functions; adds every remaining `pl.Expr` method as `def m(self, *args, **kwargs) -> 'Expr': ...`. Output is **explicitly sorted for determinism** — a source comment notes unsorted output caused spurious diffs before this was fixed.
- **`flow_frame_stub_generator.py`** → `flow_frame.pyi`. Introspects the live `FlowFrame` (so injected `LazyFrame` methods appear) plus module-level functions. **Duplicates the `PASSTHROUGH_METHODS` set** from `lazy_methods.py` by hand — if you change the passthrough set, update this copy too or the generated stub drifts silently from actual runtime behavior. Rewrites `LazyFrame` → `FlowFrame` in annotations.
- **`submodule_stub_generator.py`** → a `.pyi` for **every other `.py` file in the package, including `__init__.pyi`** (pure AST-based, no import needed; skips only `expr.py`/`flow_frame.py` and private modules; flattens `if TYPE_CHECKING:` imports; prunes unused imports; emits `__all__` in `__init__.pyi` for PyCharm). Generated-file header: `# Auto-generated stub for … — do not edit.`
- **Stale-doc trap:** `flowfile_frame/readme.md` claims `__init__.pyi` is "hand-maintained" — it is **not**; it is generated (verified by reading the header), and `flowfile_frame/CLAUDE.md` explicitly corrects this. Trust the `CLAUDE.md`, not the readme, on this point.
- Because the first two generators **import `flowfile_frame`**, running `make stubs` has the **same DB-migrating import side effect** as importing the library (§8). Isolate:
  ```bash
  FLOWFILE_DB_PATH=/tmp/stub_scratch.db make stubs
  ```
- **CI gate:** `make check_stubs` runs as the `check-stubs` job in `.github/workflows/test.yaml`, gated on `flowfile_frame/**` changes (via the `backend_frame` path-filter output) or a manual `run_all_tests` dispatch. Failure message: *"stubs are out of sync with the source. Run 'make stubs' and commit the result."* — **any public-surface change to frame code without regenerating stubs fails CI**, not just a lint warning.

---

## 11. Extend-the-API checklist

1. **Pick the emission path** (§2). Prefer a dedicated core node when one exists; use `_add_polars_code` only for genuinely complex cases, mirroring the `use_polars_code_path`/`can_use_native` flag pattern already used by `sort`/`select`/`join`.
2. **Make the generated code string sandbox-executable** (§6): no imports/`getattr`/dunder-attribute strings, assign `output_df` (or a single-line `pl.`/`col(`-prefixed expression), use `input_df`/`input_df_1..N`, prepend function sources with the `#─────SPLIT─────` convention.
3. **Follow the method skeleton** (§2): `generate_node_id()` → settings (always include `flow_id`, `node_id`, `depending_on_id(s)`, `is_setup=True`, `description`) → `flow_graph.add_*` → `self._create_child_frame(new_node_id)` for single input, or manual `"main"`/`"right"` connections + a direct `FlowFrame(...)` construction for multi-input.
4. **Accept `description: str | None = None`** on every graph-building method — the frontend uses it as the node label.
5. For `Expr` changes: keep `_repr_str` correct (this is the whole contract); propagate `_function_sources`, `convertable_to_code`, `agg_func`, `is_complex`; add an `_ff_repr` mapping when a flowfile-formula equivalent exists (unlocks native `NodeFormula` emission via `with_columns`). Explicitly-defined methods always win over injected ones.
6. **Keep the `__init__.py` exports intact**: the `LazyFrame = DataFrame = FlowFrame` aliases and Polars dtype re-exports — generated code and docs examples depend on both existing.
7. **Regenerate stubs and stage the diff** (do not commit it yourself — hand off per `flowfile-change-control`'s no-agent-commit policy): `FLOWFILE_DB_PATH=<scratch> make stubs`; `make check_stubs` is what CI runs. If you touched a passthrough-methods set, update the hand-duplicated copy in `flow_frame_stub_generator.py` too.
8. **Test both the result and the graph shape** — `.collect()` against a plain-Polars reference implementation, *and* an assertion on node type / generated code string, mirroring `test_ff_repr.py`'s `_is_formula_node`/`_is_polars_code_node` helper pattern. Round-trip anything UI-visible through `save_graph` + reopen.
9. Run `FLOWFILE_DB_PATH=/tmp/frame_test.db poetry run pytest flowfile_frame/tests --disable-warnings` and `poetry run ruff check flowfile_frame`.

---

## 12. Tests layout

`flowfile_frame/tests/` is flat — no package-specific pytest marker (root `pyproject.toml` registers only `worker`/`core`/`kernel`/`docker_integration`/`kafka`). `tests/conftest.py` sets `TESTING=True` at import and unconditionally deletes/recreates a cloud connection named `minio-flowframe-test` (s3, `http://localhost:9000`, `minioadmin`/`minioadmin`) in the catalog DB at collection time — this only *registers* the connection row; MinIO itself is needed only by the tests that actually read/write through it. Docker-gated tests use `tests/utils.py::is_docker_available()` with `@pytest.mark.skipif`.

| File | Lines | Covers |
|---|---|---|
| `test_lazy_frame.py` | 1545 | Injected `LazyFrame` methods |
| `test_flow_frame.py` | 934 | Core ops, writer round-trips, `save_graph` |
| `test_expressions.py` | 901 | `Expr` behavior |
| `test_ff_repr.py` | 848 | `_ff_repr` contract; "is this a `NodeFormula` or `NodePolarsCode`" assertions |
| `test_group_frame.py` | 477 | `GroupByFrame` |
| `test_joins.py` | 333 | `join`/`fuzzy_join` |
| `test_catalog_reference.py` | 242 | Catalog navigation |
| `test_dynamic_rename.py` | 107 | `dynamic_rename` |
| `test_flowfile_frame.py` | 117 | Misc/top-level API |
| `test_evaluate_model.py`, `test_node_groups.py`, `test_catalog_write_partition.py`, `test_cloud_write_partition.py` | 93 / 85 / 19 / 45 | ML eval, visual groups, catalog/cloud partitioned writes |

CI runs `poetry run pytest flowfile_frame/tests --disable-warnings` in both the Linux/macOS matrix and the Windows job. Isolate local runs with `FLOWFILE_DB_PATH` — the shared `TESTING` DB can be dropped mid-run by a concurrent pytest session (`flowfile-testing-and-validation` has the full isolation model).

---

## 13. Known live weaknesses (state plainly, don't paper over)

1. **Multi-column pivot fallback is broken.** Verified: `df.pivot(on=["c", "c2"], index="k", values="v", aggregate_function="sum")` raises `TypeError: LazyFrame.pivot() got an unexpected keyword argument 'sort_columns'` at graph-*build* time. The fallback template passes `DataFrame`-only kwargs to a `LazyFrame` call and assigns to a bare `result` name instead of `output_df`. Single `on`/`values` pivots (the native path) work fine.
2. **`from_dict` single-row-list quirk.** `ff.from_dict({"x": [1]})` produces a `list[i64]` column instead of `i64` (verified). Multi-row dicts behave normally; likely a `FlowDataEngine`-side raw-data inference edge case for 1-row inputs. If you see an unexpected `List` dtype on a single-row manual-input frame, this is why.
3. **`rename(strict=...)` is accepted but silently ignored** — no error, no effect.
4. **`Expr.over()` logs a broken f-string on failure**: `logger.warning("Could not create polars expression for over(): {e}")` — the `{e}` is never interpolated (missing `f` prefix). Cosmetic, but means the log line is useless for debugging an `over()` failure.
5. **The serialization fallback for a non-`LazyFrame` result silently degrades the node** to `output_df = input_df` while injecting the real result out-of-band — the visual graph misrepresents the computation, and only a `logger.error` hints at it. If a saved `.flowfile` "does nothing" for a node that should transform data, check whether that node went through this path.
6. `lazy_methods`' non-convertible-arg short-circuit passes a generator expression as a single call argument — looks structurally suspect; not exercised by the test suite as of this writing, treat as unverified rather than confirmed-safe.

---

## 14. Provenance and maintenance

Facts above dated **2026-07-03 (v0.12.7)**, branch `feature/claude-skills`. Re-run these before trusting a line number or count in a newer checkout.

```bash
# line-number/shape spot checks for the big three files
grep -n "def __new__\|def __init__\|def sort\|def select\|def join\|def filter\b\|def group_by\b" flowfile_frame/flowfile_frame/flow_frame.py
grep -n "add_expr_methods(Expr)$" flowfile_frame/flowfile_frame/expr.py
grep -n "^PASSTHROUGH_METHODS" flowfile_frame/flowfile_frame/lazy_methods.py flowfile_frame/flowfile_frame/adding_expr.py

# confirm the sandbox contract hasn't drifted
grep -n "safe_globals\|_validate_code\|_wrap_in_function" flowfile_core/flowfile_core/flowfile/flow_data_engine/polars_code_parser.py

# confirm which connectors have an execution_location=="local" in-process branch
grep -n 'execution_location == "local"' flowfile_core/flowfile_core/flowfile/flow_graph.py

# .pyi file count (was 27)
find flowfile_frame/flowfile_frame -name '*.pyi' | wc -l

# stub pipeline still wired the same way
grep -n "^stubs:\|^check_stubs:" -A 8 Makefile
grep -n "check-stubs" -A 3 .github/workflows/test.yaml

# reproduce the node-chain / node-id-skip probe from §2 (isolate the DB!)
FLOWFILE_DB_PATH=/tmp/frame_probe.db poetry run python - <<'EOF'
import flowfile_frame as ff
df = ff.from_dict({"a": [1, 2, 3], "b": ["x", "y", "z"]})
out = df.filter(ff.col("a") > 1).with_columns((ff.col("a") * 2).alias("dbl")).select("a", "dbl").sort("a", descending=True)
for n in out.flow_graph.nodes:
    print(n.node_id, n.node_type, type(n.setting_input).__name__)
print(out.collect())
EOF

# reproduce the "REST read needs no worker" claim (run with no flowfile_worker process alive)
FLOWFILE_DB_PATH=/tmp/frame_probe2.db poetry run python -c "
import flowfile_frame as ff
print(ff.read_api(url='https://jsonplaceholder.typicode.com/todos/1').collect())
"

# reproduce the multi-column pivot bug and the from_dict single-row quirk
FLOWFILE_DB_PATH=/tmp/frame_probe3.db poetry run python -c "
import flowfile_frame as ff
df = ff.from_dict({'k':[1,1,2,2],'c':['a','b','a','b'],'c2':['x','x','y','y'],'v':[1,2,3,4]})
df.pivot(on=['c','c2'], index='k', values='v', aggregate_function='sum').collect()
"

# run the frame test suite in isolation
FLOWFILE_DB_PATH=/tmp/frame_test.db poetry run pytest flowfile_frame/tests --disable-warnings
```

Corrected-in-this-writing note: an earlier assumption that `read_api`/REST reads categorically "require a running `flowfile_worker`" does **not** hold for `flowfile_frame` graphs — see §8's verified counter-example. If you find code or docs elsewhere in the repo asserting otherwise, treat *this* skill's live-verified result as current and flag the other location as drift.
