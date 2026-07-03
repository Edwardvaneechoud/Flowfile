# Discovery dossier — KEY=frame-api: the `flowfile_frame` programmatic API

All paths relative to repo root `/Users/edwardvaneechoud/flowfile_backup/Flowfile` unless absolute.
Everything below was verified by reading source or running probes on 2026-07-03 (branch `improvement/improve-naming-unnamed-flows`, clean tree) unless marked **inferred**.

---

## 1. Mental model

`flowfile_frame` (import as `import flowfile_frame as ff`) is a **Polars-LazyFrame façade that builds a Flowfile ETL graph as a side effect of every method call**. It is *not* a client of the backend HTTP API — it imports `flowfile_core` directly and mutates an **in-process `FlowGraph`** (`flowfile_frame/flowfile_frame/flow_frame.py:17-22` imports `FlowDataEngine`, `FlowGraph`, `add_connection`, `combine_flow_graphs_with_mapping`, `FlowNode`, `input_schema`, `transform_schema`).

A `FlowFrame` is a lightweight handle around four fields (set in `__new__`, `flow_frame.py:274-368`):

| field | meaning |
|---|---|
| `flow_graph` | the shared in-process `FlowGraph` |
| `node_id` | the node this frame points at |
| `parent_node_id` | upstream node id |
| `output_handle` | which source handle to read (`"output-0"` default; `"output-1"` = fail branch of `filter_split`) |
| `data` | the node's resulting **Polars LazyFrame** — `flow_graph.get_node(node_id).get_resulting_data().data_frame` |

**All initialization is in `__new__`; `__init__` is an intentional no-op** (`flow_frame.py:370-378`) — the class is a factory: pass `flow_graph`+`node_id` to wrap an existing node, pass raw data to create a `NodeManualInput` source, pass a `pl.LazyFrame` to create a graph dependency on an in-memory frame (`add_dependency_on_polars_lazy_frame`, `flow_frame.py:365`).

The graph a frame builds is the *same* DAG the Designer UI edits and core executes: it can be saved (`save_graph` → `flow_graph.apply_layout()` + `save_flow`, `flow_frame.py:2440-2444`), opened in the Designer (`open_graph_in_editor` in `flowfile/flowfile/api.py:399`), or materialized (`collect()`).

`utils.create_flow_graph()` (`flowfile_frame/flowfile_frame/utils.py:65-86`) seeds every new graph with `track_history=False` and — crucially — `execution_location = "local"` "so that the run time does not attempt to use the flowfile_worker process". Graph-building never talks to the worker or any port.

### The two (plus one) node-emission paths

1. **Native node**: when arguments fit a dedicated core node schema, the method builds a Pydantic settings object from `flowfile_core.schemas.input_schema` / `transform_schema` and calls the matching `flow_graph.add_*()` (e.g. `add_select`, `add_sort`, `add_join`, `add_group_by`, `add_pivot`, `add_formula`, `add_output` …). These render as first-class editable nodes in the UI.
2. **Polars-code node**: complex cases call `FlowFrame._add_polars_code` (`flow_frame.py:588-684`) which creates a `NodePolarsCode` whose payload is a **generated Polars source string** (`transform_schema.PolarsCodeInput(polars_code=...)`). Core re-executes that string in a sandbox (see §5), so the string must be valid, sandbox-safe Polars code.
3. **Serialization fallback** (inside `_add_polars_code`): if the expression is *not* convertible to code (`convertable_to_code=False`, e.g. an unresolvable lambda) or the code still contains a raw `<lambda> at` repr, the operation is executed on `self.data` directly via `getattr(self.data, method_name)(...)` and the resulting LazyFrame is **base64-serialized into the node** (`pl.LazyFrame.deserialize(buffer)` code, `flow_frame.py:639-656`), with an explicit warning: *"This will result in a breaking graph when using the the ui."* If the result isn't a LazyFrame at all, the node degrades to `output_df = input_df` and the real result is injected as a `precomputed_result` into `node.results.resulting_data` (`_create_child_frame`, `flow_frame.py:399-418`) — the visual graph no longer matches the computation.

### The general per-method pattern

Every graph-building method follows the same skeleton:

```
new_node_id = generate_node_id()                       # global counter (utils.py:106-116)
<decide native vs polars-code via use_polars_code_path/can_use_native flags>
<build input_schema.NodeXxx(flow_id=self.flow_graph.flow_id, node_id=new_node_id,
                            depending_on_id=self.node_id, description=..., is_setup=True)>
self.flow_graph.add_xxx(settings)                      # or self._add_polars_code(...)
return self._create_child_frame(new_node_id)           # adds NodeConnection + wraps result
```

`_create_child_frame` (`flow_frame.py:399-418`) calls `_add_connection` (a `NodeConnection.create_from_simple_input` + `add_connection`, honoring `self.output_handle`), then immediately calls `get_resulting_data()` on the new node to obtain the next LazyFrame. Multi-input methods (`join`, `concat`, `fuzzy_join`) instead wire connections manually (`"main"` / `"right"` input types) and construct the `FlowFrame(...)` directly.

**Node ids are process-global**, not per-graph: `generate_node_id` increments a module-level dict `data = {"c": 0}` (`utils.py:106-111`); `set_node_id` resets it. Combining two graphs bumps the counter by the combined node count (`flow_frame.py:806`, `2895`). Consequence: node ids in a single graph can skip values (verified: a `filter → with_columns → select → sort` chain produced node ids 1,2,4,5,6 because `with_columns` burned id 3 before delegating to the formula path).

---

## 2. Three concrete emission examples (read these to learn the pattern)

### 2a. `sort` — dual-path with explicit trigger list (`flow_frame.py:448-586`)
- Native `NodeSort` (`transform_schema.SortByInput(column=…, how="asc|desc")`) when *every* sort key is a plain string or unaltered `Column` **and** `nulls_last` is falsy **and** `multithreaded=True` **and** `maintain_order=False`.
- Any Expr, altered Column, `nulls_last`, `maintain_order`, or `multithreaded=False` flips `use_polars_code_path=True`; `_generate_sort_polars_code` (`flow_frame.py:421-446`) emits `input_df.sort(<pure expr strs>, descending=…, …)`.
- Function-source definitions collected from the exprs get prepended as a definitions section joined by `\n#─────SPLIT─────\n\n` before `output_df = <op>` (`flow_frame.py:538-545`).
- The polars-code call passes `method_name="sort"`, the raw `pl.Expr` list and kwargs so `_add_polars_code` can run the serialization fallback if needed.

### 2b. `select` — native `NodeSelect` vs polars-code (`flow_frame.py:1022-1127`)
- Strings and `Column` objects (incl. alias/cast via `Column.to_select_input()`, `expr.py:1375-1392`) go native: builds `transform_schema.SelectInput` list, adds unselected existing columns with `keep=_keep_missing`, emits `input_schema.NodeSelect`.
- Any general Expr, `Selector`, or collected function sources → `input_df.select([<expr strs>])` polars-code node.
- Special-case: `select(len().alias("number_of_records"))` is pattern-matched by *string comparison of the repr* (`str(expr) == "pl.Expr(len()).alias('number_of_records')"`, `flow_frame.py:1030-1035`) and becomes a native `NodeRecordCount`.
- `rename(mapping)` is implemented as `select([col(old).alias(new)…], _keep_missing=True)` (`flow_frame.py:1014-1020`). **Its `strict` parameter is accepted but ignored.**

### 2c. `join` — native vs code, and cross-graph combining (`flow_frame.py:686-999`)
- `_should_use_polars_code_for_join` (`flow_frame.py:781-789`): any of `maintain_order`, `coalesce`, `nulls_equal=True`, `validate`, or a non-default `suffix` forces the polars-code path → `input_df_1.join(other=input_df_2, how=…, …)` with `depending_on_ids=[self.node_id, other.node_id]` and two `"main"` connections.
- Otherwise `_create_join_mappings` builds `JoinMap`s; native path emits `NodeJoin` (or `NodeCrossJoin` for `how="cross"`), with `auto_generate_selection=True, verify_integrity=True`; right join keys get `keep=False` (`flow_frame.py:941-943`). Connections: left `"main"`, right `"right"`.
- `_ensure_same_graph` (`flow_frame.py:791-806`): if the two frames live in different graphs, `combine_flow_graphs_with_mapping` merges them and **mutates both frames' `node_id`/`flow_graph` in place**; any *other* FlowFrame still pointing at one of the old graphs becomes stale (**inferred** — no re-mapping of third-party frames occurs).
- `fuzzy_join(other, fuzzy_mappings: list[FuzzyMapping])` (`flow_frame.py:3292-3318`) is a Flowfile-only extension → `NodeFuzzyMatch`.

### 2d. (bonus) `group_by` → `GroupByFrame` (`flow_frame.py:2361-2397`, `flowfile_frame/flowfile_frame/group_frame.py`)
- `group_by()` creates *no node*; it returns a `GroupByFrame` carrying the future node id.
- `.agg(...)` tries to convert everything to native `transform_schema.AggColl` rows (`agg="groupby"` for keys). Conversion requires: `maintain_order=False`, no complex exprs, no Selectors, and every `agg_func` in `_NATIVE_AGG_FUNCS = {sum,max,mean,median,min,count,n_unique,first,last,std,var,concat}` (`group_frame.py:20-33`). Named aggs also accept `name=("col", "aggstr")` tuples. Success → `NodeGroupBy`; failure → polars-code `input_df.group_by([...], maintain_order=…).agg(...)`.
- Direct `.sum()/.mean()/.median()` generate `…agg(cs.numeric().sum())` — **numeric columns only** (`_NUMERIC_ONLY_METHODS`, `group_frame.py:235-260`), a deliberate deviation from Polars' GroupBy.sum. `.len()/.count()/.head()/.tail()/.first()/.last()/.min()/.max()` map to `.group_by(...).method(...)` directly.

---

## 3. The expression system (`flowfile_frame/flowfile_frame/expr.py`, 1679 lines)

### Core state on `Expr` (`expr.py:423-493`)
Every `Expr` carries **two parallel representations plus metadata**:
- `expr: pl.Expr | None` — the live Polars expression (used for schema propagation and the serialization fallback; may be None for selector-aggregations).
- `_repr_str: str` — **the load-bearing generated Polars source** (e.g. `"(pl.col('a') * 2).alias('dbl')"`). Every method returns a *new* Expr with the repr extended: `_create_next_expr` appends `.method(args_repr)` (`expr.py:575-616`); binary ops produce `(left op right)` and clear agg state (`expr.py:625-671`).
- `_ff_repr: str | None` — an optional **flowfile-formula representation** (`[colname]`, `"str"`, `(a + b)`, `to_integer(...)`, `uppercase(...)` …) maintained through operators (`FF_OPERATOR_MAP`), casts (`CAST_FF_MAP`), a subset of `.str` methods (`STRING_METHOD_FF_MAP`) and `.dt` parts (`DT_METHOD_FF_MAP`) (`expr.py:45-104`). When *every* expr in a `with_columns` still has `_ff_repr` + `column_name`, the operation becomes a chain of **native `NodeFormula` nodes** instead of polars-code (`flow_frame.py:3117-3124`) — verified live: `(ff.col("a") * 2).alias("dbl")` produced a `formula` node.
- `column_name` / `_initial_column_name` (rename-tracking), `agg_func` (drives GroupBy AggColl), `is_complex` (blocks native group-by conversion; heuristic `is_simple` property at `expr.py:498-557` scans `_repr_str` for operators/`when(`/etc.), `convertable_to_code` (False → serialization fallback), `_function_sources: list[str]` (extracted function definitions to prepend to generated code).

### Subclasses & namespaces
- `Column(Expr)` (`expr.py:1286-1400`): `pl.col(name)` with a `transform_schema.SelectInput`; `alias`/`cast` return new `Column`s with `is_altered`/`data_type_change` set so `select`/`sort`/`unique` can stay native. `col()`/`column()` return `Column` (`expr.py:1474-1481`).
- `When(Expr)` (`expr.py:1406-1470`): `when().then()` mutate in place; `.otherwise()` returns a plain `Expr` with full `pl.when(...).then(...).otherwise(...)` repr (verified output: `pl.when((pl.col('a') > 1)).then(pl.lit('big')).otherwise(pl.lit('small')).alias('size')`).
- `.str` → `StringMethods` (hand-written, `expr.py:124-330`), `.dt` → `DateTimeMethods` (`expr.py:333+`), `.list` → `ExprListNameSpace` (`list_name_space.py`), `.name` → `ExprNameNameSpace` (`expr_name.py`).
- `lit(value)` uses `pl.lit(value, allow_object=True)` and `repr(value)` (`expr.py:1484-1488`); `len()`, and `@agg_function`-decorated module functions (`max/min/first/last/mean/count/implode/explode/sum/corr/cov`, `expr.py:1495-1645`) render as `pl.sum('a')` etc. and set `agg_func`.

### Method injection (two decorators)
- `add_expr_methods(Expr)` runs at `expr.py:1403` (module import time). For every callable on `pl.Expr` not already defined and not a property/underscore name, it installs a wrapper (`adding_expr.py:19-121`) that: calls the real polars method (errors → `result_expr=None`, logged debug), classifies via hard-coded `agg_methods`/`complex_methods` sets, and appends the call to `_repr_str`. `PASSTHROUGH_METHODS = {"map_elements", "map_batches"}` (`adding_expr.py:16`) get callable-source extraction and set `convertable_to_code=False` (with a logged warning) if any callable can't be resolved.
- `@add_lazyframe_methods` on `FlowFrame` (`lazy_methods.py:136-187`). Explicitly defined methods always win. Injected `pl.LazyFrame` methods split into:
  - `PASSTHROUGH_METHODS` (`lazy_methods.py:9-27`): `collect, collect_async, profile, describe, explain, show_graph, fetch, collect_schema, columns, dtypes, schema, width, estimated_size, n_chunks, is_empty, chunk_lengths, get_meta` → delegate straight to `self.data`, **no node added** (so `describe()` really collects data right there).
  - everything else → generic wrapper emitting `output_df = input_df.<method>(<repr'd args>)` polars-code node (verified: `df.drop("y")` → polars_code `output_df = input_df.drop('y')`). Extra kwarg `description` is injected into every wrapped signature. Note `lazy_methods.py:108-110`: if any arg has `convertable_to_code=False` the wrapper short-circuits into a *new source frame* built from the eagerly-computed result — graph lineage is severed (edge case).

### Lambda / function source extraction (`flowfile_frame/flowfile_frame/callable_utils.py`)
- Named functions: `inspect.getsource` (`_get_function_source`), dedented and stored verbatim.
- Lambdas: `_extract_lambda_source` (`callable_utils.py:52-128`) re-synthesizes a named `def _lambda_fn_<hash % 100000>(args): return <body>` via AST, matching the lambda by arg names, and **captures closure variables** as constant assignments (repr-able values) or nested function defs; unresolvable closures → `(None, None)` → `convertable_to_code=False`.
- Verified end-to-end: in a real file, `ff.col("n").map_elements(lambda x: x + 1, return_dtype=ff.Int64)` produced a polars_code node:
  ```
  def _lambda_fn_83562(x):
      return x + 1
  #─────SPLIT─────

  output_df = input_df.with_columns([pl.col('n').map_elements(_lambda_fn_83562, return_dtype=Int64).alias('m')])
  ```
  The **same code run from stdin/REPL** (no retrievable source) fell into the base64-serialized-LazyFrame fallback ("breaking graph when using the the ui") — both variants `.collect()` correctly.
- Note the generated `return_dtype=Int64` has no `pl.` prefix — it only works because core's sandbox injects bare dtype names (see §5).

### `fold` and the generic polars-function wrapper
`lazy.py` provides `polars_function_wrapper` (`lazy.py:490+`) — a decorator machinery that wraps arbitrary `pl.*` functions into FlowFrame/Expr-returning functions by deep-repr'ing arguments; `fold` (`lazy.py:682`) is the exported example.

### Selectors (`selectors.py`)
`Selector` hierarchy mirrors `polars.selectors`; `repr_str` renders `pl.selectors.<name>()`-style strings consumed in generated code as `cs.…` is also available; selector aggregation methods (`.sum()`, `.mean()`, `.std(ddof)` …) build `Expr(expr=None, selector=…, agg_func=…)` whose repr is `<selector_repr>.<func>(…)` (`expr.py:461-472`). Selectors always force the polars-code path in `select`/`group_by`/`unpivot`.

---

## 4. Lazy semantics — when does anything actually run?

**Graph building is schema-eager, data-lazy — with several genuinely eager exceptions.**

- Every `_create_child_frame`/`FlowFrame(...)` call immediately invokes `FlowNode.get_resulting_data()` (`flowfile_core/flowfile_core/flowfile/flow_node/flow_node.py:974`), which executes the node's function under a lock. For ordinary transforms the "function" just chains LazyFrame operations, so cost is schema-resolution only. Verified in probe logs: every node prints "getting resulting data" at build time.
- `.collect(*args, **kwargs)` (`flow_frame.py:2446-2450`) simply collects the accumulated **local LazyFrame** — it does not run the graph through core's run machinery or the worker. Returns `pl.DataFrame`.
- `.columns` / `.schema` / `.dtypes` / `.width` are properties over `self.data` (`flow_frame.py:3488-3506`); `columns` calls `collect_schema()` — cheap but not free; several methods (`select`, `filter`, `unique`) read `self.columns` during graph building.

**Things that look lazy but are NOT (verified live):**

1. **Every `write_*`/`sink_*` method executes the write at call time.** `add_output`'s node function (`flow_graph.py:3511-3543`) calls `df.output(..., execute_remote=False)` when `execution_location == "local"` — which `create_flow_graph` always sets. Probe: `df.write_csv(path)` → log "Writing as csv file … Finished writing output" and the file existed **before any collect**. `sink_csv`→`write_csv`, `sink_ipc`→`write_ipc`, etc. `write_parquet`/`write_ipc`/`write_ndjson`/`write_avro` fall back to a polars-code `input_df.sink_parquet(...)` node when extra kwargs are passed (`flow_frame.py:1686-1780`, `_write_simple_file` 1782-1854).
2. **`pivot` materializes its input during graph building** — the native pivot node needs the distinct values of the pivot column to compute the output schema. Probe log: `Reading entire file: ~/.flowfile/cache/1/<uuid>.arrow … Got 2 unique values from external source`. Side effects: real compute plus a cache file under `~/.flowfile/cache/`.
3. **Passthrough methods** (`describe`, `profile`, `fetch`, …) run immediately on the LazyFrame.
4. **`from_dict` / `FlowFrame(python_data)` embed the materialized rows** into the graph as `NodeManualInput.raw_data_format` (`flow_frame_methods.py:606-637`, `flow_frame.py:349-363`) — the full data is serialized into the saved `.flowfile`. Constructing a frame from a `pl.LazyFrame` instead registers a runtime dependency (`add_dependency_on_polars_lazy_frame`) which cannot survive a save/reopen round-trip (**inferred**).
5. The **serialization fallback** and the `lazy_methods` short-circuit execute the actual polars operation at call time to capture a result.

Readers (`read_csv`, `read_parquet`, `scan_*`) are lazy scans; `scan_csv`/`scan_parquet` are literal aliases of `read_csv`/`read_parquet` (`flow_frame_methods.py:712-816`). `read_csv` uses a native `NodeRead`-style `input_schema.ReceivedTable` when parameters fit a long allowlist (`flow_frame_methods.py:163-183`), otherwise generates `pl.scan_csv` code. BytesIO CSV input degrades to `from_dict(pl.read_csv(source))` — fully eager (`flow_frame_methods.py:155-157`).

`cache()` (`flow_frame.py:2616-2620`) sets `cache_results=True` on the node settings (a graph-execution hint) and calls `self.data.cache()`.

---

## 5. The generated-code contract (core sandbox)

Polars-code nodes are executed by `flowfile_core/flowfile_core/flowfile/flow_data_engine/polars_code_parser.py` (`PolarsCodeExecutor`):

- `safe_globals` (`polars_code_parser.py:129-188`): `__builtins__` = `{}`; available names: `pl`, `cs` (= `pl.selectors`), `col`, `lit`, `expr`, **bare Polars dtype names** (`Int64`, `String`, `Datetime`, …), basic builtins (`print, len, range, enumerate, zip, list, dict, set, str, int, float, bool`), `time`, `BytesIO`, `base64`, `datetime`.
- `_validate_code` (`polars_code_parser.py:191-237`): **no `import` statements**, blocked calls (`exec`, `eval`, `getattr`, `open`, …), no dunder attribute access, and **string constants containing `__word__` patterns are rejected**.
- `_wrap_in_function` (`polars_code_parser.py:239-268`): code is wrapped in `_transform(input_df)` / `_transform(input_df_1, …, input_df_N)`; a single-line expression starting with `pl.`/`col(`/`input_df`/`expr(` is returned directly; otherwise the block must assign **`output_df`**.
- Naming convention (mirrored by frame codegen): 1 input → `input_df`; ≥2 inputs → `input_df_1..N` in upstream-discovery order (see `FlowFrame.concat`, `flow_frame.py:2940-2947`, which also dedupes duplicate sources because `add_connection` is idempotent).
- The `#─────SPLIT─────` marker between function definitions and the operation appears **only in flowfile_frame** (grep: no consumer in core) — comments are stripped before execution (`remove_comments_and_docstrings`); it is purely cosmetic for the UI code editor.

**Rule (quoted from `flowfile_frame/CLAUDE.md:27`):** "Source string is load-bearing. … A wrong string breaks graph execution silently."

---

## 6. `open_graph_in_editor` — frame → visual flow (`flowfile/flowfile/api.py`)

`open_graph_in_editor(flow_graph, storage_location=None, module_name="flowfile", automatically_open_browser=True)` (`api.py:399-451`):
1. Copies flow settings, forces `execution_location="local"` + `execution_mode="Development"`, saves via `_save_flow_to_location` (applies layout, `save_flow`; temp dir + `temp_flow_<uuid>.yaml` if no location), then restores original settings (but afterwards sets `flow_settings.path` to the saved file).
2. `start_flowfile_server_process` (`api.py:200-279`): if `GET http://127.0.0.1:63578/docs` (env-overridable `FLOWFILE_HOST`/`FLOWFILE_PORT`) doesn't answer, spawns `flowfile run ui --no-browser` — via `poetry run` when a Poetry env is detected (`FORCE_POETRY`, `POETRY_ACTIVE`, `[tool.poetry]` walk-up), else via the installed console script next to `sys.executable`. Waits up to 60 s; registers `atexit` kill.
3. `POST /auth/token` (empty JSON body) → JWT; `GET /import_flow/?flow_path=…` with Bearer header → flow id.
4. Opens `http://HOST:PORT/ui/flow/{id}` in a browser tab only when the server reports single-mode (`GET /single_mode`) *and* `FLOWFILE_MODE == "electron"` (`_open_flow_in_browser`, `api.py:373-386`).
Returns `bool`. Note: `FlowFrame` has no `open_in_editor` method — the entry point is `from flowfile import open_graph_in_editor; open_graph_in_editor(frame.flow_graph)`.

**Import side effect:** `flowfile/flowfile/__init__.py:17-18` sets `os.environ["FLOWFILE_WORKER_PORT"]="63578"` and `FLOWFILE_SINGLE_FILE_MODE"]="1"` at import time, then re-exports the whole frame API (col/lit/when, readers, connections, selectors) plus core classes — so docs examples that say `import flowfile as ff` work.

---

## 7. DB, cloud, catalog, Kafka, REST connectors

All of these **persist through flowfile_core's storage layer** (the shared SQLite catalog DB), not local files; every helper hard-codes `user_id = 1` ("single-user mode"): `database/connection_manager.py:23-30`, `cloud_storage/secret_manager.py:11-16`, `kafka.py:15`, `rest_api.py:17`, `catalog.py:19`.

- **Database** (`flowfile_frame/flowfile_frame/database/`):
  - `create_database_connection(_if_not_exists)(connection_name, database_type=postgresql|mysql|sqlite|mssql|oracle, host/port/database/username/password, ssl_enabled, url)` → `FullDatabaseConnection` stored via `store_database_connection(db, conn, user_id)` inside `get_db_context()` (`connection_manager.py:33-131`). `get_all_available_database_connections()` returns password-free interfaces; `del_database_connection` also deletes the password `Secret` row.
  - `read_database(connection_name, table_name=… | query=…, schema_name=…, flow_graph=…)` → `NodeDatabaseReader` with `DatabaseSettings(connection_mode="reference", query_mode="table"|"query")` (`frame_helpers.py:22-173`). Query wins if both given.
  - `write_database(df, connection_name, table_name, schema_name=…, if_exists="append"|"replace"|"fail")` — wraps a LazyFrame in a FlowFrame and calls `FlowFrame.write_database` → `NodeDatabaseWriter` (`frame_helpers.py:77-209`; method at `flow_frame.py:2327`).
- **Cloud storage** (`flowfile_frame/flowfile_frame/cloud_storage/`):
  - Connections: `create_cloud_storage_connection(FullCloudStorageConnection)` etc. (`secret_manager.py`).
  - `read_from_cloud_storage(source, file_format=csv|parquet|json|delta, connection_name, scan_mode, delimiter/has_header/encoding, delta_version, output_field_config)` dispatches to `scan_{csv,parquet,json}_from_cloud_storage` / `scan_delta` (`frame_helpers.py:16-92`); writers: `write_to_cloud_storage(df, path, file_format, …, partition_by delta-only)` → `df.write_{csv,parquet,json}_to_cloud_storage`/`write_delta`, all funneling to `add_write_ff_to_cloud_storage` → `NodeCloudStorageWriter` (`frame_helpers.py:150-184`).
- **Catalog** (`catalog.py`, `catalog_reference.py`): `read_catalog_table`, `read_catalog_sql`, `write_catalog_table`, `register_flow_with_catalog`, plus navigable `CatalogReference`/`SchemaReference`/`list_catalogs()`/`default_schema()` that talk to `CatalogService` directly.
- **Kafka** (`kafka.py:72 read_kafka`) and **REST** (`rest_api.py:112 read_api` with auth/pagination coercers) add `NodeKafka…`/REST-reader nodes.

**Import side effect warning:** these modules import `flowfile_core`, whose `__init__` runs `validate_setup()` + `init_db()` — importing `flowfile_frame` **creates and Alembic-migrates the catalog DB** (verified: fresh `FLOWFILE_DB_PATH` got migrations 001→028 applied on import). For any probing/testing, set `FLOWFILE_DB_PATH=<isolated path>` (and optionally `FLOWFILE_SKIP_STARTUP_MIGRATION=1`).

---

## 8. Coverage / parity vs Polars (native node vs code vs missing)

| Operation | Native node condition | Fallback |
|---|---|---|
| `select` | strings + (aliased/cast) Columns only | polars-code `input_df.select([...])` |
| `filter(exprs)` | **never native** — Expr predicates always emit polars-code | `flowfile_formula=` kwarg → native `NodeFilter(advanced)` |
| `sort` | plain cols, no nulls_last/maintain_order, multithreaded | polars-code |
| `join` | equality joins, default suffix, no validate/nulls_equal/coalesce/maintain_order; `cross` native | polars-code join |
| `group_by().agg` | simple exprs, aggs ∈ {sum,max,mean,median,min,count,n_unique,first,last,std,var,concat}, no maintain_order/Selector | polars-code |
| `pivot` | single `on` + single `values`, agg ∈ {first,last,min,max,sum,mean,median,count}; `values` **required** (ValueError if None) | polars-code — **currently broken** (see §10) |
| `unpivot` | plain columns, default variable/value names | polars-code |
| `unique` | string/unaltered-Column subset, no maintain_order | polars-code |
| `concat` | **only** `how="diagonal_relaxed"` + parallel + !rechunk + no duplicate sources → `NodeUnion(mode="relaxed")` | default `"vertical"` goes to polars-code `pl.concat([...])` |
| `with_columns` | all exprs `_ff_repr`-convertible → chain of `NodeFormula` (one node per expr); `flowfile_formulas=` kwarg (optionally auto-upgraded via `polars_expr_transformer.to_flowframe_code` ≥0.5.4, eval'd in a builtins-free namespace, `flow_frame.py:55-107`) | polars-code `input_df.with_columns([...])` |
| `with_row_index` | `name=="record_id"` or (`offset==1` and name≠"index") → `NodeRecordId`; also `cum_count().over()` detection `_detect_cum_count_record_id` (`flow_frame.py:2975`) | polars-code |
| `head`/`limit` | `NodeSample` always | — |
| `rename` | via select(keep_missing) | — |
| everything else on `pl.LazyFrame` (`drop`, `tail`, `slice`, `shift`, `reverse`, `explode`*, `fill_null`, `quantile`, `melt`, …) | injected generic wrapper → polars-code | passthroughs (§4) add no node |

*`explode` and `text_to_rows`, `solve_graph`, `dynamic_rename` have explicit implementations (`flow_frame.py:3237`, `3320`, `2488`, `2511`).

**Flowfile-only extensions** (no Polars equivalent): `filter_split` → `(pass, fail)` frames on output handles 0/1 (`flow_frame.py:1261-1328`); `random_split(splits, seed)` → N frames (`1330-1381`); ML verbs `train_model`/`apply_model`/`evaluate_model`/`wait_for` (`1383-1670`); `fuzzy_join`; `text_to_rows`; `solve_graph` (graph connected-components); `dynamic_rename` (prefix/suffix/formula/first_row); visual grouping `with df.group("name"):` context manager + `set_group` (`2399-2434`, organizational only); `write_catalog_table`, cloud/DB writers; `to_graph`/`save_graph`.

**Known parity deviations:**
- `GroupByFrame.sum/mean/median` aggregate only `cs.numeric()` columns.
- `filter` with multiple predicates AND-joins them into one code node.
- `df.concat` default is graph-friendly polars-code, not `NodeUnion`.
- `Series` is a stub (65 lines); `flowfile_frame` has no eager DataFrame — `LazyFrame = DataFrame = FlowFrame` aliases in `__init__.py:156-157` exist "for compatibility with generated code" and **must not be removed** (frame CLAUDE.md:44).

---

## 9. Stub-generation pipeline (`make stubs` / `make check_stubs`)

Because both `FlowFrame` and `Expr` get most of their methods **injected at runtime**, static type checkers see nothing without stubs. The package ships committed `.pyi` files + `py.typed` (PEP 561) for every module (verified 25 `.pyi` files incl. `database/` and `cloud_storage/`).

- `Makefile:259-266` (`stubs`):
  ```
  poetry run python flowfile_frame/expr_stub_generator.py
  poetry run python flowfile_frame/flow_frame_stub_generator.py
  poetry run python flowfile_frame/submodule_stub_generator.py
  ruff check $(find flowfile_frame/flowfile_frame -name '*.pyi') --select F401 --fix --quiet
  ```
- `expr_stub_generator.py` → `flowfile_frame/expr.pyi`. **Imports the live module** and introspects `Expr/Column/StringMethods/DateTimeMethods/When/ExprNameNameSpace/ExprListNameSpace` plus top-level functions; adds all remaining `pl.Expr` methods as `def m(self, *args, **kwargs) -> 'Expr': ...`, **sorted for deterministic output** (comment at `expr_stub_generator.py:245-247` — unsorted sets caused spurious diffs).
- `flow_frame_stub_generator.py` → `flowfile_frame/flow_frame.pyi`. Introspects the live `FlowFrame` (so injected LazyFrame methods appear) plus module-level functions; **duplicates the `PASSTHROUGH_METHODS` set** (`flow_frame_stub_generator.py:15-32`) — keep it in sync with `lazy_methods.py` when changing passthroughs; rewrites `LazyFrame`→`FlowFrame` in annotations.
- `submodule_stub_generator.py` → a `.pyi` **for every other `.py` in the package, including `__init__.pyi`** (AST-based, no import needed; skips `expr.py`/`flow_frame.py` via `HANDLED_BY_OTHER_GENERATORS` and private modules; flattens `if TYPE_CHECKING:` imports; prunes unused imports; emits `__all__` in `__init__.pyi` for PyCharm). Header on generated files: `# Auto-generated stub for … — do not edit.`
- **Stale doc trap:** `flowfile_frame/readme.md:99` claims "`__init__.pyi` is hand-maintained" — it is **not**; it is generated (verified header) and `flowfile_frame/CLAUDE.md:43` explicitly corrects this.
- Because the first two generators import `flowfile_frame`, **running `make stubs` has the same DB-touching import side effects** as importing the library.
- **CI gate:** `make check_stubs` (`Makefile:269-275`) reruns `stubs` then `git diff --exit-code -- 'flowfile_frame/flowfile_frame/*.pyi' 'flowfile_frame/flowfile_frame/**/*.pyi'`; failure message: "stubs are out of sync with the source. Run 'make stubs' and commit the result." Wired as the `check-stubs` job in `.github/workflows/test.yaml:447-472` (Python 3.11, triggers on `flowfile_frame/**` changes or workflow changes or `run_all_tests`). So **any public-surface change to frame code without regenerating stubs fails CI**.

---

## 10. Live weaknesses / open problems observed

1. **Multi-column pivot fallback is broken** (verified): `df.pivot(on=["c","c2"], index="k", values="v", aggregate_function="sum")` raises at graph-build time — `TypeError: LazyFrame.pivot() got an unexpected keyword argument 'sort_columns'`. The fallback template (`flow_frame.py:2714-2726`) passes DataFrame-only kwargs to a LazyFrame and also assigns to `result` (not `output_df`) with a bare trailing `result` line. Single on/values pivots (native path) work.
2. **`from_dict` single-row list quirk** (observed, scope unconfirmed): `ff.from_dict({"x": [1]})` produced a `list[i64]` column (concat of two such frames yielded rows `[1]`, `[2]` as lists). Likely a `FlowDataEngine(...).to_raw_data()` inference edge for 1-row inputs; multi-row dicts behaved normally in all other probes.
3. `rename(strict=…)` parameter silently ignored (`flow_frame.py:1014-1020`).
4. `Expr.over()` logs a wrong f-string on failure: `logger.warning("Could not create polars expression for over(): {e}")` — placeholder never interpolated (`expr.py:1205`).
5. Serialization fallback for a non-LazyFrame result silently degrades the node to `output_df = input_df` (UI graph lies about the computation), only a `logger.error` hints at it (`flow_frame.py:657-664`).
6. `lazy_methods` wrapper's non-convertible-arg short-circuit (`lazy_methods.py:108-110`) returns a frame built from `(arg.expr for arg in args)` — a generator passed as a single argument; looks defective (**inferred**, not exercised).
7. Root `CLAUDE.md` says migrations run 001–021; the live tree has 001–028 (observed during probe) — root doc drift, relevant only for context.

---

## 11. Tests layout

- `flowfile_frame/tests/` is flat (no package-specific pytest marker; root `pyproject.toml` registers only `worker/core/kernel/docker_integration/kafka`).
- `tests/conftest.py` (32 lines): sets `os.environ['TESTING'] = 'True'` **at import**, then unconditionally deletes/re-creates a cloud connection named `minio-flowframe-test` (s3, `http://localhost:9000`, minioadmin/minioadmin) in the catalog DB at collection time — this only *registers* the connection; MinIO itself is needed only by cloud tests. Docker-gated tests use `tests/utils.py::is_docker_available()` with `@pytest.mark.skipif`.
- Files: `test_flow_frame.py` (934 — core ops, writers round-trips, save_graph), `test_lazy_frame.py` (1545 — injected LazyFrame methods), `test_expressions.py` (901), `test_ff_repr.py` (848 — `_ff_repr` contract + "is this a NodeFormula or NodePolarsCode" assertions, with helper predicates `_is_formula_node`/`_is_polars_code_node`), `test_group_frame.py` (477), `test_joins.py` (333), `test_dynamic_rename.py`, `test_node_groups.py` (visual groups), `test_catalog_reference.py`, `test_catalog_write_partition.py`, `test_cloud_write_partition.py`, `test_evaluate_model.py`, `test_flowfile_frame.py`.
- CI runs `poetry run pytest flowfile_frame/tests --disable-warnings` in the Linux/macOS matrix and the Windows job (`test.yaml:168-174`, `347-354`).
- Per user memory: isolate local runs with `FLOWFILE_DB_PATH` (shared TESTING DB can be dropped by concurrent sessions).

---

## 12. Extend-the-API checklist (what a skill must encode)

1. **Pick the emission path.** Prefer a dedicated core node (`input_schema.NodeXxx` + `flow_graph.add_xxx`) when one exists; use `_add_polars_code` only for genuinely complex cases (mirror the `use_polars_code_path` / `can_use_native` flag pattern). Root rule: `flowfile_frame/CLAUDE.md:28`.
2. **Make the code string executable in the sandbox** (§5): `pl.` prefixes for dtypes when written explicitly (bare dtype names also resolve), no imports/getattr/dunder strings, assign `output_df`, use `input_df` / `input_df_1..N`. Prepend function sources with the `#─────SPLIT─────` convention.
3. **Follow the method skeleton**: `generate_node_id()` → settings (always include `flow_id`, `node_id`, `depending_on_id(s)`, `is_setup=True`, `description`) → `flow_graph.add_*` → `self._create_child_frame(new_node_id)` (single input) or manual connections + `FlowFrame(...)` (multi-input, output handles).
4. **Accept `description: str | None = None`** — every graph method exposes it as the frontend node label.
5. For `Expr` changes: maintain `_repr_str` correctness (this is the contract), propagate `_function_sources`, `convertable_to_code`, `agg_func`, `is_complex`; add `_ff_repr` when a flowfile-formula equivalent exists (unlocks native `NodeFormula`). Explicitly-defined methods win over injected ones; namespaces need their own repr chaining.
6. **Keep `__init__.py` exports and the `LazyFrame = DataFrame = FlowFrame` aliases + Polars dtype re-exports intact** when adding exports.
7. **Regenerate stubs: `make stubs`, commit the `.pyi` diffs; `make check_stubs` is the CI gate.** If you changed passthrough sets, update the copy in `flow_frame_stub_generator.py` too.
8. Test both the *result* (`.collect()` vs a `pl` reference implementation) and the *graph shape* (node type / generated code), like `test_ff_repr.py` does. Round-trip through `save_graph` + re-open for anything UI-visible.
9. Run `poetry run pytest flowfile_frame/tests` (isolated `FLOWFILE_DB_PATH`) and `poetry run ruff check flowfile_frame`.

---

## 13. Verified commands (all run in this session)

```bash
# run the frame test suite (isolate the catalog DB!)
FLOWFILE_DB_PATH=/tmp/frame_test.db poetry run pytest flowfile_frame/tests --disable-warnings

# regenerate stubs (imports the library — touches/migrates the catalog DB unless isolated)
make stubs
# CI drift gate: regenerates then fails on git diff of flowfile_frame/**/*.pyi
make check_stubs

# individual generators
poetry run python flowfile_frame/expr_stub_generator.py
poetry run python flowfile_frame/flow_frame_stub_generator.py
poetry run python flowfile_frame/submodule_stub_generator.py            # add --module flowfile_frame.selectors to restrict

# quick graph-shape probe (verified output shown in §2/§4)
TESTING=True FLOWFILE_DB_PATH=/tmp/probe.db poetry run python - <<'EOF'
import flowfile_frame as ff
df = ff.from_dict({"a": [1,2,3], "b": ["x","y","z"]})
out = df.filter(ff.col("a") > 1).with_columns((ff.col("a")*2).alias("dbl")).select("a","dbl").sort("a", descending=True)
for n in out.flow_graph.nodes:
    print(n.node_id, n.node_type, type(n.setting_input).__name__)
print(out.collect())
EOF
```

Probe results (abridged): node chain `manual_input → polars_code(filter) → formula(with_columns via _ff_repr) → select → sort`; `write_csv` wrote its file before any collect; native pivot materialized input to `~/.flowfile/cache/`; multi-on pivot fallback raised `TypeError`; stdin lambda → base64-LazyFrame node; file lambda → named `_lambda_fn_*` definition node.
