# CLAUDE.md - flowfile_frame

A Polars-like programmatic Python API that builds Flowfile ETL graphs under the hood. Package-specific notes; see the root `/CLAUDE.md` for monorepo-wide setup, build, lint, ports, and cross-package contracts.

## Role
`flowfile_frame` (imported as `import flowfile_frame as ff`) is a Python library giving a Polars-`LazyFrame`-like surface (`FlowFrame`, `Expr`/`Column`). Every method call appends a node to an **in-process `FlowGraph`** instead of (only) computing data.

It is **NOT independent of the backend**: it imports `flowfile_core` directly (`flowfile_core.flowfile.flow_graph.FlowGraph`, `flowfile_core.schemas.*`, `flowfile_core.database.connection`) to construct/mutate the same DAG the Designer UI and core executor use. It runs in-process — no HTTP, no ports. The graph it builds can be saved (`.flowfile` via `save_graph`) and opened in the Designer, or executed via `.collect()`.

Runtime contract: each `FlowFrame`/`Expr` carries a `_repr_str` — a literal Polars source-code string. Complex operations are serialized into graph nodes via the `_add_polars_code` path in `flow_frame.py` (which calls `flow_graph.add_polars_code`); simple ones map to dedicated node settings (`add_sort`, `add_manual_input`, etc.). The generated string must be valid, executable Polars code, since core re-evaluates it.

## Layout
- `flowfile_frame/flow_frame.py` — `FlowFrame` class; node emission, `collect`, `save_graph`, `to_graph`, writers.
- `flowfile_frame/expr.py` — `Expr`, `Column`, `When`, `col`/`lit`/`when`/aggregations; `_repr_str` machinery, `.str`/`.dt` namespaces. `add_expr_methods(Expr)` is invoked at module end.
- `flowfile_frame/flow_frame_methods.py` — module-level constructors/readers: `from_dict`, `from_raw_data`, `read_csv`, `read_parquet`, `read_excel`, `scan_csv`/`scan_parquet`/`scan_delta`, `concat`, cloud scans.
- `flowfile_frame/lazy_methods.py` — `add_lazyframe_methods` decorator (applied to `FlowFrame`) + `PASSTHROUGH_METHODS` (e.g. `collect`, `schema`, `columns` delegate to the cached Polars object).
- `flowfile_frame/adding_expr.py` — `add_expr_methods` decorator injecting Polars-mirroring methods onto `Expr`.
- `flowfile_frame/selectors.py`, `expr_name.py`, `list_name_space.py` — selectors and `.name`/`.list` namespaces.
- `flowfile_frame/group_frame.py`, `join.py`, `series.py`, `lazy.py` — group-by, join inputs, `Series`, `fold`.
- `flowfile_frame/database/` — DB connection helpers + `read_database`/`write_database` (`database/frame_helpers.py`); delegates to `flowfile_core.database` / core's connection manager.
- `flowfile_frame/cloud_storage/` — S3/cloud connection helpers + `read_from_cloud_storage`/`write_to_cloud_storage` (`cloud_storage/frame_helpers.py`).
- `flowfile_frame/catalog.py`, `catalog_reference.py`, `kafka.py`, `rest_api.py` — catalog I/O, Kafka, REST sources.
- `flowfile_frame/callable_utils.py`, `utils.py` — lambda-source extraction; `create_flow_graph()` factory that seeds a fresh `FlowGraph` (local execution, history off).
- `*_stub_generator.py` (package root) — three generators that introspect source to emit `.pyi` stubs.

## Key patterns & conventions
- **Source string is load-bearing.** When adding/altering an `Expr` or `FlowFrame` method, ensure `_repr_str` builds correct Polars code (use `pl.` prefixes for dtypes/classes, mirror Polars signatures). A wrong string breaks graph execution silently.
- **Two emission paths.** Prefer a dedicated core node setting when one exists; fall back to `_add_polars_code` only for genuinely complex expressions (`use_polars_code_path` flags in `flow_frame.py`).
- **Methods are injected**, not all hand-written: `FlowFrame` is decorated with `@add_lazyframe_methods`; `Expr` via `add_expr_methods(Expr)` at module end. Passthrough methods (`PASSTHROUGH_METHODS`) just delegate to the cached Polars object; explicitly-defined methods (e.g. `collect`) are never overwritten by the decorator.
- **Polars re-exports.** `__init__.py` re-exports Polars dtypes and aliases `LazyFrame = DataFrame = FlowFrame` for generated-code compatibility — keep these when touching exports.
- Follow root-doc rules: Polars only (never pandas); imports ordered stdlib → third-party → first-party.

## Running / entry points
Library only — no server/CLI of its own. Usage: `import flowfile_frame as ff`, build a graph, then `frame.collect()` (returns `pl.DataFrame`), `frame.save_graph("p.flowfile")`, or `open_graph_in_editor(frame.flow_graph)` (from `flowfile.api` in the `flowfile` package). Backend modules load in the same process; no port is bound.

## Testing
```bash
poetry run pytest flowfile_frame/tests
```
Tests live in `tests/` (`test_flow_frame.py`, `test_expressions.py`, `test_ff_repr.py`, `test_joins.py`, etc.). `tests/conftest.py` sets `TESTING=True` and provisions a MinIO cloud connection (`minio-flowframe-test`, expects MinIO on `localhost:9000`). Docker-dependent tests use `@pytest.mark.skipif(not is_docker_available(), ...)` (helper in `tests/utils.py`). There is no `frame`-specific pytest marker in root `pyproject.toml` (see `[tool.pytest.ini_options]` for the registered set).

## Gotchas
- **Stub gate.** Public surface of `FlowFrame`/`Expr`/submodules ships committed `.pyi` stubs (+ `py.typed`). After any public API change run `make stubs` (root) and commit; `make check_stubs` is the CI gate that fails on drift. All three generators run from `make stubs`: `expr_stub_generator.py` → `expr.pyi`, `flow_frame_stub_generator.py` → `flow_frame.pyi`, and `submodule_stub_generator.py` → every other `.pyi` **including `__init__.pyi`** (it walks all `.py`, excluding only `expr.py`/`flow_frame.py`). `make stubs` then runs `ruff --select F401 --fix` to prune unused imports.
- Don't break the `LazyFrame`/`DataFrame` aliases or Polars dtype re-exports in `__init__.py` — generated flow code depends on them.
- Per the user memory rule, core must not `.collect()` LazyFrames internally; `FlowFrame.collect()` here is the intended user-facing materialization point.
- DB/cloud/catalog helpers persist via `flowfile_core` (`get_db_context`, core's connection manager) — they touch core's storage, not a local file.

## Key files
- `flowfile_frame/flow_frame.py` — `FlowFrame`, the central graph-building class (large; read selectively).
- `flowfile_frame/expr.py` — column expression system + `_repr_str` (large).
- `flowfile_frame/__init__.py` — public API surface, Polars re-exports, `LazyFrame`/`DataFrame` aliases.
- `flowfile_frame/lazy_methods.py` — `add_lazyframe_methods`, `PASSTHROUGH_METHODS`.
- `flowfile_frame/flow_frame_methods.py` — module-level constructors/readers.
- `flowfile_frame/adding_expr.py` — `add_expr_methods` decorator.
- `flowfile_frame/utils.py` — `create_flow_graph()` factory.
- `flowfile_frame/database/connection_manager.py` / `database/frame_helpers.py` — DB connections + read/write (delegate to core).
- `flowfile_frame/cloud_storage/frame_helpers.py` — cloud read/write.
- `flow_frame_stub_generator.py` / `expr_stub_generator.py` / `submodule_stub_generator.py` (package root) — `.pyi` generators (run via `make stubs`).
- `tests/conftest.py` — sets `TESTING`, provisions MinIO connection. `tests/utils.py` — `is_docker_available()`.
- `readme.md` — quick-start + type-stub workflow.
