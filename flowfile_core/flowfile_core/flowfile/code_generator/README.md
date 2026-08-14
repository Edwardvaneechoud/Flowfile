# `code_generator`

Converts a `FlowGraph` (the in-memory DAG behind a visual flow) into runnable,
hand-written-looking Python. Three output shapes:

- **a standalone script** — Polars (`export_flow_to_polars`) or FlowFrame
  (`export_flow_to_flowframe`), one `run_etl_pipeline()` function.
- **a multi-file project** — `export_flow_to_project` (zip via
  `project_to_zip_bytes`), with python-script nodes split into runnable modules.
- **a teaching script** — `export_flow_to_plain_python`, no dataframe library at
  all (lists, dicts, loops). Not part of the parity campaign; see below.

The public surface is re-exported from `__init__.py`.

## How it works

`FlowGraphCodeConverter` walks nodes in topological order, dispatches each to a
`_handle_<node_type>` handler that emits provisional `df_<id> = …` statements,
then a **render pass** fuses linear single-use chains into one piped expression
and renames the surviving boundary variables to operation labels
(`source`/`filtered`/`joined`/…). The pass is conservative: anything it doesn't
confidently recognize stays a named statement, so output degrades to "less
pretty", never "wrong". The round-trip tests (exec the generated code,
`assert_frame_equal` against the engine) are the correctness net.

## Files

| File | Responsibility |
|------|----------------|
| `code_generator.py` | Orchestration: node dispatch, chain fusion, boundary naming, final-code assembly. Holds the base `FlowGraphCodeConverter` plus the `…ToPolarsConverter` / `…ToFlowFrameConverter` subclasses and the `export_flow_to_*` entry points. |
| `base.py` | `ConverterMixinBase` — type-only (`TYPE_CHECKING`) declaration of the converter state/helpers the handler mixins share, so cross-class `self.*` references resolve for static checkers. No runtime effect. |
| `join_handlers.py` | `JoinHandlersMixin` — standard / semi-anti / cross joins, join-key transforms, post-join processing. |
| `transform_handlers.py` | `TransformHandlersMixin` — row/column transforms (group_by, formula, pivot, sort, window, fuzzy match, record_id, …). |
| `connector_handlers.py` | `ConnectorHandlersMixin` — external connectors (cloud storage, Kafka, database, REST API, catalog readers/writers). |
| `custom_node_handlers.py` | `CustomNodeHandlersMixin` — user-defined node source registration and call emission. |
| `expression_helpers.py` | `ExpressionHelpersMixin` — filter-expression parsing and Polars dtype / aggregation mapping. |
| `chain_fusion.py` | Pure string/graph fusion pass (`render_pipeline`); no flow imports, unit-testable in isolation. |
| `project_exporter.py` | `FlowGraphToProjectConverter` — emits a multi-file project tree (`pipeline.py`, `main.py`, per-node notebooks, custom-node modules, scaffolding) instead of one script. |
| `project_shim.py` | Standalone `flowfile_ctx` shim shipped inside exported projects so python-script node code runs unchanged outside Flowfile's kernel. |
| `plain_python.py` | `FlowGraphToPlainPythonConverter` — the teaching flavour: derived node allowlist, exercise stubs, fusion disabled, plus the per-node explainer (`explain_node_in_plain_python`) behind `GET /editor/explain_node`. |
| `plain_python_handlers.py` | `PlainPythonHandlersMixin` — the loop-based emitters. Every table is a `list[dict]`. |

The handler logic is split across mixins purely to keep files focused; all mixins
compose into `FlowGraphCodeConverter`, so a handler can call any other handler or
helper via `self`.

## Adding a node type

Add a `_handle_<node_type>` to the mixin that fits its category (override it in
the Polars/FlowFrame subclass if the two frameworks differ), give it a label in
`NODE_TYPE_VAR_LABEL`, and add a round-trip test parametrized over both export
frameworks.

Dispatch goes through `_resolve_handler(node)`; a converter that only covers part
of the catalogue narrows that method so an uncovered type reaches
`_handle_unsupported` instead of silently inheriting a handler for the wrong
framework.

## The plain-Python flavour

Two properties keep it honest, and both are load-bearing:

- `PLAIN_PYTHON_NODE_TYPES` is **derived** from the `_handle_*` methods that exist
  on `PlainPythonHandlersMixin` — never hand-written. A hand-maintained allowlist
  that drifted ahead of the emitters would let an inherited Polars handler print
  `pl.` into a script advertised as dependency-free. `test_code_generator_plain_python.py`
  asserts no output ever contains `pl.`, `import polars` or `import flowfile`.
- An inexpressible node emits an **exercise stub** (a function that raises
  `NotImplementedError`) rather than raising `UnsupportedNodeError` for the whole
  flow or silently passing rows through. Emitters signal this by raising
  `PlainPythonUnsupported`.

Chain fusion is off — you cannot fuse two `for` loops into a piped expression, and
one block per node is what keeps the script legible against the canvas.

This flavour is **exempt from the codegen parity campaign**: it is a one-way
teaching artifact and is not expected to round-trip.
