# Flowfile: Planned Features

This document describes nine features planned for future versions of Flowfile. Each section covers the motivation behind the feature, the current state of the codebase relative to it, and the proposed design and implementation path.

---

## 1. Iterative Nodes

### Motivation

Many real-world ETL pipelines require looping: paginating through an API, processing files in a directory one by one, expanding rows into sub-flows, or repeatedly applying a transformation until a convergence criterion is met. Today, Flowfile only supports a strict DAG execution model where every node runs exactly once. Adding iteration would unlock a large class of pipelines that currently must be handled outside Flowfile entirely.

### Current State

The execution engine (`flowfile_core/flowfile_core/flowfile/flow_graph.py`) uses a topological sort to produce `ExecutionStage` objects, each containing a batch of nodes that can run in parallel. The `NodeExecutor` in `flow_node/executor.py` wraps each node and makes a single `ExecutionDecision` (SKIP, FULL_LOCAL, LOCAL_WITH_SAMPLING, or REMOTE). There is no concept of "run this node again with the next element."

### Proposed Design

**New node types:**

- `ForEach` — accepts a list-typed column or an external iterable as input; emits one frame per element to a downstream sub-flow.
- `While` — re-executes a sub-flow until a boolean expression evaluates to false or a max-iteration limit is reached.
- `Collect` — serves as the closing boundary of an iterative sub-flow, accumulating results from each iteration and unioning them into a single output frame.

**Execution changes:**

- `ExecutionDecision` gains an `iteration_context` field carrying the current item and iteration index.
- `compute_execution_plan` in `execution_orderer.py` is extended to detect `ForEach`/`While` boundaries and wrap the enclosed nodes into an `IterativeStage`.
- `IterativeStage.execute()` drives the loop, calling `NodeExecutor.run()` for each stage inside the boundary on every iteration.
- A max-iteration guard and an elapsed-time timeout prevent runaway loops.

**State isolation:** Each iteration receives a fresh copy of the input frame and its own cache key derived from `(node_id, iteration_index)`. This allows partial re-runs: if the flow is re-triggered mid-iteration, completed iterations are skipped via the existing cache-invalidation logic.

**Code generation:** The code generator (`code_generator.py`) emits a Python `for` or `while` loop wrapping the Polars operations for the enclosed nodes, with a `pl.concat(results)` at the `Collect` boundary.

---

## 2. Conditional Execution

### Motivation

A flow that branches based on data content or a runtime parameter — "if the source file exists, read it; otherwise pull from the API" — cannot currently be expressed in Flowfile. Conditional execution would allow flows to model decision logic directly rather than requiring users to split their pipeline into multiple separate flows.

### Current State

The topological sort in `execution_orderer.py` assumes a pure DAG with no branch semantics: every downstream node runs whenever its upstream nodes complete successfully. `NodeExecutor._decide_execution()` is the single point of authority for whether a node runs, but it only considers cache validity and upstream availability — not user-defined conditions.

### Proposed Design

**New node types:**

- `Condition` — takes one data frame input plus a boolean expression (defined via the existing formula editor). Produces two output ports: `true` and `false`.
- `Merge` — accepts frames from two branches and passes whichever branch was activated downstream (acts as a join point after a conditional).

**Execution changes:**

- `ExecutionDecision` gains a `branch_active: bool | None` field. When `None`, execution proceeds normally. When `False`, the node and all of its exclusive downstream nodes are marked `SKIP`.
- `compute_execution_plan` identifies `Condition` nodes and tags the subgraph reachable exclusively through each port as belonging to that branch.
- Before executing a stage, the engine evaluates whether any `Condition` ancestor has resolved the current node's branch as inactive and sets `SKIP` accordingly.

**Short-circuit semantics:** The inactive branch is skipped entirely — no sampling, no schema inference. This is important for nodes that have side effects (e.g., writing files).

**Code generation:** The code generator emits an `if / else` block. The boolean expression from the `Condition` node is evaluated as a Python scalar (via `.item()` on a single-row frame) and controls which branch is executed.

---

## 3. Delta Lake Catalog Storage

### Motivation

Flowfile's catalog currently stores artifacts as Parquet files on the local filesystem or object storage. Delta Lake would provide ACID transactions, time-travel (versioned snapshots), schema evolution, and efficient upserts — capabilities that are critical when multiple flows write to the same table concurrently or when users need to audit historical data.

### Current State

The catalog service (`flowfile_core/flowfile_core/catalog/service.py`) is intentionally backend-agnostic: it operates through a `CatalogRepository` protocol, making the storage layer pluggable. The cloud storage schemas (`schemas/cloud_storage_schemas.py`) and the code generator already handle Delta Lake reads from cloud storage (`delta` format is a recognized `StorageFormat`). However, the catalog repository itself only manages Parquet, and there is no Delta-specific metadata surface.

### Proposed Design

**Storage backend:**

- Add a `DeltaCatalogRepository` that implements the `CatalogRepository` protocol using `deltalake` (the Rust-backed Python Delta Lake library).
- Each catalog table maps to a Delta table at a configured base path (local or S3/GCS/Azure).
- Writes use `write_deltalake()` with `mode="append"` or `mode="overwrite"` based on flow configuration.
- Schema evolution (`schema_mode="merge"`) is enabled by default so that column additions do not break existing consumers.

**Versioning and time travel:**

- `CatalogTable` gains optional `version` and `as_of_timestamp` fields.
- `CatalogService.read_artifact(table_id, version=N)` materializes a specific snapshot as a Polars `LazyFrame` using the Delta log.
- The UI exposes a version timeline on the catalog table detail page.

**Configuration:**

- A new `FLOWFILE_CATALOG_BACKEND` environment variable selects `parquet` (default) or `delta`.
- Delta-specific settings (`FLOWFILE_DELTA_STORAGE_URI`, credential keys) follow the existing secrets pattern (Fernet-encrypted in the database).

**Code generation:** When the code generator encounters a catalog read node, it emits `pl.read_delta(path, version=N)` instead of `pl.read_parquet()`.

---

## 4. Flow Parameters

### Motivation

Flows often contain values that change between runs — date ranges, file paths, threshold values, API keys. Today these must be hard-coded inside node settings or managed via separate configuration files. Flow Parameters would make flows reusable templates: the same flow definition can be run with different inputs without any structural changes.

> **Note:** Active development is occurring on the `feature/add-flow-parameters` branch.

### Current State

`FlowGraph` holds node settings as static Pydantic models serialized to JSON. There is no notion of a run-time variable that is resolved at execution time. The code generator and executor both treat all settings as constants.

### Proposed Design

**Parameter definition:**

- A new `FlowParameters` model is attached to `FlowGraph` as `flow_graph.parameters: list[FlowParameter]`.
- `FlowParameter` has fields: `name`, `type` (string, integer, float, boolean, date), `default_value`, and `description`.
- Parameters are created and edited in a dedicated "Parameters" panel in the flow editor sidebar.

**Parameter reference:**

- Any text field in a node's settings can reference a parameter using `${param_name}` syntax.
- At execution time, `FlowGraph.resolve_parameters(overrides: dict)` walks all node settings, substitutes placeholders, and returns a resolved copy of the graph — leaving the original definition unchanged.

**New node type — `ParameterInput`:**

- A source node with no data inputs that emits a single-row, single-column frame containing the parameter's value.
- This allows parameters to flow into nodes that consume a data frame rather than a settings field (e.g., as a filter value fed into a `Join`).

**API surface:**

- `POST /flows/{flow_id}/run` accepts an optional `parameters` JSON body to override defaults for that run.
- The CLI (`flowfile run`) gains a `--param name=value` flag.

**Code generation:** The code generator emits the parameters as a `params` dict at the top of the generated script, making it trivial to override them when running the script directly.

---

## 5. Catalog Query and Data Exploration

### Motivation

The Flowfile catalog registers output artifacts from every flow run, but users currently have no way to explore or query that data without building a new flow. A built-in SQL interface and a visual exploration surface (powered by GraphicWalker) would let analysts answer ad-hoc questions against catalog tables without leaving Flowfile.

### Current State

- `CatalogService` can list tables and retrieve schemas but has no query method.
- `graphic_walker.py` already converts Flowfile column schemas to GraphicWalker `MutField` definitions and can populate a `DataModel` from a `FlowDataEngine` instance — but this is only wired up inside the flow editor for in-flow data previews, not for catalog exploration.
- There is no SQL execution surface in the catalog service or its API routes.

### Proposed Design

**SQL query endpoint:**

- `POST /catalog/query` accepts a SQL string and executes it against the catalog using DuckDB's `read_parquet()` (or `read_delta()` if Delta backend is active) virtual tables.
- Catalog table names are resolved to storage paths at query time by the `CatalogService`.
- Results are paginated and returned as the standard Flowfile column/data payload.
- A read-only SQL editor (CodeMirror 6, already present in the frontend) is added to the catalog view.

**GraphicWalker exploration panel:**

- A "Explore" button on each catalog table detail page opens an exploration panel.
- The panel loads the table schema via `CatalogService.get_table_schema()`, initialises a GraphicWalker instance using the existing `get_initial_gf_data_from_ff()` utility, then streams a sample of rows (configurable, default 50 000) via `get_gf_data_from_ff()`.
- Users can drag dimensions and measures to build charts without writing any code.

**Performance:**

- Large tables are sampled using stratified row sampling via Polars `sample()` before being sent to the frontend.
- A future enhancement would push aggregations down to DuckDB so that only aggregated results are transferred.

---

## 6. Extended Connectors

### Motivation

Flowfile currently supports flat files (CSV, Parquet, Excel, JSON, NDJSON) and cloud object storage. Most enterprise data lives in relational databases — PostgreSQL, MySQL, SQL Server, BigQuery, Snowflake. First-class database connectors would significantly expand Flowfile's reach as an ETL tool.

### Current State

The schema layer (`schemas/`) has partial SQL source models. The code generator handles cloud storage reads and writes, and the worker service has isolated compute capacity, but there is no general RDBMS connector. Connector settings are modelled per-source rather than through a shared interface.

### Proposed Design

**Connector abstraction:**

- A `DatabaseConnector` base class with three required methods: `test_connection()`, `read_table(table, schema, query)`, and `write_table(frame, table, schema, mode)`.
- Each supported database (PostgreSQL, MySQL, SQL Server, BigQuery, Snowflake) is a subclass that provides a SQLAlchemy dialect URI and any dialect-specific options.
- Connector credentials are stored encrypted via the existing Fernet secrets system.

**New node types:**

- `DatabaseReader` — connects to a database, runs a SQL query or reads a full table, and outputs a Polars `LazyFrame` via `pl.read_database_uri()`.
- `DatabaseWriter` — writes a frame to a target table with configurable write modes (append, overwrite, upsert by key columns).

**Connection manager:**

- A new "Connections" section in the Flowfile settings UI allows users to define, test, and name database connections.
- Connections are referenced by name in node settings, decoupling credentials from flow definitions.
- Connection objects are stored in the existing secrets store.

**Code generation:**

- `DatabaseReader` nodes emit `pl.read_database_uri(query, uri)` with the URI constructed from the named connection's credentials.
- `DatabaseWriter` nodes emit `frame.write_database(table, uri, if_table_exists=mode)`.

---

## 7. Standardized Custom Node Designer

### Motivation

Flowfile already supports custom nodes authored in Python, but the developer experience for creating them is fragmented. There is no guided UI for declaring inputs, outputs, and settings; the kernel code generation is basic; and there is no in-app testing surface. Standardizing the custom node designer would lower the barrier for domain experts to extend Flowfile without deep framework knowledge.

### Current State

`custom_node.py` (`flowfile_core/flowfile_core/flowfile/node_designer/custom_node.py`) is actually quite complete under the hood:

- `CustomNodeBase` provides a rich base class with standardized properties (name, category, icon, input/output counts).
- `NodeSettings`, `Section`, and `FlowfileInComponent` form a declarative settings schema system.
- `SectionBuilder` and `NodeSettingsBuilder` provide a fluent builder API.
- `generate_kernel_code()` wraps the user's `process()` method for sandboxed kernel execution, auto-generating settings proxy classes and output publishing.
- `to_frontend_schema()` produces the JSON consumed by the Vue settings panel.

The gap is that this API is only accessible by writing Python code directly — there is no UI for it.

### Proposed Design

**In-app node designer wizard:**

- A multi-step wizard in the Flowfile UI guides the user through:
  1. **Identity** — node name, icon, category.
  2. **Inputs/Outputs** — number and names of data frame ports.
  3. **Settings** — drag-and-drop section and component builder backed by `SectionBuilder`.
  4. **Code** — a CodeMirror editor pre-populated with a `process()` stub that already references the declared settings via `self.settings_schema`.
  5. **Test** — run the node inline against a sample frame without leaving the wizard.

**Live schema preview:**

- As the user adds settings components, the right panel shows a live preview of the node settings panel exactly as it will appear in the flow editor.

**Packaging and sharing:**

- Nodes can be exported as a single `.py` file or as a zip containing the file plus any `requirements.txt` dependencies.
- A future community registry (out of scope here) could allow importing shared custom nodes by URL.

**Kernel execution improvements:**

- The generated kernel code is upgraded to handle: multi-output nodes, exception propagation back to the UI, dependency injection for secrets, and progress reporting.

---

## 8. Flow as Custom Node

### Motivation

Composition is a key principle of scalable pipeline design. Being able to wrap an entire Flowfile flow as a reusable custom node — with the flow's `ParameterInput` nodes becoming the node's settings and its output nodes becoming the node's ports — would enable hierarchical flows and the creation of reusable building blocks without any additional coding.

### Current State

The code generator can export a flow as a standalone Python script, and `CustomNodeBase` provides the framework for user-defined nodes. However, there is no mechanism to automatically wrap a `FlowGraph` as a `CustomNodeBase` subclass, and there is no UI concept of "this node is itself a flow."

### Proposed Design

**Export mechanism:**

- A new `FlowGraphToCustomNodeConverter` in the code generator module accepts a `FlowGraph` and produces a `CustomNodeBase` subclass.
- The converter performs a structural analysis of the flow:
  - `ParameterInput` nodes → `FlowfileInComponent` settings fields (matching types).
  - Nodes with no upstream connections and type `external_source` → data frame input ports.
  - `WriteData` or terminal nodes with named outputs → data frame output ports.
- The generated class embeds the serialized flow definition as a class attribute and executes it via `FlowGraph.run_with_overrides()` inside `process()`.

**UI integration:**

- A "Package as Node" button in the flow editor header triggers the export.
- The resulting `.py` file is automatically installed as a custom node in the current Flowfile instance.
- The packaged node appears in the node palette under a "Flows" category.

**Nesting and recursion guard:**

- The execution engine detects and rejects circular dependencies (a flow that contains itself as a node).
- Nested flows respect the parent flow's parameter overrides (parameters propagate through the nesting hierarchy).

**Versioning:**

- Each packaging action captures the exact flow definition at that point in time (a snapshot), so updating the underlying flow does not silently break users of the packaged node. Explicit re-packaging is required to update the node.

---

## 9. Enhanced Code Generation

### Motivation

The code generator is one of Flowfile's most powerful features: it converts a visual flow into a standalone, dependency-minimal Python script. However, it currently cannot handle several important node types, and the output lacks two critical capabilities: reading from and writing to the Flowfile catalog, and wrapping code in the kernel execution format used by `kernel_runtime`.

### Current State

`code_generator.py` uses a handler pattern (`_handle_{node_type}`) and already supports CSV/Parquet/cloud storage reads, filter/select/join/formula transforms, and basic writes. Unsupported nodes are tracked and surfaced as errors. Delta Lake cloud storage reads are partially supported.

**Known gaps:**
- No catalog read/write handlers.
- No conditional or iterative node handlers (planned in features 1 and 2).
- No kernel wrapping — generated scripts run as plain Python but cannot be submitted to `kernel_runtime`.
- Custom node code generation is partial; dependencies are not automatically collected.

### Proposed Design

**Catalog read/write handlers:**

- `_handle_catalog_read(node)` emits a `pl.read_parquet(catalog.resolve_path(table_id))` call, with the `catalog` object imported from `flowfile.catalog_client`.
- `_handle_catalog_write(node)` emits a `frame.write_parquet(...)` or `write_deltalake(...)` call depending on the configured backend.
- A lightweight `catalog_client` stub is generated inline (or as an import) so that generated scripts can resolve table paths without the full Flowfile backend running.

**Kernel wrapping:**

- A new `generate_kernel_script(flow_graph)` function wraps the standard generated code in the protocol expected by `kernel_runtime/kernel_runtime/main.py`:
  - Imports `flowfile` and calls `flowfile.publish_output(name, frame)` for each terminal node.
  - Injects a `__flowfile_params__` dict at the top of the script for parameter injection.
  - Adds the standard kernel bootstrap boilerplate (`if __name__ == "__main__": ...`).

**Conditional and iterative code generation:**

- Once Conditional Execution (feature 2) and Iterative Nodes (feature 1) are implemented, corresponding handlers are added here:
  - `_handle_condition(node)` → `if / else` block.
  - `_handle_for_each(node)` → `for item in ...:` loop with a `results` accumulator.
  - `_handle_collect(node)` → `pl.concat(results)`.

**Dependency collection:**

- The generator accumulates `import` statements as it processes nodes. A final pass deduplicates and sorts them (stdlib, third-party, first-party — matching the project's Ruff isort configuration).
- Custom nodes annotate their dependencies via a `requirements: list[str]` class attribute; the generator collects these and can emit a companion `requirements.txt`.

**Validation and testing:**

- `FlowGraphToPolarsConverter` gains a `dry_run()` method that performs the full conversion without writing to disk, returning a list of warnings and unsupported nodes. This powers a "Check code generation compatibility" UI button.
