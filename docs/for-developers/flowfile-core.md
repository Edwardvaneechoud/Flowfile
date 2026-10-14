# Flowfile Core: A Developer's Guide

This guide walks `flowfile-core`'s architecture from the outside in — building a small pipeline in Python step by step, then explaining how the graph decides where each node runs.

!!! info "Looking for something else?"
    - **[Python API Reference](../users/python-api/index.md)**: to USE Flowfile
    - **[Design Philosophy](design-philosophy.md)**: to understand WHY Flowfile works this way
    - **This page**: to understand HOW Flowfile works internally

---


## The Core Architecture

At its heart, `flowfile-core` is composed of three main objects:

1.  **`FlowGraph`**: The central orchestrator. It holds your pipeline, manages the nodes, and controls the execution flow.
2.  **`FlowNode`**: An individual step in your pipeline. It's a wrapper around your settings and logic, making it an executable part of the graph.
3.  **`FlowDataEngine`**: The data itself, which flows between nodes. It's a wrapper around a [Polars LazyFrame](https://pola-rs.github.io/polars/py-polars/html/reference/lazyframe/index.html), carrying both the data and its schema.

Let's see these in action.

---

## 1. The FlowGraph: Your Pipeline Orchestrator

Everything starts with the `FlowGraph`. It is the in-memory representation of a pipeline — the same object the canvas edits.

Let's create one:

```python
from flowfile_core.flowfile.flow_graph import FlowGraph
from flowfile_core.schemas.schemas import FlowSettings

# Initialize the graph with some basic settings
graph = FlowGraph(
    flow_settings=FlowSettings(
        flow_id=1,
        name="My First Pipeline"
    )
)

print(graph)
```

<details markdown="1">
<summary>Output of <code>print(graph)</code></summary>

```
FlowGraph(
Nodes: {}                                #<-- An empty dictionary. No nodes yet!

Settings:                                #<-- The FlowSettings object you provided.
  -flow_id: 1                            #<-- A unique ID for this flow.
  -description: None                     #<-- An optional description.
  -save_location: None                   #<-- Where the flow definition is saved.
  -name: My First Pipeline               #<-- The name of our flow.
  -path:                                 #<-- Path to the flow file.
  -execution_mode: Development           #<-- 'Development' for debugging or 'Performance' for speed.
  -execution_location: auto              #<-- Where the flow runs: 'auto' (the default backend) or a backend id.
  -auto_save: False                      #<-- Auto-save changes (feature in development).
  -modified_on: None                     #<-- Last modified timestamp (feature in development).
  -show_detailed_progress: True          #<-- If True, shows detailed logs in the UI.
  -is_running: False                     #<-- Is the flow currently running?
  -is_canceled: False                    #<-- Was a cancellation requested?
)
```
</details>

```python 
print(graph.run_graph())
# flow_id=1 start_time=datetime.datetime(...) end_time=datetime.datetime(...) success=True nodes_completed=0 number_of_nodes=0 node_step_result=[]

```


It runs successfully but does nothing, as expected. The FlowGraph's job is to:

* **Contain** all the nodes.
* **Manage** the connections between them.
* **Calculate** the optimal execution order.
* **Orchestrate** the entire run lifecycle.


Let's give it a node to manage.

## 2. Adding a Node: Where Settings Come to Life
You don't add raw functions or data directly to the graph. Instead, you provide **settings objects** (Pydantic models). The graph turns these settings into executable `FlowNodes`.

```python
from flowfile_core.schemas import input_schema

# 1. Define your data using a settings object.
# This is just a Pydantic model holding configuration.
manual_input_settings = input_schema.NodeManualInput(
    flow_id=1,
    node_id=1,
    raw_data_format=input_schema.RawData.from_pylist([
        {"name": "Alice", "age": 30},
        {"name": "Bob", "age": 25}
    ])
)

# 2. Add the settings to the graph.
graph.add_manual_input(manual_input_settings)
```
So, what did the graph just do? It didn't just store our settings. It created a FlowNode.

```python
# Let's retrieve the object the graph created
node = graph.get_node(1)

print(type(node))
# <class 'flowfile_core.flowfile.flow_node.flow_node.FlowNode'>
```

The `FlowNode` is the wrapper that makes your settings operational. It holds your original settings but also adds the machinery needed for execution.

<details markdown="1">
<summary>Peek inside the <code>FlowNode</code></summary>

```python

# The FlowNode keeps your original settings
print(node.setting_input == manual_input_settings)
# True

# But it also contains the execution logic...
print(f"Has a function to run: {node._function is not None}")
# Has a function to run: True

# ...state tracking...
print(f"Can track its state: {hasattr(node, 'node_stats')}")
# Can track its state: True

# ...connections to other nodes...
print(f"Can connect to other nodes: {hasattr(node, 'leads_to_nodes')}")
# Can connect to other nodes: True

# ...and a place to store its results.
print(f"Has a place for results: {hasattr(node, 'results')}")
# Has a place for results: True
```

</details>


This separation is key: **Settings** define _what_ to do, and the **FlowNode** figures out _how_ to do it within the graph.


## 3. Connections: The Key to a Flowing Pipeline


```python
from flowfile_core.schemas.transform_schema import FilterInput

# 1. Define settings for a filter node
filter_settings = input_schema.NodeFilter(
    flow_id=1,
    node_id=2,
    filter_input=FilterInput(
        filter_type="advanced",
        advanced_filter="[age] > 28" # Polars expression syntax
    )
)
graph.add_filter(filter_settings)

# 2. Run the graph
result = graph.run_graph()
print(f"Nodes executed: {result.nodes_completed}/{len(graph.nodes)}")
# Nodes executed: 1/2
```

Only one node ran. The graph knows the filter node has no input, so it can never run successfully and is skipped.

<details markdown="1">
<summary>Why the filter node was skipped</summary>

```python
filter_node = graph.get_node(2)

# The graph checks if a node is a "start" node (has no inputs)
print(f"Is filter_node a start node? {filter_node.is_start}")
# Is filter_node a start node? False

# It sees that the filter node is missing an input connection
print(f"Does filter_node have an input? {filter_node.has_input}")
# Does filter_node have an input? False

# The graph's execution plan only includes nodes it can reach from a start node
print(f"Graph start nodes: {graph._flow_starts}")
# Graph start nodes: [Node id: 1 (manual_input)]
```

</details>

The execution engine works like this:

1. It identifies all start nodes (like our manual input).
2. It builds an execution plan by following the connections from those start nodes.
3. Any node not connected to this flow is ignored.

Let's fix that by adding a connection.

```python

from flowfile_core.flowfile.flow_graph import add_connection
# Create a connection object
connection = input_schema.NodeConnection.create_from_simple_input(
    from_id=1, # From our manual input node
    to_id=2,   # To our filter node
    input_type="main"
)

# Add it to the graph
add_connection(graph, connection)

# Let's check what changed
print(f"Node 1 now leads to: {graph.get_node(1).leads_to_nodes}")
# Node 1 now leads to: [Node id: 2 (filter)]

print(f"Node 2 now receives from: {graph.get_node(2).node_inputs.main_inputs}")
# Node 2 now receives from: [Node id: 1 (manual_input)]
```

Now that they are connected, let's run the graph again.

```python
result = graph.run_graph()

# The graph determines the correct execution order
print("Execution Order:")
for node_result in result.node_step_result:
    print(f"  - Node {node_result.node_id} ran successfully: {node_result.success}")
# Execution Order:
#   - Node 1 ran successfully: True
#   - Node 2 ran successfully: True
```

Both nodes executed. The connection let data flow from the input to the filter.

## 4. The FlowDataEngine: The Data Carrier
When data moves from one node to another, it's bundled up in a FlowDataEngine object. This isn't just raw data; it's a wrapper around a Polars LazyFrame.

```python
# Let's inspect the data after the run
node1 = graph.get_node(1)
node2 = graph.get_node(2)

# Get the resulting data from each node
data_engine1 = node1.get_resulting_data()
data_engine2 = node2.get_resulting_data()

print(f"Type of object passed between nodes: {type(data_engine1)}")
# Type of object passed between nodes: <class 'flowfile_core.flowfile.flow_data_engine.flow_data_engine.FlowDataEngine'>

# Let's see the transformation
print(f"Rows from Node 1 (Input): {len(data_engine1.collect())}")
# Rows from Node 1 (Input): 2

print(f"Rows from Node 2 (Filter): {len(data_engine2.collect())}")
# Rows from Node 2 (Filter): 1  <-- Success! Bob (age 25) was filtered out.

```

The `FlowDataEngine` is the boundary between `flowfile-core` and Polars. It:
* Carries the data (as a LazyFrame).
* Maintains schema information.
* Tracks metadata like record counts.
* Manages lazy vs. eager execution.

## 5. The Hash System: Change Detection
How does the graph know when to re-run a node? Every `FlowNode` has a unique hash based on its configuration and its inputs.

```python
node2 = graph.get_node(2)
original_hash = node2.hash
print(f"Original hash of filter node: {original_hash[:10]}...")
# Original hash of filter node: ...

# Now, let's change the filter's settings
node2.setting_input.filter_input.advanced_filter = "[age] > 20"

# The node instantly knows it's been changed
print(f"Settings changed, needs reset: {node2.needs_reset()}")
# Settings changed, needs reset: True

# Resetting recalculates the hash
node2.reset()
new_hash = node2.hash
print(f"New hash of filter node: {new_hash[:10]}...")
# New hash of filter node: ...

print(f"Hash changed: {original_hash != new_hash}")
# Hash changed: True
```

This hash is calculated from:

* The node's own settings.
* The hashes of all its direct parent nodes.

This creates a chain of **dependency**. If you change a node, `flowfile-core` knows that it and all downstream nodes need to be re-run, while upstream nodes can use their cached results. This is crucial for efficiency.


## 6. Schema Prediction
**Schema prediction** is what makes the interactive UI responsive: a node computes its output schema _without_ processing any data.

Add a "formula" node to create a new column.

```python
from flowfile_core.schemas.transform_schema import FunctionInput, FieldInput

# 1. Add a formula node to double the age
formula_settings = input_schema.NodeFormula(
    flow_id=1,
    node_id=3,
    function=FunctionInput(
        field=FieldInput(name="age_doubled", data_type="Int64"),
        function="[age] * 2" # Polars expression
    )
)
graph.add_formula(formula_settings)

# 2. Connect the filter node to our new formula node
add_connection(graph, input_schema.NodeConnection.create_from_simple_input(2, 3))

# 3. Predict the schema
formula_node = graph.get_node(3)
predicted_schema = formula_node.get_predicted_schema()

print("Predicted columns for Node 3:")
for col in predicted_schema:
    print(f"  - {col.column_name} (Type: {col.data_type})")

# This works even though the node has not run yet!
print(f"\nHas the formula node run? {formula_node.node_stats.has_run_with_current_setup}")
```

<details markdown="1">
<summary>Output of Schema Prediction</summary>

```
Predicted columns for Node 3:
  - name (Type: String)
  - age (Type: Int64)
  - age_doubled (Type: Int64)
```
</details>

How does this work? The node simply:

1. Asks its parent node(s) for their output schema.
2. Applies its own transformation logic to that schema (not the data).
3. Returns the resulting new schema.

This allows a UI to show you how your data will be transformed in real-time, as you build the pipeline.

## The Complete Picture: A Summary
Let's recap the entire lifecycle:

* **You provide Settings:** You define steps using simple Pydantic models (`NodeManualInput`, `NodeFilter`, etc.).
* **Graph Creates FlowNodes:** The `FlowGraph` takes your settings and wraps them in `FlowNode` objects, adding execution logic, state, and connection points.
* **You Connect Nodes:** You create `NodeConnection` objects. This builds the pipeline topology, which the graph uses to determine the execution order.
* **You Run the Graph:** When `graph.run_graph()` is called:
    * An execution plan is created via topological sort.
    * Execution starts from the "start nodes".
    * Each node receives a `FlowDataEngine` from its parent.
    * It applies its transformation logic.
    * It returns a new `FlowDataEngine` to its children.
* **Results Flow Through:** The data, wrapped in the `FlowDataEngine`, moves down the pipeline, getting transformed at each step.


This architecture combines flexibility, introspection, and performance, bridging a visual no-code interface and a code-driven engine that share one graph.



---

## Execution Strategy: How Nodes Decide Where to Run

When a node executes, two questions are answered in turn. **Whether** it runs is decided by the `NodeExecutor` class (`flowfile_core.flowfile.flow_node.executor`). **How** it runs is not decided in the node at all: the node builds its Polars plan and submits it, with a few hints, to the compute backend of the run's execution context, and that backend's materialisation policy decides whether the rows are computed and stored or the plan is kept as it is.

Two flow settings, both visible in the frontend, shape the answers:

- **Execution mode**: *Development* or *Performance*
- **Execution location**: *auto* or a backend id (`local`, `worker`, `worker:<name>`); the frontend offers *auto* and the configured `worker:<name>` ids. A new flow gets *auto*, and a flow file saved with *local* or *remote* opens as *auto*. With `FLOWFILE_COMPUTE_PROTOCOL=0` a flow keeps *local* or *remote*, and a new one gets *remote* unless the default backend is core's own

The [Technical Architecture page](architecture.md#execution-modes) describes what each mode is for; this section describes how a node runs under them.

### The Execution Context

`ExecutionContext.for_flow` (`flowfile/execution/context.py`) turns a flow's settings into the context its runs execute in:

| Setting | Context |
|---|---|
| `execution_location = "local"` | backend `local` (`LocalBackend`, core's own process), one node of a stage at a time |
| `execution_location = "auto"` (or the legacy `"remote"`) | the registry's default backend (`default_backend_id()`), up to `max_parallel_workers` nodes of a stage at once, one at a time when that backend is `local` |
| `execution_location = "worker"` or `"worker:<name>"` | that backend, up to `max_parallel_workers` nodes of a stage at once |
| `execution_mode = "Development"` | `preview_rows = 100` (`DEFAULT_PREVIEW_ROWS`) |
| `execution_mode = "Performance"` | `preview_rows = None` |

The default backend is `worker` (`WorkerBackend`) unless `FLOWFILE_COMPUTE_BACKEND` names another, or, with that unset, the legacy `FLOWFILE_OFFLOAD_TO_WORKER=0` makes it `local`. The headless runners (`flowfile run flow`, `--run-flow`) make `local` the default of their own process.

`run_graph` enters the graph's context with `use_context`, and every stage thread runs under a copy of it, so a node finds its backend through `current_context()` on whichever thread executes it. A run started inside another run (a `run_flow` subflow) keeps the outer run's context, and a caller outside a run enters the graph's context the same way before it executes a node.

### The Decision Pipeline

`NodeExecutor._decide_execution` is the one place that decides whether a node runs. It takes the first rule that matches:

```
output group node, run_flow, gate routing on a formula  → RUN
forced refresh (reset_cache)                           → RUN
source file changed (read nodes)                       → RUN
worker file the last run left (result or sample) gone  → RUN
cache_results, nothing held under the current hash     → RUN
cache_results, holds a stored result, last run failed  → RUN
cache_results, stored result still on the backend      → SKIP
cache_results, stored result gone from the backend     → RUN
Performance mode                                       → RUN
never ran with the current settings                    → RUN
already ran                                            → SKIP
```

A `cache_results` node takes the result it holds under its current hash on the run's backend (`FlowNode.held_result`) and, when that is a stored result and its last run did not fail, asks that backend whether it still has it (`exists`). A result held as a deferred plan is no cache and matches none of the four cache rules, so a cached node holding one follows the rules below them; on `LocalBackend`, which holds every plan deferred, a cached node that has run there always does. A cached node whose last run failed runs again even when its result is still stored, and `FlowNode._do_execute` then reads the stored result back instead of recomputing it. When asking the backend fails (a worker that cannot be reached, or one on other versions), the node fails with that error; it is never read as a missing cache.

In Performance mode every node runs on every run unless it is a cached node whose stored result is still held and whose last run did not fail. An ordinary transform's run only builds its plan and submits it again: with no preview asked for, no backend computes it until a writer or a cached node downstream reads it.

### Submit Hints

`hints_for` (`flow_node/executor.py`) describes the node's plan as `SubmitHints`:

| Hint | Value |
|---|---|
| `must_materialise` | `cache_results` is on and the node is not one of the `IN_CORE_NODE_TYPES` (`output`, `api_response`, `flow_output`, `run_flow`) |
| `wide` | the node is not a narrow transform (`is_narrow_transform`) and not one of the `IN_CORE_NODE_TYPES` |
| `fanout` | how many distinct downstream nodes read the result; no backend's policy reads it yet |
| `preview_rows` | the context's `preview_rows`: 100 in Development mode, `None` in Performance mode, except for a `cache_results` node, which keeps a 100-row preview in every mode; always `None` for the `IN_CORE_NODE_TYPES` |

`is_narrow_transform` reads the node's entry in `node_defaults`, which `configs/node_store/nodes.py` builds from the standard node templates and their `transform_type` (`narrow`, `wide` or `other`). Only `narrow` counts as narrow (select, filter, formula, sample, union and the like); `wide` and `other` nodes (join, group by, sort, record count, the readers, manual input) are submitted `wide`. A custom node has no entry there, so it is submitted `wide` whatever `transform_type` its manifest declares.

`FlowNode._do_execute` builds the plan and submits it to `current_context().backend` under `result_id = self.hash`, with the results the upstream nodes hold on that backend as its inputs. The node keeps the returned `ResultRef` on `results.result_ref`, makes `resulting_data` from `plan_for(ref)`, and takes its preview from the sample `submit` returned inline, else, outside Performance mode, from `sample(ref, n)` the first time the preview is opened.

### Materialisation Policies

| Backend | Stores the rows when | Otherwise | Preview |
|---|---|---|---|
| `LocalBackend` (`backends/local.py`) | never: every plan is held lazily in core, whatever the hints say | deferred | no inline sample; `sample` computes the first rows when a preview is opened |
| `WorkerBackend` (`backends/worker.py`) | `must_materialise`, or `wide` with `preview_rows` set | deferred: core holds the plan object in its own process, unserialised | inline when `preview_rows` is set: read from the Arrow IPC file the worker stored, or, for a deferred plan, from a worker sampler task over it |

On `LocalBackend` nothing computes a node's rows before a consumer, a write or an opened preview reads them, and a chain of deferred results composes over the frames its first plan read. On the worker, a wide plan is stored in Development mode so its preview and its consumers read rows computed once; in Performance mode nothing reads it before the run's writers do, so it stays a plan. A stored result's `plan_for` is a scan of the file the worker wrote (`pl.scan_ipc`), which a consumer's plan composes on.

### Node Classification

What each kind of node becomes on the worker backend:

| Node | Hints | Development mode | Performance mode |
|---|---|---|---|
| narrow transform (select, filter, formula, sample, union) | not `wide` | deferred; sampled inline by a worker sampler task | deferred; no sample |
| wide or other (join, group_by, sort, record_count, readers, manual_input) | `wide` | stored; sampled inline from its file | deferred; no sample |
| any node with `cache_results` | `must_materialise` | stored; sampled inline | stored; sampled inline |
| `output`, `api_response`, `flow_output`, `run_flow` | none; `preview_rows` is `None` | deferred; no inline sample, sampled when its preview is opened | deferred; no sample |

On `LocalBackend` every row of this table is deferred. In Development mode, and for a `cache_results` node in either mode, the preview is computed in core when it is opened.

### Operations

Work that is not a plain Polars plan runs through the backend's `run_op` as an `OperationKind`, the closed enum in `flowfile/execution/protocol.py`: the database, Kafka, REST and Google Analytics reads and the file reads Polars cannot scan lazily; the database, cloud and file writes; fuzzy join; custom nodes; random split; model training and apply; the Delta writes; and visualisation sessions. A node's function calls `FlowNode.run_op` with the backend it runs on, the context's as a rule, and the references a read or a transform gets back become the frame the node then submits like any other plan. Some operations run outside that call: the read node's `file_read` goes through `run_operation`, the function `FlowNode.run_op` wraps, because a new read node's schema prefetch runs it before the node is bound; a `viz_session` is opened by no node's function but by the Explore data analysis routes (`analytics/node_viz.py`); and every `file_write` but the write node's calls `run_op` on the current context's backend directly: the parquet hand-off to a kernel (`write_parquet_for_kernel` in `kernel/execution.py`), which writes the inputs of a Python Script or kernel custom node, of the custom-node dry run and of the notebook kernel runner, and the CSV export route (`routes/routes.py`). A backend lists the kinds it runs in `capabilities().operations` and raises `OperationUnsupported` for the rest. `LocalBackend` runs no Google Analytics read, model training or apply, or visualisation session, so those run on the worker backend in every flow.

---

## The FastAPI Service: Your API Layer

While `FlowGraph`, `FlowNode`, and `FlowDataEngine` power the core pipeline logic, the **FastAPI service** exposes it over HTTP:

- **HTTP interface** – Wraps the core Python objects in a REST API so UIs (like Flowfile's) or other systems can create, run, and inspect flows via standard web requests.
- **State management** – Keeps track of all active `FlowGraph` sessions. When the UI triggers a change, it's really calling one of these endpoints, which updates the in-memory graph.
- **Security** – Handles authentication and authorization so only the right users can access or modify flows.
- **Data previews** – When you view a node's output in the UI, the API calls `.get_resulting_data()` on the corresponding `FlowNode` and returns a sample to the client.

## The canvas notebook package

`flowfile_core/notebook/` turns an open `FlowGraph` into notebook cells and back. Each step is a separate module:

| Module | Role |
|---|---|
| `render.py` | `FlowGraph` → the FlowFrame export in the `ff.*` dialect, split into cells at its statements (a cell carries every node id of its statement), placeholders as `ff.canvas_node(...)`, and a `code_fingerprint` of settings (minus layout), edges and parameters that the notebook panel uses to skip layout-only changes and push uses as its precondition. A formula whose translation would need a `lambda` or the clock stays formula text, so every rendered cell can be interpreted. |
| `runner.py` | `NotebookRunner`, the clean runner `main.py` installs at startup (`install_notebook_runner`). It refuses a request over its size bounds, then, as the requesting user and holding `flowfile_frame.notebook.RUN_LOCK` on the request thread, enters a sync on the canvas snapshot, runs the frame's `clean_run` with a fresh `CellInterpreter` and ends the mode. |
| `interpret.py`, `allowlist.py` | The interpreter and its rules. `CellInterpreter` parses each cell with `ast.parse` and walks a small subset of statements and expressions; nothing a cell holds reaches `exec`, `eval` or `compile`. Every attribute read, call and subscript is looked up in `allowlist.py` by the exact type of the value it applies to and the attribute name; anything else fails on its line with "this needs a kernel". `allowlist.py` is pure data: a verdict for every `ff` name, the allowed attributes per receiver kind, the imports and generated `_` names the export uses, and the size bounds. |
| `prechecks.py` | The canvas's path and connection rules (cloud paths; cloud, database and Kafka connections), applied when a cell places a source or writer, without opening a connection or decrypting a secret. |
| `relabel.py`, `reconcile.py`, `compare.py` | Pure functions: map a clean run's node ids onto the canvas's by per-cell provenance, then compute the minimal list of editor operations between the live graph and the clean run. |
| `push.py`, `bridge.py` | The push pipeline behind `POST /editor/notebook/push/` and its preview `POST /notebook/plan`: fingerprint check (409), clean run through the installed runner, refusals, reconcile. A push that names a `trigger` (`push` or `run`) applies nothing and answers `applied: false` with its warnings, deletions and parameter changes when `needs_confirmation` says that action reviews first; the client confirms and pushes again without it. A failed cell or a refusal answers 422 with `{message, cell_id, line, kind}`, where `kind` is `needs_kernel`, `refused` or `error`. |

The routes live in `routes/notebook.py` (`/notebook/*`) plus `POST /editor/notebook/push/` and `POST /editor/notebook/run_lineage/` in `routes/routes.py`. Only those two are `editor/` routes, the two that change or run the canvas. Where `sharing.sharing_enabled()` (docker and package mode) plan and push require an admin (`routes/notebook.py::require_notebook_sync`); rendering and running do not. Core runs no notebook process: cell code is never executed as Python by core, which `tests/notebook/test_contract.py` checks over the whole corpus while it ratchets the `exec`/`eval`/`compile` call sites. The frame side is `flowfile_frame/notebook.py` (build mode and the sync) and `flowfile_frame/notebook_cells.py` (seed, snapshot session, execute through an executor, display, clean run); `exec_cell`, the executor that runs a cell as Python, is never passed by core.
