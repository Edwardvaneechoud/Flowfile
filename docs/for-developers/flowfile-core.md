# Flowfile Core: A Developer's Guide

Welcome! This guide is for developers who want to understand, use, and contribute to `flowfile-core`. We'll dive into the architecture, see how data flows, and learn how to build powerful data pipelines.

!!! info "Looking for the API docs?"
    - **[Python API Reference for users](../users/python-api/index.md)**: If you want to USE Flowfile
    - **[Design Philosophy](design-philosophy.md)**: If you want to understand WHY Flowfile works this way
    - **This page**: If you want to understand HOW Flowfile works internally

!!! tip "New to Flowfile?"
    If you're looking for the high-level Python API, start with the [Python API Overview](../users/python-api/index.md). This guide dives into the internal architecture.

Ready? Let's build something!

---


## The Core Architecture

At its heart, `flowfile-core` is composed of three main objects:

1.  **`FlowGraph`**: The central orchestrator. It holds your pipeline, manages the nodes, and controls the execution flow.
2.  **`FlowNode`**: An individual step in your pipeline. It's a wrapper around your settings and logic, making it an executable part of the graph.
3.  **`FlowDataEngine`**: The data itself, which flows between nodes. It's a smart wrapper around a [Polars LazyFrame](https://pola-rs.github.io/polars/py-polars/html/reference/lazyframe/index.html), carrying both the data and its schema.

Let's see these in action.

---

## 1. The FlowGraph: Your Pipeline Orchestrator

Everything starts with the `FlowGraph`. Think of it as the canvas for your data pipeline.

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
  -execution_location: local             #<-- Where the flow runs ('local' or 'remote').
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
You don't add raw functions or data directly to the graph. Instead, you provide **settings objects** (which are just Pydantic models). The graph then transforms these settings into executable `FlowNodes`.

Watch this:

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

Only one node ran! Why? The graph is smart; it knows the filter node has no input, thus will never succeed running.

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

Success! Both nodes executed. The connection allowed data to flow from the input to the filter.

## 4. The FlowDataEngine: The Data Carrier
When data moves from one node to another, it's bundled up in a FlowDataEngine object. This isn't just raw data; it's an enhanced wrapper around a Polars LazyFrame.

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

## 5. The Hash System: Smart Change Detection
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


## 6. Schema Prediction: See the Future
One of the most powerful features for interactive UI is **schema prediction**. A node can predict its output schema _without_ processing any data.

Let's add a "formula" node to create a new column.

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
  - city (Type: String)
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


This architecture provides a powerful combination of flexibility, introspection, and performance, bridging the gap between a visual, no-code interface and a powerful, code-driven engine.



---

## The FastAPI Service: Your API Layer

While `FlowGraph`, `FlowNode`, and `FlowDataEngine` power the core pipeline logic, the **FastAPI service** is what makes it accessible from the outside world.

Think of it as the **control panel** for your pipelines:

- **HTTP interface** – Wraps the core Python objects in a REST API so UIs (like Flowfile’s) or other systems can create, run, and inspect flows via standard web requests.
- **State management** – Keeps track of all active `FlowGraph` sessions. When the UI triggers a change, it’s really calling one of these endpoints, which updates the in-memory graph.
- **Security** – Handles authentication and authorization so only the right users can access or modify flows.
- **Data previews** – When you view a node’s output in the UI, the API calls `.get_resulting_data()` on the corresponding `FlowNode` and returns a sample to the client.

In short: **FastAPI turns the in-memory power of `flowfile-core` into a secure, interactive web service**, enabling rich, real-time applications to be built on top of your pipelines.
