# 1. First steps

Build a city sales report from supermarket invoices. This first step reads the public sample, removes exact duplicates, and calculates revenue; later chapters add targets, review routing, and scheduled runs.

```python
--8<-- "docs/examples/tutorial_01.py:read"
```

Use the environment from the [Quick Start](../quickstart.md). The CSV URL works without a repository checkout. [Complete runnable script](#complete-script).

!!! tip "You can pass an existing graph"
    In a standalone script, `read_csv()` creates a graph when you omit `flow_graph`. To add the reader to a graph you already have, pass it by keyword:

    ```python
    --8<-- "docs/examples/tutorial_01.py:existing-graph"
    ```

    The returned frame points to that same graph through `sales_on_graph.flow_graph`. You can also retrieve an automatically created graph from `sales.flow_graph` and pass it to another reader. This is an alternative setup; the walkthrough below continues with `sales` from the first example.

## Build the transformation

```python
--8<-- "docs/examples/tutorial_01.py:transform"
```

`unique()` removes identical rows. `with_columns()` adds `revenue` as unit price × quantity. `preview` is a `FlowFrame`: it holds a lazy data plan and a reference to the graph built by these calls.

## Check it

```python
--8<-- "docs/examples/tutorial_01.py:check"
```

The script prints:

```text title="Output"
--8<-- "docs/examples/output/tutorial_01.txt"
```

`print_tree()` draws the graph without collecting any data: one node per line, top to bottom, named as on the canvas and followed by its node id. `unique()` became **Drop duplicates** and `with_columns()` a **Formula**. Ids are assigned as nodes are created and can skip numbers. Later graphs branch and merge; lines on the left show where.

`collect()` runs the plan and returns a Polars `DataFrame`: the file has 1,030 rows and 1,000 unique invoices. `preview.flow_graph` still holds the operations for the Designer.

!!! info "Lazy does not mean no I/O"
    Reading this URL can fetch the file and inspect its schema while the graph is built. Laziness here means the transformation result is not materialized until collection. Writers and kernel nodes have their own execution rules, introduced later.

## Complete script

<details markdown="1">
<summary>Show the complete runnable script</summary>

```python title="tutorial_01.py"
--8<-- "docs/examples/tutorial_01.py"
```

</details>

[Download tutorial_01.py](../../../examples/tutorial_01.py){ download="tutorial_01.py" }

## Recap

You have invoice rows, a revenue column, and a graph describing how they were produced. Next, make the cleaning rules editable and see when an expression becomes a code node.

**Next:** [Clean and shape](clean-and-shape.md)

<!-- Claim-to-source verification
Graph drawing: flowfile_core/flowfile_core/flowfile/graph_tree/graph_tree.py: render_flow (canvas names from the node template).
API and lazy collection: flowfile_frame/flowfile_frame/flow_frame_methods.py: read_csv;
flowfile_frame/flowfile_frame/flow_frame.py: unique, with_columns, select, sort, collect.
Explicit and implicit graph creation: flowfile_frame/flowfile_frame/utils.py: create_flow_graph, _implicit_graph.
URL read behavior: flowfile_core/flowfile_core/flowfile/flow_data_engine/flow_data_engine.py: create_from_path.
Public exports: flowfile/flowfile/__init__.py.
Displayed output: docs/examples/output/tutorial_01.txt, compared with the script's stdout by
flowfile_core/tests/docs_examples/test_docs_examples.py.
-->
