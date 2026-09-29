# 9. Custom logic

Add a shortfall percentage to the reusable city report. Use SQL for the pipeline, then compute the same score with a custom node and a Python Script. The [complete script](#complete-script) includes the previous reader, child flow, and Gate.

```python
--8<-- "docs/examples/tutorial_09.py:sql"
```

Place `scored` into the Gate where chapter 8 used `report`. The SQL reads the supplied frame under the name `report`; it uses Polars SQL, not a database connection. Positive percentages still mean below target.

## A configurable custom node

The script imports `polars as pl` and `from flowfile import node_designer as nd` at the top.

```python
--8<-- "docs/examples/tutorial_09.py:custom"
```

`process()` receives Polars lazy frames. The settings schema exposes **Target column** as a text input, and the factory maps `target_column=` to that setting. This is useful when the calculation needs a reusable node with an editable form. For this report, SQL already expresses the calculation in one line.

## A Python Script version

```python
--8<-- "docs/examples/tutorial_09.py:script"
```

Calling the decorated function places a deferred Python Script node. `returns=` declares the output columns so downstream nodes can be built. The function body runs in a kernel when the graph executes that branch.

!!! info "Running the Python Script"
    The script builds this node but does not run it: running it needs Docker and a kernel. Replace `my-kernel` with the id of one of your kernels, then collect `script_scored`. A custom node class supplied only in Python must be [installed](../reference/native-nodes.md#custom_nodes) on another runtime before a saved flow using it can run there.

## Check it

```python
--8<-- "docs/examples/tutorial_09.py:check"
```

The script prints:

```text title="Output"
--8<-- "docs/examples/output/tutorial_09.txt"
```

The three implementations branch off **Run Flow**. The custom node and the Python Script end there; **SQL Query** continues into the Gate. The SQL and custom-node results are identical. Keep the SQL branch for the shipped report; the other two demonstrate alternative implementations of the same calculation.

## Complete script

<details markdown="1">
<summary>Show the complete runnable script</summary>

```python title="tutorial_09.py"
--8<-- "docs/examples/tutorial_09.py"
```

</details>

[Download tutorial_09.py](../../../examples/tutorial_09.py){ download="tutorial_09.py" }

## Recap

The report now includes a percentage for comparing shortfalls across different targets. Next, save this pipeline, inspect it in the Designer, and run it without the UI.

**Next:** [Ship it](ship-it.md)

<!-- Claim-to-source verification
flowfile_frame/flowfile_frame/sql_query.py: sql, _sql_frame;
custom_node.py: CustomNodeFactory.__call__, _resolve; python_script.py: python_script, PythonScriptFunction
(kernel id stored as given, not checked until the node runs);
shared/node_designer/custom_node.py: CustomNodeBase, process; shared/node_designer/ui_components.py: TextInput, Section.
Kernel execution: flowfile_core/flowfile_core/kernel/manager.py; deferred collection: flow_frame.py.
Session-only custom nodes: flowfile_frame/flowfile_frame/custom_nodes.py.
Displayed output: docs/examples/output/tutorial_09.txt, compared with the script's stdout by
flowfile_core/tests/docs_examples/test_docs_examples.py.
-->
