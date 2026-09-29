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

A class defined in a script exists only in that Python process. [`ff.custom_nodes.install`](../reference/native-nodes.md#custom_nodes) writes it to your custom nodes folder as `tutorial_shortfall_percent.py`, so the Designer and saved flows can use the node; without that file the node opens in the Designer as **Custom node not installed**. `overwrite=True` lets the script run again, replacing the file the previous run wrote. Another machine that runs the flow needs the node installed too.

## A Python Script version

The Python Script node needs a kernel. [Create one](../../visual-editor/kernels.md#creating-a-kernel) in the Designer with the **Kernel ID** `tutorial`; the script looks it up with [`ff.kernels`](../reference/native-nodes.md#kernels).

```python
--8<-- "docs/examples/tutorial_09.py:script"
```

Calling the decorated function places a deferred Python Script node. `returns=` declares the output columns so downstream nodes can be built. The function body runs on the kernel when the graph executes that branch.

Without a `tutorial` kernel, `kernel` is `None`: the script still builds, and the node opens in the Designer with an empty kernel picker (**Select a kernel...**).

!!! info "Running the Python Script"
    The script builds this node but does not run it. With Docker running and the `tutorial` kernel created, `script_scored.collect()` runs the function on that kernel.

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
custom_node.py: CustomNodeFactory.__call__, _resolve, _register_class (accepts the class install recorded),
_session_only_custom_nodes (empty after install, so open_graph_in_editor does not warn);
custom_nodes.py: CustomNodes.install (writes <node key>.py to registry.directory, the custom nodes folder; overwrite=);
kernels.py: Kernels.__contains__/__getitem__, KernelInfo; native.py: _kernel_id (stores KernelInfo.id);
python_script.py: python_script, PythonScriptFunction (kernel id stored as given, not checked until the node runs);
shared/node_designer/custom_node.py: CustomNodeBase, process; shared/node_designer/ui_components.py: TextInput, Section.
UI labels: kernel/KernelCreateForm.vue ("Kernel ID"), pythonScript/PythonScript.vue ("Select a kernel..."),
customNode/CustomNode.vue ("Custom node not installed").
Kernel execution: flowfile_core/flowfile_core/kernel/manager.py (execute starts a stopped kernel via
_ensure_running_sync); deferred collection: flow_frame.py.
Displayed output: docs/examples/output/tutorial_09.txt, compared with the script's stdout by
flowfile_core/tests/docs_examples/test_docs_examples.py.
-->
