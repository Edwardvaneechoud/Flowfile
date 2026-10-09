# 8. Reusable flows

Extract the filter, aggregation, and shortfall calculation into a child flow. The parent still reads, cleans, enriches, and routes the report. The [complete script](#complete-script) is runnable on its own.

```python
--8<-- "docs/examples/tutorial_08.py:child"
```

This replaces the inline report calculation from chapters 4–5. `FlowInput("invoices", schema=...)` declares a named input with columns but no rows. `to_flow_output("city_report")` names the result the parent can retrieve. `register_flow()` writes the child YAML and registers it in the catalog immediately.

## Feed the child

```python
--8<-- "docs/examples/tutorial_08.py:call"
```

`invoices=enriched` binds a frame to the child's input slot. `params=` passes the parent's threshold to the child's parameter. The names serve different purposes: one carries rows, the other carries a value.

The Gate from chapter 6 now reads `report`. A `RunFlow` output is deferred: building it does not run the child; collecting it runs its lineage. The complete script leaves out the chapter 7 writers; chapter 10 puts the whole pipeline together.

## Compare thresholds with iterate

```python
--8<-- "docs/examples/tutorial_08.py:iterate"
```

`param_frame` supplies one threshold per row. `ff.col("minimum")` binds that column to the child's `min_quantity`. With `iterate=True`, the child runs three times and returns the combined outputs with `param_min_quantity` and `run_index` metadata. The script then counts cities and sums invoices per threshold.

!!! tip "Iteration does not partition the invoices"
    Every run receives the same `enriched` frame. Only the bound parameter changes. The child filter selects the qualifying invoices; `iterate=True` itself does not split them into groups.

## Check it

```python
--8<-- "docs/examples/tutorial_08.py:check"
```

The script prints:

```text title="Output"
--8<-- "docs/examples/output/tutorial_08.txt"
```

The first graph is the child: **Flow Input** and **Flow Output** around the filter and aggregation. In the parent, a single **Run Flow** node takes their place between **Join** and **Gate**. The child's report matches chapter 4, and the threshold comparison counts 1,000, 314, and 107 qualifying invoices across the same five cities.

## Complete script

<details markdown="1">
<summary>Show the complete runnable script</summary>

```python title="tutorial_08.py"
--8<-- "docs/examples/tutorial_08.py"
```

</details>

[Download tutorial_08.py](../../../examples/tutorial_08.py){ download="tutorial_08.py" }

## Recap

The report has a reusable interface: invoices in, one threshold, city report out. Next, add a comparable shortfall percentage using SQL and inspect equivalent custom-logic options.

**Next:** [Custom logic](custom-logic.md)

<!-- Claim-to-source verification
flowfile_frame/flowfile_frame/run_flow.py: FlowInput, _to_flow_output, register_flow, RunFlow,
_parameter_bindings; parameters.py: parameter forwarding; flow_frame.py: collect.
Registration defaults/files: run_flow.py: _registration_path, register_flow.
Iteration execution: flowfile_core/flowfile_core/flowfile/flow_graph/builders/subflow.py: add_run_flow.
Displayed output: docs/examples/output/tutorial_08.txt, compared with the script's stdout by
flowfile_core/tests/docs_examples/test_docs_examples.py.
-->
