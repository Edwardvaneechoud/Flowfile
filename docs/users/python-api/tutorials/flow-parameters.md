# 5. Flow parameters

Replace the fixed basket threshold with a typed flow parameter. Keep the aggregation from chapter 4. The [complete script](#complete-script) contains the complete pipeline.

```python
--8<-- "docs/examples/tutorial_05.py:parameters"
```

`add_flow_parameter()` declares the parameter on the graph and returns the `Parameter` you pass in. It accepts either a `FlowFrame` or a `FlowGraph`: passing `enriched.flow_graph` instead of `enriched` declares the same parameter on the same graph.

A Python variable exists in this script. A `Parameter` declares a named value stored with the flow, so a later run can override it. `default=8` is its initial value; `type="integer"` checks values supplied through the parameter helpers.

Declare the parameter on `enriched` before building the filter. The filter retains the parameter reference, rather than storing only the number eight.

## Change a run

```python
--8<-- "docs/examples/tutorial_05.py:change"
```

`set_flow_parameter()` updates the graph's stored value. For this ordinary lazy frame, `collect()` still evaluates the plan built earlier. Run the graph explicitly, then read the report node's result to observe the new value.

!!! info "A parameter is a value"
    Use it in a comparison, not as a column name. Declaring `min_quantity` does not create a data column. `minimum.default` remains eight even after changing the graph's copy with `set_flow_parameter()`.

## Check it

The script prints the report at the default of eight, then at ten:

```text title="Output"
--8<-- "docs/examples/output/tutorial_05.txt"
```

The first report matches chapter 4: its `orders` column sums to 314 invoices. At ten the same five cities keep 107, and every city falls below its target.

## Complete script

<details markdown="1">
<summary>Show the complete runnable script</summary>

```python title="tutorial_05.py"
--8<-- "docs/examples/tutorial_05.py"
```

</details>

[Download tutorial_05.py](../../../examples/tutorial_05.py){ download="tutorial_05.py" }

## Recap

The saved report now has a typed input instead of a fixed threshold. Next, use the resulting shortfalls to choose a review or ready route for the whole report.

**Next:** [Branching with Gate](branching.md)

<!-- Claim-to-source verification
flowfile_frame/flowfile_frame/parameters.py: Parameter, add_flow_parameter, set_flow_parameter, to_expr;
flowfile_frame/flowfile_frame/flow_frame.py: collect, _param_values;
flowfile_core/flowfile_core/flowfile/flow_graph.py: run_graph;
flowfile_core/flowfile_core/flowfile/flow_node/flow_node.py: get_resulting_data.
Build-time snapshot checked by flowfile_frame/tests/test_flow_parameters.py.
Displayed output: docs/examples/output/tutorial_05.txt, compared with the script's stdout by
flowfile_core/tests/docs_examples/test_docs_examples.py.
-->
