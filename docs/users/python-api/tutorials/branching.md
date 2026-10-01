# 6. Branching with Gate

Route the five-row city report to review if any city is below target. Otherwise mark it ready. The [complete script](#complete-script) extends the parameterized report.

```python
--8<-- "docs/examples/tutorial_06.py:gate"
```

A formula Gate opens its `then` exit if **at least one row** matches. It passes the entire input frame through that exit. Here all five cities enter review, including the two above target. `otherwise` is the else exit; `else_` is an alias.

A filter decides which rows continue and `group_by()` summarizes them; a Gate decides which downstream branch runs, and passes its whole input through that branch.

The inactive branch is skipped. Rejoin the two branches with the native Union form shown above; the default vertical concat produces a different node and is not a substitute for this routing join.

!!! info "Collection now runs the graph lineage"
    A frame below a Gate needs a graph run to determine its live exit. Its `collect()` runs that frame and its ancestors with the current parameter values. It does not run unrelated writers elsewhere in the graph.

## Check it

```python
--8<-- "docs/examples/tutorial_06.py:check"
```

The script prints:

```text title="Output"
--8<-- "docs/examples/output/tutorial_06.txt"
```

The graph splits at **Gate**: the first node on each exit carries its name, `[then]` or `[else]`, and the exits meet again at **Union data**. With the default minimum of eight, three cities are below target, so the whole report takes the review exit. With a minimum of one, every city is above its target and the report takes the ready exit. The targets stay fixed when the basket threshold changes: this comparison shows the effect of broadening the included sales.

## Complete script

<details markdown="1">
<summary>Show the complete runnable script</summary>

```python title="tutorial_06.py"
--8<-- "docs/examples/tutorial_06.py"
```

</details>

[Download tutorial_06.py](../../../examples/tutorial_06.py){ download="tutorial_06.py" }

## Recap

You can route an entire report without filtering away context. The Union keeps the output shape consistent whichever branch runs. Next, write that routed result.

**Next:** [Write results](write-results.md)

<!-- Claim-to-source verification
flowfile_frame/flowfile_frame/gate.py: Gate, then, otherwise, else_;
flowfile_frame/flowfile_frame/flow_frame.py: _materialised_lazyframe, concat;
flowfile_core/flowfile_core/flowfile/flow_graph.py: _evaluate_gate_conditions, run_graph;
flowfile_core/flowfile_core/flowfile/flow_data_engine/flow_data_engine.py: gate evaluation.
Exit names in the graph drawing: flowfile_core/flowfile_core/schemas/input_schema.py: NodeGate.output_names.
Displayed output: docs/examples/output/tutorial_06.txt, compared with the script's stdout by
flowfile_core/tests/docs_examples/test_docs_examples.py.
-->
