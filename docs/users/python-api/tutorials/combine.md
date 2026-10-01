# 3. Combine

Extend the cleaned invoices with a target for each city. First separate and recombine the two customer segments to model two incoming feeds, using the same CSV throughout. The [complete script](#complete-script) includes the earlier steps.

```python
--8<-- "docs/examples/tutorial_03.py:combine"
```

The targets are tutorial planning inputs, not values supplied by the CSV.

A **union** stacks invoice rows. The two filters here are disjoint and cover the sample's `Member` and `Normal` customer types. `ff.concat(..., how="diagonal_relaxed")` places a native Union node; it matches columns by name and can accommodate different column sets.

A **join** adds columns by matching keys. This left join keeps the invoices and attaches the matching city's target. Each city appears once in `targets`, so each invoice matches exactly one target.

!!! tip "Check the lookup's grain"
    Two target rows for one city would match each invoice twice and inflate the later revenue sum. A left join also leaves a null target for any city missing from the lookup, which should be resolved before comparing revenue with targets.

## Check it

```python
--8<-- "docs/examples/tutorial_03.py:check"
```

The script prints:

```text title="Output"
--8<-- "docs/examples/output/tutorial_03.txt"
```

The two customer filters branch off the Formula and merge again at **Union data**; the targets enter through **Manual input** and meet the invoices at **Join**. The join keeps all 1,000 invoices, and every one has a target.

## Complete script

<details markdown="1">
<summary>Show the complete runnable script</summary>

```python title="tutorial_03.py"
--8<-- "docs/examples/tutorial_03.py"
```

</details>

[Download tutorial_03.py](../../../examples/tutorial_03.py){ download="tutorial_03.py" }

## Recap

The pipeline now combines invoice feeds and enriches every invoice with its city target. Next, reduce those rows to one report row per city without accidentally summing the target once per invoice.

**Next:** [Aggregate](aggregate.md)

<!-- Claim-to-source verification
flowfile_frame/flowfile_frame/flow_frame_methods.py: from_dict, concat;
flowfile_frame/flowfile_frame/flow_frame.py: concat (use_native), join;
flowfile_frame/flowfile_frame/join.py; flowfile_core/flowfile_core/schemas/transform_schema.py: JoinKeyStrategy.
Displayed output: docs/examples/output/tutorial_03.txt, compared with the script's stdout by
flowfile_core/tests/docs_examples/test_docs_examples.py.
-->
