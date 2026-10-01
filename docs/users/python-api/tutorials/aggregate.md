# 4. Aggregate

Turn the enriched invoices into a city report for baskets with at least eight items. Keep invoice counts, revenue, and the city target together. The [complete script](#complete-script) repeats chapters 1–3 first, so it runs on its own.

```python
--8<-- "docs/examples/tutorial_04.py:bulk"
```

```python
--8<-- "docs/examples/tutorial_04.py:aggregate"
```

`group_by("city").agg(...)` reduces many invoice rows to one row per city. Revenue is additive; the target is repeated on every invoice, so take its first value. This is valid because chapter 3 established one target per city.

`shortfall = target - revenue`: positive means below target; negative means above it. These are planning exceptions, not evidence of bad transactions.

!!! info "Grouping transforms data"
    `group_by()` chooses which rows contribute to each aggregate. It does not decide whether a later writer or subflow runs. A Gate makes that execution decision in chapter 6.

## Check it

Print the graph, then collect the report:

```python
--8<-- "docs/examples/tutorial_04.py:check"
```

The script prints:

```text title="Output"
--8<-- "docs/examples/output/tutorial_04.txt"
```

The last four nodes are this chapter's additions: **Filter data**, **Group by**, the **Formula** that computes `shortfall`, and **Sort data**. In the report, Bago, Naypyitaw, and Yangon have a positive shortfall, so they are below target.

## Complete script

<details markdown="1">
<summary>Show the complete runnable script</summary>

```python title="tutorial_04.py"
--8<-- "docs/examples/tutorial_04.py"
```

</details>

[Download tutorial_04.py](../../../examples/tutorial_04.py){ download="tutorial_04.py" }

## Recap

You have five report rows from 314 qualifying invoices. Three cities fall below their tutorial targets. Next, make the basket threshold an input to the flow.

**Next:** [Flow parameters](flow-parameters.md)

<!-- Claim-to-source verification
Graph output: flowfile_core/flowfile_core/flowfile/flow_graph.py: print_tree.
flowfile_frame/flowfile_frame/flow_frame.py: group_by, filter;
flowfile_frame/flowfile_frame/group_frame.py: agg, _process_group_columns,
_process_agg_expressions, _create_agg_node; expr.py: count, sum, first.
Aggregate settings: flowfile_core/flowfile_core/schemas/transform_schema.py: AggColl.
Displayed output: docs/examples/output/tutorial_04.txt, compared with the script's stdout by
flowfile_core/tests/docs_examples/test_docs_examples.py.
-->
