# 2. Clean and shape

Continue the invoice pipeline with positive-quantity and positive-price checks. Express the revenue rule as a Flowfile formula, then compare it with the expression form. The [complete script](#complete-script) repeats the reader from chapter 1.

```python
--8<-- "docs/examples/tutorial_02.py:clean"
```

`filter()` keeps matching rows. `with_columns()` adds or replaces columns without reducing the row count. These sample rows already pass the positive-value check; deduplication is what removes the 30 extra rows.

## Formulas and expressions

```python
--8<-- "docs/examples/tutorial_02.py:expressions"
```

The multiplication expression becomes an editable **Formula** node too. Flowfile uses native nodes when it can represent the operation with their settings. Unsupported expression forms fall back to **Polars code** (`polars_code`).

The `scaled` comparison deliberately replaces `revenue` while another expression reads it. Both expressions read the original input to that call: expected income is still 5% of the unscaled revenue. Keeping those semantics requires a Polars code node. Continue the pipeline with `orders`; `scaled` is only this comparison.

!!! tip "A new column needs a second expression call"
    Within one expression-based `with_columns()` call, expressions do not read one another's new values. Chain a second call when a calculation depends on the column you just added. In contrast, entries passed through `flowfile_formulas=` evaluate in sequence and can reference earlier entries.

## Check it

```python
--8<-- "docs/examples/tutorial_02.py:check"
```

The script prints:

```text title="Output"
--8<-- "docs/examples/output/tutorial_02.txt"
```

After **Filter data** the graph branches. The Formula on the right is `expression_orders`, which nothing reads. The Formula on the left is `orders`, and the **Polars code** node below it is `scaled`. For `INV-00001`, scaled revenue is 0.04041 (40.41 / 1000) and expected income is 2.0205 (5% of 40.41).

## Complete script

<details markdown="1">
<summary>Show the complete runnable script</summary>

```python title="tutorial_02.py"
--8<-- "docs/examples/tutorial_02.py"
```

</details>

[Download tutorial_02.py](../../../examples/tutorial_02.py){ download="tutorial_02.py" }

## Recap

You can choose a readable formula without giving up expressions. Node selection depends on what the operation means, including which version of a column it reads. Next, combine invoice segments and attach city targets.

**Next:** [Combine](combine.md)

<!-- Claim-to-source verification
flowfile_frame/flowfile_frame/flow_frame.py: filter, _filter_exprs_to_formula,
with_columns, _with_flowfile_formulas; flowfile_frame/flowfile_frame/expr.py: arithmetic and alias.
Node names: flowfile_core/flowfile_core/configs/node_store/nodes.py.
Displayed output: docs/examples/output/tutorial_02.txt, compared with the script's stdout by
flowfile_core/tests/docs_examples/test_docs_examples.py.
-->
