# Formulas in Python

A few FlowFrame methods accept **Flowfile formula** strings — the same Excel-like language used by the visual editor's [Formula](../../visual-editor/nodes/transform.md#formula) and [Filter](../../visual-editor/nodes/transform.md#filter-data) nodes:

```python
"[price] * [quantity]"   # Equivalent to ff.col("price") * ff.col("quantity")
```

This page documents the methods that accept formulas. The language itself — syntax, operators, and all 95 built-in functions — is documented in the [Formula Language guide](../../formulas/index.md). For everything else, use regular [Polars expressions](expressions.md).

!!! tip "Try it in your browser"
    The [interactive playground](https://edwardvaneechoud.github.io/polars_expr_transformer/) shows the generated FlowFrame code for every formula you type.

## `with_columns(flowfile_formulas=..., output_column_names=...)`

Creates one new column per formula. The two lists must have the same length; the optional `output_column_datatypes` list forces result types (otherwise they are inferred).

```python
import flowfile as ff

df = ff.FlowFrame({
    "product": ["Widget", "Gadget", "Tool"],
    "price": [10.50, 25.00, 8.75],
    "quantity": [100, 50, 200],
    "discount": [0.1, 0.15, 0.05]
})

result = df.with_columns(
    flowfile_formulas=[
        "[price] * [quantity]",                            # Simple multiplication
        "[price] * (1 - [discount])",                      # With parentheses
        "if [quantity] > 75 then 'High' else 'Low' endif", # Conditional
        "round([price] * [discount], 2)"                   # Built-in function
    ],
    output_column_names=["revenue", "discounted_price", "volume_category", "discount_amount"],
    description="Calculate derived metrics"
)
```

## `filter(flowfile_formula=...)`

Keeps rows where the formula evaluates to `true`. The `flowfile_formula` parameter is mutually exclusive with positional predicates and keyword constraints.

```python
df = df.filter(flowfile_formula="[price] > 100 and [quantity] >= 10")
```

## `filter_split(flowfile_formula=...)`

Like `filter`, but returns a `(pass, fail)` tuple of frames — rows matching the formula go to the first frame, the rest to the second. Rows where the formula evaluates to null are dropped from both.

```python
high_value, low_value = df.filter_split(flowfile_formula="[price] * [quantity] > 1000")
```

## Mixing formulas and expressions

Both build the same lazy graph, so you can switch freely between them in one pipeline:

```python
output = df.with_columns([
    ff.col("price").round(0).alias("price_rounded")          # Polars expression
]).with_columns(
    flowfile_formulas=["[price_rounded] * [quantity]"],      # Formula
    output_column_names=["estimated_revenue"]
)
```

**When to use which?** [Polars expressions](expressions.md) cover the entire Polars API and are the better fit for complex transformations. Formulas shine for simple, readable column logic and render as editable Formula/Filter node settings when you [open the pipeline in the visual editor](../reference/visual-ui.md).

---

*Next: the [Formula Language guide](../../formulas/index.md) and [function reference](../../formulas/functions.md).*
