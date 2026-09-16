# Expressions

FlowFrame methods accept standard **Polars expressions** — `ff.col`, operators, `ff.when`, and most of the Polars expression API. Expressions are the default way to express transformations.

!!! note "Most of Polars, not all of it"
    Nearly every Polars `Expr` method is available, but a few names track the pinned Polars version (`cum_sum`, not `cumsum`; `dt.weekday`, not `day_of_week`), some helpers are selectors rather than top-level functions (`ff.all_()`, not `ff.all()`; use `ff.col("*").exclude(...)`, there is no `ff.exclude()`), and expressions without a dedicated node render as `polars_code` nodes in the visual editor. [Which operations become which node](design-concepts.md#which-operations-become-which-node) lists the patterns that render natively.

## Column references and arithmetic

```python
import flowfile as ff

df = df.with_columns([
    (ff.col("price") * ff.col("quantity")).alias("revenue"),
    (ff.col("price") * 1.1).alias("price_with_tax"),
    (ff.col("total") / ff.col("count")).alias("average"),
])
```

## Conditional logic

```python
df = df.with_columns(
    ff.when(ff.col("quantity") > 75)
    .then(ff.lit("High"))
    .otherwise(ff.lit("Low"))
    .alias("volume_category")
)
```

A `when` chain renders as a Formula node with the equivalent `if … then … elseif … else … endif` formula, so the branches stay editable in the visual editor. Membership tests use `is_in`: `ff.when(ff.col("team").is_in(["DS", "DE"]))` becomes `if [team] in ("DS", "DE") then … endif`. Conditions without a formula form (a `map_elements` call, for example) push the whole chain to a `polars_code` node.

## Filtering

```python
df = df.filter(ff.col("price") > 100)
df = df.filter(ff.col("status") != "cancelled", description="Drop cancelled orders")
```

Comparison predicates render as a Filter node whose advanced expression is the equivalent formula: the first call above becomes `([price] > 100)`, and `ff.col("status").is_in(["new", "open"])` becomes `[status] in ("new", "open")`. Several predicates are joined with `and`. A predicate without a formula form (a lambda, or a method the [Formula Language](../../formulas/index.md) does not cover) renders as a `polars_code` node instead.

## Namespaces

Polars expression namespaces work as expected:

```python
df = df.with_columns([
    ff.col("name").str.to_uppercase().alias("name_upper"),
    ff.col("order_date").dt.year().alias("order_year"),
])
```

Because expressions stay lazy, they compose into one query plan that runs when you `.collect()` — see [FlowFrame and FlowGraph](design-concepts.md).

!!! tip "Looking for the Excel-like `[column]` syntax?"
    That is the **Flowfile formula language** — a separate, simpler syntax shared with the visual editor. See [Formulas in Python](formulas.md) for the FlowFrame methods that accept it, and the [Formula Language guide](../../formulas/index.md) for the language itself.
