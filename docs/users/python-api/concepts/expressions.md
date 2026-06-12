# Expressions

FlowFrame methods accept standard **Polars expressions** — `ff.col`, operators, `ff.when`, and the rest of the expression API you already know from Polars. Expressions are the default and most powerful way to express transformations, covering the entire Polars API.

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

## Filtering

```python
df = df.filter(ff.col("price") > 100)
df = df.filter(ff.col("status") != "cancelled", description="Drop cancelled orders")
```

## Namespaces

Polars expression namespaces work as expected:

```python
df = df.with_columns([
    ff.col("name").str.to_uppercase().alias("name_upper"),
    ff.col("order_date").dt.year().alias("order_year"),
])
```

Because expressions stay lazy, they compose into a single optimized query plan — see [FlowFrame and FlowGraph](design-concepts.md).

!!! tip "Looking for the Excel-like `[column]` syntax?"
    That is the **Flowfile formula language** — a separate, simpler syntax shared with the visual editor. See [Formulas in Python](formulas.md) for the FlowFrame methods that accept it, and the [Formula Language guide](../../formulas/index.md) for the language itself.

---

*Want to see more of the Flowfile Python API? Check out the [reference documentation](../reference/index.md).*
