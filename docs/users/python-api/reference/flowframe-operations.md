# DataFrame Operations

Core row- and column-level transforms: filter, select, add or modify columns, sort, deduplicate, and the string/date/list expression namespaces. Every FlowFrame method mirrors its Polars counterpart and accepts an optional `description` that shows up as node documentation in the visual editor.

The `flowfile_formula` examples below use the [Flowfile formula language](../../formulas/index.md); everything else is a [Polars expression](../concepts/expressions.md).

## A worked example

This runs against committed data and is executed by the docs test suite:

```python
--8<-- "docs/examples/python_api_operations.py:example"
```

The sections below break down each operation.

## Filtering

```python
import flowfile as ff

df = ff.FlowFrame({"price": [10, 20, 30], "qty": [5, 0, 10]})

# Polars expression predicate
df = df.filter(ff.col("price") > 15)

# With a description (surfaces in the visual editor)
df = df.filter(ff.col("price") > 15, description="Keep items over $15")

# Flowfile formula syntax
df = df.filter(flowfile_formula="[price] > 15 and [qty] > 0")
```

!!! note "Which node the filter becomes"
    `filter(flowfile_formula=...)` emits an editable Filter node. A plain `filter(ff.col(...) > x)` predicate emits a `polars_code` node instead — the result is identical, but only the formula form is editable in the visual editor.

## Selecting columns

```python
# Select specific columns by name
df = df.select(["price", "qty"])

# Select with expressions
df = df.select([
    ff.col("price"),
    ff.col("qty").alias("quantity"),
])

# Keep everything except one column with a column selector.
# There is no ff.exclude() — use ff.col("*").exclude(...) or the selectors module.
df = df.select(ff.col("*").exclude("internal_id"))
```

!!! tip "Selectors"
    `ff.numeric()`, `ff.string()`, `ff.all_()`, and the other [selector helpers](../concepts/expressions.md) pick columns by dtype or pattern — e.g. `df.select(ff.numeric())` keeps only numeric columns.

## Adding and modifying columns

```python
# Expression form
df = df.with_columns([
    (ff.col("price") * ff.col("qty")).alias("total"),
])

# Flowfile formula form
df = df.with_columns(
    flowfile_formulas=["[price] * [qty]"],
    output_column_names=["total"],
    description="Calculate line totals",
)
```

## Sorting

```python
df = df.sort("price")
df = df.sort("price", descending=True)

# Multi-column sort
df = df.sort(["category", "price"], descending=[False, True])
```

## Removing duplicates

```python
# Drop fully duplicate rows
df = df.unique()

# Deduplicate on a subset of columns
df = df.unique(subset=["product_id"])
```

!!! warning "No `drop_duplicates`"
    FlowFrame does not expose `drop_duplicates`. Use `unique()` (optionally with `subset=[...]` and `keep="first"`).

## Cleaning messy text

`data_cleansing()` is the Python form of the [Data Cleansing node](../../visual-editor/nodes/transform.md#data-cleansing): one call that fills nulls, trims and normalizes whitespace, strips unwanted characters, and fixes casing. Pass a list of column names to limit it; with no list it cleanses every column. Text rules only touch String columns and the null-to-zero rule only Numeric ones, so a mixed frame is safe to pass whole.

```python
--8<-- "docs/examples/data_cleansing.py:example"
```

`remove_null_rows` and `remove_null_columns` look at the whole frame regardless of the column list. `remove_null_columns` is the one data-dependent keyword: which columns go is decided from a null count taken when the method is called, not at `collect()`.

## String operations

```python
df = df.with_columns([
    ff.col("name").str.to_uppercase().alias("name_upper"),
    ff.col("code").str.slice(0, 3).alias("prefix"),
    ff.col("text").str.contains("pattern").alias("has_pattern"),
])
```

## Conditional logic

```python
df = df.with_columns([
    ff.when(ff.col("price") > 100)
    .then(ff.lit("Premium"))
    .when(ff.col("price") > 50)
    .then(ff.lit("Standard"))
    .otherwise(ff.lit("Budget"))
    .alias("tier"),
])
```

## Date operations

```python
df = df.with_columns([
    ff.col("date").dt.year().alias("year"),
    ff.col("date").dt.month().alias("month"),
    ff.col("date").dt.day().alias("day"),
    ff.col("date").dt.weekday().alias("weekday"),
])
```

!!! note "Polars renames apply"
    The expression namespaces track the pinned Polars version: use `dt.weekday()` (not `day_of_week`) and `cum_sum()` (not `cumsum`).

## List operations

```python
df = df.with_columns([
    ff.col("tags").list.len().alias("tag_count"),
    ff.col("values").list.sum().alias("total"),
    ff.col("items").list.first().alias("first_item"),
])
```

!!! note "Polars compatibility"
    Most Polars `Expr` methods are available. See the [Polars docs](https://pola-rs.github.io/polars/py-polars/html/reference/dataframe/index.html) for the full method reference; a few methods are renamed or fall back to `polars_code` nodes — see [Expressions](../concepts/expressions.md).

---
[← Previous: Data Types](data-types.md) | [Next: Aggregations →](aggregations.md)
