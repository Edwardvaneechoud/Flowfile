# Aggregations

Group rows and summarize them with `group_by().agg()`, and compute per-group running values with window functions. A `group_by` followed by `agg` fuses into a single group_by node in the graph, however many aggregations it carries.

## A worked example

This runs against committed data and is executed by the docs test suite:

```python
--8<-- "docs/examples/python_api_aggregations.py:example"
```

The sections below break down each part.

## Basic group by

```python
import flowfile as ff

df = ff.FlowFrame({
    "category": ["A", "B", "A", "B", "A"],
    "value": [10, 20, 30, 40, 50],
    "quantity": [1, 2, 3, 4, 5],
})

result = df.group_by("category").agg([
    ff.col("value").sum().alias("total_value"),
    ff.col("value").mean().alias("avg_value"),
    ff.col("quantity").count().alias("count"),
])

# A description shows up on the group_by node in the visual editor.
result = df.group_by("category", description="Group by product category").agg([
    ff.col("value").sum().alias("total_value"),
])
```

## Multiple grouping columns

```python
result = df.group_by(["region", "category"]).agg([
    ff.col("sales").sum().alias("total_sales"),
    ff.col("sales").mean().alias("avg_sales"),
])
```

## Grouping by an expression

```python
# Grouping by an expression emits a polars_code node (not an editable group_by node).
result = df.group_by([
    ff.col("date").dt.year().alias("year"),
]).agg([
    ff.col("amount").sum(),
])
```

## Aggregating every numeric column

The `ff.all_()` selector expands to all columns. Use it inside `select` (not directly inside a native `group_by().agg()`, which needs a concrete column name per aggregation):

```python
# Sum a whole frame of numeric columns
totals = df.select(ff.numeric()).select(ff.all_().sum())
```

## Available aggregations

| Method | Description |
|--------|-------------|
| `sum()` | Sum of values |
| `mean()` | Average value |
| `median()` | Median value |
| `min()` | Minimum value |
| `max()` | Maximum value |
| `count()` | Count of non-null values |
| `std()` | Standard deviation |
| `var()` | Variance |
| `first()` | First value in group |
| `last()` | Last value in group |
| `implode()` | Collect the group's values into a list |

!!! note "`implode`, not `list`"
    To gather a group's values into a single list column, use `implode()`. `.list` is a namespace accessor for existing list columns, not an aggregation.

## Window functions

Window functions compute a value per row *within* a group, without collapsing the frame. Use `.over(...)`:

```python
df = df.with_columns([
    ff.col("value").cum_sum().over("category").alias("running_total"),
    ff.col("value").rank(descending=True).over("category").alias("rank"),
])
```

!!! note "Use `cum_sum`, not `cumsum`"
    The cumulative-sum expression follows the pinned Polars name: `cum_sum()`. `cumsum()` raises `AttributeError`.

---
[← Previous: DataFrame Operations](flowframe-operations.md) | [Next: Joins →](joins.md)
