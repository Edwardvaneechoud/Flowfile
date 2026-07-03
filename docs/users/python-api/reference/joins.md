# Joins

Combine two FlowFrames on one or more keys, stack frames vertically, and validate keys before joining. Joins support the strategies `inner`, `left`, `right`, `full`, `semi`, `anti`, `outer`, plus `cross`.

## A worked example

This runs against committed data and is executed by the docs test suite:

```python
--8<-- "docs/examples/python_api_joins.py:example"
```

The sections below break down each pattern.

## Basic join

```python
import flowfile as ff

customers = ff.FlowFrame({
    "id": [1, 2, 3],
    "name": ["Alice", "Bob", "Charlie"],
})

orders = ff.FlowFrame({
    "order_id": [101, 102, 103],
    "customer_id": [1, 2, 1],
    "amount": [100, 200, 150],
})

result = customers.join(
    orders,
    left_on="id",
    right_on="customer_id",
    how="inner",
    description="Join customers with orders",
)
```

## Join types

```python
# Inner join (default): rows with a match on both sides
df1.join(df2, on="key", how="inner")

# Left join: all left rows, matched right columns or null
df1.join(df2, on="key", how="left")

# Right join: all right rows
df1.join(df2, on="key", how="right")

# Full join: all rows from both sides (unmatched filled with null)
df1.join(df2, on="key", how="full")

# Semi join: left rows that have a match (keeps only left columns)
df1.join(df2, on="key", how="semi")

# Anti join: left rows with no match
df1.join(df2, on="key", how="anti")
```

!!! note "`full`, not `outer`"
    Under the pinned Polars version the full outer strategy is `how="full"`. `how="outer"` is accepted as a legacy alias, but new code should use `full`.

## Multiple join keys

```python
result = df1.join(
    df2,
    on=["region", "year"],  # join on several columns
    how="inner",
)

# Different column names on each side
result = df1.join(
    df2,
    left_on=["region_code", "period"],
    right_on=["region", "year"],
    how="left",
)
```

## Cross join

```python
# Cartesian product of both frames
result = df1.join(df2, how="cross")
```

## Stacking frames

There is no `vstack` on FlowFrame — use `ff.concat` to stack frames vertically:

```python
# Vertical concatenation
combined = ff.concat([df1, df2, df3])

# Concatenate then drop duplicate rows
union_df = ff.concat([df1, df2]).unique()

# Diagonal concatenation aligns differing schemas
combined = ff.concat([df1, df2], how="diagonal")
```

## Join validation

FlowFrame has no frame-level `len()` or `n_unique()`. Count distinct keys with an expression inside `select`, and count matched rows by collecting a small aggregate rather than taking `len()` of a frame:

```python
# Are there duplicate keys in the right table?
key_stats = df2.select(
    ff.col("customer_id").n_unique().alias("distinct_keys"),
    ff.col("customer_id").count().alias("total_rows"),
).collect()
if key_stats["distinct_keys"][0] < key_stats["total_rows"][0]:
    print("Warning: duplicate keys in right table")

# How many left rows found no match?
result = df1.join(df2, on="id", how="left")
unmatched = result.filter(ff.col("amount").is_null()).select(
    ff.col("id").count().alias("unmatched")
).collect()
print(f"Unmatched records: {unmatched['unmatched'][0]}")
```

!!! warning "`join_asof` and `join_where` are not supported"
    These methods are present on FlowFrame (they are injected from Polars) but raise at call time — Flowfile has no native node for them. Use raw Polars in a Python Script node for time- or predicate-based joins.

---
[← Previous: Aggregations](aggregations.md) | [Next: Cloud Storage →](cloud-connections.md)
