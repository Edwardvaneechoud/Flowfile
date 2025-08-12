# Joins

Combining data from multiple FlowFrames.

## Basic Join

```python
import flowfile as ff

customers = ff.FlowFrame({
    "id": [1, 2, 3],
    "name": ["Alice", "Bob", "Charlie"]
})

orders = ff.FlowFrame({
    "order_id": [101, 102, 103],
    "customer_id": [1, 2, 1],
    "amount": [100, 200, 150]
})

# Inner join
result = customers.join(
    orders,
    left_on="id",
    right_on="customer_id",
    how="inner",
    description="Join customers with orders"
)
```

## Join Types

```python
# Inner join (default)
df1.join(df2, on="key", how="inner")

# Left join
df1.join(df2, on="key", how="left")

# Outer join
df1.join(df2, on="key", how="outer")

# Semi join (filter df1 by df2)
df1.join(df2, on="key", how="semi")

# Anti join (exclude matches)
df1.join(df2, on="key", how="anti")
```

## Multiple Join Keys

```python
result = df1.join(
    df2,
    on=["region", "year"],  # Join on multiple columns
    how="inner"
)

# Different column names
result = df1.join(
    df2,
    left_on=["region_code", "period"],
    right_on=["region", "year"],
    how="left"
)
```

## Cross Join

```python
# Cartesian product
result = df1.join(df2, how="cross")
```

## Union/Concatenation

```python
# Vertical concatenation
combined = ff.concat([df1, df2, df3])

# Union (removes duplicates)
union_df = df1.unique().vstack(df2.unique()).unique()

# Diagonal concatenation (handles different schemas)
combined = ff.concat([df1, df2], how="diagonal")
```

## Join Validation

```python
# Check for duplicates before joining
if df2.select("customer_id").n_unique() < len(df2):
    print("Warning: duplicate keys in right table")

# Validate join results
result = df1.join(df2, on="id", how="left")
unmatched = result.filter(ff.col("amount").is_null())
print(f"Unmatched records: {len(unmatched)}")
```

!!! warning "Unsupported Join Types"
    Currently, `join_asof` and `join_where` are not supported in Flowfile. These operations will need to be implemented using alternative approaches or raw Polars code.

---
[← Previous: Aggregations](aggregations.md) | [Next: Cloud Connection →](cloud-connections.md)
