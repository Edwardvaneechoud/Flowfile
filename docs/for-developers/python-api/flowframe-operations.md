# DataFrame Operations

Core operations for transforming data. All standard Polars operations are supported with additional Flowfile features.

## Filtering

```python
import flowfile as ff

df = ff.FlowFrame({"price": [10, 20, 30], "qty": [5, 0, 10]})

# Standard Polars filter
df = df.filter(ff.col("price") > 15)

# With description
df = df.filter(ff.col("price") > 15, description="Keep items over $15")

# Flowfile formula syntax
df = df.filter(flowfile_formula="[price] > 15 AND [qty] > 0")
```

## Selecting Columns

```python
# Select specific columns
df = df.select(["price", "qty"])

# Select with expressions
df = df.select([
    ff.col("price"),
    ff.col("qty").alias("quantity")
])

# Exclude columns
df = df.select(ff.exclude("internal_id"))
```

## Adding/Modifying Columns

```python
# Standard with_columns
df = df.with_columns([
    (ff.col("price") * ff.col("qty")).alias("total")
])

# Flowfile formula syntax
df = df.with_columns(
    flowfile_formulas=["[price] * [qty]"],
    output_column_names=["total"],
    description="Calculate line totals"
)
```

## Sorting

```python
# Sort by column
df = df.sort("price")
df = df.sort("price", descending=True)

# Multi-column sort
df = df.sort(["category", "price"], descending=[False, True])
```

## Unique Operations

```python
# Get unique rows
df = df.unique()

# Unique by specific columns
df = df.unique(subset=["product_id"])

# Drop duplicates (alias)
df = df.drop_duplicates(subset=["product_id"])
```

!!! note "Polars Compatibility"
    All standard Polars DataFrame methods work identically. See [Polars docs](https://pola-rs.github.io/polars/py-polars/html/reference/dataframe/index.html) for complete reference.

---
[← Previous: Data Types](data-types.md) | [Next: Expressions →](expressions.md)
