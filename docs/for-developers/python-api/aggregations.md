# Aggregations

Group by and aggregate operations for summarizing data.

## Basic Group By

```python
import flowfile as ff

df = ff.FlowFrame({
    "category": ["A", "B", "A", "B", "A"],
    "value": [10, 20, 30, 40, 50],
    "quantity": [1, 2, 3, 4, 5]
})

# Simple aggregation
result = df.group_by("category").agg([
    ff.col("value").sum().alias("total_value"),
    ff.col("value").mean().alias("avg_value"),
    ff.col("quantity").count().alias("count")
])

# With description
result = df.group_by("category", description="Group by product category").agg([
    ff.col("value").sum().alias("total_value")
])
```

## Multiple Grouping Columns

```python
result = df.group_by(["region", "category"]).agg([
    ff.col("sales").sum().alias("total_sales"),
    ff.col("sales").mean().alias("avg_sales")
])
```

## Complex Group By

```python
# Group by expression (creates polars_code node)
result = df.group_by([
    ff.col("date").dt.year().alias("year")
]).agg([
    ff.col("amount").sum()
])

# Dynamic aggregation
result = df.group_by("category").agg([
    ff.all().sum()  # Sum all numeric columns
])
```

## Available Aggregations

| Function | Description |
|----------|-------------|
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
| `list()` | Collect values into list |

## Window Functions

```python
# Running calculations
df = df.with_columns([
    ff.col("value").cumsum().over("category").alias("running_total"),
    ff.col("value").rank().over("category").alias("rank")
])
```

!!! note "Node Type Selection"
    Simple group_by operations create UI nodes. Complex expressions in group_by create `polars_code` nodes.