# Python API Quick Start

Get up and running with Flowfile's Python API in 5 minutes.

## Installation

```bash
pip install flowfile
```

## Your First Pipeline

```python
import flowfile as ff

# Load data
df = ff.read_csv("sales.csv", description="Load sales data")

# Transform
result = (
    df
    .filter(ff.col("amount") > 100, description="Filter large sales")
    .with_columns([
        (ff.col("amount") * 1.1).alias("amount_with_tax")
    ], description="Add tax calculation")
    .group_by("region")
    .agg([
        ff.col("amount").sum().alias("total_sales"),
        ff.col("amount").mean().alias("avg_sale")
    ])
)

# Get results as Polars DataFrame
data = result.collect()
print(data)

# Visualize in the UI
ff.open_graph_in_editor(result.flow_graph)
```

## Key Concepts

### FlowFrame
Your data container - like a Polars LazyFrame but tracks all operations:

```python
# Create from various sources
df = ff.FlowFrame({"col1": [1, 2, 3]})  # From dict
df = ff.read_csv("file.csv")            # From CSV
df = ff.read_parquet("file.parquet")    # From Parquet
```

### Always Lazy
Operations don't execute until you call `.collect()`:

```python
# These operations just build the plan
df = ff.read_csv("huge_file.csv")
df = df.filter(ff.col("status") == "active")
df = df.select(["id", "name", "amount"])

# Now it executes everything efficiently
result = df.collect()
```

### Descriptions
Document your pipeline as you build:

```python
df = (
    ff.read_csv("input.csv", description="Raw customer data")
    .filter(ff.col("active") == True, description="Keep active only")
    .drop_duplicates(description="Remove duplicates")
)
```

## Common Operations

### Filtering
```python
# Polars style
df.filter(ff.col("age") > 21)

# Flowfile formula style
df.filter(flowfile_formula="[age] > 21 AND [status] = 'active'")
```

### Adding Columns
```python
# Standard way
df.with_columns([
    (ff.col("price") * ff.col("quantity")).alias("total")
])

# Formula syntax
df.with_columns(
    flowfile_formulas=["[price] * [quantity]"],
    output_column_names=["total"]
)
```

### Grouping & Aggregation
```python
df.group_by("category").agg([
    ff.col("sales").sum().alias("total_sales"),
    ff.col("sales").mean().alias("avg_sales"),
    ff.col("id").count().alias("count")
])
```

### Joining
```python
customers = ff.read_csv("customers.csv")
orders = ff.read_csv("orders.csv")

result = customers.join(
    orders,
    left_on="customer_id",
    right_on="cust_id",
    how="left"
)
```

## Cloud Storage

```python
from pydantic import SecretStr

# Set up S3 connection
ff.create_cloud_storage_connection_if_not_exists(
    ff.FullCloudStorageConnection(
        connection_name="my-s3",
        storage_type="s3",
        auth_method="access_key",
        aws_region="us-east-1",
        aws_access_key_id="your-key",
        aws_secret_access_key=SecretStr("your-secret")
    )
)

# Read from S3
df = ff.scan_parquet_from_cloud_storage(
    "s3://bucket/data.parquet",
    connection_name="my-s3"
)

# Write to S3
df.write_parquet_to_cloud_storage(
    "s3://bucket/output.parquet",
    connection_name="my-s3"
)
```

## Visual Integration

### Open in Editor
```python
# Build pipeline in code
pipeline = ff.read_csv("data.csv").filter(ff.col("value") > 100)

# Open in visual editor
ff.open_graph_in_editor(pipeline.flow_graph)
```

### Start Web UI
```python
# Launch the web interface
ff.start_web_ui()  # Opens browser automatically
```

## Complete Example

```python
import flowfile as ff

# Build a complete ETL pipeline
pipeline = (
    ff.read_csv("raw_sales.csv", description="Load raw sales")
    .filter(ff.col("amount") > 0, description="Remove invalid")
    .with_columns([
        ff.col("date").str.strptime(ff.Date, "%Y-%m-%d"),
        (ff.col("amount") * ff.col("quantity")).alias("total")
    ], description="Parse dates and calculate totals")
    .group_by([ff.col("date").dt.year().alias("year"), "product"])
    .agg([
        ff.col("total").sum().alias("revenue"),
        ff.col("quantity").sum().alias("units_sold")
    ])
    .sort("revenue", descending=True)
)

# Execute and get results
results = pipeline.collect()
print(results)

# Visualize the pipeline
ff.open_graph_in_editor(pipeline.flow_graph)

# Save results
pipeline.write_parquet("yearly_sales.parquet")
```

## Next Steps

- 📖 [Core Concepts](concepts/design-concepts.md) - Understand FlowFrame and FlowGraph
- 📚 [API Reference](reference/index.md) - Detailed documentation
- 🎯 [Tutorials](tutorials/index.md) - Real-world examples
- 🔄 [Visual Integration](reference/visual-ui.md) - Working with the UI

## Tips

1. **Use descriptions** - They appear in the visual editor
2. **Think lazy** - Build your entire pipeline before collecting
3. **Leverage formulas** - Use `[column]` syntax for simpler expressions
4. **Visualize often** - `open_graph_in_editor()` helps debug
5. **Check schemas** - Use `df.schema` to see structure without running

---

*Ready for more? Check out the [full API reference](reference/index.md) or learn about [core concepts](concepts/design-concepts.md).*