# Python API Reference

Complete reference documentation for Flowfile's Python API.

## Core Operations

### 📥 [Reading Data](reading-data.md)
Load data from files, databases, and cloud storage.
- `read_csv()`, `read_parquet()`, `read_json()`
- `scan_*` methods for lazy loading
- Cloud storage readers with S3 support

### 📤 [Writing Data](writing-data.md)
Save results to various formats and destinations.
- `write_csv()`, `write_parquet()`, `write_json()`
- Cloud storage writers
- Delta Lake support

### 📊 [Data Types](data-types.md)
Supported data types and conversions.
- Numeric, string, datetime types
- Type casting and inference
- Null handling

## Transformations

### 🔧 [DataFrame Operations](flowframe-operations.md)
Core methods for data manipulation.
- `filter()`, `select()`, `sort()`
- `with_columns()`, `drop()`, `rename()`
- `unique()`, `drop_duplicates()`

### 📝 [Expressions](expressions.md)
Column operations and formula syntax.
- Column references with `ff.col()`
- Flowfile formula syntax `[column]`
- Mathematical and string operations
- Conditional logic

### 📈 [Aggregations](aggregations.md)
Grouping and summarization.
- `group_by()`, `agg()`
- Aggregation functions (sum, mean, count, etc.)
- Window functions

### 🔗 [Joins](joins.md)
Combining multiple datasets.
- Join types (inner, left, right, outer)
- Multiple join keys
- Cross joins and unions

## Advanced Features

### ☁️ [Cloud Storage](cloud-connections.md)
Working with S3 and cloud data.
- Connection management
- Secure credential storage
- Reading and writing cloud data

### 🖼️ [Visual Integration](visual-ui.md)
Connecting code with the visual editor.
- `open_graph_in_editor()`
- Starting the web UI
- Server management

## Quick Reference

### Creating DataFrames
```python
# From dictionary
df = ff.FlowFrame({"col1": [1, 2], "col2": [3, 4]})

# From file
df = ff.read_csv("data.csv")

# From cloud
df = ff.scan_parquet_from_cloud_storage("s3://bucket/data.parquet")
```

### Common Patterns
```python
# Filter and transform
result = (
    df
    .filter(ff.col("value") > 100)
    .with_columns([
        ff.col("text").str.to_uppercase()
    ])
    .select(["id", "text", "value"])
)

# Aggregate
summary = df.group_by("category").agg([
    ff.col("amount").sum(),
    ff.col("amount").mean()
])

# Join
merged = df1.join(df2, on="key", how="left")
```

### Execution
```python
# Get results as Polars DataFrame
data = df.collect()

# Write to file
df.write_parquet("output.parquet")

# Visualize
ff.open_graph_in_editor(df.flow_graph)
```

## API Compatibility

Flowfile's API is designed to be **Polars-compatible** with these extensions:

1. **Description parameter** - Document operations
2. **Flowfile formulas** - Excel-like syntax
3. **Visual integration** - Graph visualization
4. **Cloud storage** - Built-in S3 support

Most Polars code works with minimal changes:

```python
# Polars
import polars as pl
df = pl.read_csv("data.csv")
df = df.filter(pl.col("x") > 5)

# Flowfile (just change imports)
import flowfile as ff
df = ff.read_csv("data.csv")
df = df.filter(ff.col("x") > 5)
```

## Getting Help

- **Not finding a method?** Check the [Polars documentation](https://pola-rs.github.io/polars/py-polars/html/reference/) - most methods work identically
- **Need examples?** See our [tutorials](../tutorials/)
- **Understanding concepts?** Read about [FlowFrame and FlowGraph](../concepts/design-concepts.md)

---

*This reference covers Flowfile-specific features. For standard Polars operations, see the [Polars API Reference](https://pola-rs.github.io/polars/py-polars/html/reference/).*