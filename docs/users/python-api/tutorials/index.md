# Python API Tutorials

Worked, hands-on pipelines built with the Python API. Start with the code-to-flow walkthrough, then use the short patterns below as building blocks.

## Tutorial

### [Building Flows with Code](flowfile_frame_api.md)

Build a pipeline programmatically and open it as a visual graph in the Designer. Covers `from_dict`, expressions, conditional logic, grouping, and `open_graph_in_editor`.

## Patterns

These fragments are illustrative — adapt the paths and columns to your data. For fully tested, runnable pipelines see the reference pages' worked examples ([operations](../reference/flowframe-operations.md), [joins](../reference/joins.md), [aggregations](../reference/aggregations.md)).

### ETL pipeline

```python
import flowfile as ff

# Extract
raw_data = ff.read_csv("sales.csv")

# Transform
transformed = (
    raw_data
    .filter(ff.col("amount") > 0)
    .with_columns([
        ff.col("date").str.strptime(ff.Date, "%Y-%m-%d")
    ])
    .group_by("region")
    .agg(ff.col("amount").sum())
)

# Load
transformed.write_parquet("output.parquet")
```

### Data validation

```python
df = ff.read_csv("input.csv")

# Find duplicate keys
duplicates = df.group_by("id").agg(
    ff.count().alias("count")
).filter(ff.col("count") > 1)

# Count nulls per column
null_counts = df.select([
    ff.col(c).is_null().sum().alias(f"{c}_nulls")
    for c in df.columns
])
```

## Related

- [API Reference](../reference/index.md) — method-by-method documentation
- [Core Concepts](../concepts/index.md) — the FlowFrame and FlowGraph model
- [Quick Start](../quickstart.md) — install and build a first pipeline
