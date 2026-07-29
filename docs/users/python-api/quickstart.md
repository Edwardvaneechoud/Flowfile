# Python API Quick Start

Install the package, build a first pipeline, and tour the operations you'll reach for most.

## Installation

```bash
pip install flowfile
```

## Your first pipeline

This pipeline reads a CSV, derives a column, filters, and aggregates. It runs against committed sample data and is executed by the docs test suite:

```python
--8<-- "docs/examples/first_pipeline.py:example"
```

`collect()` runs the plan and returns a Polars `DataFrame`.

## Key ideas

### FlowFrame

Your data container — like a Polars `LazyFrame`, but every operation is also recorded as a graph node:

```python
import flowfile as ff

df = ff.FlowFrame({"col1": [1, 2, 3]})  # from a dict
df = ff.read_csv("file.csv")            # from CSV
df = ff.read_parquet("file.parquet")    # from Parquet
```

### Always lazy

Operations don't execute until you call `.collect()`:

```python
# These calls just build the plan
df = ff.read_csv("huge_file.csv")
df = df.filter(ff.col("status") == "active")
df = df.select(["id", "name", "amount"])

# Now it executes, reading only what's needed
result = df.collect()
```

Check `df.schema` to see column types without running anything.

### Descriptions

Any operation accepts a `description` that shows up on the node in the visual editor:

```python
df = (
    ff.read_csv("input.csv", description="Raw customer data")
    .filter(ff.col("active") == True, description="Keep active only")
    .unique(description="Remove duplicates")
)
```

## Common operations

### Filtering

```python
# Polars expression predicate
df.filter(ff.col("age") > 21)

# Flowfile formula (renders as an editable Filter node)
df.filter(flowfile_formula="[age] > 21 and [status] = 'active'")
```

### Adding columns

```python
# Expression form
df.with_columns([
    (ff.col("price") * ff.col("quantity")).alias("total")
])

# Formula form
df.with_columns(
    flowfile_formulas=["[price] * [quantity]"],
    output_column_names=["total"],
)
```

### Grouping and aggregation

```python
df.group_by("category").agg([
    ff.col("sales").sum().alias("total_sales"),
    ff.col("sales").mean().alias("avg_sales"),
    ff.col("id").count().alias("count"),
])
```

### Joining

```python
customers = ff.read_csv("customers.csv")
orders = ff.read_csv("orders.csv")

result = customers.join(
    orders,
    left_on="customer_id",
    right_on="customer_id",
    how="left",
)
```

See the [Joins reference](reference/joins.md) for the full set of strategies.

## Cloud storage

Store an S3 connection once, then read and write with it by name. This tested example round-trips a Parquet aggregate through S3:

```python
--8<-- "docs/examples/integrations/cloud_storage_s3.py:example"
```

!!! info "Local S3-compatible stacks vs plain S3"
    That example sets `endpoint_url`, `aws_allow_unsafe_html=True`, and inline keys to reach a local MinIO stack. Against real AWS S3, the connection needs only its `connection_name`, `storage_type`, `auth_method`, `aws_region`, and credentials — no endpoint URL and no unsafe-HTML flag. Once the connection exists, reads and writes just reference it by name. See [Cloud Connection Management](reference/cloud-connections.md).

## Visual integration

### Open in the editor

```python
pipeline = ff.read_csv("data.csv").filter(ff.col("value") > 100)
ff.open_graph_in_editor(pipeline.flow_graph)
```

### Start the web UI

```python
ff.start_web_ui()  # opens a browser tab
```

## Next steps

- [Core Concepts](concepts/design-concepts.md) — the FlowFrame and FlowGraph model
- [API Reference](reference/index.md) — method-by-method documentation
- [Tutorials](tutorials/index.md) — worked pipelines
- [Visual UI Integration](reference/visual-ui.md) — moving between code and the editor
