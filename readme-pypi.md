<h1 align="center">
  <img src="https://raw.githubusercontent.com/Edwardvaneechoud/Flowfile/main/.github/images/logo.png" alt="Flowfile Logo" width="100">
  <br>
  Flowfile
</h1>

<p align="center">
  <b>Main Repository</b>: <a href="https://github.com/Edwardvaneechoud/Flowfile">Edwardvaneechoud/Flowfile</a><br>
  <b>Documentation</b>:
  <a href="https://edwardvaneechoud.github.io/Flowfile/">Website</a> -
  <a href="https://github.com/Edwardvaneechoud/Flowfile/blob/main/flowfile_core/README.md">Core</a> -
  <a href="https://github.com/Edwardvaneechoud/Flowfile/blob/main/flowfile_worker/README.md">Worker</a> -
  <a href="https://github.com/Edwardvaneechoud/Flowfile/blob/main/flowfile_frontend/README.md">Frontend</a> -
  <a href="https://dev.to/edwardvaneechoud/building-flowfile-architecting-a-visual-etl-tool-with-polars-576c">Technical Architecture</a>
</p>

<p>
Flowfile is an open-source data platform that combines a visual pipeline builder, a data catalog with Delta Lake storage, scheduling, Kafka ingestion, sandboxed Python execution, and a Polars-compatible Python API — all in a single <code>pip install</code>.
</p>

## Quick Start

```bash
pip install Flowfile
flowfile run ui
```

This starts the backend services and opens the visual ETL interface in your browser.

## What You Get

- **Visual pipeline builder** with 30+ nodes for joins, filters, aggregations, fuzzy matching, pivots, and more
- **Data catalog** with Delta Lake storage, version history, and lineage tracking
- **Scheduling** — interval-based or triggered by catalog table updates
- **Kafka/Redpanda ingestion** as a canvas node with automatic schema inference
- **Sandboxed Python execution** in isolated Docker containers
- **Code generation** — export visual flows as standalone Python/Polars scripts
- **Flow parameters** — `${variable}` substitution, configurable via UI or CLI
- **Cloud storage** — S3, Azure Data Lake Storage, Google Cloud Storage
- **Database connectivity** — PostgreSQL, MySQL, SQL Server, Oracle, DuckDB, and more
- **Python API** with Polars-like syntax and visual flow graph generation

## Python API

```python
import flowfile as ff
from flowfile import col, open_graph_in_editor

df = ff.from_dict({
    "id": [1, 2, 3, 4, 5],
    "category": ["A", "B", "A", "C", "B"],
    "value": [100, 200, 150, 300, 250]
})

result = df.filter(col("value") > 150).with_columns([
    (col("value") * 2).alias("double_value")
])

# Open the pipeline on the visual canvas
open_graph_in_editor(result.flow_graph)
```

## Common Operations

```python
import flowfile as ff
from flowfile import col, when, lit

# Read from various sources
df = ff.read_csv("data.csv")
df_pq = ff.read_parquet("data.parquet")

# Transform
filtered = df.filter(col("value") > 150)
with_status = df.with_columns([
    when(col("value") > 200).then(lit("High")).otherwise(lit("Low")).alias("status")
])

# Aggregate
by_category = df.group_by("category").agg([
    col("value").sum().alias("total"),
    col("value").mean().alias("average")
])

# Join
joined = df.join(other_df, left_on="id", right_on="product_id")

# Visualize any pipeline
ff.open_graph_in_editor(joined.flow_graph)
```

## Code Generation

Export visual flows as standalone Python/Polars scripts:

![Code Generation](https://raw.githubusercontent.com/Edwardvaneechoud/Flowfile/refs/heads/main/.github/images/generated_code.png)

## Package Components

- **Core Service** (`flowfile_core`) — ETL engine, catalog, scheduler, auth
- **Worker Service** (`flowfile_worker`) — CPU-intensive data processing
- **Web UI** — Browser-based visual pipeline builder
- **FlowFrame API** (`flowfile_frame`) — Polars-compatible Python library
- **Scheduler** (`flowfile_scheduler`) — Interval and table-trigger scheduling

## CLI

```bash
flowfile run ui                              # Start web UI
flowfile run core --host 0.0.0.0             # Start core service
flowfile run worker --host 0.0.0.0           # Start worker service
flowfile run flow pipeline.json              # Run a flow
flowfile run flow pipeline.json --param key=value  # Run with parameters
```

## More Options

- **Desktop App**: Download from [GitHub Releases](https://github.com/Edwardvaneechoud/Flowfile#-getting-started)
- **Docker**: `docker compose up -d` for self-hosted deployments
- **Browser Demo**: [demo.flowfile.org](https://demo.flowfile.org) (WASM, no server)

## Resources

- **[Documentation](https://edwardvaneechoud.github.io/Flowfile/)**: Comprehensive guides
- **[Main Repository](https://github.com/Edwardvaneechoud/Flowfile)**: Latest code and examples
- **[Technical Architecture](https://dev.to/edwardvaneechoud/building-flowfile-architecting-a-visual-etl-tool-with-polars-576c)**: Design overview
