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
Flowfile is a visual ETL tool and Python library suite that combines drag-and-drop workflow building with the speed of Polars dataframes. Build data pipelines visually, transform data using powerful nodes, or define data flows programmatically with Python and analyze results - all with high-performance data processing.
</p>

<div align="center">
  <img src="https://raw.githubusercontent.com/Edwardvaneechoud/Flowfile/main/.github/images/group_by_screenshot.png" alt="Flowfile Interface" width="800"/>
</div>

## ⚡ Technical Design

The `Flowfile` PyPI package provides the backend services and the `flowfile_frame` Python library:

- **Core (`flowfile_core`)** (FastAPI): The main ETL engine using Polars for high-performance data transformations. Typically runs on port `:63578`.
- **Worker (`flowfile_worker`)** (FastAPI): Handles computation-intensive tasks and caching of data operations, supporting the Core service. Typically runs on port `:63579`.
- **FlowFrame API (`flowfile_frame`)**: A Python library with a Polars-like API for defining data manipulation pipelines programmatically, which also generates an underlying ETL graph compatible with the Flowfile ecosystem.

Each flow is represented as a directed acyclic graph (DAG), where nodes represent data operations and edges represent data flow between operations.

For a deeper dive into the technical architecture, check out [this article](https://dev.to/edwardvaneechoud/building-flowfile-architecting-a-visual-etl-tool-with-polars-576c) on how Flowfile leverages Polars for efficient data processing.

## ✨ Introducing FlowFile Frame - A Polars-Like API for ETL

FlowFile Frame is a Python library that provides a familiar Polars-like API for data manipulation, while simultaneously building an ETL (Extract, Transform, Load) graph under the hood. This allows you to:

1. Write data transformation code using a simple, Pandas/Polars-like API
2. Automatically generate executable ETL workflows compatible with the Flowfile ecosystem
3. Visualize, save, and share your data pipelines
4. Get the performance benefits of Polars with the traceability of ETL graphs

### FlowFrame Quick Start

```python
import flowfile_frame as ff
from flowfile_frame.utils import open_graph_in_editor

# Create a complex data pipeline
df = ff.from_dict({
    "id": [1, 2, 3, 4, 5],
    "category": ["A", "B", "A", "C", "B"],
    "value": [100, 200, 150, 300, 250]
})

open_graph_in_editor(df.flow_graph)

```

### Key FlowFrame Features

- **Familiar API**: Based on Polars, making it easy to learn if you know Pandas or Polars
- **ETL Graph Generation**: Automatically builds a directed acyclic graph of your data operations
- **Lazy Evaluation**: Operations are not executed until `collect()` or a write operation
- **Interoperability**: Saved `.flowfile` graphs can be opened in the visual Flowfile Designer
- **High Performance**: Leverages Polars for fast data processing
- **Reproducible**: Save and share your data transformation workflows

### Common FlowFrame Operations

```python
import flowfile_frame as ff
from flowfile_frame import col, when

# Create from dictionary
df = ff.from_dict({
    "id": [1, 2, 3],
    "name": ["Alice", "Bob", "Charlie"],
    "age": [25, 35, 28]
})

flow_graph = df.flow_graph
# Reading data
# df_csv = ff.read_csv("data.csv")
# df_parquet = ff.read_parquet("data.parquet")

# Filtering
adults = df.filter(col("age") >= 30)

# Select and transform
result = df.select(
    col("name"),
    (col("age") * 2).alias("double_age")
)

# Add new columns
df_with_cols = df.with_columns([
    (col("age") + 10).alias("future_age"),
    when(col("age") >= 30).then(ff.lit("Senior")).otherwise(ff.lit("Junior")).alias("status")]
)

# Group by and aggregate
df_sales = ff.from_dict({
    "region": ["North", "South", "North", "South"],
    "sales": [100, 200, 150, 300]
})
sales_by_region = df_sales.group_by("region").agg([
    col("sales").sum().alias("total_sales"),
    col("sales").mean().alias("avg_sales")
])

# Joins
customers = ff.from_dict({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]}, flow_graph=flow_graph)
orders = ff.from_dict({"id": [101, 102], "customer_id": [1, 2], "amount": [100, 200]}, flow_graph=flow_graph)
joined = customers.join(orders, left_on="id", right_on="customer_id")

# Save and visualize ETL graph

result.save_graph("my_pipeline.flowfile")
# open_graph_in_editor(result.flow_graph, "my_pipeline.flowfile") # Opens in Designer UI if installed
```

For more detailed information on all available operations, including pivoting, window functions, complex workflows, and more, please refer to the [FlowFrame documentation](https://github.com/Edwardvaneechoud/Flowfile/blob/main/flowfile_frame/README.md).

## 🔥 Example Use Cases

Flowfile is great for:

- **Data Cleaning & Transformation**
  - Complex joins (fuzzy matching)
  - Text-to-rows transformations
  - Advanced filtering and grouping
  - Custom formulas and expressions
  - Filter data based on conditions

- **Performance**
  - Built to scale out of core
  - Using Polars for data processing

- **Data Integration**
  - Standardize data formats
  - Handle messy Excel files 

- **ETL Operations**
  - Data quality checks

(For more visual examples of these use cases, please see our [main GitHub repository](https://github.com/Edwardvaneechoud/Flowfile#-example-use-cases)).

## 🚀 Getting Started

### Installing the Flowfile Python Package

This package provides the `flowfile_core` and `flowfile_worker` backend services, and the `flowfile_frame` library.

```bash
pip install Flowfile
```

Once installed, you can use `flowfile_frame` as a library in your Python scripts (see Quick Start above).

### Full Application with Visual Designer

For the complete visual ETL experience with the Designer UI, please see the [installation instructions in the main repository](https://github.com/Edwardvaneechoud/Flowfile#-getting-started).

Available options include:
- Desktop application (recommended for most users)
- Docker setup (backend services + web frontend)
- Manual setup for development

## 📋 Development Roadmap

For the latest development roadmap and TODO list, please refer to the [main repository](https://github.com/Edwardvaneechoud/Flowfile#-todo).
