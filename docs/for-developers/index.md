# **Flowfile: For Developers**

Welcome to the developer documentation for Flowfile. This is the home for anyone who wants to contribute to the platform or understand its internal architecture.

!!! note "Looking to use the Python API?"
    If you want to **use** Flowfile's Python API to build data pipelines, check out the [Python API User Guide](../users/python-api/index.md). This developer section focuses on Flowfile's internal architecture and design philosophy.

---
## The Core Philosophy: Code and UI are the Same Thing

Flowfile is built on an architecture where the Python API and the visual editor are two interfaces to the exact same underlying objects: the **`FlowGraph`** and its **`FlowNodes`**.

When you write `df.filter(...)`, you programmatically construct a `FlowNode` and attach it to the `FlowGraph`. When a user drags a "Filter" node in the UI, they create the identical object. This **dual interface philosophy** means your work is never locked into one paradigm.

- **`FlowGraph`**: The central orchestrator that holds the complete definition of your pipeline—every node, setting, and connection.
- **`FlowNode`**: An individual, executable step in your pipeline that wraps settings and logic.
- **`FlowDataEngine`**: A smart wrapper around a Polars `LazyFrame` that carries the data and its schema between nodes.

Learn more about this in our **[Dual Interface Philosophy](design-philosophy.md)** guide.

---
## Getting Started with Development

### 1. Prerequisites
To contribute to Flowfile, you should be familiar with:

- **Required Knowledge**: Python 3.10+, and a basic familiarity with Polars or Pandas.
- **Helpful Knowledge**: Experience with Polars LazyFrames, Directed Acyclic Graphs (DAGs), and Pydantic.

### 2. Set Up Your Environment

Before diving in, clone the repository and install the dependencies using Poetry:

```bash
# For development/contributing
git clone [https://github.com/edwardvaneechoud/Flowfile](https://github.com/edwardvaneechoud/Flowfile)
cd Flowfile
poetry install
```

### 3. See It in Action: A Quick Example
The following code builds a data pipeline using the Python API. This same pipeline can be generated visually in the UI.

```python
import flowfile as ff
from flowfile import col

# Create a FlowFrame from a local CSV
df = ff.read_csv("sales_data.csv", description="Load raw sales data")

# Build a transformation pipeline with a familiar, chainable API
processed_sales = (
    df.filter(col("amount") > 100, description="Filter for significant sales")
    .with_columns(
        (col("quantity") * col("price")).alias("total_revenue")
    )
    .group_by("region", description="Aggregate sales by region")
    .agg(
        col("total_revenue").sum()
    )
)

# Get your results as a Polars DataFrame
results_df = processed_sales.collect()
print(results_df)
```

## Documentation Guides

- **[Core Architecture](flowfile-core.md)**: A deep dive into how `FlowGraph`, `FlowNode`, and `FlowDataEngine` work together.
- **[Technical Architecture](architecture.md)**: An overview of the system design, including the three-service architecture and performance optimizations.
- **[Python API Reference](python-api-reference.md)**: The complete, auto-generated API reference for all core classes and methods.
- **[Visual UI Integration](../users/python-api/reference/visual-ui.md)**: Learn how to launch and control the visual editor from Python.
---
## Contributing to Flowfile

We welcome contributions! Adding a new node requires changes across the stack:

- **Backend**: You'll need to define Pydantic setting models, implement the transformation logic in the `FlowDataEngine`, and register the new node in the `FlowGraph`.
- **Frontend**: Currently, you must also manually create a Vue component for the node's configuration form in the visual editor.

For a more detailed breakdown, please read the **[Contributing section in our Design Philosophy guide](design-philosophy.md#contributing)**.

