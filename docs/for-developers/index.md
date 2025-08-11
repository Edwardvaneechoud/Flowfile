# **Flowfile: For Developers**

Welcome to the developer documentation for Flowfile. This is the home for anyone who wants to build, manage, and understand data pipelines using Python, made with Flowfile.

Flowfile provides a high-performance, Polars-compatible API that is extended visual integration, cloud connectivity, and analytics.
It's for analytics developers, business users and engineers who want to create an unified way of working of doing analytics.

!!! info "If You Know Polars, You Know Flowfile" 
    Our API is designed to be a seamless extension of Polars. The majority of the methods are identical, so you can leverage your existing knowledge to be productive from day one. The main additions are features that connect your code to the broader Flowfile ecosystem, like cloud integrations and UI visualization.


## Prerequisites

Before diving into the developer documentation:

```bash
# Install Flowfile
pip install flowfile

# For development/contributing
git clone https://github.com/edwardvaneechoud/Flowfile
cd Flowfile
poetry install
```

**Required Knowledge:**

- Python 3.10+
- Basic familiarity with Polars or Pandas
- Understanding of DataFrames and lazy evaluation (helpful but not required)

**Optional but Helpful:**

- Experience with Polars LazyFrames
- Understanding of DAGs (Directed Acyclic Graphs)
- Familiarity with Pydantic for data validation


## **Quick Start: Code-First ETL**

Install the library and build your first pipeline in minutes.

```pip install flowfile```

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

Want to see this visually? Check out our [Code to Flow guide](../guides/flowfile_frame_api.md) to see how this code automatically creates a visual pipeline.

## **The Core Philosophy: Code and UI are the Same Thing**

Flowfile is built on an architecture where the Python API and the visual editor are two interfaces to the exact same underlying objects: the FlowGraph and its FlowNodes.

- **[FlowFrame](python-api/index.md)**: A wrapper around a Polars LazyFrame that automatically builds a FlowGraph as you chain operations.

- **[FlowGraph](core/python-api-reference.md#flowfile_core.flowfile.flow_graph.FlowGraph)**: The central orchestrator that holds the complete definition of your pipeline—every node, setting, and connection.

When you write `df.filter(...)`, you are programmatically constructing a FlowNode with specific settings and attaching it to the FlowGraph. When a user drags a "Filter" node in the UI, they are creating the identical object. This **dual interface philosophy** means your work is never locked into one paradigm. Learn more about this in our [Bridge Code and Visual](flowfile-for-developers.md).


## **Key Features for Developers**

- **Polars-Powered Performance:** Leverage the full speed of the Polars engine for all your data transformations. See our [technical architecture](../guides/technical_architecture.md) for details on lazy evaluation.

- **Seamless Visual Debugging:** Use `open_graph_in_editor()` at any point in your script to instantly see your pipeline's structure, inspect intermediate data schemas, and identify issues visually. [Learn more →](python-api/visual-ui.md)

- **Integrated Cloud Connectors:** Read and write data from AWS S3 with simple, high-level functions, without worrying about the underlying connection logic. [See cloud storage guide →](python-api/cloud-connection-management.md)

- **Secure Credential Management:** Handle database passwords and API keys securely using the built-in connection manager, which keeps secrets out of your code. [Database connectivity guide →](../guides/database_connectivity.md)

- **Self-Documenting Pipelines:** The description parameter in every function annotates the corresponding node in the UI, making your pipelines understandable to your entire team. [Export to code guide →](../guides/code_generator.md)


## **Where to Go Next**

### 🚀 **Start Building**
- **[Design Concepts](python-api/design-concepts.md)** - Understand FlowFrame and FlowGraph fundamentals
- **[Reading Data](python-api/reading-data.md)** - Load data from files, databases, and cloud storage
- **[DataFrame Operations](python-api/flowframe-operations.md)** - Transform your data with Polars-compatible operations

### 📖 **API Reference**
- **[Python API Overview](python-api/index.md)** - Complete API documentation
- **[API Reference](core/python-api-reference.md)** - Detailed class and method documentation
- **[Data Types](python-api/data-types.md)** - Supported data types and conversions

### 💡 **Deep Dives**
- **[Core Architecture](core/flowfile-core.md)** - How FlowGraph, FlowNode, and FlowDataEngine work together
- **[Dual Interface Philosophy](flowfile-for-developers.md)** - The design that bridges code and visual interfaces
- **[Technical Architecture](../guides/technical_architecture.md)** - System design and performance optimizations

### 🔧 **Integration Guides**
- **[Visual UI Integration](python-api/visual-ui.md)** - Launch and control the visual editor from Python
- **[Cloud Connections](python-api/cloud-connection-management.md)** - Manage S3 and cloud storage connections
- **[Database Connectivity](../guides/database_connectivity.md)** - Connect to PostgreSQL and other databases

### 🎯 **Practical Examples**
- **[Code to Flow](../guides/flowfile_frame_api.md)** - Build pipelines in code, visualize instantly
- **[Flow to Code](../guides/code_generator.md)** - Export visual pipelines as Python code
- **[Building Flows Visually](../flows/building.md)** - Create pipelines with the drag-and-drop interface