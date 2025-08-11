# **Flowfile Python API: For Developers**

Welcome to the developer documentation for Flowfile. This is the home for anyone who wants to build, manage, and understand data pipelines using Python, made with Flowfile.

Flowfile provides a high-performance, Polars-compatible API that is extended visual integration, cloud connectivity, and analytics.
It's for analytics developers, business users and engineers who want to create an unified way of working of doing analytics.

!!! info "If You Know Polars, You Know Flowfile" 
    Our API is designed to be a seamless extension of Polars. Over 95% of the methods are identical, so you can leverage your existing knowledge to be productive from day one. The main additions are features that connect your code to the broader Flowfile ecosystem, like cloud integrations and UI visualization.


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

## **The Core Philosophy: Code and UI are the Same Thing**

Flowfile is built on an architecture where the Python API and the visual editor are two interfaces to the exact same underlying objects: the FlowGraph and its FlowNodes.

- FlowFrame: A wrapper around a Polars LazyFrame that automatically builds a FlowGraph as you chain operations.

- FlowGraph: The central orchestrator that holds the complete definition of your pipeline—every node, setting, and connection.

When you write df.filter(...), you are programmatically constructing a FlowNode with specific settings. When a user drags a "Filter" node in the UI, they are creating the identical object. This **dual interface philosophy** means your work is never locked into one paradigm.


## **Key Features for Developers**

- **Polars-Powered Performance:** Leverage the full speed of the Polars engine for all your data transformations.

- **Seamless Visual Debugging:** Use open_graph_in_editor() at any point in your script to instantly see your pipeline's structure, inspect intermediate data schemas, and identify issues visually.

- **Integrated Cloud Connectors:** Read and write data from AWS S3 with simple, high-level functions, without worrying about the underlying connection logic.

- **Secure Credential Management:** Handle database passwords and API keys securely using the built-in connection manager, which keeps secrets out of your code.

- **Self-Documenting Pipelines:** The description parameter in every function annotates the corresponding node in the UI, making your pipelines understandable to your entire team.


## **Where to Go Next**

- **🚀 First Pipeline Tutorial:** A hands-on, step-by-step guide to building a complete ETL pipeline with the Flowframe API [design-concepts](python-api/design-concepts.md).

- **📖 API Guides:** Detailed documentation on core functionalities like reading and writing data, building transformations [python-api](python-api/index.md)

- **💡 Architectural Deep Dive:** For those who want to contribute or understand the internals, explore our guides on the [Core Architecture](core/flowfile-core.md) and the [Dual Interface Philosophy](flowfile-for-developers.md).
