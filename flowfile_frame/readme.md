## Quick Start

`flowfile_frame` is a Python module included in the Flowfile package that provides a familiar Polars-like API for data manipulation, while simultaneously building an ETL (Extract, Transform, Load) graph under the hood. This allows you to:

1. Write data transformation code using a simple, Pandas/Polars-like API
2. Automatically generate executable ETL workflows
3. **Visualize, save, and open your data pipelines in the FlowFile Designer UI**
4. Get the performance benefits of Polars with the traceability of ETL graphs

1. Write data transformation code using a simple, Pandas/Polars-like API
2. Automatically generate executable ETL workflows
3. **Visualize, save, and open your data pipelines in the FlowFile Designer UI**
4. Get the performance benefits of Polars with the traceability of ETL graphs

## Installation

```bash
pip install Flowfile
```
The `flowfile_frame` module is included as part of the main Flowfile package. The installation provides the complete Flowfile toolkit, including the FlowFile Frame API, Core (`flowfile_core`), and Worker (`flowfile_worker`) components.

## Quick Start

```python
import flowfile_frame as ff

# Create a dataframe from a dictionary
df = ff.from_dict({
    "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
    "age": [25, 35, 28, 42, 31],
    "salary": [50000, 60000, 55000, 75000, 65000]
})

# Basic transformations
filtered_df = df.filter(ff.col("age") > 30)
result = filtered_df.with_columns(
    (ff.col("salary") * 1.1).alias("new_salary")
)

# Save the resulting ETL graph
result.save_graph("my_pipeline.flowfile")

# Execute and get data as a Polars DataFrame
result_data = result.collect()
```

## Visualizing and Sharing Your Data Pipelines

One of the most powerful features of FlowFile Frame is the ability to visualize, save, and share your data transformation pipelines:

```python
import flowfile as ff
from flowfile import open_graph_in_editor

# Create a complex data pipeline
df = ff.from_dict({
    "id": [1, 2, 3, 4, 5],
    "category": ["A", "B", "A", "C", "B"],
    "value": [100, 200, 150, 300, 250]
})

aggregated_df = (
    df
    .filter(ff.col("value") > 120)
    .with_columns(
        [(ff.col("value") * 1.1).alias("adjusted_value"),
        ff.when(ff.col("category") == "A").then(ff.lit("Premium"))
          .when(ff.col("category") == "B").then(ff.lit("Standard"))
          .otherwise(ff.lit("Basic")).alias("tier")]
    )
    .group_by("tier")
    .agg([
        ff.col("adjusted_value").sum().alias("total_value"),
        ff.col("adjusted_value").mean().alias("avg_value"),
        ff.col("id").count().alias("count")
    ])
    .sort("total_value", descending=True)
)

aggregated_df.write_csv("agg_output.csv")


# Open the saved graph in the FlowFile Designer UI
# This allows you to visualize, edit, and share your pipeline
# !You need to have the latest version of flowfile installed or running locally to view this.
open_graph_in_editor(aggregated_df.flow_graph)

```

### Graph Visualization Benefits

- **Inspect Data Flow**: See exactly how your data is transformed step by step
- **Debugging**: Identify issues in your data pipeline visually
- **Documentation**: Share your data transformation logic with teammates visually
- **Iteration**: Modify your pipeline in the Designer UI and export it back to code
- **Production Deployment**: Save your graph for execution in production environments

### Graph File Workflow Options

1. **Develop in Code → Visualize in UI**:
   - Build your data pipeline with the FlowFile Frame API
   - Save the graph and open it in the Designer for visualization or sharing

2. **Develop in UI → Use in Code**:
   - Build your data pipeline visually in the Designer UI
   - Save the `.flowfile` file and load it in your Python code
   - Execute or extend the pipeline programmatically

3. **Hybrid Approach**:
   - Start in one environment and refine in the other
   - Allows data scientists and engineers to collaborate using their preferred tools
