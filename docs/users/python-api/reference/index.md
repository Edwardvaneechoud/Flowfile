# Python API Flowfile Reference

This section documents Flowfile's Python API, focusing on extensions and differences from Polars. For standard Polars operations, see the [Polars documentation](https://pola-rs.github.io/polars/py-polars/html/reference/).

## Core API

### Data Input/Output

- [**Reading Data**](reading-data.md) - File formats and cloud storage
- [**Writing Data**](writing-data.md) - Saving results
- [**Data Types**](data-types.md) - Supported data types

### Transformations

- [**FlowFrame Operations**](flowframe-operations.md) - Filter, select, sort
- [**Aggregations**](aggregations.md) - Group by and summarize
- [**Joins**](joins.md) - Combining datasets

### Flowfile-Specific Features

- [**Cloud Storage**](cloud-connections.md) - S3 integration
- [**visualize pipelines**](visual-ui.md) - Working with the visual editor

## Key Extensions to Polars

### Description Parameter
Every operation accepts `description` for visual documentation:
```python
df = df.filter(ff.col("active") == True, description="Keep active records")
```

### Flowfile Formula Syntax
Alternative bracket-based syntax for expressions:
```python
df.filter(flowfile_formula="[price] > 100 AND [quantity] >= 10")
```
Read more about the formula syntax here: [Flowfile Formula Syntax](../concepts/expressions.md).
Or try it out here: [Flowfile Formula Playground](https://polars-expr-transformer-playground-whuwbghlymon84t5ciewp3.streamlit.app/)

### Automatic Node Types
Operations map to UI nodes when possible, otherwise fall back to `polars_code`:
```python
# Simple → UI node
df.group_by("category").agg(ff.col("value").sum())

# Complex → polars_code node
df.group_by([ff.col("category").str.to_uppercase()]).agg(ff.col("value").sum())
```

### Graph Access
Inspect and visualize the pipeline DAG:
```python
ff.open_graph_in_editor(df.flow_graph)
```

## Architecture Deep Dives

For understanding how Flowfile works internally:

- [**Core Architecture**](../../../for-developers/flowfile-core.md#1-the-flowgraph-your-pipeline-orchestrator) - FlowGraph, FlowNode, and FlowDataEngine internals
- [**Design Philosophy**](../../../for-developers/design-philosophy.md) - The dual interface approach


## Getting Help

- **Not finding a method?** Check the [Polars documentation](https://pola-rs.github.io/polars/py-polars/html/reference/) - most methods work identically
- **Need examples?** See our [tutorials](../tutorials/index.md)
- **Understanding concepts?** Read about [FlowFrame and FlowGraph](../concepts/design-concepts.md)

---

*This reference covers Flowfile-specific features. For standard Polars operations, see the [Polars API Reference](https://pola-rs.github.io/polars/py-polars/html/reference/).*