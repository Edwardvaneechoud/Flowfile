# Python API

Build data pipelines programmatically with Flowfile's Polars-compatible API.


!!! info "If You Know Polars, You Know Flowfile"
    Our API is designed to be a seamless extension of Polars. The majority of the methods are identical, so you can leverage your existing knowledge to be productive from day one. The main additions are features that connect your code to the broader Flowfile ecosystem, like cloud integrations and UI visualization.


## Who This Is For

- **Python developers** who prefer code over drag-and-drop
- **Data scientists** familiar with Polars or Pandas
- **Engineers** building automated data pipelines
- **Anyone** who needs version control and programmatic pipeline generation

## Quick Example

```python
import flowfile as ff

df = ff.read_csv("sales.csv")
result = df.filter(ff.col("amount") > 100).group_by("region").agg(
    ff.col("amount").sum()
)

# Visualize your pipeline
ff.open_graph_in_editor(result.flow_graph)
```

## Documentation

### [Quick Start](quickstart.md)
Get up and running in 5 minutes with your first pipeline.

### [Core Concepts](concepts/index.md)

- [FlowFrame and FlowGraph](concepts/design-concepts.md) - Fundamental building blocks
- [Formula Syntax](concepts/expressions.md) - Flowfile's Excel-like expressions

### [API Reference](reference/index.md)

- [Reading Data](reference/reading-data.md)
- [Writing Data](reference/writing-data.md)
- [Data Types](reference/data-types.md)
- [DataFrame Operations](reference/flowframe-operations.md)
- [Aggregations](reference/aggregations.md)
- [Joins](reference/joins.md)
- [Cloud Storage](reference/cloud-connections.md)
- [Visual UI Integration](reference/visual-ui.md)

### [Tutorials](tutorials/index.md)

- [Building Flows with Code](tutorials/flowfile_frame_api.md)

## For Contributors

Want to understand how Flowfile works internally or contribute to the project? See the [Developer Documentation](../../for-developers/index.md) for architecture details and internal API reference.

---

*Prefer visual workflows? Check out the [Visual Editor Guide](../visual-editor/index.md).*
