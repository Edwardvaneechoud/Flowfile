# Python API

Build data pipelines programmatically with Flowfile's Polars-compatible API. Pipelines built this way are version-controllable, reproducible, and open unchanged in the visual editor.

!!! info "Backend required — not in Flowfile Lite"
    The Python API runs the full Flowfile engine in-process. It is **not** part of the browser-only [Flowfile Lite](../deployment/lite.md) edition, which is visual-only. Install the [Python package](../deployment/python.md) to use this API.

If you know Polars, most of this API will look familiar: the `FlowFrame` mirrors a Polars `LazyFrame`, and expressions (`ff.col`, `ff.when`, the `.str`/`.dt` namespaces) work the same way. The additions are the parts that connect your code to the rest of Flowfile — cloud and database connectors, the catalog, and opening a pipeline in the visual editor.

## Quick example

This snippet is a repository file executed by CI — it cannot drift from the real API:

```python
--8<-- "docs/examples/sales_pipeline.py:example"
```

Build the same chain without the final `.collect()` and the pipeline object opens on the canvas via `ff.open_graph_in_editor(pipeline.flow_graph)` — see [Visual UI Integration](reference/visual-ui.md).

## Documentation

### [Quick Start](quickstart.md)
Install the package and build your first pipeline.

### [Core Concepts](concepts/index.md)

- [FlowFrame and FlowGraph](concepts/design-concepts.md) — the lazy, graph-connected data model
- [Expressions](concepts/expressions.md) — Polars-style column operations
- [Formulas in Python](concepts/formulas.md) — methods that accept the Excel-like [formula language](../formulas/index.md)

### [API Reference](reference/index.md)

- [Reading Data](reference/reading-data.md)
- [Writing Data](reference/writing-data.md)
- [Data Types](reference/data-types.md)
- [DataFrame Operations](reference/flowframe-operations.md)
- [Aggregations](reference/aggregations.md)
- [Joins](reference/joins.md)
- [Cloud Storage](reference/cloud-connections.md)
- [Visual UI Integration](reference/visual-ui.md)
- [Catalog References](reference/catalog-references.md)

### [Tutorials](tutorials/index.md)

- [Building Flows with Code](tutorials/flowfile_frame_api.md)

## For contributors

To understand how Flowfile works internally or contribute to the project, see the [Developer Documentation](../../for-developers/index.md).
