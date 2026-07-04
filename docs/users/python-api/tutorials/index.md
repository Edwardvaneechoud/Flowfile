# Python API Tutorials

Worked, hands-on pipelines built with the Python API. Start with the code-to-flow walkthrough, then use the short patterns below as building blocks.

## Tutorial

### [Building Flows with Code](flowfile_frame_api.md)

Build a pipeline programmatically and open it as a visual graph in the Designer. Covers `from_dict`, expressions, conditional logic, grouping, and `open_graph_in_editor`.

## Patterns

### A complete pipeline

This snippet is a repository file executed by CI — read, derive, filter, aggregate:

```python
--8<-- "docs/examples/first_pipeline.py:example"
```

For the same shape with deduplication and multiple aggregations, see the [tested sales pipeline](../../visual-editor/tutorials/sales-pipeline.md#in-python); for grouped analytics, window functions, and selectors, the [aggregations reference](../reference/aggregations.md) opens with a CI-tested example.

## Related

- [API Reference](../reference/index.md) — method-by-method documentation
- [Core Concepts](../concepts/index.md) — the FlowFrame and FlowGraph model
- [Quick Start](../quickstart.md) — install and build a first pipeline
