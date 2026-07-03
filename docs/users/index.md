# Guides by audience

Flowfile is one tool with several front doors. Pick the one that matches how you work — every path builds the same flows, and you can switch between them at any point.

## Find your starting point

<div class="grid cards" markdown>

-   :material-drag-variant: **Build flows visually**

    ---

    Drag nodes onto a canvas and preview the data at every step — no code required.

    [:octicons-arrow-right-24: Visual Editor](visual-editor/index.md)

-   :material-microsoft-excel: **Coming from Excel**

    ---

    VLOOKUPs, pivot tables, and IF-formulas, translated into flows.

    [:octicons-arrow-right-24: Coming from Excel](coming-from-excel.md)

-   :material-database: **Connect your data**

    ---

    Databases, S3/ADLS/GCS, Kafka, REST APIs — set up a connection once, use it everywhere.

    [:octicons-arrow-right-24: Connect Your Data](connect/index.md)

-   :material-chart-bar: **Analyze your data**

    ---

    Query catalog tables in the SQL editor, build visualizations, schedule refreshes.

    [:octicons-arrow-right-24: Catalog](visual-editor/catalog/index.md)

-   :material-language-python: **Write Python**

    ---

    Polars-style code that builds a visual flow as a side effect.

    [:octicons-arrow-right-24: Python API](python-api/index.md)

-   :material-server: **Deploy for a team**

    ---

    Docker, users and groups, sharing, headless runs.

    [:octicons-arrow-right-24: Deploy & Operate](deployment/index.md)

</div>

## Visual and code are the same pipeline

A flow built on the canvas and a pipeline written in Python construct the same graph underneath:

- Write code, then inspect it on the canvas with `ff.open_graph_in_editor(df.flow_graph)`.
- Build visually, then [export the flow as standalone Python](visual-editor/tutorials/code-generator.md) — plain Polars, no Flowfile dependency required to run it.
- Hand a visual flow to a colleague who prefers code, or the other way around — both are views of the same graph.

```python
--8<-- "docs/examples/sales_pipeline.py:example"
```

This is the same pipeline the [Quickstart](../quickstart.md) builds visually — read, deduplicate, filter, aggregate.

## New here?

The [Quickstart](../quickstart.md) installs Flowfile and walks both paths in a few minutes: a visual flow that ends in the catalog, and the Python version of the same pipeline.
