# Guides by audience

Flowfile is one tool with several front doors. Pick the one that matches how you work — every path builds the same flows, and you can switch between them at any point. (Not sure Flowfile is the right shape at all? Start with [What is Flowfile](../what-is-flowfile.md).)

## Find your starting point

<div class="grid cards" markdown>

-   :material-drag-variant: **Build flows visually**

    ---

    You turn messy exports into clean tables others rely on — as visible, re-runnable pipelines.

    [:octicons-arrow-right-24: Build Flows Visually](build-flows-visually.md)

-   :material-microsoft-excel: **Coming from Excel**

    ---

    VLOOKUPs, pivot tables, and IF-formulas, translated into flows.

    [:octicons-arrow-right-24: Coming from Excel](coming-from-excel.md)

-   :material-database: **Your data lives elsewhere**

    ---

    Warehouse, S3, Kafka, GA — work with data where it already is, and stop copying it around by hand.

    [:octicons-arrow-right-24: Your Data Lives Elsewhere](data-elsewhere.md)

-   :material-chart-bar: **Analyze your data**

    ---

    From question to chart you trust: shape, publish, query, visualize — and let it refresh itself.

    [:octicons-arrow-right-24: Analyze Your Data](analyze-your-data.md)

-   :material-language-python: **Write Python**

    ---

    Polars-style code with less I/O boilerplate — and every pipeline gets a canvas.

    [:octicons-arrow-right-24: Write Python](write-python.md)

-   :material-server: **Run Flowfile for a team**

    ---

    You're making it a shared tool: auth, secrets, sharing, backups, day-two operations.

    [:octicons-arrow-right-24: Run Flowfile for a Team](deploy-for-a-team.md)

-   :material-toy-brick-outline: **Build a node the palette lacks**

    ---

    The operation you need isn't built in? Make it once in the Node Designer — visually, no code file — and reuse it everywhere.

    [:octicons-arrow-right-24: Node Designer](visual-editor/node-designer.md)

</div>

## Visual and code are the same pipeline

A flow built on the canvas and a pipeline written in Python construct the same graph underneath:

- Write code, then inspect it on the canvas with `ff.open_graph_in_editor(df.flow_graph)`.
- Build visually, then [export the flow as Python](visual-editor/tutorials/code-generator.md) — pure-transformation flows export as Polars with no `flowfile` import (some formula, fuzzy-match, or graph nodes pull a small `polars_*` helper); flows with I/O nodes keep an `ff` import for their connections.
- Hand a visual flow to a colleague who prefers code, or the other way around — both are views of the same graph.

```python
--8<-- "docs/examples/sales_pipeline.py:example"
```

This is the same pipeline the [Quickstart](../quickstart.md) builds visually — read, deduplicate, filter, aggregate.

## New here?

The [Quickstart](../quickstart.md) installs Flowfile and walks both paths in a few minutes: a visual flow that ends in the catalog, and the Python version of the same pipeline.
