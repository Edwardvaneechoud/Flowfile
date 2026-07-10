# Visual Editor

Build data pipelines by dragging nodes onto a canvas and connecting them — no code required. Each node is one operation (read a file, filter rows, join two tables), and you can inspect the data after every step.

!!! tip "Try it in your browser first"
    [**Flowfile Lite**](../deployment/lite.md) runs this same canvas entirely in your browser at [demo.flowfile.org](https://demo.flowfile.org) — a lightweight subset with no backend, databases, scheduler, or AI.

## Three concepts

- **Nodes** — operations, grouped into six palette categories: [Input](nodes/input.md), [Transform](nodes/transform.md), [Combine](nodes/combine.md), [Aggregate](nodes/aggregate.md), [Output](nodes/output.md), and [Machine Learning](nodes/ml.md).
- **Connections** — drag between node handles to define how data flows, left to right.
- **Execution modes** — **Development** materializes every node so you can preview all intermediate data; **Performance** executes only what outputs need, with query optimization across nodes.

If you haven't built a flow yet, the [Quickstart](../../quickstart.md#your-first-flow-visually) walks through a complete one in five steps.

## In this section

<div class="grid cards" markdown>

-   :material-vector-polyline: **[Building Flows](building-flows.md)**

    ---

    Canvas mechanics: create, connect, configure, run, save.

-   :material-function-variant: **[Formulas](../formulas/index.md)**

    ---

    The Excel-like expression language used in Formula and Filter nodes.

-   :material-graph-outline: **[Node Reference](nodes/index.md)**

    ---

    Every node, per category, with configuration tables.

-   :material-language-python: **[Sandboxed Python](kernels.md)**

    ---

    Run arbitrary Python in Docker-isolated kernels as a node.

-   :material-toy-brick-outline: **[Node Designer](node-designer.md)**

    ---

    Missing a node you need? Build your own — visually, no Python file to write by hand — and it joins the palette with its own settings form.

-   :material-school-outline: **[Worked Examples](tutorials/index.md)**

    ---

    Complete flows with data, expected results, and downloads.

</div>

[Settings](settings.md) covers theme and user management.

## Visual or Python?

Both build the same flow graph, so this is a preference, not a commitment: any visual flow [exports to Python](tutorials/code-generator.md), and any [Python pipeline](../python-api/index.md) opens on the canvas. The canvas shines for exploring unfamiliar data and for handing work to colleagues who don't code; the Python API fits automation, version control, and logic that outgrows node settings.
