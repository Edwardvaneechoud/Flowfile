# Node Reference

Nodes are the building blocks of a Flowfile pipeline. Each node performs one operation — read a file, filter rows, join two datasets, train a model, write a result — and you connect them on the canvas to move data from source to output. This page is the entry point to the full reference: pick the category that matches the operation you need.

Nodes are grouped into six categories. The counts below are current as of 2026-09 (from `flowfile_core/flowfile_core/configs/node_store/nodes.py`) and may change as nodes are added.

| Category | Nodes | What it does |
|----------|-------|--------------|
| [Input](input.md) | 10 | Load data from files, databases, cloud storage, APIs, Kafka, or the catalog. |
| [Transform](transform.md) | 13 | Reshape one dataset — filter, sort, cleanse, add columns, run formulas, SQL, or Python. |
| [Combine](combine.md) | 8 | Merge two or more datasets by joining, unioning, fuzzy-matching, or graph-solving — or gate a branch on a condition. |
| [Aggregate](aggregate.md) | 5 | Summarize and restructure — group, pivot, unpivot, count. |
| [Output](output.md) | 7 | Write results to files, databases, cloud storage, or the catalog — or explore them visually. |
| [Machine Learning](ml.md) | 4 | Split data, train and apply a model, and evaluate its predictions. |

## How nodes work

Every node follows the same pattern:

- **Inputs and outputs.** A node reads from the nodes connected to its input handles and passes its result to whatever connects to its output. The number of inputs and outputs is fixed per node type — a Join takes two, a Filter in split mode emits two.
- **Configuration.** Selecting a node opens its settings in the right panel. The available fields depend on the node type; see each category page for the exact options.
- **Schema preview.** After you configure a node, it shows the output schema (column names and types) without running the whole flow. Run the node to see the actual data in the preview panel.
- **Lazy by default.** Most nodes build a Polars query that only materializes when you run the flow, so intermediate steps cost nothing until you ask for a result.

Not every node is available in every build. The browser-only [Flowfile Lite](../../deployment/lite.md) edition ships a subset (23 usable nodes as of 2026-08); each category page notes which of its nodes are included.

[Start with input nodes →](input.md)
