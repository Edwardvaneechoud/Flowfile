# Write Python

You already build pipelines in code — as a data scientist moving between notebooks and jobs, a backend developer automating a data chore, an analytics engineer maintaining transformation logic — and you're not shopping for a no-code tool. The reasons this API earns a place anyway are practical: connection and I/O boilerplate collapses into one-liners, Polars performance comes with Polars idioms intact, and every pipeline you write is *also* a diagram a non-developer colleague can open, inspect, and even take over.

The mental model in one line: **every method call adds a node to a graph instead of executing.** The graph is the pipeline — Python and the canvas are two editors for the same object.

![The code-and-canvas duality: a short Python pipeline on the left and the identical node graph on the right, joined by one shared FlowGraph object in the middle — open_graph_in_editor turns code into canvas, export as Python turns canvas back into code.](../assets/images/concepts/code-canvas-duality.svg)

## 1. First pipeline

`pip install flowfile`, then — this snippet is a repository file executed by CI on every commit, so it cannot drift from the real API:

```python
--8<-- "docs/examples/first_pipeline.py:example"
```

Nothing runs until `.collect()`: each call appends a node, and collection hands the whole plan to the Polars optimizer at once. The consequences are the useful kind — build a pipeline conditionally, pass it between functions, branch it — all before any data is read. [FlowFrame and FlowGraph](python-api/concepts/design-concepts.md) covers the model, including how branches share one graph and why schemas resolve without executing anything.

## 2. Your code has a canvas

```python
ff.open_graph_in_editor(result.flow_graph)
```

That one call is the collaboration story. The pipeline opens in the visual editor as nodes — your colleague walks it step by step, inspects the data between operations, and continues editing visually if that's their medium. Pass `description=` on operations and those become the node labels, which is what makes a code-built graph *legible* rather than merely visible. Details in [Visual UI Integration](python-api/reference/visual-ui.md).

## 3. Connectors without ceremony

The boilerplate you're used to writing around sources — credential plumbing, retry-prone connection setup, path handling — is replaced by [named connections](python-api/reference/cloud-connections.md): create one once (in code or in the UI; both share a single encrypted store) and reference it by name in reads and writes against Postgres/MySQL/SQLite, S3/ADLS/GCS, Kafka, and REST endpoints. The [reading](python-api/reference/reading-data.md) and [writing](python-api/reference/writing-data.md) references cover every entry point, and the database and cloud examples execute against real services in CI.

## 4. The catalog from code

The [catalog](visual-editor/catalog/index.md) is where scripts and visual flows meet: `ff.write_catalog_table` publishes a frame as a versioned Delta table anyone can query or chart, and reads come back into any script or notebook. This example round-trips an aggregate through the catalog and back out via SQL — note `read_catalog_sql` imports from `flowfile_frame`, one of the API's honest sharp edges:

```python
--8<-- "docs/examples/catalog_analysis.py:example"
```

[Catalog References](python-api/reference/catalog-references.md) documents the name-based handle API on top of this.

## 5. Notebooks, next to the data

Exploration happens in notebooks — but a local Jupyter detaches from everything: the data gets copied out, the environment drifts from production, and the notebook ends up in a folder nobody else can run. Flowfile's [notebooks live **in the catalog**](visual-editor/catalog/notebooks.md), next to the tables they analyze, and their Python cells execute on [Docker-isolated kernels](visual-editor/kernels.md) — any pip library, pinned per kernel, identical for everyone who opens the notebook.

Cells talk to the catalog directly and display like you'd expect — the last expression auto-renders, with richer options a call away:

```python
lf = flowfile_ctx.read_catalog_table("sales_by_city")
flowfile_ctx.explore(lf)   # interactive explorer; .display(lf) for the table view
```

The editor has code completions, and the same kernel machinery powers the [Python Script node](visual-editor/kernels.md) when notebook logic graduates into a flow. (Cell code runs inside a kernel — the `flowfile_ctx` API above is the kernel's, documented in full on [The flowfile_ctx API](visual-editor/kernel-api.md).)

<!-- IMAGE-PLACEHOLDER-TO-CHANGE: a catalog notebook open next to the catalog tree — cells with execution counters, one cell reading a catalog table via flowfile_ctx, the interactive explorer rendering below it -->

## 6. Know the two dialects

Two ways to express logic, differing in what the canvas can do with them later:

- **Polars-style expressions** — `ff.col`, `when/then`, the `str`/`dt`/`list` namespaces — feel native and mostly are ([Expressions](python-api/concepts/expressions.md)); renames and gaps are documented plainly in the [operations reference](python-api/reference/flowframe-operations.md). Steps built this way render on the canvas as generic code nodes: correct, but opaque to a visual editor.
- **[Flowfile formula strings](python-api/concepts/formulas.md)** — `filter(flowfile_formula="[quantity] > 7")` — render as *editable* native nodes, the same ones a canvas user would have configured by hand.

Rule of thumb: if a colleague might someday edit the pipeline visually, prefer formulas for the simple steps; keep expressions for logic that's genuinely code-shaped.

## 7. Ship and test it

Flows serialize to plain `.yaml`, which makes the engineering hygiene ordinary: version them in Git, run them headlessly in CI or cron —

```bash
flowfile run flow pipeline.yaml --param run_date=2026-07-04
```

— with exit code 0/1 and no UI or services required. Going the other direction, visual flows [export as Python](visual-editor/tutorials/code-generator.md): pure-transformation flows as dependency-free Polars, I/O-bearing flows with an `ff` import for their connections — either way, readable code when a pipeline graduates into a codebase.

---

**Fastest first taste:** the tested pipeline in step 1 — paste it into any session after `pip install flowfile`. Then the [Python API quickstart](python-api/quickstart.md) for the guided tour, and the [reference](python-api/reference/index.md) for the full surface.
