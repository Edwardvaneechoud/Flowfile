# Build flows visually

Recurring data preparation has a thousand faces: merging exports from two systems that don't talk, standardizing the files a partner sends every month, prepping inputs for a model, reconciling finance extracts, cleaning survey results. Maybe you're in operations, BI, finance, or you're the team's unofficial data steward — comfortable with data, not interested in maintaining a codebase. What these situations share is the failure mode: the preparation lives in manual steps and tribal knowledge, it breaks silently, and only one person knows how to redo it.

A flow replaces that with something that can be *seen*: every step a labeled node, every intermediate result inspectable, the whole thing re-runnable by anyone — including you, six months from now.

![A flow drawn as an assembly line: a file and a database feed one conveyor belt riding on rollers, the labeled stations — drop duplicates, join, group by — stand on the belt, each showing a small preview of its output data, and the belt runs straight into one clean result on the right; a small greyed checklist below shows the same job done by hand.](../assets/images/concepts/flow-assembly-line.svg)

## 1. Learn the canvas with one real flow

The mental model is small: **nodes are operations, the connections between them carry the data, and after a run you can look inside any node.** The [Quickstart](../quickstart.md#your-first-flow-visually) makes it concrete in five steps — read a sales export, drop duplicate rows, keep the bulk orders, summarize income per city — and [Building Flows](visual-editor/building-flows.md) covers the mechanics: connecting, configuring, running, saving. Twenty minutes, and the rest of this page is vocabulary.

## 2. Know your toolbox

Most flows lean on the same five nodes: Read data, Filter data, Formula, Join, and Group by. Node names say what they do to the data (*Drop duplicates*, *Text to rows*, *Fuzzy match*), and the [node reference](visual-editor/nodes/index.md) covers the rest — [Input](visual-editor/nodes/input.md), [Transform](visual-editor/nodes/transform.md), [Combine](visual-editor/nodes/combine.md), [Aggregate](visual-editor/nodes/aggregate.md), [Output](visual-editor/nodes/output.md), [Machine Learning](visual-editor/nodes/ml.md) — when you need a less common one.

![Flowfile's node palette, grouped into its seven categories — Input Sources, Transformations, Combine Operations, Aggregations, Machine Learning, Output Operations, User Defined Operations — with the five everyday workhorses (Read data, Filter data, Formula, Join, Group by) highlighted in cyan and the rest dimmed.](../assets/images/guides/building-flows/node-palette-annotated.svg)

And if the palette doesn't have the operation you need, you can add it yourself — the [Node Designer](visual-editor/node-designer.md) builds a custom transformation into a real palette node with its own settings form, no code file to write by hand. More on that in [Grow the toolkit](#7-grow-the-toolkit).


## 3. Write the logic as formulas

Derived columns and filter conditions use the [formula language](formulas/index.md) — column names in brackets, functions and conditionals like a spreadsheet, compiled to native Polars underneath. A margin flag looks like:

```text
if [gross_income] / [unit_price] > 0.3 then "high margin" else "standard" endif
```

and a cleanup like:

```text
trim(uppercase([customer_code]))
```

The [function reference](formulas/functions.md) lists everything available, and the [interactive playground](https://edwardvaneechoud.github.io/polars_expr_transformer/) lets you try an expression against sample data before committing it to a node.

## 4. Build in Development, ship in Performance

This is one toggle, not a migration. **Development** — the default while you build — runs every node and keeps its result, so you can inspect each step: run, look, adjust, run again. **Performance** computes only what the outputs actually need and optimizes across the whole flow, so nothing is calculated for a preview nobody's looking at. You switch in **Flow Settings**; the flow itself never changes, and scheduled or headless runs use Performance on their own.

<details markdown="1">
<summary>See it: the execution mode in Flow Settings</summary>

![Flow Settings — execution mode, location, and step previews](../assets/images/quickstart/flow_settings.gif)

</details>

Caching is separate and per-node: a node's **Cache results** toggle (in its **General Settings**) stores that node's output so a Performance run — and your downstream edits — reuse it instead of recomputing. Worth it for one expensive step whose inputs rarely change.

![The same flow run two ways: in Development every node is lit with a data preview beneath it; in Performance only the path to the output is lit, the exploratory branch is greyed out, and a cached node is short-circuited so its stored result is reused instead of recomputed.](../assets/images/concepts/dev-vs-performance.svg)

## 5. Deliver the result

Think about who consumes your output, because each consumer has a natural landing place:

- **A colleague who wants a file** — [Write data](visual-editor/nodes/output.md) to Excel, CSV, or Parquet, and you're done.
- **People who'll query, chart, or build on it** — [Write to Catalog](visual-editor/nodes/output.md#catalog-writer). The result becomes a versioned table with history and lineage, and the [analyst route](analyze-your-data.md) takes over from there. This is the option that ends the `final_v3.xlsx` problem.
- **Another system** — Database and Cloud Storage writers push results back into the warehouse or the bucket, via [saved connections](data-elsewhere.md).

## 6. Automate what you built

A finished flow shouldn't need you to press Run. Open [**Schedules**](visual-editor/catalog/schedules.md) in the app and set the flow to run on a schedule — every night, every Monday — or whenever a table it depends on is updated. No files, no command line: a fresh source cascades into fresh results on its own.

<details markdown="1">
<summary>Run it somewhere else — CI, cron, another host</summary>

Flows are plain `.yaml` files, so anything that can run a command can run one:

```bash
flowfile run flow monthly_reconciliation.yaml --param month=2026-07
```

Exit code 0 or 1, no UI — cron jobs and CI pipelines treat it like any other tool. The [CLI reference](deployment/cli.md) covers parameters and the packaged variants, and [Projects](projects.md) version the whole workspace in git.

</details>

## 7. Grow the toolkit

The habits that keep a growing collection of flows sane:

- **Stop copy-pasting node chains.** Shared logic becomes a [subflow](visual-editor/subflows.md) — a flow with named inputs and outputs that other flows call, including once-per-row over a parameter table.
- **Missing node? Make it once.** The [Node Designer](visual-editor/node-designer.md) turns a custom transformation into a real palette node with its own settings form, reusable across every flow.
- **When someone asks "what does this actually do?"** — [export the flow as Python](visual-editor/tutorials/code-generator.md). Pure-transformation flows export as Polars with no `flowfile` import; flows with I/O nodes keep an `ff` import for their connections. Either way, the logic is readable by anyone who reads code.

---

**Fastest first taste:** [open the finished sales pipeline in your browser](../assets/try-sales-pipeline.html) — nothing to install — then rebuild it yourself with the [Quickstart](../quickstart.md).
