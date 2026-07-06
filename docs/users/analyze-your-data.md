# Analyze your data

The deliverable is insight: a number someone will act on, a chart in Friday's deck, a table a colleague filters themselves, a dashboard that's still right next month. Maybe you're the finance person reconciling three exports, the marketer sizing a campaign, the researcher cleaning survey waves, or the consultant handed a client's data dump — the common thread is that pipelines and storage are means, and a result you can defend is the end.

The route below is Flowfile's answer to a problem every analyst knows: analysis that lives in files goes stale, forks into `final_v3 (2).xlsx`, and dies when you're on holiday. The alternative is a loop — **shape → publish → query → chart → refresh** — where each piece stays connected to the ones before it.

![The analyst loop: a flow shapes raw data and publishes it into a catalog table, off which a SQL editor, a chart, and a schedule all hang; a refresh arrow loops from the schedule back to the flow, so the table and everything built on it stays current.](../assets/images/concepts/analyst-loop.svg)

## 1. Get the data in

Start from wherever the data is today. A file on your machine is a [Read data node](visual-editor/nodes/input.md) — CSV, Excel, or Parquet, previewed the moment it loads. Data in company systems (a database, a bucket, an API) is a [saved connection](visual-editor/connections.md) plus the matching reader node; the [data-elsewhere route](data-elsewhere.md) covers that situation properly.

And if you have nothing at hand and just want to see the loop working:

```bash
flowfile seed-demo
```

One command creates a populated demo catalog — sales tables under a `Demo` namespace, a working flow that produced them, and a schedule that keeps one fresh — so you can explore every step below with real content before committing your own data.

## 2. Shape it on the canvas

Cleaning is where analyses silently go wrong, and the canvas is built around making each step *inspectable*: drop duplicates, filter, join, aggregate — and after every node, look at the data before trusting the next step. There is no "run the whole script and hope"; a mistake is visible at the node that made it.

The logic itself reads like the formulas you already write. Keeping high-quantity orders is:

```text
[quantity] > 7
```

and a margin classification is:

```text
if [gross_income] > 30 then "high" else "standard" endif
```

— the whole [formula language](formulas/index.md) works like that, compiled to native Polars underneath. The [Quickstart](../quickstart.md#your-first-flow-visually) walks a real example (deduplicate a sales export, filter it, summarize income per city), and the finished flow is [one click away in your browser](../assets/try-sales-pipeline.html).

## 3. Publish, don't export

Here's the mental shift this route turns on. An exported file is a *snapshot*: the moment it leaves, it starts rotting — no history, no connection to how it was made, endlessly re-attached to emails. A [Write to Catalog node](visual-editor/nodes/output.md#catalog-writer) publishes the same result as a **live table**: versioned (Delta under the hood, history and time travel included), with lineage back to the flow that produced it, and one canonical copy that every downstream query and chart reads.

Practically: the question "which numbers did we report in March?" becomes a version lookup instead of an archaeology dig through mail attachments.

![Two ways to hand off a result: on the left a file export forks into v1, v2, and final_v3 emailed around until every inbox differs; on the right one versioned catalog table with lineage back to its flow, read live by the SQL editor, a chart, and a colleague.](../assets/images/concepts/export-vs-publish.svg)

## 4. Query and chart it

Once results are tables, day-to-day analysis stops needing the canvas. The [SQL editor](visual-editor/catalog/sql-editor.md) queries and joins anything in the catalog; a query worth keeping becomes a [virtual table](visual-editor/catalog/virtual-tables.md) — a saved view that recomputes from current data on every read, so it can never be stale. [Visualizations](visual-editor/catalog/visualizations.md) chart tables or SQL results in Graphic Walker and are stored next to the data they describe.

![One catalog table fanning out to its consumers — the SQL editor querying it, a chart built on it, and a saved virtual table derived from it: publish once, analyze many ways.](../assets/images/concepts/catalog-fan-out.svg)


<!-- draft: Fable to finalize -->
Prefer to work in code? The same table is a line away from a [**notebook**](visual-editor/catalog/notebooks.md) — or any Python script — where you publish an aggregate and read it back with SQL, right next to the data.

<details markdown="1">
<summary>See the round-trip in Python (tested in CI on every commit)</summary>

```python
--8<-- "docs/examples/catalog_analysis.py:example"
```

</details>

## 5. Make it refresh itself

A report you refresh by hand is a chore you now own forever. [Schedules](visual-editor/catalog/schedules.md) close the loop: run the flow on a cron, or trigger it whenever an upstream table updates — so a fresh sync cascades into fresh summaries into fresh charts, with nobody pressing Run. The difference between "analysis" and "infrastructure someone can rely on" is exactly this step.

## 6. When the analysis outgrows clicking

Nothing above locks you in. The [Python API](write-python.md) builds the same flows in code, catalog tables pull straight into scripts and notebooks, and any visual flow [exports as Python](visual-editor/tutorials/code-generator.md) — pure-transformation flows as Polars with no `flowfile` import, I/O-bearing flows with an `ff` import for their connections — the day a pipeline graduates into an engineering codebase.

---

**Fastest first taste:** `pip install flowfile`, then `flowfile seed-demo`, then `flowfile run ui` — open the Catalog tab and start querying the demo tables.
