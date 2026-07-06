# Flowfile, in plain terms

*The non-technical lens on [What is Flowfile](what-is-flowfile.md). If you build software or pipelines for a living, the [technical lens](what-is-flowfile-technical.md) will speak your language.*

Think about the last number you produced that mattered — a monthly total, a cleaned-up customer list, a chart in a deck. Now imagine someone asks, a week later: *"can you run that again with the new data?"* If answering that means retracing your own steps — which file, which filter, which fix you made by hand somewhere along the way — you know the problem Flowfile exists to solve.

**Flowfile is a data platform built around one promise: what you did a week ago, you can do again with one click.** Not because you remembered, but because the work itself is saved as a visible recipe.

## The recipe idea

In Flowfile, you don't transform data by editing it — you build a **flow**: a chain of steps on a canvas, each one a labeled block. *Read this file. Remove the duplicates. Keep the big orders. Total them per city.* After every step, you can see the data, so you always know exactly what each step did.

That picture *is* the work. Next month's file arrives: run the flow again. A colleague asks how the number was made: show them the flow — it reads like a recipe card, not like code. Something looks off: click the step where it went wrong and look at the data right there.

![A hand-drawn recipe card on warm paper morphs into a four-node Flowfile flow — Input, Filter, Formula, Output — with a small data preview beneath each node: the work itself is the recipe, written down once and re-runnable with one click.](assets/images/concepts/recipe-to-flow.svg)

Nothing about this requires programming. The logic reads like the formulas you already know — `if [amount] > 100 then "High" else "Low" endif` — and every step is a form, not a script.

## Where it compounds: the catalog

Building a flow solves one week's problem. The **catalog** is what makes the weeks add up.

Instead of exporting results to files that scatter across mailboxes and desktops, a flow can publish its result into the catalog — Flowfile's built-in home for your **data products**. A table in the catalog is alive in ways a file never is:

- It keeps its **history** — you can see, and go back to, what it contained in March.
- It knows its **origin** — the flow that produced it is one click away, always.
- It can be **queried and charted** right there — SQL and interactive visualizations, no export.
- It can **refresh itself** — schedule the flow, and the table (and every chart on it) stays current with nobody pressing Run.
- It can be **shared** — teammates get access to the product, not a copy that immediately starts rotting.

This is the quiet shift in how the work feels: you stop producing *files* and start maintaining *a small library of living results*. The flow is the recipe; the catalog is the kitchen where the dishes stay warm.

![The catalog sits at the center: flows publish their tables into it, while the SQL editor, charts, and teammates all read from that one shared library — and a schedule refreshes the flows on a timer, so the whole loop keeps itself current.](assets/images/concepts/catalog-ecosystem-loop.svg)

## Never more complicated than your problem

Flowfile is deliberately built so you can use a tenth of it and never feel the rest looming:

- **You see everything.** Every step previews its data. There is no "run it and pray" — mistakes are visible at the step that made them.
- **You start where you are.** If you know Excel, [the concepts transfer directly](users/coming-from-excel.md). If you've never touched a formula, the steps are forms with dropdowns.
- **Features wait their turn.** The canvas doesn't ask you about scheduling; the catalog doesn't ask you about Python. Each part shows up when your problem does.
- **One install.** `pip install flowfile` — or the desktop app, or [a browser tab with nothing installed at all](https://demo.flowfile.org) — brings the whole platform: canvas, catalog, everything.

## It grows in whatever direction you do

The same platform keeps up as the work gets more ambitious — without ever demanding it:

- **More data than a spreadsheet survives?** Flowfile runs on [Polars](https://pola.rs), the same engine data teams use for speed — millions of rows on an ordinary laptop is normal, not heroic.
- **Data living in company systems?** Databases, cloud storage, Kafka streams, APIs — [connect once, read live](users/data-elsewhere.md), stop hand-carrying copies.
- **Colleagues who code?** Every flow is equally real as Python: [write pipelines in code](users/write-python.md) that appear on the canvas, or take any visual flow and [export it as a plain Python script](users/visual-editor/tutorials/code-generator.md) that runs outside Flowfile entirely. Nobody is locked in — in either direction.
- **A team?** [Run it as a shared server](users/deploy-for-a-team.md) with accounts, access control, and shared data products.

The through-line never changes: whatever you build, at whatever level, is reproducible by construction. That's the product.

## See it in ninety seconds

The [Quickstart](quickstart.md) builds a real flow — deduplicate a sales file, filter it, total income per city, publish it to the catalog — and it's the same example you can [open in your browser right now](assets/try-sales-pipeline.html), no install, already assembled.

Then pick [the route written for how you work](users/index.md).
