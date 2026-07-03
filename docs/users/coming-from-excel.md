# Coming from Excel

If you clean and analyze data in Excel — filters, VLOOKUPs, pivot tables, formula columns — you already know Flowfile's concepts. What changes is that every step becomes a visible, repeatable node instead of something buried in cell references. This page translates the Excel vocabulary into Flowfile's, so you can start from what you know.

## The mental model

An Excel workflow usually lives in one sheet: raw data on the left, helper columns in the middle, a pivot table somewhere else. In Flowfile the same work is a **flow** — a left-to-right chain of steps on a canvas, where each step shows you its output data. Nothing is hidden in cells, and re-running everything on next month's file is one click.

| In Excel | In Flowfile |
|---|---|
| A sheet of rows | A table flowing from node to node, previewed at every step |
| A formula column (`=IF(...)`) | A [Formula node](visual-editor/nodes/transform.md#formula) |
| VLOOKUP / XLOOKUP | A [Join node](visual-editor/nodes/combine.md#join) |
| A pivot table | [Group By or Pivot nodes](visual-editor/nodes/aggregate.md) |
| Data → Remove Duplicates | A [Drop Duplicates node](visual-editor/nodes/transform.md#drop-duplicates) |
| Filter and sort buttons | [Filter and Sort nodes](visual-editor/nodes/transform.md#filter-data) |
| Redoing the steps by hand next month | Re-running the flow — or [scheduling it](visual-editor/catalog/schedules.md) |
| Save As → .xlsx | A [Write Data node](visual-editor/nodes/output.md) — Excel, CSV, or Parquet |

## Formulas will feel familiar

Flowfile's [formula language](formulas/index.md) reads like spreadsheet formulas: reference columns by name in square brackets instead of by cell, and write conditions as `if ... then ... else ... endif`.

| Excel | Flowfile formula |
|---|---|
| `=IF(B2>100,"High","Low")` | `if [amount] > 100 then "High" else "Low" endif` |
| `=ROUND(B2*C2,2)` | `round([price] * [quantity], 2)` |
| `=A2&" "&B2` | `concat([first_name], " ", [last_name])` |
| `=IFERROR(B2,0)` (blank handling) | `ifnull([discount], 0)` |
| `=TODAY()` | `today()` |
| `=DATEDIF(A2,TODAY(),"d")` | `date_diff_days(today(), [hire_date])` |

Two differences worth knowing up front: a formula applies to a whole column at once (there is no dragging down), and boolean logic is written `and` / `or` rather than `AND()` / `OR()`. The built-in functions are all in the [function reference](formulas/functions.md), and there is an [interactive playground](https://edwardvaneechoud.github.io/polars_expr_transformer/) where you can try formulas against sample data in your browser.

## VLOOKUP is a Join

A VLOOKUP pulls columns from another sheet by matching a key. In Flowfile, that is a **Join** node with two inputs: your main table and the lookup table, matched on a key column.

- A **left** join is the closest match to VLOOKUP: every row of your main table is kept, and matching columns are added where the key is found (non-matches become empty values instead of `#N/A`).
- An **inner** join keeps only the rows that matched — like filtering out the `#N/A`s afterwards.

Where Excel needed the lookup value in the first column and returned one column at a time, a Join matches on any column and brings the whole lookup row along. And when the keys don't match exactly — "Acme Corp" vs "ACME Corporation" — the [Fuzzy Match node](visual-editor/nodes/combine.md#fuzzy-match) does approximate matching, something Excel cannot do natively.

## Pivot tables are two nodes

Excel's pivot table does two jobs at once, and Flowfile splits them:

- **Group By** produces summary rows — one row per group with aggregations like sum, mean, median, or count. This covers most pivot-table uses ("total sales per city").
- **Pivot** spreads a category column across the header — one column per category value, like putting a field in the pivot table's "Columns" area. [Unpivot](visual-editor/nodes/aggregate.md#unpivot-data) does the reverse, turning wide monthly columns back into tidy rows.

## Where the spreadsheet runs out

Excel stops at 1,048,576 rows and gets slow well before that. Flowfile runs on [Polars](https://pola.rs), a modern engine that comfortably processes millions of rows on a laptop. The practical differences:

- **Size** — files bigger than Excel's limit open and process normally.
- **Repeatability** — point the flow at next month's file and press run; no re-doing steps, no stale pivot caches.
- **Transparency** — every transformation is a labeled node a colleague can read, not a formula hidden in column Q.
- **A way out of files entirely** — results can land in the [catalog](visual-editor/catalog/index.md), where you query them with SQL, chart them, and schedule refreshes.

## Try it

The [Quickstart](../quickstart.md) walks through a classic Excel job — deduplicate sales rows, filter them, and build a per-city summary — as a visual flow. To try the canvas without installing anything, open the [live demo](https://demo.flowfile.org) and load a template.
