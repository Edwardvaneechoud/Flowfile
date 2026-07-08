# Docs feedback — status

Resolutions are marked **→ Done**. Voice/tone rewrites are left as drafts marked
on-page with `!!! warning "To review (Fable)"` (or inline `_[Fable review …]_` in lists)
for Fable's finalize pass — grep: `grep -rniE "To review \(Fable\)|Fable review" docs/`.
Everything is uncommitted (review via `git diff`).

---

In this page; http://localhost:8000/Flowfile/what-is-flowfile-plain.html
- This is a bad example; The logic reads like the formulas you already know — if [amount] > 100 then "High" else "Low" endif — and every step is a form, not a script. It's technical without the fact that it has to be technical
  → **Done:** dropped the printed formula; now "each step is a form you fill in — pick a column, choose a condition, name the result — the way you'd set up a formula in a spreadsheet, not the way you'd write code." *(voice draft, marked for Fable)*
- Too preachy; millions of rows on an ordinary laptop is normal, not heroic.
  → **Done:** toned the bullet down to "a fast modern data engine, so large files stay workable where a spreadsheet would stall." Also toned the sibling boast in `coming-from-excel.md` ("stays fast well past that ceiling"). *(voice drafts, marked for Fable)*

In this page; http://localhost:8000/Flowfile/users/build-flows-visually.html
- Overall, this page has some technical references, which is fine, but the whole idea is that it is for people that build visually
  → **Done:** reduced the technical asides across §2/§4/§6 (below) and corrected the export claim; remaining voice polish is marked for Fable.
- The svg looks off. It can be better.
  → **Done:** redrew `flow-assembly-line.svg` — one continuous belt the stations sit on (was disconnected stripes), inputs aligned and feeding in cyan, equal-baseline previews inside the cards, small re-anchored "manual way" inset. Verified light + dark.
- I don't understand exactly what the point is, it's preachy... make it more focussed; `The palette groups everything into six categories — ...`
  → **Done:** tightened §2 "Know your toolbox" (six categories, ~five everyday nodes, node names = data operations; dropped the "trick to reading" sermon). *(voice draft, marked for Fable.)* Also wired the annotated palette image: composed `node-palette-annotated.svg` (five workhorses in cyan, rest dimmed).
- Build in Development, ship in Performance; add that it's not a deliberate step... cache results note... where you find the settings... add the gif hidden by default.
  → **Done:** reframed §4 — "one toggle, not a migration," Performance is automatic for scheduled/headless runs; Execution Mode lives in **Flow Settings**; Cache results explained + located (node → **General Settings**); `flow_settings.gif` embedded in a collapsed `<details>` fold-out. *(voice draft, marked for Fable.)*
- This is pretty technical... focus on managing schedules in the frontend, then a hide/unhide section for running elsewhere. `6. Automate what you built ...`
  → **Done:** reframed §6 — leads with managing **Schedules** in the app; the `flowfile run flow` CLI moved into a collapsed "Run it somewhere else" fold-out. *(voice draft, marked for Fable.)*

In this page http://localhost:8000/Flowfile/users/coming-from-excel.html;
- The svg, looks a little too unorganized in the excel side.
  → **Done:** redrew `sheet-vs-flow.svg` — moved the pivot out of the grid into its own box (was overlapping cells/arrows), cut the tangle to a few legible arrows clear of the `=` marks, filled `=` down all rows, rebalanced the panel; right panel untouched. Verified light + dark.

In this page; http://localhost:8000/Flowfile/users/analyze-your-data.html;
- The connection to the notebook should be more obvious + this can be a collapse/uncollapse section `The same works from a notebook or script ...`
  → **Done:** made the notebook link prominent ("Prefer to work in code? The same table is a line away from a **notebook** …") and folded the CI-tested round-trip into a collapsed `<details>`. *(voice draft, marked for Fable.)*

In this page http://localhost:8000/Flowfile/users/write-python.html;
- This code block contains a mistake; `.agg(...).collect()` it should not collect in the function
  → **Done:** the `.collect()` was on `read_catalog_sql(...)` (a lazy `FlowFrame`) in `docs/examples/catalog_analysis.py`. Moved it below the `# --8<-- [end:example]` marker so the shown snippet ends lazy. Test `test_core_example_runs[catalog_analysis]` passes.
- The write python should also mention notebooks / `flowfile_ctx` / is it available outside notebooks / `flowfile.get_catalog()` — if not add a todo.
  → **Done:** §5 already covers notebooks + `flowfile_ctx`; added that `flowfile_ctx` is **kernel/notebook-only** and plain scripts reach the catalog via `ff.read_catalog_table` / `ff.write_catalog_table` (+ `from flowfile_frame import read_catalog_sql`). Verified `get_catalog()` does **not** exist → added a `# TODO(catalog)` in `flowfile/flowfile/__init__.py` for a `get_catalog()` convenience + re-exporting `read_catalog_sql`.
- Mention that flowfile expressions (ff.col('test') == 'test') get transformed to flowfile formula strings as well.
  → **Done:** rewrote §6 "Know the two dialects" — every `ff.col(...)` carries a bracketed formula rendering (`ff.col("amount") > 100` → `[amount] > 100`); `with_columns` promotes such expressions to **editable native Formula nodes**; `filter(expr)` stays a code node (this corrected a previously-inaccurate claim). *(marked for Fable.)*

In this page; http://localhost:8000/Flowfile/what-is-flowfile-technical.html;
- The claim `transformation flows as plain Polars with no Flowfile dependency` is an overstatement... fix and scan for all.
  → **Done:** corrected **all 10** occurrences (found one extra in `deployment/lite.md`; grep now returns 0). Accurate wording: Polars with **no `flowfile` import**, but some formula/fuzzy/graph nodes pull a lightweight `polars_*` helper package (`polars_expr_transformer` / `pl_fuzzy_frame_match` / `polars_grouper`) — never `flowfile`. Full nuance on developer/technical pages, honest-short on persona pages. Left `ml.md`'s accurate "pure Polars expression" alone.

---

## Follow-up feedback (raised in review, after the first batch)

- **Diagram backlog** — drew and wired **18** concept/developer SVGs (the `IMAGE-PLACEHOLDER` set: `recipe-to-flow`, `flow-assembly-line`, `sheet-vs-flow`, `sharing-model`, the five `catalog/*` dev diagrams, etc.), each verified light + dark; wired the landed screenshots (catalog-notebook, projects-panel, kafka-source-settings, child-flow-canvas, run-flow-settings). Backlog tracked in `DOCS_IMAGE_TODO.md`.
- **`architecture-overview.svg` — kernel edge inaccurate** ("Delta write-back"). → **Done:** kernels write Delta straight to the storage volume and POST only metadata; relabeled `metadata → core` / `Delta → volume` (verified against `kernel_runtime/flowfile_client.py`).
- **`architecture-overview.svg` — "weird"; distinguish what's stored vs how you interact.** → **Done:** restructured into two layers — the CatalogService **feature-services** grid vs a distinct **storage** band (the two substrates); scheduler/worker attach to services, projects/kernels to storage.
- **Service chips should be distinct (tables/previews/artifacts/schedules).** → **Done:** grouped into **Organize** (Namespaces, Flows, Tables, Virtual tables, Artifacts) · **Analyze** (SQL, Previews, Visualizations, Dashboards, Notebooks) · **Operate** (Schedules, Runs, Favorites, Stats). Added missing **Flows**; **Engagement → Favorites** (it's `FlowEngagementService` = favourites & follows, not API serving); added **Stats**.
- **Convert two mermaid flowcharts to brand SVGs (kernel arch, system arch).** → **Done:** authored `kernel-flow.svg` (→ `kernel-architecture.md`) and `process-map.svg` (→ `architecture.md`); removed the mermaid blocks.
- **`process-map.svg` — two lines between SqlService and Worker.** → **Done:** they were paths ② (SQL editor) and ③ (notebook SQL cells), the same route; merged into one line labeled with both.
- **Virtual-table resolution is misleading — the "slow lane" is also lazy.** → **Done:** verified against code (`resolve_virtual_flow_table` sets `flowframe.lazy = True`). Reframed the diagram + prose + alt-text: **stored plan** (deserialize, no run) vs **rebuilt plan** (re-run the flow to reconstruct the LazyFrame); both stay lazy, nothing materializes until a **collect** (a write, a visualization, or a catalog query).
- **`recipe-to-flow.svg` icons should be in the right style.** → **Done:** kept the warm hand-drawn recipe card; swapped the four node icons for the real Flowfile node glyphs (input_data / filter / formula-as-`ƒx`-lineart / output).
- **No reference to serving a flow as an API endpoint.** → **Done:** added the **⑤** path to `process-map.svg` + a 5th execution-path bullet in `architecture.md`; a user section **"Serve flows as APIs"** in `catalog/index.md`; and a developer section **"Serving flows as APIs"** in `catalog-architecture.md`. *(the last is marked for Fable — verify it belongs on that page.)*
- **Make the review markers visible, not hidden.** → **Done:** converted all Fable markers from HTML comments to on-page `!!! warning "To review (Fable)"` admonitions (inline `_[Fable review …]_` tags where they sit mid-list).
- **A notebook-in-action gif + an ML-flow screenshot** were added as placeholders and tracked in `DOCS_IMAGE_TODO.md` §2b; the `ml.md` mermaid was replaced with a screenshot placeholder.
