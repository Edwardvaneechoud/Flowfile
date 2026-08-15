# Flowfile Lite (Browser)

Flowfile Lite is the **zero-install, browser-only** edition of the visual editor. Polars runs entirely in WebAssembly via [Pyodide](https://pyodide.org/), so your flows execute client-side with **no backend, no account, and no data leaving your browser**.

[Try Flowfile Lite in your browser →](https://demo.flowfile.org) — no install, no signup; Polars in the browser via Pyodide.

It is also published as the embeddable npm package [`flowfile-editor`](https://www.npmjs.com/package/flowfile-editor), so you can drop the editor into your own web app.

!!! info "Lite vs. the full build"
    Flowfile Lite is a **lightweight subset** of Flowfile. It covers the most common file-based ETL work, but it has **no Python backend** — which means no databases, cloud storage, scheduler, kernels, AI assistant, or the Python API. For any of those, install the [full build](index.md) (Desktop, Python package, or Docker). See the [feature comparison](#feature-comparison) below.

---

## When to use it

| Use Flowfile Lite when… | Use the full build when… |
|-------------------------|--------------------------|
| You want to try Flowfile without installing anything | You need databases, cloud storage, or Kafka |
| You're transforming local CSV / Excel / Parquet files | You want to schedule or automate flows |
| Your data should never leave your machine | You want the Python API (`flowfile_frame`) |
| You want to embed a visual editor in your own web app | You need kernels, ML nodes, the AI assistant, or a governed catalog |

---

## What's included

Flowfile Lite ships **23 nodes** (as of 2026-07) across five active categories — the same canvas, settings panels, and Polars semantics as the full editor. A sixth category, Machine Learning, is present in the palette but its nodes are locked (full-build only).

| Category | Nodes |
|----------|-------|
| **Input Sources** | Read File (CSV · Excel · Parquet, local upload or remote URL), Manual Input, External Data¹, Read from Catalog |
| **Transformations** | Filter, Select, Formula, Sort, Polars Code, Unique, Rename, Record ID, Take Sample |
| **Combine Operations** | Join, Cross Join, Union |
| **Aggregations** | Group By, Pivot, Unpivot |
| **Output Operations** | Explore Data (Graphic Walker), Write Data (download CSV · Excel · Parquet), Write to Catalog, External Output¹ |

¹ *External Data / External Output are host-integration nodes used when Flowfile Lite is embedded as a library — they let the host app feed in and read out datasets.*

It also supports **exporting a flow to a Python/Polars script** and a lightweight **in-browser catalog** (CSV-only) for saving and reusing tables between flows.

!!! tip "Formula and Polars Code both ship"
    Lite includes the visual [**Formula** node](../visual-editor/nodes/transform.md#formula) for point-and-click column expressions *and* the **Polars Code** node for writing any Polars expression directly (with autocompletion).

---

## Learning mode

The Code panel has two tabs: **Polars** and **Python walkthrough**. Turning on **Learning mode** with the graduation-cap button in the left icon rail decides which one it opens on.

### The Python walkthrough

The same flow as one standalone script with no dataframe library — every table a `list[dict]`, every node an explicit loop — and a way to walk it a node at a time.

The script is the centre of the view and stays there. Step chips along the top move a highlight through it, so you always see the block you are reading about *in place*, with what came before it and what happens next still on screen. **Hover any name** — `sorted`, `setdefault`, `lambda`, one of the generated helpers — for a one-line explanation and a small example.

Underneath, **Data** shows the actual rows going in and coming out at the current step. Press **Show the data at each step** and the whole pipeline runs once in your browser, recording every intermediate table, so you can watch 1000 rows become 8 and read the loop that did it.

The editor is **editable**, and Lite is the one place where the script can **run where you are reading it**. Change a loop, press ▶, and the code executes in the same in-browser Python that runs the canvas, against the same files, with the rows shown in **Output**. **Compare to canvas** checks your version against what the flow produced.

At a wide enough window the panel docks beside the canvas rather than covering it, and the node you are reading about stays highlighted in the graph.

### Background, if you want it

By default there is no prose — just the code, the steps and the data. Press **Why does it look like this?** and a **Why** tab appears explaining the *pattern* behind the current step. It stays open as you step, and is remembered next time.

That is the part a code comment cannot carry. A group by is the accumulator-dict pattern, and it explains why that takes two passes and where else you will write the shape. A join is a hash index, and it shows why building a dictionary first turns rows×rows of work into rows+rows. A sort takes a key function — and it shows you that `None < 5` does not return `False`, it raises, which is why the key carries a `True`/`False` flag in front of the value.

Nodes with a plain-Python form: manual input, CSV read, filter (basic mode), select, sort, unique, group by, join (inner, left, semi and anti), cross join, union, record ID, take sample, rename (prefix/suffix), pivot, unpivot, and the write nodes.

Anything driven by the formula expression language — the Formula node, a filter in advanced mode, the Polars Code node — becomes an **exercise stub**: a function that quotes the rule it is supposed to apply and raises `NotImplementedError`. Fill it in, press ▶, and the rest of the script runs. A single one of them never fails the export, and the walkthrough still shows the data for every step before it.

Every node's settings panel carries the same thing at a smaller scale: a **"How would I write this myself?"** section with a plain-English description plus the loop for *that node's actual settings* — your columns, your operators — rather than a generic example.

!!! warning "It is a teaching output, not a production one"
    The generated script produces the same rows as the canvas, but it is deliberately not optimised and holds the whole table in memory as a list. Reading a CSV also leaves dates as text, where the engine recognises them — the generated helper flags that as an exercise.

!!! info "Embedders can turn it off"
    Set `teachingMode: false` on the `FlowfileEditor` component to hide the Python walkthrough tab and the per-node panel. The background prose is opt-in and collapsed by default, so an embedded editor stays a plain code view unless someone asks for more.

---

## What's *not* included

Everything that depends on the Python backend, worker, or kernel containers is **unavailable** in Flowfile Lite; those nodes appear greyed-out in the palette so the full breadth stays discoverable. The [feature comparison](#feature-comparison) below lists what's excluded; the **Window Functions** node is also unavailable.

Memory is bounded by the browser heap, and the Explore Data view materializes at most 100k rows for charting.

---

## Feature comparison

| Feature | Full Flowfile | Flowfile Lite |
|---------|:-------------:|:-------------:|
| Install / runtime | pip · Desktop · Docker | None — runs in the browser |
| Compute engine | Polars on Python backend + worker | Polars compiled to WebAssembly (Pyodide) |
| Nodes | 40+ | 23 (as of 2026-07) |
| Local files (CSV / Excel / Parquet) | ✓ | ✓ |
| Remote URL fetch | ✓ | ✓ |
| Databases (Postgres / MySQL / …) | ✓ | ✗ |
| Cloud storage (S3 / ADLS / GCS) | ✓ | ✗ |
| Kafka / REST API / Google Analytics | ✓ | ✗ |
| Formula node | ✓ | ✓ |
| Polars Code node | ✓ | ✓ |
| Python Script / Kernels | ✓ | ✗ |
| SQL Query node | ✓ | ✗ |
| Fuzzy Match / Graph Solver | ✓ | ✗ |
| Machine Learning nodes | ✓ | ✗ |
| Scheduler & automation | ✓ | ✗ |
| AI assistant (BYOK) | ✓ | ✗ |
| Catalog | Delta-backed, versioned, virtual tables | Lightweight (CSV-only) |
| Secrets & connections manager | ✓ | ✗ |
| Export flow to Python | ✓ | ✓ |
| Plain-Python (learning) export | ✓ | ✓ |
| Step-by-step Python walkthrough with live data | ✗ | ✓ |
| Edit + re-run the generated script | ✗ | ✓ |
| Graphic Walker visualization | ✓ | ✓ |
| Python API (`flowfile_frame`) | ✓ | ✗ |
| Data privacy | Sent to your backend/services as configured | Never leaves your browser |

---

## Embedding Flowfile Lite

The editor is published to npm as [`flowfile-editor`](https://www.npmjs.com/package/flowfile-editor). Host apps can mount the `FlowfileEditor` component, pass datasets in via `inputData`, drive it with a template-ref API (`executeFlow`, `exportFlow`, `importFlow`, …), and listen to `ready` / `output` / `execution-complete` events.

```bash
npm install flowfile-editor
```

See the [package README](https://github.com/edwardvaneechoud/Flowfile/tree/main/flowfile_wasm) for the full props, events, and API reference.

!!! warning "Cross-origin isolation required"
    Pyodide needs `SharedArrayBuffer`, so the host page must send COOP/COEP headers (`Cross-Origin-Opener-Policy: same-origin`, `Cross-Origin-Embedder-Policy: require-corp`) or the runtime will not load.
