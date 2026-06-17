# Flowfile Lite (Browser)

Flowfile Lite is the **zero-install, browser-only** edition of the visual editor. Polars runs entirely in WebAssembly via [Pyodide](https://pyodide.org/), so your flows execute client-side with **no backend, no account, and no data leaving your browser**.

<div align="center" style="padding: 1rem;">
  <a href="https://demo.flowfile.org"><b>▶&nbsp;&nbsp;Try Flowfile Lite in your browser&nbsp;&nbsp;→</b></a>
  <br>
  <sub>No install. No signup. Polars in the browser via Pyodide.</sub>
</div>

It is also published as the embeddable npm package [`flowfile-editor`](https://www.npmjs.com/package/flowfile-editor), so you can drop the editor into your own web app.

!!! info "Lite vs. the full build"
    Flowfile Lite is a deliberately **lightweight subset** of Flowfile. It covers the most common file-based ETL work, but it has **no Python backend** — which means no databases, cloud storage, scheduler, kernels, AI assistant, or the Python API. For any of those, install the [full build](index.md) (Desktop, Python package, or Docker). See the [feature comparison](#feature-comparison) below.

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

Flowfile Lite ships **18 nodes** across 5 categories — the same canvas, settings panels, and Polars semantics as the full editor:

| Category | Nodes |
|----------|-------|
| **Input** | Read File (CSV · Excel · Parquet, local upload or remote URL), Manual Input, Read from Catalog, External Data¹ |
| **Transform** | Filter, Select, Sort, Unique, Take Sample, Polars Code |
| **Combine** | Join |
| **Aggregate** | Group By, Pivot, Unpivot |
| **Output** | Explore Data (Graphic Walker), Write Data (download CSV · Excel · Parquet), Write to Catalog, External Output¹ |

¹ *External Data / External Output are host-integration nodes used when Flowfile Lite is embedded as a library — they let the host app feed in and read out datasets.*

It also supports **exporting a flow to a standalone Python/Polars script** and a lightweight **in-browser catalog** (CSV-only) for saving and reusing tables between flows.

!!! tip "No Formula node? Use Polars Code"
    Flowfile Lite swaps the visual [**Formula** node](../visual-editor/nodes/transform.md#formula) for the **Polars Code** node, where you can write any Polars expression directly (with autocompletion). Everything `with_columns` does in the full build is available here as code.

---

## What's *not* included

Everything that depends on the Python backend, worker, or kernel containers is **unavailable** in Flowfile Lite:

- **Databases** — no PostgreSQL / MySQL / SQL Server / Oracle readers or writers
- **Cloud storage** — no S3 / Azure ADLS / Google Cloud Storage
- **Kafka** ingestion and **REST API / Google Analytics** sources
- **Kernels / Python Script** — no sandboxed user-code execution
- **SQL editor** and SQL query node
- **Machine Learning nodes** (Train / Apply / Evaluate Model)
- **Scheduler** and flow automation
- **AI assistant** (Chat, Agent, Cmd+K, inline actions)
- **Secrets & connections manager** — no credentials are stored
- **Governed catalog** — no Delta Lake versioning, virtual tables, lineage, or saved visualizations
- **The Python API** (`flowfile_frame`) — Lite is visual-only
- Extra transforms: **Add Record ID, Text to Rows, Fuzzy Match, Union, Cross Join, Graph Solver**

Memory is bounded by the browser heap, and data previews are capped (100k rows).

---

## Feature comparison

| Feature | Full Flowfile | Flowfile Lite |
|---------|:-------------:|:-------------:|
| Install / runtime | pip · Desktop · Docker | None — runs in the browser |
| Compute engine | Polars on Python backend + worker | Polars compiled to WebAssembly (Pyodide) |
| Nodes | 40+ | 18 |
| Local files (CSV / Excel / Parquet) | ✓ | ✓ |
| Remote URL fetch | ✓ | ✓ |
| Databases (Postgres / MySQL / …) | ✓ | ✗ |
| Cloud storage (S3 / ADLS / GCS) | ✓ | ✗ |
| Kafka / REST API / Google Analytics | ✓ | ✗ |
| Polars Code node | ✓ | ✓ |
| Python Script / Kernels | ✓ | ✗ |
| SQL editor & SQL query | ✓ | ✗ |
| Machine Learning nodes | ✓ | ✗ |
| Scheduler & automation | ✓ | ✗ |
| AI assistant (BYOK) | ✓ | ✗ |
| Catalog | Delta-backed, versioned, virtual tables | Lightweight (CSV-only) |
| Secrets & connections manager | ✓ | ✗ |
| Export flow to Python | ✓ | ✓ |
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
