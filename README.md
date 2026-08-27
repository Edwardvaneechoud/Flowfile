<h1 align="center">
  <img src=".github/images/logo.png" alt="Flowfile Logo" width="100">
  <br>
  Flowfile
</h1>

<p align="center">
  <b>Visual ETL that compiles to Polars.</b>
  <br>
  <sub>Build pipelines on a canvas or in Python — two views of the same graph.<br>Runs on your laptop, in Docker for a team, or entirely in your browser.</sub>
</p>

<p align="center">
  <a href="https://github.com/edwardvaneechoud/Flowfile/actions/workflows/test.yaml"><img src="https://img.shields.io/github/actions/workflow/status/edwardvaneechoud/Flowfile/test.yaml?branch=main&style=flat-square&logo=github&label=tests" alt="CI status"></a>
  <a href="https://codecov.io/gh/edwardvaneechoud/Flowfile"><img src="https://img.shields.io/codecov/c/github/edwardvaneechoud/Flowfile?style=flat-square&logo=codecov&logoColor=white" alt="Coverage"></a>
  <a href="https://pypi.org/project/Flowfile/"><img src="https://img.shields.io/pypi/v/Flowfile?style=flat-square&logo=pypi&logoColor=white" alt="PyPI version"></a>
  <a href="https://pypi.org/project/Flowfile/"><img src="https://img.shields.io/pypi/dm/Flowfile?style=flat-square&logo=pypi&logoColor=white" alt="PyPI downloads"></a>
  <a href="https://pypi.org/project/Flowfile/"><img src="https://img.shields.io/pypi/pyversions/Flowfile?style=flat-square&logo=python&logoColor=white" alt="Python versions"></a>
  <a href="LICENSE"><img src="https://img.shields.io/github/license/edwardvaneechoud/Flowfile?style=flat-square" alt="License"></a>
  <a href="https://github.com/edwardvaneechoud/Flowfile/stargazers"><img src="https://img.shields.io/github/stars/edwardvaneechoud/Flowfile?style=flat-square&logo=github" alt="GitHub stars"></a>
</p>

<p align="center">
  <a href="https://demo.flowfile.org"><b>▶&nbsp;&nbsp;Try it in your browser&nbsp;&nbsp;→</b></a>
  <br>
  <sub>No install. No signup. Polars in the browser via Pyodide.</sub>
</p>

<p align="center">
  <a href="https://edwardvaneechoud.github.io/Flowfile/">Docs</a> ·
  <a href="https://github.com/edwardvaneechoud/Flowfile/releases">Releases</a> ·
  <a href="https://github.com/edwardvaneechoud/Flowfile/discussions">Discussions</a> ·
  <a href="https://dev.to/edwardvaneechoud/building-flowfile-architecting-a-visual-etl-tool-with-polars-576c">Architecture deep-dive</a>
</p>

---

Build pipelines on a visual canvas with a live preview at every node, or write them in Python with a Polars-like API. Code and visual are two views of the same graph: drag nodes or write Polars-style code, your choice. Beyond the canvas: a Delta-backed catalog with time-travel and virtual tables, a SQL editor with embedded viz, a scheduler, flow parameters, sandboxed Python kernels, and group sharing for teams.

- Just want to see it? [demo.flowfile.org](https://demo.flowfile.org).
- Transforming files on your laptop? `pip install Flowfile` (Python 3.10–3.13).
- Running it for a team? Docker Compose, which adds accounts, groups and a shared catalog. See the [Quick Start](#quick-start).

<div align="center">
  <img src=".github/images/superstore_demo.gif" alt="Building a Superstore pipeline on the canvas — filter, join, pivot, and aggregate" width="800"/>
  <br>
  <sub>Building a Superstore pipeline on the canvas — filters, a join, a pivot, and aggregations, with a live data preview updating at every step.</sub>
</div>

&nbsp;

---

## What's in Flowfile

### Canvas and code

**A visual canvas** with 45 node types — joins, fuzzy matching, filters, pivots, aggregations, text-to-rows, window functions. Beyond the nodes, the formula editor brings 95 transformation functions, and a Polars code node gives you full Polars for anything the palette doesn't cover — all running in-process, no external engine.

Flowfile is a transformation tool rather than an ingestion platform, and its connections reflect that: local files (CSV, Parquet, Excel, JSON, IPC, NDJSON, Avro), five databases (PostgreSQL, MySQL, SQL Server, SQLite, DuckDB), cloud storage (S3, ADLS, GCS — Delta and Iceberg tables included, though Iceberg is read-only for now), Kafka (consuming; there's no producer yet), Google Analytics, and REST APIs — and it writes wherever you point it. That's the whole list, honestly: no Snowflake, BigQuery or Oracle driver yet, and no CDC. If your data lives in one of those, land it somewhere Flowfile can reach — a bucket, a Postgres, plain files — and it takes over from there.

**A Python API** with Polars-like syntax. Code and visual are two ways to build the same object graph — write a pipeline, call `open_graph_in_editor()`, and see it visually without re-building anything. Since the syntax mirrors Polars, porting an existing Polars script over is mostly mechanical.

<div align="center">
  <img src="docs/assets/images/quickstart/python_example.png" alt="A pipeline built with the Flowfile Python API, opened in the visual editor" width="800"/>
  <br>
  <sub>A pipeline written with the Python API, opened as an editable flow in the visual editor.</sub>
</div>

&nbsp;

**An AI assistant.** Tell it what you want and it builds the flow with you, on the canvas. Bring your own key — Anthropic, OpenAI, Google, Groq, OpenRouter — or point it at Ollama or a local model.

<div align="center">
  <img src="docs/assets/images/ai/ai-overview.gif" alt="Flowfile AI assistant building a pipeline on the canvas" width="800"/>
  <br>
  <sub>Describe what you want, get a runnable flow.</sub>
</div>

&nbsp;

**Code generation.** Prototype visually, ship a plain script: flows export as Python, and save as human-readable YAML so version control works. A flow of standard transforms on local files exports as pure Polars, usually with `import polars as pl` as its only import.

<details>
<summary>What the export needs, exactly</summary>

Fuzzy matching, graph solving, and formulas that don't translate to a native Polars expression pull in the helper packages Flowfile itself is built on (`pl-fuzzy-frame-match`, `polars-grouper`, `polars-expr-transformer`). They're normal pip installs and don't drag Flowfile along.

Database and REST nodes export as `flowfile` calls, so their stored connections and secrets resolve at run time. The platform nodes (catalog, cloud storage, Kafka, ML) export against the FlowFrame API rather than raw Polars. And a few nodes (Google Analytics, SQL query, API response) have no code generation yet.

</details>

<div align="center">
  <img src=".github/images/generated_code.png" alt="Export visual flows as Polars code" width="800"/>
  <br>
  <sub>The same flow as code: toggle between pure Polars and FlowFrame output.</sub>
</div>

&nbsp;

### Around the canvas

Everything a flow produces can land in the data catalog: a catalog > schema > table hierarchy with Delta Lake underneath, so tables get version history and time travel. Flows write into it through a Catalog Writer node — or register their output as a virtual table, with nothing materialised. For those, Flowfile stores the Polars query plan rather than the data (as long as the producing graph is lazy-safe), so a consumer's filters push down straight through the flow boundary, and upstream Delta versions are tracked per read to catch stale data.

There's a SQL editor on top (Polars SQLContext under the hood): query any registered table, chart the result in the embedded Graphic Walker, and if an ad-hoc query turns out to be useful, save it as a flow in one click.

<div align="center">
  <img src=".github/images/sql_editor.png" alt="SQL editor with Graphic Walker visualization" width="800"/>
  <br>
  <sub>SQL queries run against catalog tables, with results feeding into Graphic Walker for visual exploration.</sub>
</div>

&nbsp;

You can put flows on a schedule: on an interval, when a catalog table updates, or once a whole set of tables has refreshed. Run history, logs and cancellation live in the UI, and the scheduler runs embedded, standalone or in Docker. Any node setting takes `${variable}` parameters — file paths, SQL queries, formulas — with defaults managed in the Designer and overridden at run time with `--param`.

With the Docker deployment it all becomes multi-user: accounts, an admin role, user groups. Share a connection, flow or catalog namespace with a group at "use" or "manage" level (secrets can be shared too, read-only); everything else stays private to its owner. The desktop app is single-user.

**Python kernels.** Run user code in isolated Docker containers with their own package environments, keeping the host process safe — the one thing they ask in return is that Docker is running locally. Jupyter-style notebook editor with cell execution, autocompletions, and rich display output (matplotlib, plotly, PIL, HTML).

**Custom nodes and a community registry.** Build your own nodes in the visual Node Designer — a typed settings form, live preview, and a single-file `.py` output — then share them through the [community registry](https://github.com/edwardvaneechoud/flowfile-community-nodes). Publishing is a pull request opened straight from the app; installing is one click from Catalog → Community Nodes, with sha256-pinned downloads and a capability consent dialog.

**An embeddable editor.** The browser editor also ships as a standalone Vue component, [`flowfile-editor`](https://www.npmjs.com/package/flowfile-editor), so you can drop a Polars-powered visual ETL canvas into any web app with zero backend: `npm install flowfile-editor`.

**Templates and clipboard import.** Get started with built-in flow templates, or paste tabular data from Excel / Google Sheets directly onto the canvas to create a pre-filled input node.

A single flow can filter, join, pivot and aggregate, then branch into as many outputs as you need:

<div align="center">
  <img src="docs/assets/images/guides/sales_dashboard/dashboard_overview.png" alt="A complete Flowfile pipeline with joins, pivots, and aggregations feeding multiple outputs" width="800"/>
  <br>
  <sub>Joins, pivots and aggregations feeding a product leaderboard, a monthly trend and a city matrix.</sub>
</div>

---

## Quick Start

**In the browser** — [demo.flowfile.org](https://demo.flowfile.org) runs a 23-node subset on Pyodide; good for a first look and small files.

**On your laptop** (Python 3.10–3.13):

```bash
pip install Flowfile
flowfile run ui
```

Or start from code:

```python
import flowfile as ff
from flowfile import col, open_graph_in_editor

df = ff.from_dict({
    "id": [1, 2, 3, 4, 5],
    "category": ["A", "B", "A", "C", "B"],
    "value": [100, 200, 150, 300, 250]
})

result = (
    df.filter(col("value") > 150)
      .with_columns((col("value") * 2).alias("double_value"))
      .group_by("category")
      .agg(col("value").sum().alias("total"))
)

open_graph_in_editor(result.flow_graph)
```

**For a team** — the Docker stack runs core, worker, and the web UI with user accounts and a shared catalog:

```bash
git clone https://github.com/edwardvaneechoud/Flowfile.git
cd Flowfile
docker compose up -d   # UI at http://localhost:8080
```

The compose file in this repo builds from source. To run the published Docker Hub images on a server, behind HTTPS, use [flowfile-hosting](https://github.com/edwardvaneechoud/flowfile-hosting).

**Desktop app** — installers for Windows, macOS, and Linux on the [Releases](https://github.com/edwardvaneechoud/Flowfile/releases) page.

<details>
<summary><b>From source</b> — for contributors (Python 3.10–3.13, Node.js 20+)</summary>

```bash
git clone https://github.com/edwardvaneechoud/Flowfile.git
cd Flowfile
poetry install

# Backend (two separate terminals)
poetry run flowfile_worker  # :63579
poetry run flowfile_core    # :63578

# Frontend
cd flowfile_frontend
npm install && npm run dev:web  # :8080
```

</details>

> **Note:** Windows installers aren't code-signed yet — SmartScreen will warn; click "More info" → "Run anyway". On macOS, if the app shows as damaged after download: `find /Applications/Flowfile.app -exec xattr -c {} \;`

---

## Where Flowfile fits

Flowfile is deliberately a one-machine tool: Polars in-process, no cluster to stand up. Core and worker can run as separate containers, but they share a filesystem, not a fleet. That's a real limit and a real feature at once — it's why the whole platform installs with `pip`, runs in a browser tab, and never asks you to size anything. And a surprising amount of data fits on one good machine.

| Instead of | The difference |
|---|---|
| **Alteryx / KNIME** | The same canvas idea, but MIT-licensed, Polars underneath, and every pipeline exports to Python you can take with you. |
| **dbt** | dbt transforms data that's already in a warehouse. Flowfile works on files, databases and streams directly, no warehouse needed, and adds a visual layer. |
| **Airflow / Dagster** | Orchestrators run pipelines; Flowfile is where the pipeline gets built. It has a small scheduler of its own, and exported scripts run fine under any orchestrator. |
| **Plain Polars** | You keep Polars. Flowfile adds a canvas, a preview at every node, a catalog and a scheduler, and gets out of the way again when you export. |

---

## Architecture

Three services, plus an embedded **scheduler** and a sandboxed **kernel runtime** for the Python Script nodes:

- **Designer** (Tauri + Vue) — visual interface
- **Core** (FastAPI) — ETL engine running Polars (`:63578`)
- **Worker** (FastAPI) — computation and caching (`:63579`)

Each flow is a directed acyclic graph: nodes are data operations, edges are data flow.

Deeper dive: [Architecting a Visual ETL Tool with Polars](https://dev.to/edwardvaneechoud/building-flowfile-architecting-a-visual-etl-tool-with-polars-576c).

---

## Project status

Actively developed, pre-1.0. Releases go out from this repo to PyPI, the desktop installers, Docker Hub, and npm. The test suite is over 7,000 Python tests plus 150+ frontend and WASM test files, spread over seven CI test workflows (unit, Playwright e2e, kernel, Kafka, Docker auth); backend coverage lands on [Codecov](https://codecov.io/gh/edwardvaneechoud/Flowfile).

What's next is tracked in [Issues](https://github.com/edwardvaneechoud/Flowfile/issues) and discussed in [Discussions](https://github.com/edwardvaneechoud/Flowfile/discussions); every release has a feedback thread on [Releases](https://github.com/edwardvaneechoud/Flowfile/releases).

## License

[MIT](LICENSE)

---

## Acknowledgments

Built on [Polars](https://pola.rs/), [Vue.js](https://vuejs.org/), [FastAPI](https://fastapi.tiangolo.com/), [VueFlow](https://vueflow.dev/), [Delta Lake](https://delta.io/), [Graphic Walker](https://github.com/Kanaries/graphic-walker), and [Tauri](https://tauri.app/).
