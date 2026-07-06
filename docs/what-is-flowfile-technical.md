# Flowfile, the technical version

*The technical lens on [What is Flowfile](what-is-flowfile.md). Sending this to a non-technical colleague? Give them the [plain-terms lens](what-is-flowfile-plain.md).*

Flowfile bundles a visual flow editor, a data catalog, a scheduler, and a Polars-based Python API into one package. Every part has a standalone equivalent you already know; the value proposition for an experienced builder is the integration work removed. Below is what you no longer build or maintain, each with the mechanism behind it.

<!-- IMAGE-PLACEHOLDER-TO-CHANGE: draw.io — system boundary diagram: user-authored transformation logic in the center; platform-provided services around it, each labeled with its mechanism: secret store (Fernet, per-user keys), kernel containers (resource caps, pinned deps), connection layer (named references), scheduler (cron + table triggers), catalog storage (Delta, versioned), run history/lineage -->

## What it automates

**Secret management.** Credentials are stored once, encrypted at rest (Fernet with per-user key derivation), and referenced by name from both the UI and code. Flow files never contain secrets, so they can be committed and shared. In multi-user mode, secrets [shared with a group](users/deployment/sharing.md) are use-only: member flows execute with them, no one can read the values, and changing a shared connection's host/endpoint requires re-entering credentials.

**Python environments.** [Kernels](users/visual-editor/kernels.md) are Docker containers with CPU/memory limits, optional GPU passthrough, and a pinned package set. Everyone who runs the flow or opens the [notebook](users/visual-editor/catalog/notebooks.md) executes against the same environment. User code accesses data through `flowfile_ctx`; raw credentials are not exposed to it.

**Source connections.** [Named connections](users/data-elsewhere.md) cover PostgreSQL/MySQL/SQLite, S3/ADLS/GCS (CSV, Parquet, JSON, Delta, Iceberg), Kafka, REST endpoints, and GA4. The database reader accepts a query, so filtering and pre-aggregation can run at the source. [Kafka consumption](users/connect/kafka.md) is incremental by consumer group: offsets commit only after a successful run, so a scheduled flow reads exactly the messages that arrived since its last successful run.

**Data freshness.** [Cron schedules and table triggers](users/visual-editor/catalog/schedules.md) are part of the catalog; the scheduler is embedded in the core service (a standalone mode exists). A table update triggers the flows watching it; set-triggers fire when all listed tables have updated. Dependent-pipeline execution is derived from trigger edges — there is no separate DAG definition to maintain.

**Data organization.** The [catalog](users/visual-editor/catalog/index.md) stores results as Delta tables with version history and time travel, organized in namespaces. Lineage is recorded in both directions: each table knows the flow that produced it and the flows that read it. Runs are recorded with per-node results and a snapshot of the flow version that executed.

**Visualization.** Any catalog table or SQL result opens in [Graphic Walker](users/visual-editor/catalog/visualizations.md); chart definitions are stored with the data. In notebook cells: `flowfile_ctx.explore(lf)`.

**Explaining what a pipeline does.** Flows are graphs with labeled, describable nodes, whether built on the canvas or in code — `ff.open_graph_in_editor(df.flow_graph)` renders a code-built pipeline for anyone to inspect. Combined with run snapshots, "what produced this number" is answerable from the UI without the author present.

## Standard engineering concerns

- **Reproducibility and CI.** Flows are plain `.yaml`. `flowfile run flow <path> --param k=v` executes one headlessly, exit code 0/1, no services required. [Projects](users/projects.md) mirror flows, credential-free connections, and catalog metadata into a git repository automatically. The code examples in these docs are repository files executed by CI; this one writes to the catalog and queries it back:

    ```python
    --8<-- "docs/examples/catalog_analysis.py:example"
    ```

- **Performance.** Polars end to end, lazy by default — pipelines are optimized as whole plans. In the server deployment, full-dataset compute runs in a separate worker service (spawned subprocesses own dataset memory), keeping the API process responsive.
- **Lock-in.** Bidirectional: code renders on the canvas, and flows [export as Python](users/visual-editor/tutorials/code-generator.md) — pure-transformation flows as plain Polars with no Flowfile dependency, I/O-bearing flows with an `ff` import for connection resolution. Table data is Delta/Parquet on disk, readable by any tool.
- **Architecture.** FastAPI core, separate compute worker, Docker kernels, embedded scheduler; a browser-only WASM build runs the editor with no backend. Details in [Architecture](for-developers/architecture.md) and [Catalog Architecture](for-developers/catalog-architecture.md).

## Design goal

The same one stated on the plain-terms page: every pipeline, table, and schedule is reproducible from its stored definition. The features above exist to make that hold without per-project effort.

**Fastest first taste:** `pip install flowfile`, run the [tested first pipeline](users/write-python.md#1-first-pipeline) — or [open the sales pipeline in your browser](assets/try-sales-pipeline.html) with nothing installed. Then take the [Write Python route](users/write-python.md).
