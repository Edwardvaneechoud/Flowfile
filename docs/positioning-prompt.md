# Flowfile Positioning & Messaging Guide

Use this as a prompt/reference when writing about Flowfile for social media, blog posts, talks, README badges, or conversations with potential users and contributors.

---

## The One-Liner

**Flowfile is an open-source data platform with a visual pipeline builder, data catalog, Delta Lake storage, scheduling, Kafka ingestion, and sandboxed Python execution — and it installs with `pip install Flowfile`.**

---

## The Elevator Pitch (30 seconds)

Most data tools make you choose: visual and shallow, or powerful and impossible to set up. Flowfile is both. You get a drag-and-drop canvas with 30+ transformation nodes, a data catalog backed by Delta Lake with full version history, scheduling that triggers on data changes, Kafka ingestion as a canvas node, sandboxed Python execution in Docker containers, and code generation that turns any visual flow into a standalone Polars script. It's a single `pip install`. No Kubernetes. No YAML. No separate orchestration tool.

---

## Key Differentiators (use these in feature comparisons)

### 1. Everything serves the canvas
Kafka isn't a separate streaming module — it's a node you drop on the canvas. Scheduling isn't a separate orchestration UI — it lives in the catalog. Delta Lake isn't a storage backend you configure — it's just how the catalog works. This isn't a collection of features bolted together. Every capability is one or two clicks from the flow designer.

### 2. Zero-ops installation
`pip install Flowfile && flowfile run ui`. That command gives you the visual builder, backend engine, data catalog, scheduler, and web UI. Compare that to standing up Airflow + dbt + a metadata catalog + a streaming ingestion tool.

### 3. No vendor lock-in by design
Click "Generate Code" and your visual flow becomes a standalone Python/Polars script. No Flowfile dependency required. You can prototype visually and ship as code. Or keep using Flowfile — your choice.

### 4. Polars-native performance
Not a wrapper around pandas. Not an abstraction over Spark. Flowfile is built directly on Polars, which means columnar processing, lazy evaluation, and the ability to handle datasets that don't fit in memory — without a cluster.

### 5. Bidirectional: code to canvas, canvas to code
Build a pipeline in Python with the FlowFrame API, then visualize it on the canvas with `open_graph_in_editor()`. Or build visually and export to code. Both directions work.

---

## Talking Points by Audience

### For Data Engineers
- "It's like if Dagster, dbt, and NiFi had a baby that installed with pip"
- Delta Lake catalog with upsert/merge, version history, and table-trigger scheduling
- Export any visual flow to production-ready Polars code
- Kafka ingestion without standing up Kafka Connect
- Flow parameters with `${variable}` substitution and CLI overrides

### For Data Analysts
- Build pipelines visually — no code required
- Preview data at every step
- Fuzzy matching, pivots, text-to-rows, and 30+ transformation nodes
- Schedule your flows to run automatically
- Works with Excel, CSV, Parquet, databases, and cloud storage

### For Engineering Managers
- Single tool replaces multiple components of the modern data stack
- MIT licensed, no vendor lock-in (code generation)
- Runs on a laptop or in Docker — no infrastructure team needed
- Python API means developers can automate what analysts prototype

### For Open Source Evaluators
- Solo-maintainer project with unusual architectural discipline
- Clean separation: core (FastAPI), worker (FastAPI), frontend (Vue 3/Electron), frame (Python library)
- Active development: catalog, Delta Lake, scheduling, Kafka, kernels all shipped in recent releases
- Genuine community opportunity — the gap between capability and visibility is large

---

## What Flowfile Is NOT

Be honest about boundaries. Credibility comes from knowing what you're not:

- **Not a distributed compute engine** — It's Polars on a single machine (which handles a lot more than people expect)
- **Not a real-time streaming platform** — Kafka ingestion is batch-oriented (poll N messages), not continuous streaming
- **Not a notebook** — The Python Script node runs code in kernels, but the primary interface is the canvas, not a notebook
- **Not trying to replace Spark** — If you have petabyte-scale data across a cluster, Flowfile isn't the tool. If your data fits on one machine (and most data does), it might be faster

---

## Social Media Templates

### Twitter/X (short)
> Built a visual data pipeline, clicked "Generate Code", got a standalone Polars script.
>
> No lock-in. No YAML. No Kubernetes.
>
> `pip install Flowfile`

### Twitter/X (feature highlight)
> Flowfile now has:
> - Visual pipeline builder (30+ nodes)
> - Data catalog with Delta Lake
> - Scheduling with table-change triggers
> - Kafka ingestion as a canvas node
> - Sandboxed Python execution
> - Code generation to Polars
>
> Still installs with `pip install Flowfile`.
> That's not normal for an open source project at this stage.

### LinkedIn (professional)
> Most open-source data tools at this feature level require Kubernetes, multiple services, and a dedicated infrastructure team. Flowfile doesn't.
>
> It combines a visual pipeline builder, a Delta Lake-backed data catalog, scheduling, Kafka ingestion, and sandboxed Python execution into a single `pip install`. Every feature is designed around the visual canvas — nothing feels bolted on.
>
> If you're building data pipelines and want something that's both powerful and simple to set up, it's worth 5 minutes of your time.

### Hacker News (technical)
> Flowfile is an open-source visual ETL platform built on Polars and FastAPI. What makes it different:
>
> 1. Data catalog backed by Delta Lake (version history, merge/upsert, time travel)
> 2. Scheduling triggered by catalog table updates (not just cron)
> 3. Kafka ingestion as a drag-and-drop node
> 4. Sandboxed Python execution in Docker containers
> 5. Code generation: visual flow → standalone Polars script
> 6. `pip install Flowfile` installs everything
>
> The architecture is unusually coherent for a project with this feature surface area. Every capability is integrated into the canvas, not bolted on as a separate module.

### Reddit (r/dataengineering)
> I've been working on Flowfile — an open-source data platform that combines a visual pipeline builder with a proper data catalog, Delta Lake storage, scheduling, and Kafka ingestion.
>
> The thing I'm most proud of: it still installs with `pip install Flowfile`. No Docker required for the base platform (just for sandboxed Python execution). No Kubernetes. No 15-page setup guide.
>
> Key features:
> - 30+ canvas nodes (joins, fuzzy matching, pivots, etc.)
> - Delta Lake catalog with version history and merge/upsert
> - Schedule flows on intervals or trigger on table changes
> - Kafka/Redpanda ingestion as a canvas node
> - Export any visual flow as standalone Polars code
> - Python API with Polars-like syntax
>
> Would love feedback from people who've struggled with the complexity of the modern data stack.

---

## Comparison Positioning

| When someone says... | Respond with... |
|---------------------|-----------------|
| "How is this different from Apache NiFi?" | "NiFi is Java, enterprise-heavy, and requires dedicated infrastructure. Flowfile is `pip install` and focuses on data transformation with Polars performance, not just data routing." |
| "Why not just use Dagster/Prefect?" | "Those are orchestrators — they run your code on a schedule. Flowfile is where you build the transformations themselves, with a visual canvas. It also has its own scheduler, but the core value is the pipeline builder + catalog." |
| "Can't I just write Python?" | "You can — and Flowfile generates that Python for you. Build visually, export as code. Or use the Python API to build programmatically and visualize on the canvas. Both directions work." |
| "Does it scale?" | "It's Polars on a single machine, which handles larger datasets than most people expect. If your data fits on one machine — and most data does — Flowfile is likely faster than your Spark cluster for that workload." |
| "It looks like a visual CSV cleaner" | "That's the perception gap we're closing. It has a Delta Lake catalog, Kafka ingestion, table-trigger scheduling, sandboxed Python execution, and code generation. Try `pip install Flowfile` and see." |
