# Flowfile Roadmap

**Flowfile 0.22 . updated 2026-10-08 · [discuss this roadmap](https://github.com/edwardvaneechoud/Flowfile/discussions/categories/announcements)**

Flowfile is for analytics and exploratory ETL. You start with a question,
work through the data, and end up with a pipeline worth keeping. You should
be able to run that pipeline wherever it makes sense, without being tied to
the setup where you built it.

Some of that already works. The canvas and the Python API edit the same
graph. Tables use Delta and live on disk or in your own bucket. Compute runs
in a separate worker, and flows export to Polars scripts. There are still
gaps, especially around running workers on separate machines, connecting to
external catalogs and exporting every node type. This roadmap covers those
gaps and what comes after.

There are no delivery dates here. Each track has a first step and a clear
way to check whether it works. We'll discuss the design publicly before
implementing it.

---

## 1. Where Flowfile sits

Visual tools like Alteryx, KNIME and Power Query start with a canvas and let
you add scripts where needed. Tools like dbt, Airflow and Dagster start with
code and show you a graph of what you wrote. Both approaches work.

In Flowfile, the canvas and Python API edit **the same graph**. An analyst
can build a flow visually, and an engineer can open it in Python and carry
on from there, with the same node IDs.

![The visual designer and Python API edit the same flow graph. The graph exports to a Polars script that runs without Flowfile.](docs/assets/images/roadmap/one-graph-three-renderings.svg)

It works the other way too: a flow written in Python opens as editable nodes
on the canvas. Both export to a Polars script that runs without Flowfile
installed.

This is roughly where we place Flowfile alongside other tools:

![A two-by-two chart comparing tools: Alteryx, KNIME and Power Query favour the canvas; dbt, Airflow, Dagster, Databricks and Polars favour code; Excel and one-off scripts sit bottom-left. Enso and Flowfile sit top-right, with both canvas and code as primary ways to work.](docs/assets/images/roadmap/where-flowfile-sits.svg)

*This is our view of each tool's priorities. Product names belong to their
respective owners, and Flowfile isn't affiliated with them.*

Enso is the closest neighbour. It also treats the diagram and code as one
thing, using its own language. Flowfile uses Python and Polars, which many
data teams already work with.

The Alteryx importer reports what it couldn't reproduce for each tool.
The canvas notebook shows the open flow as Python cells and writes edits
back to it. On every commit, tests compare exported code with the canvas
result. The same requirement applies to new features: they need to work on
the canvas, in the Python API and in the export.

---

## 2. Where we are going

We want a flow to meet four requirements:

| | Requirement | What it means |
|---|---|---|
| 1 | **Your compute** | Data is processed on machines you choose and control. |
| 2 | **Open formats** | Other engines can read your tables without Flowfile. |
| 3 | **Your catalog** | You run the catalog. It passes credentials to the compute, so the flow doesn't need to hold them. |
| 4 | **Keep your work** | If you stop using Flowfile, your pipeline still runs. |

The first target is work that fits on one good machine. Many analytics
pipelines deal with gigabytes, and a single machine handles them fine. When
you do need a cluster, you should be able to use one without rebuilding the
flow.

These requirements also matter to teams moving to European or on-premises
infrastructure. Choosing a datacenter is part of data sovereignty; the tools
you use every day need to support that choice too.

### The target architecture

The idea is to let you choose the parts of the stack, and replace them when
you need to.

![The planned stack: the visual designer, Python API, canvas notebook and AI planner share one flow graph, with Polars export and Alteryx import. Execution options include a laptop, your own worker, Polars Cloud and sandboxed kernels. Tables use Delta internally and Iceberg for exchange. Catalog options include Flowfile metadata, Lakekeeper and other Iceberg REST catalogs, backed by your choice of object store.](docs/assets/images/roadmap/target-architecture.svg)

**Legend:** plain cards are shipped · `GAP` marks a known limitation in
something shipped · dimmed cards are planned.

The diagram marks two gaps. A few node types, including Google Analytics,
SQL query and API response, don't have code export yet. The worker has its
own URL and authentication, but returns results as a path on its local
disk. Core needs to read that file, so the two still need a shared
filesystem. Both need fixing.

### What 1.0 means

A flow built on a laptop must meet all four requirements without changes:

- It runs on a worker on another machine, with no shared disk.
- Its tables live in your bucket, and other engines can read them through a
  catalog you run.
- You sign in with your own identity provider. The catalog passes
  credentials to the compute; the flow doesn't store them.
- Its exported code produces the same DataFrame with Flowfile uninstalled.

The full stack must start with one Docker Compose command, with CI checking
that it all works together. That's the bar for 1.0.

---

## 3. What comes first

**Now:** let workers return results over the network, and implement the
Iceberg reader. Add a round-trip test that counts what gets lost in export,
a benchmark for the AI planner, and a Compose profile with PostgreSQL and
MinIO.

**Next, towards 1.0:** introduce a common execution interface with pluggable
backends, and try a Polars Cloud backend behind a feature flag. Connect
Iceberg to external catalogs, mirror the Flowfile catalog in Lakekeeper, and
let each catalog root choose its table format. Verify in CI that two Flowfile
instances can share one catalog. Add OIDC login and finish code export for
every node type.

**Later:** a stable Polars Cloud backend, Lakekeeper in delegate mode,
components shared through the catalog, and kernels running on a remote
Docker host. Also, a practical checklist showing which settings cover the
four requirements above.

---

## 4. The tracks

The work is split into six tracks. Each gets a Discussion in
[Announcements](https://github.com/edwardvaneechoud/Flowfile/discussions/categories/announcements)
when work starts. That's where we'll keep the detailed milestones, design
questions and acceptance criteria.

### Track 1 · Bring your own compute

Run a flow on your laptop, a VM in your own account or a managed Polars
cluster without changing it. The worker is already a separate service with
its own URL and authentication. The remaining dependency is its result
file: core opens it to read the schema and preview.

The first step is to return a limited sample, schema and row count over the
network, behind a feature flag. Core should no longer need access to the
worker's files. We'll verify this by running the end-to-end suite with core
and worker on separate hosts and no shared filesystem.

After that, we'll replace the scattered local-or-remote branches with one
execution interface and support backends as entry-point plugins. That makes
room for Polars Cloud, with no credentials in submitted plans, and kernels
running on a remote Docker host.

### Track 2 · Open formats and catalogs

Other engines should be able to read Flowfile's tables, and Flowfile should
work with the catalogs you already use. Delta stays the internal format. It
doesn't require a catalog, and our change data feed, merge, SCD2, row edits
and time travel support already use it.

We'll add Iceberg for reading and writing data across tools, starting with
Lakekeeper and then Polaris, Unity, Glue and S3 Tables through the Iceberg
REST API.

First comes an Iceberg reader that takes a metadata path. It will run in the
worker so credentials stay out of the plan. The writer follows once the
catalog connection is in place, since committing to Iceberg requires a
catalog operation.

The first check is to read an Iceberg table stored in MinIO and written by
another engine, using the node, Python API and exported code. Later, a Delta
table written by Flowfile should appear in Lakekeeper and be readable by
another engine, using credentials supplied by the catalog. That engine
shouldn't need storage keys in its config.

### Track 3 · One catalog, many instances

Separate Flowfile installs should be able to share a catalog, each with its
own compute. We have the parts: PostgreSQL metadata, data in your bucket,
and secrets that any instance with the master key can re-derive. We still
need to prove they work together safely.

We'll start with a test: two cores, one PostgreSQL catalog, one MinIO bucket
and a worker for each core. Instance B must read what instance A wrote.
Cloud-backed reads must never replay a plan containing another instance's
credentials.

Once that runs in CI, the next step is OIDC login in server mode, passing
the user's identity to external catalogs. Then custom nodes, flow
references and connections can move from per-instance files into the shared
catalog.

### Track 4 · The round-trip

Moving between the canvas, Python API and exported Polars script should
preserve the flow. Today, information can be lost in two places: nodes
without an export handler, and expressions the Python API can't turn back
into source code. Those expressions are stored as opaque blobs.

We'll add a round-trip test over a fixed set of flows and a CI check that
counts both cases. The target is zero for both, with every node type in the
palette supported by code export.

### Track 5 · Measuring the AI planner

The planner already shows changes as a diff for review. It pauses if you
edit the canvas while it's working, and applies or rejects each set of
changes in full. We still need a reliable measure of how often it gets the
job right.

We'll start with a held-out benchmark using recorded provider responses,
measuring success rate and retries. We'll set the pass thresholds in
advance, and the planner must meet them without a person correcting tool
names. We'll also track refusals and use the benchmark to check fixes where
the planner hands work to the executor.

### Track 6 · The reference deployment

One command should start the full stack on your own machines: PostgreSQL
for metadata, MinIO for tables, Lakekeeper as the catalog, plus core, worker
and kernels. This gives people a working setup to try the whole roadmap
end to end.

First, we'll add the `sovereign` Docker Compose profile.

We'll check the setup with a documented walkthrough: install it, write a
table, read that table from another engine, run a flow on a second worker,
then export the flow and run it with Flowfile uninstalled. A new contributor
should be able to do this on a clean machine in under an hour. Every step
must also run in CI.

---

## 5. What Flowfile builds and connects to

Flowfile handles part of a data platform. For the rest, we'll integrate with
existing tools.

![Flowfile builds the shared flow graph, canvas, Python API, code export, nodes, catalog metadata, execution interface, worker and importers. It integrates with compute clusters, external catalogs, identity providers, orchestrators, storage, model providers, warehouses and BI tools.](docs/assets/images/roadmap/we-build-we-support.svg)

| Layer | What we do | What we leave to other tools |
|---|---|---|
| **Compute at scale** | Pluggable backends, your workers, and Polars Cloud as the reference scale-out backend | Cluster management and distributed engines |
| **Catalog servers** | Connect to Iceberg REST catalogs and mirror our tables into them | Building an Iceberg REST server |
| **Identity** | OIDC login and token pass-through | Providing an identity service |
| **Orchestration** | A small embedded scheduler, plus exports and a CLI for use with any orchestrator | Full orchestration, as in Airflow or Dagster |
| **Storage** | Read and write S3-compatible stores, ADLS, GCS and local disk | Operating storage |
| **Language models** | Use your own key and provider, or a local model | Training and hosting models |
| **Analytics and BI** | Previews at every node and lightweight exploration dashboards | A full BI product |
| **Warehouses** | Read and write through database nodes | Building a warehouse |

---

## 6. Technology choices

What we use, what we're looking at, and what we've dropped.

| Tool | Role in Flowfile | Status | Why |
|---|---|---|---|
| **Polars** | Data engine | Keeping | One pinned version across platforms; plugins and the kernel image update with it |
| **delta-rs** | Catalog table storage | Keeping | No catalog required; supports CDC, merge, SCD2, row edits and time travel |
| **Apache Iceberg** (pyiceberg) | Reading and writing tables across tools | Adopting | Already a dependency; the reader is the first step in Track 2 |
| **Lakekeeper** | Reference Iceberg catalog | Adopting | Apache 2.0, Rust, credential vending and OIDC; also registers non-Iceberg tables |
| **Polars Cloud** | Reference scale-out backend | Evaluating | Same API, compute in your account or on-prem, European vendor. They run the control plane, so it will remain one of several backend options |
| **PostgreSQL** | Catalog metadata | Shipped | Available alongside SQLite; CI runs the migrations against both |
| **OIDC** (any provider) | Server-mode identity | Adopting | Needed for per-user authorization in external catalogs |
| **Docker kernels** | Sandboxed Python execution | Shipped | Support for a remote Docker host is planned |
| **Tauri** | Desktop shell | Migrated in May 2026 | Replaced Electron for a smaller, faster app with signed sidecars |
| **Alteryx `.yxmd` import** | Importing existing visual workflows | Shipped | Reports unsupported behaviour per tool instead of silently approximating it |
| **Airbyte connector** | Cloud ingestion | Retired in July 2025 | Replaced by native cloud-storage reads |
| **In-house fuzzy matcher** | Fuzzy join | Extracted in August 2025 | Now an external package imported by the worker |
| **DuckDB** | Second SQL engine | Not planned | A frequent request, but Polars SQL covers the SQL nodes. Keeping one engine makes it easier to keep canvas, API and export results consistent |
| **Spark** | Distributed compute | Not planned | We'll add scale through execution backends |

---

## 7. Out of scope

Some limits are worth spelling out:

- **A distributed engine or cluster scheduler.** We'll connect to existing
  ones through backends.
- **An Iceberg REST catalog server.** Flowfile will use Lakekeeper, Polaris
  and the cloud providers' implementations.
- **Replacing Delta internally.** Iceberg support is for working with data
  across tools.
- **A second DataFrame engine.** We'll keep one engine so the canvas, API
  and export stay consistent.
- **A custom metadata store.** We'll stick with SQL and standard migrations.
- **A BI product.** Previews and lightweight exploration are enough for
  Flowfile's role.
- **An identity provider.** We'll support existing OIDC providers.
- **Certification work.** Frameworks can refer to the reference deployment,
  but certification processes won't drive the project.
- **A hosted multi-tenant service.** This roadmap is for software you run
  where you choose.

---

## 8. Open source commitment

Flowfile is MIT-licensed. Everything on this roadmap will ship under MIT in
this repository: the backend interface, local and remote workers, Iceberg
and Lakekeeper support, OIDC, the reference deployment, round-trip fixes and
AI work.

The rule for anything we might charge for is simple: **could one person
working on one machine need it?** If yes, it belongs here, under MIT.

Only features needed because an organization is large, such as audit,
policy or support contracts, could sit outside this repository. Those would
be separate packages using the public interfaces described here, without
requiring changes to the open code path. Any proposed change to that rule
would go through a public Discussion first.

There is no contributor license agreement, and we don't plan to add one.
Your contributions stay under MIT; we won't relicense them later.

---

## 9. Get involved

- **Pick a track.** Once work starts, its Discussion in
  [Announcements](https://github.com/edwardvaneechoud/Flowfile/discussions/categories/announcements)
  will cover the design, open questions and what each milestone needs to
  deliver. For anything larger than a bug fix, comment there before writing
  code.
- **Suggest a change.** Open a Discussion titled `RFC: <topic>`. Describe the
  problem, how it affects the four requirements, the smallest useful first
  step and how we'd test whether it solves the problem.
- **Start small.** Have a look at
  [`good first issue`](https://github.com/edwardvaneechoud/Flowfile/issues?q=is%3Aopen+label%3A%22good+first+issue%22)
  issues or the two gaps marked in the architecture diagram.
- **Read [CONTRIBUTING.md](https://github.com/edwardvaneechoud/Flowfile/blob/main/CONTRIBUTING.md)**
  for how we test, review and release changes, why we favour integration
  tests over mocks, and the checks that keep the canvas, API and export in
  sync.

Suggestions for this file are welcome as pull requests too. The Git history
keeps track of what changed.
