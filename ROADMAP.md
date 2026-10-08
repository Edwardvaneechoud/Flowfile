# Flowfile Roadmap

### One graph. Two doors. Your environment.

**Flowfile 0.22.1 · updated 2026-10-08 · [discuss this roadmap](https://github.com/edwardvaneechoud/Flowfile/discussions/categories/announcements)**

Flowfile is a tool for analytics and exploratory ETL: work that starts as a
question about some data, turns into a pipeline, and tends to end up tied to
the place it was built. This roadmap is about making that work portable. A
flow built on a laptop should run on a machine you choose, keep its tables in
a format other engines can read, use a catalog you operate, and still work as
plain Python if you stop using Flowfile.

Flowfile already does part of this. The analyst's canvas and the engineer's
Python edit the same graph. Tables are stored as Delta, on disk or in your
own bucket. Compute runs in a worker you can place where you like, and every
flow exports to a Polars script. The rest of this document is about closing
the distance between that and the full picture.

It is a direction, not a delivery schedule: no dates, no version numbers next
to features. Each track names the step it starts with and the result you will
be able to check, and the design of each step is open to argument in a
public Discussion before its code lands.

---

## 1. Where Flowfile sits

Most data tools are built around one way of working, and do it well. Visual
tools such as Alteryx, KNIME and Power Query put the canvas first and offer a
script node for the cases the palette does not cover. Code tools such as dbt,
Airflow and Dagster put code first and draw a graph so you can see what you
wrote. Each is a sensible choice, and a lot of good work gets done in both.

Flowfile tries to serve both ways of working on one graph. The analyst on the
canvas and the engineer in Python edit **the same object**, not a translation
or an export of it:

![The visual designer on the left and the Python API on the right both edit one shared flow graph in the middle, and that graph exports to a plain Polars script that runs with no Flowfile installed.](docs/assets/images/roadmap/one-graph-three-renderings.svg)

A flow built by dragging nodes opens in Python with the same node ids. A flow
written in Python opens on the canvas as editable nodes. Either one exports
to a Polars script that runs without Flowfile installed.

Where that puts Flowfile among the tools people use for this kind of work:

![A two-by-two chart: visual-first tools such as Alteryx, KNIME and Power Query sit top-left; Excel and one-off scripts sit bottom-left; code-first tools such as dbt, Airflow, Dagster, Databricks and plain Polars sit bottom-right; Enso and Flowfile sit top-right, where the canvas and the code are both main surfaces on one graph, with Flowfile furthest into that corner.](docs/assets/images/roadmap/where-flowfile-sits.svg)

*Placements are our reading of each tool's design priorities, not a feature
scorecard. Product names are trademarks of their owners; Flowfile is not
affiliated with any of them.*

Enso is the closest neighbour: it also treats the diagram and the code as one
thing, and does it with a language of its own. Flowfile does it with Python
and Polars, so what you build runs in the stack most data teams already have.

This is why the Alteryx importer reports, per tool, what it could not
reproduce instead of guessing, why the canvas notebook renders the open flow
as Python cells and pushes edits back, and why the code export is tested for
equality against the canvas result on every commit. It is also the rule for
everything below: a feature is done when it exists on the canvas, in the
Python API and in the export.

---

## 2. Where we are going

Portable needs a concrete meaning. We check it with four questions:

| | Question | What it asks |
|---|---|---|
| 1 | **Own compute** | Is the data processed where you choose, on machines you control? |
| 2 | **Open formats** | Can another engine read your tables without Flowfile? |
| 3 | **Own catalog** | Is the catalog a service you run, and does it hand credentials to the compute so the flow never holds them? |
| 4 | **Exit without rewrite** | If you stop using Flowfile, does the pipeline still run? |

The aim is a yes to all four for the work that fits on one good machine, and
a path to a cluster when you need one without changing the flow. Most
analytics pipelines handle gigabytes, not terabytes, and one machine is often
the right size for them.

Many teams are asking these same questions under the heading of data
sovereignty, as workloads move to European or on-premises infrastructure. The
questions are the same whatever you call them, and they are decided by the
tools a team uses day to day more than by the datacenter.

### The target architecture

Every box in this picture can be swapped for something else.

![Six layers from top to bottom: you author in the visual designer, the Python API, the canvas notebook or the AI planner; everything meets in one flow graph that exports to plain Polars and imports Alteryx workflows; it runs on your laptop, your own worker, Polars Cloud or sandboxed kernels; tables are stored as Delta inside the catalog or Iceberg at the edges; the catalog is Flowfile's own metadata, Lakekeeper, or any Iceberg REST catalog such as Unity, Polaris, Glue or S3 Tables; and it all sits on any object store. Plain cards are shipped, a GAP tag marks a known gap, dimmed cards are planned.](docs/assets/images/roadmap/target-architecture.svg)

**Legend:** plain cards are shipped · a `GAP` tag marks something shipped
with a known gap · dimmed cards are on this roadmap.

Two gaps are marked. The flow graph exports almost every node, but a few
types (Google Analytics, SQL query, API response) have no code export yet.
The worker runs as a separate service with its own URL and auth, but it hands
results back as a path on its own disk, so today it has to share a filesystem
with core. Both are first on the road.

### What 1.0 means

Flowfile calls itself 1.0 when a flow built on a laptop answers yes to all
four questions without being changed: it runs on a worker on another machine
with no shared disk; its tables sit in the owner's bucket and other engines
can read them through a catalog the owner runs; the owner signs in with their
own identity provider, and credentials reach the compute through the catalog
rather than through the flow; and its code export runs with Flowfile
uninstalled and gives the same frame. The whole stack comes up from one
compose command, and CI proves it.

---

## 3. The road

Three horizons, no dates.

**Now.** The two gaps in the architecture picture close first: worker results
travel over the wire instead of as a path, and the Iceberg reader stops being
a stub. Alongside them, the round-trip gets a test that counts lossy exports,
the AI planner gets a benchmark, and the compose file gets a profile with
PostgreSQL and MinIO.

**Next, the road to 1.0.** Execution becomes a seam with pluggable backends,
with a Polars Cloud spike behind a flag. Iceberg gets a catalog connection,
Lakekeeper mirrors the Flowfile catalog, and a catalog root can choose its
table format. Two Flowfile instances share one catalog, proven in CI. OIDC
login. Every node type exports.

**Later.** A stable Polars Cloud backend, Lakekeeper in delegate mode,
components shared through the catalog instead of per-instance files, kernels
on a remote Docker host, and a checklist mapping the four questions to
settings.

---

## 4. The tracks

Six tracks carry the road. A track gets a Discussion thread in
[Announcements](https://github.com/edwardvaneechoud/Flowfile/discussions/categories/announcements)
when work on it starts, holding its milestones with their design, their
open questions and the concrete result that says each one landed. Here,
each track keeps only its intent, where it starts, and how you will know.

### Track 1 · Bring your own compute

Run the compute wherever the data owner chooses, from a laptop to a VM in
their own account to a managed Polars cluster, without changing the flow. The
worker is already a separate service with its own URL and auth; what keeps it
on the same machine as core is that it hands results back as a path on its
own disk, and core reads that file for the schema and the preview.

*Where it starts:* the worker returns a bounded sample, the schema and the
row count over the wire, behind a flag, so core never opens the worker's
files.

*How you will know:* core and worker run on two hosts with no shared
filesystem and the end-to-end suite is green. After that: one execution
interface in place of the inline local-or-remote branches, backends as
entry-point plugins, a Polars Cloud backend whose submitted plans carry no
credentials, and kernels on a remote Docker host.

### Track 2 · Open formats and catalogs

Flowfile tables are readable by any engine, and Flowfile reads and writes the
catalogs the ecosystem is standardizing on. Delta stays the internal format
because it needs no catalog and because change data feed, merge, SCD2, row
edits and time travel are built on it. Iceberg is added at the edges, and
Flowfile works with the Iceberg REST catalogs you already run: Lakekeeper
first, then Polaris, Unity, Glue and S3 Tables.

*Where it starts:* the Iceberg reader, by metadata path, built in the worker
so credentials never enter a plan. The writer follows with the catalog
connection, because an Iceberg commit is a catalog operation.

*How you will know:* an Iceberg table written by another engine reads in
Flowfile from MinIO, through the node, the Python API and the export. Later,
a Delta table Flowfile wrote shows up in Lakekeeper, and another engine reads
it with credentials the catalog vended, with no storage keys in that engine's
config.

### Track 3 · One catalog, many instances

Independent Flowfile installs, each with its own compute, share one catalog
safely. The pieces exist: metadata on PostgreSQL, data in the owner's bucket,
secrets any master-key holder can re-derive. The proof does not.

*Where it starts:* a test. Two cores on one PostgreSQL catalog and one MinIO
bucket, each with its own worker; instance B reads what instance A wrote, and
no cloud-backed read is ever served from a replayed plan carrying another
instance's credentials.

*How you will know:* that test runs in CI. Then OIDC login in server mode,
with the user's own identity reaching external catalogs; then custom nodes,
flow references and connections shared through the catalog instead of
per-instance files.

### Track 4 · The round-trip

The canvas, the exported Polars script and the Python API are three
renderings of one flow, with nothing lost between them. Today the trip is
lossy in two places: a node type with no export handler, and an expression
the Python API cannot turn back into source, which it stores as an opaque
blob.

*Where it starts:* a round-trip test over a fixed flow corpus, and a gate that
counts both.

*How you will know:* both counters read zero and every node type in the
palette exports.

### Track 5 · AI that edits pipelines, measured

The planner already stages every change as a reviewable diff, pauses when
you edit the canvas underneath it, and applies or rejects atomically. What is
missing is a number for how often it is right.

*Where it starts:* a held-out benchmark against a recorded provider that
reports success rate and retries.

*How you will know:* pre-registered thresholds cleared with zero human
tool-name corrections, with refusal statistics and executor-seam fixes
measured against the benchmark along the way.

### Track 6 · The reference deployment

One command stands up the whole stack on machines you own: PostgreSQL for
metadata, MinIO for tables and Lakekeeper as the catalog, next to core,
worker and kernels. It is the setup people usually mean by a sovereign
deployment, and the quickest way to try everything on this roadmap end to
end.

*Where it starts:* `docker compose --profile sovereign`.

*How you will know:* a new contributor completes the documented walk-through
(install, write a table, read it from another engine, run a flow on a second
worker, export the flow and run it with Flowfile uninstalled) from a clean
machine in under an hour, and every step is a CI job.

---

## 5. The boundary

Flowfile is one layer of a data platform. These are the layers it does not
build and integrates with instead. We will not compete with them, and we will
make them easy to plug in.

![Two columns: Flowfile builds the flow graph and its three renderings, the node palette with custom and community nodes, catalog metadata with lineage, sharing and change tracking, the execution backends interface and the worker, and importers from other tools; it supports and never replaces compute clusters, Iceberg catalogs, identity providers, orchestrators, object storage, LLM providers, and warehouses and BI tools.](docs/assets/images/roadmap/we-build-we-support.svg)

| Layer | What we do | What we will not do |
|---|---|---|
| **Compute at scale** | Pluggable backends; your workers; Polars Cloud as the reference scale-out backend | Build a cluster manager or a distributed engine |
| **Catalog servers** | Be a client of Iceberg REST catalogs; mirror our tables into them | Ship our own Iceberg REST server |
| **Identity** | OIDC login and token pass-through | Run an identity provider |
| **Orchestration** | A small embedded scheduler; exports and a CLI that run under any orchestrator | Compete with Airflow or Dagster on orchestration |
| **Storage** | Read and write any S3-compatible store, ADLS, GCS, local disk | Operate storage |
| **Language models** | Bring your own key, any provider, or a local model | Train or host models |
| **Analytics and BI** | Previews at every node, lightweight exploration dashboards | Become a BI product |
| **Warehouses** | Read from and write to them through database nodes | Be a warehouse |

---

## 6. Tools: adopting, evaluating, keeping, retired

Which tools Flowfile builds on, which it is evaluating, and which it has set
aside, with the reasons.

| Tool | Role in Flowfile | Status | Why |
|---|---|---|---|
| **Polars** | The engine, everywhere | Keeping | One cross-platform pin; plugins and the kernel image move with it |
| **delta-rs** | Catalog table storage | Keeping | Needs no catalog; carries CDC, merge, SCD2, row edits, time travel |
| **Apache Iceberg** (pyiceberg) | Interop format at the edges | Adopting | Already a dependency; the reader is the first step of Track 2 |
| **Lakekeeper** | Reference Iceberg catalog | Adopting | Apache 2.0, Rust, vended credentials, OIDC; registers non-Iceberg tables too |
| **Polars Cloud** | Reference scale-out backend | Evaluating | Same API, compute in your account or on-prem; European vendor; control plane is theirs, so never the only backend |
| **PostgreSQL** | Catalog metadata | Shipped | Alongside SQLite; the migration chain runs against both in CI |
| **OIDC** (any provider) | Identity in server mode | Adopting | Needed for per-user authorization in external catalogs |
| **Docker kernels** | Sandboxed Python execution | Shipped | Remote Docker host planned |
| **Tauri** | Desktop shell | Migrated to, 2026-05 | Replaced Electron: smaller, faster, signed sidecars |
| **Alteryx `.yxmd` import** | Migration path for visual users | Shipped | Per-tool report; nothing is silently approximated |
| **Airbyte connector** | Cloud ingestion | Retired, 2025-07 | Replaced by native cloud-storage reads |
| **In-house fuzzy matcher** | Fuzzy join | Extracted, 2025-08 | Lives on as an external package the worker imports |
| **DuckDB** | Second SQL engine | Not planned | Asked often; Polars SQL covers the SQL nodes and one engine keeps parity possible |
| **Spark** | Distributed compute | Not planned | Scale comes through backends, not a second engine |

---

## 7. Out of scope

Things we considered and set aside.

- **Our own distributed engine or cluster scheduler.** Backends, not clusters.
- **Our own Iceberg REST catalog server.** Lakekeeper, Polaris and the cloud
  providers ship the spec; Flowfile is a client of them.
- **Replacing Delta as the internal table format.** Iceberg is an edge.
- **A second DataFrame engine.** Parity between canvas, API and export is only
  possible with one.
- **A bespoke metadata store.** Standard SQL, standard migrations.
- **A BI product.** Previews and exploration, yes. Dashboards as a business,
  no.
- **An identity provider.** Any OIDC provider, never ours.
- **Certification work.** Frameworks are welcome to point at the reference
  deployment; the project will not organize itself around certification
  processes.
- **A hosted multi-tenant service as part of this roadmap.** Everything here
  runs where you put it.

---

## 8. Open source commitment

Flowfile is MIT-licensed and every item on this roadmap ships under that
license in this repository: the execution backend interface, the local and
remote worker backends, the Iceberg and Lakekeeper integrations, OIDC, the
reference deployment, the round-trip work and the AI work.

One rule decides what could ever be paid: **would a single person on one
machine ever need it?** If yes, it is MIT and lives here. Only capabilities
that exist purely because an organization is large (audit, policy, support
contracts) could ever sit outside, and they would sit on the public
interfaces this roadmap defines, as separately installed packages that never
require a change to the open code path. If that line ever needs to move, it
will be proposed in a public Discussion first, not discovered in a release.

There is no contributor license agreement and none is planned. Your
contributions stay MIT, which also means they cannot be relicensed later.

---

## 9. Join

- **Pick a track.** Tracks with work under way have a Discussion in
  [Announcements](https://github.com/edwardvaneechoud/Flowfile/discussions/categories/announcements)
  with the design, the open questions and the result that says each
  milestone landed; the other tracks get theirs the day work on them starts.
  Comment there before writing code for anything larger than a bug fix.
- **Change the roadmap.** Open a Discussion titled `RFC: <topic>`. An RFC
  needs a problem statement, its effect on the four questions, the smallest
  shippable first step and a result that can prove it wrong.
- **Start small.** Issues labeled
  [`good first issue`](https://github.com/edwardvaneechoud/Flowfile/issues?q=is%3Aopen+label%3A%22good+first+issue%22)
  and the two gaps in the architecture picture are the fastest way in.
- **How we work** is in [CONTRIBUTING.md](https://github.com/edwardvaneechoud/Flowfile/blob/main/CONTRIBUTING.md): real
  integration tests over mocks, the drift gates, how a change is reviewed and
  released.

This file changes by pull request like everything else. Its history is its
changelog.
