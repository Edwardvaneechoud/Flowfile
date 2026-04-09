# I Built a Visual ETL Tool. It Accidentally Became a Data Platform.

Flowfile started as a visual pipeline builder. Drag a CSV onto a canvas, wire up some transforms, export Polars code. That was the whole idea.

Somewhere between adding a catalog, a scheduler, Delta Lake storage, and a Kafka consumer, it became something else. Not because I sat down and designed a "platform." Because each feature made the next one obvious, and the canvas turned out to be flexible enough to hold all of it.

The canvas is still the point. And the canvas is still simple enough that you don't need to understand any of what I'm about to describe. You can use Flowfile without ever touching a CLI, writing a parameter, or reading a YAML file. Drag, connect, run. Everything below is optional depth — layers that exist when you need them, invisible when you don't.

## Everything is just nodes

Other platforms say everything is compute. In Flowfile, everything is a node — which, fine, is also compute, but compute you can see.

Here's a Kafka-to-Delta-Lake ingestion pipeline in Flowfile: two nodes on a canvas. A `kafka_source` node connected to a `catalog_writer` node. The `kafka_source` reads from a topic with schema inference — it samples ten messages and figures out the fields. The `catalog_writer` writes to a Delta Lake table with a write mode you pick: overwrite, append, upsert, whatever you need.

Behind the scenes, the `create_kafka_sync` endpoint generates this flow automatically. It infers the schema, resets the consumer group to the beginning so the first run picks up all historical data, and saves the whole thing as a YAML file. Two node definitions with settings. Add a schedule and you have a streaming ingestion pipeline.

To build the same thing without Flowfile, you'd wire together Kafka Connect, some orchestrator like Airflow, and a Delta writer. Those are excellent tools. But the setup cost for a two-node pipeline is real. You're writing connector configs, DAG definitions, deployment manifests. Flowfile compresses that into something you can see on a screen.

The YAML is almost boring. Two nodes, their settings, a connection between them. That's the right kind of boring.

## Scheduling shouldn't require a separate tool

The scheduler exists as its own module — `flowfile_scheduler` — with no dependency on `flowfile_core`. It's SQLAlchemy and shared models, nothing else. It runs embedded inside the core service, standalone, or in Docker. Same code, different deployment.

Three schedule types:

- **Interval**: run every N seconds. Simple.
- **Table trigger**: fire when a specific catalog table gets updated. This uses a dual-path approach — the catalog pushes a trigger on overwrite, and the scheduler polls every 30 seconds as a safety net. Belt and suspenders.
- **Table set trigger**: fire when *all* tables in a set have been updated. For pipelines that need multiple upstream sources to be fresh before they run.

Only one scheduler instance runs at a time. It uses an advisory lock with a heartbeat — each instance writes its heartbeat every poll cycle, and if a holder goes silent for 90 seconds, another instance takes over. No ZooKeeper, no Redis, no external coordination. Just a database row.

Before launching a flow, the scheduler checks for an active run. No double-fires. The flow gets a `FlowRun` record marked as `run_type="scheduled"` before the subprocess even starts.

It turns out that scheduling is mostly bookkeeping. The hard part is making the bookkeeping reliable.

## Delta Lake happened because Parquet wasn't enough

The catalog started as a Parquet file organizer. You'd write dataframes to named tables, read them back later. It worked fine until it didn't.

The problems arrived in a predictable order: someone overwrites a table mid-read, someone wants to undo a bad write, someone needs to upsert instead of replace. Parquet files don't do transactions. They don't do time travel. They definitely don't do merge operations.

Delta Lake solved all of this. The catalog writer now supports five write modes: overwrite, append, upsert (merge on key columns), update, and delete. Upsert is the one people actually wanted — match on a set of key columns, update existing rows, insert new ones. One node setting.

Time travel came almost for free. The UI shows version history for every table, and you can read any previous version. Made a bad write? Roll back. Need to compare today's data with last week's? Read version N.

The migration was straightforward — a utility converts existing Parquet catalog storage to Delta format. Everything is still local-first, backed by `delta-rs`. No cloud service required.

## Parameters make flows reusable

A flow shouldn't be a single-use artifact. Parameters solve this with `${}` substitution, resolved at runtime across the entire flow's Pydantic models.

You can set parameters in the UI — there's a panel for it, no CLI required. But if you want to automate, it works from the command line too:

```bash
flowfile run flow etl_pipeline.json \
  --param input_dir=/data/2025/q1 \
  --param output_table=quarterly_summary
```

One flow, multiple environments. Dev reads from a sample directory, staging from last month's data, production from the live feed. Same YAML, different parameters.

Parameters that don't already exist in the flow get created on the fly. It's not a templating engine — it's just string substitution in the right places — but it's enough to make flows genuinely reusable without copying them.

## The code is always yours

This is the trust story. Flowfile generates pure Polars code — `import polars as pl`, nothing else. Zero Flowfile dependency in the output.

The `FlowGraphToPolarsConverter` walks the DAG in execution order, generates a variable assignment for each node (`df_1 = pl.read_csv(...)`, `df_2 = df_1.filter(...)`, and so on), wraps it in a `run_etl_pipeline()` function, and writes it out. The generated code uses `LazyFrame` throughout. You can take that file and run it anywhere Polars runs.

What makes this work is `_ff_repr` — a string representation tracked on every expression. When you write `ff.col("revenue").sum()`, the expression carries both the Polars operation and its Flowfile formula equivalent (`sum([revenue])`). This enables round-trip conversion: code to visual, visual to code, and back again.

```python
import flowfile as ff
from flowfile import open_graph_in_editor

pipeline = (
    ff.from_dict({
        "region": ["US", "EU", "US", "EU"],
        "revenue": [100, 200, 150, 300]
    })
    .filter(ff.col("revenue") > 120)
    .group_by("region")
    .agg([ff.col("revenue").sum().alias("total_revenue")])
)

# Opens the pipeline in the visual editor
open_graph_in_editor(pipeline.flow_graph)
```

That last line starts the Flowfile server, imports the flow, and opens it in your browser. You see nodes on a canvas representing the exact pipeline you just wrote in code. Edit it visually, or don't. The point is you're never locked in.

## The honest limits

Flowfile runs on a single machine. It's not petabyte-scale. It's not competing with Spark or Snowflake.

The ceiling is whatever fits in memory. Polars handles streaming and out-of-core processing well, but there are real limits. If your dataset is too large for one machine, you need a distributed system, and Flowfile isn't that.

There's no distributed compute, no cluster mode, no horizontal scaling. If you need a cluster, use a cluster. Flowfile is for the vast number of data pipelines that don't need one.

## Nothing is bolted on

Looking back, the thing I didn't expect is the coherence.

Each feature pulled the next one into existence. The catalog needed Delta Lake. Delta Lake needed scheduling to stay fresh. Scheduling needed parameters to stay flexible. Parameters needed code generation to stay portable. But none of these became separate subsystems. Kafka isn't a streaming module with its own UI — it's two nodes on the same canvas. Scheduling isn't a separate orchestration layer — it's tied to catalog tables. Delta Lake isn't a storage backend you configure in a YAML file — it's just how the catalog works now.

Every feature serves the canvas. That wasn't a design principle I wrote down. It's just what happened when I kept asking "where does this belong?" and the answer kept being "on the graph."

The result is a tool that installs with `pip install flowfile` and does things that normally require three products and a week of configuration. The multi-service architecture — core, worker, frontend — is invisible. One install, one command. And for most users, the experience is still: drag, connect, run.

## The tool is ahead of its story

I'll be honest about one thing. Someone glancing at the GitHub might still think "oh, a visual CSV cleaner." They'd be completely wrong, but I can't blame them. I've been building faster than I've been writing about what I built. Three major releases worth of features and almost no narrative around them.

That's what this post is — catching the story up to the tool.

The question isn't whether the product is ready. It's whether enough people know what it's become.

```bash
pip install flowfile
```

[GitHub](https://github.com/FlowFile/FlowFile) | [Documentation](https://docs.flowfile.io) | [Demo](https://demo.flowfile.io)
