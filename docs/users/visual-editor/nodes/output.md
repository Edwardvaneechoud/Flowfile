# Output Operations

Output actions are where a flow's work lands: a file, a database table, cloud storage, a catalog table, an API response — or a chart you look at on screen. Most take an input and produce no output, so they sit at the end of a branch.

| Action | What it does | Lite |
|---|---|:--:|
| [Write data](#write-data) | Save to a local CSV, Excel, Parquet, Arrow, NDJSON or Avro file | ● |
| [Write to Database](#database-writer) | Append to, replace, or create a database table | |
| [Write to cloud provider](#cloud-storage-writer) | Write to S3, Azure Data Lake or Google Cloud Storage | |
| [Write to Catalog](#catalog-writer) | Register the result as a catalog table others can query | ● |
| [Explore data](#explore-data) | Build charts interactively on the node's input | ● |
| [Flow Output](#flow-output) | Named exit point when this flow runs inside another | |
| [API response](#api-response) | Return this dataset as the body of an HTTP endpoint | |

!!! info "In Flowfile Lite"
    In the browser-only [Flowfile Lite](../../deployment/lite.md) build, **Write data** downloads a file to your browser (CSV, Parquet or Excel — not the full desktop format set), and **Write to Catalog** and **Explore data** work as usual. Database and cloud writers need a backend, so they are not available.

## ![Write data](../../../assets/images/nodes/output.svg){ width="44" height="44" } Write data

Saves the incoming table to a local file.

**Settings**

| Setting | Description |
|---|---|
| **File Path** | Directory and filename for the output. |
| **File Format** | CSV, Excel, Parquet, IPC, NDJSON or Avro. |
| **Write Mode** | `overwrite` replaces the file; `new file` fails if one already exists. CSV also offers `append`. |

### Format options

| Format | Extension | Compression | Write modes |
|---|---|---|---|
| CSV | `.csv` | — (set **Delimiter** and **Encoding** instead) | overwrite, new file, append |
| Excel | `.xlsx` | — (set **Sheet Name**, default `Sheet1`) | overwrite, new file |
| Parquet | `.parquet` | `zstd` (default), `snappy`, `gzip`, `lz4`, `brotli`, `uncompressed` | overwrite, new file |
| Arrow IPC / Feather | `.arrow` | `uncompressed` (default), `lz4`, `zstd` | overwrite, new file |
| NDJSON | `.ndjson` | `uncompressed` (default), `gzip`, `zstd` | overwrite, new file |
| Avro | `.avro` | `uncompressed` (default), `snappy`, `deflate` | overwrite, new file |

CSV defaults to a `,` delimiter and `UTF-8` encoding.

## ![Write to Database](../../../assets/images/nodes/database_writer.svg){ width="44" height="44" } Write to Database { #database-writer }

<div class="ff-split" markdown>

<div markdown>
Writes the incoming table to a database table.

[Connect to PostgreSQL](../tutorials/database-connectivity.md) is a step-by-step walkthrough.
</div>

![Database Writer settings](../../../assets/images/guides/nodes/database-writer-settings.png)

</div>

**Connection modes**

| Mode | Description |
|---|---|
| **Reference** | Use a saved connection from the [Connection Manager](../connections.md). Recommended. |
| **Inline** | Enter credentials directly in the node settings. |

**Settings**

| Setting | Description |
|---|---|
| **Schema** | Target schema, e.g. `public`. |
| **Table** | Target table name. |
| **Write Mode** | **Append** adds rows to the existing table, **Replace** drops and recreates it with the new data, **Fail** errors if the table already exists. |

## ![Write to cloud provider](../../../assets/images/nodes/cloud_storage_writer.svg){ width="44" height="44" } Write to cloud provider { #cloud-storage-writer }

<div class="ff-split" markdown>

<div markdown>
Writes directly to cloud object storage: AWS S3 (including S3-compatible services like MinIO), Azure Data Lake Storage, and Google Cloud Storage.

Authenticate with a [saved cloud connection](../tutorials/cloud-connections.md), or with **No connection** to use the credentials of the machine running Flowfile. **No connection** ignores saved endpoints and is unavailable on a multi-user server; see [Running a node without a connection](../tutorials/cloud-connections.md#no-connection).
</div>

![Screenshot of the Cloud Storage Writer configuration](../../../assets/images/ui/screenshot_cloud_writer_output.png)

</div>

**Settings**

| Setting | Description |
|---|---|
| **File Path** | Full URI including scheme, bucket or container, and file name, e.g. `s3://bucket/folder/output.parquet`. A Delta table is a folder, so its path names the table, e.g. `s3://bucket/warehouse/orders`. **Browse** picks a folder and names the file. The drawer warns while the path is empty or has no scheme, and such a node fails before writing anything. |
| **File Format** | CSV, Parquet, JSON or Delta Lake. |
| **Write Mode** | CSV, Parquet and JSON always overwrite. Delta Lake offers the modes in the next table. |

CSV adds **Delimiter** (default `,`) and **Encoding** (UTF-8 or UTF-8 Lossy). Parquet adds **Compression**: Snappy (default), Gzip, Brotli, LZ4 or Zstd.

!!! note "Parquet defaults differ between the two writers"
    This node defaults Parquet to **Snappy**, while local [Write data](#write-data) defaults to **Zstd**. Set the codec explicitly if the two paths need to match.

!!! warning "Overwrite replaces the target"
    In `overwrite` mode any existing file or data at the path is replaced. Verify the path before running.

### Delta Lake write modes

| Mode | Description |
|---|---|
| **Overwrite** | Replace all existing data in the table. |
| **Error if exists** | Fail if the table already exists. |
| **Append** | Add rows to the existing table. Without **Track changes**, the incoming columns must match the table's. |
| **Upsert** | Insert new rows, update existing ones matching the key columns. |
| **Update** | Update only existing rows matching the key columns; no inserts. |
| **Delete** | Remove target rows matching the key columns in the source. |

**Key columns** are required for Upsert, Update and Delete — they are the columns rows are matched on. Upsert and Update first add any source column the table does not have yet.

**Partition by** is optional and offered for Overwrite, Error if exists and Append. It sets the partition columns when the write creates the table; later writes must match that partitioning.

**Track changes** turns [change tracking](../catalog/change-tracking.md) on for the table, so a [Read from cloud provider](input.md#cloud-storage-reader) node can read only the rows each later write inserted, updated or deleted. As on the [catalog writer](#catalog-writer), it is enable-only: on an existing untracked table it is switched on just before this write, so this write is the first one tracked, and clearing it never turns tracking back off. Unavailable with Overwrite.

When the path points at an existing Delta table, the Delta options show its current version and partition columns; otherwise they note that the write creates a new table.

!!! note "Delta on Google Cloud Storage"
    Upsert, Update, Delete and Track changes are not available for `gs://` paths: the drawer disables them, and a run that asks for them fails before writing. They work on S3 and Azure Data Lake Storage.

## ![Write to Catalog](../../../assets/images/nodes/catalog_writer.svg){ width="44" height="44" } Write to Catalog { #catalog-writer }

<div class="ff-split" markdown>

<div markdown>
Registers the result as a table in the [Catalog](../catalog/index.md), where colleagues can query, chart and schedule against it without rebuilding the flow.

Two modes sit behind tabs: **physical**, materialized as a Delta table on disk, and **virtual**, which stores no data and resolves on demand.
</div>

</div>

**Settings**

| Setting | Description |
|---|---|
| **Table Name** | Name for the catalog table. |
| **Catalog / Schema** | Target namespace in the catalog hierarchy. |
| **Description** | Optional description. |

### Physical tables and write modes

A physical write materializes a Delta table with full schema metadata, row count and lineage.

| Mode | Description |
|---|---|
| **Overwrite** | Replace all existing data in the table. |
| **Error if exists** | Fail if the table already exists. |
| **Append** | Add rows to the existing table. |
| **Upsert** | Insert new rows, update existing ones matching the key columns. |
| **Update** | Update only existing rows matching the key columns; no inserts. |
| **Delete** | Remove target rows matching the key columns in the source. |
| **SCD2** | Track history rather than overwriting it. See below. |

**Key Columns** are required for Upsert, Update, Delete and SCD2 — they are the columns rows are matched on.

**Track changes** turns [change tracking](../catalog/change-tracking.md) on for the table, so a downstream flow can read only the rows each write inserted, updated or deleted. It is enable-only: ticking it records the row changes of this and every later write, and clearing it again never turns tracking back off. Unavailable for the Overwrite, SCD2 and virtual modes.

### SCD2: keeping history

The SCD2 write mode tracks row history instead of overwriting it. Each write adds four generated columns (`sk`, `valid_from`, `valid_to`, `is_current`), end-dates the rows whose tracked columns changed, and inserts the new versions alongside the unchanged ones.

| Setting | Description |
|---|---|
| **Business key columns** | The business key — the same underlying field as **Key Columns**, which the UI relabels for SCD2. Required. |
| **Compare Columns** | Columns checked for changes. Empty (the default) compares every column that is neither a key column nor one of the four generated columns. |
| **Full Snapshot** | Off by default. When on, business keys present in an earlier write but absent from the current input are end-dated as no longer current; when off, absent keys stay current. |
| **Output** | What passes downstream. Every choice emits the input's columns plus the four generated ones: *All records that are inputted* (default) returns the input rows with each one's current surrogate key, *All changed records* returns only the versions this run inserted or end-dated, and *All active records* returns the table's whole current slice. |

Unlike every other write mode, an SCD2 writer's output is not its input: it carries the four generated columns, so a downstream node can use the surrogate key of the version just written. The node only grows an output handle on the canvas while SCD2 is selected; in every other mode it stays an endpoint.

A table that is already SCD2-tracked accepts only further `scd2` writes or a plain `overwrite`. An `overwrite` rebuilds the table and clears SCD2 tracking, while **append**, **upsert**, **update** and **delete** against an SCD2-tracked table fail at run time, since they would corrupt the version history. Writing `scd2` onto an existing table that is not already tracked also fails — pick a new table name, or delete the existing table first.

[Slowly Changing Dimensions](../catalog/slowly-changing-dimensions.md) covers the generated columns, change detection and reading history.

### Virtual tables

The **Virtual Table** tab creates a [virtual flow table](../catalog/virtual-tables.md) — a catalog entry that stores no data on disk and resolves on demand by executing the producer flow. Selecting the tab checks whether the pipeline supports optimized resolution:

- **Green checkmark** — every upstream node is lazy, so the virtual table stores a serialized execution plan and resolves instantly with predicate and projection pushdown.
- **Yellow warning** — some upstream nodes are eager or conditional, so resolution re-executes the full producer flow on each read. The blocking nodes are listed.

!!! warning "Flow registration required"
    Virtual tables require the flow to be registered in the catalog. If it is not, the virtual write fails — open the flow from the catalog, or register it first.

## ![Explore data](../../../assets/images/nodes/explore_data.svg){ width="44" height="44" } Explore data

Opens an interactive drag-and-drop chart builder (powered by [Graphic Walker](https://github.com/Kanaries/graphic-walker)) on the node's input. Drag columns onto the x and y axes, colour and size shelves to build bar, line, scatter and other charts — nothing to configure up front and no code. Chart configurations are saved with the node, so they survive reopening the flow.

It takes one input and produces no output: a terminal preview for eyeballing a dataset, not a step that transforms or writes anything. The builder shows an empty state until the upstream has run. There are no settings — the chart you compose *is* the configuration.

!!! note "Visual editor only"
    Headless runs — the `flowfile run flow` CLI, the scheduler, and other non-UI paths — skip Explore data nodes automatically, since there is nowhere to draw the chart. It has no effect on the data flowing through the rest of the pipeline.

To keep a chart as a shareable, reusable artifact rather than an ad-hoc preview, use the catalog's [visualizations](../catalog/visualizations.md).

## ![Flow Output](../../../assets/images/nodes/flow_output.svg){ width="44" height="44" } Flow Output

A named exit point for a [subflow](../subflows.md). Each Flow Output exposes one dataset to the parent flow that calls it, appearing as an output handle on the parent's [Run Flow](combine.md#run-flow) node. A flow can carry several, each with its own name.

**Settings**

| Setting | Description |
|---|---|
| **Output name** | The port name the parent reads from. Default `output`. |

## ![API response](../../../assets/images/nodes/api_response.svg){ width="44" height="44" } API response

Marks its input as the body of an HTTP endpoint. When a flow is published as an API, the data arriving here is serialized and returned to the caller; a published flow must contain exactly one. During interactive runs it passes data through unchanged, so previews keep working.

**Settings**

| Setting | Description |
|---|---|
| **Orientation** | `records` (a list of row objects, default) or `columns` (column-oriented). |
| **Max rows** | Optional cap on the number of rows returned. |

[Serve Flows as APIs](../catalog/flow-api.md) covers publishing, keys and parameters.

---

[← Machine Learning](ml.md) | [Back to all actions](index.md)
