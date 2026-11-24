# Writing Data

Flowfile's writers are Polars-compatible, with added cloud storage integration and visual workflow features.

!!! info "Polars Compatibility"
    Local file writers work identically to Polars, plus optional `description` for visual documentation.

## Local File Writing

### CSV Files

```python
import flowfile as ff

# Basic usage (same as Polars)
df = ff.read_csv("input.csv")
df.write_csv("output.csv")

# With Flowfile description
df.write_csv("processed_data.csv", description="Save cleaned customer data")

# Polars parameters work identically
df.write_csv(
    "output.csv",
    separator=";",
    encoding="utf-8",
    description="Export with semicolon delimiter"
)
```

**Key Parameters (same as Polars):**

- `separator`: Field delimiter (default: `,`)
- `encoding`: File encoding (default: `utf-8`)

### Parquet Files

```python
# Basic usage
df.write_parquet("output.parquet")

# With description and compression
df.write_parquet(
    "compressed_data.parquet",
    description="Save with high compression",
    compression="gzip"
)
```

### Arrow IPC / Feather, NDJSON, and Avro Files

```python
# Arrow IPC / Feather (lazy sink)
df.write_ipc("output.arrow")
df.write_ipc("output.arrow", compression="zstd")

# Newline-delimited JSON (lazy sink)
df.write_ndjson("events.ndjson", compression="gzip")

# Avro (eager write — materialised in the worker)
df.write_avro("output.avro", compression="snappy")
```

`sink_ipc` / `sink_ndjson` aliases are available for the lazy formats.

**Compression options (`compression=`):**

- Parquet: `"zstd"` (default), `"snappy"`, `"gzip"`, `"lz4"`, `"brotli"`, `"uncompressed"`
- IPC/Arrow: `"uncompressed"` (default), `"lz4"`, `"zstd"`
- NDJSON: `"uncompressed"` (default), `"gzip"`, `"zstd"`
- Avro: `"uncompressed"` (default), `"snappy"`, `"deflate"`

!!! info "Core never holds the dataset"
    Each of these writers runs on the flow's compute backend (for a script, the
    worker embedded in your Python process), as a file write, or as Polars code
    when you pass other keyword arguments. The lazy formats (IPC, NDJSON) sink
    straight from the plan; Avro has no lazy sink, so it is collected first.
    `flowfile_core` itself never materialises the data.

## Cloud Storage Writing

Flowfile extends writing capabilities with specialized cloud storage writers that integrate with secure connection management.

### Unified Cloud Storage Writer

`write_to_cloud_storage()` is a single entry point for writing any supported format to cloud storage.

```python
import flowfile as ff

# Write Parquet (default format)
ff.write_to_cloud_storage(
    df, "s3://bucket/output.parquet",
    connection_name="my-conn",
)

# Write CSV
ff.write_to_cloud_storage(
    df, "s3://bucket/output.csv",
    file_format="csv",
    connection_name="my-conn",
    delimiter=",",
)

# Write Delta with append mode
ff.write_to_cloud_storage(
    df, "s3://warehouse/my_table",
    file_format="delta",
    connection_name="my-conn",
    write_mode="append",
)
```

**Parameters:**

- `df`: The `LazyFrame` to write
- `path`: Cloud storage destination path
- `file_format`: `"csv"`, `"parquet"`, `"json"`, `"delta"` or `"iceberg"` (default: `"parquet"`)
- `connection_name`: Name of the stored cloud storage connection
- `delimiter`: CSV field separator (default: `;`). Only used for CSV
- `encoding`: CSV encoding (default: `utf8`). Only used for CSV
- `compression`: Parquet compression: `"snappy"`, `"gzip"`, `"brotli"`, `"lz4"`, `"zstd"` (default: `"snappy"`). Only used for Parquet
- `write_mode`: `"overwrite"` (default), `"append"`, `"error"`, `"upsert"`, `"update"` or `"delete"`. Only used for Delta; see [Cloud Storage Write Modes](#cloud-storage-write-modes)
- `partition_by`: Delta partition columns, applied when the write creates the table. Only used for Delta
- `merge_keys`: Key columns rows are matched on, required for `upsert`, `update` and `delete`. Only used for Delta
- `track_changes`: Turn the table's change feed on. Only used for Delta; see [Delta Lake Writing](#delta-lake-writing)
- `catalog_uri`, `catalog_warehouse`, `catalog_token_secret`: The Iceberg REST catalog's address, its warehouse and the
  name of a stored secret holding a bearer token. Only used for Iceberg; `write_mode` is then `"overwrite"` or
  `"append"`. See [An Iceberg table kept by a REST catalog](#an-iceberg-table-kept-by-a-rest-catalog)
- `catalog`: The name of a catalog added to Flowfile's catalog, instead of the three above

!!! tip "Recommended Approach"
    `write_to_cloud_storage()` is the recommended way to write to cloud storage. The format-specific methods below still work and are useful when you want a more concise call for a known format.

### An Iceberg table kept by a REST catalog

With `file_format="iceberg"`, `path` is the table's name (`namespace.table`) and `catalog_uri` the address of the
Iceberg REST catalog that keeps it. The write runs when the function is called; `write_mode` is `"overwrite"`
(default) or `"append"`. A catalog added to Flowfile's catalog is named instead, with `catalog="polaris"`: its
address, warehouse and sign-in come from there, and writing it needs Manage on its schema on a multi-user server.

```python
import flowfile as ff

orders = ff.read_from_cloud_storage(
    "demo.orders", file_format="iceberg",
    catalog_uri="http://127.0.0.1:18181/catalog", catalog_warehouse="proof",
)
ff.write_to_cloud_storage(
    orders.filter(ff.col("grp") == 3), "demo.orders_grp3", file_format="iceberg",
    catalog_uri="http://127.0.0.1:18181/catalog", catalog_warehouse="proof",
)
```

A write that fails after it wrote data files names the folders new under the table's `data/` folder; nothing
removes them, since they cannot be told apart from the files of another write that ran at the same moment. When the
catalog does not say whether the commit went through, the message says so: load the table before removing anything.
What is supported, in plain words:

- The table's files are on S3-compatible storage and its catalog hands out keys to them (vended credentials). Flowfile
  holds no storage key of its own for the table: the catalog hands one to the process that reads or writes, each run
  (Flowfile's core is also handed one when it asks the catalog for the table's columns, and keeps none).
  The keys last as long as the catalog says (often an hour) and are not renewed during a run; a run with less than a
  minute left on them is refused.
- Iceberg version 2 and 3 tables are read; version 2 tables are written. Tables with equality deletes (rows deleted by
  value, as Flink writes them) are refused with a sentence.
- A write appends, or overwrites the table's rows, and creates a missing table in an existing namespace. There is no
  merge, and a write never changes a table's columns or partitioning: the data's columns must be the table's, and it
  is cast to the table's types.
- A read of a table another engine deleted rows from can be slower: Polars' own reader cannot apply every delete file,
  PyIceberg reads it instead, and the run's log says so.
- A catalog that needs a sign-in takes a bearer token from a stored secret (`catalog_token_secret`, the secret's name;
  the token is never saved with the flow), or a client id and secret when it is named by `catalog=`.
- It runs from Python, and in a flow opened in the designer; the designer's own forms do not offer Iceberg yet. A
  multi-user server refuses a catalog named by its address and runs one named by `catalog=`.

### Format-Specific Cloud Writers

#### Cloud CSV Writing

```python
# Write to S3
df.write_csv_to_cloud_storage(
    "s3://my-bucket/output.csv",
    connection_name="my-aws-connection",
    delimiter=",",
    encoding="utf8",
    description="Export processed data to S3"
)
```

**Parameters:**

- `path`: Full S3 path including bucket and file name
- `connection_name`: Name of configured cloud storage connection
- `delimiter`: CSV field separator (default: `;`)
- `encoding`: File encoding (`utf8` or `utf8-lossy`)

#### Cloud Parquet Writing

```python
# Write to S3 with compression
df.write_parquet_to_cloud_storage(
    "s3://data-lake/processed/results.parquet",
    connection_name="data-lake-connection",
    compression="snappy",
    description="Save analysis results to data lake"
)
```

**Parameters:**

- `path`: Full S3 path for the output file
- `connection_name`: Name of configured cloud storage connection  
- `compression`: Compression algorithm (`snappy`, `gzip`, `brotli`, `lz4`, `zstd`)

#### Cloud JSON Writing

```python
# Write JSON to cloud storage
df.write_json_to_cloud_storage(
    "s3://api-data/export.json", 
    connection_name="api-storage",
    description="Export for API consumption"
)
```

#### Delta Lake Writing

```python
# Replace the table (the default mode)
df.write_delta(
    "s3://warehouse/customer_dim",
    connection_name="warehouse-connection",
    write_mode="overwrite",
    description="Update customer dimension table"
)

# Append to existing Delta table
new_data.write_delta(
    "s3://warehouse/customer_dim",
    connection_name="warehouse-connection",
    write_mode="append",
    description="Add new customers to dimension"
)
```

**Parameters:**

- `path`: Cloud storage path of the Delta table — the table's folder, e.g. `s3://warehouse/customer_dim`
- `connection_name`: Name of configured cloud storage connection
- `write_mode`: `"overwrite"` (default), `"append"`, `"error"`, `"upsert"`, `"update"` or `"delete"`. See [Cloud Storage Write Modes](#cloud-storage-write-modes)
- `partition_by`: Partition columns, applied when the write creates the table; later writes must match
- `merge_keys`: Key columns rows are matched on, required for `upsert`, `update` and `delete`
- `track_changes`: Turn the table's [change feed](../../visual-editor/catalog/change-tracking.md#cloud-delta-tables) on, so a reader can ask for only what each later write changed. Enable-only: `False` never turns it off. Not allowed with `write_mode="overwrite"`
- `description`: Optional description for this operation

The tested example upserts into a change-tracked Delta table on S3 twice, then reads back only the rows the second write changed:

```python
--8<-- "docs/examples/integrations/cloud_delta_changes.py:example"
```

## Catalog Writing

Write data to the Flowfile catalog as managed Delta tables. Available as both a standalone function and a FlowFrame method.

The tested example writes an aggregate to the catalog, then queries it back with SQL:

```python
--8<-- "docs/examples/catalog_analysis.py:example"
```

### Standalone Function

```python
import flowfile as ff

# Resolve (or create) the target schema once
schema = ff.CatalogReference("sales", auto_create=True).schema("staging", auto_create=True)

ff.write_catalog_table(
    df, "output_table",
    schema=schema,
    write_mode="upsert",
    merge_keys=["id"],
)
```

**Parameters:**

- `df`: The `LazyFrame` to write
- `table_name`: Name of the catalog table to write to (required)
- `schema`: A [`SchemaReference`](catalog-references.md) identifying the target catalog/schema. Preferred over `namespace_id`.
- `namespace_full_name`: The target schema as a plain `"catalog.schema"` string, resolved when the flow runs. Exported flow code uses this form
- `write_mode`: How to handle existing data (default: `"overwrite"`). See [Catalog Write Modes](#catalog-write-modes)
- `merge_keys`: Column names for merge operations (required for `upsert`, `update`, `delete`)
- `track_changes`: Turn [change tracking](../../visual-editor/catalog/change-tracking.md) on for the table. Enable-only; rejected with `write_mode` `"overwrite"`, `"virtual"` or `"scd2"`
- `description`: Optional description for the table

!!! info "Catalog handles vs raw IDs"
    `schema=` accepts a [`SchemaReference`](catalog-references.md) — a validated, name-based handle that resolves to the underlying namespace id once. Construction fails fast if the catalog or schema doesn't exist (or creates it if `auto_create=True`). The legacy `namespace_id=<int>` keyword still works for back-compat, but you can't pass both.

### FlowFrame Method

```python
# Convenience: write through the schema handle
schema.write_table(df, "output_table", write_mode="overwrite")

# Or as a method on the FlowFrame
df.write_catalog_table(
    "output_table",
    schema=schema,
    write_mode="overwrite",
)
```

Returns a new child `FlowFrame` representing the written data, allowing further chaining.

## Database Writing

Write data to a SQL database using a stored connection. Available as a method on `FlowFrame`.

```python
import flowfile as ff

df = ff.read_csv("data.csv")
df.write_database(
    connection_name="my_db",
    table_name="users",
    schema_name="public",
    if_exists="append",
)
```

**Parameters:**

- `connection_name`: Name of the stored database connection (required)
- `table_name`: Name of the table to write to (required)
- `schema_name`: Database schema name (e.g., `"public"`, `"dbo"` on SQL Server, `"PUBLIC"` on Snowflake)
- `database_name`: Database to write to, for a connection that reaches several (Snowflake, SQL Server). Unset,
  the connection's default database; set, the target is `database_name.schema_name.table_name` and a schema is
  required
- `if_exists`: What to do if the table exists: `"append"`, `"replace"`, or `"fail"` (default: `"append"`)
- `description`: Optional description for this operation

Returns a new child `FlowFrame`.

!!! note "DuckDB"
    Connections with `database_type="duckdb"` write to a local file. The same `if_exists`
    modes apply, and rich types (nested lists/structs, decimals, timestamps) are stored
    natively rather than text-encoded. A DuckDB file accepts one writer at a time — close
    other tools using the file while a flow writes to it.

!!! note "Snowflake"
    A write to a `database_type="snowflake"` connection uploads the rows to a temporary stage, which
    needs no grant of its own, and copies them into the table, so the connection's role needs `INSERT`
    on the table, and `CREATE TABLE` on the schema when the write creates it.
    Unquoted names are upper-cased by Snowflake (`order_id` becomes `ORDER_ID`), and
    `if_exists="replace"` recreates the table. A connection without a default database needs
    `database_name=` (for example `df.write_database("snowflake-sales", "ORDERS_COPY", schema_name="PUBLIC",
    database_name="ANALYTICS")`). See [Snowflake connections](../../visual-editor/connections.md#snowflake).

## Write Modes

### Cloud Storage Write Modes

CSV, Parquet and JSON cloud writes always replace the target file. Delta tables take a `write_mode`:

| Mode | Description |
|------|-------------|
| `overwrite` | Replace the entire table (default) |
| `error` | Fail if the table already exists |
| `append` | Add rows to the existing table |
| `upsert` | Insert new rows or update existing rows matched by `merge_keys` |
| `update` | Update only existing rows matched by `merge_keys` |
| `delete` | Delete rows matching `merge_keys` |

```python
# CSV, Parquet and JSON: no write_mode, the file is replaced
df.write_parquet_to_cloud_storage(
    "s3://bucket/data.parquet",
    connection_name="conn",
)

# Delta: append to the existing table
df.write_delta(
    "s3://warehouse/events",
    connection_name="conn",
    write_mode="append"
)
```

`upsert` and `update` first add any source column the table does not have yet.

!!! info "Google Cloud Storage"
    `upsert`, `update`, `delete` and `track_changes=True` are not supported for `gs://` paths. They work on S3 and Azure Data Lake Storage.

### Catalog Write Modes

The catalog supports these write modes:

| Mode | Description |
|------|-------------|
| `overwrite` | Replace the entire table |
| `error` | Fail if the table already exists |
| `append` | Add rows to the existing table |
| `upsert` | Insert new rows or update existing rows matched by `merge_keys` |
| `update` | Update only existing rows matched by `merge_keys` |
| `delete` | Delete rows matching `merge_keys` |
| `scd2` | Track history: end-date changed rows and insert new versions, keyed on `merge_keys`. See [Slowly Changing Dimensions](../../visual-editor/catalog/slowly-changing-dimensions.md) |
| `virtual` | Create a [virtual table](../../visual-editor/catalog/virtual-tables.md) — no data written to disk |

```python
# Upsert: insert or update based on merge keys
ff.write_catalog_table(
    df, "customers",
    write_mode="upsert",
    merge_keys=["customer_id"],
)
```

The tested example writes an SCD2-tracked dimension in two runs and reads back both the active rows and the full history:

```python
--8<-- "docs/examples/catalog_scd2.py:example"
```

!!! warning "Merge Keys Required"
    The `upsert`, `update`, `delete`, and `scd2` modes require `merge_keys` to be specified.

!!! info "scd2 keyword arguments require write_mode='scd2'"
    `write_catalog_table` and `write_table` accept `scd2_compare_columns`, `scd2_full_snapshot`, `scd2_partition_on_current`, `scd2_output_mode`, and the four `scd2_*_column` name overrides. Passing any of them with a `write_mode` other than `"scd2"` raises an error.

!!! info "What an scd2 write returns"
    An `scd2` write returns a frame carrying the input's columns followed by the four generated columns, so the surrogate key of the version just written is available downstream. `scd2_output_mode` picks the rows: `"input"` (the default) returns the input rows in order, each with its current surrogate key; `"changed"` returns only the versions this write inserted or end-dated; `"current"` returns the table's whole current slice. Every other write mode returns the input frame unchanged. See [Slowly Changing Dimensions](../../visual-editor/catalog/slowly-changing-dimensions.md#what-the-writer-passes-downstream).

!!! info "Virtual Mode"
    The `virtual` write mode creates a catalog entry without materializing data to disk. When the virtual table is read, the producer flow is re-executed on demand. This requires the flow to be registered in the catalog. See [Virtual Flow Tables](../../visual-editor/catalog/virtual-tables.md) for details.

## Connection Requirements

All cloud storage writing requires a configured connection referenced by name. See [Cloud Connection Management](cloud-connections.md).


--- 

[← Previous: Reading Data](reading-data.md) | [Next: Data Types →](data-types.md)
