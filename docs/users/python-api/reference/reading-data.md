# Reading Data

Flowfile provides Polars-compatible readers with additional cloud storage integration and visual workflow features.

!!! info "Polars Compatibility"
    All Flowfile readers accept the same parameters as Polars, plus optional `description` for visual documentation.

## Local File Reading

### CSV Files

```python
import flowfile as ff

# Basic usage (same as Polars)
df = ff.read_csv("data.csv")

# With Flowfile description
df = ff.read_csv("data.csv", description="Load customer data")

# Polars parameters work identically
df = ff.read_csv(
    "data.csv",
    separator=",",
    has_header=True,
    skip_rows=1,
    n_rows=1000,
    description="Sample first 1000 customer records"
)
```

**Key Parameters (same as Polars):**

- `separator`: Field delimiter (default: `,`)
- `has_header`: First row contains column names (default: `True`)
- `skip_rows`: Skip rows at start of file
- `n_rows`: Maximum rows to read
- `encoding`: File encoding (default: `utf8`)
- `null_values`: Values to treat as null
- `schema_overrides`: Override column types

### Parquet Files

```python
# Basic usage
df = ff.read_parquet("data.parquet")

# With description
df = ff.read_parquet("sales_data.parquet", description="Q4 sales results")
```

### Arrow IPC / Feather, NDJSON, and Avro Files

Flowfile also reads Arrow IPC/Feather, newline-delimited JSON (NDJSON), and Avro
files. These readers live in `flowfile_frame` and are **not** re-exported on the
`ff` namespace — import them directly:

```python
from flowfile_frame import read_ipc, read_ndjson, read_avro, scan_ipc, scan_ndjson

# Arrow IPC / Feather (lazy scan — like parquet)
df = read_ipc("data.arrow", description="Arrow IPC source")

# Newline-delimited JSON (lazy scan)
df = read_ndjson("events.ndjson")

# Avro (eager read — offloaded to the worker so core never holds the dataset)
df = read_avro("data.avro")
```

IPC and NDJSON are scanned lazily, so they also provide `scan_ipc` / `scan_ndjson`.
Avro has no lazy scan in Polars, so its read is offloaded to the worker.

### Scanning vs Reading

Flowfile provides both `read_*` and `scan_*` functions for Polars compatibility:

```python
# These are identical in Flowfile
df1 = ff.read_csv("data.csv")
df2 = ff.scan_csv("data.csv")  # Alias for read_csv

# The lazy IPC/NDJSON scans are imported from flowfile_frame (not ff.*)
from flowfile_frame import scan_ipc, scan_ndjson

df3 = scan_ipc("data.arrow")
df4 = scan_ndjson("events.ndjson")
```

## Cloud Storage Reading

Flowfile extends Polars with specialized cloud storage readers that integrate with secure connection management.

### Unified Cloud Storage Reader

`read_from_cloud_storage()` is a single entry point for reading any supported format from cloud storage. It dispatches to the appropriate format-specific reader internally.

```python
import flowfile as ff

# Read Parquet (default format)
df = ff.read_from_cloud_storage(
    "s3://bucket/data.parquet",
    connection_name="my-conn",
)

# Read CSV
df = ff.read_from_cloud_storage(
    "s3://bucket/data.csv",
    file_format="csv",
    connection_name="my-conn",
    delimiter=",",
    has_header=True,
)

# Read Delta with time travel
df = ff.read_from_cloud_storage(
    "s3://warehouse/my_table",
    file_format="delta",
    connection_name="my-conn",
    delta_version=5,
)
```

**Parameters:**

- `source`: Cloud storage path (e.g., `s3://bucket/path/file.parquet`)
- `file_format`: `"csv"`, `"parquet"`, `"json"`, or `"delta"` (default: `"parquet"`)
- `connection_name`: Name of the stored cloud storage connection
- `scan_mode`: `"single_file"` or `"directory"`. Auto-detected from path if `None`
- `delimiter`: CSV field separator (default: `;`). Only used for CSV
- `has_header`: Whether CSV has headers (default: `True`). Only used for CSV
- `encoding`: CSV encoding (default: `utf8`). Only used for CSV
- `delta_version`: Delta table version for time-travel queries. Only used for Delta

!!! tip "Recommended Approach"
    `read_from_cloud_storage()` is the recommended way to read from cloud storage. The format-specific `scan_*` functions below still work and are useful when you want a more concise call for a known format.

### Format-Specific Cloud Readers

#### Cloud CSV Reading

```python
# Read from S3 with connection
df = ff.scan_csv_from_cloud_storage(
    "s3://my-bucket/data.csv",
    connection_name="my-aws-connection",
    delimiter=",",
    has_header=True,
    encoding="utf8"
)

# Directory scanning (reads all CSV files)
df = ff.scan_csv_from_cloud_storage(
    "s3://my-bucket/csv-files/",
    connection_name="my-aws-connection"
)
```

!!! note "CSV delimiter default"
    The cloud CSV readers default `delimiter=";"`. Pass `delimiter=","` explicitly for comma-separated files (as above).

#### Cloud Parquet Reading

```python
# Single file
df = ff.scan_parquet_from_cloud_storage(
    "s3://data-lake/sales.parquet",
    connection_name="data-lake-connection"
)

# Directory of files
df = ff.scan_parquet_from_cloud_storage(
    "s3://data-lake/partitioned-data/",
    connection_name="data-lake-connection",
    scan_mode="directory"
)
```

#### Cloud JSON Reading

```python
df = ff.scan_json_from_cloud_storage(
    "s3://my-bucket/data.json",
    connection_name="my-aws-connection"
)
```

#### Delta Lake Reading

```python
# Latest version
df = ff.scan_delta(
    "s3://data-lake/delta-table",
    connection_name="data-lake-connection"
)

# Time-travel to a specific version
df = ff.scan_delta(
    "s3://data-lake/delta-table",
    connection_name="data-lake-connection",
    version=5,
)
```

## Catalog Reading

Read tables from the Flowfile catalog. The catalog provides a managed layer for discovering and versioning datasets stored as Delta tables. Both physical and [virtual tables](../../visual-editor/catalog/virtual-tables.md) are supported.

### Read a Table by Name

```python
import flowfile as ff

# Read a catalog table (physical or virtual)
df = ff.read_catalog_table("my_table")

# Scope to a specific schema using a typed reference
schema = ff.CatalogReference("sales").schema("raw")
df = ff.read_catalog_table("my_table", schema=schema)

# Or call read_table directly on the schema handle
df = schema.read_table("my_table")

# Time travel to a specific Delta version (physical tables only)
df = ff.read_catalog_table("my_table", delta_version=5)
```

**Parameters:**

- `table_name`: Name of the catalog table to read (required)
- `schema`: A [`SchemaReference`](catalog-references.md) identifying the catalog/schema to read from. Preferred over `namespace_id`.
- `delta_version`: Optional Delta version for time-travel queries (physical tables only)

Returns a `FlowFrame`. Use `.collect()` to materialize, `.data` to access the underlying `LazyFrame`, or `open_graph_in_editor()` to visualize in the UI.

!!! info "Looking up tables by name"
    See [Catalog References](catalog-references.md) for the `CatalogReference` / `SchemaReference` API. The legacy `namespace_id=<int>` keyword is still accepted but discouraged — passing both `schema=` and `namespace_id=` raises `ValueError`.

!!! info "Virtual table resolution"
    When reading a virtual table, the data is resolved on demand. Optimized virtual tables deserialize a stored execution plan instantly. Non-optimized virtual tables execute the producer flow to produce results. See [Virtual Flow Tables](../../visual-editor/catalog/virtual-tables.md) for details.

### Query with SQL

Use `read_catalog_sql()` to execute SQL queries against all catalog tables — both physical and virtual. Tables are registered by name in a Polars SQL context. `read_catalog_sql` is imported from `flowfile_frame` (it is not on the `ff` namespace):

```python
from flowfile_frame import read_catalog_sql

# Query a single table
df = read_catalog_sql("SELECT * FROM customers WHERE region = 'Europe'")

# Join across catalog tables
df = read_catalog_sql("""
    SELECT o.order_id, c.name, o.total
    FROM orders o
    JOIN customers c ON o.customer_id = c.id
    WHERE o.total > 1000
""")

# Aggregate virtual and physical tables together
df = read_catalog_sql("""
    SELECT category, SUM(amount) as total
    FROM sales_summary
    GROUP BY category
""")
```

**Parameters:**

- `sql_query`: SQL query string to execute (required)

Returns a `FlowFrame` backed by a catalog SQL reader node. The SQL dialect is Polars SQL, which supports standard `SELECT`, `WHERE`, `JOIN`, `GROUP BY`, `ORDER BY`, `HAVING`, `UNION`, subqueries, and window functions.

## Kafka Reading

Read messages from a Kafka topic using a stored Flowfile connection.

```python
import flowfile as ff

df = ff.read_kafka(
    "my-kafka-connection",
    topic_name="events",
    start_offset="earliest",
    max_messages=10_000,
)
```

**Parameters:**

- `connection_name`: Name of the stored Kafka connection (required)
- `topic_name`: Kafka topic to consume from (required)
- `max_messages`: Maximum number of messages to consume (default: `100_000`)
- `start_offset`: Where to start consuming: `"earliest"` or `"latest"` (default: `"latest"`)
- `poll_timeout_seconds`: How long to poll for messages in seconds (default: `30.0`)
- `value_format`: Message value format (default: `"json"`)

Returns a `FlowFrame`.

## Database Reading

Read data from SQL databases using stored connections.

### Setup Connection

```python
import flowfile as ff

ff.create_database_connection(
    connection_name="my_db",
    database_type="postgresql",
    host="localhost",
    port=5432,
    database="mydb",
    username="user",
    password="pass"
)
```

### Read a Table

```python
df = ff.read_database(
    "my_db",
    table_name="users",
    schema_name="public"
)
```

### Read with SQL Query

```python
df = ff.read_database(
    "my_db",
    query="SELECT id, name FROM users WHERE active = true"
)
```

**Parameters:**

- `connection_name`: Name of a stored database connection (required)
- `table_name`: Table to read from
- `schema_name`: Database schema (e.g., "public")
- `query`: Custom SQL query (takes precedence over `table_name`)

!!! note "Return Type"
    `read_database()` returns a `FlowFrame` (not a raw Polars `LazyFrame`). The result supports `.collect()` to materialize data, `.data` to access the underlying `LazyFrame`, and `open_graph_in_editor()` to visualize the pipeline in the UI.

The tested integration example reads a table and a query from PostgreSQL through a stored connection:

```python
--8<-- "docs/examples/integrations/database_read.py:example"
```

### DuckDB

DuckDB connections point at a local file — no host, port, or credentials:

```python
ff.create_database_connection(
    connection_name="local_duckdb",
    database_type="duckdb",
    database="/path/to/analytics.duckdb",
)

df = ff.read_database("local_duckdb", table_name="events")
```

The tested DuckDB example (runs fully in-process):

```python
--8<-- "docs/examples/integrations/database_read_duckdb.py:example"
```

### SQL Server

SQL Server connections use `database_type="mssql"` (default port 1433; the default schema is `dbo`):

```python
ff.create_database_connection(
    connection_name="analytics-sqlserver",
    database_type="mssql",
    host="sqlserver.example.com",
    port=1433,
    database="analytics",
    username="user",
    password="pass",
)

df = ff.read_database("analytics-sqlserver", schema_name="dbo", table_name="events")
```

The tested SQL Server example reads a table and a query through a stored connection:

```python
--8<-- "docs/examples/integrations/database_read_mssql.py:example"
```

### Snowflake

Snowflake connections use `database_type="snowflake"` with no host or port — the account
identifier (plus an optional warehouse and role) goes in `extra_params`:

```python
ff.create_database_connection(
    connection_name="analytics-snowflake",
    database_type="snowflake",
    database="ANALYTICS",
    username="user",
    password="pass",
    extra_params={
        "account": "myorg-myaccount",
        "warehouse": "COMPUTE_WH",
        "role": "ANALYST",
    },
)

df = ff.read_database("analytics-snowflake", schema_name="PUBLIC", table_name="EVENTS")
```

For key-pair (JWT) authentication — Snowflake's recommended method for programmatic
access — pass `auth_method="key_pair"` with the private key PEM *text* (never a file
path; read the file yourself). Add `private_key_passphrase` when the PEM is encrypted:

```python
ff.create_database_connection(
    connection_name="analytics-snowflake-kp",
    database_type="snowflake",
    database="ANALYTICS",
    username="svc_user",
    auth_method="key_pair",
    private_key=open("rsa_key.p8").read(),
    extra_params={"account": "myorg-myaccount", "warehouse": "COMPUTE_WH"},
)
```

The key is stored as an encrypted secret, exactly like a password.

Semi-structured columns (`VARIANT`, `OBJECT`, `ARRAY`) are read as JSON text. The tested
Snowflake example reads a table and a query through a stored connection (it runs only when
Snowflake test credentials are configured):

```python
--8<-- "docs/examples/integrations/database_read_snowflake.py:example"
```

## Connection Management

Set up cloud and database connections once, then reference them by name. See [Cloud Connection Management](cloud-connections.md).

## Examples

```python
import flowfile as ff

# Read local file
customers = ff.read_csv("customers.csv", description="Customer master data")

# Read from cloud
orders = ff.scan_parquet_from_cloud_storage(
    "s3://data-warehouse/orders/",
    connection_name="warehouse",
    description="Order history from data warehouse"
)

# Continue processing...
result = customers.join(orders, on="customer_id")
```


[← Previous: Introduction](index.md) | [Next: Writing Data →](writing-data.md)
