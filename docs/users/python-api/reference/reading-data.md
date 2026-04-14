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

### Scanning vs Reading

Flowfile provides both `read_*` and `scan_*` functions for Polars compatibility:

```python
# These are identical in Flowfile
df1 = ff.read_csv("data.csv")
df2 = ff.scan_csv("data.csv")  # Alias for read_csv
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

### Cloud CSV Reading

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

### Cloud Parquet Reading

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

### Cloud JSON Reading

```python
df = ff.scan_json_from_cloud_storage(
    "s3://my-bucket/data.json",
    connection_name="my-aws-connection"
)
```

### Delta Lake Reading

```python
# Latest version
df = ff.scan_delta(
    "s3://data-lake/delta-table",
    connection_name="data-lake-connection"
)

# Specific version (if supported)
df = ff.scan_delta(
    "s3://data-lake/delta-table",
    connection_name="data-lake-connection"
    # Note: version parameter support depends on implementation
)
```

## Catalog Reading

Read tables from the Flowfile catalog. The catalog provides a managed layer for discovering and versioning datasets stored as Delta tables. Both physical and [virtual tables](../../visual-editor/catalog/virtual-tables.md) are supported.

### Read a Table by Name

```python
import flowfile as ff

# Read a catalog table (physical or virtual)
df = ff.read_catalog_table("my_table")

# Read from a specific namespace
df = ff.read_catalog_table("my_table", namespace_id=3)

# Time travel to a specific Delta version (physical tables only)
df = ff.read_catalog_table("my_table", delta_version=5)
```

**Parameters:**

- `table_name`: Name of the catalog table to read (required)
- `namespace_id`: Optional namespace ID to scope the lookup
- `delta_version`: Optional Delta version for time-travel queries (physical tables only)

Returns a `FlowFrame`. Use `.collect()` to materialize, `.data` to access the underlying `LazyFrame`, or `open_graph_in_editor()` to visualize in the UI.

!!! info "Virtual table resolution"
    When reading a virtual table, the data is resolved on demand. Optimized virtual tables deserialize a stored execution plan instantly. Non-optimized virtual tables execute the producer flow to produce results. See [Virtual Flow Tables](../../visual-editor/catalog/virtual-tables.md) for details.

### Query with SQL

Use `read_catalog_sql()` to execute SQL queries against all catalog tables — both physical and virtual. Tables are registered by name in a Polars SQL context.

```python
import flowfile as ff

# Query a single table
df = ff.read_catalog_sql("SELECT * FROM customers WHERE region = 'Europe'")

# Join across catalog tables
df = ff.read_catalog_sql("""
    SELECT o.order_id, c.name, o.total
    FROM orders o
    JOIN customers c ON o.customer_id = c.id
    WHERE o.total > 1000
""")

# Aggregate virtual and physical tables together
df = ff.read_catalog_sql("""
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

## Connection Management

Before reading from cloud storage, set up connections:

```python
import flowfile as ff
from pydantic import SecretStr

# Create S3 connection
ff.create_cloud_storage_connection_if_not_exists(
    ff.FullCloudStorageConnection(
        connection_name="my-aws-connection",
        storage_type="s3",
        auth_method="access_key",
        aws_region="us-east-1",
        aws_access_key_id="your-access-key",
        aws_secret_access_key=SecretStr("your-secret-key")
    )
)
```

## Flowfile-Specific Features

### Description Parameter

Every reader accepts an optional `description` for visual documentation:

```python
df = ff.read_csv(
    "quarterly_sales.csv",
    description="Load Q4 2024 sales data for analysis"
)
```

### Automatic Scan Mode Detection

Cloud storage readers automatically detect scan mode:

```python
# Automatically detects single file
df = ff.scan_parquet_from_cloud_storage("s3://bucket/file.parquet")

# Automatically detects directory scan
df = ff.scan_parquet_from_cloud_storage("s3://bucket/folder/")
```

### Integration with Visual UI

All reading operations create nodes in the visual workflow:

```python
df = ff.read_csv("data.csv", description="Source data")

# Open in visual editor
ff.open_graph_in_editor(df.flow_graph)
```

## Examples

### Standard Data Pipeline

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

### Multi-Format Cloud Pipeline

```python
# Different formats from same connection
config_data = ff.scan_json_from_cloud_storage(
    "s3://configs/settings.json",
    connection_name="app-data"
)

sales_data = ff.scan_parquet_from_cloud_storage(
    "s3://analytics/sales/",
    connection_name="app-data"
)

delta_data = ff.scan_delta(
    "s3://warehouse/customer_dim",
    connection_name="app-data"
)
```


[← Previous: Introduction](index.md) | [Next: Writing Data →](writing-data.md)
