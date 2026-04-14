# Writing Data

Flowfile provides Polars-compatible writers with additional cloud storage integration and visual workflow features.

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
- `file_format`: `"csv"`, `"parquet"`, `"json"`, or `"delta"` (default: `"parquet"`)
- `connection_name`: Name of the stored cloud storage connection
- `delimiter`: CSV field separator (default: `;`). Only used for CSV
- `encoding`: CSV encoding (default: `utf8`). Only used for CSV
- `compression`: Parquet compression: `"snappy"`, `"gzip"`, `"brotli"`, `"lz4"`, `"zstd"` (default: `"snappy"`). Only used for Parquet
- `write_mode`: `"overwrite"` or `"append"` (default: `"overwrite"`). Only used for Delta

!!! tip "Recommended Approach"
    `write_to_cloud_storage()` is the recommended way to write to cloud storage. The format-specific methods below still work and are useful when you want a more concise call for a known format.

### Format-Specific Cloud Writers

### Cloud CSV Writing

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

### Cloud Parquet Writing

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

### Cloud JSON Writing

```python
# Write JSON to cloud storage
df.write_json_to_cloud_storage(
    "s3://api-data/export.json", 
    connection_name="api-storage",
    description="Export for API consumption"
)
```

### Delta Lake Writing

```python
# Write Delta table (supports append mode)
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

- `path`: S3 path for the Delta table
- `connection_name`: Name of configured cloud storage connection
- `write_mode`: `overwrite` (replace) or `append` (add to existing)

## Catalog Writing

Write data to the Flowfile catalog as managed Delta tables. Available as both a standalone function and a FlowFrame method.

### Standalone Function

```python
import flowfile as ff

ff.write_catalog_table(
    df, "output_table",
    namespace_id=3,
    write_mode="upsert",
    merge_keys=["id"],
)
```

**Parameters:**

- `df`: The `LazyFrame` to write
- `table_name`: Name of the catalog table to write to (required)
- `namespace_id`: Optional namespace ID for the table
- `write_mode`: How to handle existing data (default: `"overwrite"`). See [Write Modes](#write-modes)
- `merge_keys`: Column names for merge operations (required for `upsert`, `update`, `delete`)
- `description`: Optional description for the table

### FlowFrame Method

```python
df.write_catalog_table(
    "output_table",
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
- `schema_name`: Database schema name (e.g., `"public"`)
- `if_exists`: What to do if the table exists: `"append"`, `"replace"`, or `"fail"` (default: `"append"`)
- `description`: Optional description for this operation

Returns a new child `FlowFrame`.

## Write Modes

### Overwrite vs Append

```python
# Overwrite existing data (default)
df.write_parquet_to_cloud_storage(
    "s3://bucket/data.parquet",
    connection_name="conn",
    write_mode="overwrite"  # Default for most formats
)

# Append to existing (Delta Lake only)
df.write_delta(
    "s3://warehouse/events",
    connection_name="conn",
    write_mode="append"
)
```

!!! info "Append Mode"
    For cloud storage, append is only supported for Delta Lake format. Other formats always overwrite.

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
| `virtual` | Create a [virtual table](../../visual-editor/virtual-tables.md) — no data written to disk |

```python
# Upsert: insert or update based on merge keys
ff.write_catalog_table(
    df, "customers",
    write_mode="upsert",
    merge_keys=["customer_id"],
)
```

!!! warning "Merge Keys Required"
    The `upsert`, `update`, and `delete` modes require `merge_keys` to be specified.

!!! info "Virtual Mode"
    The `virtual` write mode creates a catalog entry without materializing data to disk. When the virtual table is read, the producer flow is re-executed on demand. This requires the flow to be registered in the catalog. See [Virtual Flow Tables](../../visual-editor/virtual-tables.md) for details.

## Connection Requirements

All cloud storage writing requires a configured connection:

```python
import flowfile as ff
from pydantic import SecretStr

# Set up connection before writing
ff.create_cloud_storage_connection_if_not_exists(
    ff.FullCloudStorageConnection(
        connection_name="data-lake",
        storage_type="s3", 
        auth_method="access_key",
        aws_region="us-east-1",
        aws_access_key_id="your-key",
        aws_secret_access_key=SecretStr("your-secret")
    )
)

# Now you can write to cloud storage
df.write_parquet_to_cloud_storage(
    "s3://data-lake/output.parquet",
    connection_name="data-lake"
)
```


--- 

[← Previous: Reading Data](reading-data.md) | [Next: Data Types →](data-types.md)
