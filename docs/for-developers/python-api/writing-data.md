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

!!! warning "Overwrite Mode"
    `overwrite` mode replaces any existing file at the target path. Double-check paths before executing.

!!! info "Append Mode"
    Currently only supported for Delta Lake format. Other formats always overwrite.

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

## Flowfile-Specific Features

### Method Chaining

All write operations return a new FlowFrame, enabling method chaining:

```python
result = (
    df.filter(ff.col("active") == True)
    .write_parquet_to_cloud_storage(
        "s3://processed/active_customers.parquet",
        connection_name="storage",
        description="Save active customers"
    )
    .write_csv_to_cloud_storage(
        "s3://exports/active_customers.csv", 
        connection_name="storage",
        description="CSV export for reporting"
    )
)
```

### Visual Integration

Write operations create nodes in the visual workflow:

```python
pipeline = (
    ff.read_csv("input.csv")
    .filter(ff.col("status") == "processed")
    .write_parquet("output.parquet", description="Final results")
)

# Visualize the complete pipeline
ff.open_graph_in_editor(pipeline.flow_graph)
```

### Multiple Output Formats

Write the same data to multiple formats:

```python
# Process once, output multiple formats
processed_data = df.filter(ff.col("quality_score") > 0.8)

# Save for data science team (Parquet)
processed_data.write_parquet_to_cloud_storage(
    "s3://analytics/high_quality_data.parquet",
    connection_name="analytics",
    compression="snappy",
    description="High-quality data for ML models"
)

# Save for business users (CSV) 
processed_data.write_csv_to_cloud_storage(
    "s3://reports/high_quality_data.csv",
    connection_name="analytics", 
    delimiter=",",
    description="High-quality data for business reporting"
)

# Save as Delta table for warehouse
processed_data.write_delta(
    "s3://warehouse/quality_data",
    connection_name="warehouse",
    write_mode="overwrite",
    description="Update quality data table"
)
```

## Complete Pipeline Example

```python
import flowfile as ff
from pydantic import SecretStr

# Set up cloud connection
ff.create_cloud_storage_connection_if_not_exists(
    ff.FullCloudStorageConnection(
        connection_name="data-pipeline",
        storage_type="s3",
        auth_method="access_key", 
        aws_region="us-west-2",
        aws_access_key_id="your-key",
        aws_secret_access_key=SecretStr("your-secret")
    )
)

# Build processing pipeline with multiple outputs
pipeline = (
    ff.scan_csv_from_cloud_storage(
        "s3://raw-data/sales.csv",
        connection_name="data-pipeline",
        description="Load raw sales data"
    )
    .filter(
        ff.col("amount") > 0,
        description="Remove invalid transactions"
    )
    .with_columns([
        (ff.col("amount") * 1.1).alias("amount_with_tax")
    ], description="Calculate tax-inclusive amounts")
    .write_parquet_to_cloud_storage(
        "s3://processed-data/sales_processed.parquet",
        connection_name="data-pipeline",
        compression="gzip",
        description="Save processed sales data"
    )
    .write_delta(
        "s3://warehouse/sales_fact",
        connection_name="data-pipeline",
        write_mode="append", 
        description="Append to sales fact table"
    )
)

# View the complete pipeline
ff.open_graph_in_editor(pipeline.flow_graph)
```