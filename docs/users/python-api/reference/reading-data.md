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

## Error Handling

```python
# Handle missing files gracefully
try:
    df = ff.read_csv("might_not_exist.csv")
except FileNotFoundError:
    # Create empty DataFrame with expected schema
    df = ff.FlowFrame({"id": [], "name": []})

# For cloud storage, connection errors are handled automatically
# Invalid connections will raise clear error messages
```

[← Previous: Introduction](index.md) | [Next: Writing Data →](writing-data.md)
