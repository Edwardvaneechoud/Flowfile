# Flowfile Python API

The Flowfile Python API provides a Polars-compatible interface with additional features for visual ETL workflows, cloud storage integration, and secure secrets management.

!!! info "Polars Compatibility"
    Flowfile's API is designed to be nearly identical to Polars. If you know Polars, you already know 95% of Flowfile. The main additions are:
    
- `description` parameter for visual documentation
- `open_graph_in_editor()` for UI integration
- Cloud storage readers with integrated secrets

## Installation

```bash
pip install flowfile
```

## Quick Start


```python
import flowfile as ff
from flowfile import col

# Works exactly like Polars
df = ff.read_csv("sales.csv")
result = df.filter(col("amount") > 1000).group_by("region").agg(
    col("amount").sum().alias("total_sales")
)

# Flowfile addition: add descriptions
df = ff.read_csv("sales.csv", description="Load Q4 sales data")
```

=== "With Visual UI"

```python
# Open your pipeline in the visual editor
import flowfile as ff

ff.open_graph_in_editor(result.flow_graph)
```

## Documentation

### 📘 Core API

#### Data Input/Output
- [**Reading Data**](api/reading-data.md) - File formats and cloud storage
- [**Writing Data**](api/writing-data.md) - Saving results
- [**Data Types**](api/data-types.md) - Supported data types

#### Transformations
- [**DataFrame Operations**](api/dataframe-operations.md) - Filter, select, sort
- [**Expressions**](api/expressions.md) - Column operations
- [**Aggregations**](api/aggregations.md) - Group by and summarize
- [**Joins**](api/joins.md) - Combining datasets

### 🔐 Flowfile-Specific Features

#### Cloud & Security
- [**Cloud Storage**](features/cloud-storage.md) - S3, Azure ADLS integration
- [**Secrets Management**](features/secrets.md) - Secure credential handling
- [**Connections**](features/connections.md) - Managing data sources

#### Visual Integration
- [**UI Integration**](features/ui-integration.md) - Working with the visual editor
- [**Descriptions**](features/descriptions.md) - Documenting pipelines

### 📚 Reference
- [**API Reference**](reference/api-reference.md) - Complete method list
- [**Polars Differences**](reference/differences.md) - What's different from Polars
- [**Configuration**](reference/configuration.md) - Environment settings

## Key Differences from Polars

### 1. Description Parameter

Every Flowfile operation accepts an optional `description` parameter:

```python
# Standard Polars
import polars as pl
import flowfile as ff

df = pl.DataFrame({"status": ["active", "inactive", "active"], "amount": [100, 200, 300]})

df = df.filter(pl.col("status") == "active")

# Flowfile - with description for visual node

df = ff.FlowFrame({"status": ["active", "inactive", "active"], "amount": [100, 200, 300]})

df = df.filter(ff.col("status") == "active", description="Keep only active customers")
```

### 2. Cloud Storage Readers

Flowfile provides specialized readers for cloud storage with integrated authentication:

```python
# Flowfile cloud readers
import flowfile as ff
df = ff.scan_parquet_from_cloud_storage(
    "s3://bucket/data.parquet",
    connection_name="my-aws-account"
)

```

### 3. Secrets Management

Securely store and use credentials:

```python
# Create encrypted connection
import flowfile as ff
from pydantic import SecretStr

ff.create_cloud_storage_connection(
    ff.FullCloudStorageConnection(
    connection_name="data-lake",
    storage_type="s3",
    auth_method="access_key",
    aws_region="us-east-1",
    endpoint_url="http://localhost:9000",
    aws_allow_unsafe_html=True,
    aws_access_key_id="minioadmin",
    aws_secret_access_key=SecretStr("minioadmin")
)
)
```

### 4. Visual Integration

Open any pipeline in the Flowfile UI:

```python
# Build pipeline
import flowfile as ff

pipeline = ff.read_csv("data.csv").filter(ff.col("value") > 0)

# Visualize
ff.open_graph_in_editor(pipeline.flow_graph)
```

## Examples

### Standard Polars-Compatible Operations

```python
# These work exactly like Polars
import flowfile as ff

df = ff.read_parquet("data.parquet")
df = df.select(["id", "name", "amount"])
df = df.filter(ff.col("amount") > 100)
df = df.with_columns([
    (ff.col("amount") * 1.1).alias("amount_with_tax")
])
df = df.group_by("category").agg([
    ff.col("amount").sum().alias("total"),
    ff.col("id").count().alias("count")
])

# Get results
polars_df = df.collect()  # Returns standard Polars DataFrame
```

### Cloud Data Pipeline

```python

import flowfile as ff
from pydantic import SecretStr

ff.create_cloud_storage_connection_if_not_exists(
    ff.FullCloudStorageConnection(
    connection_name="data-lake",
    storage_type="s3",
    auth_method="access_key",
    aws_region="us-east-1",
    endpoint_url="http://localhost:9000",
    aws_allow_unsafe_html=True,
    aws_access_key_id="minioadmin",
    aws_secret_access_key=SecretStr("minioadmin")
)
)

# Read from cloud
df = ff.scan_parquet_from_cloud_storage(
    "s3://data-lake/sales_data",
    connection_name="data-lake",
)

# Process
result = df.filter(
    ff.col("quality_score") > 0.8,
    description="Keep high quality records only"
).group_by(
    "product_id",
    description="Aggregate by product"
).agg([
    ff.col("revenue").sum(),
    ff.col("quantity").sum()
])

# Write back to cloud
result.write_parquet_to_cloud_storage(
    "s3://processed/summary.parquet",
    connection_name="data-lake",
    description="Save aggregated results"
)
```

## Getting Started

1. **If you know Polars**: Start using Flowfile immediately - the API is the same
2. **For cloud data**: Set up connections using our [Cloud Storage Guide](features/cloud-storage.md)
3. **For visual workflows**: Use `open_graph_in_editor()` to see your pipelines

## Quick Links

- 🚀 [First Steps](getting-started/first-steps.md) - Build your first pipeline
- 🔧 [API Reference](reference/api-reference.md) - All methods and functions
- 🌩️ [Cloud Setup](features/cloud-storage.md) - Configure cloud connections
- 📊 [Visual Workflows](features/ui-integration.md) - Code to visual and back