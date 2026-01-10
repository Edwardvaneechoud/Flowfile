# Azure Data Lake Storage (ADLS) Testing with Azurite

This directory contains utilities for testing ADLS connections using Azurite, Microsoft's official Azure Storage emulator.

## Quick Start

### Starting Azurite

```bash
# Start Azurite container with test data
poetry run start_azurite
```

This will:
- Start an Azurite Docker container
- Create test containers (test-container, flowfile-test, sample-data, etc.)
- Populate with sample data in Parquet, CSV, and JSON formats
- Print connection details

### Stopping Azurite

```bash
# Stop and clean up Azurite container
poetry run stop_azurite
```

## Connection Details

When Azurite is running, use these connection details:

- **Account Name**: `devstoreaccount1`
- **Account Key**: `Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==`
- **Blob Endpoint**: `http://localhost:10000/devstoreaccount1`

## Authentication Methods

### 1. Access Key Authentication

The simplest method for testing:

```python
from flowfile_core.schemas.cloud_storage_schemas import FullCloudStorageConnection

connection = FullCloudStorageConnection(
    storage_type="adls",
    auth_method="access_key",
    connection_name="azurite_test",
    azure_account_name="devstoreaccount1",
    azure_account_key="Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==",
    verify_ssl=False,  # Required for local testing
)
```

### 2. Service Principal Authentication

For production Azure environments:

```python
connection = FullCloudStorageConnection(
    storage_type="adls",
    auth_method="service_principal",
    connection_name="prod_adls",
    azure_account_name="mystorageaccount",
    azure_tenant_id="12345678-1234-1234-1234-123456789012",
    azure_client_id="87654321-4321-4321-4321-210987654321",
    azure_client_secret="your-client-secret",
    verify_ssl=True,
)
```

### 3. SAS Token Authentication

For temporary, scoped access:

```python
connection = FullCloudStorageConnection(
    storage_type="adls",
    auth_method="sas_token",
    connection_name="sas_access",
    azure_account_name="mystorageaccount",
    azure_sas_token="sv=2021-06-08&ss=bfqt&srt=sco&sp=rwdlacupiytfx...",
    verify_ssl=True,
)
```

## Reading from ADLS

### Single File

```python
from flowfile_core.flowfile.flow_data_engine.cloud_storage_reader import CloudStorageReader

# Get storage options
storage_options = CloudStorageReader.get_storage_options(connection)

# Read single file
df = pl.scan_parquet(
    "az://test-container/data/test_data.parquet",
    storage_options=storage_options
).collect()
```

### Directory with Wildcards

```python
# Read all parquet files in a directory
df = pl.scan_parquet(
    "az://test-container/data/partitioned/*.parquet",
    storage_options=storage_options
).collect()
```

## Writing to ADLS

```python
from flowfile_core.schemas.cloud_storage_schemas import (
    CloudStorageWriteSettings,
    get_cloud_storage_write_settings_worker_interface
)

# Create write settings
write_settings = CloudStorageWriteSettings(
    auth_mode="access_key",
    connection_name="azurite_test",
    resource_path="az://test-container/output/result.parquet",
    file_format="parquet",
    parquet_compression="snappy",
    write_mode="overwrite",
)

# Write data
from flowfile_worker.external_sources.s3_source.main import write_df_to_cloud

write_df_to_cloud(df.lazy(), write_settings, logger)
```

## Test Data Structure

After running `start_azurite`, the following test data is available:

```
test-container/
├── data/
│   ├── test_data.parquet       # Sample DataFrame (5 rows)
│   ├── test_data.csv           # Same data in CSV format
│   ├── test_data.json          # Same data in NDJSON format
│   └── partitioned/            # Partitioned data for directory reads
│       ├── part_0.parquet
│       ├── part_1.parquet
│       └── part_2.parquet
```

## Frontend Usage

### Creating an ADLS Connection

1. Navigate to **Cloud Connections** in the UI
2. Click **New Connection**
3. Select **Azure Data Lake Storage** as the storage type
4. Choose authentication method:
   - **Access Key**: Account name + account key
   - **Service Principal**: Tenant ID + Client ID + Client Secret
   - **SAS Token**: Account name + SAS token
5. For local testing with Azurite:
   - Account Name: `devstoreaccount1`
   - Account Key: `Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==`
   - Custom Endpoint URL: `http://localhost:10000`
   - Uncheck **Verify SSL**

### Using ADLS in Nodes

**Cloud Storage Reader Node:**
1. Add Cloud Storage Reader node to your flow
2. Select your ADLS connection
3. Enter path: `az://test-container/data/test_data.parquet`
4. Choose file format (Parquet, CSV, JSON, Delta)
5. Run the flow

**Cloud Storage Writer Node:**
1. Add Cloud Storage Writer node to your flow
2. Select your ADLS connection
3. Enter output path: `az://test-container/output/result.parquet`
4. Choose file format and compression
5. Run the flow

## URI Formats

ADLS supports two URI formats:

### Simple Format
```
az://container/path/to/file.parquet
```

### ABFS Format (with account)
```
abfs://container@account.dfs.core.windows.net/path/to/file.parquet
```

Both formats are supported by Flowfile.

## Supported File Formats

- **Parquet**: Columnar format, excellent compression
- **CSV**: Text format with configurable delimiter
- **JSON**: NDJSON (newline-delimited JSON)
- **Delta Lake**: ACID transactions, time travel

## Environment Variables

You can customize Azurite settings:

```bash
export TEST_AZURITE_HOST=localhost
export TEST_AZURITE_BLOB_PORT=10000
export TEST_AZURITE_ACCOUNT_NAME=devstoreaccount1
export KEEP_AZURITE_RUNNING=true  # Keep container after tests
```

## Troubleshooting

### Connection Refused

If you get connection errors:
1. Verify Azurite is running: `docker ps | grep azurite`
2. Check port 10000 is available: `lsof -i :10000`
3. Restart Azurite: `poetry run stop_azurite && poetry run start_azurite`

### SSL Verification Errors

For local Azurite testing:
- Always set `verify_ssl=False` in connections
- Or uncheck "Verify SSL" in the UI

### Container Not Found

Azurite uses the account name in the endpoint:
- Correct: `http://localhost:10000/devstoreaccount1`
- Wrong: `http://localhost:10000`

### Authentication Errors

Double-check the account key has no line breaks or extra spaces.

## Production ADLS Setup

For real Azure Storage accounts:

1. **Create Storage Account** in Azure Portal
2. **Get Credentials**:
   - Access Key: Storage Account → Access Keys
   - Service Principal: Azure AD → App Registrations
   - SAS Token: Storage Account → Shared access signature
3. **Grant Permissions**:
   - Storage Blob Data Contributor role for service principals
4. **Create Connection** in Flowfile with production credentials
5. **Enable SSL**: Always use `verify_ssl=True` for production

## Additional Resources

- [Azure Storage Documentation](https://docs.microsoft.com/en-us/azure/storage/)
- [Azurite Emulator](https://github.com/Azure/Azurite)
- [Polars Azure Support](https://docs.pola.rs/user-guide/io/cloud-storage/)
