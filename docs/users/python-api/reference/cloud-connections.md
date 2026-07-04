# Cloud Connections in Python

A cloud storage connection stores a provider, its credentials (encrypted, scoped to your user), and a name to reference them by. Code and the UI share one connection store: a connection created here appears in the Cloud Storage Reader/Writer nodes' dropdowns, and one created in the [UI](../../visual-editor/tutorials/cloud-connections.md) is usable from `ff.*` functions by name.

## Creating Connections

```python
import flowfile as ff
from pydantic import SecretStr

ff.create_cloud_storage_connection(
    ff.FullCloudStorageConnection(
        connection_name="data-lake",
        storage_type="s3",
        auth_method="access_key",
        aws_region="us-east-1",
        aws_access_key_id="AKIAIOSFODNN7EXAMPLE",
        aws_secret_access_key=SecretStr("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
    )
)
```

## Connection Types

### S3 Connection (Access Key)

```python
ff.FullCloudStorageConnection(
    connection_name="my-s3",
    storage_type="s3",
    auth_method="access_key",
    aws_region="us-east-1",
    aws_access_key_id="AKIAIOSFODNN7EXAMPLE",
    aws_secret_access_key=SecretStr("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"),
    endpoint_url="https://s3.amazonaws.com"  # Optional, for custom endpoints
)
```

```python
ff.FullCloudStorageConnection(
    connection_name="my-s3-cli",
    storage_type="s3",
    auth_method="aws-cli",  # Uses local AWS CLI credentials (note the hyphen)
    aws_region="us-east-1"
)
```

!!! warning "The CLI auth literal is `aws-cli` (hyphen)"
    `auth_method="aws_cli"` (underscore) raises a Pydantic `ValidationError`.

### Connection fields

`FullCloudStorageConnection` covers all three cloud backends. `storage_type` selects one; fill only the fields for that provider.

| Field group | Fields | Used by |
|---|---|---|
| Identity | `connection_name`, `storage_type` (`"s3"` / `"adls"` / `"gcs"`), `auth_method` | all |
| AWS S3 | `aws_region`, `aws_access_key_id`, `aws_secret_access_key`, `aws_role_arn`, `aws_session_token`, `aws_allow_unsafe_html`, `endpoint_url` | `s3` |
| Azure ADLS | `azure_account_name`, `azure_account_key`, `azure_tenant_id`, `azure_client_id`, `azure_client_secret`, `azure_sas_token` | `adls` |
| Google GCS | `gcs_service_account_key`, `gcs_project_id` | `gcs` |

`auth_method` accepts `access_key`, `iam_role`, `service_principal`, `managed_identity`, `sas_token`, `aws-cli`, `env_vars`, and `service_account` — pick the one your `storage_type` supports. Secret fields (`aws_secret_access_key`, `azure_account_key`, `gcs_service_account_key`, …) take a `SecretStr`.

!!! note "`aws_allow_unsafe_html`"
    This flag permits plain-HTTP (non-TLS) S3 endpoints. Set it to `True` only for local or dev stacks such as MinIO reached over `http://`; leave it unset for real AWS.

### Round-trip example

The tested integration example creates a connection, writes Parquet to S3, and reads it back:

```python
--8<-- "docs/examples/integrations/cloud_storage_s3.py:example"
```

!!! info "S3-compatible local stacks vs plain S3"
    The `endpoint_url`, `aws_allow_unsafe_html`, and inline keys in that example wire an S3-compatible local stack (MinIO). Against real AWS S3, a connection needs only `connection_name`, `storage_type`, `auth_method`, `aws_region`, and credentials — no `endpoint_url` and no unsafe-HTML flag.

## Managing Connections

### Create If Not Exists

Safely create connections without duplicates:

```python
# Only creates if "data-lake" doesn't exist
ff.create_cloud_storage_connection_if_not_exists(
    ff.FullCloudStorageConnection(
        connection_name="data-lake",
        storage_type="s3",
        # ... other parameters
    )
)
```

### List All Connections

```python
# Get all available connections for current user
connections = ff.get_all_available_cloud_storage_connections()

for conn in connections:
    print(f"Name: {conn.connection_name}")
    print(f"Type: {conn.storage_type}")
    print(f"Region: {conn.aws_region}")
```

### Delete Connection

```python
# Remove a connection by name
ff.del_cloud_storage_connection("old-connection")
```

## Using Connections

Once created, use connections in read/write operations:

```python
# Reading with connection
df = ff.scan_parquet_from_cloud_storage(
    "s3://bucket/data.parquet",
    connection_name="data-lake"  # Use the connection name
)

# Writing with connection
df.write_parquet_to_cloud_storage(
    "s3://bucket/output.parquet",
    connection_name="data-lake"
)
```

## Troubleshooting

### Common Issues

| Issue | Solution |
|-------|----------|
| "Connection not found" | Ensure connection exists with `get_all_available_cloud_storage_connections()` |
| "Access denied" | Verify credentials and permissions |
| "Invalid endpoint" | Check `endpoint_url` for custom S3 services |
| Cannot reach a plain-HTTP endpoint | Set `aws_allow_unsafe_html=True` — for local/dev endpoints only |

### Debug Connection

```python
# List all connections to verify
conns = ff.get_all_available_cloud_storage_connections()
print(f"Available connections: {[c.connection_name for c in conns]}")

# Check specific connection details
my_conn = next((c for c in conns if c.connection_name == "data-lake"), None)
if my_conn:
    print(f"Storage type: {my_conn.storage_type}")
    print(f"Auth method: {my_conn.auth_method}")
    print(f"Region: {my_conn.aws_region}")
```

---
[← Previous: Joins](joins.md) | [Next: Visual UI Integration →](visual-ui.md)
