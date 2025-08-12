# Cloud Connection Management

Flowfile provides secure, centralized management for cloud storage connections. Connections can be created through code or the UI—both store credentials in an encrypted database. 
On this page we will cover how to create and manage them in Python. If you want to learn how to create them in the UI, 
check out the [UI guide](../../visual-editor/tutorials/cloud-connections.md).

## Creating Connections

### Code Approach

```python
import flowfile as ff
from pydantic import SecretStr

# Create a new S3 connection
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

### Visual Editor Integration

Connections created through code are immediately available in the Flowfile visual editor:

```python
# Create connection in code
ff.create_cloud_storage_connection(
    ff.FullCloudStorageConnection(
        connection_name="data-lake",
        # ... parameters
    )
)

# This connection now appears in:
# - Cloud Storage Reader node's connection dropdown
# - Cloud Storage Writer node's connection dropdown
# - Any other nodes that use cloud connections
```

!!! info "Seamless Integration"
    There's no difference between connections created via code or UI. Both are stored in the same encrypted database and are instantly available across all interfaces.

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
    auth_method="aws_cli",  # Uses local AWS CLI credentials
    aws_region="us-east-1"
)
```

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

## Security Features

### Credential Encryption

- All credentials are encrypted before storage
- Secrets never appear in logs or error messages
- Use `SecretStr` wrapper for sensitive values

### User Isolation

- Connections are scoped to the current user
- Each user manages their own connections
- No cross-user credential access


## Troubleshooting

### Common Issues

| Issue | Solution |
|-------|----------|
| "Connection not found" | Ensure connection exists with `get_all_available_cloud_storage_connections()` |
| "Access denied" | Verify credentials and permissions |
| "Invalid endpoint" | Check `endpoint_url` for custom S3 services |
| "SSL verification failed" | Use `aws_allow_unsafe_html=True` for local/dev endpoints only |

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

!!! tip "UI Integration"
    All connections created via code are immediately available in the UI's connection dropdown when configuring nodes.

---
[← Previous: Joins](joins.md) | [Next: visual Ui →](visual-ui.md)
