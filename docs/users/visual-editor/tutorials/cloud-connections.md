# Connect to AWS S3 and S3-compatible storage

This guide walks you through creating an AWS S3 (or S3-compatible, such as MinIO) connection in Flowfile so you can read and write cloud data from your flows.

!!! info "Not in Flowfile Lite"
    Cloud storage connections require the full desktop/server build. This guide does not apply to the browser-only [Flowfile Lite](../../deployment/lite.md) edition, which has no backend.

## Overview

A cloud storage connection stores your AWS credentials and configuration under a name, which you reference from reader and writer nodes across workflows.

## Create an S3 connection

**1. Open the dialog.** In the left sidebar, open **Settings → Connections → All connections**, select the **Cloud Storage** tab and click **"+ Add Connection"**.

![create_new_cloud_storage](../../../assets/images/guides/create_cloud_connection/add_cloud_connection.png)

**2. Configure the connection.**

#### Basic Settings

| Field | Description |
|-------|-------------|
| **Connection Name** | A unique identifier for this connection (e.g., `my_s3_storage`) |
| **Storage Type** | Select **AWS S3** |

#### Authentication Methods

Choose one of the following authentication methods:

##### Access Key
- **AWS Access Key ID**: Your AWS access key (e.g., `AKIAIOSFODNN7EXAMPLE`)
- **AWS Secret Access Key**: Your AWS secret access key
- **AWS Region**: The AWS region where your S3 buckets are located (e.g., `us-east-1`)

##### AWS CLI
- Uses credentials from your local AWS CLI configuration
- **AWS Region**: The AWS region where your S3 buckets are located


#### Advanced Settings (Optional)

| Field | Description |
|-------|-------------|
| **Custom Endpoint URL** | For S3-compatible services (e.g., MinIO) |
| **Allow Unsafe HTTP** | Enable for non-HTTPS endpoints, such as a local MinIO server |
| **Verify SSL** | Disable only for testing with self-signed certificates |

**3. Save.** Click **"Create Connection"**.

## Using S3 Connections in Workflows

Once created, your S3 connection will appear in the Cloud Storage Reader and Writer node's connection dropdown.

1. Add a **Cloud Storage Reader** node to your workflow
2. Select your connection from the dropdown
3. Click **Browse** to navigate the bucket and pick a file or folder, or type the S3 path
   yourself (e.g., `s3://my-bucket/data/file.csv`) — always the full URI, including `s3://`
4. Configure file format options
5. Run your workflow

Picking a file sets the scan mode to *Single File*; picking a folder sets it to *Directory*.
If the connection's credentials aren't allowed to list buckets, the browser asks for a bucket
name instead — everything below it still browses normally.

## In Python

The same connection, created and used from code — this example is tested against a real S3-compatible service on every commit:

```python
--8<-- "docs/examples/integrations/cloud_storage_s3.py:example"
```

See [Cloud Connections in Python](../../python-api/reference/cloud-connections.md) for all fields and auth methods.
