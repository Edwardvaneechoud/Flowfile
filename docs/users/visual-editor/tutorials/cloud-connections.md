# Manage S3 Connections

This guide walks you through creating AWS S3 connections in Flowfile to access your cloud data.

## Overview

Cloud storage connections securely store your AWS credentials and configuration, allowing you to reuse them across multiple workflows without re-entering credentials.

## Steps to Create an S3 Connection

### 1. Access Cloud Storage Connections

Click the Cloud icon in the left sidebar to access the Cloud Storage Connections page.

<details markdown="1">
<summary>Screenshot: Cloud Storage Connections Page</summary>

![Cloud storage connection page](../../../assets/images/guides/create_cloud_connection/cloud_storage_connection_page.png)


</details>

### 2. Add New Connection

Click the **"+ Add Connection"** button to open the connection configuration dialog.

<details markdown="1">
<summary>Screenshot: Add Connection Dialog</summary>

![create_new_cloud_storage](../../../assets/images/guides/create_cloud_connection/add_cloud_connection.png)

</details>

### 3. Configure Connection Settings

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
| **Allow Unsafe HTML** | Enable if your S3 data contains HTML content |
| **Verify SSL** | Disable only for testing with self-signed certificates |

### 4. Save Connection

Click **"Create Connection"** to save your configuration.

## Using S3 Connections in Workflows

Once created, your S3 connection will appear in the Cloud Storage Reader and Writer node's connection dropdown. Simply:

1. Add a **Cloud Storage Reader** node to your workflow
2. Select your connection from the dropdown
3. Enter the S3 path (e.g., `s3://my-bucket/data/file.csv`)
4. Configure file format options
5. Run your workflow
