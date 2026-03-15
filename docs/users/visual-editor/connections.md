# Connections

Save and reuse database and cloud storage credentials across your flows.

Connections store your credentials securely (passwords are encrypted via [Secrets](../secrets.md))
so you can reference them by name in Database Reader, Database Writer, Cloud Storage Reader,
and Cloud Storage Writer nodes without re-entering credentials each time.

---

## Database Connections

### Supported Databases

| Database | Type Key |
|----------|----------|
| **PostgreSQL** | `postgresql` |
| **MySQL** | `mysql` |

### Creating a Database Connection

1. Click the **Database** icon in the left sidebar to open the Database Connection Manager
2. Click **Create New Connection**
3. Fill in the connection fields:

| Field | Description | Example |
|-------|-------------|---------|
| **Connection Name** | Unique identifier for this connection | `prod_postgres` |
| **Database Type** | PostgreSQL or MySQL | `postgresql` |
| **Host** | Database server hostname | `db.example.com` |
| **Port** | Database port | `5432` |
| **Database** | Database name | `analytics` |
| **Username** | Database user | `readonly_user` |
| **Password** | Stored as an encrypted secret | |
| **Enable SSL** | Use SSL for the connection | Recommended for cloud databases |

4. Click **Update Connection** to save

<!-- PLACEHOLDER: Screenshot of the Database Connection Manager page -->
![Database Connection Manager](../../assets/images/guides/connections/database-manager.png)

*The Database Connection Manager showing saved connections*

<!-- PLACEHOLDER: Screenshot of the Create Database Connection form -->
![Create Database Connection](../../assets/images/guides/connections/create-db-connection.png)

*Creating a new PostgreSQL connection*

### Using Database Connections in Flows

In a **Database Reader** or **Database Writer** node:

1. Set **Connection Mode** to **Reference**
2. Select your saved connection from the dropdown
3. Configure schema, table, and query settings

!!! tip "Reference vs Inline Mode"
    **Reference** mode uses a saved connection (recommended). Credentials are encrypted,
    reusable, and supported by the [code generator](tutorials/code-generator.md).

    **Inline** mode lets you enter credentials directly in the node settings. This is convenient for
    quick tests but credentials are not reusable and inline connections cannot be exported to Python code.

---

## Cloud Storage Connections

### Supported Providers

| Provider | Description |
|----------|-------------|
| **AWS S3** | Amazon Simple Storage Service (including S3-compatible services like MinIO) |

!!! note "Coming Soon"
    Azure Data Lake Storage and Google Cloud Storage support are planned for a future release.

### Creating a Cloud Storage Connection

1. Click the **Cloud** icon in the left sidebar
2. Click **Add Connection**
3. Configure the connection:

| Field | Description |
|-------|-------------|
| **Connection Name** | Unique identifier (e.g., `my_s3_storage`) |
| **Storage Type** | AWS S3 |
| **AWS Access Key ID** | Your access key |
| **AWS Secret Access Key** | Stored as encrypted secret |
| **AWS Region** | e.g., `us-east-1` |
| **Custom Endpoint URL** | For S3-compatible services (MinIO, etc.) |
| **Verify SSL** | Disable only for self-signed certificates |
| **Allow Unsafe HTTP** | Enable for non-HTTPS endpoints (e.g., local MinIO) |

4. Click **Create Connection**

<!-- PLACEHOLDER: Screenshot of Cloud Connection Manager -->
![Cloud Connection Manager](../../assets/images/guides/connections/cloud-manager.png)

*The Cloud Storage Connection Manager*

### Using Cloud Connections in Flows

In a **Cloud Storage Reader** or **Cloud Storage Writer** node, select your saved connection from the dropdown.

For a step-by-step tutorial, see [Manage Cloud Storage](tutorials/cloud-connections.md).

---

## Security

- Passwords and secret keys are stored as encrypted [Secrets](../secrets.md) using Fernet encryption
- Connection metadata (host, port, database name) is stored in the local database
- Credentials are decrypted only at runtime when a flow executes
- Each user's connections are isolated (Docker multi-user mode)

---

## Related Documentation

- [Secrets](../secrets.md) — How credential encryption works
- [Input Nodes: Database Reader](nodes/input.md#database-reader) — Reading from databases
- [Output Nodes: Database Writer](nodes/output.md#database-writer) — Writing to databases
- [Tutorial: Connect to PostgreSQL](tutorials/database-connectivity.md)
- [Tutorial: Manage Cloud Storage](tutorials/cloud-connections.md)
