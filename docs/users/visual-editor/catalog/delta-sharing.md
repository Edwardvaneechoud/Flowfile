# Delta Sharing

Delta Sharing lets other tools read catalog tables straight from Flowfile, without copying files or opening up your storage. Flowfile speaks the open [Delta Sharing protocol](https://github.com/delta-io/delta-sharing), so pandas, Spark, Power BI, Tableau, DuckDB, Databricks and anything else with a Delta Sharing connector can read a table you share. This page covers what can be shared, creating a share, giving someone access with a profile file, and reading it from other tools.

## How it works

- A **share** is a named set of catalog tables. Inside a share each table has a schema and a table name, so a recipient addresses it as `share.schema.table`.
- A **recipient** is an [API consumer](flow-api.md#api-keys-and-consumers): the same service accounts whose keys call published flows. Grant a consumer a share, and every key it holds can read that share.
- A **profile file** (`<share>.share`) holds the server's address and a new key for the consumer. Recipients point their tool at the file.
- When a tool reads a table, Flowfile answers with the table's schema and a short-lived download link for each data file. The tool downloads the Parquet files itself. Flowfile reads only the table's Delta log, never the rows.

Recipients only read. They see the tables of the shares their consumer is granted and nothing else.

## What can be shared

| Table | Shared |
|-------|--------|
| Delta table in a catalog on local disk (such as **General**) | Yes. Flowfile serves the files itself through signed links |
| Delta table in a catalog on S3 or an S3-compatible store | Yes, when the catalog's storage connection holds an access key (presigned S3 links) |
| Delta table in a catalog on Azure Data Lake | Yes, when the connection uses an account key or a SAS token |
| Table on Google Cloud Storage | Not yet |
| Catalog on a connection that signs in as the server (`env_vars`, `managed_identity`) | No: Flowfile never signs links with the server's own credentials |
| Virtual table | No. Write it to a table first |
| Table in a catalog kept elsewhere (Iceberg) | No |
| Table with deletion vectors or column mapping | No: the protocol's Parquet format cannot express them |

You can add a table only if you **manage** it. A table that is deleted, or that the share's owner no longer manages, disappears from the share at once. Recipients then get "does not exist".

## Create a share

1. Catalog → **APIs** tab → **Delta Sharing**.
2. Type a name under **New share** and press **Create**. The name is part of the recipient's URLs, so use letters, digits, `_` and `-`.
3. In the share's drawer, pick a table under **Tables** and press **Add**. It is shared as `<schema>.<table>`, using the names it has in the catalog.
4. **History** (on by default) lets recipients read earlier versions of the table. Turn it off to serve only the current version.

## Give someone access

1. Create a consumer under **APIs → Consumers** if you do not have one, for example `partner-bi`.
2. In the share's drawer, pick the consumer under **Recipients** and press **Grant**.
3. Press **Profile file** next to the consumer. Check the endpoint, optionally set an expiry in days, then press **Create profile**.
4. Press **Save &lt;share&gt;.share** and send the file to the recipient.

The key inside the file is shown only once. Anyone holding the file can read every share the consumer is granted, so treat it like a password. To revoke access, disable or delete the key or the consumer under **Consumers**, or revoke the share in the share's drawer. Revoking takes effect on the next request.

### The endpoint

The profile's endpoint is the address recipients reach Flowfile at, followed by `/delta-sharing`. By default it is the address your browser uses. On the desktop app that address is `127.0.0.1`, which only works on your own machine. On a server, give the address other machines use, or set it once for everyone:

```bash
FLOWFILE_DELTA_SHARING_ENDPOINT=https://flowfile.example.com/api/delta-sharing
```

With the bundled Docker setup the protocol lives under the frontend's `/api/` path, for example `https://flowfile.example.com/api/delta-sharing`. Serve it over HTTPS whenever recipients connect over a network you do not control: the bearer token travels with every request.

## Read a shared table

### pandas and Python

```python
# pip install delta-sharing
import delta_sharing

profile = "retail.share"
print(delta_sharing.SharingClient(profile).list_all_tables())
df = delta_sharing.load_as_pandas(f"{profile}#retail.default.orders")
earlier = delta_sharing.load_as_pandas(f"{profile}#retail.default.orders", version=3)
```

### Spark

```python
df = spark.read.format("deltaSharing").load("retail.share#retail.default.orders")
```

### Power BI, Tableau, Databricks

Choose the **Delta Sharing** connector and give it the endpoint and the bearer token from the profile file.

## What is served

| Request | Supported |
|---------|-----------|
| List shares, schemas and tables | Yes |
| Table version, including the first version after a timestamp | Yes |
| Table metadata (schema, partition columns) | Yes |
| Read the current version | Yes |
| Read an earlier `version` or `timestamp` | Yes, unless the table is shared without history |
| Change data feed (`startingVersion`/`endingVersion`) | Not yet |
| `responseFormat=delta` | Not yet; Flowfile answers in the Parquet format |
| Filter pushdown (`predicateHints`, `limitHint`) | Hints are ignored, so every file of the version is returned |

Download links last one hour by default (`FLOWFILE_DELTA_SHARING_URL_TTL`, in seconds). Clients request new ones as they need them.

## Settings

| Variable | Default | Effect |
|----------|---------|--------|
| `FLOWFILE_DELTA_SHARING` | on | `0`/`false`/`no`/`off` turns Delta Sharing off; every protocol and management route answers 404 |
| `FLOWFILE_DELTA_SHARING_ENDPOINT` | unset | The endpoint written into every profile file and used for download links |
| `FLOWFILE_DELTA_SHARING_URL_TTL` | `3600` | How long a download link stays valid, in seconds (60 seconds to 7 days) |

Shares are not part of a [project](../../projects.md) export: they grant access to consumers, which a project does not hold.
