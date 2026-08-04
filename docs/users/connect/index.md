# Connect your data

Flowfile reads from and writes to databases, object stores, Kafka topics, REST APIs, and Google Analytics. This section is the map of every source Flowfile can read from and every sink it can write to, and where each one is configured. Start here to find the right connector, then follow the link to its setup page.

Flowfile connects to external systems in two ways. In the visual editor you add a **reader** or **writer** node and point it at a saved connection. In the [Python API](../python-api/index.md) you call the matching `ff.*` function. Both paths go through the same stored connections, so a connection you save once in the UI is usable from code, and vice versa.

## Connector matrix

Each connector below is read-only, write-only, or both. "Where configured" names the tab on the **Connections** page (or the node that carries inline settings) where you set it up.

| Connector | Reads | Writes | Where configured | Setup |
|---|---|---|---|---|
| PostgreSQL / MySQL / SQLite / DuckDB / SQL Server | yes | yes | Connections → Database | [Databases](../visual-editor/tutorials/database-connectivity.md) |
| Cloud storage (S3 / ADLS / GCS) | yes | yes | Connections → Cloud Storage | [Cloud storage](../visual-editor/tutorials/cloud-connections.md) |
| Kafka / Redpanda | yes | no | Connections → Kafka | [Kafka](kafka.md) |
| REST API | yes | no | REST API Reader node (inline) | [REST APIs](apis.md#rest-apis) |
| Google Analytics 4 | yes | no | Connections → Google Analytics | [Google Analytics](apis.md#google-analytics) |
| Data catalog (Delta Lake) | yes | yes | Catalog Reader / Writer nodes | [Catalog](../visual-editor/catalog/index.md) |
| Saved secrets & connections | — | — | Connections page | [Connections](../visual-editor/connections.md) · [Secrets](../visual-editor/catalog/secrets.md) |

!!! info "Not in Flowfile Lite"
    Saved connections and the secrets that back them require the full desktop or server build. The browser-only [Flowfile Lite](../deployment/lite.md) edition has no backend, so none of the connectors above are available there — Lite works with files you load directly in the browser.

## Databases

Typed connections to **PostgreSQL**, **MySQL**, **SQLite**, **DuckDB**, and **SQL Server**. Both directions are supported: the Database Reader node (`ff.read_database`) runs a query or reads a whole table, and the Database Writer node (`ff.write_database`) writes a frame back. Credentials are stored encrypted and referenced by name.

See [Databases](../visual-editor/tutorials/database-connectivity.md) for the connection form and worked reader/writer examples.

## Cloud storage

Read and write **Amazon S3**, **Azure Data Lake Storage (ADLS)**, and **Google Cloud Storage (GCS)**. Eight authentication methods are available (`access_key`, `iam_role`, `service_principal`, `managed_identity`, `sas_token`, `aws-cli`, `env_vars`, and `service_account`), so a connection can use stored keys or delegate to the ambient cloud credentials. Read formats are CSV, Parquet, JSON, Delta, and Iceberg; write formats are CSV, Parquet, JSON, and Delta.

See [Cloud storage](../visual-editor/tutorials/cloud-connections.md) for provider-specific setup.

## Kafka

Consume JSON messages from a **Kafka** or **Redpanda** topic through the Kafka Source node (`ff.read_kafka`). Offsets are tracked broker-side by consumer group, so a flow that runs on a schedule reads only what arrived since the last run. Kafka is **read-only** — there is no Kafka writer node.

See [Kafka](kafka.md) for the connection form, security options, and a runnable example.

## REST APIs and Google Analytics

The **REST API Reader** node (`ff.read_api`) fetches JSON from an HTTP endpoint with configurable authentication and pagination. The **Google Analytics** reader pulls GA4 property reports through a stored OAuth or service-account connection. Both are read-only.

See [REST APIs and Google Analytics](apis.md).

## Catalog

The [data catalog](../visual-editor/catalog/index.md) is Flowfile's own storage layer, backed by Delta Lake. The Catalog Reader node loads a registered table into a flow and the Catalog Writer node persists a result back as a versioned table. Unlike the external connectors above, the catalog is managed inside Flowfile — see the [Catalog](../visual-editor/catalog/index.md) section for details.
