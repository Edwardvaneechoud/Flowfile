# Connect to a PostgreSQL database

This tutorial connects Flowfile to a PostgreSQL database, reads a table, enriches and aggregates it, and writes the ranked result back to a new table. The default walkthrough uses a **local PostgreSQL you can spin up in one command**, so the whole flow is reproducible end to end — the same flow runs in Flowfile's CI on every change. [Connecting to Supabase](#connecting-to-supabase) at the end covers a hosted database as a special case.

!!! info "Not in Flowfile Lite"
    Database connectivity requires the full desktop/server build. This tutorial does not apply to the browser-only [Flowfile Lite](../../deployment/lite.md) edition, which has no backend.

![Full flow overview](../../../assets/images/guides/database_connectivity/main_image.png)

*The finished flow: read a table, enrich and aggregate it, rank, and write back.*

**Flow file:** [`database_transform_write.yaml`](https://github.com/edwardvaneechoud/Flowfile/blob/main/docs/assets/flows/database_transform_write.yaml) · [download](../../../assets/flows/database_transform_write.yaml)

## Set up a database to follow along

You need a PostgreSQL server with a table to read. Any server works; the walkthrough uses a sample **movies** table (~4,800 films with budget, revenue, and rating columns).

=== "Local sample (from a checkout)"

    If you have the repository checked out, one command builds a Postgres container preloaded with the movies sample:

    ```bash
    poetry run start_postgres
    ```

    It listens on `localhost:5433` — database `testdb`, user `testuser`, password `testpass`. Stop it later with `poetry run stop_postgres`.

=== "Your own PostgreSQL"

    Point the connection below at any PostgreSQL server you can reach, and substitute your own table and columns in the steps that follow.

## Create a database connection

Save the credentials once so every flow can reference them by name — the flow file never stores the secret.

1. Open the **Connections** page from the left sidebar and select the **Database** tab.
2. Click **Add Connection**.
3. Fill in the fields:
    - **Connection Name**: `analytics-postgres` (the name the flow and Python example below expect)
    - **Database Type**: PostgreSQL
    - **Host**: `localhost` (or your server's host)
    - **Port**: `5433` (the local sample) or `5432` (a standard server)
    - **Database**: `testdb`
    - **Username** / **Password**: `testuser` / `testpass` (or your credentials)
    - **Enable SSL**: check if your server requires it
4. Click **Update Connection** to save.

![Connection overview in Flowfile](../../../assets/images/guides/database_connectivity/db_connection.png)

*A saved PostgreSQL connection on the Connections page.*

See [Connections](../connections.md#database-connections) for a full field reference.

## Build the flow

The flow reads the table, derives a column, filters, aggregates several metrics per language, keeps well-sampled languages, ranks them, and writes the result back.

### 1. Read from the database

1. Drag a **Read from Database** node onto the canvas.
2. Set **Connection Mode** to **reference** and pick `analytics-postgres`.
3. Set **Schema** to `public` and **Table** to `movies`.
4. Click **Validate Settings**, then **Run** and click the node to preview the rows.

![Database read configuration](../../../assets/images/guides/database_connectivity/configure_read_db.png)

*The Read from Database node configured in reference mode.*

![Successful database read run](../../../assets/images/guides/database_connectivity/initial_run.png)

### 2. Enrich, filter, and aggregate

1. Add a **Formula** node to derive `profit` as `[revenue] - [budget]`.
2. Add a **Filter** node keeping only films with real financials: `[budget] > 0 and [revenue] > 0`.
3. Add a **Group by** node keyed on `original_language`, aggregating:
    - count of `title` → `films`
    - mean of `vote_average` → `avg_score`
    - sum of `profit` → `total_profit`
    - median of `profit` → `median_profit`
4. Add a second **Filter** to keep languages with `[films] >= 10`, then a **Sort** on `total_profit` descending.

For per-node settings, see the [node reference](../nodes/transform.md).

![Connected transformation nodes](../../../assets/images/guides/database_connectivity/transformations.png)

*Read → Formula → Filter → Group by → Filter → Sort.*

### 3. Write the result back

1. Drag a **Write to Database** node onto the canvas and connect it to the **Sort** node.
2. In its settings panel:
    - **Connection Mode**: **reference**
    - **Connection**: `analytics-postgres`
    - **Schema**: `public`
    - **Table**: `language_profitability`
    - **If Table Exists**: **Replace** (drop and recreate), **Append**, or **Fail**

![Write to Database configuration](../../../assets/images/guides/database_connectivity/configure_write_db.png)

*The Write to Database node in reference mode.*

## Run it

Click **Run** to execute the whole flow. Flowfile reads `movies`, applies the transformations, and writes the ranked result to `language_profitability`. Query the destination table to confirm — five languages remain, ranked by total profit:

| original_language | films | avg_score | total_profit |
|---|---|---|---|
| en | 3102 | 6.287 | 257,105,728,489 |
| ja | 13 | 7.192 | 853,264,950 |
| zh | 13 | 6.338 | 546,289,508 |
| fr | 25 | 6.824 | 376,482,920 |
| es | 15 | 6.927 | 350,284,666 |

![Result table in the database](../../../assets/images/guides/database_connectivity/result.png)

*The destination table populated with the ranked result.*

## The flow file

The finished flow is committed and runs in CI against the local Postgres sample, so it can't drift from a working pipeline. Download it and open it in Flowfile with **Create → Open**, or inspect it below — it references the connection by name only, so it carries no credentials.

<details markdown="1">
<summary>See the flow YAML</summary>

```yaml
--8<-- "docs/assets/flows/database_transform_write.yaml"
```

</details>

## In Python

The same flow with the [`flowfile` Python API](../../python-api/index.md). Reference-mode database nodes become `ff.read_database()` / `write_database()` calls, so a flow built in the editor and one written in Python are the same graph. This snippet is included from a repository file that runs in CI against the local Postgres sample:

```python
--8<-- "docs/examples/integrations/database_transform_write.py:example"
```

## Connecting to Supabase

[Supabase](https://supabase.com) is hosted PostgreSQL — the only differences from the walkthrough above are the connection details:

- **Host**: your project's pooler host, e.g. `aws-0-eu-central-1.pooler.supabase.com`
- **Port**: `5432`
- **Database**: `postgres`
- **Enable SSL**: on (Supabase requires it)

Load a table (the Table Editor has a CSV import), then use it as the source in the read step. Everything downstream — transform, aggregate, write back — is identical.

## Next steps

- Reference the same connection in other flows — no need to re-enter credentials.
- Export the flow to Python with the [Code Generator](code-generator.md).
- Schedule the flow to refresh on a timer — see [Schedules](../catalog/schedules.md).

## Related documentation

- [Connections](../connections.md) — saved database, cloud, and Kafka connections
- [Input Nodes: Database Reader](../nodes/input.md#database-reader)
- [Output Nodes: Database Writer](../nodes/output.md#database-writer)
- [Secrets](../catalog/secrets.md) — how credentials are encrypted
