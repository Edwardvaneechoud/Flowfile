# Feature 6: Extended Connectors

## Motivation

Flowfile already has broad database connectivity — the `database_reader` node can connect to any database that ConnectorX supports (PostgreSQL, MySQL, MariaDB, SQLite, MSSQL, Oracle, and more) via `pl.read_database_uri()`, with both table-based reads and custom SQL queries. The `database_writer` writes to any SQLAlchemy-compatible database. A comprehensive SQL type mapper (100+ type mappings) converts database types to Polars types, and connections can be stored as references with encrypted credentials via the secrets system.

What's missing is not basic connectivity, but **database-specific optimizations and cloud-native data warehouses**:

- **Partitioned reads**: ConnectorX supports `PARTITION ON` for parallel reads, but this is not exposed in the node settings.
- **Bulk loading**: Writes use generic `df.write_database()` (row-by-row INSERT). Database-specific bulk methods (PostgreSQL `COPY`, MySQL `LOAD DATA`) would be 10-100x faster.
- **Cloud data warehouses**: BigQuery and Snowflake use fundamentally different connection models (OAuth, project IDs, warehouses, stages) that don't fit the current `host:port/database` URI pattern.
- **Incremental loading**: No built-in support for watermark-based reads or change data capture.
- **Schema browsing in UI**: The backend can inspect schemas via SQLAlchemy `inspect()`, but the frontend doesn't expose a visual schema browser in the node configuration panel.

## Current State

- **`database_reader` node** (`input_schema.py`): `NodeDatabaseReader` with `DatabaseSettings` supporting:
  - `connection_mode`: `"inline"` (direct settings) or `"reference"` (stored connection name)
  - `DatabaseConnection` model: `database_type` (free-form string, default `"postgresql"`), `username`, `password_ref` (SecretRef), `host`, `port`, `database`, optional `url` override
  - `query_mode`: `"query"` (custom SQL), `"table"` (full table read), or `"reference"`
  - `schema_name` and `table_name` fields
  - SQL validation: strict allow-list for SELECT/WITH, blocks DDL/DML
- **`database_writer` node** (`input_schema.py`): `NodeDatabaseWriter` with the same `DatabaseSettings`, writing via `df.write_database()`.
- **ConnectorX** (`connectorx ^0.4.2`): Primary read driver, supports PostgreSQL, MySQL, MariaDB, SQLite, MSSQL, Oracle. Used via `pl.read_database_uri()`.
- **SQLAlchemy** (`sqlalchemy ^2.0.27`): Used for writes, schema inspection (`inspect()`), and type mapping.
- **SQL type mapper** (`sql_source/utils.py`): 100+ SQLAlchemy-to-Polars type mappings covering Int64, Float64, Utf8, Date, Datetime, Boolean, Decimal, Binary, etc.
- **URI construction** (`sql_source/utils.py`): Builds `{database_type}://{username}:{password}@{host}:{port}/{database}` from `DatabaseConnection` fields.
- **Cloud storage connectors**: S3, ADLS, GCS with 7 authentication methods. Well-structured with `AuthSettingsInput`, `FullCloudStorageConnection`, etc. in `cloud_storage_schemas.py`. Cloud read supports CSV, Parquet, JSON, Delta, and Iceberg formats.
- **Connection management**: Cloud connections are registered and encrypted per-user via the secrets system. Database connections support both inline and reference modes.

## Proposed Design

### Extended Connection Model for Cloud Data Warehouses

The existing `DatabaseConnection` model uses a standard `host:port/database` URI pattern that works well for PostgreSQL, MySQL, MSSQL, etc. Cloud data warehouses (BigQuery, Snowflake) need additional fields that don't fit this pattern.

**Extend `DatabaseConnection`** (`input_schema.py`):

```python
class DatabaseConnection(BaseModel):
    database_type: str = "postgresql"
    username: str | None = None
    password_ref: SecretRef | None = None
    host: str | None = None
    port: int | None = None
    database: str | None = None
    url: str | None = None
    # NEW: BigQuery-specific
    project_id: str | None = None
    credentials_json_ref: SecretRef | None = None   # service account JSON
    # NEW: Snowflake-specific
    account: str | None = None                       # e.g., "xy12345.us-east-1"
    warehouse: str | None = None
    role: str | None = None
```

BigQuery and Snowflake use their own connector libraries that build connection strings differently from the standard URI pattern. The `url` override field can accommodate this, but explicit fields improve the UI experience.

### Schema Browser

**New API endpoints**:

```python
@router.get("/connections/{id}/databases")
async def list_databases(id: int) -> list[str]:
    ...

@router.get("/connections/{id}/schemas")
async def list_schemas(id: int, database: str | None = None) -> list[str]:
    ...

@router.get("/connections/{id}/tables")
async def list_tables(id: int, schema: str | None = None) -> list[TableInfo]:
    ...

@router.get("/connections/{id}/columns")
async def list_columns(id: int, table: str, schema: str | None = None) -> list[ColumnInfo]:
    ...

@router.get("/connections/{id}/preview")
async def preview_table(id: int, table: str, limit: int = 100) -> PreviewResult:
    ...
```

**Schema browser models**:

```python
class TableInfo(BaseModel):
    name: str
    schema: str
    row_count: int | None       # estimated, from statistics
    size_bytes: int | None
    table_type: str             # "TABLE", "VIEW", "MATERIALIZED_VIEW"

class ColumnInfo(BaseModel):
    name: str
    data_type: str              # database-native type
    polars_type: str            # mapped Polars type
    nullable: bool
    is_primary_key: bool
    default_value: str | None
```

### Database-Specific Optimizations

**PostgreSQL**:
- Read via `connectorx` with partitioned queries for large tables (`PARTITION ON` column).
- Write via `COPY FROM STDIN` for bulk loading (10-100x faster than INSERT).
- Support for `LISTEN/NOTIFY` for CDC use cases (future).

**MySQL**:
- Read via `connectorx` with partition pushdown.
- Write via `LOAD DATA LOCAL INFILE` for bulk loading.
- Support for binary log CDC (future).

**BigQuery**:
- Read via `google-cloud-bigquery` with Storage API for large result sets.
- Write via load jobs (Parquet staging → BigQuery load).
- Support for partitioned/clustered table writes.

**Snowflake**:
- Read via `snowflake-connector-python` with result set caching.
- Write via internal stage → COPY INTO for bulk loading.
- Support for warehouse auto-scaling hints.

### Incremental Loading

**Watermark-based reads**:

```python
class IncrementalReadSettings(BaseModel):
    enabled: bool = False
    watermark_column: str           # e.g., "updated_at"
    watermark_type: Literal["timestamp", "integer"]
    last_watermark: Any | None      # stored in catalog metadata
```

The reader tracks the high-water mark from the last read. On subsequent runs, it adds a `WHERE watermark_column > last_watermark` predicate. The watermark is stored in the catalog as table metadata.

### Enhanced Node Settings

**Extend `DatabaseSettings`** (used by both reader and writer):

```python
class DatabaseSettings(BaseModel):
    # ... existing fields (connection_mode, database_connection, query_mode, etc.) ...

    # NEW: Performance optimization
    partition_column: str | None = None      # ConnectorX PARTITION ON column
    partition_count: int = 4                 # number of parallel read partitions
    selected_columns: list[str] | None = None  # column pruning (SELECT specific cols)

    # NEW: Incremental loading
    incremental: IncrementalReadSettings | None = None

    # NEW: Write optimization
    bulk_load: bool = True                   # use DB-specific bulk loading (COPY, LOAD DATA)
    write_mode: Literal["append", "replace", "fail", "upsert"] = "append"
    upsert_keys: list[str] | None = None     # for upsert mode
    batch_size: int = 10000
```

These extend the existing `DatabaseSettings` model rather than replacing `NodeDatabaseReader`/`NodeDatabaseWriter`, preserving backward compatibility with existing flows.

### Frontend Changes

**Connection manager** (new page or modal):
- List registered database connections.
- Add/edit/test connections with database-specific forms.
- Test connectivity button with status indicator.

**Schema browser** (in database reader node config):
- Tree view: connection → database → schema → table → columns.
- Click table to auto-fill node settings.
- Column checkboxes for `selected_columns`.
- Preview button for sample data.

**Database writer config**:
- Table selector from schema browser.
- Write mode dropdown with explanations.
- Column mapping grid for upsert keys.

## Key Files to Modify

| File | Change |
|------|--------|
| `flowfile_core/flowfile_core/schemas/input_schema.py` | Extend `DatabaseSettings`, `DatabaseConnection` with new fields |
| `flowfile_core/flowfile_core/flowfile/sources/external_sources/sql_source/sql_source.py` | Add BigQuery/Snowflake connection builders, bulk write strategies |
| `flowfile_core/flowfile_core/flowfile/sources/external_sources/sql_source/utils.py` | Add BigQuery/Snowflake type mappings, URI construction |
| `flowfile_core/flowfile_core/configs/node_store/nodes.py` | Update database node templates |
| `flowfile_core/flowfile_core/main.py` | Add connection management and schema browser endpoints |
| `flowfile_core/flowfile_core/catalog/` | Store database connections as catalog entities |
| `flowfile_worker/` | Implement DB-specific read/write strategies |
| `flowfile_frontend/` | Connection manager, schema browser, updated node configs |
| `pyproject.toml` | Add optional deps: `psycopg2`, `mysql-connector-python`, `google-cloud-bigquery`, `snowflake-connector-python` |

## Open Questions

1. **Connection pooling**: Should the worker maintain persistent connection pools? This complicates the stateless worker model. Alternative: create connections per-request but cache credentials.
2. **Pushdown predicates**: How much of Polars' filter expressions should be pushed down to SQL? Start with simple column comparisons, expand later.
3. **BigQuery/Snowflake type mapping**: The existing SQL type mapper covers standard SQL types well. BigQuery (STRUCT, ARRAY, GEOGRAPHY) and Snowflake (VARIANT, OBJECT) have proprietary types that need new mappings.
4. **OAuth for BigQuery/Snowflake**: Both support OAuth flows. How to integrate with the existing secrets/auth model?
5. **Dependency management**: BigQuery and Snowflake drivers are heavy. Should they be optional extras (`pip install flowfile[bigquery]`) or bundled? ConnectorX already covers PostgreSQL/MySQL/MSSQL without extra drivers.
6. **ConnectorX vs native drivers**: ConnectorX already supports PostgreSQL, MySQL, etc. For bulk write operations, we need native drivers (psycopg2, mysql-connector). Should reads also use native drivers for consistency, or keep ConnectorX for read performance?
