# Feature 6: Extended Connectors

## Motivation

Flowfile currently supports file-based I/O (CSV, Parquet, Excel, JSON), cloud storage (S3, ADLS, GCS), and generic database read/write. However, real-world ETL pipelines need robust, optimized connectors for specific database systems with features like:

- Connection pooling and persistent sessions.
- Schema browsing (list databases, schemas, tables, columns).
- Database-specific optimizations (pushdown predicates, partitioned reads).
- Incremental loading (CDC, watermark-based reads).
- Bulk loading (COPY commands, staging tables).

This feature extends the connector ecosystem with first-class support for PostgreSQL, MySQL, BigQuery, and Snowflake.

## Current State

- **`database_reader` node**: Reads from databases using generic SQL via `connectorx` or `sqlalchemy`. Configuration is a connection string + SQL query. No schema browsing, no connection management.
- **`database_writer` node**: Writes DataFrames to database tables. Generic approach, no bulk loading optimization.
- **Cloud storage connectors**: S3, ADLS, GCS with 7 authentication methods. Well-structured with `AuthSettingsInput`, `FullCloudStorageConnection`, etc. in `cloud_storage_schemas.py`. These serve as the model for database connector architecture.
- **Connection management**: Cloud connections are registered and encrypted per-user via the secrets system. Database connections are ad-hoc (connection string in node settings).
- **Node templates** (`nodes.py`): `database_reader` and `database_writer` exist as standard nodes.

## Proposed Design

### Connection Registry

Model database connections similarly to cloud storage connections — registered, encrypted, and reusable.

**New models** (`schemas/database_schemas.py`):

```python
class DatabaseType(str, Enum):
    POSTGRESQL = "postgresql"
    MYSQL = "mysql"
    BIGQUERY = "bigquery"
    SNOWFLAKE = "snowflake"
    MSSQL = "mssql"
    SQLITE = "sqlite"

class DatabaseConnectionSettings(BaseModel):
    name: str
    database_type: DatabaseType
    host: str
    port: int
    database: str
    username: str
    password: SecretStr
    schema: str | None = None
    extra_params: dict[str, str] = {}
    # BigQuery-specific
    project_id: str | None = None
    credentials_json: SecretStr | None = None
    # Snowflake-specific
    account: str | None = None
    warehouse: str | None = None
    role: str | None = None

class DatabaseConnectionRef(BaseModel):
    """Reference to a registered connection (stored encrypted in catalog)."""
    connection_id: int
    database_type: DatabaseType
```

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

**Updated `NodeDatabaseReader`**:

```python
class NodeDatabaseReader(NodeBase):
    connection_ref: DatabaseConnectionRef    # registered connection
    # Option 1: table-based read
    table_name: str | None = None
    schema_name: str | None = None
    selected_columns: list[str] | None = None
    filter_expression: str | None = None     # WHERE clause
    # Option 2: SQL query read
    sql_query: str | None = None
    # Optimization
    partition_column: str | None = None
    partition_count: int = 4
    # Incremental
    incremental: IncrementalReadSettings | None = None
```

**Updated `NodeDatabaseWriter`**:

```python
class NodeDatabaseWriter(NodeSingleInput):
    connection_ref: DatabaseConnectionRef
    table_name: str
    schema_name: str | None = None
    write_mode: Literal["append", "overwrite", "upsert"] = "append"
    upsert_keys: list[str] | None = None    # for upsert mode
    bulk_load: bool = True                   # use DB-specific bulk loading
    batch_size: int = 10000
    create_table_if_not_exists: bool = True
```

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
| `flowfile_core/flowfile_core/schemas/database_schemas.py` | NEW: database connection, schema browser models |
| `flowfile_core/flowfile_core/schemas/input_schema.py` | Update `NodeDatabaseReader`, `NodeDatabaseWriter` |
| `flowfile_core/flowfile_core/configs/node_store/nodes.py` | Update database node templates |
| `flowfile_core/flowfile_core/main.py` | Add connection management and schema browser endpoints |
| `flowfile_core/flowfile_core/catalog/` | Store database connections as catalog entities |
| `flowfile_worker/` | Implement DB-specific read/write strategies |
| `flowfile_frontend/` | Connection manager, schema browser, updated node configs |
| `pyproject.toml` | Add optional deps: `psycopg2`, `mysql-connector-python`, `google-cloud-bigquery`, `snowflake-connector-python` |

## Open Questions

1. **Connection pooling**: Should the worker maintain persistent connection pools? This complicates the stateless worker model. Alternative: create connections per-request but cache credentials.
2. **Pushdown predicates**: How much of Polars' filter expressions should be pushed down to SQL? Start with simple column comparisons, expand later.
3. **Type mapping**: Database types don't map 1:1 to Polars types. Where should the mapping logic live? Proposed: a `TypeMapper` per database type in the worker.
4. **OAuth for BigQuery/Snowflake**: Both support OAuth flows. How to integrate with the existing auth model?
5. **Dependency management**: Database drivers are heavy. Should they be optional extras (`pip install flowfile[postgresql]`) or bundled?
