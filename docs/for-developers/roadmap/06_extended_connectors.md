# Feature 6: Extended Connectors

## Motivation

Flowfile currently supports **only PostgreSQL** for database connectivity and **only AWS S3** for cloud storage. While the underlying libraries (ConnectorX, SQLAlchemy, Polars) can theoretically connect to other databases, Flowfile has only implemented, tested, and exposed PostgreSQL in the UI. The frontend database type dropdown literally has one option: `postgresql`.

This feature is about building **real, tested, production-ready support** for additional databases and cloud storage providers:

- **MySQL** — the most widely used open-source database; many ETL workloads need it.
- **ADLS (Azure Data Lake Storage)** — Azure-native cloud storage, required for Azure-centric organizations.
- **GCS (Google Cloud Storage)** — Google Cloud equivalent.
- **BigQuery** — Google's serverless data warehouse, widely used for analytics.
- **Snowflake** — dominant cloud data warehouse, fundamentally different connection model.

Beyond new connectors, existing PostgreSQL support can be enhanced with partitioned reads, bulk loading, and incremental loading.

## Current State

### Database Support: PostgreSQL Only

- **`database_reader` node** (`input_schema.py`): `NodeDatabaseReader` with `DatabaseSettings` supporting:
  - `connection_mode`: `"inline"` (direct settings) or `"reference"` (stored connection name)
  - `DatabaseConnection` model: `database_type` (string, default `"postgresql"`), `username`, `password_ref` (SecretRef), `host`, `port`, `database`, optional `url` override
  - `query_mode`: `"query"` (custom SQL), `"table"` (full table read), or `"reference"`
  - SQL validation: strict allow-list for SELECT/WITH, blocks DDL/DML
- **`database_writer` node** (`input_schema.py`): `NodeDatabaseWriter` with the same `DatabaseSettings`, writing via `df.write_database()`.
- **Tests**: Only PostgreSQL is tested (`test_sql_source.py` uses `postgresql://` connections). Test infrastructure in `test_utils/postgres/` provides Docker-based PostgreSQL via testcontainers.
- **Frontend**: `DatabaseConnectionSettings.vue` has a single `<option value="postgresql">PostgreSQL</option>`.
- **Documentation**: The database tutorial covers only PostgreSQL (via Supabase).
- **Type mapping** (`sql_source/utils.py`): 100+ SQLAlchemy-to-Polars type mappings exist, but only validated against PostgreSQL types.
- **URI construction** (`sql_source/utils.py`): Builds `{database_type}://{username}:{password}@{host}:{port}/{database}`. Works for standard databases but not for BigQuery/Snowflake.

### Cloud Storage Support: AWS S3 Only

- **`cloud_storage_reader`/`cloud_storage_writer` nodes**: Schema definitions exist for S3, ADLS, and GCS (`cloud_storage_schemas.py`) with 7 auth methods and multiple file formats (CSV, Parquet, JSON, Delta, Iceberg).
- **Actual implementation and testing**: Only AWS S3 is implemented and tested. ADLS and GCS connection types are defined in the schema but not production-ready — no tests, no verified integration, no documented workflows.

### What Exists in Code but is NOT Implemented

- `flowfile_frame` defines `database_type: Literal["postgresql", "mysql", "sqlite", "mssql", "oracle"]` in type hints — but these are aspirational, not functional.
- `cloud_storage_schemas.py` defines `storage_type: Literal["s3", "adls", "gcs"]` — but only S3 is tested and supported.
- ConnectorX is a dependency (`^0.4.2`) but the actual database reads go through `pl.read_database_uri()`. ConnectorX's multi-database support is not actively leveraged or tested.

## Proposed Design

### Phase 1: MySQL Support

MySQL is the most impactful addition — it's the world's most popular open-source database and many ETL workloads involve MySQL sources.

**Implementation**:
1. **Backend**: Add MySQL-specific URI construction, test type mapping against MySQL types, handle MySQL-specific SQL dialect differences.
2. **Tests**: Add MySQL testcontainer to `test_utils/`, mirror PostgreSQL test suite for MySQL.
3. **Frontend**: Add `<option value="mysql">MySQL</option>` to database type dropdown. Adjust port default (3306 vs 5432).
4. **Documentation**: Add MySQL connection tutorial.

**Type mapping gaps**: MySQL-specific types (`ENUM`, `SET`, `MEDIUMINT`, `TINYINT`, `YEAR`) need explicit Polars mappings.

### Phase 2: ADLS and GCS Cloud Storage

The schema infrastructure exists. The work is in implementation and testing.

**ADLS (Azure)**:
1. Implement actual connection logic for `service_principal`, `managed_identity`, `sas_token`, and `access_key` auth methods.
2. Add ADLS test infrastructure (Azurite emulator or integration tests).
3. Test read/write for all supported formats (CSV, Parquet, JSON, Delta).
4. Frontend: Verify ADLS connection form works end-to-end.

**GCS (Google)**:
1. Implement GCS connection logic with service account and application default credentials.
2. Add GCS test infrastructure (fake-gcs-server or integration tests).
3. Test read/write for all supported formats.
4. Frontend: Verify GCS connection form works end-to-end.

### Phase 3: BigQuery and Snowflake

These are fundamentally different from standard databases and need dedicated connector implementations.

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

**BigQuery**:
- Read via `google-cloud-bigquery` with Storage API for large result sets.
- Write via load jobs (Parquet staging → BigQuery load).
- Support for partitioned/clustered table writes.
- Dedicated frontend form (project ID, dataset, credentials).

**Snowflake**:
- Read via `snowflake-connector-python` with result set caching.
- Write via internal stage → COPY INTO for bulk loading.
- Dedicated frontend form (account, warehouse, role, database).

### Phase 4: PostgreSQL Enhancements

Improve the existing PostgreSQL connector:

**Partitioned reads**: Expose ConnectorX's `PARTITION ON` for parallel reads of large tables.

```python
class DatabaseSettings(BaseModel):
    # ... existing fields ...
    # NEW: Performance optimization
    partition_column: str | None = None
    partition_count: int = 4
```

**Bulk loading**: Replace `df.write_database()` (row-by-row INSERT) with `COPY FROM STDIN` via psycopg2 for 10-100x faster writes.

**Incremental loading**:

```python
class IncrementalReadSettings(BaseModel):
    enabled: bool = False
    watermark_column: str           # e.g., "updated_at"
    watermark_type: Literal["timestamp", "integer"]
    last_watermark: Any | None      # stored in catalog metadata
```

The reader tracks the high-water mark from the last read. On subsequent runs, it adds a `WHERE watermark_column > last_watermark` predicate.

### Schema Browser

A visual schema browser in the node configuration panel, for all supported databases:

**New API endpoints**:

```python
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

**Frontend**: Tree view in database reader node config — connection → schema → table → columns. Click to auto-fill node settings.

### Frontend Changes

**Database type dropdown**: Expand from just PostgreSQL to all supported databases, with database-specific form fields:

| Database | Form Fields |
|----------|-------------|
| PostgreSQL | host, port (5432), database, username, password, schema |
| MySQL | host, port (3306), database, username, password |
| BigQuery | project ID, dataset, service account JSON |
| Snowflake | account, warehouse, role, database, username, password |

**Connection test**: "Test connection" button for all database types.

## Key Files to Modify

| File | Change |
|------|--------|
| `flowfile_core/flowfile_core/schemas/input_schema.py` | Extend `DatabaseConnection` with BigQuery/Snowflake fields |
| `flowfile_core/flowfile_core/flowfile/sources/external_sources/sql_source/sql_source.py` | Add MySQL/BigQuery/Snowflake connection logic |
| `flowfile_core/flowfile_core/flowfile/sources/external_sources/sql_source/utils.py` | Add MySQL/BigQuery/Snowflake type mappings and URI construction |
| `flowfile_core/flowfile_core/main.py` | Add schema browser endpoints |
| `flowfile_worker/flowfile_worker/external_sources/sql_source/` | Database-specific read/write strategies, bulk loading |
| `flowfile_frontend/.../databaseReader/DatabaseConnectionSettings.vue` | Expand database type dropdown, add DB-specific forms |
| `flowfile_frontend/.../databaseReader/databaseConnectionTypes.ts` | Extend type from `"postgresql"` to union of supported types |
| `test_utils/` | Add MySQL testcontainer (mirror `test_utils/postgres/`) |
| `flowfile_core/tests/`, `flowfile_worker/tests/` | Add MySQL, BigQuery, Snowflake test suites |
| `pyproject.toml` | Add optional deps: `mysql-connector-python`, `google-cloud-bigquery`, `snowflake-connector-python` |

## Open Questions

1. **Prioritization**: Which database after MySQL is most requested by users? BigQuery or Snowflake?
2. **ADLS/GCS testing**: Use emulators (Azurite, fake-gcs-server) for CI, or rely on integration tests with real cloud credentials?
3. **BigQuery/Snowflake type mapping**: Both have proprietary types (BigQuery: STRUCT, ARRAY, GEOGRAPHY; Snowflake: VARIANT, OBJECT) that need new Polars mappings.
4. **OAuth for BigQuery/Snowflake**: Both support OAuth flows. How to integrate with the existing secrets/auth model?
5. **Dependency management**: BigQuery and Snowflake drivers are heavy. Should they be optional extras (`pip install flowfile[bigquery]`) or bundled?
6. **Bulk write across databases**: PostgreSQL has `COPY`, MySQL has `LOAD DATA`. Should the API expose this as a generic "bulk mode" toggle, or database-specific options?
