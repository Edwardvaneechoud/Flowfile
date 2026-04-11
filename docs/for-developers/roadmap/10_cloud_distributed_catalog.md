# Feature 10: Cloud & Distributed Catalog

## Motivation

The Flowfile catalog currently stores everything locally: metadata in SQLite, table data as Parquet files on the local filesystem. This works well for single-user desktop use, but limits Flowfile in team and production environments:

- **No shared catalog**: Two users running Flowfile on different machines each have their own isolated catalog. There's no shared view of available tables, flows, or artifacts.
- **No cloud storage for catalog data**: Table data lives on the local disk. For cloud-native deployments (Docker, Kubernetes), data should live in S3, ADLS, or GCS.
- **SQLite limitations**: SQLite doesn't support concurrent writes from multiple processes well. In a multi-worker or multi-user deployment, the metadata database becomes a bottleneck.
- **No catalog federation**: Organizations with existing data lakes or lakehouses can't register external tables in the Flowfile catalog without copying data.

This feature makes the catalog backend pluggable — both the metadata database and the table storage layer.

## Current State

### Metadata Database: SQLite

- **Engine**: SQLAlchemy ORM with `create_engine(get_database_url())`.
- **Default URL**: `sqlite:///{storage.database_directory}/flowfile.db` (typically `~/.flowfile/database/flowfile.db`).
- **Configuration** (`shared/storage_config.py`): `get_database_url()` checks `FLOWFILE_DB_PATH` env var first, then falls back to default SQLite path. SQLite-specific `check_same_thread=False` is conditionally applied.
- **Connection** (`database/connection.py`): `SessionLocal` factory with `autocommit=False`, `autoflush=False`.
- **Already flexible in principle**: The code uses SQLAlchemy throughout (not raw SQLite calls), so switching to PostgreSQL is architecturally possible. But it's never been tested, and there are likely SQLite-specific assumptions in queries or migrations.

### ORM Models (`database/models.py`)

16+ models including:
- `CatalogNamespace` — hierarchical namespaces (2-level: catalog → schema)
- `CatalogTable` — table metadata (schema_json, row_count, column_count, size_bytes, file_path)
- `CatalogTableReadLink` — lineage tracking (which flows read which tables)
- `FlowRegistration` — flow definitions
- `FlowRun` — execution history
- `FlowSchedule` / `ScheduleTriggerTable` — scheduling
- `GlobalArtifact` — versioned Python objects
- `User`, `Secret`, `DatabaseConnection`, `CloudStorageConnection`

### Repository Layer

- **Protocol** (`CatalogRepository`): Abstract interface — clean separation from storage backend.
- **Implementation** (`SQLAlchemyCatalogRepository`): SQLAlchemy-backed concrete implementation.
- This protocol/implementation split is the right foundation for making backends pluggable.

### Table Data Storage

- **Local Parquet files**: `file_path` on `CatalogTable` points to a local path.
- After Feature 3 (Delta Lake), this becomes local Delta tables.
- No cloud storage for catalog-managed data.

## Proposed Design

### Part 1: PostgreSQL as Catalog Database

Make PostgreSQL a first-class alternative to SQLite for the metadata database.

**Configuration**:

```bash
# SQLite (default, current behavior)
FLOWFILE_DB_URL=sqlite:///~/.flowfile/database/flowfile.db

# PostgreSQL (new)
FLOWFILE_DB_URL=postgresql://user:password@host:5432/flowfile_catalog
```

**Implementation**:

1. **Audit SQL compatibility**: Review all queries in `SQLAlchemyCatalogRepository` for SQLite-specific syntax. Common issues:
   - `AUTOINCREMENT` vs `SERIAL`
   - Boolean handling (`0`/`1` vs `TRUE`/`FALSE`)
   - JSON column support (SQLite `JSON` vs PostgreSQL native `JSONB`)
   - Date/time handling differences

2. **Migration system**: Add Alembic for schema migrations. Current approach (likely `create_all()`) doesn't handle schema evolution.

```
flowfile_core/flowfile_core/database/
├── connection.py          # existing
├── models.py              # existing
├── migrations/            # NEW: Alembic migrations
│   ├── env.py
│   └── versions/
│       ├── 001_initial.py
│       └── ...
```

3. **Connection pooling**: PostgreSQL benefits from connection pooling. Add pool configuration:

```python
engine = create_engine(
    db_url,
    pool_size=5,
    max_overflow=10,
    pool_timeout=30,
    pool_recycle=1800,
)
```

4. **Test both backends**: Run the full catalog test suite against both SQLite and PostgreSQL. Add PostgreSQL testcontainer (similar to existing `test_utils/postgres/` for database reader tests).

### Part 2: Cloud Storage for Catalog Table Data

Allow catalog tables to be stored in cloud object storage instead of local disk.

**Catalog table storage configuration**:

```python
class CatalogStorageConfig(BaseModel):
    storage_type: Literal["local", "s3", "adls", "gcs"] = "local"
    # Local
    local_base_path: str | None = None
    # Cloud
    cloud_connection_name: str | None = None   # references a registered cloud connection
    bucket: str | None = None
    prefix: str | None = None                  # e.g., "flowfile-catalog/"
```

**Storage path mapping**:

```
Local:  /catalog_storage/{namespace}/{table}/
S3:     s3://bucket/flowfile-catalog/{namespace}/{table}/
ADLS:   abfss://container@account/flowfile-catalog/{namespace}/{table}/
GCS:    gs://bucket/flowfile-catalog/{namespace}/{table}/
```

**Implementation**:

1. The `CatalogTable.file_path` field becomes a URI (local path or cloud URI).
2. Read/write operations use the appropriate backend based on the URI scheme.
3. Cloud credentials flow through the existing `FullCloudStorageConnection` model with per-user encryption.
4. `sink_delta` already supports `storage_options` for cloud credentials — this plugs in directly.

```python
# Cloud-backed catalog write
source_lf.sink_delta(
    "s3://bucket/flowfile-catalog/production/customers/",
    mode="overwrite",
    storage_options={
        "AWS_REGION": "us-east-1",
        "AWS_ACCESS_KEY_ID": "...",
        "AWS_SECRET_ACCESS_KEY": "...",
    },
)
```

### Part 3: Shared Catalog for Teams

With PostgreSQL metadata + cloud table storage, multiple Flowfile instances can share the same catalog.

**Architecture**:

```
┌─────────────┐   ┌─────────────┐   ┌─────────────┐
│  User A      │   │  User B      │   │  Scheduled   │
│  (Desktop)   │   │  (Desktop)   │   │  (Docker)    │
└──────┬───────┘   └──────┬───────┘   └──────┬───────┘
       │                  │                   │
       └──────────┬───────┴───────────────────┘
                  │
       ┌──────────▼──────────┐
       │  PostgreSQL          │
       │  (catalog metadata)  │
       └──────────┬──────────┘
                  │
       ┌──────────▼──────────┐
       │  S3 / ADLS / GCS    │
       │  (table data)        │
       └─────────────────────┘
```

**Concurrency considerations**:
- PostgreSQL handles concurrent reads/writes natively.
- Delta Lake handles concurrent table writes via optimistic concurrency (conflict resolution on commit).
- The catalog service should use database transactions for metadata consistency.

### Part 4: Catalog Federation (External Tables)

Allow registering external Delta/Parquet tables in the catalog without copying data into catalog-managed storage.

```python
class CatalogTable(BaseModel):
    # ... existing fields ...
    storage_mode: Literal["managed", "external"] = "managed"
    # managed: Flowfile owns the data, stores in catalog storage
    # external: data lives elsewhere, catalog only stores metadata + pointer
    external_uri: str | None = None  # e.g., "s3://data-lake/production/customers/"
```

**Managed tables**: Flowfile writes and owns the data. Full CRUD, versioning, merge support.
**External tables**: Read-only pointer to data managed outside Flowfile. Schema is inferred on registration and refreshed periodically. No write/merge support.

This allows organizations to register their existing data lake tables in the Flowfile catalog for discovery and use in flows, without data duplication.

## Key Files to Modify

| File | Change |
|------|--------|
| `shared/storage_config.py` | Support PostgreSQL URL in `get_database_url()` |
| `flowfile_core/flowfile_core/database/connection.py` | Conditional pool config for PostgreSQL vs SQLite |
| `flowfile_core/flowfile_core/database/models.py` | Audit for SQLite-specific assumptions; add `storage_mode`, `external_uri` to `CatalogTable` |
| `flowfile_core/flowfile_core/database/migrations/` | NEW: Alembic migration framework |
| `flowfile_core/flowfile_core/catalog/service.py` | Cloud-backed read/write; federation support |
| `flowfile_core/flowfile_core/catalog/repository.py` | Ensure queries work on both SQLite and PostgreSQL |
| `flowfile_core/flowfile_core/schemas/` | Catalog storage configuration models |
| `flowfile_core/flowfile_core/main.py` | Endpoints for storage config, external table registration |
| `test_utils/` | PostgreSQL testcontainer for catalog tests |

## Open Questions

1. **Migration path**: How do existing SQLite users migrate to PostgreSQL? Export/import tool, or dual-database transition period?
2. **Mixed storage**: Can some tables be local and others cloud within the same catalog? The URI-based `file_path` supports this, but the UI and configuration need to handle it.
3. **Latency**: Cloud storage adds latency for reads. Should the catalog cache table metadata (schema, row count) aggressively to avoid round-trips?
4. **Cost**: Cloud storage API calls and data transfer have costs. Should the catalog track usage or warn about expensive operations?
5. **Authentication scope**: If multiple users share a PostgreSQL catalog, each user's cloud credentials are different. How does credential scoping work for shared tables?
6. **External table freshness**: How often should external table schemas be refreshed? On every access, on a schedule, or manually?
