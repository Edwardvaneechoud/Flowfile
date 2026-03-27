# Feature 3: Delta Lake Catalog Storage

## Motivation

The Flowfile catalog currently stores table data as plain Parquet files on the local filesystem. While Parquet is efficient for reads, it lacks:

- **ACID transactions**: Concurrent writes can corrupt data. There is no atomic commit.
- **Time travel**: No way to query previous versions of a table or roll back changes.
- **Schema evolution**: Adding/removing columns requires rewriting all files.
- **Incremental updates**: Appending data means rewriting the entire file or managing a directory of parts manually.

Delta Lake solves all of these while remaining compatible with Polars and the existing Parquet-based read path.

## Current State

- **Catalog service** (`catalog/service.py`): Manages table metadata in a database via `CatalogRepository`. Table data is materialized as Parquet files referenced by `file_path`. Materialization happens via subprocess worker calls (`trigger_catalog_materialize()`).
- **Cloud storage schemas** (`cloud_storage_schemas.py`): Already define `delta` as a supported format in `CloudStorageReadSettings` and `CloudStorageWriteSettings`. The infrastructure for reading/writing Delta tables to cloud storage exists.
- **Polars Delta support**: Polars supports `pl.read_delta()` and `df.write_delta()` via the `deltalake` (delta-rs) Python package. This is already a dependency path for cloud storage nodes.
- **Catalog tables**: Tracked with metadata (schema, row_count, column_count, size_bytes) in `CatalogMaterializationResult`.

## Proposed Design

### Migration Path

Replace the catalog's Parquet file storage with Delta Lake tables. This is a **storage layer change** — the catalog API, metadata database, and frontend remain the same.

### Phase 1: Delta Lake for Catalog Tables

**Storage layout change**:

```
Before:
  /catalog_storage/{namespace}/{table_name}.parquet

After:
  /catalog_storage/{namespace}/{table_name}/
    _delta_log/
      00000000000000000000.json
    part-00000-*.parquet
    part-00001-*.parquet
```

**`catalog/service.py` changes**:

```python
# Current materialization
def materialize_table(self, table_id: int) -> CatalogMaterializationResult:
    # ... writes a single .parquet file
    df.write_parquet(file_path)

# Proposed materialization (using LazyFrame.sink_delta)
def materialize_table(self, table_id: int, mode: str = "overwrite") -> CatalogMaterializationResult:
    lf.sink_delta(delta_table_path, mode=mode)
```

**Read path**:

```python
# Current
df = pl.read_parquet(file_path)

# Proposed
df = pl.read_delta(delta_table_path)
# Or with time travel:
df = pl.read_delta(delta_table_path, version=5)
```

### Phase 2: Time Travel & Versioning

**New catalog metadata fields**:

```python
class CatalogTableVersion(BaseModel):
    version: int
    timestamp: datetime
    operation: str          # "WRITE", "APPEND", "MERGE", "DELETE"
    row_count: int
    schema_hash: str
    user_id: int | None
    flow_run_id: int | None
```

**New API endpoints**:

| Endpoint | Description |
|----------|-------------|
| `GET /catalog/tables/{id}/versions` | List all versions of a table |
| `GET /catalog/tables/{id}/version/{v}` | Read table at specific version |
| `GET /catalog/tables/{id}/diff/{v1}/{v2}` | Schema and row count diff between versions |
| `POST /catalog/tables/{id}/restore/{v}` | Restore table to a previous version |

### Phase 3: Schema Evolution & Merge

**Schema evolution**: Delta Lake handles additive schema changes (new columns) automatically. The catalog service should:
1. Detect schema changes on write.
2. Update catalog metadata to reflect the new schema.
3. Surface schema evolution history in the UI.

**Merge (upsert) support**:

Merge needs to know which columns to match on. These merge keys come from the **`catalog_writer` node's settings** — the user specifies them when configuring the write, alongside the write mode.

**`catalog_writer` node extension** (`input_schema.py`):

```python
class NodeCatalogWriter(NodeSingleInput):
    catalog_table_id: int | None = None
    catalog_table_name: str | None = None
    catalog_namespace_id: int | None = None
    write_mode: Literal["overwrite", "append", "merge"] = "overwrite"   # NEW
    merge_keys: list[str] | None = None     # NEW: required when write_mode == "merge"
```

The UI shows a column selector for `merge_keys` when `write_mode` is set to `"merge"` — populated from the input DataFrame's schema.

Optionally, the catalog table metadata can also store declared primary keys for validation:

```python
class CatalogTableMetadata(BaseModel):
    # ... existing fields ...
    primary_keys: list[str] | None = None   # declared key columns (optional)
```

If the table has declared primary keys and the writer's `merge_keys` don't match, a warning is surfaced.

**Catalog service write/merge method** using Polars' native `sink_delta`:

```python
def materialize_table(
    self,
    table_id: int,
    source_lf: pl.LazyFrame,
    mode: Literal["overwrite", "append", "merge"] = "overwrite",
    merge_keys: list[str] | None = None,
):
    table_info = self.get_table(table_id)
    delta_path = table_info.file_path

    if mode == "merge":
        predicate = " AND ".join(f"s.{k} = t.{k}" for k in merge_keys)
        (
            source_lf.sink_delta(
                delta_path,
                mode="merge",
                delta_merge_options={
                    "predicate": predicate,
                    "source_alias": "s",
                    "target_alias": "t",
                },
            )
            .when_matched_update_all()
            .when_not_matched_insert_all()
            .execute()
        )
    else:
        source_lf.sink_delta(delta_path, mode=mode)
```

This uses `pl.LazyFrame.sink_delta()` directly — no need to convert to Arrow or manage `DeltaTable` objects manually. The merge mode returns a `TableMerger` that supports the same `.when_matched_update_all().when_not_matched_insert_all()` API.

Note: `sink_delta` is marked as unstable in Polars. This should be monitored across Polars releases.

### Phase 4: Cloud Delta Tables

The existing cloud storage infrastructure already supports Delta format. This phase connects the catalog to cloud-hosted Delta tables:

- **S3**: `s3://bucket/catalog/{namespace}/{table}/`
- **ADLS**: `abfss://container@account/{namespace}/{table}/`
- **GCS**: `gs://bucket/catalog/{namespace}/{table}/`

Cloud credentials flow through the existing `FullCloudStorageConnection` model with per-user encryption.

### Backward Compatibility

- **Migration tool**: A one-time script converts existing `.parquet` catalog files to Delta tables.
- **Read fallback**: If a table path contains a `.parquet` file (old format), read it directly. If it contains a `_delta_log/` directory, use `read_delta()`.
- **Version field**: Add `storage_format: Literal["parquet", "delta"] = "delta"` to catalog table metadata for the transition period.

## Key Files to Modify

| File | Change |
|------|--------|
| `flowfile_core/flowfile_core/catalog/service.py` | Replace Parquet write/read with Delta; add version/merge APIs |
| `flowfile_core/flowfile_core/catalog/repository.py` | Add version tracking tables to ORM |
| `flowfile_core/flowfile_core/schemas/cloud_storage_schemas.py` | Extend Delta options (merge keys, write mode) |
| `flowfile_core/flowfile_core/main.py` | Add version/restore/diff endpoints |
| `flowfile_worker/` | Update materialization to produce Delta tables |
| `pyproject.toml` | Ensure `deltalake` is a core dependency (not just optional) |

## Open Questions

1. **Compaction**: Delta tables accumulate small files over many appends. When and how should automatic compaction (OPTIMIZE) run?
2. **Vacuum**: Old versions consume disk space. What retention policy? Default to 7 days?
3. **Concurrent writers**: Multiple flow executions writing to the same catalog table — Delta handles optimistic concurrency, but should the catalog service add its own locking?
4. **WASM compatibility**: The `deltalake` Python package is not available in WASM. Catalog operations would be remote-only in the WASM frontend.
