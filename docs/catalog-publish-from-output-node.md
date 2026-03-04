# Publish to Catalog from the Output (Write) Node

## Overview

The **Publish to Catalog** feature lets you automatically register the file
produced by an output node as a catalog table. When a flow runs and writes
data to disk, it can optionally call the Catalog Service to create a
metadata entry that points to the written file — making the result
immediately available for other flows or users to discover and query.

Currently the catalog always materializes registered files as **Parquet**.
This is a deliberate choice: Parquet is columnar, compressed, and
schema-aware, so catalog reads are fast and predictable regardless of
the original output format. A future iteration could layer Apache Iceberg
on top, reusing the same Parquet files as the underlying data layer.

---

## How It Works (End to End)

```
┌────────────────┐
│  Output Node   │  User enables "Publish to Catalog" toggle,
│  (Vue.js UI)   │  optionally sets table name + namespace.
└───────┬────────┘
        │  Settings saved to flow JSON/YAML
        ▼
┌────────────────┐
│  Flow Graph    │  During execution, the output node writes
│  Execution     │  the file (CSV/Parquet/Excel) to the
│                │  configured directory.
└───────┬────────┘
        │  If publish_to_catalog == true
        ▼
┌────────────────────────────┐
│  _publish_output_to_catalog│  Opens a DB session, instantiates
│  (FlowGraph static method) │  CatalogService, and calls
│                            │  register_table().
└───────┬────────────────────┘
        ▼
┌────────────────────────────────────────────┐
│  CatalogService.register_table()           │
│                                            │
│  1. Validate namespace exists              │
│  2. Check no duplicate table name          │
│  3. Read source file with Polars           │
│  4. Write Parquet copy to catalog storage  │
│     ~/.flowfile/catalog_tables/            │
│     <name>_<uuid8>.parquet                 │
│  5. Extract schema (column names + dtypes) │
│  6. INSERT metadata row into               │
│     catalog_tables (SQLite/PostgreSQL)     │
└────────────────────────────────────────────┘
```

---

## Data Model

### OutputSettings (Backend — Pydantic)

Three new optional fields on `OutputSettings`
(`flowfile_core/flowfile_core/schemas/input_schema.py`):

| Field                  | Type            | Default | Purpose                                       |
|------------------------|-----------------|---------|-----------------------------------------------|
| `publish_to_catalog`   | `bool`          | `False` | Master toggle                                 |
| `catalog_table_name`   | `str \| None`   | `None`  | Override table name (defaults to file stem)    |
| `catalog_namespace_id` | `int \| None`   | `None`  | Target namespace (catalog/schema); `None` = unscoped |

These fields are also included in the `OutputSettingsYaml` TypedDict
(`flowfile_core/flowfile_core/schemas/yaml_types.py`) so they persist when
flows are saved as YAML.

### OutputSettings (Frontend — TypeScript)

Matching optional fields on the `OutputSettings` interface
(`flowfile_frontend/src/renderer/app/types/node.types.ts`):

```typescript
export interface OutputSettings {
  // ... existing fields ...
  publish_to_catalog?: boolean;
  catalog_table_name?: string | null;
  catalog_namespace_id?: number | null;
}
```

### Database — `catalog_tables` table

When a table is registered the service creates a row in `catalog_tables`:

| Column          | Type     | Description                                               |
|-----------------|----------|-----------------------------------------------------------|
| `id`            | int (PK) | Auto-generated                                            |
| `name`          | string   | Logical table name                                        |
| `namespace_id`  | int (FK) | Links to `catalog_namespaces`                             |
| `file_path`     | string   | Absolute path to the materialized `.parquet` file         |
| `schema_json`   | text     | JSON array of `{"name": "col", "dtype": "Int64"}` objects |
| `row_count`     | int      | Number of rows                                            |
| `column_count`  | int      | Number of columns                                         |
| `size_bytes`    | int      | Size of the Parquet file on disk                          |
| `owner_id`      | int (FK) | User who registered the table                             |
| `created_at`    | datetime | Timestamp                                                 |
| `updated_at`    | datetime | Timestamp                                                 |

A unique constraint `(name, namespace_id)` prevents duplicate names
within the same schema.

---

## Execution Path (Python)

### 1. Output node writes the file

In `FlowGraph.add_output()` (`flowfile_core/flowfile_core/flowfile/flow_graph.py`):

```python
def _func(df: FlowDataEngine):
    # Normal file write (CSV, Parquet, or Excel)
    df.output(output_fs=output_file.output_settings, ...)

    # Then publish if requested
    if output_file.output_settings.publish_to_catalog:
        self._publish_output_to_catalog(output_file.output_settings)
    return df
```

### 2. `_publish_output_to_catalog` bridges to the catalog service

```python
@staticmethod
def _publish_output_to_catalog(settings: OutputSettings):
    table_name = settings.catalog_table_name or settings.name.rsplit(".", 1)[0]
    file_path  = settings.abs_file_path  # resolved by Pydantic validator

    with get_db_context() as db:
        repo = SQLAlchemyCatalogRepository(db)
        svc  = CatalogService(repo)
        svc.register_table(
            name=table_name,
            file_path=file_path,
            owner_id=1,
            namespace_id=settings.catalog_namespace_id,
            description="Published from output node",
        )
```

- Uses `get_db_context()` (a context-manager version of the FastAPI
  `get_db` dependency) to get a session outside of request scope.
- Failures are caught and logged as warnings — they do **not** fail the
  flow execution.

### 3. `CatalogService.register_table` materializes and records

Located in `flowfile_core/flowfile_core/catalog/service.py`:

1. **Namespace validation** — if `namespace_id` is provided, checks it exists.
2. **Duplicate check** — queries `catalog_tables` for an existing row with
   the same `(name, namespace_id)`.
3. **Read source** — uses Polars to read the output file:
   - `.csv` / `.txt` / `.tsv` → `pl.read_csv()`
   - `.parquet` → `pl.read_parquet()`
   - `.xlsx` / `.xls` → `pl.read_excel()`
4. **Write Parquet copy** — writes to `~/.flowfile/catalog_tables/<name>_<8hex>.parquet`.
5. **Extract schema** — iterates columns to build `[{name, dtype}]` metadata.
6. **Persist** — creates a `CatalogTable` ORM record and commits.

---

## Frontend UI

The Output.vue component (`flowfile_frontend/.../elements/output/Output.vue`)
adds a third settings section below the file-type and write-mode options:

1. **Checkbox** — `el-checkbox` bound to `output_settings.publish_to_catalog`.
2. **Table name** (shown when checked) — `el-input` bound to
   `output_settings.catalog_table_name`. If left blank, defaults to the
   file name without extension.
3. **Catalog / Schema** (shown when checked) — `el-select` bound to
   `output_settings.catalog_namespace_id`. Populated on mount by calling
   `CatalogApi.getNamespaceTree()` and flattening to
   `"<catalog> / <schema>"` labels.

---

## File Layout on Disk

```
~/.flowfile/
└── catalog_tables/
    ├── sales_report_a1b2c3d4.parquet    ← materialized copy
    ├── customers_e5f6g7h8.parquet
    └── monthly_kpi_i9j0k1l2.parquet
```

Each registered table gets its own Parquet file with a UUID suffix to
avoid name collisions. The original output file (the one the output node
wrote) is left untouched — the catalog stores its own independent copy.

---

## Future: Iceberg Integration

The current design stores each table as a single Parquet file with
metadata in the `catalog_tables` database table. Apache Iceberg tables
are also backed by Parquet data files but add:

- A metadata layer (manifest lists + manifests) that tracks snapshots
- Schema evolution, time-travel, and partition pruning
- ACID guarantees for concurrent writes

Because the catalog already materializes to Parquet, a future migration
path would be:

1. Replace the direct `df.write_parquet()` with an Iceberg table write
   (e.g. via `pyiceberg`).
2. Store the Iceberg table location (warehouse path) instead of a single
   file path in `catalog_tables.file_path`.
3. Read-back would use `pyiceberg` to scan the table instead of
   `pl.read_parquet()`.

The Parquet foundation means existing catalog tables could be
bulk-migrated by creating Iceberg metadata over the existing files.
