# Slowly Changing Dimensions (SCD2)

SCD2 is a catalog write mode that keeps history instead of overwriting it: every write end-dates the rows whose tracked columns changed and inserts new versions alongside the ones that stayed the same. Use it for dimension tables — customers, products, accounts — where you need to know what a row looked like at a past point in time, not just what it looks like now. This page covers how the write behaves, the columns it generates, how to read history back, and the rules that protect an SCD2 table from an incompatible write.

## What an SCD2 write does

Set **Write Mode** to `scd2` on a [Catalog Writer](../nodes/output.md#catalog-writer) node, or pass `write_mode="scd2"` to [`write_catalog_table`](../../python-api/reference/writing-data.md#catalog-writing). Each write compares the input against the table's current rows, keyed on the **business key** (the same `merge_keys` / **Key Columns** field used by upsert/update/delete):

- A business key with no current row is a new row — inserted as version 1.
- A business key whose compared columns changed — the current row is end-dated and a new version is inserted.
- A business key whose compared columns are unchanged is left as-is; nothing is written for it.

The first write to a table (or the first write after the table was deleted) is an **initial load**: every input row becomes a current version, with no comparison against anything.

## Generated columns and half-open validity

Every SCD2 write adds four columns to the table, alongside the input's own columns:

| Column | Meaning |
|--------|---------|
| `sk` | Surrogate key — one value per row version |
| `valid_from` | UTC timestamp this version became current |
| `valid_to` | UTC timestamp this version stopped being current, or empty while it still is |
| `is_current` | `true` for the current version of a business key, `false` for a superseded one |

A row's validity window is **half-open**: `[valid_from, valid_to)`. The current version's `valid_to` is empty, not a large sentinel date — check `is_current` or `valid_to IS NULL` to find it. The generated column names can be overridden on the writer's SCD2 settings if `sk`/`valid_from`/`valid_to`/`is_current` collide with an existing column.

## Deterministic surrogate keys

`sk` is a deterministic hash of the business key and `valid_from` — not a random UUID and not an auto-incrementing counter. Re-running the exact same write twice produces the exact same `sk` for the exact same version, which is what makes a no-op re-run genuinely a no-op: if nothing in the input changed since the last write, Flowfile detects that no business key is new or changed and skips the write entirely — no new Delta version is committed, and `updated_at` on the catalog table does not move.

Business keys must be string, integer, boolean, date, or datetime — floating-point and nested (list/struct) keys have no stable way to be encoded into the surrogate key and are rejected.

## Change detection scope

By default, every column in the input other than the business key and the four generated columns is compared for change detection. Narrow this with **Compare Columns** (`scd2_compare_columns` in Python): only the listed columns are checked, so a change in an untracked column does not trigger a new version.

## Full snapshot vs incremental

By default (**Full Snapshot** off), a business key that is absent from one write's input but present in an earlier write is left untouched — it stays current. Turn **Full Snapshot** on when each write is a complete replacement of the dimension: business keys missing from the current input are then end-dated, the same as a changed row, so the "current" set always matches the most recent full input exactly.

## Reading history

A [Catalog Reader](../nodes/input.md#catalog-reader) on an SCD2-tracked table shows a **History** selector (`scd2_view` in Python):

| View | Behavior |
|------|----------|
| **All records (default)** (`None` on the wire) | No filter — every version of every row |
| **Active records** (`"active"`) | Only current rows (`valid_to` empty) |
| **Active at a point in time** (`"active_at"`, with a timestamp) | The version that was current at that point in time — a half-open comparison against `valid_from`/`valid_to` |

The default is **all records**, not active-only: a Catalog Reader pointed at an SCD2 table with History left unset returns full history. Set History to **Active records** explicitly whenever the downstream flow expects one row per business key — a join or aggregation over unfiltered history silently multiplies rows per key.

The tested example below writes the same two-row dimension twice, changing one row's `tier` on the second write, then reads both the active set and the full history:

```python
--8<-- "docs/examples/catalog_scd2.py:example"
```

!!! note "SQL Editor and codegen"
    Querying an SCD2 table from the [SQL Editor](sql-editor.md) or via `flowfile_frame`'s SQL context returns every version, the same as an unfiltered `scd2_view`. Add `WHERE valid_to IS NULL` (or the table's configured `is_current` column) yourself to get only current rows.

## Protection rules

An SCD2-tracked table only accepts further `scd2` writes or a plain `overwrite`:

- **append, upsert, update, delete** against an SCD2-tracked table fail at run time with an error. These modes would insert or mutate rows without maintaining `valid_from`/`valid_to`/`is_current`, corrupting the version history.
- **overwrite** is allowed and rebuilds the table as a normal table with the current write's data — it clears SCD2 tracking. The four generated columns are removed along with the rest of the history; a subsequent `scd2` write treats the table as a fresh initial load.
- Writing `scd2` onto a table that already exists but was **not** created with `scd2` also fails. Flowfile does not convert an existing plain table in place.

**Re-initializing a table as SCD2:** if you need to start SCD2 tracking on data that already has a plain table, either write to a new table name, or delete the existing table first (from the [catalog browser](index.md#catalog-tables) or via the API) and let the next `scd2` write perform the initial load.

## Partitioning advice

New SCD2 tables are partitioned by the is-current column by default: closed versions are quarantined into files that later writes never rewrite, and both the write's own change detection and *Active records* reads scan only the current partition. Any `partition_by` columns you choose nest above it (for example `region`, giving `region/is_current` partitions), and the **Partition on is_current** switch (`scd2_partition_on_current` in the Python API) is the deliberate opt-out. All of this takes effect only on the table's initial load, since Delta partitioning is immutable afterward. Flowfile rejects partitioning by `sk` or `valid_from` (a `partition_by` validation error): both grow unboundedly with every write and would produce an ever-increasing number of tiny partitions.

## Concurrent writers on object storage

!!! warning "No locking provider for S3/remote catalogs"
    Flowfile's Delta writes use delta-rs's optimistic concurrency control, with no external locking provider configured. On a local filesystem this is safe. On S3 (or another object-storage-backed catalog), two writers committing to the same table at the same time can conflict, and SCD2's read-then-merge write is inherently more conflict-prone than a plain append. Do not schedule two flows that write to the same SCD2 table concurrently against an object-storage catalog.

## Related documentation

- [Catalog Writer](../nodes/output.md#catalog-writer) — the `scd2` write mode's settings panel
- [Catalog Reader](../nodes/input.md#catalog-reader) — the History selector
- [Writing Data](../../python-api/reference/writing-data.md#catalog-writing) — `write_catalog_table`'s `scd2_*` keyword arguments
- [Catalog](index.md) — the four generated columns on disk, and how vacuum treats SCD2 row history
- [Catalog Architecture](../../../for-developers/catalog-architecture.md) — `CatalogTable.scd2_config` as the single source of truth
