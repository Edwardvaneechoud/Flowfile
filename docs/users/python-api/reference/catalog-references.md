# Catalog References

The Flowfile catalog organizes tables in a two-level hierarchy: **catalogs** contain **schemas**, and tables live under schemas (Unity Catalog–style). Internally each namespace is keyed by an autoincrement integer (`namespace_id`), but you don't need to know that ID to do anything useful.

`CatalogReference` and `SchemaReference` are validated, name-based handles. Construct one once at the top of your script — it resolves the name to the underlying ID and either confirms the catalog/schema exists or creates it. Pass the handle around instead of looking up integer IDs by hand.

This example runs in CI on every commit:

```python
--8<-- "docs/examples/catalog_references.py:example"
```

## `CatalogReference`

Validated handle to a top-level catalog (level-0 namespace).

```python
ff.CatalogReference(
    name: str,
    *,
    auto_create: bool = False,
    description: str | None = None,
)
```

**Parameters:**

- `name`: Catalog name. Cannot contain `.` (reserved for fully-qualified table references).
- `auto_create`: When `True`, create the catalog if it doesn't exist. When `False` (default), raise `NamespaceNotFoundError` if missing.
- `description`: Optional description. Only applied when the catalog is created — ignored when an existing catalog is found.

**Attributes:**

- `name: str` — the resolved catalog name.
- `id: int` — the database-internal namespace ID. Stable for a given deployment but not portable across environments.

The reference is **immutable**, **hashable**, and **picklable** — safe to store in sets, use as dict keys, or pass between processes.

### Methods

#### `schema(name, *, auto_create=False, description=None) -> SchemaReference`

Return a [`SchemaReference`](#schemareference) for a child schema of this catalog.

```python
catalog = ff.CatalogReference("sales")
raw = catalog.schema("raw")                          # must exist
staging = catalog.schema("staging", auto_create=True) # creates if missing
```

#### `list_schemas() -> list[SchemaReference]`

Return every schema (level-1 namespace) under this catalog as `SchemaReference` objects.

```python
for schema in catalog.list_schemas():
    print(schema.name, schema.list_tables())
```

#### `list_tables() -> list[CatalogTableOut]` — CatalogReference

Return tables across **every** schema in this catalog, as a flat list. Each row's `namespace_id` field tells you which schema it belongs to. For a per-schema view, use [`SchemaReference.list_tables()`](#list_tables-listcatalogtableout-schemareference).

```python
for table in catalog.list_tables():
    print(table.namespace_id, table.name)
```

## `SchemaReference`

Validated handle to a schema under a catalog (level-1 namespace).

```python
ff.SchemaReference(
    catalog: CatalogReference,
    name: str,
    *,
    auto_create: bool = False,
    description: str | None = None,
)
```

You can also build one fluently from a catalog handle: `catalog.schema("name")`.

**Parameters:**

- `catalog`: The parent [`CatalogReference`](#catalogreference).
- `name`: Schema name. Cannot contain `.`.
- `auto_create`: When `True`, create the schema under `catalog` if it doesn't exist.
- `description`: Optional description, only applied on create.

**Attributes:**

- `catalog: CatalogReference` — the parent catalog.
- `name: str` — the resolved schema name.
- `id: int` — the database-internal namespace ID. This is the value that the legacy `namespace_id=` keyword expects.

Like `CatalogReference`, schema references are immutable, hashable, and picklable.

### Methods

#### `list_tables() -> list[CatalogTableOut]` — SchemaReference

Return tables registered in this schema.

#### `read_table(name, *, delta_version=None, scd2_view=None, scd2_as_of=None, flow_graph=None) -> FlowFrame`

Convenience for [`ff.read_catalog_table(name, schema=self, ...)`](reading-data.md#catalog-reading).

```python
df = schema.read_table("orders")
df_v5 = schema.read_table("orders", delta_version=5)
active = schema.read_table("customers", scd2_view="active")  # SCD2 tables only
```

`scd2_view` (`"active"` / `"all"` / `"active_at"`, default `None` — every version, no filter) and `scd2_as_of` (required with `"active_at"`) select a history view on an [SCD2-tracked](../../visual-editor/catalog/slowly-changing-dimensions.md) table; both are ignored on a plain table.

#### `write_table(df, name, *, write_mode="overwrite", merge_keys=None, partition_by=None, scd2_compare_columns=None, scd2_full_snapshot=False, scd2_surrogate_key_column="sk", scd2_valid_from_column="valid_from", scd2_valid_to_column="valid_to", scd2_is_current_column="is_current", scd2_partition_on_current=True, description=None) -> None`

Convenience for [`df.write_catalog_table(name, schema=self, ...)`](writing-data.md#catalog-writing).

```python
schema.write_table(df, "orders", write_mode="upsert", merge_keys=["id"])
schema.write_table(df, "customers", write_mode="scd2", merge_keys=["customer_id"])
```

The `scd2_*` keywords configure a `write_mode="scd2"` write (see [Slowly Changing Dimensions](../../visual-editor/catalog/slowly-changing-dimensions.md)) and raise if passed with any other `write_mode`.

## Module-level helpers

### `list_catalogs() -> list[CatalogReference]`

Enumerate every catalog (root namespace) in the backend.

```python
for catalog in ff.list_catalogs():
    print(catalog.name)
```

### `default_schema() -> SchemaReference`

Return a handle to the seeded `General/default` schema. Useful when you don't care which catalog you write to and just want something to work.

```python
schema = ff.default_schema()
schema.write_table(df, "scratch")
```

Raises `LookupError` if the default schema hasn't been initialized for this deployment.

## Integration with existing functions

Every catalog-aware function and method now accepts a `schema=` keyword that supersedes `namespace_id=`:

| Function / method | Accepts |
|---|---|
| [`ff.read_catalog_table`](reading-data.md#catalog-reading) | `schema=`, `namespace_id=` |
| [`ff.write_catalog_table`](writing-data.md#catalog-writing) | `schema=`, `namespace_id=` |
| `FlowFrame.write_catalog_table` | `schema=`, `namespace_id=` |
| `FlowFrame.train_model` | `schema=`, `namespace_id=` |
| `FlowFrame.apply_model` | `schema=`, `namespace_id=` |

!!! warning "Don't pass both"
    Passing both `schema=` and `namespace_id=` raises `ValueError("Pass either schema= or namespace_id=, not both")`.

The legacy `namespace_id=<int>` form still works for back-compat. New code should prefer `schema=`.

## Validation, errors, and lifecycle

- **Construction is eager.** `CatalogReference("missing")` hits the database immediately and raises `flowfile_core.catalog.NamespaceNotFoundError` if the catalog doesn't exist (and `auto_create=False`). The effect is to fail at the top of your script rather than deep inside a write call.
- **`auto_create=True` is idempotent.** If two processes race to create the same catalog, one wins and the loser refetches the existing namespace transparently.
- **References don't re-validate on every call.** If the underlying catalog or schema is deleted *after* you constructed the reference, subsequent operations (e.g. `list_tables`) will surface the backend error. Construct a new reference if you suspect drift.
- **Names cannot contain `.`** — the dot is reserved for fully-qualified `catalog.schema.table` references and is rejected with `ValueError` at construction time.

## Example: end-to-end flow

```python
import flowfile as ff

# Resolve / create the target once
catalog = ff.CatalogReference("sales", auto_create=True)
raw = catalog.schema("raw", auto_create=True)
staging = catalog.schema("staging", auto_create=True)

# Read raw, transform, write to staging — no namespace IDs anywhere
orders = raw.read_table("orders")
clean = (
    orders
    .filter(ff.col("status") != "cancelled")
    .with_columns(ff.col("total").cast(ff.Float64))
)
staging.write_table(clean, "orders_clean", write_mode="overwrite")

# Discover what's there
print([t.name for t in catalog.list_tables()])
```

---
[← Previous: Visual UI Integration](visual-ui.md)
