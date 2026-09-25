# Catalog References

The Flowfile catalog organizes tables in a two-level hierarchy: **catalogs** contain **schemas**, and tables live under schemas (Unity Catalog–style). Internally each namespace is keyed by an autoincrement integer (`namespace_id`), but you don't need to know that ID to do anything useful.

`CatalogReference` and `SchemaReference` are validated, name-based handles. Construct one once at the top of your script — it resolves the name to the underlying ID and either confirms the catalog/schema exists or creates it. Pass the handle around instead of looking up integer IDs by hand.

A schema handle also reaches the flows registered under it: `get_flow`, `list_flows` and `register_flow` return [`FlowRef`](native-nodes.md#flowref-and-flow_ref) handles that [`RunFlow`](native-nodes.md#runflow) calls as subflows.

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

#### `get_schema(name, *, auto_create=False, description=None) -> SchemaReference`

Alias of [`schema(...)`](#schemaname-auto_createfalse-descriptionnone-schemareference), named like the kernel's `flowfile_ctx.get_catalog(...).get_schema(...)`.

```python
sales = ff.get_catalog("Demo").get_schema("sales")
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

#### `read_table(name, *, delta_version=None, scd2_view=None, scd2_as_of=None, changes_since=None, changes_consumer=None, changes_start="now", include_change_preimage=False, flow_graph=None) -> FlowFrame`

Convenience for [`ff.read_catalog_table(name, schema=self, ...)`](reading-data.md#catalog-reading).

```python
df = schema.read_table("orders")
df_v5 = schema.read_table("orders", delta_version=5)
active = schema.read_table("customers", scd2_view="active")  # SCD2 tables only
```

`scd2_view` (`"active"` / `"all"` / `"active_at"`, default `None` — every version, no filter) and `scd2_as_of` (required with `"active_at"`) select a history view on an [SCD2-tracked](../../visual-editor/catalog/slowly-changing-dimensions.md) table; both are ignored on a plain table.

`changes_since` reads a [change feed](../../visual-editor/catalog/change-tracking.md) instead of the table: an `int` reads everything committed after that version, `"last_run"` reads everything after the position this consumer last committed, and an ISO-8601 string or `datetime` reads everything from that instant. `changes_consumer` names the cursor (required with `"last_run"` unless the flow is registered in the catalog), `changes_start` (`"now"` / `"beginning"`) decides where a cursor with no prior position starts, and `include_change_preimage` keeps the before-image rows of each update. The table must have change tracking enabled.

```python
changes = schema.read_table(
    "orders", changes_since="last_run", changes_consumer="orders-feed"
)
```

#### `write_table(df, name, *, write_mode="overwrite", merge_keys=None, partition_by=None, scd2_compare_columns=None, scd2_full_snapshot=False, scd2_surrogate_key_column="sk", scd2_valid_from_column="valid_from", scd2_valid_to_column="valid_to", scd2_is_current_column="is_current", scd2_partition_on_current=True, scd2_output_mode="input", track_changes=False, description=None) -> FlowFrame`

Convenience for [`df.write_catalog_table(name, schema=self, ...)`](writing-data.md#catalog-writing).

```python
schema.write_table(df, "orders", write_mode="upsert", merge_keys=["id"])
schema.write_table(df, "customers", write_mode="scd2", merge_keys=["customer_id"])
```

The `scd2_*` keywords configure a `write_mode="scd2"` write (see [Slowly Changing Dimensions](../../visual-editor/catalog/slowly-changing-dimensions.md)) and raise if passed with any other `write_mode`.

`track_changes=True` turns [change tracking](../../visual-editor/catalog/change-tracking.md) on for the table so later reads can pull only what each write changed. It is enable-only — `False` never turns tracking off — and is rejected with `write_mode` `"overwrite"`, `"virtual"` or `"scd2"`.

#### `get_flow(name) -> FlowRef`

Return the flow registered under this schema with that name; same as [`ff.flow_ref(self, name)`](native-nodes.md#flowref-and-flow_ref). Raises `FlowNotFoundError` when there is none, and `AmbiguousFlowError` (listing the candidates) when more than one registration has the name.

```python
clean = sales.get_flow("Clean orders")
run = ff.RunFlow(clean, orders=orders)
```

#### `list_flows() -> list[FlowRef]`

Return every flow registered under this schema that the current user may use.

#### `register_flow(flow_or_frame, *, name, overwrite=False) -> FlowRef`

Convenience for [`ff.register_flow(flow_or_frame, name=name, schema=self, overwrite=overwrite)`](native-nodes.md#register_flow): saves the flow as a YAML file, registers it under this schema when called, and returns its `FlowRef`. Re-running the same script reuses the registration.

```python
child = ff.create_flow_graph()
# ... build the child with ff.FlowInput / to_flow_output ...
clean = sales.register_flow(child, name="Clean orders")
```

## Module-level helpers

### `get_catalog(name) -> CatalogReference`

Same lookup as `CatalogReference(name)`: raises `NamespaceNotFoundError` when the catalog does not exist. Together with `get_schema` and `get_flow` it gives one chain from a catalog name to a flow:

```python
clean = ff.get_catalog("Demo").get_schema("sales").get_flow("Clean orders")
```

!!! info "Same shape as `flowfile_ctx`"
    Inside a kernel, `flowfile_ctx.get_catalog(name).get_schema(name)` navigates the catalog the same way, so code reads alike on both sides. The flow methods (`get_flow`, `list_flows`, `register_flow`) exist only in the Python API.

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
[← Previous: Visual UI Integration](visual-ui.md) | [Next: Native Node Classes →](native-nodes.md)
