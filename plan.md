# Plan: Complete Code Generation for All Node Types

## Context

The code generator (`FlowGraphToPolarsConverter`) currently supports 24 of 28 user-facing node types. Four nodes raise `UnsupportedNodeError`, and database reader/writer only work in "reference" mode. Since we're OK with `import flowfile as ff` in generated code (already used for cloud storage and database nodes), the path forward is clear.

## Current State

### Fully Unsupported (no handler)
| Node | Why it's hard without `ff` | Approach with `ff` |
|------|---------------------------|-------------------|
| `catalog_reader` | Needs catalog DB lookup to resolve file path | `ff.read_catalog_table()` |
| `catalog_writer` | Needs catalog DB + Delta merge logic | `ff.write_catalog_table()` |
| `kafka_source` | Needs live Kafka cluster + encrypted credentials | `ff.read_kafka()` |
| `python_script` | Arbitrary user code runs on Docker kernel | Inline user code in a function wrapper |

### Partially Unsupported
| Node | Current limitation |
|------|-------------------|
| `database_reader` | Inline connections fail; only `reference` mode works |
| `database_writer` | Inline connections fail; only `reference` mode works |

---

## Phase 1: New `flowfile_frame` API Functions

Before the code generator can emit calls to these functions, they need to exist. The pattern is already established by `ff.read_database()`, `ff.write_database()`, and the cloud storage functions.

### 1.1 `ff.read_catalog_table()`

**File:** `flowfile_frame/flowfile_frame/catalog.py` (new)
**Exposed in:** `flowfile_frame/__init__.py`

```python
def read_catalog_table(
    table_name: str,
    namespace_id: int | None = None,
    delta_version: int | None = None,
) -> pl.LazyFrame:
    """Read a table from the Flowfile catalog.

    Requires a running flowfile_core instance.
    Resolves the table via the catalog API, then reads the
    underlying Delta/Parquet file directly.
    """
```

**Implementation approach:**
- Call `GET /catalog/tables/{table_name}` on flowfile_core to resolve the file path and storage format
- If Delta: `pl.scan_delta(resolved_path, version=delta_version)`
- If Parquet: `pl.scan_parquet(resolved_path)`
- The catalog API already exists (`CatalogService.resolve_table_file_path()`)
- Need a lightweight HTTP client call — reuse the existing `requests` pattern from `flowfile_frame/database.py`

### 1.2 `ff.write_catalog_table()`

**File:** `flowfile_frame/flowfile_frame/catalog.py` (same new file)
**Exposed in:** `flowfile_frame/__init__.py`

```python
def write_catalog_table(
    df: pl.DataFrame | pl.LazyFrame,
    table_name: str,
    namespace_id: int | None = None,
    write_mode: str = "overwrite",  # overwrite | append | upsert | update | delete
    merge_keys: list[str] | None = None,
    description: str | None = None,
) -> None:
    """Write a DataFrame to the Flowfile catalog as a Delta table."""
```

**Implementation approach:**
- Collect the LazyFrame if needed
- POST to the catalog write endpoint with the data (Arrow IPC serialized) and settings
- Alternatively: resolve the destination path from catalog, write Delta directly using `deltalake`, then register via API
- The second approach is more robust and avoids serializing large datasets over HTTP
- Reuse `CatalogService.resolve_write_destination()` logic

### 1.3 `ff.read_kafka()`

**File:** `flowfile_frame/flowfile_frame/kafka.py` (new)
**Exposed in:** `flowfile_frame/__init__.py`

```python
def read_kafka(
    connection_name: str,
    topic_name: str,
    max_messages: int = 100_000,
    start_offset: str = "latest",  # earliest | latest
    poll_timeout_seconds: float = 30.0,
    value_format: str = "json",
) -> pl.LazyFrame:
    """Read messages from a Kafka topic using a named Flowfile connection."""
```

**Implementation approach:**
- Resolve the Kafka connection via `GET /kafka/connections/{name}` on flowfile_core (credentials are decrypted server-side)
- Delegate actual consumption to the worker or core Kafka consumer
- Return the consumed messages as a LazyFrame
- This is inherently a "connected" operation — the generated code requires a running Flowfile instance with the named connection configured

---

## Phase 2: Code Generator Handlers

### 2.1 `_handle_catalog_reader`

**File:** `flowfile_core/flowfile_core/flowfile/code_generator/code_generator.py`

```python
def _handle_catalog_reader(
    self, settings: input_schema.NodeCatalogReader, var_name: str, input_vars: dict[str, str]
) -> None:
    self.imports.add("import flowfile as ff")

    table_name = settings.catalog_table_name
    if not table_name and not settings.catalog_table_id:
        self.unsupported_nodes.append((
            settings.node_id, "catalog_reader",
            "Catalog Reader node has no table name or ID configured"
        ))
        return

    self._add_code(f"# Read from catalog table: {table_name or f'id={settings.catalog_table_id}'}")
    self._add_code(f"{var_name} = ff.read_catalog_table(")
    if table_name:
        self._add_code(f'    table_name="{table_name}",')
    if settings.catalog_namespace_id is not None:
        self._add_code(f"    namespace_id={settings.catalog_namespace_id},")
    if settings.delta_version is not None:
        self._add_code(f"    delta_version={settings.delta_version},")
    self._add_code(")")
```

**Complexity:** Low. Straightforward parameter mapping.

### 2.2 `_handle_catalog_writer`

```python
def _handle_catalog_writer(
    self, settings: input_schema.NodeCatalogWriter, var_name: str, input_vars: dict[str, str]
) -> None:
    self.imports.add("import flowfile as ff")

    ws = settings.catalog_write_settings
    input_df = input_vars.get("main", "df")

    if not ws.table_name:
        self.unsupported_nodes.append((
            settings.node_id, "catalog_writer",
            "Catalog Writer node has no table name configured"
        ))
        return

    self._add_code(f"# Write to catalog table: {ws.table_name}")
    self._add_code("ff.write_catalog_table(")
    self._add_code(f"    {input_df}.collect(),")
    self._add_code(f'    table_name="{ws.table_name}",')
    if ws.namespace_id is not None:
        self._add_code(f"    namespace_id={ws.namespace_id},")
    self._add_code(f'    write_mode="{ws.write_mode}",')
    if ws.merge_keys:
        self._add_code(f"    merge_keys={ws.merge_keys},")
    if ws.description:
        self._add_code(f'    description="{ws.description}",')
    self._add_code(")")
    self._add_code(f"{var_name} = {input_df}  # Pass through the input DataFrame")
```

**Complexity:** Low-medium. Straightforward, but needs to handle all 6 write modes.

### 2.3 `_handle_kafka_source`

```python
def _handle_kafka_source(
    self, settings: input_schema.NodeKafkaSource, var_name: str, input_vars: dict[str, str]
) -> None:
    self.imports.add("import flowfile as ff")

    ks = settings.kafka_settings
    connection_name = ks.kafka_connection_name

    if not connection_name and not ks.kafka_connection_id:
        self.unsupported_nodes.append((
            settings.node_id, "kafka_source",
            "Kafka Source node has no connection name or ID configured"
        ))
        return

    self._add_code(f"# Read from Kafka topic: {ks.topic_name}")
    self._add_code(f"{var_name} = ff.read_kafka(")
    if connection_name:
        self._add_code(f'    connection_name="{connection_name}",')
    self._add_code(f'    topic_name="{ks.topic_name}",')
    self._add_code(f"    max_messages={ks.max_messages},")
    self._add_code(f'    start_offset="{ks.start_offset}",')
    self._add_code(f"    poll_timeout_seconds={ks.poll_timeout_seconds},")
    self._add_code(")")
```

**Complexity:** Low for the handler itself. The heavy lifting is in Phase 1 (the `ff.read_kafka()` implementation).

### 2.4 `_handle_python_script`

This is the most complex handler because user code is arbitrary. The approach: wrap the user's code in a function, pass inputs as DataFrames, and capture the output.

```python
def _handle_python_script(
    self, settings: input_schema.NodePythonScript, var_name: str, input_vars: dict[str, str]
) -> None:
    code = settings.python_script_input.code
    cells = settings.python_script_input.cells

    if not code and not cells:
        self.unsupported_nodes.append((
            settings.node_id, "python_script",
            "Python Script node has no code"
        ))
        return

    # Combine cells into a single code block if using notebook mode
    if cells:
        code = "\n".join(cell.source for cell in cells if cell.cell_type == "code")

    # Generate a helper function that wraps the user's code
    func_name = f"_python_script_{settings.node_id}"

    self._add_code(f"def {func_name}(inputs: dict[str, pl.DataFrame]) -> pl.DataFrame:")
    self._add_code(f'    """Auto-generated from Python Script node {settings.node_id}."""')

    # Add the user's code, indented
    for line in code.split("\n"):
        self._add_code(f"    {line}")

    self._add_code("")

    # Build inputs dict
    input_dict_items = []
    for key, df_var in input_vars.items():
        input_dict_items.append(f'"{key}": {df_var}.collect()')

    inputs_str = ", ".join(input_dict_items)
    self._add_code(f"{var_name} = {func_name}({{{inputs_str}}}).lazy()")
```

**Complexity:** High.
- The user's code may use kernel-specific APIs (`read_input()`, `publish_output()`, `publish_artifact()`). These won't exist in standalone mode.
- **Mitigation:** Add a comment warning that kernel-specific APIs are not available, and provide a compatibility shim that maps `read_input(name)` to `inputs[name]`.
- Multi-output nodes (`output_names: ["main", "secondary"]`) need special handling — the function would need to return a dict, and the code generator would need to destructure it.

**Limitations to document:**
- Kernel-specific APIs (artifacts, shared files) won't work
- `pip install` requirements from the kernel won't be auto-installed
- Multi-output scripts need the function to return a dict of DataFrames

---

## Phase 3: Handle Inline Database Connections

Currently, `database_reader` and `database_writer` fail with inline connections. With `ff` as a dependency, we can support inline connections by emitting the connection string directly.

**However:** This is a security concern — inline connections contain credentials. The generated code would contain plaintext connection strings.

**Recommendation:** Keep the current behavior (require reference mode). Add a clearer error message suggesting the user switch to a named connection. Optionally, add a `--include-credentials` flag to the code generator for users who explicitly want inline connections in their generated code.

---

## Phase 4: Tests

For each new handler, add tests following the existing pattern in:
- `flowfile_core/tests/test_code_generator.py`
- `flowfile_core/tests/test_code_generator_edge_cases.py`

### Test cases per handler:

**catalog_reader:**
- Read by table name
- Read by table name + namespace
- Read with delta version pinned
- Missing table name → UnsupportedNodeError

**catalog_writer:**
- Write with overwrite mode
- Write with upsert mode + merge keys
- Write with append mode
- Missing table name → UnsupportedNodeError

**kafka_source:**
- Read with connection name
- Read with all parameters
- Missing connection → UnsupportedNodeError

**python_script:**
- Simple single-input, single-output script
- Multi-input script
- Notebook cells mode
- Empty code → UnsupportedNodeError
- Script using kernel-specific APIs → warning comment in output

---

## Execution Order

1. **Phase 1** first — the `ff` API functions must exist before the code generator can emit calls to them
2. **Phase 2** depends on Phase 1 — add handlers to the code generator
3. **Phase 3** is independent — can be done anytime
4. **Phase 4** should accompany each phase

## Estimated Scope

| Phase | Files changed | New files | Difficulty |
|-------|--------------|-----------|------------|
| Phase 1 | `flowfile_frame/__init__.py` | `flowfile_frame/catalog.py`, `flowfile_frame/kafka.py` | Medium |
| Phase 2 | `code_generator.py` | — | Medium (High for python_script) |
| Phase 3 | `code_generator.py` | — | Low |
| Phase 4 | `test_code_generator.py` | — | Low |

## Open Questions

1. **`python_script` multi-output:** Should the generated code return a dict and destructure, or should we only support single-output scripts initially?
2. **`read_kafka` in flowfile_frame:** Should this call flowfile_core's API, or should it have its own Kafka consumer? Using the core API is simpler and keeps credentials managed, but requires a running instance.
3. **`write_catalog_table` approach:** Direct Delta write + API registration, or full API-based write? Direct write is faster for large datasets.
4. **Code generator mode flag:** Should we add a flag to the converter like `allow_flowfile_imports=True` (default) vs `standalone_only=True` that would error on nodes requiring `ff`? This preserves the "pure Polars" export option for simpler flows.
