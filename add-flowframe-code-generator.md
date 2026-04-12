# Add FlowFrame Code Generation Mode

## Goal

Add a second code generation mode that outputs FlowFrame code instead of raw Polars. Polars mode stays as-is but refuses I/O nodes (catalog, database, cloud storage, Kafka) — those require FlowFrame mode. The UI lets the user pick which mode to use.

---

## Scope

### Polars mode (existing, with one change)

Polars mode continues to generate standalone code using only `polars` (plus `polars_expr_transformer`, `polars_grouper`, `pl_fuzzy_frame_match` where needed). The one change:

**I/O node types now add to `unsupported_nodes` instead of emitting `ff.*` calls.** These handlers currently generate `import flowfile as ff` code, which contradicts the "standalone Polars" promise:

| Handler | Current behavior | New behavior in Polars mode |
|---------|-----------------|---------------------------|
| `_handle_database_reader` | Emits `ff.read_database(...)` | Add to `unsupported_nodes` with message: "Use FlowFrame code generation for database nodes" |
| `_handle_database_writer` | Emits `ff.write_database(...)` | Same |
| `_handle_cloud_storage_reader` | Emits `ff.read_from_cloud_storage(...)` | Same |
| `_handle_cloud_storage_writer` | Emits `ff.write_to_cloud_storage(...)` | Same |
| `_handle_catalog_reader` | Emits `ff.read_catalog_table(...)` | Same |
| `_handle_catalog_writer` | Emits `ff.write_catalog_table(...)` | Same |
| `_handle_kafka_source` | Emits `ff.read_kafka(...)` | Same |

These are the only handlers that currently emit `import flowfile as ff`. All other handlers use only `polars` and stay unchanged.

### Polars mode — supported node types (no changes needed)

These handlers already emit pure Polars code:

- `_handle_read` (CSV, Parquet, Excel from local files)
- `_handle_manual_input` (list of dicts → `pl.LazyFrame(...)`)
- `_handle_output` (write CSV, Parquet, Excel to local files)
- `_handle_filter`
- `_handle_select`
- `_handle_join` (and all join sub-handlers)
- `_handle_group_by`
- `_handle_formula`
- `_handle_pivot` / `_handle_unpivot`
- `_handle_union`
- `_handle_sort`
- `_handle_sample`
- `_handle_unique`
- `_handle_fuzzy_match`
- `_handle_text_to_rows`
- `_handle_record_id`
- `_handle_record_count`
- `_handle_cross_join`
- `_handle_graph_solver`
- `_handle_polars_code`
- `_handle_explore_data` (pass-through)
- `_handle_user_defined` (custom nodes)

### FlowFrame mode (new)

A new converter class (or mode flag) that generates `flowfile`-based code. The key differences from Polars mode:

**1. Imports**

```python
# Polars mode
import polars as pl

# FlowFrame mode
import flowfile as ff
from flowfile import col
import polars as pl  # still needed for pl.lit(), data types, etc.
```

**2. Source nodes — the only handlers that need FlowFrame-specific code**

| Node type | Polars mode output | FlowFrame mode output |
|-----------|-------------------|----------------------|
| `read` (local CSV) | `df_1 = pl.scan_csv("data.csv", ...)` | `df_1 = ff.read_csv("data.csv", ...)` |
| `read` (local Parquet) | `df_1 = pl.scan_parquet("data.parquet")` | `df_1 = ff.read_parquet("data.parquet")` |
| `read` (local Excel) | `df_1 = pl.read_excel("data.xlsx", ...).lazy()` | `df_1 = ff.read_excel("data.xlsx", ...)` |
| `manual_input` | `df_1 = pl.LazyFrame({...})` | `df_1 = ff.from_dict({...})` |
| `database_reader` | *unsupported* | `df_1 = ff.read_database("conn", ...)` |
| `catalog_reader` | *unsupported* | `df_1 = ff.read_catalog_table("table", ...)` |
| `cloud_storage_reader` | *unsupported* | `df_1 = ff.read_from_cloud_storage("s3://...", ...)` |
| `kafka_source` | *unsupported* | `df_1 = ff.read_kafka("conn", ...)` |

**3. Transformation nodes — no changes needed**

FlowFrame supports the same method signatures as Polars LazyFrame. These handlers emit code like:

```python
df_2 = df_1.filter(pl.col("x") > 5)
df_3 = df_2.select([pl.col("a"), pl.col("b").alias("c")])
df_4 = df_3.group_by(["category"]).agg([pl.col("value").sum()])
```

This works identically on both `pl.LazyFrame` and `FlowFrame`. **All ~20 transformation handlers are reused as-is.**

One consideration: `pl.col(...)` expressions still work inside FlowFrame methods. No need to change them to `ff.col(...)` — though using `col(...)` from the `from flowfile import col` import would also work and be slightly cleaner.

**4. Sink nodes — method calls instead of standalone functions**

| Node type | Polars mode output | FlowFrame mode output |
|-----------|-------------------|----------------------|
| `output` (local CSV) | `df.sink_csv("out.csv", ...)` | `df.write_csv("out.csv", ...)` |
| `output` (local Parquet) | `df.sink_parquet("out.parquet")` | `df.write_parquet("out.parquet")` |
| `output` (local Excel) | `df.collect().write_excel("out.xlsx", ...)` | `df.write_excel("out.xlsx", ...)` |
| `database_writer` | *unsupported* | `df.write_database("conn", "table", ...)` |
| `catalog_writer` | *unsupported* | `df.write_catalog_table("table", ...)` |
| `cloud_storage_writer` | *unsupported* | `df.write_to_cloud_storage("s3://...", ...)` |

**5. Return value**

```python
# Polars mode
return df_5  # pl.LazyFrame

# FlowFrame mode
return df_5  # FlowFrame (user can call .collect() or .data as needed)
```

---

## Implementation Plan

### Step 1: Add `target` parameter to converter

Add a `target: Literal["polars", "flowframe"]` parameter to `FlowGraphToPolarsConverter.__init__`. Default to `"polars"` so existing behavior is unchanged.

```python
def __init__(self, flow_graph: FlowGraph, target: Literal["polars", "flowframe"] = "polars"):
    self.target = target
    self.imports = {"import polars as pl"}
    if target == "flowframe":
        self.imports.add("import flowfile as ff")
        self.imports.add("from flowfile import col")
```

### Step 2: Gate I/O handlers in Polars mode

In the 7 I/O handlers (`_handle_database_reader`, `_handle_database_writer`, `_handle_cloud_storage_reader`, `_handle_cloud_storage_writer`, `_handle_catalog_reader`, `_handle_catalog_writer`, `_handle_kafka_source`), add an early return when in Polars mode:

```python
def _handle_database_reader(self, settings, var_name, input_vars):
    if self.target == "polars":
        self.unsupported_nodes.append((
            settings.node_id, "database_reader",
            "Database nodes require FlowFrame code generation. Select 'FlowFrame' as the output format."
        ))
        return
    # ... existing FlowFrame code generation (remove .data suffix) ...
```

Remove the `.data` suffixes and pass-through hacks from these handlers — they only run in FlowFrame mode now.

### Step 3: Add FlowFrame variants for source/sink handlers

For handlers that exist in both modes (`_handle_read`, `_handle_manual_input`, `_handle_output`), add a mode check:

```python
def _handle_read(self, settings, var_name, input_vars):
    file_settings = settings.received_file
    if self.target == "flowframe":
        self._handle_read_flowframe(file_settings, var_name)
    else:
        # existing Polars code generation (unchanged)
        ...

def _handle_read_flowframe(self, file_settings, var_name):
    if file_settings.file_type == "csv":
        self._add_code(f'{var_name} = ff.read_csv("{file_settings.abs_file_path}",')
        # ... same parameters as Polars mode ...
        self._add_code(")")
    elif file_settings.file_type == "parquet":
        self._add_code(f'{var_name} = ff.read_parquet("{file_settings.abs_file_path}")')
    elif file_settings.file_type in ("xlsx", "excel"):
        self._add_code(f'{var_name} = ff.read_excel("{file_settings.abs_file_path}",')
        if file_settings.table_settings.sheet_name:
            self._add_code(f'    sheet_name="{file_settings.table_settings.sheet_name}",')
        self._add_code(")")
    self._add_code("")
```

Similar pattern for `_handle_manual_input` (`pl.LazyFrame({...})` → `ff.from_dict({...})`) and `_handle_output`.

### Step 4: Add `export_flow_to_flowframe` entry point

```python
def export_flow_to_flowframe(flow_graph: FlowGraph) -> str:
    converter = FlowGraphToPolarsConverter(flow_graph, target="flowframe")
    return converter.convert()
```

### Step 5: Add API endpoint

In `flowfile_core/flowfile_core/routes/routes.py`, add a new endpoint alongside the existing one:

```python
@router.get("/editor/code_to_flowframe", tags=[], response_model=str)
def get_generated_flowframe_code(flow_id: int) -> str:
    """Generates FlowFrame code representing the flow."""
    flow = flow_file_handler.get_flow(int(flow_id))
    if flow is None:
        raise HTTPException(404, "could not find the flow")
    return export_flow_to_flowframe(flow)
```

The existing `/editor/code_to_polars` endpoint stays unchanged.

### Step 6: Add UI toggle

In `flowfile_frontend/src/renderer/app/views/DesignerView/CodeGenerator/CodeGenerator.vue`:

- Add a toggle/dropdown to switch between "Polars" and "FlowFrame" output modes.
- Change the `fetchCode` function to call the appropriate endpoint based on the selected mode.
- Default to "FlowFrame" (since it covers all node types).
- Update the export filename: `pipeline_code.py` for both, or `pipeline_polars.py` / `pipeline_flowframe.py` to distinguish.

### Step 7: Tests

For each handler that has FlowFrame-specific behavior, add a test that verifies the generated code. The test pattern:

```python
def test_read_csv_flowframe_mode():
    flow = create_basic_flow()
    # ... set up a CSV read node ...
    code = export_flow_to_flowframe(flow)
    assert "ff.read_csv(" in code
    assert "pl.scan_csv(" not in code
    result = get_result_from_generated_code(code)
    # ... verify result ...
```

Tests for I/O nodes in Polars mode should verify they raise `UnsupportedNodeError`:

```python
def test_database_reader_polars_mode_unsupported():
    flow = create_basic_flow()
    # ... set up a database reader node ...
    with pytest.raises(UnsupportedNodeError):
        export_flow_to_polars(flow)
```

---

## Files to modify

| File | Change |
|------|--------|
| `flowfile_core/flowfile_core/flowfile/code_generator/code_generator.py` | Add `target` param, FlowFrame variants for source/sink handlers, gate I/O in Polars mode, add `export_flow_to_flowframe` |
| `flowfile_core/flowfile_core/flowfile/code_generator/__init__.py` | Export `export_flow_to_flowframe` |
| `flowfile_core/flowfile_core/routes/routes.py` | Add `/editor/code_to_flowframe` endpoint |
| `flowfile_frontend/.../CodeGenerator/CodeGenerator.vue` | Add Polars/FlowFrame toggle, call correct endpoint |
| `flowfile_core/tests/flowfile/test_code_generator.py` | Add FlowFrame mode tests, add Polars mode unsupported-node tests |

## Files NOT modified

All transformation handlers stay exactly as they are. The `_build_final_code`, `convert`, `_generate_node_code`, `_get_input_vars`, and `add_return_code` methods are mode-agnostic and don't need changes.

---

## Handler change summary

| Handler | Polars mode | FlowFrame mode | Code change needed |
|---------|-------------|----------------|-------------------|
| `_handle_read` | `pl.scan_csv(...)` | `ff.read_csv(...)` | Add FlowFrame branch |
| `_handle_manual_input` | `pl.LazyFrame({...})` | `ff.from_dict({...})` | Add FlowFrame branch |
| `_handle_output` | `df.sink_csv(...)` | `df.write_csv(...)` | Add FlowFrame branch |
| `_handle_formula` | unchanged | unchanged | None |
| `_handle_filter` | unchanged | unchanged | None |
| `_handle_select` | unchanged | unchanged | None |
| `_handle_join` | unchanged | unchanged | None |
| `_handle_group_by` | unchanged | unchanged | None |
| `_handle_sort` | unchanged | unchanged | None |
| `_handle_pivot` | unchanged | unchanged | None |
| `_handle_unpivot` | unchanged | unchanged | None |
| `_handle_union` | unchanged | unchanged | None |
| `_handle_sample` | unchanged | unchanged | None |
| `_handle_unique` | unchanged | unchanged | None |
| `_handle_fuzzy_match` | unchanged | unchanged | None |
| `_handle_text_to_rows` | unchanged | unchanged | None |
| `_handle_record_id` | unchanged | unchanged | None |
| `_handle_record_count` | unchanged | unchanged | None |
| `_handle_cross_join` | unchanged | unchanged | None |
| `_handle_graph_solver` | unchanged | unchanged | None |
| `_handle_polars_code` | unchanged | unchanged | None |
| `_handle_explore_data` | unchanged | unchanged | None |
| `_handle_user_defined` | unchanged | unchanged | None |
| `_handle_database_reader` | unsupported | `ff.read_database(...)` | Add gate + remove `.data` |
| `_handle_database_writer` | unsupported | `df.write_database(...)` | Add gate + remove pass-through |
| `_handle_cloud_storage_reader` | unsupported | `ff.read_from_cloud_storage(...)` | Add gate + remove `.data` |
| `_handle_cloud_storage_writer` | unsupported | `df.write_to_cloud_storage(...)` | Add gate + remove pass-through |
| `_handle_catalog_reader` | unsupported | `ff.read_catalog_table(...)` | Add gate + remove `.data` |
| `_handle_catalog_writer` | unsupported | `df.write_catalog_table(...)` | Add gate + remove pass-through |
| `_handle_kafka_source` | unsupported | `ff.read_kafka(...)` | Add gate + remove `.data` |

**Handlers that need changes: 10** (3 dual-mode + 7 I/O gated)
**Handlers unchanged: 23**
