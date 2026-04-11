# Code Generator Review: FlowFrame-First Direction

## Context

This review covers the changes on `feature/improve-support-code-to-flow` that add catalog, Kafka, and unified cloud storage support to both the FlowFrame API and the code generator. These changes are evaluated against the long-term goal of generating FlowFrame code (not raw Polars) from visual flows.

---

## Strategic Alignment

The diff moves in the right direction. The end state is: visual flow -> FlowFrame code -> optional `.to_polars()`. This is stronger than the current visual flow -> raw Polars code, because:

- **Round-trip fidelity**: FlowFrame operations map 1:1 to canvas nodes, enabling code-to-canvas re-import.
- **First-class I/O**: Catalog, cloud storage, Kafka, and database operations are native FlowFrame concepts. They stop being special cases in generated code and become regular method calls.
- **Schema prediction**: FlowFrame tracks schemas through the DAG; raw Polars loses this until execution.
- **Minimal learning curve**: ~99% API overlap with Polars. Users add `.to_polars()` at the end if they want raw frames.

---

## What This Diff Gets Right

| Change | Why it matters |
|--------|---------------|
| `read_database()` returns `FlowFrame` | Aligns with FlowFrame-first API. DB reads are now tracked in the DAG. |
| `write_database()` delegates to `FlowFrame.write_database()` | Removes pandas/SQLAlchemy bypass. Writes go through the flow graph like everything else. |
| Unified `read_from_cloud_storage()` / `write_to_cloud_storage()` | Single entry point per direction. Reduces code generator branching from 4 format-specific blocks to 1. |
| New `read_catalog_table()` / `write_catalog_table()` / `read_kafka()` | Fills API gaps. These were canvas-only; now they work programmatically. |
| Code generator handlers for catalog_reader, catalog_writer, kafka_source | Generated code covers more node types. Good test coverage on the new handlers. |
| `catalog_reader` added to source node types in `flow_graph.py` | Fixes DAG construction — catalog readers are source nodes (no upstream dependency). |

---

## Action Items Before Merge

### 1. Uncomment or replace `test_cloud_storage_writer`

**Priority: High**

The test at `flowfile_core/tests/flowfile/test_code_generator.py` (old lines 714-780) is commented out. The new `write_to_cloud_storage()` API is untested in an integration context (the unit test `test_cloud_storage_writer_handler_unified` only checks code output, not execution).

Options:
- Rewrite the test to use `ff.write_to_cloud_storage()` and verify the file lands in S3.
- If the old test was flaky, add a `@pytest.mark.skip(reason="...")` with a tracking issue instead of commenting.

### 2. Consolidate `get_current_user_id()`

**Priority: Medium**

Three identical copies exist:
- `flowfile_frame/catalog.py`
- `flowfile_frame/kafka.py`
- `flowfile_frame/cloud_storage/secret_manager.py`

All return hardcoded `1`. As FlowFrame becomes the primary interface, user context needs to flow through consistently. Consolidate into a single location (e.g., `flowfile_frame/auth.py` or `flowfile_frame/utils.py`) and import from there.

### 3. Widen `write_database()` input type

**Priority: Medium**

The signature was narrowed from `pl.DataFrame | pl.LazyFrame` to `pl.LazyFrame`. Since FlowFrame is the long-term canonical type, consider accepting all three and coercing internally:

```python
def write_database(
    df: FlowFrame | pl.LazyFrame | pl.DataFrame,
    ...
) -> None:
    if isinstance(df, pl.DataFrame):
        df = df.lazy()
    frame = FlowFrame(data=df) if not isinstance(df, FlowFrame) else df
    ...
```

This keeps the top-level API forgiving during the transition.

### 4. Verify `scan_delta` keyword for version

**Priority: Medium**

`read_from_cloud_storage()` calls:
```python
scan_delta(source, connection_name=connection_name, version=delta_version)
```

The code generator emits `delta_version=` as the parameter name. Confirm that `scan_delta` accepts `version` (not `version_id` or `delta_version`). A mismatch would silently pass `None` or raise a `TypeError`.

### 5. Document the `read_database()` return type change

**Priority: Low** (can be done at release time)

`read_database()` now returns `FlowFrame` instead of `pl.LazyFrame`. This is the correct long-term direction, but existing users may have code like:

```python
df = ff.read_database("conn", table_name="users")
result = df.collect()  # previously pl.LazyFrame.collect()
```

Add a note to the changelog. FlowFrame likely supports `.collect()` already, so breakage may be minimal — but it should be called out.

---

## Temporary Technical Debt (Acceptable)

These items exist because the code generator still targets Polars output. They resolve naturally when the generator switches to FlowFrame-native output.

| Item | Why it's OK for now |
|------|-------------------|
| `.data` suffix on generated `read_database()`, `read_catalog_table()`, `read_kafka()` calls | Extracts the LazyFrame from FlowFrame. Remove when generated code uses FlowFrame variables directly. |
| `df_2 = df_1  # pass-through after write` pattern in cloud/catalog writers | Needed because generated code works with LazyFrames. With FlowFrame output, writes return child frames naturally. |
| Format-specific dispatch inside `read_from_cloud_storage()` / `write_to_cloud_storage()` | These delegate to existing methods. Once the underlying methods are unified further, the dispatch simplifies. |

---

## Roadmap Sketch: Polars Output -> FlowFrame Output

When ready to make the switch in the code generator:

1. **Generated variables become FlowFrames** — Remove `.data` suffixes. `df_1 = ff.read_database(...)` returns a FlowFrame directly.
2. **Chain operations on FlowFrame** — `.filter()`, `.with_columns()`, `.join()`, etc. already exist on FlowFrame. The generated code barely changes.
3. **Writes become method calls** — `df.write_database(...)` instead of `ff.write_database(df, ...)`.
4. **Add `.to_polars()` at the end** — For users who need a raw LazyFrame/DataFrame.
5. **Update `get_result_from_generated_code()` in tests** — The test helper that executes generated code will need to handle FlowFrame results.

The transition can be incremental: start with I/O nodes (already halfway there), then transformations.

---

## Documentation Updates Required

The current docs describe an API surface that this diff changes. Three files need updates before or shortly after merge.

### 1. `docs/users/python-api/reference/reading-data.md`

**What's outdated:**
- No mention of `ff.read_from_cloud_storage()` — the new unified entry point for all cloud formats.
- No mention of `ff.read_catalog_table()` or `ff.read_kafka()`.
- `ff.read_database()` is documented as returning data directly, with no indication it now returns a `FlowFrame` (which matters for chaining and visualization).

**What to add:**

- A **Catalog Reading** section:
  ```python
  df = ff.read_catalog_table("my_table", namespace_id=3)
  df = ff.read_catalog_table("my_table", delta_version=5)  # time travel
  ```

- A **Kafka Reading** section:
  ```python
  df = ff.read_kafka(
      "my-kafka-connection",
      topic_name="events",
      start_offset="earliest",
      max_messages=10_000,
  )
  ```

- A **Unified Cloud Storage** section showing `read_from_cloud_storage()` as the recommended approach, with the format-specific `scan_*` functions kept as alternatives:
  ```python
  # Recommended
  df = ff.read_from_cloud_storage(
      "s3://bucket/data.parquet",
      file_format="parquet",
      connection_name="my-conn",
  )

  # Format-specific alternatives still work
  df = ff.scan_parquet_from_cloud_storage("s3://bucket/data.parquet", connection_name="my-conn")
  ```

- A note under **Database Reading** that `read_database()` now returns a `FlowFrame`, which supports `.collect()`, `.data`, and visualization via `open_graph_in_editor()`.

### 2. `docs/users/python-api/reference/writing-data.md`

**What's outdated:**
- No mention of `ff.write_to_cloud_storage()` — the unified cloud writer.
- No mention of `ff.write_catalog_table()` or `FlowFrame.write_catalog_table()`.
- No mention of `FlowFrame.write_database()` — database writes were previously standalone functions only.

**What to add:**

- A **Catalog Writing** section:
  ```python
  # Standalone function
  ff.write_catalog_table(
      df, "output_table",
      namespace_id=3,
      write_mode="upsert",
      merge_keys=["id"],
  )

  # Or as a method on FlowFrame
  df.write_catalog_table("output_table", write_mode="overwrite")
  ```

- A **Unified Cloud Storage Writing** section showing `write_to_cloud_storage()` as the recommended approach:
  ```python
  ff.write_to_cloud_storage(
      df, "s3://bucket/output.parquet",
      file_format="parquet",
      connection_name="my-conn",
  )
  ```

- A **Database Writing** section showing `FlowFrame.write_database()`:
  ```python
  df.write_database(
      connection_name="my_db",
      table_name="users",
      schema_name="public",
      if_exists="append",
  )
  ```

- Update the write modes section to mention catalog write modes (`overwrite`, `append`, `upsert`, `update`, `delete`) alongside the existing Delta modes.

### 3. `docs/users/visual-editor/tutorials/code-generator.md`

**What's outdated:**
- States generated code is "entirely self-contained and relies mostly on the Polars library" — this is no longer true for flows that include catalog, Kafka, database, or cloud storage nodes. Those generate `import flowfile as ff` and call FlowFrame API functions.
- The "Key Characteristics" section claims code is **Standalone** (only requires Polars). Flows with I/O nodes now require `flowfile` as a dependency.
- All three examples show pure Polars output. There are no examples of generated code for I/O nodes.

**What to change:**

- Update the intro to acknowledge two categories of generated code:
  - **Pure transformation flows** (filter, join, group_by, etc.) generate standalone Polars code with no Flowfile dependency.
  - **Flows with I/O nodes** (database, catalog, cloud storage, Kafka) generate code that uses `flowfile` for connection-aware operations. The transformation logic is still Polars.

- Update "Key Characteristics" to say:
  - **Mostly Standalone**: Transformation logic uses only Polars. I/O operations (database, catalog, cloud, Kafka) use `flowfile` for managed connections.
  - **Ready for Integration**: Pure transformation flows can be embedded anywhere. I/O flows require `pip install Flowfile`.

- Add an **Example 4** showing generated code for a database or catalog read:
  ```python
  import flowfile as ff
  import polars as pl

  def run_etl_pipeline():
      df_1 = ff.read_catalog_table("sales_data", namespace_id=3).data
      df_2 = df_1.filter(pl.col("amount") > 100)
      return df_2
  ```

- Optionally add a forward-looking note: _"In a future release, generated code will use the FlowFrame API directly, enabling round-trip editing between code and canvas."_
