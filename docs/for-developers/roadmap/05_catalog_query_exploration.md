# Feature 5: Catalog Query & Data Exploration

## Motivation

The Flowfile catalog stores registered tables with rich metadata, but users cannot query them interactively. To explore data, users must build a flow with a `catalog_reader` → `explore_data` node chain. There is no way to:

- Run ad-hoc SQL queries against catalog tables.
- Visually explore distributions, correlations, and patterns without building a flow.
- Compare data across tables or versions.
- Save and share commonly used queries.

This feature adds a SQL query interface and enhanced visual exploration (GraphicWalker) directly in the catalog UI.

## Current State

- **Catalog service** (`catalog/service.py`): Manages table registration, metadata, and materialization. Tables are stored as Parquet files (or Delta tables after Feature 3). The service can list tables, get schemas, and read data.
- **GraphicWalker integration** (`graphic_walker.py`): Basic — converts `FlowfileColumn` types to GraphicWalker `MutField` metadata. Provides `get_gf_data_from_ff()` to create a `DataModel` from a LazyFrame. Currently used by the `explore_data` node.
- **`explore_data` node**: An output node type that opens the GraphicWalker UI for interactive exploration. Requires building a flow to use.
- **No SQL endpoint**: There is no API to execute SQL queries against catalog tables.
- **Polars SQL context**: Polars provides `pl.SQLContext` for registering DataFrames and running SQL queries. Not currently used in the codebase.

## Proposed Design

### Part 1: SQL Query Interface

**New API endpoints** (`main.py` or new `catalog/query.py` router):

```python
@router.post("/catalog/query")
async def execute_catalog_query(request: CatalogQueryRequest) -> CatalogQueryResponse:
    """Execute a SQL query against registered catalog tables."""
    ...

@router.get("/catalog/tables/{table_id}/preview")
async def preview_catalog_table(table_id: int, limit: int = 100) -> CatalogQueryResponse:
    """Quick preview of a catalog table."""
    ...

@router.post("/catalog/queries/save")
async def save_query(request: SaveQueryRequest) -> SavedQuery:
    """Save a named query for reuse."""
    ...

@router.get("/catalog/queries")
async def list_saved_queries(namespace_id: int | None = None) -> list[SavedQuery]:
    """List saved queries."""
    ...
```

**Query execution model**:

```python
class CatalogQueryRequest(BaseModel):
    sql: str                                # SQL query string
    namespace_id: int | None = None         # scope for unqualified table names
    limit: int = 1000                       # result row limit
    timeout_seconds: int = 30               # query timeout

class CatalogQueryResponse(BaseModel):
    columns: list[FlowfileColumn]           # result schema
    data: list[dict]                        # result rows (JSON-serializable)
    row_count: int                          # total rows returned
    truncated: bool                         # whether limit was hit
    execution_time_ms: int                  # query duration
```

**Execution engine** (new module: `catalog/query_engine.py`):

```python
class CatalogQueryEngine:
    def __init__(self, catalog_service: CatalogService):
        self.catalog = catalog_service

    def execute(self, request: CatalogQueryRequest) -> CatalogQueryResponse:
        # 1. Build Polars SQLContext
        ctx = pl.SQLContext()

        # 2. Register all tables in the target namespace as LazyFrames
        for table in self.catalog.list_tables(namespace_id=request.namespace_id):
            lf = pl.scan_parquet(table.file_path)  # or scan_delta after Feature 3
            ctx.register(table.name, lf)

        # 3. Execute query with lazy evaluation
        result_lf = ctx.execute(request.sql)

        # 4. Collect with limit
        result_df = result_lf.limit(request.limit).collect()

        # 5. Return response
        return CatalogQueryResponse(
            columns=df_to_flowfile_columns(result_df),
            data=result_df.to_dicts(),
            row_count=len(result_df),
            truncated=len(result_df) >= request.limit,
            execution_time_ms=...,
        )
```

### Part 2: Enhanced GraphicWalker Exploration

**Upgrade the GraphicWalker integration** from basic field mapping to full interactive exploration:

**`graphic_walker.py` enhancements**:

```python
def get_catalog_table_exploration(table_id: int, sample_size: int = 10000) -> DataModel:
    """Create a full GraphicWalker DataModel for a catalog table."""
    # 1. Read table (sample if large)
    # 2. Compute column statistics (min, max, mean, nulls, unique count)
    # 3. Build DataModel with typed fields and data
    # 4. Return for frontend rendering

def get_query_result_exploration(query_result: CatalogQueryResponse) -> DataModel:
    """Create a GraphicWalker DataModel from a SQL query result."""
    # Convert query response to DataModel for visual exploration
```

**New API endpoints**:

```python
@router.get("/catalog/tables/{table_id}/explore")
async def explore_catalog_table(table_id: int, sample_size: int = 10000) -> dict:
    """Get GraphicWalker DataModel for interactive exploration."""
    ...

@router.post("/catalog/query/explore")
async def explore_query_result(request: CatalogQueryRequest) -> dict:
    """Execute query and return result as GraphicWalker DataModel."""
    ...
```

### Part 3: Column Statistics & Profiling

**New endpoint for table profiling**:

```python
class ColumnProfile(BaseModel):
    name: str
    data_type: str
    null_count: int
    null_percentage: float
    unique_count: int
    min_value: Any | None
    max_value: Any | None
    mean_value: float | None          # numeric columns only
    std_value: float | None           # numeric columns only
    top_values: list[dict] | None     # categorical: [(value, count), ...]
    histogram: list[dict] | None      # numeric: [(bin_start, bin_end, count), ...]

@router.get("/catalog/tables/{table_id}/profile")
async def profile_catalog_table(table_id: int) -> list[ColumnProfile]:
    """Compute column-level statistics for a catalog table."""
    ...
```

### Frontend Changes

**Catalog Explorer page** (new route: `/catalog/explore`):

1. **SQL Editor pane**:
   - CodeMirror 6 with SQL syntax highlighting.
   - Table name autocomplete from catalog registry.
   - Column name autocomplete from table schemas.
   - Query execution button with loading state.
   - Result grid (AG Grid) below the editor.
   - "Explore" button to send results to GraphicWalker.

2. **Table browser sidebar**:
   - Tree view of namespaces → tables.
   - Click to preview table (first 100 rows).
   - Right-click context menu: "Preview", "Profile", "Explore", "Copy name".

3. **GraphicWalker integration**:
   - Embedded GraphicWalker component for visual exploration.
   - Drag-and-drop fields to build charts, filters, and aggregations.
   - Save chart configurations as "views" on the catalog table.

4. **Query history**:
   - Recent queries panel with execution time and row counts.
   - Click to re-run or edit a previous query.
   - Save frequently used queries with names and descriptions.

## Key Files to Modify

| File | Change |
|------|--------|
| `flowfile_core/flowfile_core/catalog/service.py` | Add query execution, profiling methods |
| `flowfile_core/flowfile_core/catalog/query_engine.py` | NEW: SQL query engine using `pl.SQLContext` |
| `flowfile_core/flowfile_core/flowfile/analytics/graphic_walker.py` | Enhance for catalog tables, add profiling |
| `flowfile_core/flowfile_core/main.py` | Add query, explore, profile endpoints |
| `flowfile_core/flowfile_core/schemas/` | Add query request/response models |
| `flowfile_frontend/` | Catalog explorer page, SQL editor, GraphicWalker embedding |

## Open Questions

1. **Query security**: Should SQL queries be sandboxed? Users could write expensive `CROSS JOIN` queries. Proposed: enforce `LIMIT`, add timeout, and restrict to `SELECT` statements only.
2. **Cross-namespace queries**: Should users be able to join tables from different namespaces? If so, use `namespace.table_name` syntax.
3. **Caching**: Should query results be cached? Useful for repeated exploration, but stale if the underlying table changes. Proposed: cache with TTL, invalidate on table write.
4. **Delta integration**: After Feature 3, SQL queries should support `AT VERSION` or `AT TIMESTAMP` syntax for time travel queries.
