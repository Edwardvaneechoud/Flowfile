import { defineStore } from 'pinia'
import { ref, shallowRef } from 'vue'

declare global {
  interface Window {
    loadPyodide: (config?: any) => Promise<any>
  }
}

export const usePyodideStore = defineStore('pyodide', () => {
  const pyodide = shallowRef<any>(null)
  const isReady = ref(false)
  const isLoading = ref(false)
  const loadingStatus = ref('')
  const error = ref<string | null>(null)

  async function initialize() {
    if (isReady.value || isLoading.value) {
      return
    }

    isLoading.value = true
    error.value = null

    try {
      loadingStatus.value = 'Loading Pyodide...'

      // Load Pyodide from CDN - using v0.27.7 which is the last version with Polars support
      const script = document.createElement('script')
      script.src = 'https://cdn.jsdelivr.net/pyodide/v0.27.7/full/pyodide.js'
      document.head.appendChild(script)

      await new Promise<void>((resolve, reject) => {
        script.onload = () => resolve()
        script.onerror = () => reject(new Error('Failed to load Pyodide script'))
      })

      loadingStatus.value = 'Initializing Python runtime...'
      pyodide.value = await window.loadPyodide({
        indexURL: 'https://cdn.jsdelivr.net/pyodide/v0.27.7/full/'
      })

      loadingStatus.value = 'Installing packages...'
      await pyodide.value.loadPackage(['numpy', 'polars', 'pydantic'])

      loadingStatus.value = 'Setting up execution engine...'
      await setupExecutionEngine()

      isReady.value = true
      loadingStatus.value = 'Ready'
    } catch (err) {
      error.value = err instanceof Error ? err.message : 'Failed to initialize Pyodide'
      console.error('Pyodide initialization error:', err)
    } finally {
      isLoading.value = false
    }
  }

  async function setupExecutionEngine() {
    await pyodide.value.runPythonAsync(`
import polars as pl
import json
import gc
from typing import Dict, List, Any, Optional, Union, Tuple
from hashlib import md5
from collections import OrderedDict

# =============================================================================
# Global Storage: LazyFrames + Preview Cache
# =============================================================================

# Store LazyFrames (query plans, not materialized data)
_lazyframes: Dict[int, pl.LazyFrame] = {}

# Cache for materialized previews (only computed on demand)
# Using OrderedDict for LRU eviction
_preview_cache: OrderedDict[int, Dict] = OrderedDict()
_PREVIEW_CACHE_MAX_SIZE = 20  # Max number of cached previews
_PREVIEW_CACHE_MAX_MEMORY_MB = 50  # Approximate max memory for preview cache

# Track query plan hashes to invalidate cache when upstream changes
_plan_hashes: Dict[int, str] = {}

# Schema cache (can be obtained from LazyFrame without collecting)
_schemas: Dict[int, List[Dict[str, str]]] = {}


def _estimate_preview_size_mb(preview_data: Dict) -> float:
    """Estimate memory size of preview data in MB."""
    try:
        data = preview_data.get("data", [])
        if not data:
            return 0.001
        # Rough estimate: each cell ~50 bytes average
        num_cells = len(data) * len(data[0]) if data and len(data) > 0 else 0
        return (num_cells * 50) / (1024 * 1024)
    except:
        return 0.1


def _evict_preview_cache_if_needed():
    """Evict oldest entries if cache exceeds limits."""
    # Evict by count
    while len(_preview_cache) > _PREVIEW_CACHE_MAX_SIZE:
        _preview_cache.popitem(last=False)

    # Evict by estimated memory
    total_mb = sum(_estimate_preview_size_mb(v) for v in _preview_cache.values())
    while total_mb > _PREVIEW_CACHE_MAX_MEMORY_MB and len(_preview_cache) > 1:
        _preview_cache.popitem(last=False)
        total_mb = sum(_estimate_preview_size_mb(v) for v in _preview_cache.values())


def _get_plan_hash(lf: pl.LazyFrame) -> str:
    """Get a hash of the query plan to detect changes."""
    try:
        plan_str = str(lf.explain(optimized=True))
        return md5(plan_str.encode()).hexdigest()[:16]
    except:
        return str(id(lf))


def store_lazyframe(node_id: int, lf: pl.LazyFrame):
    """Store a LazyFrame for a node. Invalidates preview cache if plan changed."""
    # Get schema from LazyFrame (doesn't require collection!)
    schema = lf.collect_schema()
    _schemas[node_id] = [
        {"name": name, "data_type": str(dtype)}
        for name, dtype in schema.items()
    ]

    # Check if plan changed - if so, invalidate preview cache
    new_hash = _get_plan_hash(lf)
    old_hash = _plan_hashes.get(node_id)

    if old_hash != new_hash:
        _preview_cache.pop(node_id, None)
        _plan_hashes[node_id] = new_hash

    _lazyframes[node_id] = lf


def get_lazyframe(node_id: int) -> Optional[pl.LazyFrame]:
    """Get a LazyFrame for a node."""
    return _lazyframes.get(node_id)


def get_schema(node_id: int) -> List[Dict[str, str]]:
    """Get schema for a node (available without collecting)."""
    return _schemas.get(node_id, [])


def has_cached_preview(node_id: int) -> bool:
    """Check if a node has a cached preview."""
    return node_id in _preview_cache


def get_cached_preview(node_id: int) -> Optional[Dict]:
    """Get cached preview data if available."""
    return _preview_cache.get(node_id)


def materialize_preview(node_id: int, max_rows: int = 100) -> Dict:
    """
    Materialize a preview for a node (on-demand).
    This is the expensive operation - only called when user clicks to view.
    Memory-optimized: collects once with row count included.
    """
    lf = _lazyframes.get(node_id)
    if lf is None:
        return {"error": f"No LazyFrame found for node #{node_id}"}

    try:
        # Optimized: Add row number to get total count in single collection
        # This avoids the double .collect() call
        lf_with_count = lf.with_row_index("__row_idx__")
        preview_df = lf_with_count.head(max_rows).collect()

        # Get total rows from a separate lightweight query (just count, no data)
        try:
            total_rows = lf.select(pl.len()).collect().item()
        except:
            # Fallback: if we got max_rows, there's probably more
            total_rows = len(preview_df)

        # Remove the temporary index column before returning
        if "__row_idx__" in preview_df.columns:
            preview_df = preview_df.drop("__row_idx__")

        preview_data = {
            "columns": preview_df.columns,
            "data": preview_df.to_numpy().tolist() if len(preview_df) > 0 else [],
            "total_rows": total_rows,
            "preview_rows": len(preview_df)
        }

        # Explicitly delete the DataFrame to free memory immediately
        del preview_df

        # Cache the preview with LRU eviction
        _preview_cache[node_id] = preview_data
        _preview_cache.move_to_end(node_id)  # Mark as recently used
        _evict_preview_cache_if_needed()

        return preview_data
    except Exception as e:
        return {"error": str(e)}


def fetch_preview(node_id: int, max_rows: int = 100, force_refresh: bool = False) -> Dict:
    """
    Fetch preview data for a node. Called when user clicks to view.
    Uses LRU cache with eviction.
    """
    if not force_refresh and has_cached_preview(node_id):
        cached = get_cached_preview(node_id)
        # Mark as recently used for LRU
        if node_id in _preview_cache:
            _preview_cache.move_to_end(node_id)
        return {
            "success": True,
            "data": cached,
            "from_cache": True
        }

    preview_data = materialize_preview(node_id, max_rows)

    if "error" in preview_data:
        return {
            "success": False,
            "error": preview_data["error"]
        }

    return {
        "success": True,
        "data": preview_data,
        "from_cache": False
    }


def invalidate_downstream_previews(node_id: int, node_graph: Dict[int, List[int]]):
    """
    Invalidate preview cache for a node and all its downstream dependents.
    """
    to_invalidate = [node_id]
    visited = set()

    while to_invalidate:
        current = to_invalidate.pop(0)
        if current in visited:
            continue
        visited.add(current)

        _preview_cache.pop(current, None)

        downstream = node_graph.get(current, [])
        to_invalidate.extend(downstream)


def clear_node(node_id: int):
    """Clear all data for a node."""
    _lazyframes.pop(node_id, None)
    _schemas.pop(node_id, None)
    _preview_cache.pop(node_id, None)
    _plan_hashes.pop(node_id, None)


def clear_all():
    """Clear all data."""
    _lazyframes.clear()
    _schemas.clear()
    _preview_cache.clear()
    _plan_hashes.clear()
    gc.collect()


def run_gc():
    """Force garbage collection. Call after heavy operations."""
    gc.collect()
    return {"freed": True}


def get_memory_stats() -> Dict:
    """Get memory statistics for debugging."""
    import sys
    return {
        "lazyframes_count": len(_lazyframes),
        "preview_cache_count": len(_preview_cache),
        "schemas_count": len(_schemas),
        "plan_hashes_count": len(_plan_hashes),
        "preview_cache_size_estimate_mb": sum(_estimate_preview_size_mb(v) for v in _preview_cache.values())
    }


# =============================================================================
# Pydantic Schema Validation (matching flowfile_core/schemas/schemas.py)
# =============================================================================
from pydantic import BaseModel, Field
from typing import Literal

ExecutionModeLiteral = Literal["Development", "Performance"]
ExecutionLocationsLiteral = Literal["local", "remote"]

# Fields to exclude from setting_input when serializing
SETTING_INPUT_EXCLUDE = {
    "flow_id", "node_id", "pos_x", "pos_y", "is_setup",
    "description", "user_id", "is_flow_output",
    "is_user_defined", "depending_on_id", "depending_on_ids"
}

class FlowfileSettings(BaseModel):
    """Settings for flowfile serialization (YAML/JSON)."""
    description: Optional[str] = None
    execution_mode: ExecutionModeLiteral = "Performance"
    execution_location: ExecutionLocationsLiteral = "local"
    auto_save: bool = False
    show_detailed_progress: bool = True

class FlowfileNode(BaseModel):
    """Node representation for flowfile serialization."""
    id: int
    type: str
    is_start_node: bool = False
    description: Optional[str] = ""
    x_position: Optional[float] = 0
    y_position: Optional[float] = 0
    left_input_id: Optional[int] = None
    right_input_id: Optional[int] = None
    input_ids: Optional[List[int]] = Field(default_factory=list)
    outputs: Optional[List[int]] = Field(default_factory=list)
    setting_input: Optional[Any] = None

class FlowfileData(BaseModel):
    """Root model for flowfile serialization (YAML/JSON)."""
    flowfile_version: str
    flowfile_id: int
    flowfile_name: str
    flowfile_settings: FlowfileSettings
    nodes: List[FlowfileNode]

def validate_flowfile_data(data: Dict) -> Dict:
    """Validate flowfile data using Pydantic schemas.

    Returns a dict with:
    - success: bool
    - data: validated data (if successful)
    - error: error message (if failed)
    """
    try:
        validated = FlowfileData.model_validate(data)
        return {
            "success": True,
            "data": validated.model_dump(),
            "error": None
        }
    except Exception as e:
        return {
            "success": False,
            "data": None,
            "error": str(e)
        }

def clean_setting_input(settings: Dict) -> Dict:
    """Clean setting_input by removing excluded fields."""
    if settings is None:
        return None
    return {k: v for k, v in settings.items() if k not in SETTING_INPUT_EXCLUDE}

def prepare_node_for_export(node: Dict) -> Dict:
    """Prepare a node for export by cleaning setting_input."""
    result = dict(node)
    if "setting_input" in result and result["setting_input"]:
        result["setting_input"] = clean_setting_input(result["setting_input"])
    return result

def df_to_preview(df: pl.DataFrame, max_rows: int = 100) -> Dict:
    """Convert dataframe to preview format"""
    preview_df = df.head(max_rows)
    return {
        "columns": df.columns,
        "data": preview_df.to_numpy().tolist() if len(preview_df) > 0 else [],
        "total_rows": len(df)
    }

def format_error(node_type: str, node_id: int, error: Exception, df: Optional[pl.DataFrame] = None, column: str = None) -> str:
    """Format error message with context and suggestions"""
    error_str = str(error)
    msg_parts = [f"{node_type.replace('_', ' ').title()} error on node #{node_id}:"]

    # Check for common column-related errors
    column_keywords = ['column', 'ColumnNotFoundError', 'not found', 'SchemaError']
    is_column_error = any(kw.lower() in error_str.lower() for kw in column_keywords)

    if is_column_error and df is not None:
        available_cols = df.columns
        msg_parts.append(f"'{column or 'unknown'}' - {error_str}")
        msg_parts.append(f"Available columns: {', '.join(available_cols)}")

        # Try to suggest similar column names
        if column:
            similar = [c for c in available_cols if column.lower() in c.lower() or c.lower() in column.lower()]
            if similar:
                msg_parts.append(f"Did you mean: {', '.join(similar)}?")
    else:
        msg_parts.append(error_str)

    # Add suggestions based on error type
    if 'type' in error_str.lower() and 'cannot' in error_str.lower():
        msg_parts.append("Suggestion: Check that the column data types match the operation.")
    elif 'null' in error_str.lower() or 'none' in error_str.lower():
        msg_parts.append("Suggestion: Consider filtering out null values first.")
    elif 'parse' in error_str.lower() or 'csv' in error_str.lower():
        msg_parts.append("Suggestion: Check your CSV delimiter and header settings.")

    return " ".join(msg_parts)

def format_error_lf(node_type: str, node_id: int, error: Exception, lf: Optional[pl.LazyFrame] = None, column: str = None) -> str:
    """Format error message with context for LazyFrame operations"""
    error_str = str(error)
    msg_parts = [f"{node_type.replace('_', ' ').title()} error on node #{node_id}:"]

    column_keywords = ['column', 'ColumnNotFoundError', 'not found', 'SchemaError']
    is_column_error = any(kw.lower() in error_str.lower() for kw in column_keywords)

    if is_column_error and lf is not None:
        try:
            schema = lf.collect_schema()
            available_cols = list(schema.keys())
            msg_parts.append(f"'{column or 'unknown'}' - {error_str}")
            msg_parts.append(f"Available columns: {', '.join(available_cols)}")

            if column:
                similar = [c for c in available_cols if column.lower() in c.lower() or c.lower() in column.lower()]
                if similar:
                    msg_parts.append(f"Did you mean: {', '.join(similar)}?")
        except:
            msg_parts.append(error_str)
    else:
        msg_parts.append(error_str)

    if 'type' in error_str.lower() and 'cannot' in error_str.lower():
        msg_parts.append("Suggestion: Check that the column data types match the operation.")
    elif 'null' in error_str.lower() or 'none' in error_str.lower():
        msg_parts.append("Suggestion: Consider filtering out null values first.")
    elif 'parse' in error_str.lower() or 'csv' in error_str.lower():
        msg_parts.append("Suggestion: Check your CSV delimiter and header settings.")

    return " ".join(msg_parts)


# =============================================================================
# Node execution functions (LazyFrame-based)
# =============================================================================

def execute_read_csv(node_id: int, file_content: str, settings: Dict) -> Dict:
    """Execute read CSV node - creates a LazyFrame"""
    try:
        import io
        table_settings = settings.get("received_file", {}).get("table_settings", {})

        # Source nodes: read into DataFrame first, then convert to lazy
        df = pl.read_csv(
            io.StringIO(file_content),
            has_header=table_settings.get("has_headers", True),
            separator=table_settings.get("delimiter", ","),
            skip_rows=table_settings.get("starting_from_line", 0)
        )
        lf = df.lazy()
        store_lazyframe(node_id, lf)
        return {"success": True, "schema": get_schema(node_id), "has_data": True}
    except Exception as e:
        return {"success": False, "error": format_error("read", node_id, e)}


def execute_manual_input(node_id: int, data_content: str, settings: Dict) -> Dict:
    """Execute manual input node - creates a LazyFrame"""
    try:
        import io

        raw_data_format = settings.get("raw_data_format")
        if raw_data_format and raw_data_format.get("columns") and raw_data_format.get("data"):
            columns_meta = raw_data_format["columns"]
            data = raw_data_format["data"]

            if len(columns_meta) > 0 and len(data) > 0:
                col_names = [c["name"] for c in columns_meta]
                df_dict = {name: values for name, values in zip(col_names, data)}
                df = pl.DataFrame(df_dict)
            else:
                df = pl.DataFrame()
        else:
            manual_input = settings.get("manual_input", {})
            has_headers = manual_input.get("has_headers", True)
            delimiter = manual_input.get("delimiter", ",")

            df = pl.read_csv(
                io.StringIO(data_content),
                has_header=has_headers,
                separator=delimiter
            )

        lf = df.lazy()
        store_lazyframe(node_id, lf)
        return {"success": True, "schema": get_schema(node_id), "has_data": True}
    except Exception as e:
        return {"success": False, "error": format_error("manual_input", node_id, e)}


def convert_filter_value(value: str, dtype) -> any:
    """Convert a single filter value to match column data type"""
    if dtype in (pl.Int8, pl.Int16, pl.Int32, pl.Int64, pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64):
        return int(value)
    elif dtype in (pl.Float32, pl.Float64):
        return float(value)
    elif dtype == pl.Boolean:
        return value.lower() == 'true'
    return value

def convert_filter_values(values: list[str], dtype) -> list:
    """Convert filter values to match column data type"""
    return [convert_filter_value(v, dtype) for v in values]

def execute_filter(node_id: int, input_id: int, settings: Dict) -> Dict:
    """Execute filter node - chains onto input LazyFrame"""
    input_lf = get_lazyframe(input_id)
    if input_lf is None:
        return {"success": False, "error": f"Filter error on node #{node_id}: No input data from node #{input_id}. Make sure the upstream node executed successfully."}

    filter_input = settings.get("filter_input", {})
    mode = filter_input.get("mode", "basic")
    field = None

    try:
        if mode == "advanced":
            expr = filter_input.get("advanced_filter", "")
            if expr:
                result_lf = input_lf.filter(eval(expr))
            else:
                result_lf = input_lf
        else:
            basic = filter_input.get("basic_filter", {})
            field = basic.get("field", "")
            operator = basic.get("operator", "equals")
            value = basic.get("value", "")
            value2 = basic.get("value2", "")

            if not field:
                result_lf = input_lf
            else:
                col = pl.col(field)

                # Get column data type from schema
                schema = input_lf.collect_schema()
                col_dtype = schema.get(field)

                # DON'T convert here - do it per operator

                if operator == "equals":
                    result_lf = input_lf.filter(col == convert_filter_value(value, col_dtype))
                elif operator == "not_equals":
                    result_lf = input_lf.filter(col != convert_filter_value(value, col_dtype))
                elif operator == "greater_than":
                    result_lf = input_lf.filter(col > convert_filter_value(value, col_dtype))
                elif operator == "greater_than_or_equals":
                    result_lf = input_lf.filter(col >= convert_filter_value(value, col_dtype))
                elif operator == "less_than":
                    result_lf = input_lf.filter(col < convert_filter_value(value, col_dtype))
                elif operator == "less_than_or_equals":
                    result_lf = input_lf.filter(col <= convert_filter_value(value, col_dtype))
                elif operator == "contains":
                    result_lf = input_lf.filter(col.str.contains(value))
                elif operator == "not_contains":
                    result_lf = input_lf.filter(~col.str.contains(value))
                elif operator == "starts_with":
                    result_lf = input_lf.filter(col.str.starts_with(value))
                elif operator == "ends_with":
                    result_lf = input_lf.filter(col.str.ends_with(value))
                elif operator == "is_null":
                    result_lf = input_lf.filter(col.is_null())
                elif operator == "is_not_null":
                    result_lf = input_lf.filter(col.is_not_null())
                elif operator == "in":
                    values = [v.strip() for v in value.split(",")]
                    result_lf = input_lf.filter(col.is_in(convert_filter_values(values, col_dtype)))
                elif operator == "not_in":
                    values = [v.strip() for v in value.split(",")]
                    result_lf = input_lf.filter(~col.is_in(convert_filter_values(values, col_dtype)))
                elif operator == "between":
                    v1 = convert_filter_value(value, col_dtype)
                    v2 = convert_filter_value(value2, col_dtype)
                    result_lf = input_lf.filter((col >= v1) & (col <= v2))
                else:
                    result_lf = input_lf
        store_lazyframe(node_id, result_lf)
        return {"success": True, "schema": get_schema(node_id), "has_data": True}
    except Exception as e:
        return {"success": False, "error": format_error_lf("filter", node_id, e, input_lf, field)}


def execute_select(node_id: int, input_id: int, settings: Dict) -> Dict:
    """Execute select node - column selection/renaming (lazy)"""
    input_lf = get_lazyframe(input_id)
    if input_lf is None:
        return {"success": False, "error": f"Select error on node #{node_id}: No input data from node #{input_id}. Make sure the upstream node executed successfully."}

    try:
        select_input = settings.get("select_input", [])

        if not select_input:
            result_lf = input_lf
        else:
            kept = [s for s in select_input if s.get("keep", True)]
            kept.sort(key=lambda x: x.get("position", 0))

            # Get available columns from schema
            schema = input_lf.collect_schema()
            available_cols = set(schema.keys())

            exprs = []
            for s in kept:
                old_name = s.get("old_name", "")
                new_name = s.get("new_name", old_name)
                if old_name in available_cols:
                    if old_name != new_name:
                        exprs.append(pl.col(old_name).alias(new_name))
                    else:
                        exprs.append(pl.col(old_name))

            if exprs:
                result_lf = input_lf.select(exprs)
            else:
                result_lf = input_lf

        store_lazyframe(node_id, result_lf)
        return {"success": True, "schema": get_schema(node_id), "has_data": True}
    except Exception as e:
        return {"success": False, "error": format_error_lf("select", node_id, e, input_lf)}

def execute_group_by(node_id: int, input_id: int, settings: Dict) -> Dict:
    """Execute group by node (lazy)"""
    input_lf = get_lazyframe(input_id)
    if input_lf is None:
        return {"success": False, "error": f"Group By error on node #{node_id}: No input data from node #{input_id}. Make sure the upstream node executed successfully."}

    try:
        groupby_input = settings.get("groupby_input", {})
        agg_cols = groupby_input.get("agg_cols", [])

        if not agg_cols:
            result_lf = input_lf
        else:
            group_cols = [pl.col(c["old_name"]).alias(c["new_name"]) for c in agg_cols if c.get("agg") == "groupby"]
            agg_defs = [c for c in agg_cols if c.get("agg") != "groupby"]

            if not group_cols:
                exprs = []
                for a in agg_defs:
                    col = pl.col(a["old_name"])
                    agg = a.get("agg", "count")
                    new_name = a.get("new_name", f"{a['old_name']}_{agg}")

                    if agg == "sum":
                        exprs.append(col.sum().alias(new_name))
                    elif agg == "max":
                        exprs.append(col.max().alias(new_name))
                    elif agg == "min":
                        exprs.append(col.min().alias(new_name))
                    elif agg == "count":
                        exprs.append(col.count().alias(new_name))
                    elif agg == "mean":
                        exprs.append(col.mean().alias(new_name))
                    elif agg == "median":
                        exprs.append(col.median().alias(new_name))
                    elif agg == "first":
                        exprs.append(col.first().alias(new_name))
                    elif agg == "last":
                        exprs.append(col.last().alias(new_name))
                    elif agg == "n_unique":
                        exprs.append(col.n_unique().alias(new_name))
                    elif agg == "concat":
                        exprs.append(col.cast(pl.Utf8).str.concat(",").alias(new_name))

                result_lf = input_lf.select(exprs) if exprs else input_lf
            else:
                exprs = []
                for a in agg_defs:
                    col = pl.col(a["old_name"])
                    agg = a.get("agg", "count")
                    new_name = a.get("new_name", f"{a['old_name']}_{agg}")

                    if agg == "sum":
                        exprs.append(col.sum().alias(new_name))
                    elif agg == "max":
                        exprs.append(col.max().alias(new_name))
                    elif agg == "min":
                        exprs.append(col.min().alias(new_name))
                    elif agg == "count":
                        exprs.append(col.count().alias(new_name))
                    elif agg == "mean":
                        exprs.append(col.mean().alias(new_name))
                    elif agg == "median":
                        exprs.append(col.median().alias(new_name))
                    elif agg == "first":
                        exprs.append(col.first().alias(new_name))
                    elif agg == "last":
                        exprs.append(col.last().alias(new_name))
                    elif agg == "n_unique":
                        exprs.append(col.n_unique().alias(new_name))
                    elif agg == "concat":
                        exprs.append(col.cast(pl.Utf8).str.concat(",").alias(new_name))

                if exprs:
                    result_lf = input_lf.group_by(group_cols).agg(exprs)
                else:
                    result_lf = input_lf.group_by(group_cols).agg(pl.count()).drop("count")

        store_lazyframe(node_id, result_lf)
        return {"success": True, "schema": get_schema(node_id), "has_data": True}
    except Exception as e:
        return {"success": False, "error": format_error_lf("group_by", node_id, e, input_lf)}

def execute_join(node_id: int, left_id: int, right_id: int, settings: Dict) -> Dict:
    """Execute join node (lazy)"""
    left_lf = get_lazyframe(left_id)
    right_lf = get_lazyframe(right_id)

    if left_lf is None:
        return {"success": False, "error": f"Join error on node #{node_id}: No left input data from node #{left_id}. Make sure the upstream node executed successfully."}
    if right_lf is None:
        return {"success": False, "error": f"Join error on node #{node_id}: No right input data from node #{right_id}. Make sure the upstream node executed successfully."}

    try:
        join_input = settings.get("join_input", {})
        join_type = join_input.get("join_type", "inner")
        mapping = join_input.get("join_mapping", [])
        left_suffix = join_input.get("left_suffix", "_left")
        right_suffix = join_input.get("right_suffix", "_right")

        if not mapping:
            return {"success": False, "error": f"Join error on node #{node_id}: No join columns specified. Configure the join mapping in the node settings."}

        left_on = [m.get("left_col") for m in mapping]
        right_on = [m.get("right_col") for m in mapping]

        # Validate columns exist using schema
        left_schema = left_lf.collect_schema()
        right_schema = right_lf.collect_schema()

        missing_left = [c for c in left_on if c not in left_schema]
        missing_right = [c for c in right_on if c not in right_schema]
        if missing_left:
            return {"success": False, "error": f"Join error on node #{node_id}: Left columns not found: {missing_left}. Available columns: {list(left_schema.keys())}"}
        if missing_right:
            return {"success": False, "error": f"Join error on node #{node_id}: Right columns not found: {missing_right}. Available columns: {list(right_schema.keys())}"}

        result_lf = left_lf.join(
            right_lf,
            left_on=left_on,
            right_on=right_on,
            how=join_type,
            suffix=right_suffix
        )

        store_lazyframe(node_id, result_lf)
        return {"success": True, "schema": get_schema(node_id), "has_data": True}
    except Exception as e:
        return {"success": False, "error": format_error_lf("join", node_id, e, left_lf)}

def execute_sort(node_id: int, input_id: int, settings: Dict) -> Dict:
    """Execute sort node (lazy)"""
    input_lf = get_lazyframe(input_id)
    if input_lf is None:
        return {"success": False, "error": f"Sort error on node #{node_id}: No input data from node #{input_id}. Make sure the upstream node executed successfully."}

    try:
        # sort_input is now a flat list matching flowfile_core: [{column, how}]
        sort_input = settings.get("sort_input", [])

        if not sort_input:
            result_lf = input_lf
        else:
            by = [c.get("column") for c in sort_input]
            descending = [c.get("how") == "desc" for c in sort_input]
            result_lf = input_lf.sort(by, descending=descending)

        store_lazyframe(node_id, result_lf)
        return {"success": True, "schema": get_schema(node_id), "has_data": True}
    except Exception as e:
        return {"success": False, "error": format_error_lf("sort", node_id, e, input_lf)}

def _build_local_vars(node_id: int, input_ids: List[int]) -> Tuple[Dict, Optional[Dict], List[str]]:
    """Build local variables dict with inputs. Returns (local_vars, error_dict, df_keys_to_cleanup)."""
    local_vars = {"pl": pl, "output_df": None, "output_lf": None}
    df_keys_to_cleanup = []  # Track which keys hold materialized DataFrames

    if len(input_ids) == 0:
        return local_vars, None, df_keys_to_cleanup

    if len(input_ids) == 1:
        input_lf = get_lazyframe(input_ids[0])
        if input_lf is None:
            return None, {"success": False, "error": f"Polars Code error on node #{node_id}: No input data from node #{input_ids[0]}. Make sure the upstream node executed successfully."}, []
        local_vars["input_df"] = input_lf.collect()
        local_vars["input_lf"] = input_lf
        df_keys_to_cleanup.append("input_df")
    else:
        for i, inp_id in enumerate(input_ids, start=1):
            lf = get_lazyframe(inp_id)
            if lf is None:
                # Clean up any already-collected DataFrames before returning error
                for key in df_keys_to_cleanup:
                    if key in local_vars:
                        del local_vars[key]
                gc.collect()
                return None, {"success": False, "error": f"Polars Code error on node #{node_id}: No input data from node #{inp_id}. Make sure the upstream node executed successfully."}, []
            local_vars[f"input_df_{i}"] = lf.collect()
            local_vars[f"input_lf_{i}"] = lf
            df_keys_to_cleanup.append(f"input_df_{i}")
        local_vars["input_df"] = local_vars["input_df_1"]
        local_vars["input_lf"] = local_vars["input_lf_1"]

    return local_vars, None, df_keys_to_cleanup


def _cleanup_local_vars(local_vars: Dict, df_keys_to_cleanup: List[str]):
    """Clean up materialized DataFrames from local_vars to free memory."""
    for key in df_keys_to_cleanup:
        if key in local_vars:
            del local_vars[key]
    # Also clean up any user-created DataFrames that aren't the result
    for key in list(local_vars.keys()):
        if key not in ("pl", "output_df", "output_lf", "input_lf") and not key.startswith("input_lf_"):
            val = local_vars.get(key)
            if isinstance(val, (pl.DataFrame, pl.LazyFrame)):
                del local_vars[key]
    gc.collect()


def _to_lazyframe(value: Any) -> Optional[pl.LazyFrame]:
    """Convert value to LazyFrame if possible."""
    if isinstance(value, pl.LazyFrame):
        return value
    if isinstance(value, pl.DataFrame):
        return value.lazy()
    return None


def _find_result_in_locals(local_vars: Dict) -> Optional[pl.LazyFrame]:
    """Find result from well-known variable names."""
    for var_name in ["output_lf", "output_df", "result", "df"]:
        value = local_vars.get(var_name)
        if value is not None:
            result = _to_lazyframe(value)
            if result is not None:
                return result
    return None


def _find_any_dataframe(local_vars: Dict) -> Optional[pl.LazyFrame]:
    """Find any DataFrame/LazyFrame created by user code."""
    skip_vars = {"input_df", "input_lf", "pl", "output_df", "output_lf"}
    for var_name in local_vars:
        if var_name.startswith("input_df_") or var_name.startswith("input_lf_"):
            skip_vars.add(var_name)
    
    for var_name, var_val in local_vars.items():
        if var_name not in skip_vars:
            result = _to_lazyframe(var_val)
            if result is not None:
                return result
    return None


def _try_eval_last_line(code: str, global_vars: Dict, local_vars: Dict) -> Optional[pl.LazyFrame]:
    """Try to eval the last line if it's an expression."""
    lines = code.splitlines()
    last_line = lines[-1].strip()
    
    if last_line and not last_line.startswith('#') and '=' not in last_line:
        try:
            result = eval(last_line, global_vars, local_vars)
            return _to_lazyframe(result)
        except:
            pass
    return None


def _extract_result(code: str, global_vars: Dict, local_vars: Dict, has_inputs: bool) -> Tuple[Optional[pl.LazyFrame], Optional[str]]:
    """Extract result from local_vars after code execution. Returns (result, error_message)."""
    # 1. Check well-known variable names
    result = _find_result_in_locals(local_vars)
    if result is not None:
        return result, None
    
    # 2. Find any DataFrame/LazyFrame created
    result = _find_any_dataframe(local_vars)
    if result is not None:
        return result, None
    
    # 3. Try eval on last line
    result = _try_eval_last_line(code, global_vars, local_vars)
    if result is not None:
        return result, None
    
    # 4. Fall back to input passthrough if available
    if has_inputs:
        return local_vars.get("input_lf"), None
    
    return None, "Code must produce a DataFrame or LazyFrame (set output_df or output_lf)"


def execute_polars_code(node_id: int, input_ids: List[int], settings: Dict) -> Dict:
    """Execute polars code node - supports zero, single, or multiple inputs.
    Memory-optimized: cleans up materialized DataFrames after execution."""

    # Build inputs
    local_vars, error, df_keys_to_cleanup = _build_local_vars(node_id, input_ids)
    if error:
        return error

    try:
        polars_code_input = settings.get("polars_code_input", {})
        code = (polars_code_input.get("polars_code") or "").strip()

        # Handle empty code
        if not code:
            if len(input_ids) == 0:
                _cleanup_local_vars(local_vars, df_keys_to_cleanup)
                return {"success": False, "error": f"Polars Code error on node #{node_id}: No code provided and no input to pass through."}
            result_lf = local_vars["input_lf"]
        else:
            global_vars = {"pl": pl}

            # Execute code
            try:
                exec(code, global_vars, local_vars)
            except SyntaxError:
                # Maybe it's just an expression
                result = eval(code, global_vars, local_vars)
                result_lf = _to_lazyframe(result)
                if result_lf is None:
                    _cleanup_local_vars(local_vars, df_keys_to_cleanup)
                    return {"success": False, "error": f"Polars Code error on node #{node_id}: Code must produce a DataFrame or LazyFrame, got {type(result).__name__}"}
                store_lazyframe(node_id, result_lf)
                _cleanup_local_vars(local_vars, df_keys_to_cleanup)
                return {"success": True, "schema": get_schema(node_id), "has_data": True}

            # Extract result
            result_lf, error_msg = _extract_result(code, global_vars, local_vars, len(input_ids) > 0)
            if error_msg:
                _cleanup_local_vars(local_vars, df_keys_to_cleanup)
                return {"success": False, "error": f"Polars Code error on node #{node_id}: {error_msg}"}

        store_lazyframe(node_id, result_lf)
        _cleanup_local_vars(local_vars, df_keys_to_cleanup)
        return {"success": True, "schema": get_schema(node_id), "has_data": True}

    except Exception as e:
        input_lf = local_vars.get("input_lf") if len(input_ids) > 0 else None
        _cleanup_local_vars(local_vars, df_keys_to_cleanup)
        return {"success": False, "error": format_error_lf("polars_code", node_id, e, input_lf)}

def execute_unique(node_id: int, input_id: int, settings: Dict) -> Dict:
    """Execute unique node (lazy)"""
    input_lf = get_lazyframe(input_id)
    if input_lf is None:
        return {"success": False, "error": f"Unique error on node #{node_id}: No input data from node #{input_id}. Make sure the upstream node executed successfully."}

    try:
        unique_input = settings.get("unique_input", {})
        subset = unique_input.get("subset") or unique_input.get("columns") or []
        keep = unique_input.get("keep") or unique_input.get("strategy") or "first"
        maintain_order = unique_input.get("maintain_order", True)

        if subset:
            result_lf = input_lf.unique(subset=subset, keep=keep, maintain_order=maintain_order)
        else:
            result_lf = input_lf.unique(keep=keep, maintain_order=maintain_order)

        store_lazyframe(node_id, result_lf)
        return {"success": True, "schema": get_schema(node_id), "has_data": True}
    except Exception as e:
        return {"success": False, "error": format_error_lf("unique", node_id, e, input_lf)}

def execute_head(node_id: int, input_id: int, settings: Dict) -> Dict:
    """Execute head/limit node (lazy)"""
    input_lf = get_lazyframe(input_id)
    if input_lf is None:
        return {"success": False, "error": f"Head error on node #{node_id}: No input data from node #{input_id}. Make sure the upstream node executed successfully."}

    try:
        head_input = settings.get("head_input", {})
        n = head_input.get("n", 10)

        result_lf = input_lf.head(n)

        store_lazyframe(node_id, result_lf)
        return {"success": True, "schema": get_schema(node_id), "has_data": True}
    except Exception as e:
        return {"success": False, "error": format_error_lf("head", node_id, e, input_lf)}

def execute_preview(node_id: int, input_id: int) -> Dict:
    """Execute preview node - just passes through the LazyFrame"""
    input_lf = get_lazyframe(input_id)
    if input_lf is None:
        return {"success": False, "error": f"Preview error on node #{node_id}: No input data from node #{input_id}. Make sure the upstream node executed successfully."}

    try:
        store_lazyframe(node_id, input_lf)
        return {"success": True, "schema": get_schema(node_id), "has_data": True}
    except Exception as e:
        return {"success": False, "error": format_error_lf("explore_data", node_id, e, input_lf)}

def execute_pivot(node_id: int, input_id: int, settings: Dict) -> Dict:
    """Execute pivot node - converts data from long to wide format
    Note: Pivot requires collecting data due to dynamic column creation.
    Memory-optimized: cleans up intermediate DataFrames."""
    input_lf = get_lazyframe(input_id)
    if input_lf is None:
        return {"success": False, "error": f"Pivot error on node #{node_id}: No input data from node #{input_id}. Make sure the upstream node executed successfully."}

    df = None
    grouped = None
    result = None

    try:
        # Pivot needs to collect to determine unique values for columns
        df = input_lf.collect()

        pivot_input = settings.get("pivot_input", {})
        index_columns = pivot_input.get("index_columns", [])
        pivot_column = pivot_input.get("pivot_column", "")
        value_col = pivot_input.get("value_col", "")
        aggregations = pivot_input.get("aggregations", ["sum"])

        if not pivot_column:
            del df
            gc.collect()
            return {"success": False, "error": f"Pivot error on node #{node_id}: No pivot column specified. Please select a column whose values will become new columns."}
        if not value_col:
            del df
            gc.collect()
            return {"success": False, "error": f"Pivot error on node #{node_id}: No value column specified. Please select a column containing values to aggregate."}
        if pivot_column not in df.columns:
            cols = list(df.columns)
            del df
            gc.collect()
            return {"success": False, "error": f"Pivot error on node #{node_id}: Pivot column '{pivot_column}' not found. Available columns: {cols}"}
        if value_col not in df.columns:
            cols = list(df.columns)
            del df
            gc.collect()
            return {"success": False, "error": f"Pivot error on node #{node_id}: Value column '{value_col}' not found. Available columns: {cols}"}

        max_unique = 200
        unique_values = df.select(pl.col(pivot_column).cast(pl.String)).unique().sort(pivot_column).limit(max_unique).to_series().to_list()

        if len(unique_values) >= max_unique:
            del df
            gc.collect()
            return {"success": False, "error": f"Pivot error on node #{node_id}: Pivot column '{pivot_column}' has too many unique values (>={max_unique}). Please use a column with fewer unique values."}

        group_cols = index_columns + [pivot_column] if index_columns else [pivot_column]

        agg_map = {
            "sum": lambda c: pl.col(c).sum(),
            "mean": lambda c: pl.col(c).mean(),
            "min": lambda c: pl.col(c).min(),
            "max": lambda c: pl.col(c).max(),
            "count": lambda c: pl.col(c).count(),
            "first": lambda c: pl.col(c).first(),
            "last": lambda c: pl.col(c).last(),
            "median": lambda c: pl.col(c).median(),
        }

        agg_exprs = []
        for agg in aggregations:
            if agg in agg_map:
                agg_exprs.append(agg_map[agg](value_col).alias(agg))
            else:
                agg_exprs.append(pl.col(value_col).sum().alias(agg))

        if not agg_exprs:
            agg_exprs = [pl.col(value_col).sum().alias("sum")]

        grouped = df.group_by(group_cols).agg(agg_exprs)

        # Free original df memory immediately after grouping
        del df
        df = None

        if index_columns:
            index_exprs = [pl.col(c) for c in index_columns]
        else:
            grouped = grouped.with_columns(pl.lit(1).alias("__temp_idx__"))
            index_columns = ["__temp_idx__"]
            index_exprs = [pl.col("__temp_idx__")]

        pivot_exprs = []
        for unique_val in unique_values:
            for agg in aggregations:
                col_name = f"{unique_val}_{agg}" if len(aggregations) > 1 else str(unique_val)
                pivot_exprs.append(
                    pl.col(agg).filter(pl.col(pivot_column) == unique_val).first().alias(col_name)
                )

        result = grouped.group_by(index_exprs).agg(pivot_exprs)

        # Free grouped memory
        del grouped
        grouped = None

        if "__temp_idx__" in result.columns:
            result = result.drop("__temp_idx__")

        result_lf = result.lazy()
        store_lazyframe(node_id, result_lf)

        # Clean up result DataFrame
        del result
        gc.collect()

        return {"success": True, "schema": get_schema(node_id), "has_data": True}
    except Exception as e:
        # Clean up on error
        if df is not None:
            del df
        if grouped is not None:
            del grouped
        if result is not None:
            del result
        gc.collect()
        return {"success": False, "error": format_error("pivot", node_id, e)}

def execute_unpivot(node_id: int, input_id: int, settings: Dict) -> Dict:
    """Execute unpivot node - converts data from wide to long format (lazy)"""
    input_lf = get_lazyframe(input_id)
    if input_lf is None:
        return {"success": False, "error": f"Unpivot error on node #{node_id}: No input data from node #{input_id}. Make sure the upstream node executed successfully."}

    try:
        unpivot_input = settings.get("unpivot_input", {})
        index_columns = unpivot_input.get("index_columns", [])
        value_columns = unpivot_input.get("value_columns", [])
        data_type_selector = unpivot_input.get("data_type_selector")
        selector_mode = unpivot_input.get("data_type_selector_mode", "column")

        if selector_mode == "data_type" and data_type_selector:
            import polars.selectors as cs
            selector_map = {
                "float": cs.float,
                "numeric": cs.numeric,
                "string": cs.string,
                "date": cs.temporal,
                "all": cs.all,
            }
            if data_type_selector in selector_map:
                on_selector = selector_map[data_type_selector]()
            else:
                on_selector = cs.all()
            result_lf = input_lf.unpivot(on=on_selector, index=index_columns if index_columns else None)
        elif value_columns:
            schema = input_lf.collect_schema()
            missing = [c for c in value_columns if c not in schema]
            if missing:
                return {"success": False, "error": f"Unpivot error on node #{node_id}: Columns not found: {missing}. Available columns: {list(schema.keys())}"}
            result_lf = input_lf.unpivot(on=value_columns, index=index_columns if index_columns else None)
        else:
            result_lf = input_lf.unpivot(index=index_columns if index_columns else None)

        store_lazyframe(node_id, result_lf)
        return {"success": True, "schema": get_schema(node_id), "has_data": True}
    except Exception as e:
        return {"success": False, "error": format_error_lf("unpivot", node_id, e, input_lf)}

def execute_output(node_id: int, input_id: int, settings: Dict) -> Dict:
    """Execute output node - prepares data for download.
    Note: This must collect data to generate the output file.
    Memory-optimized: cleans up DataFrame after generating output."""
    input_lf = get_lazyframe(input_id)
    if input_lf is None:
        return {"success": False, "error": f"Output error on node #{node_id}: No input data from node #{input_id}. Make sure the upstream node executed successfully."}

    df = None
    try:
        import io

        # Collect data for output
        df = input_lf.collect()
        row_count = len(df)

        output_settings = settings.get("output_settings", {})
        file_type = output_settings.get("file_type", "csv")
        file_name = output_settings.get("name", "output.csv")
        table_settings = output_settings.get("table_settings", {})

        # Store as lazyframe for schema access
        store_lazyframe(node_id, df.lazy())

        if file_type == "parquet":
            # Parquet export is not supported in the browser/WASM environment
            del df
            gc.collect()
            return {"success": False, "error": f"Output error on node #{node_id}: Parquet export is not supported in the browser. Please use CSV format instead."}
        else:
            delimiter = table_settings.get("delimiter", ",")
            if delimiter == "tab":
                delimiter = "\\t"

            buffer = io.StringIO()
            df.write_csv(buffer, separator=delimiter)
            content = buffer.getvalue()
            mime_type = "text/csv"

        # Free DataFrame memory immediately after writing
        del df
        gc.collect()

        return {
            "success": True,
            "schema": get_schema(node_id),
            "has_data": True,
            "download": {
                "content": content,
                "file_name": file_name,
                "file_type": file_type,
                "mime_type": mime_type,
                "row_count": row_count
            }
        }
    except Exception as e:
        if df is not None:
            del df
            gc.collect()
        return {"success": False, "error": format_error_lf("output", node_id, e, input_lf)}
`)
  }

  async function runPython(code: string): Promise<any> {
    if (!isReady.value) {
      throw new Error('Pyodide is not ready')
    }
    return await pyodide.value.runPythonAsync(code)
  }

  async function runPythonWithResult(code: string): Promise<any> {
    if (!isReady.value) {
      throw new Error('Pyodide is not ready')
    }

    const rawResult = await pyodide.value.runPythonAsync(code)
    if (!rawResult?.toJs) {
      return rawResult
    }

    // Convert Python result to JavaScript with deep conversion
    const jsResult = rawResult.toJs({ dict_converter: Object.fromEntries })

    // Recursively convert any remaining Map objects to plain objects
    function deepConvert(obj: any): any {
      if (obj instanceof Map) {
        const result: Record<string, any> = {}
        obj.forEach((value: any, key: string) => {
          result[key] = deepConvert(value)
        })
        return result
      }
      if (Array.isArray(obj)) {
        return obj.map(deepConvert)
      }
      if (obj && typeof obj === 'object' && obj.constructor === Object) {
        const result: Record<string, any> = {}
        for (const [key, value] of Object.entries(obj)) {
          result[key] = deepConvert(value)
        }
        return result
      }
      return obj
    }

    return deepConvert(jsResult)
  }

  function setGlobal(name: string, value: unknown): void {
    if (!isReady.value) {
      throw new Error('Pyodide is not ready')
    }
    pyodide.value.globals.set(name, value)
  }

  function deleteGlobal(name: string): void {
    if (!isReady.value) {
      throw new Error('Pyodide is not ready')
    }
    pyodide.value.globals.delete(name)
  }

  return {
    pyodide,
    isReady,
    isLoading,
    loadingStatus,
    error,
    initialize,
    runPython,
    runPythonWithResult,
    setGlobal,
    deleteGlobal
  }
})