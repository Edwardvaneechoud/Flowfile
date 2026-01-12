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
from typing import Dict, List, Any, Optional, Union

# Global dataframe storage
_dataframes: Dict[int, pl.DataFrame] = {}
_schemas: Dict[int, List[Dict[str, str]]] = {}

def store_dataframe(node_id: int, df: pl.DataFrame):
    """Store a dataframe for a node"""
    _dataframes[node_id] = df
    _schemas[node_id] = [{"name": col, "data_type": str(dtype)} for col, dtype in zip(df.columns, df.dtypes)]

def get_dataframe(node_id: int) -> Optional[pl.DataFrame]:
    """Get a dataframe for a node"""
    return _dataframes.get(node_id)

def get_schema(node_id: int) -> List[Dict[str, str]]:
    """Get schema for a node"""
    return _schemas.get(node_id, [])

def clear_node(node_id: int):
    """Clear data for a node"""
    _dataframes.pop(node_id, None)
    _schemas.pop(node_id, None)

def clear_all():
    """Clear all data"""
    _dataframes.clear()
    _schemas.clear()

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
    x_position: Optional[int] = 0
    y_position: Optional[int] = 0
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

# Node execution functions
def execute_read_csv(node_id: int, file_content: str, settings: Dict) -> Dict:
    """Execute read CSV node with proper nested settings access"""
    try:
        import io
        # 1. Access the nested table settings to match core schema
        table_settings = settings.get("received_table", {}).get("table_settings", {})
        
        # 2. Extract values using the correct keys (delimiter, starting_from_line)
        df = pl.read_csv(
            io.StringIO(file_content),
            has_header=table_settings.get("has_headers", True),
            separator=table_settings.get("delimiter", ","),
            skip_rows=table_settings.get("starting_from_line", 0)
        )
        store_dataframe(node_id, df)
        return {"success": True, "data": df_to_preview(df), "schema": get_schema(node_id)}
    except Exception as e:
        return {"success": False, "error": format_error("read_csv", node_id, e)}


def execute_manual_input(node_id: int, data_content: str, settings: Dict) -> Dict:
    """Execute manual input node

    Supports two formats:
    1. raw_data_format (flowfile_core format): columnar data with columns metadata
    2. Legacy format: CSV string with manual_input settings (has_headers, delimiter)
    """
    try:
        import io

        # Check for flowfile_core format (raw_data_format)
        raw_data_format = settings.get("raw_data_format")
        if raw_data_format and raw_data_format.get("columns") and raw_data_format.get("data"):
            # Build DataFrame from columnar format
            columns_meta = raw_data_format["columns"]
            data = raw_data_format["data"]

            # data is in columnar format: [[col1_values], [col2_values], ...]
            if len(columns_meta) > 0 and len(data) > 0:
                col_names = [c["name"] for c in columns_meta]
                # Create dict for DataFrame constructor
                df_dict = {name: values for name, values in zip(col_names, data)}
                df = pl.DataFrame(df_dict)
            else:
                df = pl.DataFrame()
        else:
            # Legacy format: parse CSV from data_content string
            manual_input = settings.get("manual_input", {})
            has_headers = manual_input.get("has_headers", True)
            delimiter = manual_input.get("delimiter", ",")

            df = pl.read_csv(
                io.StringIO(data_content),
                has_header=has_headers,
                separator=delimiter
            )

        store_dataframe(node_id, df)
        return {"success": True, "data": df_to_preview(df), "schema": get_schema(node_id)}
    except Exception as e:
        return {"success": False, "error": format_error("manual_input", node_id, e)}

def execute_filter(node_id: int, input_id: int, settings: Dict) -> Dict:
    """Execute filter node"""
    df = get_dataframe(input_id)
    if df is None:
        return {"success": False, "error": f"Filter error on node #{node_id}: No input data from node #{input_id}. Make sure the upstream node executed successfully."}

    filter_input = settings.get("filter_input", {})
    mode = filter_input.get("mode", "basic")
    field = None

    try:
        if mode == "advanced":
            expr = filter_input.get("advanced_filter", "")
            if expr:
                result = df.filter(eval(expr))
            else:
                result = df
        else:
            basic = filter_input.get("basic_filter", {})
            field = basic.get("field", "")
            operator = basic.get("operator", "equals")
            value = basic.get("value", "")
            value2 = basic.get("value2", "")

            if not field:
                result = df
            else:
                col = pl.col(field)

                # Try to convert value to appropriate type
                try:
                    num_value = float(value) if '.' in value else int(value)
                except:
                    num_value = value

                if operator == "equals":
                    result = df.filter(col == num_value)
                elif operator == "not_equals":
                    result = df.filter(col != num_value)
                elif operator == "greater_than":
                    result = df.filter(col > num_value)
                elif operator == "greater_than_or_equals":
                    result = df.filter(col >= num_value)
                elif operator == "less_than":
                    result = df.filter(col < num_value)
                elif operator == "less_than_or_equals":
                    result = df.filter(col <= num_value)
                elif operator == "contains":
                    result = df.filter(col.str.contains(value))
                elif operator == "not_contains":
                    result = df.filter(~col.str.contains(value))
                elif operator == "starts_with":
                    result = df.filter(col.str.starts_with(value))
                elif operator == "ends_with":
                    result = df.filter(col.str.ends_with(value))
                elif operator == "is_null":
                    result = df.filter(col.is_null())
                elif operator == "is_not_null":
                    result = df.filter(col.is_not_null())
                elif operator == "in":
                    values = [v.strip() for v in value.split(",")]
                    result = df.filter(col.is_in(values))
                elif operator == "not_in":
                    values = [v.strip() for v in value.split(",")]
                    result = df.filter(~col.is_in(values))
                elif operator == "between":
                    try:
                        v1 = float(value) if '.' in value else int(value)
                        v2 = float(value2) if '.' in value2 else int(value2)
                    except:
                        v1, v2 = value, value2
                    result = df.filter((col >= v1) & (col <= v2))
                else:
                    result = df

        store_dataframe(node_id, result)
        return {"success": True, "data": df_to_preview(result), "schema": get_schema(node_id)}
    except Exception as e:
        return {"success": False, "error": format_error("filter", node_id, e, df, field)}

def execute_select(node_id: int, input_id: int, settings: Dict) -> Dict:
    """Execute select node"""
    df = get_dataframe(input_id)
    if df is None:
        return {"success": False, "error": f"Select error on node #{node_id}: No input data from node #{input_id}. Make sure the upstream node executed successfully."}

    try:
        select_input = settings.get("select_input", [])

        if not select_input:
            result = df
        else:
            # Filter to keep only columns marked as keep
            kept = [s for s in select_input if s.get("keep", True)]
            kept.sort(key=lambda x: x.get("position", 0))

            exprs = []
            for s in kept:
                old_name = s.get("old_name", "")
                new_name = s.get("new_name", old_name)
                if old_name in df.columns:
                    if old_name != new_name:
                        exprs.append(pl.col(old_name).alias(new_name))
                    else:
                        exprs.append(pl.col(old_name))

            if exprs:
                result = df.select(exprs)
            else:
                result = df

        store_dataframe(node_id, result)
        return {"success": True, "data": df_to_preview(result), "schema": get_schema(node_id)}
    except Exception as e:
        return {"success": False, "error": format_error("select", node_id, e, df)}

def execute_group_by(node_id: int, input_id: int, settings: Dict) -> Dict:
    """Execute group by node"""
    df = get_dataframe(input_id)
    if df is None:
        return {"success": False, "error": f"Group By error on node #{node_id}: No input data from node #{input_id}. Make sure the upstream node executed successfully."}

    try:
        groupby_input = settings.get("groupby_input", {})
        agg_cols = groupby_input.get("agg_cols", [])

        if not agg_cols:
            result = df
        else:
            # Separate groupby columns from aggregation columns
            group_cols = [pl.col(c["old_name"]).alias(c["new_name"]) for c in agg_cols if c.get("agg") == "groupby"]
            agg_defs = [c for c in agg_cols if c.get("agg") != "groupby"]

            if not group_cols:
                # No groupby columns, just aggregate
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

                result = df.select(exprs) if exprs else df
            else:
                # Group by and aggregate
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
                    result = df.group_by(group_cols).agg(exprs)
                else:
                    result = df.group_by(group_cols).agg(pl.count()).drop("count")

        store_dataframe(node_id, result)
        return {"success": True, "data": df_to_preview(result), "schema": get_schema(node_id)}
    except Exception as e:
        return {"success": False, "error": format_error("group_by", node_id, e, df)}

def execute_join(node_id: int, left_id: int, right_id: int, settings: Dict) -> Dict:
    """Execute join node"""
    left_df = get_dataframe(left_id)
    right_df = get_dataframe(right_id)

    if left_df is None:
        return {"success": False, "error": f"Join error on node #{node_id}: No left input data from node #{left_id}. Make sure the upstream node executed successfully."}
    if right_df is None:
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

        # Validate columns exist
        missing_left = [c for c in left_on if c not in left_df.columns]
        missing_right = [c for c in right_on if c not in right_df.columns]
        if missing_left:
            return {"success": False, "error": f"Join error on node #{node_id}: Left columns not found: {missing_left}. Available columns: {list(left_df.columns)}"}
        if missing_right:
            return {"success": False, "error": f"Join error on node #{node_id}: Right columns not found: {missing_right}. Available columns: {list(right_df.columns)}"}

        result = left_df.join(
            right_df,
            left_on=left_on,
            right_on=right_on,
            how=join_type,
            suffix=right_suffix
        )

        store_dataframe(node_id, result)
        return {"success": True, "data": df_to_preview(result), "schema": get_schema(node_id)}
    except Exception as e:
        return {"success": False, "error": format_error("join", node_id, e, left_df)}

def execute_sort(node_id: int, input_id: int, settings: Dict) -> Dict:
    """Execute sort node"""
    df = get_dataframe(input_id)
    if df is None:
        return {"success": False, "error": f"Sort error on node #{node_id}: No input data from node #{input_id}. Make sure the upstream node executed successfully."}

    try:
        sort_input = settings.get("sort_input", {})
        sort_cols = sort_input.get("sort_cols", [])

        if not sort_cols:
            result = df
        else:
            by = [c.get("column") for c in sort_cols]
            descending = [c.get("descending", False) for c in sort_cols]
            result = df.sort(by, descending=descending)

        store_dataframe(node_id, result)
        return {"success": True, "data": df_to_preview(result), "schema": get_schema(node_id)}
    except Exception as e:
        return {"success": False, "error": format_error("sort", node_id, e, df)}

def execute_polars_code(node_id: int, input_id: int, settings: Dict) -> Dict:
    """Execute polars code node - runs arbitrary Polars code"""
    input_df = get_dataframe(input_id)
    if input_df is None:
        return {"success": False, "error": f"Polars Code error on node #{node_id}: No input data from node #{input_id}. Make sure the upstream node executed successfully."}

    try:
        polars_code_input = settings.get("polars_code_input", {})
        code = polars_code_input.get("polars_code", "input_df")

        if not code or not code.strip():
            result = input_df
        else:
            code = code.strip()
            # Set up execution environment
            local_vars = {"input_df": input_df, "pl": pl, "output_df": None}
            global_vars = {"pl": pl}

            # Try exec first (handles assignments like output_df = ...)
            try:
                exec(code, global_vars, local_vars)
                # Check if output_df was set
                if local_vars.get("output_df") is not None and isinstance(local_vars["output_df"], pl.DataFrame):
                    result = local_vars["output_df"]
                # Check if result was set
                elif local_vars.get("result") is not None and isinstance(local_vars["result"], pl.DataFrame):
                    result = local_vars["result"]
                # Check if df was set
                elif local_vars.get("df") is not None and isinstance(local_vars["df"], pl.DataFrame):
                    result = local_vars["df"]
                else:
                    # Try to find any DataFrame that was created
                    found_df = None
                    for var_name, var_val in local_vars.items():
                        if isinstance(var_val, pl.DataFrame) and var_name not in ["input_df"]:
                            found_df = var_val
                            break
                    if found_df is not None:
                        result = found_df
                    else:
                        # Fallback: try eval on last line
                        lines = code.split('\\n')
                        last_line = lines[-1].strip()
                        if last_line and not last_line.startswith('#') and '=' not in last_line:
                            result = eval(last_line, global_vars, local_vars)
                        else:
                            result = input_df
            except SyntaxError:
                # If exec fails with syntax error, try eval (for simple expressions)
                result = eval(code, global_vars, local_vars)

            # Validate result
            if not isinstance(result, pl.DataFrame):
                return {"success": False, "error": f"Polars Code error on node #{node_id}: Code must produce a DataFrame, got {type(result).__name__}. Assign result to 'output_df', 'result', or 'df'. Available columns in input: {list(input_df.columns)}"}

        store_dataframe(node_id, result)
        return {"success": True, "data": df_to_preview(result), "schema": get_schema(node_id)}
    except Exception as e:
        return {"success": False, "error": format_error("polars_code", node_id, e, input_df)}

def execute_unique(node_id: int, input_id: int, settings: Dict) -> Dict:
    """Execute unique node"""
    df = get_dataframe(input_id)
    if df is None:
        return {"success": False, "error": f"Unique error on node #{node_id}: No input data from node #{input_id}. Make sure the upstream node executed successfully."}

    try:
        unique_input = settings.get("unique_input", {})
        # Support both flowfile_core format (columns/strategy) and component format (subset/keep)
        subset = unique_input.get("subset") or unique_input.get("columns") or []
        keep = unique_input.get("keep") or unique_input.get("strategy") or "first"
        maintain_order = unique_input.get("maintain_order", True)

        if subset:
            result = df.unique(subset=subset, keep=keep, maintain_order=maintain_order)
        else:
            result = df.unique(keep=keep, maintain_order=maintain_order)

        store_dataframe(node_id, result)
        return {"success": True, "data": df_to_preview(result), "schema": get_schema(node_id)}
    except Exception as e:
        return {"success": False, "error": format_error("unique", node_id, e, df)}

def execute_head(node_id: int, input_id: int, settings: Dict) -> Dict:
    """Execute head/limit node"""
    df = get_dataframe(input_id)
    if df is None:
        return {"success": False, "error": f"Head error on node #{node_id}: No input data from node #{input_id}. Make sure the upstream node executed successfully."}

    try:
        head_input = settings.get("head_input", {})
        n = head_input.get("n", 10)

        result = df.head(n)

        store_dataframe(node_id, result)
        return {"success": True, "data": df_to_preview(result), "schema": get_schema(node_id)}
    except Exception as e:
        return {"success": False, "error": format_error("head", node_id, e, df)}

def execute_preview(node_id: int, input_id: int) -> Dict:
    """Execute preview node - just passes through data"""
    df = get_dataframe(input_id)
    if df is None:
        return {"success": False, "error": f"Preview error on node #{node_id}: No input data from node #{input_id}. Make sure the upstream node executed successfully."}

    try:
        store_dataframe(node_id, df)
        return {"success": True, "data": df_to_preview(df), "schema": get_schema(node_id)}
    except Exception as e:
        return {"success": False, "error": format_error("preview", node_id, e, df)}

def execute_pivot(node_id: int, input_id: int, settings: Dict) -> Dict:
    """Execute pivot node - converts data from long to wide format"""
    df = get_dataframe(input_id)
    if df is None:
        return {"success": False, "error": f"Pivot error on node #{node_id}: No input data from node #{input_id}. Make sure the upstream node executed successfully."}

    try:
        pivot_input = settings.get("pivot_input", {})
        index_columns = pivot_input.get("index_columns", [])
        pivot_column = pivot_input.get("pivot_column", "")
        value_col = pivot_input.get("value_col", "")
        aggregations = pivot_input.get("aggregations", ["sum"])

        if not pivot_column:
            return {"success": False, "error": f"Pivot error on node #{node_id}: No pivot column specified. Please select a column whose values will become new columns."}
        if not value_col:
            return {"success": False, "error": f"Pivot error on node #{node_id}: No value column specified. Please select a column containing values to aggregate."}
        if pivot_column not in df.columns:
            return {"success": False, "error": f"Pivot error on node #{node_id}: Pivot column '{pivot_column}' not found. Available columns: {list(df.columns)}"}
        if value_col not in df.columns:
            return {"success": False, "error": f"Pivot error on node #{node_id}: Value column '{value_col}' not found. Available columns: {list(df.columns)}"}

        # Get unique values for the pivot column (limit to prevent too many columns)
        max_unique = 200
        unique_values = df.select(pl.col(pivot_column).cast(pl.String)).unique().sort(pivot_column).limit(max_unique).to_series().to_list()

        if len(unique_values) >= max_unique:
            return {"success": False, "error": f"Pivot error on node #{node_id}: Pivot column '{pivot_column}' has too many unique values (>={max_unique}). Please use a column with fewer unique values."}

        # Determine group columns (index + pivot)
        group_cols = index_columns + [pivot_column] if index_columns else [pivot_column]

        # Build aggregation expressions
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

        # First aggregate the data
        agg_exprs = []
        for agg in aggregations:
            if agg in agg_map:
                agg_exprs.append(agg_map[agg](value_col).alias(agg))
            else:
                agg_exprs.append(pl.col(value_col).sum().alias(agg))

        if not agg_exprs:
            agg_exprs = [pl.col(value_col).sum().alias("sum")]

        grouped = df.group_by(group_cols).agg(agg_exprs)

        # Now pivot the data
        # Build the pivot manually by filtering for each unique value
        if index_columns:
            index_exprs = [pl.col(c) for c in index_columns]
        else:
            # No index columns - add a temporary constant column
            grouped = grouped.with_columns(pl.lit(1).alias("__temp_idx__"))
            index_columns = ["__temp_idx__"]
            index_exprs = [pl.col("__temp_idx__")]

        # Group by index columns and create struct for each pivot value
        pivot_exprs = []
        for unique_val in unique_values:
            for agg in aggregations:
                col_name = f"{unique_val}_{agg}" if len(aggregations) > 1 else str(unique_val)
                pivot_exprs.append(
                    pl.col(agg).filter(pl.col(pivot_column) == unique_val).first().alias(col_name)
                )

        result = grouped.group_by(index_exprs).agg(pivot_exprs)

        # Remove temp column if added
        if "__temp_idx__" in result.columns:
            result = result.drop("__temp_idx__")

        store_dataframe(node_id, result)
        return {"success": True, "data": df_to_preview(result), "schema": get_schema(node_id)}
    except Exception as e:
        return {"success": False, "error": format_error("pivot", node_id, e, df)}

def execute_unpivot(node_id: int, input_id: int, settings: Dict) -> Dict:
    """Execute unpivot node - converts data from wide to long format"""
    df = get_dataframe(input_id)
    if df is None:
        return {"success": False, "error": f"Unpivot error on node #{node_id}: No input data from node #{input_id}. Make sure the upstream node executed successfully."}

    try:
        unpivot_input = settings.get("unpivot_input", {})
        index_columns = unpivot_input.get("index_columns", [])
        value_columns = unpivot_input.get("value_columns", [])
        data_type_selector = unpivot_input.get("data_type_selector")
        selector_mode = unpivot_input.get("data_type_selector_mode", "column")

        # Determine which columns to unpivot
        if selector_mode == "data_type" and data_type_selector:
            # Select columns by data type
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
            result = df.unpivot(on=on_selector, index=index_columns if index_columns else None)
        elif value_columns:
            # Explicit column list
            # Validate columns exist
            missing = [c for c in value_columns if c not in df.columns]
            if missing:
                return {"success": False, "error": f"Unpivot error on node #{node_id}: Columns not found: {missing}. Available columns: {list(df.columns)}"}
            result = df.unpivot(on=value_columns, index=index_columns if index_columns else None)
        else:
            # No columns specified - unpivot all non-index columns
            result = df.unpivot(index=index_columns if index_columns else None)

        store_dataframe(node_id, result)
        return {"success": True, "data": df_to_preview(result), "schema": get_schema(node_id)}
    except Exception as e:
        return {"success": False, "error": format_error("unpivot", node_id, e, df)}

def execute_output(node_id: int, input_id: int, settings: Dict) -> Dict:
    """Execute output node - prepares data for download in WASM environment.

    Returns the serialized data content that can be downloaded by the browser.
    Supports CSV and Parquet formats.
    """
    df = get_dataframe(input_id)
    if df is None:
        return {"success": False, "error": f"Output error on node #{node_id}: No input data from node #{input_id}. Make sure the upstream node executed successfully."}

    try:
        import io
        import base64
        output_settings = settings.get("output_settings", {})
        file_type = output_settings.get("file_type", "csv")
        file_name = output_settings.get("name", "output.csv")
        table_settings = output_settings.get("table_settings", {})

        # Store dataframe for preview
        store_dataframe(node_id, df)

        # Prepare download content based on file type
        if file_type == "parquet":
            # Write parquet to bytes buffer
            buffer = io.BytesIO()
            df.write_parquet(buffer)
            content = buffer.getvalue()
            content = base64.b64encode(content).decode('utf-8')
            mime_type = "application/octet-stream"
        else:
            # Default to CSV
            delimiter = table_settings.get("delimiter", ",")
            # Handle tab delimiter
            if delimiter == "tab":
                delimiter = "\\t"

            buffer = io.StringIO()
            df.write_csv(buffer, separator=delimiter)
            content = buffer.getvalue()
            mime_type = "text/csv"

        return {
            "success": True,
            "data": df_to_preview(df),
            "schema": get_schema(node_id),
            "download": {
                "content": content,
                "file_name": file_name,
                "file_type": file_type,
                "mime_type": mime_type,
                "row_count": len(df)
            }
        }
    except Exception as e:
        return {"success": False, "error": format_error("output", node_id, e, df)}
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
