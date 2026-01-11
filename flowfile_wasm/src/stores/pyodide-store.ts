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
    if (isReady.value || isLoading.value) return

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
      await pyodide.value.loadPackage(['numpy', 'polars'])

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
    """Execute read CSV node"""
    try:
        import io
        df = pl.read_csv(
            io.StringIO(file_content),
            has_header=settings.get("has_headers", True),
            separator=settings.get("delimiter", ","),
            skip_rows=settings.get("skip_rows", 0)
        )
        store_dataframe(node_id, df)
        return {"success": True, "data": df_to_preview(df), "schema": get_schema(node_id)}
    except Exception as e:
        return {"success": False, "error": format_error("read_csv", node_id, e)}

def execute_manual_input(node_id: int, data_content: str, settings: Dict) -> Dict:
    """Execute manual input node"""
    try:
        import io
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
            group_cols = [c["old_name"] for c in agg_cols if c.get("agg") == "groupby"]
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
                    result = df.group_by(group_cols).agg(pl.count())

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
        subset = unique_input.get("subset", [])
        keep = unique_input.get("keep", "first")
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
    const result = await pyodide.value.runPythonAsync(code)
    return result?.toJs ? result.toJs({ dict_converter: Object.fromEntries }) : result
  }

  return {
    pyodide,
    isReady,
    isLoading,
    loadingStatus,
    error,
    initialize,
    runPython,
    runPythonWithResult
  }
})
