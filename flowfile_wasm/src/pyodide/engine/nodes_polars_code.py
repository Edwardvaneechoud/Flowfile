import gc
from typing import Any

import polars as pl

from .errors import format_error_lf
from .log import log_node
from .state import get_lazyframe, get_schema, store_lazyframe


def _build_local_vars(node_id: int, input_ids: list[int]) -> tuple[dict, dict | None, list[str]]:
    """Build local variables dict with inputs. Returns (local_vars, error_dict, df_keys_to_cleanup)."""
    local_vars = {"pl": pl, "output_df": None, "output_lf": None}
    df_keys_to_cleanup = []  # Track which keys hold materialized DataFrames

    if len(input_ids) == 0:
        return local_vars, None, df_keys_to_cleanup

    if len(input_ids) == 1:
        input_lf = get_lazyframe(input_ids[0])
        if input_lf is None:
            return (
                None,
                {
                    "success": False,
                    "error": f"Polars Code error on node #{node_id}: No input data from node #{input_ids[0]}. Make sure the upstream node executed successfully.",
                },
                [],
            )
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
                return (
                    None,
                    {
                        "success": False,
                        "error": f"Polars Code error on node #{node_id}: No input data from node #{inp_id}. Make sure the upstream node executed successfully.",
                    },
                    [],
                )
            local_vars[f"input_df_{i}"] = lf.collect()
            local_vars[f"input_lf_{i}"] = lf
            df_keys_to_cleanup.append(f"input_df_{i}")
        local_vars["input_df"] = local_vars["input_df_1"]
        local_vars["input_lf"] = local_vars["input_lf_1"]

    return local_vars, None, df_keys_to_cleanup


def _cleanup_local_vars(local_vars: dict, df_keys_to_cleanup: list[str]):
    """Clean up materialized DataFrames from local_vars to free memory."""
    for key in df_keys_to_cleanup:
        if key in local_vars:
            del local_vars[key]
    # Also clean up any user-created DataFrames that aren't the result
    for key in list(local_vars.keys()):
        if key not in ("pl", "output_df", "output_lf", "input_lf") and not key.startswith("input_lf_"):
            val = local_vars.get(key)
            if isinstance(val, pl.DataFrame | pl.LazyFrame):
                del local_vars[key]
    gc.collect()


def _to_lazyframe(value: Any) -> pl.LazyFrame | None:
    """Convert value to LazyFrame if possible."""
    if isinstance(value, pl.LazyFrame):
        return value
    if isinstance(value, pl.DataFrame):
        return value.lazy()
    return None


def _find_result_in_locals(local_vars: dict) -> pl.LazyFrame | None:
    """Find result from well-known variable names."""
    for var_name in ["output_lf", "output_df", "result", "df"]:
        value = local_vars.get(var_name)
        if value is not None:
            result = _to_lazyframe(value)
            if result is not None:
                return result
    return None


def _find_any_dataframe(local_vars: dict) -> pl.LazyFrame | None:
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


def _try_eval_last_line(code: str, global_vars: dict, local_vars: dict) -> pl.LazyFrame | None:
    """Try to eval the last line if it's an expression."""
    lines = code.splitlines()
    last_line = lines[-1].strip()

    if last_line and not last_line.startswith("#") and "=" not in last_line:
        try:
            result = eval(last_line, global_vars, local_vars)
            return _to_lazyframe(result)
        except Exception:
            pass
    return None


def _extract_result(
    code: str, global_vars: dict, local_vars: dict, has_inputs: bool
) -> tuple[pl.LazyFrame | None, str | None]:
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


@log_node
def execute_polars_code(node_id: int, input_ids: list[int], settings: dict) -> dict:
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
                return {
                    "success": False,
                    "error": f"Polars Code error on node #{node_id}: No code provided and no input to pass through.",
                }
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
                    return {
                        "success": False,
                        "error": f"Polars Code error on node #{node_id}: Code must produce a DataFrame or LazyFrame, got {type(result).__name__}",
                    }
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


def build_polars_code_schema(input_lfs: list[pl.LazyFrame], settings: dict) -> pl.LazyFrame:
    """Resolve a polars_code node's output schema by running the user code
    against EMPTY (0-row) input frames. Raises on failure (caught upstream)."""
    local_vars = {"pl": pl, "output_df": None, "output_lf": None}
    if len(input_lfs) == 1:
        local_vars["input_df"] = input_lfs[0].collect()
        local_vars["input_lf"] = input_lfs[0]
    elif len(input_lfs) > 1:
        for i, lf in enumerate(input_lfs, start=1):
            local_vars[f"input_df_{i}"] = lf.collect()
            local_vars[f"input_lf_{i}"] = lf
        local_vars["input_df"] = local_vars["input_df_1"]
        local_vars["input_lf"] = local_vars["input_lf_1"]

    code = (settings.get("polars_code_input", {}).get("polars_code") or "").strip()
    if not code:
        if not input_lfs:
            raise ValueError("No code provided and no input to pass through.")
        return input_lfs[0]

    global_vars = {"pl": pl}
    try:
        exec(code, global_vars, local_vars)
    except SyntaxError:
        result = eval(code, global_vars, local_vars)
        lf = _to_lazyframe(result)
        if lf is None:
            raise ValueError("Code must produce a DataFrame or LazyFrame") from None
        return lf

    result_lf, error_msg = _extract_result(code, global_vars, local_vars, len(input_lfs) > 0)
    if error_msg:
        raise ValueError(error_msg)
    return result_lf
