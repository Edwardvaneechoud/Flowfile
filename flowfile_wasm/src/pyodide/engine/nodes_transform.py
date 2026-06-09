import polars as pl

from .errors import format_error_lf
from .state import get_lazyframe, get_schema, store_lazyframe


def convert_filter_value(value: str, dtype) -> any:
    """Convert a single filter value to match column data type"""
    if dtype in (pl.Int8, pl.Int16, pl.Int32, pl.Int64, pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64):
        return int(value)
    elif dtype in (pl.Float32, pl.Float64):
        return float(value)
    elif dtype == pl.Boolean:
        return value.lower() == "true"
    return value


def convert_filter_values(values: list[str], dtype) -> list:
    """Convert filter values to match column data type"""
    return [convert_filter_value(v, dtype) for v in values]


def build_filter(input_lf: pl.LazyFrame, settings: dict) -> pl.LazyFrame:
    """Build the filtered LazyFrame from input + settings (no store, no collect)."""
    filter_input = settings.get("filter_input", {})
    mode = filter_input.get("mode", "basic")

    if mode == "advanced":
        expr = filter_input.get("advanced_filter", "")
        if expr:
            return input_lf.filter(eval(expr))
        return input_lf

    basic = filter_input.get("basic_filter", {})
    field = basic.get("field", "")
    operator = basic.get("operator", "equals")
    value = basic.get("value", "")
    value2 = basic.get("value2", "")

    if not field:
        return input_lf

    col = pl.col(field)
    # Get column data type from schema
    schema = input_lf.collect_schema()
    col_dtype = schema.get(field)

    if operator == "equals":
        return input_lf.filter(col == convert_filter_value(value, col_dtype))
    elif operator == "not_equals":
        return input_lf.filter(col != convert_filter_value(value, col_dtype))
    elif operator == "greater_than":
        return input_lf.filter(col > convert_filter_value(value, col_dtype))
    elif operator == "greater_than_or_equals":
        return input_lf.filter(col >= convert_filter_value(value, col_dtype))
    elif operator == "less_than":
        return input_lf.filter(col < convert_filter_value(value, col_dtype))
    elif operator == "less_than_or_equals":
        return input_lf.filter(col <= convert_filter_value(value, col_dtype))
    elif operator == "contains":
        return input_lf.filter(col.str.contains(value))
    elif operator == "not_contains":
        return input_lf.filter(~col.str.contains(value))
    elif operator == "starts_with":
        return input_lf.filter(col.str.starts_with(value))
    elif operator == "ends_with":
        return input_lf.filter(col.str.ends_with(value))
    elif operator == "is_null":
        return input_lf.filter(col.is_null())
    elif operator == "is_not_null":
        return input_lf.filter(col.is_not_null())
    elif operator == "in":
        values = [v.strip() for v in value.split(",")]
        return input_lf.filter(col.is_in(convert_filter_values(values, col_dtype)))
    elif operator == "not_in":
        values = [v.strip() for v in value.split(",")]
        return input_lf.filter(~col.is_in(convert_filter_values(values, col_dtype)))
    elif operator == "between":
        v1 = convert_filter_value(value, col_dtype)
        v2 = convert_filter_value(value2, col_dtype)
        return input_lf.filter((col >= v1) & (col <= v2))
    return input_lf


def execute_filter(node_id: int, input_id: int, settings: dict) -> dict:
    """Execute filter node - chains onto input LazyFrame"""
    input_lf = get_lazyframe(input_id)
    if input_lf is None:
        return {
            "success": False,
            "error": f"Filter error on node #{node_id}: No input data from node #{input_id}. Make sure the upstream node executed successfully.",
        }

    field = settings.get("filter_input", {}).get("basic_filter", {}).get("field")
    try:
        result_lf = build_filter(input_lf, settings)
        store_lazyframe(node_id, result_lf)
        return {"success": True, "schema": get_schema(node_id), "has_data": True}
    except Exception as e:
        return {"success": False, "error": format_error_lf("filter", node_id, e, input_lf, field)}


def build_select(input_lf: pl.LazyFrame, settings: dict) -> pl.LazyFrame:
    """Build the column-selected/renamed LazyFrame (no store, no collect)."""
    select_input = settings.get("select_input", [])
    if not select_input:
        return input_lf

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

    return input_lf.select(exprs) if exprs else input_lf


def execute_select(node_id: int, input_id: int, settings: dict) -> dict:
    """Execute select node - column selection/renaming (lazy)"""
    input_lf = get_lazyframe(input_id)
    if input_lf is None:
        return {
            "success": False,
            "error": f"Select error on node #{node_id}: No input data from node #{input_id}. Make sure the upstream node executed successfully.",
        }

    try:
        result_lf = build_select(input_lf, settings)
        store_lazyframe(node_id, result_lf)
        return {"success": True, "schema": get_schema(node_id), "has_data": True}
    except Exception as e:
        return {"success": False, "error": format_error_lf("select", node_id, e, input_lf)}


def build_sort(input_lf: pl.LazyFrame, settings: dict) -> pl.LazyFrame:
    """Build the sorted LazyFrame (no store, no collect)."""
    # sort_input is now a flat list matching flowfile_core: [{column, how}]
    sort_input = settings.get("sort_input", [])
    if not sort_input:
        return input_lf
    by = [c.get("column") for c in sort_input]
    descending = [c.get("how") == "desc" for c in sort_input]
    return input_lf.sort(by, descending=descending)


def execute_sort(node_id: int, input_id: int, settings: dict) -> dict:
    """Execute sort node (lazy)"""
    input_lf = get_lazyframe(input_id)
    if input_lf is None:
        return {
            "success": False,
            "error": f"Sort error on node #{node_id}: No input data from node #{input_id}. Make sure the upstream node executed successfully.",
        }

    try:
        result_lf = build_sort(input_lf, settings)
        store_lazyframe(node_id, result_lf)
        return {"success": True, "schema": get_schema(node_id), "has_data": True}
    except Exception as e:
        return {"success": False, "error": format_error_lf("sort", node_id, e, input_lf)}


def build_unique(input_lf: pl.LazyFrame, settings: dict) -> pl.LazyFrame:
    """Build the de-duplicated LazyFrame (no store, no collect)."""
    unique_input = settings.get("unique_input", {})
    subset = unique_input.get("subset") or unique_input.get("columns") or []
    keep = unique_input.get("keep") or unique_input.get("strategy") or "first"
    maintain_order = unique_input.get("maintain_order", True)
    if subset:
        return input_lf.unique(subset=subset, keep=keep, maintain_order=maintain_order)
    return input_lf.unique(keep=keep, maintain_order=maintain_order)


def execute_unique(node_id: int, input_id: int, settings: dict) -> dict:
    """Execute unique node (lazy)"""
    input_lf = get_lazyframe(input_id)
    if input_lf is None:
        return {
            "success": False,
            "error": f"Unique error on node #{node_id}: No input data from node #{input_id}. Make sure the upstream node executed successfully.",
        }

    try:
        result_lf = build_unique(input_lf, settings)
        store_lazyframe(node_id, result_lf)
        return {"success": True, "schema": get_schema(node_id), "has_data": True}
    except Exception as e:
        return {"success": False, "error": format_error_lf("unique", node_id, e, input_lf)}


def build_head(input_lf: pl.LazyFrame, settings: dict) -> pl.LazyFrame:
    """Build the head/limit LazyFrame (no store, no collect)."""
    n = settings.get("head_input", {}).get("n", 10)
    return input_lf.head(n)


def execute_head(node_id: int, input_id: int, settings: dict) -> dict:
    """Execute head/limit node (lazy)"""
    input_lf = get_lazyframe(input_id)
    if input_lf is None:
        return {
            "success": False,
            "error": f"Head error on node #{node_id}: No input data from node #{input_id}. Make sure the upstream node executed successfully.",
        }

    try:
        result_lf = build_head(input_lf, settings)
        store_lazyframe(node_id, result_lf)
        return {"success": True, "schema": get_schema(node_id), "has_data": True}
    except Exception as e:
        return {"success": False, "error": format_error_lf("head", node_id, e, input_lf)}


def execute_preview(node_id: int, input_id: int) -> dict:
    """Execute preview node - just passes through the LazyFrame"""
    input_lf = get_lazyframe(input_id)
    if input_lf is None:
        return {
            "success": False,
            "error": f"Preview error on node #{node_id}: No input data from node #{input_id}. Make sure the upstream node executed successfully.",
        }

    try:
        store_lazyframe(node_id, input_lf)
        return {"success": True, "schema": get_schema(node_id), "has_data": True}
    except Exception as e:
        return {"success": False, "error": format_error_lf("explore_data", node_id, e, input_lf)}
