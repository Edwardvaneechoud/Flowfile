import polars as pl

from .errors import format_error_lf
from .log import log_node
from .state import get_lazyframe, get_schema, store_lazyframe


def build_join(left_lf: pl.LazyFrame, right_lf: pl.LazyFrame, settings: dict) -> pl.LazyFrame:
    """Build the joined LazyFrame (no store, no collect). Raises on bad config."""
    join_input = settings.get("join_input", {})
    join_type = join_input.get("join_type", "inner")
    mapping = join_input.get("join_mapping", [])
    right_suffix = join_input.get("right_suffix", "_right")

    if not mapping:
        raise ValueError("No join columns specified. Configure the join mapping in the node settings.")

    left_on = [m.get("left_col") for m in mapping]
    right_on = [m.get("right_col") for m in mapping]

    # Validate columns exist using schema
    left_schema = left_lf.collect_schema()
    right_schema = right_lf.collect_schema()

    missing_left = [c for c in left_on if c not in left_schema]
    missing_right = [c for c in right_on if c not in right_schema]
    if missing_left:
        raise ValueError(f"Left columns not found: {missing_left}. Available columns: {list(left_schema.keys())}")
    if missing_right:
        raise ValueError(f"Right columns not found: {missing_right}. Available columns: {list(right_schema.keys())}")

    return left_lf.join(right_lf, left_on=left_on, right_on=right_on, how=join_type, suffix=right_suffix)


@log_node
def execute_join(node_id: int, left_id: int, right_id: int, settings: dict) -> dict:
    """Execute join node (lazy)"""
    left_lf = get_lazyframe(left_id)
    right_lf = get_lazyframe(right_id)

    if left_lf is None:
        return {
            "success": False,
            "error": f"Join error on node #{node_id}: No left input data from node #{left_id}. Make sure the upstream node executed successfully.",
        }
    if right_lf is None:
        return {
            "success": False,
            "error": f"Join error on node #{node_id}: No right input data from node #{right_id}. Make sure the upstream node executed successfully.",
        }

    try:
        result_lf = build_join(left_lf, right_lf, settings)
        store_lazyframe(node_id, result_lf)
        return {"success": True, "schema": get_schema(node_id), "has_data": True}
    except Exception as e:
        return {"success": False, "error": format_error_lf("join", node_id, e, left_lf)}


def build_cross_join(left_lf: pl.LazyFrame, right_lf: pl.LazyFrame, settings: dict) -> pl.LazyFrame:
    """Build the cross-joined (cartesian product) LazyFrame (no store, no collect)."""
    suffix = (settings.get("cross_join_input") or {}).get("right_suffix", "_right")
    return left_lf.join(right_lf, how="cross", suffix=suffix)


@log_node
def execute_cross_join(node_id: int, left_id: int, right_id: int, settings: dict) -> dict:
    """Execute cross join node (lazy) - cartesian product of both inputs."""
    left_lf = get_lazyframe(left_id)
    right_lf = get_lazyframe(right_id)

    if left_lf is None:
        return {
            "success": False,
            "error": f"Cross join error on node #{node_id}: No left input data from node #{left_id}. Make sure the upstream node executed successfully.",
        }
    if right_lf is None:
        return {
            "success": False,
            "error": f"Cross join error on node #{node_id}: No right input data from node #{right_id}. Make sure the upstream node executed successfully.",
        }

    try:
        result_lf = build_cross_join(left_lf, right_lf, settings)
        store_lazyframe(node_id, result_lf)
        return {"success": True, "schema": get_schema(node_id), "has_data": True}
    except Exception as e:
        return {"success": False, "error": format_error_lf("cross_join", node_id, e, left_lf)}


# Map the friendly UI mode to Polars concat strategies. Relaxed variants cast to a
# common supertype so e.g. Int64 + Float64 columns stack without erroring.
_UNION_HOW = {
    "vertical": "vertical_relaxed",
    "diagonal": "diagonal_relaxed",
}


def build_union(lfs: list[pl.LazyFrame], settings: dict) -> pl.LazyFrame:
    """Build the unioned (stacked) LazyFrame from N inputs (no store, no collect).

    'vertical' requires matching columns; 'diagonal' takes the union of columns,
    filling missing ones with null."""
    valid = [lf for lf in lfs if lf is not None]
    if not valid:
        raise ValueError("Union needs at least one connected input.")
    if len(valid) == 1:
        return valid[0]
    mode = (settings.get("union_input") or {}).get("mode", "diagonal")
    how = _UNION_HOW.get(mode, "diagonal_relaxed")
    return pl.concat(valid, how=how)


@log_node
def execute_union(node_id: int, input_ids: list[int], settings: dict) -> dict:
    """Execute union node (lazy) - stacks rows from all connected inputs."""
    lfs = [get_lazyframe(i) for i in input_ids]
    missing = [i for i, lf in zip(input_ids, lfs, strict=True) if lf is None]
    if missing:
        return {
            "success": False,
            "error": f"Union error on node #{node_id}: No input data from node(s) #{missing}. Make sure the upstream nodes executed successfully.",
        }

    try:
        result_lf = build_union(lfs, settings)
        store_lazyframe(node_id, result_lf)
        return {"success": True, "schema": get_schema(node_id), "has_data": True}
    except Exception as e:
        in_lf = lfs[0] if lfs else None
        return {"success": False, "error": format_error_lf("union", node_id, e, in_lf)}
