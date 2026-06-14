import polars as pl

from .errors import format_error_lf
from .log import log_node
from .state import get_lazyframe, get_schema, store_lazyframe

# "Auto" (or anything unmapped) => no cast; keep the expression's natural dtype.
_DTYPE_MAP = {
    "String": pl.String,
    "Utf8": pl.String,
    "Int64": pl.Int64,
    "Int32": pl.Int32,
    "Float64": pl.Float64,
    "Float32": pl.Float32,
    "Boolean": pl.Boolean,
    "Date": pl.Date,
    "Datetime": pl.Datetime,
}


def _to_expr(expr_text: str) -> pl.Expr:
    # Lazy import: polars-expr-transformer is micropip-installed on demand and is
    # NOT present when the engine package loads at boot. Importing at module top
    # would break the whole engine bootstrap.
    from polars_expr_transformer import simple_function_to_expr

    return simple_function_to_expr(expr_text)


def build_formula(input_lf: pl.LazyFrame, settings: dict) -> pl.LazyFrame:
    """Build the formula LazyFrame: add (or replace) one column from an expression
    string like ``[a] + [b] * 2`` (no store, no collect)."""
    fn = settings.get("function") or {}
    field = fn.get("field") or {}
    name = field.get("name") or "new_column"
    expr_text = (fn.get("function") or "").strip()
    if not expr_text:
        return input_lf
    expr = _to_expr(expr_text)
    dtype = _DTYPE_MAP.get(field.get("data_type"))
    if dtype is not None:
        expr = expr.cast(dtype)
    return input_lf.with_columns(expr.alias(name))


@log_node
def execute_formula(node_id: int, input_id: int, settings: dict) -> dict:
    """Execute formula node - adds a computed column (lazy)."""
    input_lf = get_lazyframe(input_id)
    if input_lf is None:
        return {
            "success": False,
            "error": f"Formula error on node #{node_id}: No input data from node #{input_id}. Make sure the upstream node executed successfully.",
        }

    try:
        result_lf = build_formula(input_lf, settings)
        store_lazyframe(node_id, result_lf)
        return {"success": True, "schema": get_schema(node_id), "has_data": True}
    except Exception as e:
        return {"success": False, "error": format_error_lf("formula", node_id, e, input_lf)}
