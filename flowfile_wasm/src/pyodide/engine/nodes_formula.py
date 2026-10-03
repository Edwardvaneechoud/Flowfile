import polars as pl

from .errors import format_error_lf
from .log import log_node
from .state import get_lazyframe, get_schema, store_lazyframe

# "Auto" / unmapped => no cast.
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
    # Lazy: micropip-installed at runtime, absent when the engine imports at boot.
    from polars_expr_transformer import simple_function_to_expr

    return simple_function_to_expr(expr_text)


def formula_entries(settings: dict) -> list[dict]:
    """The ordered entries: ``functions`` when present (core omits ``function`` for 2+), else the legacy ``function``."""
    functions = settings.get("functions")
    if isinstance(functions, list):
        return [fn for fn in functions if isinstance(fn, dict)]
    fn = settings.get("function")
    return [fn] if isinstance(fn, dict) else []


def build_formula(input_lf: pl.LazyFrame, settings: dict) -> pl.LazyFrame:
    """Build the formula LazyFrame: each entry adds (or replaces) one column from an expression
    string like ``[a] + [b] * 2``, chained so entry N sees the columns entries 1..N-1 made.

    Blank expressions are skipped, as in core (no store, no collect)."""
    lf = input_lf
    for fn in formula_entries(settings):
        expr_text = (fn.get("function") or "").strip()
        if not expr_text:
            continue
        field = fn.get("field") or {}
        name = field.get("name") or "new_column"
        expr = _to_expr(expr_text)
        dtype = _DTYPE_MAP.get(field.get("data_type"))
        if dtype is not None:
            expr = expr.cast(dtype)
        lf = lf.with_columns(expr.alias(name))
    return lf


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
