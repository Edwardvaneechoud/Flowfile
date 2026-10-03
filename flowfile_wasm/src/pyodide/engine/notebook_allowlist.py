"""The notebook dialect of the browser build: every name, attribute, call and operator a cell may use.

The same shape as ``flowfile_core/notebook/allowlist.py`` and a subset of it: what the browser's
node set can express. Pure string-keyed data. :mod:`notebook_interpret` looks every attribute read
and call up by ``(kind, attribute)``; anything not listed is refused on its line, the way the
full app answers "this needs a kernel".

The expression tables (``Expr``, ``StringNS``, ``DateTimeNS``) are flowfile_core's own, entry for
entry: whether a translated formula renders as code or stays formula text is decided by them, and
both builds must decide it the same way.
"""

from __future__ import annotations

CALL = "call"
READ = "read"
BOTH = "both"
ALLOW = "allow"

_DTYPES = (
    "Array", "Binary", "Boolean", "Categorical", "Date", "Datetime", "Decimal", "Duration", "Enum", "Field",
    "Float32", "Float64", "Int8", "Int16", "Int32", "Int64", "Int128", "List", "Null", "Object", "String",
    "Struct", "Time", "UInt8", "UInt16", "UInt32", "UInt64", "Unknown", "Utf8",
)  # fmt: skip

_FL_CALLS = (
    "col", "lit", "len", "when", "from_raw_data", "read_csv", "scan_csv", "scan_parquet", "read_excel",
    "polars_code", "canvas_node", "concat",
)  # fmt: skip

FL_VERDICTS: dict[str, tuple[str, str]] = {
    **{name: (ALLOW, BOTH) for name in _DTYPES},
    **{name: (ALLOW, CALL) for name in _FL_CALLS},
}
"""Every name of ``import flowfile as ff`` a cell may use: ``(ALLOW, usage)``."""

_WINDOW = ("rolling_sum", "rolling_mean", "rolling_min", "rolling_max", "rolling_std")
_CUMULATIVE = ("cum_sum", "cum_count", "cum_min", "cum_max")
_FORMULA_EXPR = (
    "abs", "arccos", "arcsin", "arctan", "ceil", "cos", "eq", "exp", "floor", "hash", "is_between", "log", "mod",
    "neg", "pow", "sign", "sin", "sqrt", "tan", "tanh",
)  # fmt: skip
_FORMULA_STR = (
    "decode", "encode", "len_chars", "reverse", "strip_chars", "strip_chars_end", "strip_chars_start", "to_date",
    "to_datetime", "to_lowercase", "to_titlecase", "to_uppercase",
)  # fmt: skip
_FORMULA_DT = (
    "day", "hour", "minute", "month", "month_end", "month_start", "ordinal_day", "quarter", "second", "to_string",
    "total_days", "total_nanoseconds", "total_seconds", "truncate", "week", "weekday", "year",
)  # fmt: skip

_FRAME_METHODS = (
    "drop", "dynamic_rename", "filter", "group_by", "head", "join", "pivot", "polars_code", "rename", "select",
    "sort", "unique", "unpivot", "with_columns", "with_row_index", "write_csv", "write_excel", "write_parquet",
)  # fmt: skip
_EXPR_METHODS = (
    "alias", "cast", "count", "first", "last", "max", "mean", "median", "min", "n_unique", "std", "sum", "var", "over",
    "round", "is_in", "is_null", "is_not_null", "not_", "fill_null", "rank", "then", "otherwise",
    *_WINDOW, *_CUMULATIVE, *_FORMULA_EXPR,
)  # fmt: skip

ALLOWLIST: dict[str, dict[str, str]] = {
    "datetime": {"date": CALL, "datetime": CALL},
    "FlowFrame": {name: CALL for name in _FRAME_METHODS},
    "GroupByFrame": {"agg": CALL},
    "Expr": {**{name: CALL for name in _EXPR_METHODS}, "str": READ, "dt": READ},
    "StringNS": {name: CALL for name in ("contains", "starts_with", "ends_with", "join", *_FORMULA_STR)},
    "DateTimeNS": {name: CALL for name in _FORMULA_DT},
}
"""Receiver kind -> attribute -> usage. ``ff`` is looked up in :data:`FL_VERDICTS` instead."""

IMPORTS: dict[tuple[str, str | None], str] = {
    ("flowfile", "ff"): "ff",
    ("polars", "pl"): "pl",
    ("datetime", None): "datetime",
}
"""``import <module> [as <alias>]`` -> what it binds."""

USER_NAME = r"[A-Za-z][A-Za-z0-9_]*"
GENERATED_NAMES: tuple[str, ...] = (
    r"_polars_code_\d+",
    r"_join_\d+_(left|right)",
)
"""The ``_`` names the render binds; any other name starting with ``_`` is refused."""

RESERVED_NAMES: frozenset[str] = frozenset({"ff", "pl", "flow", "datetime", "hashlib", "json", "display"})

BINARY_OPERATORS: frozenset[str] = frozenset({"Add", "Sub", "Mult", "Div", "FloorDiv", "Mod", "BitAnd", "BitOr"})
COMPARE_OPERATORS: frozenset[str] = frozenset({"Eq", "NotEq", "Lt", "LtE", "Gt", "GtE"})
"""Operators, only with an expression on one side (a comparison takes one operator, never a chain)."""

BOUNDS: dict[str, int] = {
    "cells_per_request": 1_000,
    "bytes_per_cell": 4 * 1024 * 1024,
    "bytes_per_request": 16 * 1024 * 1024,
    "ast_nodes_per_cell": 500_000,
    "statements_per_cell": 2_000,
    "depth": 100,
    "string_length": 1_000_000,
    "literal_elements_per_request": 2_000_000,
    "steps_per_request": 5_000_000,
    "nodes_per_request": 10_000,
}
"""Size limits, flowfile_core's own values."""
