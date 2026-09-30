"""The notebook dialect: every import, name, attribute, call and operator a cell may use when core interprets it.

Pure string-keyed data, so nothing here imports the frame. :mod:`flowfile_core.notebook.interpret` looks
every attribute read, call and subscript up by ``(kind, attribute)``, a kind being a value's exact type;
anything not listed needs a kernel. The list is positive and no larger than the FlowFrame exporter's output:
``tests/notebook`` ties every entry to a rendered corpus cell or to the exporter handler that emits it.

Usages: ``call`` (called as ``x.attr(...)``), ``read`` (read as ``x.attr``), ``both``,
``decorator`` (only as ``@fl.attr`` or ``@fl.attr(...)``). The attribute ``__call__`` makes a
kind callable by name, ``[]`` subscriptable with a literal string key, and ``*`` makes every
attribute a data key (``fl.custom_nodes.<node key>``).
"""

from __future__ import annotations

CALL = "call"
READ = "read"
BOTH = "both"
DECORATOR = "decorator"
SUBSCRIPT = "subscript"
ALLOW = "allow"
REFUSE = "refuse"

_DTYPES = (
    "Array", "Binary", "Boolean", "Categorical", "Date", "Datetime", "Decimal", "Duration", "Enum", "Field",
    "Float32", "Float64", "Int8", "Int16", "Int32", "Int64", "Int128", "List", "Null", "Object", "String",
    "Struct", "Time", "UInt8", "UInt16", "UInt32", "UInt64", "Unknown", "Utf8",
)  # fmt: skip
_SELECTORS = (
    "all_", "boolean", "by_dtype", "categorical", "contains", "date", "datetime", "duration", "ends_with", "float_",
    "integer", "list_", "matches", "numeric", "object_", "starts_with", "string", "struct", "temporal", "time",
)  # fmt: skip
_CONNECTION_HELPERS = (
    "create_cloud_storage_connection", "create_cloud_storage_connection_if_not_exists", "del_cloud_storage_connection",
    "create_database_connection", "create_database_connection_if_not_exists", "del_database_connection",
    "get_all_available_cloud_storage_connections", "get_all_available_database_connections",
    "get_database_connection_by_name",
)  # fmt: skip

_FL_CALLS = (
    "col", "lit", "len", "when", "from_raw_data", "read_csv", "scan_csv", "scan_parquet", "read_excel", "list_files",
    "read_database", "read_kafka", "read_api", "read_from_cloud_storage", "read_catalog_table", "read_catalog_sql",
    "write_catalog_table", "write_database", "write_to_cloud_storage", "Gate", "RunFlow", "PythonScript",
    "polars_code", "sql", "canvas_node", "FlowInput", "flow_ref", "concat", "FuzzyMapping", "Parameter",
    "add_flow_parameter",
)  # fmt: skip
_CORE_CLASSES = (
    "FlowGraph", "FlowDataEngine", "FlowNode", "FlowSettings", "FlowInformation", "FlowfileColumn",
    "FullCloudStorageConnection", "FlowFrame", "GroupByFrame", "DataType", "DataTypeClass",
)  # fmt: skip
_NOT_EMITTED = (
    "Node", "NodeType", "CustomNode", "custom_node", "FlowOutput", "FlowRef", "ParamType", "GateOperator",
    "set_flow_parameter", "NativeNodeError", "from_dict", "read_parquet", "scan_delta", "scan_csv_from_cloud_storage",
    "scan_parquet_from_cloud_storage", "scan_json_from_cloud_storage", "column", "count", "cum_count", "sum", "min",
    "max", "mean",
)  # fmt: skip

FL_VERDICTS: dict[str, tuple[str, str]] = {
    **{name: (ALLOW, BOTH) for name in _DTYPES},
    **{name: (ALLOW, CALL) for name in _FL_CALLS},
    "python_script": (ALLOW, DECORATOR),
    "custom_nodes": (ALLOW, READ),
    "kernels": (REFUSE, "lists and reaches the user's kernels"),
    "node_designer": (REFUSE, "its SecretSelector decrypts secrets for a caller-supplied user"),
    "start_web_ui": (REFUSE, "starts the web UI"),
    "open_graph_in_editor": (REFUSE, "opens a graph in the web UI"),
    "create_flow_graph": (REFUSE, "builds a graph outside the notebook's"),
    **{name: (REFUSE, "is a core class, not part of a flow description") for name in _CORE_CLASSES},
    "node_interface": (REFUSE, "is a module"),
    "transform_schema": (REFUSE, "is a module"),
    **{name: (REFUSE, "writes or reads stored connections") for name in _CONNECTION_HELPERS},
    "register_flow": (REFUSE, "writes a flow file and a catalog row"),
    **{
        name: (REFUSE, "navigates the catalog, and can create namespaces")
        for name in ("CatalogReference", "SchemaReference", "get_catalog", "list_catalogs", "default_schema")
    },
    **{name: (REFUSE, "is a selector, which the notebook does not emit") for name in _SELECTORS},
    **{name: (REFUSE, "is not part of the code the notebook emits") for name in _NOT_EMITTED},
}
"""Every name ``import flowfile as fl`` provides: ``(ALLOW, usage)`` or ``(REFUSE, reason)``."""

_READERS = ("scan_ipc", "scan_ndjson", "read_avro", "read_ipc_stream")
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
    "apply_model", "data_cleansing", "drop", "dynamic_rename", "evaluate_model", "filter", "filter_split", "fuzzy_join",
    "group_by", "head", "join", "multi_field_formula", "pivot", "polars_code", "random_split", "rename", "sample",
    "select", "solve_graph", "sort", "text_to_rows", "to_flow_output", "train_model", "unique", "unpivot", "wait_for",
    "with_columns", "with_row_index", "write_csv", "write_excel", "write_parquet",
)  # fmt: skip
_EXPR_METHODS = (
    "alias", "cast", "count", "first", "last", "max", "mean", "median", "min", "n_unique", "std", "sum", "var", "over",
    "round", "is_in", "is_null", "is_not_null", "not_", "fill_null", "rank", "then", "otherwise",
    *_WINDOW, *_CUMULATIVE, *_FORMULA_EXPR,
)  # fmt: skip

ALLOWLIST: dict[str, dict[str, str]] = {
    "pl": {"DataFrame": CALL},
    "datetime": {"date": CALL, "datetime": CALL},
    "FlowFrame": {name: CALL for name in _FRAME_METHODS},
    "GroupByFrame": {"agg": CALL},
    "Expr": {**{name: CALL for name in _EXPR_METHODS}, "str": READ, "dt": READ},
    "StringNS": {name: CALL for name in ("contains", "starts_with", "ends_with", "join", *_FORMULA_STR)},
    "DateTimeNS": {name: CALL for name in _FORMULA_DT},
    "Gate": {"then": READ, "otherwise": READ},
    "NodeOutputs": {"output": READ, "[]": SUBSCRIPT},
    "CustomNodes": {"*": READ, "[]": SUBSCRIPT},
    "CustomNodeFactory": {"__call__": CALL, "node": CALL},
    "ScriptFunction": {"__call__": CALL, "node": CALL},
    "helper": {"__call__": CALL},
    "reader": {"__call__": CALL},
}
"""Receiver kind -> attribute -> usage. ``fl`` is looked up in :data:`FL_VERDICTS` instead."""

IMPORTS: dict[tuple[str, str | None], str] = {
    ("flowfile", "fl"): "fl",
    ("polars", "pl"): "pl",
    ("datetime", None): "datetime",
    ("hashlib", None): "inert",
    ("json", None): "inert",
}
"""``import <module> [as <alias>]`` -> what it binds. Any other import only as a Python Script's prelude."""

FROM_IMPORTS: dict[str, frozenset[str]] = {"flowfile_frame": frozenset(_READERS)}
"""``from <module> import <name>`` (no alias): the frame readers ``fl`` does not re-export."""

HELPERS: tuple[str, ...] = ("_flowfile_flow_parameter", "_flowfile_expr_literal")
"""Module helpers the render defines; a cell's ``def`` binds one only when its text is the render's own."""

USER_NAME = r"[A-Za-z][A-Za-z0-9_]*"
GENERATED_NAMES: tuple[str, ...] = (
    r"_polars_code_\d+",
    r"_script_\d+",
    r"_join_\d+_(left|right)",
    r"_fuzzy_(left|right)_\d+",
)
"""The ``_`` names the exporter binds; any other name starting with ``_`` is refused."""

RESERVED_NAMES: frozenset[str] = frozenset({"fl", "pl", "flow", "datetime", "hashlib", "json", "display"})
"""Names a cell binds only through their import (or never: ``flow`` is the session graph)."""

BINARY_OPERATORS: frozenset[str] = frozenset({"Add", "Sub", "Mult", "Div", "FloorDiv", "Mod", "BitAnd", "BitOr"})
COMPARE_OPERATORS: frozenset[str] = frozenset({"Eq", "NotEq", "Lt", "LtE", "Gt", "GtE"})
"""Operators, only with an expression on one side (a comparison takes one operator, never a chain)."""

ARGUMENT_KINDS: dict[tuple[str, str], dict[int | str, str]] = {
    ("fl", "add_flow_parameter"): {0: "graph", "flow": "graph"},
    ("fl", "FlowInput"): {"flow_graph": "graph", "sample": "pl_frame"},
    ("fl", "polars_code"): {0: "polars_code_def", "code": "polars_code_def"},
    ("FlowFrame", "polars_code"): {0: "polars_code_def", "code": "polars_code_def"},
}
"""Arguments (position or keyword) that also take one non-data kind: the session graph ``flow``, a flow
input's sample frame, or a Polars Code ``def``."""

REFUSED_KEYWORDS: dict[tuple[str, str], frozenset[str]] = {
    ("fl", "RunFlow"): frozenset({"name", "schema", "overwrite"}),
}

KEYWORD_REQUIRES: dict[tuple[str, str], tuple[str, str]] = {
    ("FlowFrame", "with_columns"): ("flowfile_formulas", "output_column_datatypes"),
}
"""A keyword that is only accepted with another: typed formulas never reach the frame's translating ``eval``."""

LITERAL_ARGUMENTS: dict[tuple[str, str], str] = {
    ("pl", "DataFrame"): "sample",
    ("datetime", "date"): "ints",
    ("datetime", "datetime"): "ints",
    ("fl", "canvas_node"): "node_id",
}
"""Calls whose arguments must have one literal shape: a flow input's sample frame, integer date parts, a node id."""

BOUNDS: dict[str, int] = {
    "cells_per_request": 1_000,
    "bytes_per_cell": 4 * 1024 * 1024,
    "bytes_per_request": 16 * 1024 * 1024,
    "cell_id_length": 128,
    "provenance_entries_per_request": 10_000,
    "ast_nodes_per_cell": 500_000,
    "statements_per_cell": 2_000,
    "depth": 100,
    "string_length": 1_000_000,
    "literal_elements_per_request": 2_000_000,
    "steps_per_request": 5_000_000,
    "nodes_per_request": 10_000,
    "expression_chars_per_request": 16_000_000,
}
"""Size limits: the first five per request (the runner), the rest per cell or per run (the interpreter).

``provenance_entries_per_request`` matches ``nodes_per_request``: provenance lists each canvas node a cell
renders once, and a canvas with more nodes than a sync may build cannot sync anyway.
"""
