"""The notebook dialect: every import, name, attribute, call and operator a cell may use when core interprets it.

Pure data. :mod:`flowfile_core.notebook.interpret` decides a value's *kind* from its exact type and
looks each attribute read, call and subscript up here by ``(kind, attribute)``; anything not listed
fails on its line with "this needs a kernel". The list is positive and as small as the FlowFrame
exporter's output (the notebook render): every entry is used by a rendered corpus cell or named in
:data:`EMITTED_OUTSIDE_THE_CORPUS` with the exporter handler that emits it, and every ``fl`` name
has a verdict in :data:`FL_VERDICTS`. Keys are strings, so nothing here imports the frame.

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

_CG = "flowfile_core.flowfile.code_generator"
_FF = f"{_CG}.code_generator:FlowGraphToFlowFrameConverter"
_BASE = f"{_CG}.code_generator:FlowGraphCodeConverter"
_CONNECTORS = f"{_CG}.connector_handlers:ConnectorHandlersMixin"
_NATIVE = f"{_CG}.native_handlers:NativeHandlersMixin"
_FILTER = f"{_CG}.expression_helpers:ExpressionHelpersMixin._create_basic_filter_expr"
_WINDOWS = f"{_CG}.transform_handlers:TransformHandlersMixin._build_window_expr_code"
_FORMULA = f"{_FF}._translate_to_ff_code"

EMITTED_OUTSIDE_THE_CORPUS: dict[tuple[str, str], str] = {
    **{
        ("fl", name): f"{_CG}.native_handlers:_dtype_expr"
        for name in _DTYPES
        if name not in ("Boolean", "Float64", "Int32", "Int64", "Utf8")
    },
    ("fl", "when"): _WINDOWS,
    ("fl", "read_csv"): f"{_BASE}._handle_csv_read_non_utf8",
    ("fl", "list_files"): f"{_CONNECTORS}._handle_list_files",
    ("fl", "read_database"): f"{_CONNECTORS}._handle_database_reader",
    ("fl", "read_kafka"): f"{_FF}._handle_kafka_source",
    ("fl", "read_api"): f"{_CONNECTORS}._handle_rest_api_reader",
    ("fl", "read_from_cloud_storage"): f"{_FF}._handle_cloud_storage_reader",
    ("fl", "read_catalog_sql"): f"{_CONNECTORS}._handle_catalog_sql_reader",
    ("fl", "write_database"): f"{_CONNECTORS}._handle_database_writer",
    ("fl", "write_to_cloud_storage"): f"{_FF}._handle_cloud_storage_writer",
    ("datetime", "date"): f"{_CG}.expression_helpers:_temporal_literal",
    ("datetime", "datetime"): f"{_CG}.expression_helpers:_temporal_literal",
    ("FlowFrame", "drop"): f"{_BASE}._handle_select",
    ("FlowFrame", "rename"): f"{_CG}.join_handlers:JoinHandlersMixin._apply_pre_join_transformations",
    ("FlowFrame", "head"): f"{_FF}._handle_sample",
    ("FlowFrame", "write_excel"): f"{_BASE}._handle_output_excel",
    ("Expr", "median"): f"{_CG}.expression_helpers:ExpressionHelpersMixin._get_agg_function",
    ("StringNS", "join"): f"{_CG}.expression_helpers:ExpressionHelpersMixin._get_agg_function",
    **{("Expr", name): _FILTER for name in ("is_in", "is_null", "is_not_null", "not_", "str")},
    **{("StringNS", name): _FILTER for name in ("contains", "starts_with", "ends_with")},
    **{("Expr", name): _WINDOWS for name in (*_WINDOW, *_CUMULATIVE, "rank", "fill_null", "then", "otherwise")},
    **{("Expr", name): _FORMULA for name in (*_FORMULA_EXPR, "dt")},
    **{("StringNS", name): _FORMULA for name in _FORMULA_STR},
    **{("DateTimeNS", name): _FORMULA for name in _FORMULA_DT},
    ("NodeOutputs", "[]"): f"{_NATIVE}._bind_outputs",
    ("CustomNodes", "[]"): f"{_NATIVE}._handle_user_defined",
    ("CustomNodeFactory", "node"): f"{_NATIVE}._handle_user_defined",
    ("ScriptFunction", "node"): f"{_NATIVE}._decorated_text",
    ("reader", "__call__"): f"{_FF}._frame_reader",
    ("helper", "_flowfile_expr_literal"): f"{_BASE}._gate_formula_arg",
    ("import", "datetime"): f"{_CG}.expression_helpers:ExpressionHelpersMixin._create_basic_filter_expr",
    ("import", "hashlib"): f"{_CG}.base:ConverterMixinBase._register_expr_stdlib_imports",
    ("import", "json"): f"{_BASE}._mark_expr_literal_needed",
    **{("from flowfile_frame", name): f"{_FF}._frame_reader" for name in _READERS},
}
"""Allowed entries no rendered corpus cell uses, each with the exporter handler (``module:qualname``) emitting it."""

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
    "ast_nodes_per_cell": 500_000,
    "statements_per_cell": 2_000,
    "depth": 100,
    "string_length": 1_000_000,
    "literal_elements_per_request": 2_000_000,
    "steps_per_request": 5_000_000,
    "nodes_per_request": 10_000,
    "expression_chars_per_request": 16_000_000,
}
"""Size limits: the first four per request (the runner), the rest per cell or per run (the interpreter)."""
