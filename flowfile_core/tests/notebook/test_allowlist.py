"""The allowlist is complete and exact: every ``ff`` name has a verdict, every entry exists on its receiver,
no entry runs code or reaches data, every entry outside the corpus names a real exporter handler, and
everything the exporter can emit is allowed or kept as formula text. The input-only table is exactly the
frame's own pure transforms and node builders the render does not write, none of them a hazard, plus
``ff.LazyFrame`` / ``ff.DataFrame``, which take Polars' constructor arguments only."""

from __future__ import annotations

import ast
import importlib
import inspect
import json
import re
import typing
from pathlib import Path

import polars as pl
from polars_expr_transformer import to_flowframe_code
from polars_expr_transformer.code_gen import FUNCTION_CODE_GEN

import flowfile_frame as ff
from flowfile_core.flowfile.code_generator.code_generator import (
    FlowGraphToFlowFrameConverter,
    _interprets_without_a_kernel,
    _polars_code_to_flowframe,
)
from flowfile_core.notebook import allowlist
from flowfile_core.notebook.interpret import _Datetime, kind_of
from flowfile_core.schemas import transform_schema
from flowfile_frame import _fl_namespace
from flowfile_frame.custom_node import CustomNodeFactory
from flowfile_frame.expr import DateTimeMethods, StringMethods
from flowfile_frame.flow_frame import FlowFrame
from flowfile_frame.gate import Gate
from flowfile_frame.group_frame import _NATIVE_AGG_FUNCS, GroupByFrame
from flowfile_frame.lazy_methods import PASSTHROUGH_METHODS, PURE_TRANSFORMS
from flowfile_frame.notebook_cells import _CanvasNode
from flowfile_frame.python_script import PythonScript, PythonScriptFunction
from flowfile_frame.run_flow import RunFlow

FRONTEND = Path(__file__).parents[3] / "flowfile_frontend/src/renderer/app/components"
COMPLETIONS = FRONTEND / "notebook/flCompletions.json"
AGGREGATIONS = FRONTEND / "nodes/baseNode/aggregations.ts"
WEB_UI_NAMES = {"open_graph_in_editor", "start_web_ui"}


def test_every_fl_name_has_a_verdict():
    completions = {entry["name"] for entry in json.loads(COMPLETIONS.read_text(encoding="utf-8"))["ff"]}
    assert set(allowlist.FL_VERDICTS) == completions == set(_fl_namespace.__all__) | WEB_UI_NAMES
    allowed = {name for name, (verdict, _) in allowlist.FL_VERDICTS.items() if verdict == allowlist.ALLOW}
    assert allowed <= set(_fl_namespace.__all__)
    assert {verdict for verdict, _ in allowlist.FL_VERDICTS.values()} == {allowlist.ALLOW, allowlist.REFUSE}


def _receivers() -> dict[str, list]:
    column = ff.col("a")
    return {
        "pl": [pl],
        "datetime": [_Datetime("datetime")],
        "FlowFrame": [FlowFrame],
        "GroupByFrame": [GroupByFrame],
        "Expr": [column, ff.when(column > 1)],
        "StringNS": [column.str],
        "DateTimeNS": [column.dt],
        "Gate": [Gate],
        "NodeOutputs": [RunFlow, PythonScript, ff.CustomNode, _CanvasNode],
        "CustomNodes": [type(ff.custom_nodes)],
        "CustomNodeFactory": [CustomNodeFactory],
        "ScriptFunction": [PythonScriptFunction],
        "helper": [],
        "reader": [],
    }


def test_every_entry_exists_on_its_receiver():
    receivers = _receivers()
    assert set(receivers) == set(allowlist.ALLOWLIST)
    missing = [
        (kind, attr)
        for kind, entries in allowlist.ALLOWLIST.items()
        for attr in entries
        if attr != "*" and receivers[kind]
        if not any(hasattr(receiver, {"[]": "__getitem__"}.get(attr, attr)) for receiver in receivers[kind])
    ]
    assert not missing
    node_classes = receivers["NodeOutputs"]
    assert all(hasattr(cls, "output") and hasattr(cls, "__getitem__") for cls in node_classes)
    assert StringMethods is type(ff.col("a").str) and DateTimeMethods is type(ff.col("a").dt)


def test_no_entry_runs_code_reaches_data_or_leaves_the_graph():
    frame = set(allowlist.ALLOWLIST["FlowFrame"])
    assert not frame & set(PASSTHROUGH_METHODS)
    assert not frame & {"to_graph", "save_graph", "cache", "pipe", "map_batches", "serialize", "lazy", "concat"}
    assert not {name for name in frame if name.startswith(("sink_", "collect"))}
    expr = set(allowlist.ALLOWLIST["Expr"])
    assert not expr & {"map_elements", "map_batches", "pipe", "apply", "inspect", "meta", "name", "list", "struct"}
    assert set(allowlist.ALLOWLIST["pl"]) == {"DataFrame"}
    names = {name for entries in allowlist.ALLOWLIST.values() for name in entries}
    assert {name for name in names if name.startswith("_")} == {"__call__"}


FRAME_NODE_BUILDERS = frozenset(
    {
        "sink_csv", "sink_ipc", "sink_ndjson", "sql", "write_avro", "write_catalog_table", "write_csv_to_cloud_storage",
        "write_database", "write_delta", "write_ipc", "write_json_to_cloud_storage", "write_ndjson",
        "write_parquet_to_cloud_storage",
    }
)  # fmt: skip
"""FlowFrame's own methods that place one wired SQL Query, Output or writer node and read nothing at build."""
FRAME_OWN_TRANSFORMS = frozenset({"explode", "limit"})
"""The input-only pure transforms FlowFrame writes itself instead of taking them from ``pl.LazyFrame``."""


def _wrapped_polars_methods(names) -> set[str]:
    """The ``names`` FlowFrame takes from ``pl.LazyFrame`` through the ``lazy_methods`` wrapper."""
    return {
        name
        for name in names
        if hasattr(pl.LazyFrame, name)
        and getattr(getattr(FlowFrame, name, None), "__wrapped__", None) is getattr(pl.LazyFrame, name)
    }


def test_input_only_entries_are_the_frames_pure_transforms_and_node_builders():
    frame = allowlist.INPUT_ONLY["FlowFrame"]
    assert set(allowlist.INPUT_ONLY) == {"FlowFrame", "ff"} and set(frame.values()) == {allowlist.CALL}
    assert set(frame) == (PURE_TRANSFORMS - set(allowlist.ALLOWLIST["FlowFrame"])) | FRAME_NODE_BUILDERS
    wrapped = _wrapped_polars_methods(frame)
    assert set(frame) - wrapped == FRAME_NODE_BUILDERS | FRAME_OWN_TRANSFORMS
    assert FRAME_NODE_BUILDERS | FRAME_OWN_TRANSFORMS <= set(vars(FlowFrame))
    for name in wrapped:
        assert inspect.signature(getattr(pl.LazyFrame, name)).return_annotation in ("LazyFrame", pl.LazyFrame), name


def test_the_input_only_fl_names_are_the_frame_aliases_taking_polars_constructor_arguments():
    names = allowlist.INPUT_ONLY["ff"]
    assert names == {"LazyFrame": allowlist.CALL, "DataFrame": allowlist.CALL}
    assert all(getattr(_fl_namespace, name) is FlowFrame for name in names)
    assert all(allowlist.FL_VERDICTS[name][0] == allowlist.REFUSE for name in names)
    assert set(allowlist.DATA_ARGUMENTS) == {("ff", name) for name in names}
    polars = set(inspect.signature(pl.DataFrame).parameters)
    assert polars == set(inspect.signature(pl.LazyFrame).parameters)
    frame = set(inspect.signature(FlowFrame.__new__).parameters)
    for accepted in allowlist.DATA_ARGUMENTS.values():
        assert accepted == polars & frame
        assert accepted.isdisjoint({"flow_graph", "node_id", "parent_node_id", "output_handle", "deferred"})


def test_no_input_only_entry_runs_code_reaches_data_or_leaves_the_graph():
    frame = set(allowlist.INPUT_ONLY["FlowFrame"])
    assert not frame & set(allowlist.ALLOWLIST["FlowFrame"])
    assert not frame & set(PASSTHROUGH_METHODS)
    hazards = {
        "show", "collect_batches", "execute", "remote", "serialize", "deserialize", "sink_parquet", "sink_delta",
        "sink_iceberg", "sink_batches", "inspect", "save_graph", "to_graph", "cache", "set_group", "group",
        "get_node_settings", "pipe", "pipe_with_schema", "map_batches", "lazy", "clone", "set_sorted", "concat",
        "join_asof", "join_where", "merge_sorted", "update", "with_context", "group_by_dynamic", "rolling",
    }  # fmt: skip
    assert not frame & hazards
    assert not {name for name in frame if name.startswith(("collect", "_"))}
    assert not _wrapped_polars_methods(name for name in frame if name.startswith("sink_"))


def _resolve(handler: str):
    module_name, _, qualname = handler.partition(":")
    target = importlib.import_module(module_name)
    for part in qualname.split("."):
        target = getattr(target, part)
    return target


def _allowed_entries() -> set[tuple[str, str]]:
    entries = {("ff", name) for name, (verdict, _) in allowlist.FL_VERDICTS.items() if verdict == allowlist.ALLOW}
    entries |= {(kind, attr) for kind, table in allowlist.ALLOWLIST.items() for attr in table}
    entries |= {("import", module) for module, _ in allowlist.IMPORTS}
    entries |= {(f"from {module}", name) for module, names in allowlist.FROM_IMPORTS.items() for name in names}
    return entries | {("helper", name) for name in allowlist.HELPERS}


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
        ("ff", name): f"{_CG}.native_handlers:_dtype_expr"
        for name in allowlist._DTYPES
        if name not in ("Boolean", "Float64", "Int32", "Int64", "Utf8")
    },
    ("ff", "when"): _WINDOWS,
    ("ff", "read_csv"): f"{_BASE}._handle_csv_read_non_utf8",
    ("ff", "list_files"): f"{_CONNECTORS}._handle_list_files",
    ("ff", "read_database"): f"{_CONNECTORS}._handle_database_reader",
    ("ff", "read_kafka"): f"{_FF}._handle_kafka_source",
    ("ff", "read_api"): f"{_CONNECTORS}._handle_rest_api_reader",
    ("ff", "read_from_cloud_storage"): f"{_FF}._handle_cloud_storage_reader",
    ("ff", "read_catalog_sql"): f"{_CONNECTORS}._handle_catalog_sql_reader",
    ("ff", "write_database"): f"{_CONNECTORS}._handle_database_writer",
    ("ff", "write_to_cloud_storage"): f"{_FF}._handle_cloud_storage_writer",
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
    **{
        ("Expr", name): _WINDOWS
        for name in (*allowlist._WINDOW, *allowlist._CUMULATIVE, "rank", "fill_null", "then", "otherwise")
    },
    **{("Expr", name): _FORMULA for name in (*allowlist._FORMULA_EXPR, "dt")},
    **{("StringNS", name): _FORMULA for name in allowlist._FORMULA_STR},
    **{("DateTimeNS", name): _FORMULA for name in allowlist._FORMULA_DT},
    ("NodeOutputs", "[]"): f"{_NATIVE}._bind_outputs",
    ("CustomNodes", "[]"): f"{_NATIVE}._handle_user_defined",
    ("CustomNodeFactory", "node"): f"{_NATIVE}._handle_user_defined",
    ("ScriptFunction", "node"): f"{_NATIVE}._decorated_text",
    ("reader", "__call__"): f"{_FF}._frame_reader",
    ("helper", "_flowfile_expr_literal"): f"{_BASE}._gate_formula_arg",
    ("import", "datetime"): f"{_CG}.expression_helpers:ExpressionHelpersMixin._create_basic_filter_expr",
    ("import", "hashlib"): f"{_CG}.base:ConverterMixinBase._register_expr_stdlib_imports",
    ("import", "json"): f"{_BASE}._mark_expr_literal_needed",
    **{("from flowfile_frame", name): f"{_FF}._frame_reader" for name in allowlist._READERS},
}
"""Allowed entries no rendered corpus cell uses, each with the exporter handler (``module:qualname``) emitting it."""


def test_every_entry_outside_the_corpus_names_a_real_exporter_handler():
    assert set(EMITTED_OUTSIDE_THE_CORPUS) <= _allowed_entries()
    assert all(callable(_resolve(handler)) for handler in EMITTED_OUTSIDE_THE_CORPUS.values())


def test_argument_rules_name_allowed_entries_and_known_kinds():
    calls = {(kind, attr) for kind, attr in _allowed_entries()}
    for rules in (allowlist.ARGUMENT_KINDS, allowlist.REFUSED_KEYWORDS, allowlist.KEYWORD_REQUIRES):
        assert set(rules) <= calls
    assert set(allowlist.LITERAL_ARGUMENTS) <= calls
    assert {kind for slots in allowlist.ARGUMENT_KINDS.values() for kind in slots.values()} == {
        "graph",
        "pl_frame",
        "polars_code_def",
    }


def test_kinds_are_exact_types():
    class Subframe(FlowFrame):
        pass

    frame = ff.from_dict({"a": [1]})
    assert kind_of(frame) == "FlowFrame" and kind_of(object.__new__(Subframe)) is None
    assert kind_of(pl.Int64) == "dtype" and kind_of(pl.Datetime("us")) == "dtype"
    assert kind_of(pl) == "pl" and kind_of(ff) is None and kind_of(print) is None
    assert kind_of(True) == "bool" and kind_of(1) == "int"


KEPT_AS_FORMULA_TEXT = frozenset(
    {
        ("Expr", "map_elements"),
        ("datetime.datetime", "now"),
        ("datetime.datetime", "today"),
        ("Expr", "when"),
        ("Expr", "dtype"),
        ("Expr", "sample"),
        ("DateTimeNS", "offset_by"),
        *(("StringNS", name) for name in ("count_matches", "find", "pad_end", "pad_start", "replace_many")),
        *(("StringNS", name) for name in ("slice", "split")),
        *(("ff", name) for name in ("coalesce", "concat_list", "concat_str", "duration", "int_range")),
        *(("ff", name) for name in ("max_horizontal", "min_horizontal")),
    }
)
"""What the formula translator emits outside the allowlist: hashing's ``lambda``, clock reads, ``elseif``
chains and the rest; the notebook render keeps a formula using any of them as its formula text."""
_MODULES = {"ff", "pl", "datetime", "hashlib"}


def _emitted(code: str) -> set[tuple[str, str]]:
    """The ``(kind, attribute)`` of every attribute an ``ff`` snippet reads or calls outside a ``lambda``."""

    def receiver(node: ast.expr) -> str:
        if isinstance(node, ast.Name):
            return node.id
        if isinstance(node, ast.Attribute):
            inner = receiver(node.value)
            if inner in _MODULES:
                return f"{inner}.{node.attr}"
            if node.attr in ("str", "dt"):
                return {"str": "StringNS", "dt": "DateTimeNS"}[node.attr]
        return "Expr"

    found = set()
    stack: list[ast.AST] = [ast.parse(code, mode="eval")]
    while stack:
        node = stack.pop()
        if isinstance(node, ast.Lambda):
            continue
        if isinstance(node, ast.Attribute):
            found.add((receiver(node.value), node.attr))
        stack.extend(ast.iter_child_nodes(node))
    return found


def _translations() -> dict[str, str]:
    """Every function the formula translator maps, applied to a column and literals, plus its conditional
    and membership forms, as the render writes them."""
    arguments = ['ff.col("a")', "ff.lit(1)", "ff.lit(2)"]
    snippets = {name: generate(arguments, prefix="ff") for name, generate in FUNCTION_CODE_GEN.items()}
    for formula in ("if [a] > 1 then 1 elseif [a] > 0 then 2 else 3 endif", "[a] in ([b], 1)", "[a] in (1, 2)"):
        snippets[formula] = _polars_code_to_flowframe(to_flowframe_code(formula), modules=("ff",))
    return snippets


def _handler_expressions() -> list[str]:
    """What the exporter writes for every aggregation, window function and basic filter operator a canvas holds."""
    converter = FlowGraphToFlowFrameConverter.__new__(FlowGraphToFlowFrameConverter)
    converter.imports = set()
    aggregations = set(re.findall(r'value: "(\w+)"', AGGREGATIONS.read_text())) | set(_NATIVE_AGG_FUNCS)
    expressions = [f'ff.col("a").{converter._get_agg_function(agg)}' for agg in sorted(aggregations)]
    for function in typing.get_args(transform_schema.WindowFunctionName):
        window = transform_schema.WindowFunctionInput(
            column="a", function=function, new_column_name="b", window_size=2, number_of_groups=2
        )
        for edge in ("require_full", "fill_zero"):
            window = window.model_copy(update={"edge_behavior": edge})
            order = [transform_schema.SortByInput(column="a")]
            expressions.append(converter._build_window_expr_code(window, ["g"], order))
    values = {
        "Int64": ("1", "2"),
        "String": ("x", "y"),
        "Boolean": ("true", "false"),
        "Date": ("2024-01-02", "2024-02-03"),
    }
    for operator in transform_schema.FilterOperator:
        for dtype, (value, value2) in values.items():
            basic = transform_schema.BasicFilter(field="a", operator=operator, value=value, value2=value2)
            expressions.append(converter._create_basic_filter_expr(basic, dtype))
    return expressions


def test_everything_the_exporter_emits_is_allowed_or_kept_as_formula_text():
    allowed = _allowed_entries()
    handled = set().union(*(_emitted(code) for code in _handler_expressions()))
    assert handled <= allowed, sorted(handled - allowed)
    translations = _translations()
    emitted = {name: _emitted(code) for name, code in translations.items()}
    outside = set().union(*emitted.values()) - allowed
    assert outside == KEPT_AS_FORMULA_TEXT, sorted(outside ^ KEPT_AS_FORMULA_TEXT)
    kept = [name for name, entries in emitted.items() if entries & KEPT_AS_FORMULA_TEXT]
    assert kept and not [name for name in kept if _interprets_without_a_kernel(translations[name])]
