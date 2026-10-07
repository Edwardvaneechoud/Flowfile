"""Read edited notebook cells back into changes to the flow, without ever executing them.

The inverse of :mod:`notebook_render`, for the cells the user changed. The flow is rendered again
here, so what counts as changed is decided against what the canvas says now:

* a cell whose text is the rendered text keeps its nodes and only binds its names;
* inside a changed cell, a call that reads exactly as the render writes it for its node keeps
  that node untouched;
* any other call is read by the handler for its method, which answers with the settings that
  call describes, in flowfile_core's dialect, to be laid over the node's own. A call the cell
  did not have before is a new node;
* a node the cell stood for that none of its calls reads any more is removed, as core's push
  deletes it, unless a step the push keeps still reads it.

Inputs follow names: a node reads whatever its input's name is bound to where its cell stands, so
a changed cell that binds a name to another node moves the readers of that name with it.

A changed call no handler reads fails on its line. Nothing a cell contains reaches ``exec``,
``eval`` or ``compile``: a cell is parsed and walked, and its arguments are read by
:class:`notebook_interpret.CellReader` against the allowlist.
"""

from __future__ import annotations

import ast
import datetime
import keyword
import posixpath
import re
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import polars as pl

from . import notebook_allowlist as allowlist
from .notebook_interpret import _BUILTIN_NAMES, _STORE_NAME, CellFailure, CellReader, needs_kernel, parse_cell
from .notebook_render import (
    AUTO_DATA_TYPE,
    NODE_TEMPLATE_NAMES,
    NODE_TYPE_VAR_LABEL,
    STRING_CONCAT_DELIMITER,
    NotebookRenderer,
    _formula_entries,
    _Node,
    node_label,
)
from .notebook_shim import Dtype, Expr, Module, kind_of, register_kind

_REFERENCE = re.compile(r"[a-z][a-z0-9_]*")
_GENERATED = re.compile(rf"(?:df|{'|'.join(sorted(set(NODE_TYPE_VAR_LABEL.values())))})_\d+(?:_\w+)?")
_KERNEL_WORDING = "is not part of the notebook's flow code; this needs a kernel"
_BROWSER_WORDING = "is not flow code, and the browser runs no Python cells"
_COMPARISONS = {
    "Eq": "equals",
    "NotEq": "not_equals",
    "Gt": "greater_than",
    "GtE": "greater_than_or_equals",
    "Lt": "less_than",
    "LtE": "less_than_or_equals",
}
_TEXT_MATCHES = {"str.contains": "contains", "str.starts_with": "starts_with", "str.ends_with": "ends_with"}
_NEGATED = {"contains": "not_contains", "in": "not_in"}
# The types a Select node casts to, by the name a cell gives them.
_SELECT_TYPES = {
    "Utf8": "String",
    "String": "String",
    "Int64": "Int64",
    "Int32": "Int32",
    "Float64": "Float64",
    "Float32": "Float32",
    "Boolean": "Boolean",
    "Date": "Date",
    "Datetime": "Datetime",
}
_SINKS = {"csv": "sink_csv", "parquet": "sink_parquet"}
# The aggregations a Group by node runs, by the expression method a cell writes for them.
_AGGREGATIONS = frozenset({"sum", "max", "min", "count", "mean", "median", "first", "last", "n_unique", "std", "var"})
# A new node of these types stays although no name holds it: it writes, which is what it is for.
_KEPT_UNNAMED = frozenset({"output"})
# The column types of a frame written as data: the ones a Manual Input gives back as they were written.
_FRAME_TYPES = frozenset(
    {"String", "Boolean", "Null", "Float32", "Float64", "Int8", "Int16", "Int32", "Int64", "UInt8", "UInt16", "UInt32", "UInt64"}
)  # fmt: skip
_FRAME_VALUE_KINDS = frozenset({"none", "bool", "int", "float", "str", "date", "datetime_value"})
_SAFE_INTEGER = 2**53 - 1


class Frame:
    """A node of the flow, as the name a cell gives it."""

    def __init__(self, node_id: int) -> None:
        self.node_id = node_id


register_kind(Frame, "FlowFrame")


def _refused(message: str, line: int | None) -> CellFailure:
    return CellFailure(message, line, "refused")


@dataclass
class _Unit:
    """How the render spells one node: what a changed cell is compared with."""

    node: _Node
    target: str | None
    base: str | None
    calls: list[tuple[str, str]]
    helpers: list[str]
    helper_names: list[str]
    inputs: dict[str, tuple[str, int]]
    readable: bool = True
    added: bool = False
    columns: list[str] | None = None
    """The columns of the frame the call being read is made on, where they are known."""

    @property
    def label(self) -> str:
        return _step_label(self.node)


def _step_label(node: _Node) -> str:
    return f"#{node.id} {NODE_TEMPLATE_NAMES.get(node.type) or node.type}"


def _unroll(value: ast.expr) -> tuple[ast.expr, list[ast.Call]]:
    """A method chain as what it starts on and its calls, first to last."""
    calls: list[ast.Call] = []
    while isinstance(value, ast.Call) and isinstance(value.func, ast.Attribute):
        calls.append(value)
        value = value.func.value
    calls.reverse()
    return value, calls


def _signature(call: ast.Call) -> tuple[str, str]:
    """A call by its method and arguments, whatever it is called on and however it is laid out."""
    arguments = ast.Call(func=ast.Name(id="_", ctx=ast.Load()), args=call.args, keywords=call.keywords)
    return call.func.attr, ast.dump(arguments)


def _bound_names(statement: ast.stmt) -> list[str]:
    if isinstance(statement, ast.FunctionDef):
        return [statement.name]
    targets = getattr(statement, "targets", None) or []
    return [target.id for target in targets if isinstance(target, ast.Name)]


def _units(renderer: NotebookRenderer) -> dict[int, _Unit]:
    units: dict[int, _Unit] = {}
    for node_id, code in renderer.node_statements().items():
        node = renderer.nodes[node_id]
        inputs = renderer.node_inputs(node)
        unreadable = _Unit(node, None, None, [], [], [], inputs, readable=False)
        try:
            body = ast.parse(code).body
        except SyntaxError:
            body = []
        last = body[-1] if body else None
        if isinstance(last, ast.Assign) and len(last.targets) == 1 and isinstance(last.targets[0], ast.Name):
            target, value = last.targets[0].id, last.value
        elif isinstance(last, ast.Expr):
            target, value = None, last.value
        else:
            units[node_id] = unreadable
            continue
        base, calls = _unroll(value)
        if not isinstance(base, ast.Name):
            units[node_id] = unreadable
            continue
        helpers = body[:-1]
        units[node_id] = _Unit(
            node,
            target,
            None if base.id == "ff" else base.id,
            [_signature(call) for call in calls],
            [ast.dump(statement) for statement in helpers],
            [name for statement in helpers for name in _bound_names(statement)],
            inputs,
        )
    return units


def _arguments(what: str, params: tuple[str, ...], args: list, kwargs: dict, line: int) -> dict[str, Any]:
    """The arguments of a call by parameter name; one the node has no setting for is refused."""
    if len(args) > len(params):
        raise CellFailure(f"TypeError: {what}() takes at most {len(params)} positional arguments here", line)
    bound = dict(zip(params, args, strict=False))
    for name, value in kwargs.items():
        if name not in params:
            raise _refused(f"`{name}=` has no setting on this node, so {what}() cannot take it here", line)
        if name in bound:
            raise CellFailure(f"TypeError: {what}() got multiple values for argument {name!r}", line)
        bound[name] = value
    return bound


def _names(value: Any, what: str, line: int) -> list[str]:
    values = [value] if isinstance(value, str) else value
    if not isinstance(values, list | tuple) or not all(type(item) is str for item in values):
        raise _refused(f"{what} takes column names here", line)
    return list(values)


def _text(value: Any, what: str, line: int) -> str:
    if type(value) is not str:
        raise CellFailure(f"TypeError: {what} takes text", line)
    return value


def _whole(value: Any, what: str, line: int) -> int:
    if type(value) is not int or value < 0:
        raise CellFailure(f"TypeError: {what} takes a whole number, zero or more", line)
    return value


def _read_sort(unit: _Unit, args: list, kwargs: dict, line: int) -> dict:
    bound = _arguments("sort", ("by", "descending"), args, kwargs, line)
    if "by" not in bound:
        raise CellFailure("TypeError: sort() needs the columns to sort by", line)
    columns = _names(bound["by"], "sort()", line)
    descending = bound.get("descending", False)
    flags = [descending] * len(columns) if type(descending) is bool else descending
    if not isinstance(flags, list) or len(flags) != len(columns) or not all(type(flag) is bool for flag in flags):
        raise CellFailure("ValueError: `descending` takes True or False, once or once per sort column", line)
    return {
        "sort_input": [
            {"column": column, "how": "desc" if flag else "asc"} for column, flag in zip(columns, flags, strict=True)
        ]
    }


def _read_head(unit: _Unit, args: list, kwargs: dict, line: int) -> dict:
    bound = _arguments("head", ("n",), args, kwargs, line)
    return {"sample_size": _whole(bound.get("n", 5), "head()", line)}


def _read_unique(unit: _Unit, args: list, kwargs: dict, line: int) -> dict:
    bound = _arguments("unique", ("subset", "keep"), args, kwargs, line)
    subset = bound.get("subset")
    columns = None if subset is None else _names(subset, "unique(subset=)", line)
    keep = bound.get("keep", "any")
    if keep not in ("first", "last", "any", "none"):
        raise CellFailure("ValueError: `keep` is one of 'first', 'last', 'any' or 'none'", line)
    return {"unique_input": {"columns": columns or None, "strategy": keep}}


def _read_row_index(unit: _Unit, args: list, kwargs: dict, line: int) -> dict:
    if "group_by" in kwargs:
        raise _refused("Numbering rows per group is not available in the browser", line)
    bound = _arguments("with_row_index", ("name", "offset"), args, kwargs, line)
    name = _text(bound.get("name", "index"), "with_row_index(name=)", line)
    offset = _whole(bound.get("offset", 0), "with_row_index(offset=)", line)
    return {"record_id_input": {"output_column_name": name, "offset": offset}}


def _read_unpivot(unit: _Unit, args: list, kwargs: dict, line: int) -> dict:
    bound = _arguments("unpivot", ("on", "index", "variable_name", "value_name"), args, kwargs, line)
    if bound.get("variable_name", "variable") != "variable" or bound.get("value_name", "value") != "value":
        raise _refused("The Unpivot node always names its columns 'variable' and 'value'", line)
    index = [] if bound.get("index") is None else _names(bound["index"], "unpivot(index=)", line)
    on = [] if bound.get("on") is None else _names(bound["on"], "unpivot(on=)", line)
    unpivot: dict[str, Any] = {"index_columns": index, "value_columns": on}
    if on:
        unpivot["data_type_selector_mode"] = "column"
    return {"unpivot_input": unpivot}


def _read_dynamic_rename(unit: _Unit, args: list, kwargs: dict, line: int) -> dict:
    params = ("mode", "prefix", "suffix", "formula", "columns", "data_type")
    bound = _arguments("dynamic_rename", params, args, kwargs, line)
    mode = bound.get("mode", "prefix")
    if mode not in ("prefix", "suffix", "formula", "first_row"):
        raise CellFailure("ValueError: `mode` is one of 'prefix', 'suffix', 'formula' or 'first_row'", line)
    columns, data_type = bound.get("columns"), bound.get("data_type")
    if columns is not None and data_type is not None:
        raise CellFailure("ValueError: dynamic_rename() takes `columns` or `data_type`, not both", line)
    selection = "list" if columns is not None else "data_type" if data_type is not None else "all"
    return {
        "dynamic_rename_input": {
            "rename_mode": mode,
            "prefix": _text(bound.get("prefix", ""), "dynamic_rename(prefix=)", line),
            "suffix": _text(bound.get("suffix", ""), "dynamic_rename(suffix=)", line),
            "formula": _text(bound.get("formula", ""), "dynamic_rename(formula=)", line),
            "selection_mode": selection,
            "selected_columns": [] if columns is None else _names(columns, "dynamic_rename(columns=)", line),
            "selected_data_type": None if data_type is None else _text(data_type, "dynamic_rename(data_type=)", line),
        }
    }


def _column(value: Any) -> str | None:
    if isinstance(value, Expr) and value.op == "col" and not value.kwargs:
        return value.args[0]
    return None


def _value_text(value: Any) -> str | None:
    """A literal as the text a basic filter stores, or None for a value it cannot hold."""
    if isinstance(value, Expr) and value.op == "lit":
        value = value.args[0]
    if type(value) is bool:
        return "true" if value else "false"
    if type(value) in (int, float, str):
        return str(value)
    if type(value) is datetime.datetime:
        return value.isoformat(sep=" ")
    if type(value) is datetime.date:
        return value.isoformat()
    return None


def _comparison(expr: Any) -> tuple[str, str, str] | None:
    """``ff.col(name) <op> literal`` as ``(operator, name, text)``, else None."""
    if not isinstance(expr, Expr) or not expr.op.startswith("compare:") or len(expr.args) != 2:
        return None
    field, text = _column(expr.args[0]), _value_text(expr.args[1])
    if field is None or text is None:
        return None
    return expr.op.removeprefix("compare:"), field, text


def _basic_filter(expr: Any) -> dict | None:
    """The basic filter one condition spells: the shapes the render writes for one, and no others."""
    if not isinstance(expr, Expr):
        return None
    compared = _comparison(expr)
    if compared is not None:
        operator, field, text = compared
        return {"field": field, "operator": _COMPARISONS[operator], "value": text}
    if expr.op == "binary:BitAnd":
        low, high = _comparison(expr.args[0]), _comparison(expr.args[1])
        if low and high and (low[0], high[0]) == ("GtE", "LtE") and low[1] == high[1]:
            return {"field": low[1], "operator": "between", "value": low[2], "value2": high[2]}
        return None
    if expr.kwargs:
        return None
    field = _column(expr.args[0]) if expr.args else None
    if expr.op in _TEXT_MATCHES and len(expr.args) == 2 and field is not None and type(expr.args[1]) is str:
        return {"field": field, "operator": _TEXT_MATCHES[expr.op], "value": expr.args[1]}
    if expr.op in ("is_null", "is_not_null") and len(expr.args) == 1 and field is not None:
        return {"field": field, "operator": expr.op, "value": ""}
    if expr.op == "is_in" and len(expr.args) == 2 and field is not None and isinstance(expr.args[1], list):
        members = [_value_text(member) for member in expr.args[1]]
        if not members or any(member is None or "," in member or member != member.strip() for member in members):
            return None
        return {"field": field, "operator": "in", "value": ", ".join(members)}
    if expr.op == "not_" and len(expr.args) == 1:
        inner = _basic_filter(expr.args[0])
        if inner is not None and inner["operator"] in _NEGATED:
            return {**inner, "operator": _NEGATED[inner["operator"]]}
    return None


def _read_filter(unit: _Unit, args: list, kwargs: dict, line: int) -> dict:
    kwargs = dict(kwargs)
    formula = kwargs.pop("flowfile_formula", None)
    bound = _arguments("filter", ("predicate",), args, kwargs, line)
    if (formula is None) == ("predicate" not in bound):
        raise CellFailure("TypeError: filter() takes one condition, or `flowfile_formula=`", line)
    if formula is not None:
        return {"filter_input": {"mode": "advanced", "advanced_filter": _text(formula, "flowfile_formula=", line)}}
    basic = _basic_filter(bound["predicate"])
    if basic is None:
        raise _refused(
            "Only a single comparison on one column can be changed here for now. "
            "Write the condition as `flowfile_formula='...'`, or change the filter on the canvas",
            line,
        )
    return {"filter_input": {"mode": "basic", "basic_filter": basic}}


def _formula_call(args: list, kwargs: dict, line: int) -> list[dict]:
    """The entries one ``with_columns(flowfile_formulas=...)`` call adds."""
    if args:
        raise _refused(
            "A formula written as an expression cannot be changed here yet. "
            "Write it as `flowfile_formulas=[...], output_column_names=[...]`, or change it on the canvas",
            line,
        )
    bound = _arguments(
        "with_columns", ("flowfile_formulas", "output_column_names", "output_column_datatypes"), [], kwargs, line
    )
    formulas, names = bound.get("flowfile_formulas"), bound.get("output_column_names")
    types = bound.get("output_column_datatypes")
    if not isinstance(formulas, list) or not isinstance(names, list) or len(formulas) != len(names) or not formulas:
        raise CellFailure("ValueError: with_columns() takes one output column name per formula", line)
    if types is None:
        types = [None] * len(formulas)
    if not isinstance(types, list) or len(types) != len(formulas):
        raise CellFailure("ValueError: with_columns() takes one output data type per formula", line)
    entries = []
    for formula, name, data_type in zip(formulas, names, types, strict=True):
        if type(formula) is not str or type(name) is not str or not (data_type is None or type(data_type) is str):
            raise CellFailure("TypeError: formulas, column names and data types are text", line)
        entries.append({"field": {"name": name, "data_type": data_type or AUTO_DATA_TYPE}, "function": formula})
    return entries


def _rendered_path(output: dict) -> str:
    path = output.get("abs_file_path")
    if not path:
        directory, name = output.get("directory") or "", output.get("name") or ""
        path = f"{directory.rstrip('/')}/{name}" if directory else name
    return str(path)


def _read_write(file_type: str, params: tuple[str, ...]) -> Callable:
    def read(unit: _Unit, args: list, kwargs: dict, line: int) -> dict:
        output = unit.node.settings.get("output_settings") or {}
        if not unit.added and output.get("file_type") != file_type:
            raise _refused(f"This node writes {output.get('file_type')}; change its file type on the canvas", line)
        bound = _arguments(f"write_{file_type}", params, args, kwargs, line)
        if "path" not in bound:
            raise CellFailure(f"TypeError: write_{file_type}() needs the path to write to", line)
        path = _text(bound["path"], f"write_{file_type}()", line)
        patch: dict[str, Any] = {}
        table: dict[str, Any] = {}
        if file_type == "csv":
            table["delimiter"] = _text(bound.get("separator", ","), "write_csv(separator=)", line)
            table["encoding"] = _text(bound.get("encoding", "utf-8"), "write_csv(encoding=)", line)
        elif file_type == "parquet":
            if "compression" in bound or "compression" in (output.get("table_settings") or {}):
                table["compression"] = bound.get("compression", "zstd")
        else:
            table["sheet_name"] = _text(bound.get("worksheet", "Sheet1"), "write_excel(worksheet=)", line)
            patch["write_mode"] = _text(bound.get("write_mode", "overwrite"), "write_excel(write_mode=)", line)
        if unit.added:
            patch["file_type"] = table["file_type"] = file_type
            patch.setdefault("write_mode", "overwrite")
            if file_type in _SINKS:
                patch["polars_method"] = _SINKS[file_type]
        if table:
            patch["table_settings"] = table
        if unit.added or path != _rendered_path(output):
            patch["directory"], patch["name"] = posixpath.dirname(path), posixpath.basename(path)
            if output.get("abs_file_path"):
                patch["abs_file_path"] = path
        return {"output_settings": patch} if patch else {}

    return read


def _read_raw_data(unit: _Unit, args: list, kwargs: dict, line: int) -> dict:
    bound = _arguments("from_raw_data", ("data",), args, kwargs, line)
    raw = bound.get("data")
    columns, data = (raw.get("columns"), raw.get("data")) if isinstance(raw, dict) else (None, None)
    shaped = isinstance(columns, list) and isinstance(data, list) and all(isinstance(column, list) for column in data)
    if not shaped or not all(isinstance(column, dict) and type(column.get("name")) is str for column in columns):
        raise CellFailure(
            "TypeError: from_raw_data() takes {'columns': [{'name': ..., 'data_type': ...}], 'data': [[...], ...]}",
            line,
        )
    if data and len(data) != len(columns):
        raise CellFailure("ValueError: from_raw_data() takes one list of values per column", line)
    return {
        "raw_data_format": {
            "columns": [
                {"name": column["name"], "data_type": str(column.get("data_type") or "String")} for column in columns
            ],
            "data": [list(values) for values in data],
        }
    }


def _literal_data(value: Any, what: str, line: int) -> None:
    """Data as a cell writes it: numbers, text, True, False, None, and lists, tuples and dicts of them."""
    if isinstance(value, list | tuple):
        for item in value:
            _literal_data(item, what, line)
    elif isinstance(value, dict):
        for item in value.values():
            _literal_data(item, what, line)
    elif kind_of(value) not in _FRAME_VALUE_KINDS:
        raise _refused(
            f"ff.{what}() takes the data written out here: numbers, text, True, False, None, "
            "and lists and dicts of them",
            line,
        )


def _polars_schema(value: Any, what: str, line: int) -> Any:
    """A schema as Polars takes it: each ``ff`` data type as the Polars one of that name."""
    if isinstance(value, Dtype):
        if value.args or value.kwargs or value.name not in (*_FRAME_TYPES, "Utf8"):
            raise _refused(_no_such_column(value.name), line)
        return getattr(pl, value.name)
    if isinstance(value, dict):
        return {key: _polars_schema(item, what, line) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return type(value)(_polars_schema(item, what, line) for item in value)
    if value is None or type(value) is str:
        return value
    raise CellFailure(f"TypeError: ff.{what}() takes column names and `ff` data types as its schema", line)


def _no_such_column(type_name: str, column: str | None = None) -> str:
    named = f" (`{column}`)" if column else ""
    return (
        f"A {type_name} column{named} cannot be written as data in the browser notebook; "
        "write it as text or a number and convert it in a later step"
    )


def _read_frame(what: str) -> Callable:
    """``ff.DataFrame(...)`` / ``ff.LazyFrame(...)``: the frame Polars builds from the data, as a Manual Input."""
    params = ("data", "schema", "schema_overrides", "strict", "orient", "infer_schema_length", "nan_to_null")

    def read(unit: _Unit, args: list, kwargs: dict, line: int) -> dict:
        if len(args) > 2:
            raise CellFailure(f"TypeError: ff.{what}() takes its data and schema by position, the rest by name", line)
        bound = _arguments(what, params, args, kwargs, line)
        data = bound.pop("data", None)
        if isinstance(data, str):
            raise needs_kernel(f"`ff.{what}` with a string as data", line)
        _literal_data(data, what, line)
        options: dict[str, Any] = {}
        for key, value in bound.items():
            if key in ("schema", "schema_overrides"):
                options[key] = _polars_schema(value, what, line)
            else:
                _literal_data(value, what, line)
                options[key] = value
        try:
            frame = pl.DataFrame(data, **options)
        except Exception as exc:
            raise CellFailure(f"ValueError: Could not convert data to a polars DataFrame: {exc}", line) from exc
        columns = []
        for name, dtype in frame.schema.items():
            type_name = str(dtype).split("(")[0]
            if type_name not in _FRAME_TYPES:
                raise _refused(_no_such_column(type_name, name), line)
            columns.append({"name": name, "data_type": type_name})
        values = [frame.get_column(name).to_list() for name in frame.columns]
        if any(type(value) is int and abs(value) > _SAFE_INTEGER for column in values for value in column):
            raise _refused("A whole number this large cannot be written as data in the browser notebook", line)
        return {"raw_data_format": {"columns": columns, "data": values}}

    return read


def _selected(item: Any, line: int) -> tuple[str, str, str | None]:
    """One item of a select: the column, the name it gets and the type it is cast to."""
    if type(item) is str:
        return item, item, None
    name = cast = None
    expr = item
    while isinstance(expr, Expr) and expr.op in ("alias", "cast") and len(expr.args) == 2 and not expr.kwargs:
        argument = expr.args[1]
        if expr.op == "alias" and name is None and type(argument) is str:
            name = argument
        elif expr.op == "cast" and cast is None and isinstance(argument, Dtype) and not argument.args:
            cast = argument.name
        else:
            break
        expr = expr.args[0]
    column = _column(expr)
    if column is None:
        raise _refused(
            "select() takes columns here: a name or `ff.col(name)`, with `.alias(...)` and `.cast(...)`. "
            "A computed column is made with with_columns()",
            line,
        )
    if cast is not None and cast not in _SELECT_TYPES:
        raise _refused(f"The Select node casts to {', '.join(sorted(set(_SELECT_TYPES.values())))} only", line)
    return column, name or column, _SELECT_TYPES.get(cast or "")


def _counts_records(args: list, kwargs: dict) -> bool:
    """``select(ff.len().alias('number_of_records'))``, which is how a Count records node reads."""
    if kwargs or len(args) != 1 or not isinstance(args[0], Expr) or args[0].op != "alias":
        return False
    counted, name = args[0].args
    return isinstance(counted, Expr) and counted.op == "len" and name == "number_of_records"


def _read_nothing(unit: _Unit, args: list, kwargs: dict, line: int) -> dict:
    return {}


def _not_known(what: str, line: int) -> CellFailure:
    return _refused(
        f"The columns of this frame are not known yet, so {what} cannot say which ones it leaves out. "
        "Push or run the steps before it first",
        line,
    )


def _frame_columns(unit: _Unit, what: str, line: int) -> list[str]:
    """The columns of the frame a Select is written on: the input's, else the ones the node already names."""
    if unit.columns is not None:
        return list(unit.columns)
    rows = sorted(unit.node.settings.get("select_input") or [], key=lambda row: row.get("position") or 0)
    if not rows:
        raise _not_known(what, line)
    return [row.get("old_name") for row in rows]


def _missing(names: list[str], columns: list[str], line: int) -> None:
    absent = next((name for name in names if name not in columns), None)
    if absent is not None:
        raise CellFailure(f"ColumnNotFoundError: `{absent}` is not one of {', '.join(columns)}", line)


def _read_select(unit: _Unit, args: list, kwargs: dict, line: int) -> dict:
    """The columns listed are kept in that order; a column the node knew and the list leaves out is dropped."""
    if kwargs:
        raise _refused("select() takes its columns by position here", line)
    items = args[0] if len(args) == 1 and isinstance(args[0], list | tuple) else args
    if not items:
        raise CellFailure("TypeError: select() needs at least one column", line)
    if unit.columns is None and not unit.node.settings.get("select_input"):
        raise _not_known("a select", line)
    return _select_rows(unit, [_selected(item, line) for item in items], line)


def _read_drop(unit: _Unit, args: list, kwargs: dict, line: int) -> dict:
    """``drop(...)`` is a Select node that keeps every other column, as the full app lowers it."""
    bound = _arguments("drop", ("strict",), [], kwargs, line)
    strict = bound.get("strict", True)
    if type(strict) is not bool:
        raise CellFailure("TypeError: drop(strict=) takes True or False", line)
    names = [name for item in args for name in _names(item, "drop()", line)]
    columns = _frame_columns(unit, "a drop", line)
    if strict:
        _missing(names, columns, line)
    kept = [(column, column, None) for column in columns if column not in names]
    if not kept:
        raise _refused("A drop that leaves no column is not a step the Select node can make", line)
    return _select_rows(unit, kept, line)


def _read_rename(unit: _Unit, args: list, kwargs: dict, line: int) -> dict:
    """``rename({old: new})`` is a Select node that keeps every column and renames the ones listed."""
    bound = _arguments("rename", ("mapping", "strict"), args, kwargs, line)
    mapping, strict = bound.get("mapping"), bound.get("strict", True)
    if not isinstance(mapping, dict) or not all(type(k) is str and type(v) is str for k, v in mapping.items()):
        raise _refused("rename() takes a dict of column names here: {old: new}", line)
    if type(strict) is not bool:
        raise CellFailure("TypeError: rename(strict=) takes True or False", line)
    columns = _frame_columns(unit, "a rename", line)
    if strict:
        _missing(list(mapping), columns, line)
    picks = [(column, mapping.get(column, column), None) for column in columns]
    _unique_names([name for _, name, _ in picks], line)
    return _select_rows(unit, picks, line)


def _unique_names(names: list[str], line: int) -> None:
    repeated = next((name for name in names if names.count(name) > 1), None)
    if repeated is not None:
        raise CellFailure(f"DuplicateError: the name `{repeated}` is given to more than one column", line)


def _select_rows(unit: _Unit, picks: list[tuple[str, str, str | None]], line: int) -> dict:
    """Select settings keeping ``picks`` (column, name, cast) in that order and naming every other column dropped."""
    known = {row.get("old_name"): row for row in unit.node.settings.get("select_input") or []}
    rows: list[dict] = []
    for column, name, cast in picks:
        if any(row["old_name"] == column for row in rows):
            raise _refused(f"The Select node takes each column once; `{column}` is listed twice", line)
        if unit.columns is not None and column not in unit.columns:
            raise CellFailure(f"ColumnNotFoundError: `{column}` is not one of {', '.join(unit.columns)}", line)
        rows.append(
            {
                **known.get(column, {}),
                "old_name": column,
                "new_name": name,
                "keep": True,
                "is_available": True,
                "position": len(rows),
                # No cast written is no cast: a stored type would be one.
                "data_type": cast,
                "data_type_change": cast is not None,
                "is_altered": cast is not None or name != column,
            }
        )
    listed = {row["old_name"] for row in rows}
    rows += [{**row, "keep": False} for column, row in known.items() if column not in listed]
    # The editor keeps a column a Select node does not name, so every other column is named as dropped.
    rows += [
        {"old_name": column, "new_name": column, "keep": False, "is_available": True, "data_type": None}
        for column in unit.columns or []
        if column not in listed and column not in known
    ]
    for position, row in enumerate(rows):
        row["position"] = position
    return {"select_input": rows, "keep_missing": False}


def _group_key(item: Any, line: int) -> tuple[str, str]:
    """One grouping column: a name, ``ff.col(name)``, or ``ff.col(name).alias(new_name)``."""
    if type(item) is str:
        return item, item
    expr, name = item, None
    if isinstance(expr, Expr) and expr.op == "alias" and len(expr.args) == 2 and not expr.kwargs:
        expr, name = expr.args
    column = _column(expr)
    if column is None or not (name is None or type(name) is str):
        raise _refused("group_by() takes column names here, or `ff.col(name).alias(new_name)`", line)
    return column, name or column


def _aggregation(item: Any, line: int) -> tuple[str, str, str]:
    """One aggregation: ``ff.col(name).<aggregation>()``, named with ``.alias(...)``; ``(column, agg, name)``."""
    expr, name = item, None
    if isinstance(expr, Expr) and expr.op == "alias" and len(expr.args) == 2 and not expr.kwargs:
        expr, name = expr.args
    agg = column = None
    if isinstance(expr, Expr) and not expr.kwargs:
        if expr.op in _AGGREGATIONS and len(expr.args) == 1:
            agg, column = expr.op, _column(expr.args[0])
        elif expr.op == "str.join" and len(expr.args) == 2:
            if expr.args[1] != STRING_CONCAT_DELIMITER:
                raise _refused(f"The Group by node joins text with {STRING_CONCAT_DELIMITER!r} only", line)
            agg, column = "concat", _column(expr.args[0])
    if column is None or not (name is None or type(name) is str):
        raise _refused(
            f"agg() takes `ff.col(name).<aggregation>()` here, one of {', '.join(sorted(_AGGREGATIONS))} "
            f"or `.str.join({STRING_CONCAT_DELIMITER!r})`, named with `.alias(...)`",
            line,
        )
    return column, agg, name or column


def _items(call: ast.Call, args: list) -> tuple[list, list[int]]:
    """A call's items, given as one list or one by one, with the line each is written on."""
    if len(args) == 1 and isinstance(args[0], list | tuple):
        elements = call.args[0].elts if isinstance(call.args[0], ast.List | ast.Tuple) else []
        return list(args[0]), [element.lineno for element in elements] or [call.args[0].lineno] * len(args[0])
    return list(args), [argument.lineno for argument in call.args]


def _read_group_by(
    keys_call: ast.Call,
    keys_read: tuple[list, dict],
    agg_call: ast.Call,
    aggs_read: tuple[list, dict],
    columns: list[str] | None,
) -> dict:
    """``group_by(keys).agg(aggregations)`` as a Group by node's ``agg_cols``: the keys, then the aggregations."""
    keys_line = getattr(keys_call.func, "end_lineno", None) or keys_call.lineno
    agg_line = getattr(agg_call.func, "end_lineno", None) or agg_call.lineno
    args, kwargs = keys_read
    if kwargs:
        name = next(iter(kwargs))
        raise _refused(f"`{name}=` has no setting on this node, so group_by() cannot take it here", keys_line)
    items, key_lines = _items(keys_call, args)
    grouped = [_group_key(item, line) for item, line in zip(items, key_lines, strict=True)]
    args, kwargs = aggs_read
    if kwargs:
        raise _refused("agg() takes its aggregations by position here; name each one with `.alias(...)`", agg_line)
    items, agg_lines = _items(agg_call, args)
    aggregations = [_aggregation(item, line) for item, line in zip(items, agg_lines, strict=True)]
    named: list[tuple[str, str, int]] = [
        (column, name, line) for (column, name), line in zip(grouped, key_lines, strict=True)
    ]
    named += [(column, name, line) for (column, _, name), line in zip(aggregations, agg_lines, strict=True)]
    seen: set[str] = set()
    for column, name, line in named:
        if columns is not None:
            _missing([column], columns, line)
        if name in seen:
            raise CellFailure(f"DuplicateError: the name `{name}` is given to more than one column", line)
        seen.add(name)
    return {
        "groupby_input": {
            "agg_cols": [{"old_name": column, "new_name": name, "agg": "groupby"} for column, name in grouped]
            + [{"old_name": column, "new_name": name, "agg": agg} for column, agg, name in aggregations]
        }
    }


def _columns_after(node_type: str, columns: list[str] | None, settings: dict) -> list[str] | None:
    """The columns a node hands on, for the node types where its settings and input say so; else None."""
    if node_type == "manual_input":
        raw = (settings.get("raw_data_format") or {}).get("columns") or []
        return [column.get("name") for column in raw]
    if node_type == "record_count":
        return ["number_of_records"]
    if node_type == "group_by":
        rows = (settings.get("groupby_input") or {}).get("agg_cols") or []
        keys = [row.get("new_name") or row["old_name"] for row in rows if row.get("agg") == "groupby"]
        return keys + [row.get("new_name") or row["old_name"] for row in rows if row.get("agg") != "groupby"]
    if node_type == "unpivot":
        return [*((settings.get("unpivot_input") or {}).get("index_columns") or []), "variable", "value"]
    if columns is None:
        return None
    if node_type in ("sort", "filter", "sample", "unique", "output"):
        return list(columns)
    if node_type == "select":
        rows = [
            row for row in settings.get("select_input") or [] if row.get("keep", True) and row["old_name"] in columns
        ]
        return [
            row.get("new_name") or row["old_name"] for row in sorted(rows, key=lambda row: row.get("position") or 0)
        ]
    if node_type == "record_id":
        return [(settings.get("record_id_input") or {}).get("output_column_name", "record_id"), *columns]
    if node_type == "formula":
        names = [(entry.get("field") or {}).get("name") for entry in _formula_entries(settings)]
        return [*columns, *(name for name in dict.fromkeys(names) if name not in columns)]
    return None


# (receiver kind, method) -> (the node types the call stands for, what reads its arguments into settings)
_HANDLERS: dict[tuple[str, str], tuple[tuple[str, ...], Callable | None]] = {
    ("FlowFrame", "sort"): (("sort",), _read_sort),
    ("FlowFrame", "head"): (("sample",), _read_head),
    ("FlowFrame", "unique"): (("unique",), _read_unique),
    ("FlowFrame", "with_row_index"): (("record_id",), _read_row_index),
    ("FlowFrame", "unpivot"): (("unpivot",), _read_unpivot),
    ("FlowFrame", "dynamic_rename"): (("dynamic_rename",), _read_dynamic_rename),
    ("FlowFrame", "filter"): (("filter",), _read_filter),
    ("FlowFrame", "select"): (("select",), _read_select),
    ("FlowFrame", "drop"): (("select",), _read_drop),
    ("FlowFrame", "rename"): (("select",), _read_rename),
    ("FlowFrame", "with_columns"): (("formula",), None),
    ("FlowFrame", "group_by"): (("group_by",), None),
    ("FlowFrame", "write_csv"): (("output",), _read_write("csv", ("path", "separator", "encoding"))),
    ("FlowFrame", "write_parquet"): (("output",), _read_write("parquet", ("path", "compression"))),
    ("FlowFrame", "write_excel"): (("output",), _read_write("excel", ("path", "worksheet", "write_mode"))),
    ("ff", "from_raw_data"): (("manual_input",), _read_raw_data),
    ("ff", "DataFrame"): (("manual_input",), _read_frame("DataFrame")),
    ("ff", "LazyFrame"): (("manual_input",), _read_frame("LazyFrame")),
    ("ff", "concat"): (("union",), None),
}


class _CellReading:
    """One changed cell being read: its statements against the nodes the cell stood for."""

    def __init__(self, sync: _Sync, cell: dict, draft: str) -> None:
        self.sync = sync
        self.cell = cell
        self.reader = CellReader(draft, sync.namespace, self._nested_frame)
        self.pending = [sync.units[node_id] for node_id in cell["node_ids"] if node_id in sync.units]
        self.nodes: list[int] = []
        self.lines: list[list[int]] = []
        # Each line that gives its frame no name, with the nodes it read.
        self.unnamed: list[tuple[str, list[int]]] = []
        self.helper_names: dict[str, _Unit] = {}
        self.helpers_seen: dict[int, set[str]] = {}
        self.bound: set[str] = set()

    def read(self, tree: ast.Module) -> list[_Unit]:
        """Read every statement; the nodes the cell stood for that no call of it reads any more."""
        if any(not unit.readable for unit in self.pending):
            raise _refused("This cell holds code the notebook cannot read back; change it on the canvas", 1)
        for statement in tree.body:
            self._statement(statement)
        return [unit for unit in self.pending if unit.calls]

    @staticmethod
    def _nested_frame(kind: Any, receiver: Any, attribute: str, args: list, kwargs: dict, node: ast.Call) -> Any:
        line = getattr(node.func, "end_lineno", None) or node.lineno
        raise _refused("Give this frame a line of its own, then use its name here", line)

    def _statement(self, statement: ast.stmt) -> None:
        dumped = ast.dump(statement)
        for unit in self.pending:
            if dumped in unit.helpers:
                self.helpers_seen.setdefault(unit.node.id, set()).add(dumped)
                self.helper_names.update(dict.fromkeys(_bound_names(statement), unit))
                return
        if isinstance(statement, ast.Import):
            return self.sync.bind_import(statement)
        if isinstance(statement, ast.FunctionDef):
            raise _refused(
                "Polars code cannot be changed from the notebook yet; change it on the canvas", statement.lineno
            )
        if isinstance(statement, ast.Assign):
            target = statement.targets[0]
            if len(statement.targets) != 1 or not isinstance(target, ast.Name):
                raise needs_kernel(f"The assignment {self.reader.text(statement)}", statement.lineno)
            name: str | None = target.id
            if not _STORE_NAME.fullmatch(target.id) or target.id in allowlist.RESERVED_NAMES:
                raise needs_kernel(f"The name `{target.id}`", statement.lineno)
        elif isinstance(statement, ast.Expr):
            name = None
        else:
            raise needs_kernel(self.reader.text(statement), statement.lineno)
        read = len(self.nodes)
        frame, unit = self._chain(statement.value)
        if self.nodes[read:]:
            self.lines.append(self.nodes[read:])
        if name is None:
            self.unnamed.append((self.reader.source(statement), self.nodes[read:]))
            return
        self.sync.namespace[name] = frame
        self.bound.add(name)
        if unit is not None:
            self.sync.name_node(unit, name, self.cell["cell_id"], statement.lineno)

    def _chain(self, value: ast.expr) -> tuple[Frame, _Unit | None]:
        """Read one line's method chain, node by node; the frame it ends on and that frame's node."""
        base, calls = _unroll(value)
        if not isinstance(base, ast.Name):
            raise needs_kernel(f"{self.reader.text(value)} (a line of a cell builds a frame)", value.lineno)
        signatures = [_signature(call) for call in calls]
        current: Frame | None = None
        helper_base = base.id if base.id in self.helper_names else None
        if helper_base is None:
            bound = self.reader.name(base)
            if isinstance(bound, Frame):
                current = bound
            elif kind_of(bound) != "ff":
                raise needs_kernel(f"Using {self.reader.text(base)} as a frame", base.lineno)
        if current is None and not calls:
            raise needs_kernel(f"Using {self.reader.text(base)} as a frame", base.lineno)
        last: _Unit | None = None
        index = 0
        if current is not None:
            current, last = self._passed_through(current, calls, signatures)
        while index < len(calls):
            call = calls[index]
            line = getattr(call.func, "end_lineno", None) or call.lineno
            source = current is None and helper_base is None
            unit = self._matching(signatures, index, source, helper_base if current is None else None)
            if unit is not None:
                desired, count = self._inputs_as_written(unit, current, line), len(unit.calls)
            elif current is None and helper_base is not None:
                raise _refused(f"Changing `{call.func.attr}` from the notebook is not supported yet", line)
            else:
                unit, desired, count = self._changed(calls, signatures, index, current, line)
            if unit in self.pending:
                self.pending.remove(unit)
            self.sync.wire(unit, desired)
            self.sync.follow_columns(unit, desired)
            self.nodes.append(unit.node.id)
            current, last = Frame(unit.node.id), unit
            index += count
        return current, last

    def _passed_through(self, current: Frame, calls: list, signatures: list) -> tuple[Frame, _Unit | None]:
        """A node the render writes as its input's name, with no call of its own, stays where it was."""
        unit = self.pending[0] if self.pending else None
        if unit is None or unit.calls or unit.base is None:
            return current, None
        if calls and self._matching(signatures, 0, False, None) is None:
            types = _HANDLERS.get(("FlowFrame", calls[0].func.attr), ((), None))[0]
            if unit.node.type in types:
                return current, None
        self.pending.remove(unit)
        self.sync.wire(unit, {"main": current.node_id})
        self.sync.follow_columns(unit, {"main": current.node_id})
        self.nodes.append(unit.node.id)
        return Frame(unit.node.id), unit

    def _matching(self, signatures: list, index: int, source: bool, helper_base: str | None) -> _Unit | None:
        """The first node of this cell still unread whose rendered calls are the ones written here."""
        for unit in self.pending:
            count = len(unit.calls)
            if not count or (unit.base is None) != source:
                continue
            if helper_base is None and unit.base in unit.helper_names:
                continue
            if helper_base is not None and unit.base != helper_base:
                continue
            if set(unit.helpers) - self.helpers_seen.get(unit.node.id, set()):
                continue
            if signatures[index : index + count] == unit.calls:
                return unit
        return None

    def _inputs_as_written(self, unit: _Unit, current: Frame | None, line: int) -> dict[str, int]:
        """The inputs of a node whose calls are unchanged: the frames its input names are bound to now."""
        desired: dict[str, int] = {}
        for key, (name, _producer) in unit.inputs.items():
            if current is not None and name == unit.base:
                desired[key] = current.node_id
                continue
            frame = self.sync.namespace.get(name)
            if not isinstance(frame, Frame):
                raise CellFailure(f"NameError: name {name!r} is not defined", line)
            desired[key] = frame.node_id
        return desired

    def _changed(
        self, calls: list, signatures: list, index: int, current: Frame | None, line: int
    ) -> tuple[_Unit, dict[str, int], int]:
        """Read a call the render did not write: the node it changes or makes, its inputs and the calls it took."""
        call = calls[index]
        attribute = call.func.attr
        kind = "ff" if current is None else "FlowFrame"
        receiver = Module("ff") if current is None else current
        usage = self.reader.usage(receiver, attribute, call.func, input_only=True)
        if usage not in (allowlist.CALL, allowlist.BOTH):
            raise needs_kernel(f"Calling {self.reader.text(call.func)}", line)
        types, handler = _HANDLERS.get((kind, attribute), ((), None))
        if not types:
            raise _refused(
                f"`{attribute}` cannot be written or changed from the notebook yet; use the canvas for this step", line
            )
        run = _RUN_READERS.get((kind, attribute))
        if run is not None:
            return run(self, self._unread(types), calls, signatures, index, current, line)
        args, kwargs = self.reader.arguments(call)
        accepted = allowlist.DATA_ARGUMENTS.get((kind, attribute))
        unaccepted = next((name for name in kwargs if accepted is not None and name not in accepted), None)
        if unaccepted is not None:
            raise needs_kernel(f"The argument `{unaccepted}=`", line)
        description = _text(kwargs.pop("description", ""), "description=", line)
        if attribute == "select" and _counts_records(args, kwargs):
            types, handler = ("record_count",), _read_nothing
        # The first node of this type the cell has not read yet; with none left, the call is a new node.
        unit = self._unread(types) or self.sync.new_unit(types[0], line)
        unit.columns = None if current is None else self.sync.columns_of(current.node_id)
        if handler is None:
            desired, patch = self._members(args, kwargs, line), {}
        else:
            desired = {} if current is None else {"main": current.node_id}
            patch = handler(unit, args, kwargs, line)
        self.sync.change(unit, patch, description)
        return unit, desired, 1

    def _unread(self, types: tuple[str, ...]) -> _Unit | None:
        return next((candidate for candidate in self.pending if candidate.node.type in types), None)

    def _members(self, args: list, kwargs: dict, line: int) -> dict[str, int]:
        bound = _arguments("concat", ("items", "how"), args, kwargs, line)
        items = bound.get("items")
        if not isinstance(items, list) or not items or not all(isinstance(item, Frame) for item in items):
            raise CellFailure("TypeError: ff.concat() takes a list of frames", line)
        if bound.get("how") != "diagonal_relaxed":
            raise _refused("The Union node combines frames with how='diagonal_relaxed'", line)
        ids = [item.node_id for item in items]
        if len(set(ids)) != len(ids):
            raise _refused("A frame can be in a union only once", line)
        return (
            {"main": ids[0]} if len(ids) == 1 else {f"main_{position}": node_id for position, node_id in enumerate(ids)}
        )

    def _formula(
        self, unit: _Unit | None, calls: list, signatures: list, index: int, current: Frame | None, line: int
    ) -> tuple[_Unit, dict[str, int], int]:
        """A Formula node is one ``with_columns`` per entry: read as many as the node had, each on its own.

        With no Formula node left to read, the run of ``with_columns`` calls is one new node.
        """
        assert current is not None
        run = 0
        while index + run < len(calls) and calls[index + run].func.attr == "with_columns":
            run += 1
        if unit is None:
            unit, count = self.sync.new_unit("formula", line), run
        else:
            count = min(run, max(1, len(unit.calls)))
        kept = [entry for entry in _formula_entries(unit.node.settings) if (entry.get("function") or "").strip()]
        aligned = len(unit.calls) == len(kept)
        entries: list[dict] = []
        description = ""
        for position in range(count):
            call = calls[index + position]
            line = getattr(call.func, "end_lineno", None) or call.lineno
            unchanged = aligned and position < len(kept) and signatures[index + position] == unit.calls[position]
            if unchanged:
                entries.append(kept[position])
                # The render puts the description on the node's last call: left as it was, it stays.
                if position == count - 1 == len(kept) - 1 and not description:
                    description = unit.node.description
                continue
            args, kwargs = self.reader.arguments(call)
            description = _text(kwargs.pop("description", description), "description=", line)
            entries.extend(_formula_call(args, kwargs, line))
        self.sync.change(unit, {"functions": entries}, description)
        return unit, {"main": current.node_id}, count

    def _group_by(
        self, unit: _Unit | None, calls: list, signatures: list, index: int, current: Frame | None, line: int
    ) -> tuple[_Unit, dict[str, int], int]:
        """A Group by node is ``group_by(keys)`` and the ``agg(...)`` called on it: two calls, one node."""
        assert current is not None
        follow = calls[index + 1] if index + 1 < len(calls) else None
        if follow is None or follow.func.attr != "agg":
            raise _refused(
                "A group_by() is a step together with its .agg([...]); write the aggregations after it", line
            )
        keys_read = self.reader.arguments(calls[index])
        description = _text(keys_read[1].pop("description", ""), "description=", line)
        columns = self.sync.columns_of(current.node_id)
        patch = _read_group_by(calls[index], keys_read, follow, self.reader.arguments(follow), columns)
        unit = unit or self.sync.new_unit("group_by", line)
        unit.columns = columns
        self.sync.change(unit, patch, description)
        return unit, {"main": current.node_id}, 2


# Calls whose node reads a run of calls, from the one at ``index`` on: the reader answers how many it took.
_RUN_READERS: dict[tuple[str, str], Callable] = {
    ("FlowFrame", "with_columns"): _CellReading._formula,
    ("FlowFrame", "group_by"): _CellReading._group_by,
}


class _Sync:
    """One reading of the notebook against the flow as it stands."""

    def __init__(
        self,
        flow: dict,
        schemas: dict | None,
        locked: dict | None,
        drafts: dict[str, str],
        next_id: int | None,
        layout: list | None = None,
        order: list[str] | None = None,
        new_cells: dict[str, str] | None = None,
    ) -> None:
        self.renderer = NotebookRenderer(flow, schemas, locked, layout)
        self.rendering = self.renderer.render()
        self.units = _units(self.renderer)
        self.drafts = drafts
        self.order = order or []
        self.new_cells = new_cells or {}
        self.node_ids_by_cell: dict[str, list[list[int]]] = {}
        self.unnamed: dict[str, list[tuple[str, list[int]]]] = {}
        self.unnamed_by_cell: dict[str, list[str]] = {}
        self.rendered_names = {name for cell in self.rendering["cells"] for name in cell["defines"]}
        self.next_id = max([int(next_id or 0), *(node_id + 1 for node_id in self.renderer.nodes), 1])
        self.first_new_id = self.next_id
        self.added: list[_Unit] = []
        # The id each new node is answered under: the ones that stay are numbered without gaps.
        self.final_ids: dict[int, int] = {}
        # The columns of the nodes this reading changes: what they hand on now, or None when it cannot say.
        self.columns: dict[int, list[str] | None] = {}
        # The render writes the imports a flow needs, so a cell may use the dialect's modules before it does.
        self.namespace: dict[str, Any] = {name: Module(name) for name in allowlist.IMPORTS.values()}
        self.settings: dict[int, dict] = {}
        self.descriptions: dict[int, str] = {}
        self.references: dict[int, tuple[str | None, str, int]] = {}
        self.inputs: dict[int, dict[str, int]] = {}
        # The nodes a changed cell no longer writes, by the cell that held them: a push removes them.
        self.removed: dict[int, str] = {}
        self.warnings: list[str] = []

    def run(self) -> dict:
        rendered = {cell["cell_id"]: cell for cell in self.rendering["cells"]}
        stray = next((cell_id for cell_id in self.drafts if cell_id not in rendered), None)
        if stray is not None:
            return _failure(stray, CellFailure("This cell no longer stands for a step of the flow", None))
        if len(self.drafts) + len(self.new_cells) > allowlist.BOUNDS["cells_per_request"]:
            return _failure(next(iter(self.drafts)), _refused("The notebook has too many changed cells to read", None))
        # Cells are read in the order the notebook shows them; a new cell is one the flow has no node for yet.
        fresh = {
            cell_id: {"cell_id": cell_id, "node_ids": [], "kind": "node", "status": "code", "code": "", "defines": []}
            for cell_id in self.new_cells
        }
        shown = [cell_id for cell_id in self.order if cell_id in rendered or cell_id in fresh]
        sequence = [*shown, *(cell_id for cell_id in [*rendered, *fresh] if cell_id not in shown)]
        for cell in ({**rendered, **fresh}[cell_id] for cell_id in sequence):
            draft = self.new_cells.get(cell["cell_id"], self.drafts.get(cell["cell_id"]))
            try:
                if cell["kind"] == "imports":
                    self._imports(cell["code"] if draft is None else draft)
                elif draft is None or draft == cell["code"]:
                    self._keep(cell)
                else:
                    self._read(cell, draft)
            except CellFailure as failure:
                return _failure(cell["cell_id"], failure)
        still_read = self._still_read()
        if still_read is not None:
            return still_read
        for table in (self.settings, self.descriptions, self.references, self.inputs, self.columns):
            for node_id in self.removed:
                table.pop(node_id, None)
        self._prune()
        taken = self._taken_reference()
        if taken is not None:
            return taken
        new = {unit.node.id for unit in self.added}
        added = [
            {
                "id": self._final(unit.node.id),
                "type": unit.node.type,
                "settings": self.settings.get(unit.node.id, {}),
                "description": self.descriptions.get(unit.node.id, ""),
                "node_reference": self.references.get(unit.node.id, (None,))[0],
            }
            for unit in self.added
        ]
        nodes: dict[str, dict] = {}
        for node_id in sorted({*self.settings, *self.descriptions, *self.references} - new):
            change: dict[str, Any] = {}
            if self.settings.get(node_id):
                change["settings"] = self.settings[node_id]
            if node_id in self.descriptions:
                change["description"] = self.descriptions[node_id]
            if node_id in self.references:
                change["node_reference"] = self.references[node_id][0]
            if change:
                nodes[str(node_id)] = change
        return {
            "ok": True,
            "nodes": nodes,
            "added": added,
            "inputs": {
                str(self._final(node_id)): _ports({key: self._final(source) for key, source in desired.items()})
                for node_id, desired in sorted(self.inputs.items())
            },
            "node_ids_by_cell": {
                cell_id: [[self._final(node_id) for node_id in line] for line in lines]
                for cell_id, lines in self.node_ids_by_cell.items()
            },
            "unnamed_by_cell": self.unnamed_by_cell,
            "removed": [
                {"id": node_id, "label": _step_label(self.renderer.nodes[node_id])} for node_id in sorted(self.removed)
            ],
            "warnings": self.warnings,
        }

    def _still_read(self) -> dict | None:
        """A step a push removes must not be one a step it keeps still reads; that fails on the removing cell."""
        readers: dict[int, list[tuple[_Node, str | None]]] = {}
        for node_id, node in self.renderer.nodes.items():
            if node_id in self.removed:
                continue
            rendered = self.units[node_id].inputs if node_id in self.units else self.renderer.node_inputs(node)
            desired = self.inputs.get(node_id, {key: producer for key, (_, producer) in rendered.items()})
            for key, source in desired.items():
                if source in self.removed:
                    name = rendered[key][0] if rendered.get(key, (None, None))[1] == source else None
                    readers.setdefault(source, []).append((node, name))
        if not readers:
            return None
        source, read_by = min(readers.items())
        read_by.sort(key=lambda each: each[0].id)
        labels = [_step_label(node) for node, _ in read_by]
        one = len(labels) == 1
        listed = labels[0] if one else f"{', '.join(labels[:-1])} and {labels[-1]}"
        them = labels[0] if one else "those steps"
        name = next((name for _, name in read_by if name), None)
        message = f"This change removes {_step_label(self.renderer.nodes[source])}, which {listed} still "
        message += "reads" if one else "read"
        if name:
            message += f" as `{name}`; give that name to another frame, or change {them} first"
        else:
            message += f"; change {them} first"
        return _failure(self.removed[source], _refused(message, None))

    def _prune(self) -> None:
        """A new node stays only upstream of a named frame, a writer or a node the flow has: core's rule for a push.

        What a line builds without giving it a name is looked at, not added. The names are the ones
        bound once every cell is read, so a name given to another frame later no longer keeps the first.
        """
        new = {unit.node.id: unit for unit in self.added}
        named = {frame.node_id for frame in self.namespace.values() if isinstance(frame, Frame)}
        stack = [
            *(node_id for node_id in self.renderer.nodes if node_id not in self.removed),
            *(node_id for node_id, unit in new.items() if node_id in named or unit.node.type in _KEPT_UNNAMED),
        ]
        kept: set[int] = set()
        while stack:
            node_id = stack.pop()
            if node_id in kept:
                continue
            kept.add(node_id)
            unit = self.units.get(node_id)
            rendered = [producer for _, producer in unit.inputs.values()] if unit else []
            stack.extend(self.inputs[node_id].values() if node_id in self.inputs else rendered)
        dropped = set(new) - kept
        self.added = [unit for unit in self.added if unit.node.id not in dropped]
        self.final_ids = {unit.node.id: self.first_new_id + position for position, unit in enumerate(self.added)}
        for table in (self.settings, self.descriptions, self.references, self.inputs, self.columns):
            for node_id in dropped:
                table.pop(node_id, None)
        for cell_id, lines in self.node_ids_by_cell.items():
            remaining = [[node_id for node_id in line if node_id not in dropped] for line in lines]
            self.node_ids_by_cell[cell_id] = [line for line in remaining if line]
        for cell_id, statements in self.unnamed.items():
            sources = [source for source, made in statements if dropped.intersection(made)]
            if sources:
                self.unnamed_by_cell[cell_id] = sources

    def _final(self, node_id: int) -> int:
        return self.final_ids.get(node_id, node_id)

    def new_unit(self, node_type: str, line: int) -> _Unit:
        """A node the cells call for and the flow does not have yet, under the next free id."""
        if len(self.renderer.nodes) + len(self.added) >= allowlist.BOUNDS["nodes_per_request"]:
            raise _refused("The flow would have too many steps", line)
        node = _Node({"id": self.next_id, "type": node_type, "setting_input": {}})
        self.next_id += 1
        unit = _Unit(node, None, None, [], [], [], {}, added=True)
        self.added.append(unit)
        return unit

    def bind_import(self, statement: ast.Import) -> None:
        for alias in statement.names:
            bound = allowlist.IMPORTS.get((alias.name, alias.asname))
            if bound is None:
                spelled = f"import {alias.name}" + (f" as {alias.asname}" if alias.asname else "")
                raise needs_kernel(f"`{spelled}`", statement.lineno)
            self.namespace[bound] = Module(bound)

    def _imports(self, code: str) -> None:
        for statement in parse_cell(code).body:
            if not isinstance(statement, ast.Import):
                raise needs_kernel("Anything but an import in the imports cell", statement.lineno)
            self.bind_import(statement)

    def _keep(self, cell: dict) -> None:
        """An unchanged cell keeps its nodes; each reads whatever its input's name is bound to now."""
        inside = set(cell["node_ids"])
        for node_id in cell["node_ids"]:
            unit = self.units.get(node_id)
            if unit is None:
                continue
            desired = {key: producer for key, (_, producer) in unit.inputs.items()}
            for key, (name, producer) in unit.inputs.items():
                frame = self.namespace.get(name)
                if producer not in inside and isinstance(frame, Frame):
                    desired[key] = frame.node_id
            self.wire(unit, desired)
            self.follow_columns(unit, desired)
        self._bind_rendered(cell, set())

    def _read(self, cell: dict, draft: str) -> None:
        if cell["status"] == "placeholder":
            raise _refused("This step stays on the canvas; it cannot be changed from the notebook", 1)
        if len(draft.encode("utf-8")) > allowlist.BOUNDS["bytes_per_cell"]:
            raise _refused("The cell is too large to read", 1)
        reading = _CellReading(self, cell, draft)
        for unit in reading.read(parse_cell(draft)):
            self.removed[unit.node.id] = cell["cell_id"]
        # A name the cell now gives to another node no longer names the node that had it.
        for node_id in cell["node_ids"]:
            if node_id in self.removed:
                continue
            node = self.renderer.nodes[node_id]
            holder = self.namespace.get(node.reference) if node.reference in reading.bound else None
            if isinstance(holder, Frame) and holder.node_id != node_id:
                self.references[node_id] = (None, cell["cell_id"], 1)
        self.node_ids_by_cell[cell["cell_id"]] = reading.lines
        self.unnamed[cell["cell_id"]] = reading.unnamed
        self._bind_rendered(cell, reading.bound)

    def _bind_rendered(self, cell: dict, bound: set[str]) -> None:
        """Names the render gave this cell's nodes keep naming them, unless the cell bound them itself."""
        for node_id in cell["node_ids"]:
            unit = self.units.get(node_id)
            if unit is None or unit.target is None or unit.target not in self.rendered_names or unit.target in bound:
                continue
            if node_id in self.removed:
                continue
            if bound:
                self.namespace.setdefault(unit.target, Frame(node_id))
            else:
                self.namespace[unit.target] = Frame(node_id)

    def columns_of(self, node_id: int) -> list[str] | None:
        """The columns a node hands on: what this reading worked out, else what the editor last knew."""
        if node_id in self.columns:
            return self.columns[node_id]
        schema = self.renderer.schemas.get(node_id)
        return [column.get("name") for column in schema] if schema else None

    def follow_columns(self, unit: _Unit, desired: dict[str, int]) -> None:
        """A node that is new, changed, rewired or fed by one that is hands on other columns than before."""
        node_id = unit.node.id
        upstream = any(source in self.columns for source in desired.values())
        if not (unit.added or upstream or node_id in self.settings or node_id in self.inputs):
            return
        settings = {**unit.node.settings, **self.settings.get(node_id, {})}
        main = desired.get("main")
        columns = None if main is None else self.columns_of(main)
        self.columns[node_id] = _columns_after(unit.node.type, columns, settings)

    def wire(self, unit: _Unit, desired: dict[str, int]) -> None:
        if desired != {key: producer for key, (_, producer) in unit.inputs.items()}:
            self.inputs[unit.node.id] = desired

    def change(self, unit: _Unit, patch: dict, description: str) -> None:
        if _differs(unit.node.settings, patch):
            self.settings[unit.node.id] = patch
        if description != unit.node.description:
            self.descriptions[unit.node.id] = description

    def name_node(self, unit: _Unit, name: str, cell_id: str, line: int) -> None:
        """A name the user chose becomes the node's reference; the generated one stands for none."""
        node = unit.node
        if name in (node_label(node.type, node.id), f"df_{node.id}"):
            reference: str | None = None
        elif (
            _REFERENCE.fullmatch(name)
            and not _GENERATED.fullmatch(name)
            and not keyword.iskeyword(name)
            and name not in _BUILTIN_NAMES
            and name not in ("main", *allowlist.RESERVED_NAMES)
        ):
            reference = name
        else:
            if not _GENERATED.fullmatch(name):
                self.warnings.append(
                    f"`{name}` cannot be kept as the name of {unit.label}: a name is lowercase letters, digits "
                    "and underscores, and not one the notebook generates"
                )
            return
        if reference != (node.reference or None):
            self.references[node.id] = (reference, cell_id, line)

    def _taken_reference(self) -> dict | None:
        for node_id, (reference, cell_id, line) in self.references.items():
            if reference is None:
                continue
            for other in [*self.renderer.nodes.values(), *(unit.node for unit in self.added)]:
                if other.id in self.removed:
                    continue
                named = self.references[other.id][0] if other.id in self.references else other.reference
                if other.id != node_id and named == reference:
                    step = self._final(other.id)
                    message = f"The name `{reference}` already names another step (#{step}); pick another name"
                    return _failure(cell_id, CellFailure(message, line))
        return None


def _differs(settings: Any, patch: Any) -> bool:
    """Whether laying ``patch`` over ``settings`` would change anything."""
    if isinstance(patch, dict) and isinstance(settings, dict):
        return any(key not in settings or _differs(settings[key], value) for key, value in patch.items())
    return settings != patch


def _ports(desired: dict[str, int]) -> dict[str, Any]:
    if "main" in desired:
        mains = [desired["main"]]
    else:
        keys = sorted((key for key in desired if key.startswith("main_")), key=lambda key: int(key[5:]))
        mains = [desired[key] for key in keys]
    return {"main": mains, "right": desired.get("right"), "left": desired.get("left")}


def _failure(cell_id: str, failure: CellFailure) -> dict:
    return {
        "ok": False,
        "cell_id": cell_id,
        "line": failure.line,
        "kind": failure.kind,
        "message": failure.message.replace(_KERNEL_WORDING, _BROWSER_WORDING),
    }


def notebook_surface() -> dict:
    """What a cell may write, for the editor's completions: the allowlist, and the calls a push reads back.

    ``ff`` lists every ``ff.<name>``, ``methods`` the attributes each kind of value offers, and ``pushable``
    the ``"<kind>.<method>"`` calls a changed cell can turn into settings; any other call stays as the render
    wrote it or is refused at a push. ``node_types`` (core's names) are the nodes a push can change.
    """
    readers = {*_HANDLERS, *_RUN_READERS}
    return {
        "ff": sorted({*allowlist.FL_VERDICTS, *allowlist.INPUT_ONLY.get("ff", {})}),
        "methods": {kind: sorted(attributes) for kind, attributes in allowlist.ALLOWLIST.items()},
        "pushable": sorted(f"{kind}.{method}" for kind, method in readers if _HANDLERS.get((kind, method), ((),))[0]),
        # A Count records node is read by the select handler.
        "node_types": sorted({"record_count", *(node_type for types, _ in _HANDLERS.values() for node_type in types)}),
    }


def sync_notebook(
    flow: dict,
    schemas: dict | None = None,
    locked: dict | None = None,
    drafts: dict[str, str] | None = None,
    next_id: int | None = None,
    layout: list | None = None,
    order: list[str] | None = None,
    new_cells: dict[str, str] | None = None,
) -> dict:
    """What the changed cells in ``drafts`` (cell id -> text) change in ``flow``, flowfile_core's dialect.

    Answers ``{"ok": True, "nodes", "added", "inputs", "warnings"}``: per node id the settings to lay
    over its own, its description and its reference; the new nodes (``id`` from ``next_id`` on,
    ``type``, ``settings`` to lay over the type's defaults); and per node id the inputs it reads now
    (``{"main": [ids], "right": id, "left": id}``). A cell that cannot be read answers
    ``{"ok": False, "cell_id", "line", "kind", "message"}`` and changes nothing.

    A new node is added only when a name holds its frame, or a frame downstream of it, once every
    cell is read; a writer and a node the flow has stay as they are. ``unnamed_by_cell`` answers,
    per cell, the text of each line whose nodes were left out for that reason.

    ``removed`` lists the nodes a changed cell no longer writes (``id``, ``label``), for the editor
    to remove once the user agrees; a node another step still reads is refused instead.

    ``layout`` is the render's (the cells a user wrote). ``new_cells`` (cell id -> text) are cells the
    flow has no node for yet, and ``order`` the cell ids as the notebook shows them: cells are read
    in that order. ``node_ids_by_cell`` answers which nodes each cell that was read now holds, line by line.
    """
    return _Sync(flow, schemas, locked, dict(drafts or {}), next_id, layout, order, new_cells).run()
