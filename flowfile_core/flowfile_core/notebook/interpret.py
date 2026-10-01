"""Interpret notebook cells in core: read each cell as a description of the flow and never execute it.

:class:`CellInterpreter` is the cell executor core hands to ``notebook_cells.clean_run``. Nothing a cell
contains reaches ``exec``, ``eval`` or ``compile``: a cell is read with ``ast.parse`` and ``symtable``, and
only the frame calls :mod:`flowfile_core.notebook.allowlist` names for a value's *kind* (its exact type) run;
anything else fails on its line with "this needs a kernel". The statement subset, the ``def`` shapes and
the failure lines are documented with the notebook package in ``flowfile_core/CLAUDE.md``.
"""

from __future__ import annotations

import ast
import builtins
import datetime
import functools
import importlib.util
import operator
import re
import symtable
import sys
import traceback
import types
from collections.abc import Callable, Mapping
from typing import Any

import polars as pl
from polars.datatypes.classes import DataTypeClass

from flowfile_core.notebook import allowlist

_DATA_KINDS = frozenset(
    {
        "none", "bool", "int", "float", "str", "list", "tuple", "dict", "date", "datetime_value",
        "FlowFrame", "Expr", "dtype", "Parameter", "FlowRef", "FuzzyMapping",
    }
)  # fmt: skip
_LITERAL_KINDS = frozenset({"none", "bool", "int", "float", "str"})
_LITERAL_DATA_KINDS = frozenset({*_LITERAL_KINDS, "list", "tuple", "dict", "date", "datetime_value", "dtype"})
_OPERAND_KINDS = frozenset({"Expr", "int", "float", "str", "bool", "none", "Parameter", "date", "datetime_value"})
_MODULE_KINDS = frozenset({"fl", "pl", "datetime", "inert", "reader", "helper", "polars_code_def"})
_DISPLAYED_KINDS = frozenset({"FlowFrame", "Gate", "NodeOutputs"})
_BINARY = {
    "Add": operator.add, "Sub": operator.sub, "Mult": operator.mul, "Div": operator.truediv,
    "FloorDiv": operator.floordiv, "Mod": operator.mod, "BitAnd": operator.and_, "BitOr": operator.or_,
}  # fmt: skip
_COMPARE = {
    "Eq": operator.eq, "NotEq": operator.ne, "Lt": operator.lt, "LtE": operator.le, "Gt": operator.gt,
    "GtE": operator.ge,
}  # fmt: skip
_STORE_NAME = re.compile(rf"{allowlist.USER_NAME}|{'|'.join(allowlist.GENERATED_NAMES)}")
_SCRIPT_NAME = re.compile(rf"{allowlist.USER_NAME}|_script_\d+")
_PRELUDE_NAME = re.compile(rf"_?{allowlist.USER_NAME}")
_POLARS_CODE_NAME = re.compile(r"_polars_code_\d+")
_BUILTIN_NAMES = frozenset(dir(builtins))


def _flowfile_flow_parameter(frame, name, value, **declaration):
    """The parameters cell declares every parameter, so a gate only names it."""
    return name


def _flowfile_expr_literal(value):
    """A parameter inside a formula stays its ``${name}`` reference."""
    from flowfile_frame.parameters import Parameter

    return value.ref if isinstance(value, Parameter) else value


_HELPER_FUNCTIONS: dict[str, Callable[..., Any]] = {
    "_flowfile_flow_parameter": _flowfile_flow_parameter,
    "_flowfile_expr_literal": _flowfile_expr_literal,
}


class _Standin:
    """A value a cell can name but only use as the allowlist says (a stand-in module, reader, helper or def)."""

    def __init__(self, name: str, target: Any = None) -> None:
        self.name = name
        self.target = target


class _Inert(_Standin):
    """A module imported for a Python Script's prelude: usable by name only inside the script's body."""


class _Reader(_Standin):
    """A frame reader ``fl`` does not re-export (``from flowfile_frame import scan_ipc``)."""


class _Helper(_Standin):
    """A render helper whose ``def`` text matched the render's own; ``target`` is core's copy of it."""


class _PolarsCodeDef(_Standin):
    """A Polars Code ``def``: its node code text (``target``), accepted only as ``polars_code``'s code."""


class _Datetime(_Standin):
    """``import datetime``: ``datetime.date`` and ``datetime.datetime`` built from literal integers."""

    date = datetime.date
    datetime = datetime.datetime


@functools.cache
def _kinds() -> dict[type, str]:
    """Exact type -> kind, for every value a cell may hold (``type(value)``, never ``isinstance``)."""
    from pl_fuzzy_frame_match.models import FuzzyMapping

    from flowfile_core.flowfile.flow_graph import FlowGraph
    from flowfile_frame.custom_node import CustomNode, CustomNodeFactory
    from flowfile_frame.custom_nodes import CustomNodes
    from flowfile_frame.expr import Column, DateTimeMethods, Expr, StringMethods, When
    from flowfile_frame.flow_frame import FlowFrame
    from flowfile_frame.gate import Gate
    from flowfile_frame.group_frame import GroupByFrame
    from flowfile_frame.notebook_cells import _CanvasNode
    from flowfile_frame.parameters import Parameter
    from flowfile_frame.python_script import PythonScript, PythonScriptFunction
    from flowfile_frame.run_flow import FlowRef, RunFlow

    return {
        type(None): "none", bool: "bool", int: "int", float: "float", str: "str",
        list: "list", tuple: "tuple", dict: "dict", datetime.date: "date", datetime.datetime: "datetime_value",
        FlowFrame: "FlowFrame", GroupByFrame: "GroupByFrame", Expr: "Expr", Column: "Expr", When: "Expr",
        StringMethods: "StringNS", DateTimeMethods: "DateTimeNS", Gate: "Gate",
        RunFlow: "NodeOutputs", PythonScript: "NodeOutputs", CustomNode: "NodeOutputs", _CanvasNode: "NodeOutputs",
        CustomNodes: "CustomNodes", CustomNodeFactory: "CustomNodeFactory", PythonScriptFunction: "ScriptFunction",
        Parameter: "Parameter", FlowRef: "FlowRef", FuzzyMapping: "FuzzyMapping", pl.DataFrame: "pl_frame",
        FlowGraph: "graph", pl.Field: "dtype",
        _Inert: "inert", _Reader: "reader", _Helper: "helper", _PolarsCodeDef: "polars_code_def", _Datetime: "datetime",
    }  # fmt: skip


def kind_of(value: Any) -> str | None:
    """The kind of ``value`` (see :mod:`flowfile_core.notebook.allowlist`), or ``None`` when a cell may not hold it."""
    kind = _kinds().get(type(value))
    if kind is not None:
        return kind
    if value is pl:
        return "pl"
    if type(value) is types.ModuleType and value.__name__ == "flowfile":
        return "fl"
    if isinstance(value, pl.DataType) or type(value) is DataTypeClass:
        return "dtype" if type(value).__module__.startswith("polars.") else None
    return None


def _own_strings(table: Mapping[str, Any]) -> dict[str, str]:
    return {key: key for key in table}


_TABLE_STRINGS: dict[str, dict[str, str]] = {kind: _own_strings(table) for kind, table in allowlist.ALLOWLIST.items()}
_INPUT_ONLY_STRINGS: dict[str, dict[str, str]] = {
    kind: _own_strings(table) for kind, table in allowlist.INPUT_ONLY.items()
}
_FL_STRINGS: dict[str, str] = _own_strings(allowlist.FL_VERDICTS)


class _Failure(Exception):
    """An interpreter failure on a line; turned into ``notebook_cells.CellFailure`` at the executor's edge."""

    def __init__(self, message: str, line: int | None, kind: str | None) -> None:
        super().__init__(message)
        self.message = message
        self.line = line
        self.kind = kind


def _needs_kernel(what: str, line: int | None) -> _Failure:
    return _Failure(f"{what} is not part of the notebook's flow code; this needs a kernel", line, "needs_kernel")


def _error(exc: BaseException, line: int | None) -> _Failure:
    """A failure that carries ``exc`` as its cause; ``execute_cell`` classifies it (refusal or error)."""
    failure = _Failure("".join(traceback.format_exception_only(type(exc), exc)), line, None)
    failure.__cause__ = exc
    return failure


def _kind_label(kind: str | None) -> str:
    return {"fl": "`fl`", "pl": "`pl`", "none": "None", "graph": "the session graph `flow`"}.get(kind or "", kind or "")


class CellInterpreter:
    """The notebook's cell executor in core: describes each cell through the allowlisted frame calls.

    Called as ``executor(filename, code, namespace)`` (see ``notebook_cells.CellExecutor``); one
    instance serves one run, whose steps, literal elements, expression text and placed nodes it
    bounds (``allowlist.BOUNDS``). ``used`` collects every ``(kind, attribute)`` allowlist entry the
    cells exercised, ``used_input_only`` every ``allowlist.INPUT_ONLY`` one; with ``emitted_only``
    only what the render writes is accepted. A failure is raised as ``notebook_cells.CellFailure``.
    """

    def __init__(self, emitted_only: bool = False) -> None:
        self.emitted_only = emitted_only
        self.used: set[tuple[str, str]] = set()
        self.used_input_only: set[tuple[str, str]] = set()
        self.steps = 0
        self.elements = 0
        self.expression_chars = 0
        self._fl: types.ModuleType | None = None

    def fl(self) -> types.ModuleType:
        """The ``fl`` module ``import flowfile as fl`` binds: the notebook cell namespace's own."""
        if self._fl is None:
            from flowfile_frame.notebook_cells import new_namespace

            self._fl = new_namespace()["fl"]
        return self._fl

    def __call__(self, filename: str, code: str, namespace: dict[str, Any]) -> Any:
        from flowfile_frame.notebook_cells import CellFailure

        try:
            tree = _parse(filename, code)
            return _Cell(self, code, namespace).run(tree)
        except _Failure as failure:
            raise CellFailure(failure.message, line=failure.line, kind=failure.kind) from failure.__cause__
        except RecursionError as exc:
            raise CellFailure("The cell nests too deeply to read", line=1, kind="refused") from exc


def interprets_expression(code: str) -> bool:
    """Whether a cell interprets ``code``, one expression naming only ``fl``, ``pl`` and ``datetime``.

    The notebook render asks this of each translated formula, so a translation outside the dialect
    keeps its formula text instead of rendering a cell that needs a kernel.
    """
    interpreter = CellInterpreter(emitted_only=True)
    namespace = {"fl": interpreter.fl(), "pl": pl, "datetime": _Datetime("datetime")}
    try:
        _Cell(interpreter, code, namespace).expr(ast.parse(code, mode="eval").body)
    except (_Failure, RecursionError):
        return False
    return True


def _parse(filename: str, code: str) -> ast.Module:
    """``ast.parse`` plus ``symtable`` (the scope errors ``compile`` reports), then the size bounds."""
    try:
        tree = ast.parse(code, filename, "exec")
        symtable.symtable(code, filename, "exec")
    except (RecursionError, MemoryError) as exc:
        raise _Failure("The cell is too large or too deeply nested to read", 1, "refused") from exc
    bounds = allowlist.BOUNDS
    if len(tree.body) > bounds["statements_per_cell"]:
        raise _Failure(f"The cell has more than {bounds['statements_per_cell']} statements", 1, "refused")
    count = 0
    stack: list[tuple[ast.AST, int]] = [(tree, 0)]
    while stack:
        node, depth = stack.pop()
        count += 1
        line = getattr(node, "lineno", 1)
        if count > bounds["ast_nodes_per_cell"]:
            raise _Failure("The cell is too large to read", 1, "refused")
        if depth > bounds["depth"]:
            raise _Failure("The cell nests too deeply to read", line, "refused")
        if isinstance(node, ast.Constant) and isinstance(node.value, str | bytes):
            if len(node.value) > bounds["string_length"]:
                raise _Failure("A string in the cell is too long", line, "refused")
        for child in ast.iter_child_nodes(node):
            stack.append((child, depth if _chain_link(node, child) else depth + 1))
    return tree


def _chain_link(parent: ast.AST, child: ast.AST) -> bool:
    """Whether ``child`` is the receiver or callee of ``parent`` in a method chain (read iteratively)."""
    if isinstance(parent, ast.Call):
        return child is parent.func
    return isinstance(parent, ast.Attribute | ast.Subscript) and child is parent.value


class _Cell:
    """One cell's interpretation over the run's namespace."""

    def __init__(self, interpreter: CellInterpreter, code: str, namespace: dict[str, Any]) -> None:
        self.interpreter = interpreter
        self.code = code
        self.namespace = namespace

    def text(self, node: ast.AST, limit: int = 60) -> str:
        segment = ast.get_source_segment(self.code, node) or type(node).__name__
        segment = " ".join(segment.split())
        return f"`{segment if len(segment) <= limit else segment[: limit - 3] + '...'}`"

    def step(self, node: ast.AST) -> None:
        self.interpreter.steps += 1
        if self.interpreter.steps > allowlist.BOUNDS["steps_per_request"]:
            raise _Failure("The notebook takes too many steps to read", node.lineno, "refused")

    def run(self, tree: ast.Module) -> Any:
        body = tree.body
        script_lines = [i for i, stmt in enumerate(body) if isinstance(stmt, ast.FunctionDef) and _is_script(stmt)]
        value = None
        for index, statement in enumerate(body):
            prelude = bool(script_lines) and index < script_lines[-1]
            value = self.statement(statement, prelude)
        if body and isinstance(body[-1], ast.Expr) and kind_of(value) in _DISPLAYED_KINDS:
            return value
        return None

    def statement(self, node: ast.stmt, prelude: bool) -> Any:
        self.step(node)
        if isinstance(node, ast.Import):
            for alias in node.names:
                self.bind_import(alias, node, prelude)
            return None
        if isinstance(node, ast.ImportFrom):
            return self.import_from(node)
        if isinstance(node, ast.Assign):
            if prelude and self.prelude_constant(node):
                return None
            return self.assign(node)
        if isinstance(node, ast.Expr):
            if not isinstance(node.value, ast.Call | ast.Name | ast.Attribute | ast.Subscript | ast.Constant):
                raise _needs_kernel(f"The expression statement {self.text(node)}", node.lineno)
            return self.expr(node.value)
        if isinstance(node, ast.FunctionDef):
            return self.function(node)
        raise _needs_kernel(f"A `{type(node).__name__}` statement", node.lineno)

    def bind_import(self, alias: ast.alias, node: ast.stmt, prelude: bool) -> None:
        binding = allowlist.IMPORTS.get((alias.name, alias.asname))
        bound = alias.asname or alias.name.partition(".")[0]
        if binding is None:
            if not prelude:
                raise _needs_kernel(f"`import {alias.name}`", node.lineno)
            self.check_store(bound, node.lineno, prelude=True)
            top = alias.name.partition(".")[0]
            if sys.modules.get(top) is None and importlib.util.find_spec(top) is None:
                raise _error(ModuleNotFoundError(f"No module named {top!r}", name=top), node.lineno)
            self.namespace[bound] = _Inert(alias.name if alias.asname else top)
            return
        self.interpreter.used.add(("import", alias.name))
        values = {"fl": self.interpreter.fl, "pl": lambda: pl, "datetime": lambda: _Datetime("datetime")}
        self.namespace[bound] = values[binding]() if binding in values else _Inert(alias.name)

    def import_from(self, node: ast.ImportFrom) -> None:
        allowed = allowlist.FROM_IMPORTS.get(node.module or "", frozenset())
        for alias in node.names:
            if node.level or alias.asname or alias.name not in allowed:
                raise _needs_kernel(f"`from {node.module} import {alias.name}`", node.lineno)
            import flowfile_frame

            self.interpreter.used.add((f"from {node.module}", alias.name))
            self.namespace[alias.name] = _Reader(alias.name, getattr(flowfile_frame, alias.name))

    def assign(self, node: ast.Assign) -> None:
        if len(node.targets) != 1:
            raise _needs_kernel(f"The chained assignment {self.text(node)}", node.lineno)
        target = node.targets[0]
        names = [target] if isinstance(target, ast.Name) else getattr(target, "elts", None)
        if names is None or not all(isinstance(name, ast.Name) for name in names):
            raise _needs_kernel(f"Assigning to {self.text(target)}", target.lineno)
        for name in names:
            self.check_store(name.id, name.lineno)
        value = self.expr(node.value)
        kind = kind_of(value)
        if isinstance(target, ast.Name):
            self.check_bindable(kind, target.id, node.lineno)
            self.namespace[target.id] = value
            return
        if kind not in ("tuple", "list"):
            raise _needs_kernel(f"Unpacking {_kind_label(kind)} into {self.text(target)}", node.lineno)
        if len(value) != len(names):
            problem = "too many values to unpack" if len(value) > len(names) else "not enough values to unpack"
            got = "" if len(value) > len(names) else f", got {len(value)}"
            raise _error(ValueError(f"{problem} (expected {len(names)}{got})"), node.lineno)
        for name, item in zip(names, value, strict=True):
            self.check_bindable(kind_of(item), name.id, node.lineno)
            self.namespace[name.id] = item

    def prelude_constant(self, node: ast.Assign) -> bool:
        """Bind ``NAME = <literal>`` above a script's ``def``, the way ``python_script._prelude`` writes a constant.

        A prelude also holds what ``callable_utils._is_safely_representable`` accepts beyond a cell's
        literals (sets, bytes, tuple keys) and ``_`` names; ``ast.literal_eval`` reads the parsed value,
        building literals only. ``False`` for any other statement, which then binds as usual.
        """
        target = node.targets[0] if len(node.targets) == 1 else None
        if not isinstance(target, ast.Name) or not _is_prelude_literal(node.value):
            return False
        self.check_store(target.id, target.lineno, prelude=True)
        self.charge(sum(1 for _ in ast.walk(node.value)), node)
        try:
            self.namespace[target.id] = ast.literal_eval(node.value)
        except (TypeError, ValueError) as exc:
            raise _error(exc, node.lineno) from exc
        return True

    def check_store(self, name: str, line: int, prelude: bool = False) -> None:
        """A name a cell may bind: not one the interpreter binds itself; a script's prelude may bind ``_name``."""
        pattern = _PRELUDE_NAME if prelude else _STORE_NAME
        if name in allowlist.RESERVED_NAMES or name in allowlist.HELPERS or not pattern.fullmatch(name):
            raise _needs_kernel(f"Binding the name `{name}`", line)

    def check_bindable(self, kind: str | None, name: str, line: int) -> None:
        if kind is None or kind in _MODULE_KINDS:
            raise _needs_kernel(f"Binding {_kind_label(kind) or 'this value'} to `{name}`", line)

    def function(self, node: ast.FunctionDef) -> None:
        """The three ``def`` shapes the render emits: a module helper, a Polars Code body, a Python Script."""
        from flowfile_core.notebook.render import _NOTEBOOK_HELPERS

        if node.name in allowlist.HELPERS and not node.decorator_list:
            if ast.get_source_segment(self.code, node) != _NOTEBOOK_HELPERS[node.name]:
                raise _needs_kernel(f"A `{node.name}` that differs from the notebook's own", node.lineno)
            self.interpreter.used.add(("helper", node.name))
            self.namespace[node.name] = _Helper(node.name, _HELPER_FUNCTIONS[node.name])
        elif _POLARS_CODE_NAME.fullmatch(node.name) and not node.decorator_list:
            self.polars_code_def(node)
        elif _is_script(node):
            self.script_def(node)
        else:
            raise _needs_kernel(f"The function `{node.name}`", node.lineno)

    def polars_code_def(self, node: ast.FunctionDef) -> None:
        from flowfile_frame.flow_frame import _polars_code_text

        args = node.args
        if args.posonlyargs or args.vararg or args.kwonlyargs or args.kwarg or args.defaults or node.returns:
            raise _needs_kernel(f"The signature of `{node.name}`", node.lineno)
        for arg in args.args:
            annotation = arg.annotation
            if annotation is None:
                continue
            if not (
                isinstance(annotation, ast.Attribute)
                and isinstance(annotation.value, ast.Name)
                and annotation.attr == "FlowFrame"
                and kind_of(self.name(annotation.value)) == "fl"
            ):
                raise _needs_kernel(f"The annotation {self.text(annotation)}", annotation.lineno)
        self.check_store(node.name, node.lineno)
        self.namespace[node.name] = _PolarsCodeDef(node.name, _polars_code_text(_block(self.code, node.lineno)))

    def script_def(self, node: ast.FunctionDef) -> None:
        """``@fl.python_script(...)``: the decorator's keywords like a call's, the cells from the source text."""
        from flowfile_frame.python_script import PythonScriptFunction

        decorator = node.decorator_list[0]
        call = decorator if isinstance(decorator, ast.Call) else None
        target = call.func if call is not None else decorator
        if kind_of(self.name(target.value)) != "fl":
            raise _needs_kernel(f"The decorator {self.text(decorator)}", decorator.lineno)
        self.interpreter.used.add(("fl", "python_script"))
        if call is not None and call.args:
            raise _needs_kernel(f"The decorator {self.text(decorator)}", decorator.lineno)
        keywords = {}
        for keyword in call.keywords if call is not None else ():
            if keyword.arg is None or keyword.arg.startswith("_"):
                raise _needs_kernel(f"The decorator argument {self.text(keyword)}", keyword.value.lineno)
            keywords[keyword.arg] = self.argument(keyword.value, None, keyword.arg)
        if not _SCRIPT_NAME.fullmatch(node.name):
            raise _needs_kernel(f"The function name `{node.name}`", node.lineno)
        self.check_store(node.name, node.lineno)
        view = {name: _as_global(value) for name, value in self.namespace.items()}
        try:
            script = PythonScriptFunction._from_source(self.code, decorator.lineno, view, **keywords)
        except Exception as exc:
            # 3.10 reports a failing decorator application on the `def` line
            raise _error(exc, node.lineno if sys.version_info < (3, 11) else decorator.lineno) from exc
        self.namespace[node.name] = script

    def name(self, node: ast.Name) -> Any:
        self.step(node)
        name = node.id
        if name.startswith("__") or (name.startswith("_") and not _STORE_NAME.fullmatch(name)):
            if name not in allowlist.HELPERS:
                raise _needs_kernel(f"The name `{name}`", node.lineno)
        if name not in self.namespace:
            if name in _BUILTIN_NAMES or name == "display":
                raise _needs_kernel(f"`{name}`", node.lineno)
            raise _error(NameError(f"name {name!r} is not defined", name=name), node.lineno)
        value = self.namespace[name]
        if kind_of(value) is None:
            raise _needs_kernel(f"`{name}`", node.lineno)
        return value

    def expr(self, node: ast.expr) -> Any:
        self.step(node)
        if isinstance(node, ast.Constant):
            if kind_of(node.value) not in _LITERAL_KINDS:
                raise _needs_kernel(f"The constant {self.text(node)}", node.lineno)
            return node.value
        if isinstance(node, ast.Name):
            return self.name(node)
        if isinstance(node, ast.Call | ast.Attribute | ast.Subscript):
            return self.chain(node)
        if isinstance(node, ast.List | ast.Tuple):
            self.charge(len(node.elts), node)
            if any(isinstance(element, ast.Starred) for element in node.elts):
                raise _needs_kernel(f"Unpacking inside {self.text(node)}", node.lineno)
            items = [self.data(element) for element in node.elts]
            return items if isinstance(node, ast.List) else tuple(items)
        if isinstance(node, ast.Dict):
            return self.dict_literal(node)
        if isinstance(node, ast.BinOp):
            op = type(node.op).__name__
            return self.operator(node, op, allowlist.BINARY_OPERATORS, _BINARY, node.left, node.right)
        if isinstance(node, ast.Compare):
            if len(node.ops) != 1:
                raise _needs_kernel(f"The chained comparison {self.text(node)}", node.lineno)
            op = type(node.ops[0]).__name__
            return self.operator(node, op, allowlist.COMPARE_OPERATORS, _COMPARE, node.left, node.comparators[0])
        if isinstance(node, ast.UnaryOp):
            operand = node.operand
            if isinstance(node.op, ast.USub | ast.UAdd) and isinstance(operand, ast.Constant):
                if type(operand.value) in (int, float):
                    return -operand.value if isinstance(node.op, ast.USub) else operand.value
            raise _needs_kernel(f"The operator in {self.text(node)}", node.lineno)
        if isinstance(node, ast.JoinedStr):
            return self.joined_str(node)
        raise _needs_kernel(f"{self.text(node)}", node.lineno)

    def charge(self, count: int, node: ast.AST) -> None:
        self.interpreter.elements += count
        if self.interpreter.elements > allowlist.BOUNDS["literal_elements_per_request"]:
            raise _Failure("The notebook's literals hold too many elements to read", node.lineno, "refused")

    def data(self, node: ast.expr) -> Any:
        value = self.expr(node)
        kind = kind_of(value)
        if kind not in _DATA_KINDS:
            raise _needs_kernel(f"Using {_kind_label(kind) or 'this value'} as {self.text(node)} here", node.lineno)
        return value

    def dict_literal(self, node: ast.Dict) -> dict[Any, Any]:
        self.charge(len(node.keys), node)
        result = {}
        for key, value in zip(node.keys, node.values, strict=True):
            if key is None:
                raise _needs_kernel(f"Unpacking inside {self.text(node)}", node.lineno)
            evaluated = self.expr(key)
            if kind_of(evaluated) not in _LITERAL_KINDS:
                raise _needs_kernel(f"The key {self.text(key)}", key.lineno)
            result[evaluated] = self.data(value)
        return result

    def operator(
        self, node: ast.expr, op: str, allowed: frozenset[str], functions: Mapping[str, Callable], left, right
    ) -> Any:
        if op not in allowed:
            raise _needs_kernel(f"The operator in {self.text(node)}", node.lineno)
        values = [self.expr(left), self.expr(right)]
        kinds = [kind_of(value) for value in values]
        if "Expr" not in kinds or not all(kind in _OPERAND_KINDS for kind in kinds):
            raise _needs_kernel(f"{self.text(node)} (an operator needs an expression on one side)", node.lineno)
        try:
            result = functions[op](*values)
        except Exception as exc:
            raise _error(exc, node.lineno) from exc
        return self.checked(result, node, node.lineno)

    def joined_str(self, node: ast.JoinedStr) -> str:
        """An f-string whose every placeholder is ``_flowfile_expr_literal(<name>)``, no conversion or format spec."""
        parts = []
        for value in node.values:
            if isinstance(value, ast.Constant):
                parts.append(value.value)
                continue
            call = value.value
            if not (
                value.conversion == -1
                and value.format_spec is None
                and isinstance(call, ast.Call)
                and isinstance(call.func, ast.Name)
                and call.func.id == "_flowfile_expr_literal"
                and len(call.args) == 1
                and isinstance(call.args[0], ast.Name)
                and not call.keywords
            ):
                raise _needs_kernel(f"The f-string {self.text(node)}", node.lineno)
            result = self.chain(call)
            if kind_of(result) not in _LITERAL_KINDS:
                raise _needs_kernel(f"The f-string {self.text(node)}", node.lineno)
            parts.append(format(result, ""))
        return "".join(parts)

    def chain(self, node: ast.expr) -> Any:
        """A method chain, read link by link from its base (so its length costs no recursion)."""
        links: list[ast.expr] = []
        current = node
        while isinstance(current, ast.Call | ast.Attribute | ast.Subscript):
            links.append(current)
            current = current.func if isinstance(current, ast.Call) else current.value
        links.reverse()
        value = self.expr(current)
        index = 0
        while index < len(links):
            link = links[index]
            self.step(link)
            method = index + 1 < len(links) and isinstance(links[index + 1], ast.Call)
            if isinstance(link, ast.Attribute) and method and links[index + 1].func is link:
                value = self.method(value, link, links[index + 1])
                index += 2
                continue
            if isinstance(link, ast.Attribute):
                value = self.read(value, link)
            elif isinstance(link, ast.Subscript):
                value = self.subscript(value, link)
            else:
                value = self.call_value(value, link)
            index += 1
        return value

    def entry(self, receiver: Any, attr: str, line: int) -> tuple[str, str, str]:
        """``(kind, attribute, usage)`` for ``receiver.attr``, recorded as used; else a refusal on ``line``.

        The attribute string handed to ``getattr`` is the allowlist's own; only a custom node key
        (the ``*`` entry of ``fl.custom_nodes``, never one of its class's attributes) is data. An ``fl``
        name the render refuses is still an input-only entry when ``allowlist.INPUT_ONLY`` names it.
        """
        kind = kind_of(receiver)
        if attr.startswith("_"):
            raise _needs_kernel(f"The attribute `.{attr}`", line)
        input_only = {} if self.interpreter.emitted_only else allowlist.INPUT_ONLY.get(kind or "", {})
        if kind == "fl":
            verdict, detail = allowlist.FL_VERDICTS.get(attr, (allowlist.REFUSE, "is not a flowfile name"))
            if verdict == allowlist.ALLOW:
                self.interpreter.used.add((kind, attr))
                return kind, _FL_STRINGS[attr], detail
            if attr not in input_only:
                raise _needs_kernel(f"`fl.{attr}` {detail}; it", line)
        table = allowlist.ALLOWLIST.get(kind or "", {})
        if attr in table:
            self.interpreter.used.add((kind, attr))
            return kind, _TABLE_STRINGS[kind][attr], table[attr]
        if attr in input_only:
            self.interpreter.used_input_only.add((kind, attr))
            return kind, _INPUT_ONLY_STRINGS[kind][attr], input_only[attr]
        if "*" in table and not hasattr(type(receiver), attr):
            self.interpreter.used.add((kind, "*"))
            return kind, attr, table["*"]
        raise _needs_kernel(f"`.{attr}` on {_kind_label(kind) or 'this value'}", line)

    def attribute(self, receiver: Any, kind: str, attr: str, line: int) -> Any:
        try:
            if kind == "CustomNodes":
                return type(receiver).__getattr__(receiver, attr)
            return getattr(receiver, attr)
        except Exception as exc:
            raise _error(exc, line) from exc

    def read(self, receiver: Any, node: ast.Attribute) -> Any:
        kind, attr, usage = self.entry(receiver, node.attr, node.end_lineno)
        if usage not in (allowlist.READ, allowlist.BOTH):
            raise _needs_kernel(f"Reading {self.text(node)} without calling it", node.end_lineno)
        return self.checked(self.attribute(receiver, kind, attr, node.end_lineno), node, node.end_lineno)

    def method(self, receiver: Any, node: ast.Attribute, call: ast.Call) -> Any:
        """``receiver.attr(...)``: a call entry, or a readable attribute whose value is callable by kind."""
        line = node.end_lineno
        kind, attr, usage = self.entry(receiver, node.attr, line)
        if usage == allowlist.DECORATOR:
            raise _needs_kernel(f"Calling `fl.{attr}` outside a decorator", line)
        # 3.10 compiles a method call with keywords as a plain call, reported on the call's first line
        call_line = call.lineno if sys.version_info < (3, 11) and call.keywords else line
        if usage in (allowlist.CALL, allowlist.BOTH):
            function = self.attribute(receiver, kind, attr, line)
            return self.invoke(function, (kind, attr), call, call_line)
        return self.call_value(self.checked(self.attribute(receiver, kind, attr, line), node, line), call, call_line)

    def call_value(self, callee: Any, call: ast.Call, line: int | None = None) -> Any:
        """Calling a value by its kind's ``__call__`` entry (a helper, a reader, a script, a custom node)."""
        kind = kind_of(callee)
        if "__call__" not in allowlist.ALLOWLIST.get(kind or "", {}):
            raise _needs_kernel(f"Calling {_kind_label(kind) or 'this value'} in {self.text(call)}", call.lineno)
        self.interpreter.used.add((kind, "__call__"))
        function = callee.target if isinstance(callee, _Standin) else callee
        return self.invoke(function, (kind, "__call__"), call, line or call.lineno)

    def subscript(self, receiver: Any, node: ast.Subscript) -> Any:
        kind = kind_of(receiver)
        if "[]" not in allowlist.ALLOWLIST.get(kind or "", {}):
            raise _needs_kernel(f"Subscripting {_kind_label(kind) or 'this value'} in {self.text(node)}", node.lineno)
        key = node.slice
        if not (isinstance(key, ast.Constant) and type(key.value) is str):
            raise _needs_kernel(f"The subscript {self.text(node)} (keys are literal strings)", node.lineno)
        self.interpreter.used.add((kind, "[]"))
        try:
            value = receiver[key.value]
        except Exception as exc:
            raise _error(exc, node.lineno) from exc
        return self.checked(value, node, node.lineno)

    def invoke(self, function: Callable, key: tuple[str, str], call: ast.Call, line: int) -> Any:
        """Evaluate the arguments under the entry's rules, call ``function`` and check what it returns."""
        shape = allowlist.LITERAL_ARGUMENTS.get(key)
        if shape is not None:
            _check_literal_shape(shape, call, self)
        refused = allowlist.REFUSED_KEYWORDS.get(key, frozenset())
        accepted = allowlist.DATA_ARGUMENTS.get(key)
        present = {keyword.arg for keyword in call.keywords}
        needs = allowlist.KEYWORD_REQUIRES.get(key)
        if needs is not None and needs[0] in present and needs[1] not in present:
            raise _needs_kernel(f"`{key[1]}({needs[0]}=...)` without `{needs[1]}=`", call.lineno)
        args = []
        for position, argument in enumerate(call.args):
            if isinstance(argument, ast.Starred):
                raise _needs_kernel(f"Unpacking arguments in {self.text(call)}", argument.lineno)
            args.append(self.argument(argument, key, position))
        kwargs = {}
        for keyword in call.keywords:
            if keyword.arg is None:
                raise _needs_kernel(f"Unpacking keyword arguments in {self.text(call)}", keyword.value.lineno)
            unaccepted = accepted is not None and keyword.arg not in accepted
            if keyword.arg.startswith("_") or keyword.arg in refused or unaccepted:
                raise _needs_kernel(f"The argument `{keyword.arg}=`", keyword.value.lineno)
            kwargs[keyword.arg] = self.argument(keyword.value, key, keyword.arg)
        if needs is not None and needs[0] in kwargs and kwargs[needs[1]] is None:
            raise _needs_kernel(f"`{key[1]}({needs[0]}=...)` without `{needs[1]}=`", call.lineno)
        # Polars makes a row of each character of a string, which no literal budget charges
        if accepted is not None and isinstance(args[0] if args else kwargs.get("data"), str):
            raise _needs_kernel(f"`fl.{key[1]}` with a string as data", line)
        try:
            result = function(*args, **kwargs)
        except Exception as exc:
            raise _error(exc, line) from exc
        self.check_nodes(call)
        return self.checked(result, call, line)

    def argument(self, node: ast.expr, key: tuple[str, str] | None, slot: int | str) -> Any:
        """An argument value: data (literal data only for a ``DATA_ARGUMENTS`` call), or the one non-data kind
        the allowlist accepts in this slot."""
        value = self.expr(node)
        kind = kind_of(value)
        kinds = _LITERAL_DATA_KINDS if key in allowlist.DATA_ARGUMENTS else _DATA_KINDS
        if kind in kinds:
            self.check_data(value, node, kinds)
            return value
        special = allowlist.ARGUMENT_KINDS.get(key, {}).get(slot) if key is not None else None
        if kind is not None and kind == special:
            if kind == "graph":
                from flowfile_frame import notebook

                mode = notebook.current()
                if mode is None or value is not mode.graph:
                    raise _needs_kernel("A graph other than the notebook's session graph", node.lineno)
            if kind == "polars_code_def":
                from flowfile_frame.flow_frame import _PolarsCodeText

                return _PolarsCodeText(value.target)
            return value
        raise _needs_kernel(f"Passing {_kind_label(kind) or 'this value'} as {self.text(node)}", node.lineno)

    def check_data(self, value: Any, node: ast.AST, kinds: frozenset[str] = _DATA_KINDS, depth: int = 0) -> None:
        """Every element of a container argument is data too (of ``kinds``).

        Each element visited is a step, so a list holding one list many times cannot multiply the walk
        past the step budget. The literal budget is left alone: a literal was charged when it was built.
        """
        if depth > allowlist.BOUNDS["depth"]:
            raise _Failure("A value in the cell nests too deeply to read", node.lineno, "refused")
        if isinstance(value, list | tuple):
            items = value
        elif isinstance(value, dict):
            items = [*value.keys(), *value.values()]
        else:
            return
        for item in items:
            self.step(node)
            if kind_of(item) not in kinds:
                raise _needs_kernel(f"A {_kind_label(kind_of(item)) or 'value'} inside {self.text(node)}", node.lineno)
            self.check_data(item, node, kinds, depth + 1)

    def checked(self, value: Any, node: ast.AST, line: int | None) -> Any:
        """A value a call or read produced: it must have a kind (containers: every element).

        Every expression's generated text is charged to the run, so an operator or call repeated on
        its own result (each doubling the text) is refused on its line before the text outgrows memory.
        """
        kind = kind_of(value)
        if kind is None:
            raise _needs_kernel(f"{self.text(node)}, which gives a `{type(value).__name__}`,", line)
        if kind == "Expr":
            self.interpreter.expression_chars += len(value._repr_str)
            if self.interpreter.expression_chars > allowlist.BOUNDS["expression_chars_per_request"]:
                raise _Failure("The notebook builds expressions too long to read", line, "refused")
        if kind in ("list", "tuple", "dict"):
            self.check_data(value, node)
        return value

    def check_nodes(self, node: ast.AST) -> None:
        from flowfile_frame import notebook

        mode = notebook.current()
        if mode is not None and len(mode.graph._node_db) > allowlist.BOUNDS["nodes_per_request"]:
            raise _Failure("The notebook places too many nodes", node.lineno, "refused")


def _check_literal_shape(shape: str, call: ast.Call, cell: _Cell) -> None:
    """The literal-only argument shapes of ``allowlist.LITERAL_ARGUMENTS``, checked on the AST before the call."""
    line = call.lineno
    if shape == "ints":
        if call.keywords or not all(isinstance(a, ast.Constant) and type(a.value) is int for a in call.args):
            raise _needs_kernel(f"{cell.text(call)} (dates are built from literal integers)", line)
    elif shape == "node_id":
        if not call.args or not (isinstance(call.args[0], ast.Constant) and type(call.args[0].value) is int):
            raise _needs_kernel(f"{cell.text(call)} (a canvas node is named by its literal id)", line)
    elif shape == "sample":
        data = call.args[0] if len(call.args) == 1 else None
        literal = isinstance(data, ast.Dict) and all(
            isinstance(key, ast.Constant)
            and type(key.value) is str
            and isinstance(value, ast.List)
            and all(isinstance(item, ast.Constant) or _is_signed_number(item) for item in value.elts)
            for key, value in zip(data.keys, data.values, strict=True)
        )
        keywords = {keyword.arg for keyword in call.keywords}
        if not literal or not keywords <= {"schema", "strict"}:
            raise _needs_kernel(f"{cell.text(call)} (a sample frame holds literal columns)", line)


def _is_signed_number(node: ast.expr) -> bool:
    """``-5`` or ``+2.5``: a unary sign over an int or float constant, as ``repr`` writes a negative number."""
    return (
        isinstance(node, ast.UnaryOp)
        and isinstance(node.op, ast.USub | ast.UAdd)
        and isinstance(node.operand, ast.Constant)
        and type(node.operand.value) in (int, float)
    )


def _is_prelude_literal(node: ast.expr) -> bool:
    """A value ``repr`` writes for a prelude constant: scalars, bytes, signed numbers, their containers, ``set()``."""
    if isinstance(node, ast.Constant):
        return type(node.value) in (int, float, bool, str, bytes, type(None))
    if isinstance(node, ast.List | ast.Tuple | ast.Set):
        return all(_is_prelude_literal(item) for item in node.elts)
    if isinstance(node, ast.Dict):
        return all(key is not None and _is_prelude_literal(key) for key in node.keys) and all(
            _is_prelude_literal(value) for value in node.values
        )
    if isinstance(node, ast.Call):
        return isinstance(node.func, ast.Name) and node.func.id == "set" and not node.args and not node.keywords
    return _is_signed_number(node)


def _is_script(node: ast.FunctionDef) -> bool:
    """Exactly one decorator, ``fl.python_script`` or ``fl.python_script(...)``."""
    if len(node.decorator_list) != 1:
        return False
    decorator = node.decorator_list[0]
    target = decorator.func if isinstance(decorator, ast.Call) else decorator
    return (
        isinstance(target, ast.Attribute)
        and target.attr == "python_script"
        and isinstance(target.value, ast.Name)
        and target.value.id == "fl"
    )


def _block(code: str, first_line: int) -> str:
    """The ``def`` starting on ``first_line``, as ``inspect.getsource`` reads it from the cell."""
    import inspect
    import textwrap

    return textwrap.dedent("".join(inspect.getblock(code.splitlines(True)[first_line - 1 :])))


def _as_global(value: Any) -> Any:
    """How a script's prelude sees a cell name: a stand-in module as a module object, anything else as it is."""
    if isinstance(value, _Inert | _Datetime):
        return types.ModuleType(value.name)
    return value
