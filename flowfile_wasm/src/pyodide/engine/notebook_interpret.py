"""Read notebook cells as a description of the flow, without ever executing them.

A cell is parsed with ``ast`` and walked. Each name, attribute, call and operator is looked up in
:mod:`notebook_allowlist` by the *kind* of the value it is used on; anything else fails on its
line. Nothing a cell contains reaches ``exec``, ``eval`` or ``compile``.

This follows the rules of ``flowfile_core/notebook/interpret.py``. The difference is what an
allowed call does: the full app runs the real frame method, the browser records it on the
stand-ins in :mod:`notebook_shim`.
"""

from __future__ import annotations

import ast
import builtins
import datetime
import re
from typing import Any

from . import notebook_allowlist as allowlist
from .notebook_shim import DateTimeNS, Dtype, Expr, Module, StringNS, kind_of

_DATA_KINDS = frozenset(
    {"none", "bool", "int", "float", "str", "list", "tuple", "dict", "date", "datetime_value", "FlowFrame", "Expr", "dtype"}
)  # fmt: skip
_LITERAL_KINDS = frozenset({"none", "bool", "int", "float", "str"})
_OPERAND_KINDS = frozenset({"Expr", "int", "float", "str", "bool", "none", "date", "datetime_value"})
_MODULE_KINDS = frozenset({"ff", "pl", "datetime"})
_STORE_NAME = re.compile(rf"{allowlist.USER_NAME}|{'|'.join(allowlist.GENERATED_NAMES)}")
_BUILTIN_NAMES = frozenset(dir(builtins))
# The frame's own string matchers take the pattern as text; an expression there fails in the full app too.
_TEXT_PATTERN_METHODS = frozenset({"contains", "starts_with", "ends_with"})


class CellFailure(Exception):
    """Why a cell could not be read, and the line it happened on."""

    def __init__(self, message: str, line: int | None = None, kind: str = "error") -> None:
        super().__init__(message)
        self.message = message
        self.line = line
        self.kind = kind


def needs_kernel(what: str, line: int | None) -> CellFailure:
    return CellFailure(f"{what} is not part of the notebook's flow code; this needs a kernel", line, "needs_kernel")


def _kind_label(kind: str | None) -> str:
    return {"ff": "`ff`", "pl": "`pl`", "none": "None"}.get(kind or "", kind or "")


def parse_cell(code: str) -> ast.Module:
    """``ast.parse`` plus the size bounds; a syntax error is reported on its own line."""
    try:
        tree = ast.parse(code)
    except SyntaxError as exc:
        raise CellFailure(f"SyntaxError: {exc.msg}", exc.lineno or 1, "error") from exc
    except (RecursionError, MemoryError) as exc:
        raise CellFailure("The cell is too large or too deeply nested to read", 1, "refused") from exc
    bounds = allowlist.BOUNDS
    if len(tree.body) > bounds["statements_per_cell"]:
        raise CellFailure(f"The cell has more than {bounds['statements_per_cell']} statements", 1, "refused")
    count = 0
    stack: list[tuple[ast.AST, int]] = [(tree, 0)]
    while stack:
        node, depth = stack.pop()
        count += 1
        line = getattr(node, "lineno", 1)
        if count > bounds["ast_nodes_per_cell"]:
            raise CellFailure("The cell is too large to read", 1, "refused")
        if depth > bounds["depth"]:
            raise CellFailure("The cell nests too deeply to read", line, "refused")
        if isinstance(node, ast.Constant) and isinstance(node.value, str | bytes):
            if len(node.value) > bounds["string_length"]:
                raise CellFailure("A string in the cell is too long", line, "refused")
        for child in ast.iter_child_nodes(node):
            stack.append((child, depth if _chain_link(node, child) else depth + 1))
    return tree


def _chain_link(parent: ast.AST, child: ast.AST) -> bool:
    """Whether ``child`` is the receiver or callee of ``parent`` in a method chain."""
    if isinstance(parent, ast.Call):
        return child is parent.func
    return isinstance(parent, ast.Attribute | ast.Subscript) and child is parent.value


class CellReader:
    """One cell's reading over a namespace of bound names.

    The expression rules live here; :mod:`notebook_cells` adds the statements and the frame calls
    by passing ``frame_calls``, a handler for ``(kind, attribute)`` calls on frames.
    """

    def __init__(self, code: str, namespace: dict[str, Any], frame_calls: Any = None) -> None:
        self.code = code
        self.namespace = namespace
        self.frame_calls = frame_calls
        self.steps = 0
        self.elements = 0

    def text(self, node: ast.AST, limit: int = 60) -> str:
        segment = ast.get_source_segment(self.code, node) or type(node).__name__
        segment = " ".join(segment.split())
        return f"`{segment if len(segment) <= limit else segment[: limit - 3] + '...'}`"

    def source(self, node: ast.AST) -> str:
        return ast.get_source_segment(self.code, node) or ""

    def step(self, node: ast.AST) -> None:
        self.steps += 1
        if self.steps > allowlist.BOUNDS["steps_per_request"]:
            raise CellFailure("The notebook takes too many steps to read", getattr(node, "lineno", 1), "refused")

    def charge(self, count: int, node: ast.AST) -> None:
        self.elements += count
        if self.elements > allowlist.BOUNDS["literal_elements_per_request"]:
            raise CellFailure("The notebook's literals hold too many elements to read", node.lineno, "refused")

    def name(self, node: ast.Name) -> Any:
        self.step(node)
        name = node.id
        if name.startswith("__") or (name.startswith("_") and not _STORE_NAME.fullmatch(name)):
            raise needs_kernel(f"The name `{name}`", node.lineno)
        if name not in self.namespace:
            if name in _BUILTIN_NAMES or name == "display":
                raise needs_kernel(f"`{name}`", node.lineno)
            raise CellFailure(f"NameError: name {name!r} is not defined", node.lineno, "error")
        value = self.namespace[name]
        if kind_of(value) is None:
            raise needs_kernel(f"`{name}`", node.lineno)
        return value

    def expr(self, node: ast.expr) -> Any:
        self.step(node)
        if isinstance(node, ast.Constant):
            if kind_of(node.value) not in _LITERAL_KINDS:
                raise needs_kernel(f"The constant {self.text(node)}", node.lineno)
            return node.value
        if isinstance(node, ast.Name):
            return self.name(node)
        if isinstance(node, ast.Call | ast.Attribute):
            return self.chain(node)
        if isinstance(node, ast.List | ast.Tuple):
            self.charge(len(node.elts), node)
            if any(isinstance(element, ast.Starred) for element in node.elts):
                raise needs_kernel(f"Unpacking inside {self.text(node)}", node.lineno)
            items = [self.data(element) for element in node.elts]
            return items if isinstance(node, ast.List) else tuple(items)
        if isinstance(node, ast.Dict):
            return self.dict_literal(node)
        if isinstance(node, ast.BinOp):
            op = type(node.op).__name__
            return self.operator(node, op, allowlist.BINARY_OPERATORS, "binary", node.left, node.right)
        if isinstance(node, ast.Compare):
            if len(node.ops) != 1:
                raise needs_kernel(f"The chained comparison {self.text(node)}", node.lineno)
            op = type(node.ops[0]).__name__
            return self.operator(node, op, allowlist.COMPARE_OPERATORS, "compare", node.left, node.comparators[0])
        if isinstance(node, ast.UnaryOp):
            operand = node.operand
            if isinstance(node.op, ast.USub | ast.UAdd) and isinstance(operand, ast.Constant):
                if type(operand.value) in (int, float):
                    return -operand.value if isinstance(node.op, ast.USub) else operand.value
            if isinstance(node.op, ast.Not):
                raise needs_kernel(f"{self.text(node)} (negate an expression with `.not_()`, not `not`)", node.lineno)
            raise needs_kernel(f"The operator in {self.text(node)}", node.lineno)
        if isinstance(node, ast.BoolOp):
            hint = "combine expressions with `&` / `|`, not `and` / `or`"
            raise needs_kernel(f"{self.text(node)} ({hint})", node.lineno)
        raise needs_kernel(f"{self.text(node)}", node.lineno)

    def data(self, node: ast.expr) -> Any:
        value = self.expr(node)
        kind = kind_of(value)
        if kind not in _DATA_KINDS:
            raise needs_kernel(f"Using {_kind_label(kind) or 'this value'} as {self.text(node)} here", node.lineno)
        return value

    def dict_literal(self, node: ast.Dict) -> dict[Any, Any]:
        self.charge(len(node.keys), node)
        result = {}
        for key, value in zip(node.keys, node.values, strict=True):
            if key is None:
                raise needs_kernel(f"Unpacking inside {self.text(node)}", node.lineno)
            evaluated = self.expr(key)
            if kind_of(evaluated) not in _LITERAL_KINDS:
                raise needs_kernel(f"The key {self.text(key)}", key.lineno)
            result[evaluated] = self.data(value)
        return result

    def operator(self, node: ast.expr, op: str, allowed: frozenset[str], family: str, left: ast.expr, right: ast.expr):
        if op not in allowed:
            raise needs_kernel(f"The operator in {self.text(node)}", node.lineno)
        a, b = self.expr(left), self.expr(right)
        kinds = (kind_of(a), kind_of(b))
        if "Expr" not in kinds or not all(kind in _OPERAND_KINDS for kind in kinds):
            raise needs_kernel(f"The operator in {self.text(node)} (it needs an expression on one side)", node.lineno)
        return Expr(f"{family}:{op}", (a, b), source=self.source(node))

    def chain(self, node: ast.Call | ast.Attribute) -> Any:
        """An attribute read or a call, looked up by the kind of what it is used on."""
        if isinstance(node, ast.Attribute):
            receiver, attribute = self.expr(node.value), node.attr
            usage = self.usage(receiver, attribute, node)
            if usage not in (allowlist.READ, allowlist.BOTH):
                raise needs_kernel(f"Reading {self.text(node)} without calling it", node.lineno)
            return self.read(receiver, attribute)
        func = node.func
        if not isinstance(func, ast.Attribute):
            raise needs_kernel(f"Calling {self.text(func)}", node.lineno)
        receiver, attribute = self.expr(func.value), func.attr
        usage = self.usage(receiver, attribute, func)
        if usage not in (allowlist.CALL, allowlist.BOTH):
            raise needs_kernel(f"Calling {self.text(func)}", node.lineno)
        args, kwargs = self.arguments(node)
        return self.call(receiver, attribute, args, kwargs, node)

    def usage(self, receiver: Any, attribute: str, node: ast.Attribute, input_only: bool = False) -> str:
        """How ``attribute`` may be used on ``receiver``; with ``input_only`` also what only a sync reads."""
        line = getattr(node, "end_lineno", None) or node.lineno
        if attribute.startswith("_"):
            raise needs_kernel(f"The attribute `{attribute}`", line)
        kind = kind_of(receiver)
        if input_only and attribute in allowlist.INPUT_ONLY.get(kind or "", {}):
            return allowlist.INPUT_ONLY[kind][attribute]
        if kind == "ff":
            verdict = allowlist.FL_VERDICTS.get(attribute)
            if verdict is None or verdict[0] != allowlist.ALLOW:
                raise needs_kernel(f"`ff.{attribute}`", line)
            return verdict[1]
        usage = allowlist.ALLOWLIST.get(kind or "", {}).get(attribute)
        if usage is None:
            raise needs_kernel(f"`.{attribute}` on {_kind_label(kind) or 'this value'}", line)
        return usage

    def read(self, receiver: Any, attribute: str) -> Any:
        kind = kind_of(receiver)
        if kind == "ff":
            return Dtype(attribute)
        if kind == "Expr" and attribute == "str":
            return StringNS(receiver, "str")
        return DateTimeNS(receiver, "dt")

    def arguments(self, node: ast.Call) -> tuple[list[Any], dict[str, Any]]:
        if any(isinstance(argument, ast.Starred) for argument in node.args):
            raise needs_kernel(f"Unpacking arguments in {self.text(node)}", node.lineno)
        args = [self.data(argument) for argument in node.args]
        kwargs: dict[str, Any] = {}
        for keyword in node.keywords:
            if keyword.arg is None or keyword.arg.startswith("_"):
                raise needs_kernel(f"The argument {self.text(keyword.value)}", keyword.value.lineno)
            kwargs[keyword.arg] = self.data(keyword.value)
        return args, kwargs

    def call(self, receiver: Any, attribute: str, args: list[Any], kwargs: dict[str, Any], node: ast.Call) -> Any:
        kind = kind_of(receiver)
        line = getattr(node.func, "end_lineno", None) or node.lineno
        source = self.source(node)
        if kind == "ff":
            if attribute in ("col", "lit", "len", "when"):
                return self.expression_head(attribute, args, kwargs, line, source)
            if attribute in allowlist._DTYPES:
                return Dtype(attribute, tuple(args), kwargs)
        elif kind == "datetime":
            return self.date_value(attribute, args, kwargs, line)
        elif kind == "Expr":
            return Expr(attribute, (receiver, *args), kwargs, source)
        elif kind in ("StringNS", "DateTimeNS"):
            if attribute in _TEXT_PATTERN_METHODS and any(kind_of(argument) == "Expr" for argument in args):
                raise CellFailure("TypeError: cannot create expression literal for value of type Expr.", line, "error")
            return Expr(f"{receiver.name}.{attribute}", (receiver.expr, *args), kwargs, source)
        if self.frame_calls is not None:
            return self.frame_calls(kind, receiver, attribute, args, kwargs, node)
        raise needs_kernel(f"Calling {self.text(node.func)}", line)

    def expression_head(self, attribute: str, args: list[Any], kwargs: dict[str, Any], line: int, source: str) -> Expr:
        if kwargs:
            raise CellFailure(f"TypeError: ff.{attribute}() takes no keyword arguments here", line, "error")
        if attribute == "len":
            if args:
                raise CellFailure("TypeError: ff.len() takes no arguments", line, "error")
            return Expr("len", source=source)
        if len(args) != 1:
            raise CellFailure(f"TypeError: ff.{attribute}() takes exactly one argument", line, "error")
        value = args[0]
        if attribute == "col" and type(value) is not str:
            raise CellFailure("TypeError: ff.col() takes a column name", line, "error")
        if attribute == "when" and kind_of(value) != "Expr":
            raise CellFailure("TypeError: ff.when() takes a condition", line, "error")
        if attribute == "lit" and kind_of(value) not in (*_LITERAL_KINDS, "date", "datetime_value"):
            raise CellFailure("TypeError: ff.lit() takes a literal value", line, "error")
        return Expr(attribute, (value,), source=source)

    def date_value(self, attribute: str, args: list[Any], kwargs: dict[str, Any], line: int):
        if kwargs or not args or not all(type(part) is int for part in args):
            raise needs_kernel(f"`datetime.{attribute}` with anything but integer parts", line)
        try:
            return datetime.date(*args) if attribute == "date" else datetime.datetime(*args)
        except (TypeError, ValueError) as exc:
            raise CellFailure(f"{type(exc).__name__}: {exc}", line, "error") from exc


def expression_namespace() -> dict[str, Any]:
    """The names a translated formula may use: ``ff``, ``pl`` and ``datetime``."""
    return {"ff": Module("ff"), "pl": Module("pl"), "datetime": Module("datetime")}


def interprets_expression(code: str) -> bool:
    """Whether a cell reads ``code``, one expression naming only ``ff``, ``pl`` and ``datetime``.

    The render asks this of each translated formula, so a translation outside the dialect keeps
    its formula text instead of rendering a cell that could not be read back.
    """
    try:
        body = ast.parse(code, mode="eval").body
        CellReader(code, expression_namespace()).expr(body)
    except (CellFailure, SyntaxError, RecursionError):
        return False
    return True
