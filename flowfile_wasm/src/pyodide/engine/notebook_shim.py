"""The values a notebook cell can hold while it is being read.

A cell is never executed. Reading it builds these recording stand-ins instead: an ``Expr`` is the
tree of calls that produced it, a ``Frame`` is one node of the flow being described. Nothing here
touches data.
"""

from __future__ import annotations

import datetime
from typing import Any


class Module:
    """``ff``, ``pl`` or ``datetime`` as a cell names it."""

    def __init__(self, name: str) -> None:
        self.name = name


class Dtype:
    """A Polars data type as ``ff`` spells it: ``ff.Int64`` or ``ff.Datetime("us")``."""

    def __init__(self, name: str, args: tuple = (), kwargs: dict | None = None) -> None:
        self.name = name
        self.args = args
        self.kwargs = kwargs or {}


class Expr:
    """A column expression, kept as the call tree that built it.

    ``op`` is ``col``, ``lit``, ``len``, ``when``, a method (``sum``, ``str.contains``,
    ``dt.year``) or an operator (``binary:Add``, ``compare:Gt``); a method's receiver is
    ``args[0]``. ``source`` is the cell text that spelled it.
    """

    def __init__(self, op: str, args: tuple = (), kwargs: dict | None = None, source: str = "") -> None:
        self.op = op
        self.args = args
        self.kwargs = kwargs or {}
        self.source = source


class Namespace:
    """``expr.str`` or ``expr.dt``, waiting for its method."""

    def __init__(self, expr: Expr, name: str) -> None:
        self.expr = expr
        self.name = name


class StringNS(Namespace):
    pass


class DateTimeNS(Namespace):
    pass


_KINDS: dict[type, str] = {
    type(None): "none",
    bool: "bool",
    int: "int",
    float: "float",
    str: "str",
    list: "list",
    tuple: "tuple",
    dict: "dict",
    datetime.date: "date",
    datetime.datetime: "datetime_value",
    Expr: "Expr",
    StringNS: "StringNS",
    DateTimeNS: "DateTimeNS",
    Dtype: "dtype",
}


def register_kind(cls: type, kind: str) -> None:
    """Name the kind of another value class (the frame classes register theirs)."""
    _KINDS[cls] = kind


def kind_of(value: Any) -> str | None:
    """The kind of ``value`` by its exact type, or None when a cell may not hold it."""
    if type(value) is Module:
        return value.name
    return _KINDS.get(type(value))
