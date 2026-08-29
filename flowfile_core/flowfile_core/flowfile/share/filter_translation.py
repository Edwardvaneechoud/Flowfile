"""Rewrite a plain advanced filter as the basic filter it already is.

An advanced filter can never travel as itself: core writes a flowfile formula
into ``advanced_filter`` while the browser build ``eval``s that field as Python
(``engine/nodes_transform.py::build_filter``), so the string is both a different
dialect and executable code. Most advanced filters, though, say nothing that
needs an expression — ``[quantity] > 7`` is the basic filter "quantity
greater_than 7" spelled differently, and the browser's basic path builds exactly
the comparison core's formula builds. Translating those keeps the node running
in the browser instead of demoting it to a placeholder.

The translation is deliberately narrow — exactly one comparison, a bare column
on the left, a bare number or text literal on the right — because the browser
converts a basic filter's value by the *column's* dtype
(``convert_filter_value``) while core's formula lits the value as written, and
only these shapes are provably the same expression on both sides:

* a whole number reproduces itself through both ``int()`` and ``float()``, so it
  matches on every integer and float column;
* a text literal is passed through unconverted, so it matches on every
  non-numeric column.

Every other pairing raises on both sides (polars refuses to compare a string
column with a numeric literal, or a temporal column with a string), so no flow
that runs in core diverges. Two shapes are left out for that reason: a
**fractional** number, which ``int()`` rejects on an integer column where core
happily widens to float, and a boolean or null literal, which the basic filter
has no equivalent for.
"""

# `!=` parses to does_not_equal (`eq(...).not_()`, which drops nulls exactly
# like the browser's `!=`); the other five arrive as pl.Expr methods.
_COMPARISONS = {
    "pl.Expr.eq": "equals",
    "pl.Expr.ne": "not_equals",
    "does_not_equal": "not_equals",
    "pl.Expr.gt": "greater_than",
    "pl.Expr.ge": "greater_than_or_equals",
    "pl.Expr.lt": "less_than",
    "pl.Expr.le": "less_than_or_equals",
}


def _func_name(node) -> str | None:
    return getattr(getattr(node, "func_ref", None), "val", None)


def _args(node) -> list:
    return getattr(node, "args", None) or []


def _unwrap(node):
    """Peel the ``pl.lit`` wrappers the parser puts around a whole expression.

    One is always there; a parenthesised expression gets a second.
    """
    while _func_name(node) == "pl.lit" and len(_args(node)) == 1 and _func_name(_args(node)[0]) is not None:
        node = _args(node)[0]
    return node


def _column_name(node) -> str | None:
    """The column a bare ``pl.col("name")`` reads, or None for anything else."""
    if _func_name(node) != "pl.col" or len(_args(node)) != 1:
        return None
    val = getattr(_args(node)[0], "val", None)
    if isinstance(val, str) and len(val) >= 2 and val.startswith('"') and val.endswith('"'):
        return val[1:-1]
    return None


def _literal_value(node) -> str | None:
    """The literal's text as a basic filter stores it, or None if it cannot travel."""
    sign = ""
    if _func_name(node) == "negation" and len(_args(node)) == 1:
        sign, node = "-", _args(node)[0]
    if _func_name(node) != "pl.lit" or len(_args(node)) != 1:
        return None
    classifier = _args(node)[0]
    kind = getattr(classifier, "val_type", None)
    val = getattr(classifier, "val", None)
    if not isinstance(val, str):
        return None
    if kind == "string":
        if sign or len(val) < 2 or not val.startswith('"') or not val.endswith('"'):
            return None
        return val[1:-1]
    if kind == "number":
        text = sign + val
        try:
            int(text)
        except ValueError:
            return None  # fractional bounds break the browser's int columns
        return text
    return None


def translate_advanced_filter(expression: str) -> dict | None:
    """The ``filter_input`` a ``[col] <op> literal`` formula is equivalent to, or None."""
    if not expression or not expression.strip():
        return None
    try:
        from polars_expr_transformer.process.polars_expr_transformer import build_func

        tree = _unwrap(build_func(expression))
    except Exception:
        return None  # an unparseable expression is never translated, only demoted

    operator = _COMPARISONS.get(_func_name(tree))
    if operator is None or len(_args(tree)) != 2:
        return None
    field = _column_name(_args(tree)[0])
    value = _literal_value(_args(tree)[1])
    if not field or value is None:
        return None
    return {
        "mode": "basic",
        "basic_filter": {"field": field, "operator": operator, "value": value},
        "advanced_filter": "",
    }


def rewrite_filter_settings(settings: dict) -> dict | None:
    """``settings`` with the advanced filter replaced by its basic form, or None.

    None means the node keeps whatever the compatibility check makes of it —
    for an advanced filter, a placeholder.
    """
    filter_input = settings.get("filter_input")
    if not isinstance(filter_input, dict) or filter_input.get("mode") != "advanced":
        return None
    basic = translate_advanced_filter(filter_input.get("advanced_filter") or "")
    if basic is None:
        return None
    return {**settings, "filter_input": basic}
