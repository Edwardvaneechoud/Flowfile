"""Formula text to notebook code, the way flowfile_core's FlowFrame export decides it.

Three questions the render asks of a formula: is an advanced filter really one basic comparison,
does the formula translate to ``ff.`` code, and will a cell read that code back. The functions
here are flowfile_core's own (``share/filter_translation.py``, ``notebook/compare.py``,
``code_generator.py``), so both builds answer alike.

polars-expr-transformer is installed on demand in the browser; without it nothing translates and
every formula keeps its text form, which is always valid.
"""

from __future__ import annotations

import functools
import io
import re
import tokenize

from .notebook_interpret import interprets_expression

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
_FRAME_CLASS_NAMES = frozenset({"LazyFrame", "DataFrame"})


def strip_outer_parens(formula: str) -> str:
    """``formula`` without parentheses that wrap all of it (quoted text and ``[column]`` names respected)."""
    text = formula.strip()
    while text.startswith("(") and text.endswith(")"):
        depth, quote, closes_at = 0, None, None
        for index, char in enumerate(text):
            if quote:
                quote = None if char == quote else quote
            elif char in "\"'[":
                quote = "]" if char == "[" else char
            elif char == "(":
                depth += 1
            elif char == ")":
                depth -= 1
                if depth == 0:
                    closes_at = index
                    break
        if closes_at != len(text) - 1:
            break
        text = text[1:-1].strip()
    return text


def _func_name(node) -> str | None:
    return getattr(getattr(node, "func_ref", None), "val", None)


def _args(node) -> list:
    return getattr(node, "args", None) or []


def _unwrap(node):
    """Peel the ``pl.lit`` wrappers the parser puts around a whole expression."""
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
    """The literal's text as a basic filter stores it, or None if it has no basic form."""
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
            return None
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
        return None

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


def polars_code_to_flowframe(code: str, modules: tuple[str, ...] = ("pl", "ff")) -> str:
    """Rewrite ``modules`` attribute access to ``ff.`` and ``<module>.LazyFrame``/``DataFrame`` to ``ff.FlowFrame``.

    Token-level so string literals, comments and names such as ``df_pl`` are left alone;
    code that does not tokenize falls back to the plain text replacement.
    """
    try:
        tokens = [
            tok
            for tok in tokenize.generate_tokens(io.StringIO(code).readline)
            if tok.type not in (tokenize.NL, tokenize.NEWLINE, tokenize.INDENT, tokenize.DEDENT, tokenize.COMMENT)
        ]
    except (tokenize.TokenError, IndentationError, SyntaxError):
        for module in modules:
            code = re.sub(rf"\b{module}\.", "ff.", code)
        return code.replace("ff.LazyFrame", "ff.FlowFrame").replace("ff.DataFrame", "ff.FlowFrame")

    def is_module_ref(index: int) -> bool:
        tok = tokens[index]
        preceded_by_dot = index > 0 and tokens[index - 1].string == "."
        followed_by_dot = index + 1 < len(tokens) and tokens[index + 1].string == "."
        return tok.type == tokenize.NAME and tok.string in modules and followed_by_dot and not preceded_by_dot

    edits: dict[int, list[tuple[int, int, str]]] = {}
    for index, tok in enumerate(tokens):
        replacement = None
        if is_module_ref(index):
            replacement = "ff"
        elif tok.type == tokenize.NAME and tok.string in _FRAME_CLASS_NAMES and index >= 2 and is_module_ref(index - 2):
            replacement = "FlowFrame"
        if replacement is not None:
            edits.setdefault(tok.start[0] - 1, []).append((tok.start[1], tok.end[1], replacement))
    lines = code.split("\n")
    for row, row_edits in edits.items():
        line = lines[row]
        for start, end, replacement in sorted(row_edits, reverse=True):
            line = line[:start] + replacement + line[end:]
        lines[row] = line
    return "\n".join(lines)


@functools.lru_cache(maxsize=2048)
def translate_to_ff_code(formula: str) -> str | None:
    """A formula as native ``ff.`` expression code a cell reads back, or None to keep its text.

    None covers every way it can fail: the package is not installed yet, the formula does not
    translate, or the translation uses something outside the notebook's dialect.
    """
    try:
        from polars_expr_transformer.process.polars_expr_transformer import to_flowframe_code

        generated = to_flowframe_code(strip_outer_parens(formula))
    except Exception:
        return None
    if not generated:
        return None
    code = polars_code_to_flowframe(generated, modules=("ff",))
    return code if interprets_expression(code) else None
