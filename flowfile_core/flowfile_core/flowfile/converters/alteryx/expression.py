"""Fail-closed translator from Alteryx formula expressions to the Flowfile formula dialect.

An expression is only translated when it tokenizes, parses, resolves every function through
``FUNCTION_MAP`` and the rendered result is accepted by the real Flowfile formula parser.
Anything else comes back as ``TranslationOutcome(translated=None, reason=...)`` — never a guess.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

from polars_expr_transformer import simple_function_to_expr

__all__ = ["TranslationOutcome", "try_translate", "FUNCTION_MAP", "REJECTED_FUNCTIONS"]


@dataclass
class TranslationOutcome:
    """Result of a translation attempt. ``translated is None`` means untranslatable and ``reason`` is set."""

    translated: str | None = None
    reason: str | None = None


class _Untranslatable(Exception):
    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


# ---------------------------------------------------------------------------
# Function mapping
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class FunctionSpec:
    """One verified Alteryx -> Flowfile function mapping."""

    alteryx_name: str
    target: str | None
    min_args: int
    max_args: int | None
    special: str | None = None


def _spec(alteryx_name: str, target: str, args: int) -> FunctionSpec:
    return FunctionSpec(alteryx_name, target, args, args)


FUNCTION_MAP: dict[str, FunctionSpec] = {
    # conditional
    "iif": FunctionSpec("IIF", None, 3, 3, special="iif"),
    "switch": FunctionSpec("Switch", None, 4, None, special="switch"),
    # string
    "uppercase": _spec("Uppercase", "uppercase", 1),
    "lowercase": _spec("Lowercase", "lowercase", 1),
    "titlecase": _spec("TitleCase", "titlecase", 1),
    "left": _spec("Left", "left", 2),
    "right": _spec("Right", "right", 2),
    "substring": _spec("Substring", "substring", 3),
    "trim": _spec("Trim", "trim", 1),
    "trimleft": _spec("TrimLeft", "left_trim", 1),
    "trimright": _spec("TrimRight", "right_trim", 1),
    "length": _spec("Length", "length", 1),
    "contains": FunctionSpec("Contains", "contains", 2, 2, special="search"),
    "startswith": FunctionSpec("StartsWith", "starts_with", 2, 2, special="search"),
    "endswith": FunctionSpec("EndsWith", "ends_with", 2, 2, special="search"),
    "findstring": _spec("FindString", "find_position", 2),
    "replace": _spec("Replace", "replace", 3),
    "replacechar": FunctionSpec("ReplaceChar", None, 3, 3, special="replacechar"),
    "padleft": _spec("PadLeft", "pad_left", 3),
    "padright": _spec("PadRight", "pad_right", 3),
    "reversestring": _spec("ReverseString", "reverse", 1),
    # null / logic
    "isnull": _spec("IsNull", "is_empty", 1),
    "isempty": FunctionSpec("IsEmpty", None, 1, 1, special="isempty"),
    # type conversion
    "tonumber": _spec("ToNumber", "to_number", 1),
    "tostring": _spec("ToString", "to_string", 1),
    # math
    "abs": _spec("Abs", "abs", 1),
    "ceil": _spec("Ceil", "ceil", 1),
    "floor": _spec("Floor", "floor", 1),
    "sqrt": _spec("Sqrt", "sqrt", 1),
    "exp": _spec("Exp", "exp", 1),
    "log": _spec("Log", "log", 1),
    "log10": _spec("Log10", "log10", 1),
    "pow": _spec("Pow", "power", 2),
    "mod": _spec("Mod", "mod", 2),
    "sin": _spec("Sin", "sin", 1),
    "cos": _spec("Cos", "cos", 1),
    "tan": _spec("Tan", "tan", 1),
    "asin": _spec("ASin", "asin", 1),
    "acos": _spec("ACos", "acos", 1),
    "atan": _spec("ATan", "atan", 1),
    "min": FunctionSpec("Min", "least", 2, None),
    "max": FunctionSpec("Max", "greatest", 2, None),
    "round": FunctionSpec("Round", None, 2, 2, special="round"),
    # date / time
    "datetimeadd": FunctionSpec("DateTimeAdd", None, 3, 3, special="datetimeadd"),
    "datetimediff": FunctionSpec("DateTimeDiff", None, 3, 3, special="datetimediff"),
    "datetimetrim": FunctionSpec("DateTimeTrim", None, 2, 2, special="datetimetrim"),
    "datetimeformat": FunctionSpec("DateTimeFormat", None, 2, 2, special="datetimeformat"),
    "datetimeparse": FunctionSpec("DateTimeParse", None, 2, 2, special="datetimeparse"),
    "datetimeyear": _spec("DateTimeYear", "year", 1),
    "datetimemonth": _spec("DateTimeMonth", "month", 1),
    "datetimeday": _spec("DateTimeDay", "day", 1),
    "datetimehour": _spec("DateTimeHour", "hour", 1),
    "datetimeminute": _spec("DateTimeMinute", "minute", 1),
    "datetimeminutes": _spec("DateTimeMinutes", "minute", 1),
    "datetimesecond": _spec("DateTimeSecond", "second", 1),
    "datetimeseconds": _spec("DateTimeSeconds", "second", 1),
    "datetimenow": FunctionSpec("DateTimeNow", "now", 0, 0),
    "datetimetoday": FunctionSpec("DateTimeToday", "today", 0, 0),
    "datetimefirstofmonth": FunctionSpec("DateTimeFirstOfMonth", None, 0, 0, special="firstofmonth"),
    "datetimelastofmonth": FunctionSpec("DateTimeLastOfMonth", None, 0, 0, special="lastofmonth"),
    # date / time, short forms
    "year": _spec("Year", "year", 1),
    "month": _spec("Month", "month", 1),
    "day": _spec("Day", "day", 1),
    "hour": _spec("Hour", "hour", 1),
    "minute": _spec("Minute", "minute", 1),
    "second": _spec("Second", "second", 1),
}

REJECTED_FUNCTIONS: dict[str, str] = {
    "null": "the Alteryx NULL() literal has no Flowfile formula equivalent",
    "rowcount": "RowCount() has no Flowfile formula equivalent (use a Record ID node instead)",
    "getword": "GetWord() has no Flowfile formula equivalent",
    "spellnumber": "SpellNumber() has no Flowfile formula equivalent",
    "randint": "RandInt() is non-deterministic and has no verified Flowfile equivalent",
    "rand": "Rand() is non-deterministic and has no verified Flowfile equivalent",
}

_DATETIME_ADD_UNITS = {
    "year": "add_years",
    "month": "add_months",
    "week": "add_weeks",
    "day": "add_days",
    "hour": "add_hours",
    "minute": "add_minutes",
    "second": "add_seconds",
}

_DATETIME_DIFF_UNITS = {
    "day": "date_diff_days",
    "second": "datetime_diff_seconds",
}

_DATETIME_TRIM_PARTS = {"year", "month", "day", "hour", "minute", "second"}
_DATETIME_TRIM_CALLS = {"firstofmonth": "start_of_month", "lastofmonth": "end_of_month"}

# Date-format codes that are byte-identical in the Alteryx dialect and in chrono's strftime.
_DATE_FORMAT_CODES = frozenset("YymdHIMSpbBaAj%")
_PARSE_TIME_CODES = frozenset("HIMSp")

# Regex metacharacters, split by whether a one-character class can neutralise them.
_REGEX_META = "$()*+.?{|"
_REGEX_UNESCAPABLE = "[^"

_KEYWORDS = {"if", "then", "elseif", "else", "endif", "and", "or", "not", "in", "true", "false", "null"}

_REASON_IN = (
    "the IN operator has no Flowfile formula equivalent; rewrite it as a chain of '=' comparisons joined by 'or'"
)

# ---------------------------------------------------------------------------
# Tokenizer
# ---------------------------------------------------------------------------

_MULTI_CHAR_OPS = (">=", "<=", "!=", "<>", "==", "&&", "||")
_SINGLE_CHAR_OPS = "=<>+-*/"
_NUMBER_RE = re.compile(r"\d+(?:\.\d+)?|\.\d+")
_IDENT_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


@dataclass
class _Token:
    kind: str  # number | string | field | ident | op | lparen | rparen | comma
    value: str
    pos: int


def _tokenize(text: str) -> list[_Token]:
    tokens: list[_Token] = []
    i = 0
    n = len(text)
    while i < n:
        ch = text[i]
        if ch.isspace():
            i += 1
            continue
        if ch == "[":
            end = text.find("]", i + 1)
            if end == -1:
                raise _Untranslatable(f"unterminated field reference starting at position {i + 1}")
            tokens.append(_Token("field", text[i + 1 : end], i))
            i = end + 1
            continue
        if ch in "\"'":
            value, end = _read_string(text, i)
            tokens.append(_Token("string", value, i))
            i = end
            continue
        if ch == "(":
            tokens.append(_Token("lparen", ch, i))
            i += 1
            continue
        if ch == ")":
            tokens.append(_Token("rparen", ch, i))
            i += 1
            continue
        if ch == ",":
            tokens.append(_Token("comma", ch, i))
            i += 1
            continue
        if ch == "!":
            if text[i : i + 2] == "!=":
                tokens.append(_Token("op", "!=", i))
                i += 2
                continue
            tokens.append(_Token("op", "!", i))
            i += 1
            continue
        if ch == "^":
            raise _Untranslatable("the '^' power operator is not supported; rewrite it as Pow(base, exponent)")
        two = text[i : i + 2]
        if two in _MULTI_CHAR_OPS:
            tokens.append(_Token("op", two, i))
            i += 2
            continue
        if ch in _SINGLE_CHAR_OPS:
            tokens.append(_Token("op", ch, i))
            i += 1
            continue
        match = _NUMBER_RE.match(text, i)
        if match:
            tokens.append(_Token("number", match.group(0), i))
            i = match.end()
            continue
        match = _IDENT_RE.match(text, i)
        if match:
            tokens.append(_Token("ident", match.group(0), i))
            i = match.end()
            continue
        raise _Untranslatable(f"unsupported character {ch!r} at position {i + 1}")
    return tokens


def _read_string(text: str, start: int) -> tuple[str, int]:
    quote = text[start]
    i = start + 1
    n = len(text)
    while i < n:
        ch = text[i]
        if ch == "\\":
            raise _Untranslatable(
                f"the string literal starting at position {start + 1} contains a backslash escape, "
                "which cannot be converted safely"
            )
        if ch in "\r\n":
            raise _Untranslatable(f"the string literal starting at position {start + 1} spans multiple lines")
        if ch == quote:
            if i + 1 < n and text[i + 1] == quote:
                raise _Untranslatable(
                    f"the string literal starting at position {start + 1} uses a doubled-quote escape, "
                    "which cannot be converted safely"
                )
            return text[start + 1 : i], i + 1
        if ch == '"':
            raise _Untranslatable(
                f"the string literal starting at position {start + 1} contains a nested double quote, "
                "which cannot be converted safely"
            )
        i += 1
    raise _Untranslatable(f"unterminated string literal starting at position {start + 1}")


# ---------------------------------------------------------------------------
# AST
# ---------------------------------------------------------------------------

_PREC_IF = 0
_PREC_OR = 1
_PREC_AND = 2
_PREC_CMP = 3
_PREC_ADD = 4
_PREC_MUL = 5
_PREC_UNARY = 6
_PREC_ATOM = 7


@dataclass
class _Node:
    pass


@dataclass
class _Literal(_Node):
    text: str


@dataclass
class _Field(_Node):
    name: str


@dataclass
class _Binary(_Node):
    op: str
    left: _Node
    right: _Node
    prec: int


@dataclass
class _Unary(_Node):
    operand: _Node


@dataclass
class _Not(_Node):
    operand: _Node


@dataclass
class _Call(_Node):
    name: str
    args: list[_Node]
    pos: int


@dataclass
class _If(_Node):
    branches: list[tuple[_Node, _Node]] = field(default_factory=list)
    otherwise: _Node | None = None


_CMP_OPS = {"=": "=", "==": "=", "!=": "!=", "<>": "!=", ">": ">", "<": "<", ">=": ">=", "<=": "<="}


# ---------------------------------------------------------------------------
# Parser
# ---------------------------------------------------------------------------


class _Parser:
    def __init__(self, tokens: list[_Token]) -> None:
        self._tokens = tokens
        self._i = 0

    def parse(self) -> _Node:
        node = self._expr()
        if self._peek() is not None:
            tok = self._peek()
            raise _Untranslatable(f"unexpected trailing input {tok.value!r} at position {tok.pos + 1}")
        return node

    # -- token helpers -----------------------------------------------------

    def _peek(self) -> _Token | None:
        return self._tokens[self._i] if self._i < len(self._tokens) else None

    def _next(self) -> _Token:
        tok = self._peek()
        if tok is None:
            raise _Untranslatable("the expression ends unexpectedly")
        self._i += 1
        return tok

    def _peek_ident(self) -> str | None:
        tok = self._peek()
        return tok.value.lower() if tok is not None and tok.kind == "ident" else None

    def _peek_op(self) -> str | None:
        tok = self._peek()
        return tok.value if tok is not None and tok.kind == "op" else None

    def _expect_keyword(self, keyword: str, context: str) -> None:
        if self._peek_ident() != keyword:
            tok = self._peek()
            found = "the end of the expression" if tok is None else repr(tok.value)
            raise _Untranslatable(f"expected {keyword.upper()!r} in {context} but found {found}")
        self._i += 1

    # -- grammar -----------------------------------------------------------

    def _expr(self) -> _Node:
        return self._or_expr()

    def _or_expr(self) -> _Node:
        node = self._and_expr()
        while self._peek_ident() == "or" or self._peek_op() == "||":
            self._i += 1
            node = _Binary("or", node, self._and_expr(), _PREC_OR)
        return node

    def _and_expr(self) -> _Node:
        node = self._cmp_expr()
        while self._peek_ident() == "and" or self._peek_op() == "&&":
            self._i += 1
            node = _Binary("and", node, self._cmp_expr(), _PREC_AND)
        return node

    def _cmp_expr(self) -> _Node:
        node = self._add_expr()
        while True:
            if self._peek_ident() == "in":
                raise _Untranslatable(_REASON_IN)
            op = self._peek_op()
            if op not in _CMP_OPS:
                return node
            self._i += 1
            node = _Binary(_CMP_OPS[op], node, self._add_expr(), _PREC_CMP)

    def _add_expr(self) -> _Node:
        node = self._mul_expr()
        while self._peek_op() in ("+", "-"):
            op = self._next().value
            node = _Binary(op, node, self._mul_expr(), _PREC_ADD)
        return node

    def _mul_expr(self) -> _Node:
        node = self._unary()
        while self._peek_op() in ("*", "/"):
            op = self._next().value
            node = _Binary(op, node, self._unary(), _PREC_MUL)
        return node

    def _unary(self) -> _Node:
        if self._peek_ident() == "not" or self._peek_op() == "!":
            self._i += 1
            return _Not(self._unary())
        if self._peek_op() == "-":
            self._i += 1
            return _Unary(self._unary())
        return self._primary()

    def _primary(self) -> _Node:
        tok = self._peek()
        if tok is None:
            raise _Untranslatable("the expression ends unexpectedly")
        if tok.kind == "number":
            self._i += 1
            return _Literal(tok.value)
        if tok.kind == "string":
            self._i += 1
            return _Literal(f'"{tok.value}"')
        if tok.kind == "field":
            self._i += 1
            return _Field(_check_field_name(tok.value))
        if tok.kind == "lparen":
            self._i += 1
            node = self._expr()
            closing = self._peek()
            if closing is None or closing.kind != "rparen":
                raise _Untranslatable(f"unbalanced parentheses: the '(' at position {tok.pos + 1} is never closed")
            self._i += 1
            return node
        if tok.kind == "ident":
            return self._ident_primary(tok)
        raise _Untranslatable(f"unexpected token {tok.value!r} at position {tok.pos + 1}")

    def _ident_primary(self, tok: _Token) -> _Node:
        low = tok.value.lower()
        if low in ("true", "false"):
            self._i += 1
            return _Literal(low)
        if low == "in":
            raise _Untranslatable(_REASON_IN)
        if low == "if":
            return self._if_expr()
        if low in _KEYWORDS and low != "null":
            raise _Untranslatable(f"unexpected keyword {tok.value!r} at position {tok.pos + 1}")
        following = self._tokens[self._i + 1] if self._i + 1 < len(self._tokens) else None
        if following is None or following.kind != "lparen":
            if low == "null":
                raise _Untranslatable(REJECTED_FUNCTIONS["null"])
            raise _Untranslatable(
                f"unbracketed field reference {tok.value!r} at position {tok.pos + 1}; "
                "Alteryx field references must be written as [Field] to be converted"
            )
        return self._call(tok)

    def _call(self, name_tok: _Token) -> _Node:
        self._i += 2  # name + '('
        args: list[_Node] = []
        if self._peek() is not None and self._peek().kind == "rparen":
            self._i += 1
            return _Call(name_tok.value, args, name_tok.pos)
        while True:
            args.append(self._expr())
            tok = self._peek()
            if tok is None:
                raise _Untranslatable(f"the argument list of {name_tok.value!r} is never closed")
            if tok.kind == "comma":
                self._i += 1
                continue
            if tok.kind == "rparen":
                self._i += 1
                return _Call(name_tok.value, args, name_tok.pos)
            raise _Untranslatable(
                f"unexpected token {tok.value!r} at position {tok.pos + 1} in the arguments of {name_tok.value!r}"
            )

    def _if_expr(self) -> _Node:
        self._i += 1  # 'if'
        node = _If()
        condition = self._expr()
        self._expect_keyword("then", "an IF expression")
        node.branches.append((condition, self._expr()))
        while self._peek_ident() == "elseif":
            self._i += 1
            branch_condition = self._expr()
            self._expect_keyword("then", "an ELSEIF branch")
            node.branches.append((branch_condition, self._expr()))
        if self._peek_ident() != "else":
            raise _Untranslatable("the IF expression has no ELSE branch; the Flowfile formula language requires one")
        self._i += 1
        node.otherwise = self._expr()
        self._expect_keyword("endif", "an IF expression")
        return node


def _check_field_name(name: str) -> str:
    if not name.strip():
        raise _Untranslatable("empty field reference '[]'")
    if ":" in name:
        raise _Untranslatable(
            f"row-offset field reference '[{name}]' has no Flowfile formula equivalent "
            "(formulas cannot look at other rows)"
        )
    if re.fullmatch(r"_.+_", name):
        raise _Untranslatable(f"special Alteryx field reference '[{name}]' has no Flowfile formula equivalent")
    if "]" in name or "[" in name:
        raise _Untranslatable(f"malformed field reference '[{name}]'")
    return name


# ---------------------------------------------------------------------------
# Renderer
# ---------------------------------------------------------------------------


def _emit(node: _Node) -> tuple[str, int]:
    if isinstance(node, _Literal):
        return node.text, _PREC_ATOM
    if isinstance(node, _Field):
        return f"[{node.name}]", _PREC_ATOM
    if isinstance(node, _Unary):
        return f"-{_emit_child(node.operand, _PREC_UNARY)}", _PREC_UNARY
    if isinstance(node, _Not):
        return f"not({_emit_child(node.operand, _PREC_IF)})", _PREC_ATOM
    if isinstance(node, _Binary):
        if node.op == "=":
            # The Flowfile parser gives '=' maximum binding power, so compound operands need parens.
            return f"{_emit_child(node.left, _PREC_ATOM)} = {_emit_child(node.right, _PREC_ATOM)}", node.prec
        left = _emit_child(node.left, node.prec)
        right = _emit_child(node.right, node.prec, right=True)
        return f"{left} {node.op} {right}", node.prec
    if isinstance(node, _If):
        parts = []
        for index, (condition, value) in enumerate(node.branches):
            keyword = "if" if index == 0 else "elseif"
            parts.append(f"{keyword} {_emit_child(condition, _PREC_OR)} then {_emit_child(value, _PREC_OR)}")
        parts.append(f"else {_emit_child(node.otherwise, _PREC_OR)} endif")
        return " ".join(parts), _PREC_IF
    if isinstance(node, _Call):
        return _emit_call(node)
    raise _Untranslatable("the expression contains a construct that cannot be converted")


def _emit_child(node: _Node, parent_prec: int, right: bool = False) -> str:
    text, prec = _emit(node)
    if prec < parent_prec or (right and prec == parent_prec):
        return f"({text})"
    return text


def _emit_call(node: _Call) -> tuple[str, int]:
    low = node.name.lower()
    if low.startswith("regex_"):
        raise _Untranslatable(f"regular-expression function {node.name}() has no Flowfile formula equivalent")
    if low in REJECTED_FUNCTIONS:
        raise _Untranslatable(REJECTED_FUNCTIONS[low])
    spec = FUNCTION_MAP.get(low)
    if spec is None:
        raise _Untranslatable(f"Alteryx function {node.name}() has no verified Flowfile formula equivalent")
    count = len(node.args)
    if count < spec.min_args or (spec.max_args is not None and count > spec.max_args):
        raise _Untranslatable(f"Alteryx function {spec.alteryx_name}() expects {_arity_text(spec)} but got {count}")
    if spec.special == "iif":
        condition, when_true, when_false = node.args
        return (
            f"if {_emit_child(condition, _PREC_OR)} then {_emit_child(when_true, _PREC_OR)} "
            f"else {_emit_child(when_false, _PREC_OR)} endif"
        ), _PREC_IF
    if spec.special == "switch":
        return _emit_switch(node), _PREC_IF
    if spec.special == "replacechar":
        return _emit_replace_char(node), _PREC_ATOM
    if spec.special == "search":
        return _emit_search(node, spec), _PREC_ATOM
    if spec.special == "firstofmonth":
        return "start_of_month(today())", _PREC_ATOM
    if spec.special == "lastofmonth":
        return "end_of_month(today())", _PREC_ATOM
    if spec.special == "round":
        return _emit_round(node), _PREC_ATOM
    if spec.special == "datetimeadd":
        return _emit_datetime_add(node), _PREC_ATOM
    if spec.special == "datetimediff":
        return _emit_datetime_diff(node), _PREC_ATOM
    if spec.special == "datetimetrim":
        return _emit_datetime_trim(node), _PREC_ATOM
    if spec.special == "datetimeformat":
        return _emit_datetime_format(node), _PREC_ATOM
    if spec.special == "datetimeparse":
        return _emit_datetime_parse(node), _PREC_ATOM
    if spec.special == "isempty":
        # Alteryx IsEmpty() is true for null *and* the empty string; is_empty() only covers null.
        rendered = _emit_child(node.args[0], _PREC_IF)
        return f'(is_empty({rendered}) or {rendered} = "")', _PREC_ATOM
    rendered_args = ", ".join(_emit_child(arg, _PREC_IF) for arg in node.args)
    return f"{spec.target}({rendered_args})", _PREC_ATOM


def _arity_text(spec: FunctionSpec) -> str:
    if spec.max_args is None:
        return f"at least {spec.min_args} argument(s)"
    if spec.min_args == spec.max_args:
        return f"{spec.min_args} argument(s)"
    return f"between {spec.min_args} and {spec.max_args} arguments"


def _literal_string(node: _Node, function_name: str, position: str) -> str:
    if not isinstance(node, _Literal) or not node.text.startswith('"'):
        raise _Untranslatable(
            f"{function_name}() can only be converted when the {position} argument is a literal string"
        )
    return node.text[1:-1]


def _emit_switch(node: _Call) -> str:
    """Switch(v, default, c1, r1, ...) becomes an if/elseif chain.

    A null value makes every emitted '=' comparison null, which falls through to the
    ELSE default — exactly Alteryx's 'null matches no case' behaviour.
    """
    value, default, *pairs = node.args
    if len(pairs) % 2:
        raise _Untranslatable("Switch() expects case/result pairs after the default, but one case has no result")
    branches = [
        (_Binary("=", value, case, _PREC_CMP), result) for case, result in zip(pairs[::2], pairs[1::2], strict=True)
    ]
    rendered, _ = _emit(_If(branches=branches, otherwise=default))
    return rendered


def _emit_replace_char(node: _Call) -> str:
    """ReplaceChar(x, chars, repl) nests one literal replace() per character.

    Alteryx replaces every character in ``chars`` with the *first* character of ``repl``
    (an empty ``repl`` deletes). Because all characters map to the same single target,
    the nested sequential replaces are equivalent to Alteryx's simultaneous pass.
    """
    chars = _literal_string(node.args[1], "ReplaceChar", "second")
    replacement = _literal_string(node.args[2], "ReplaceChar", "third")[:1]
    if not chars:
        raise _Untranslatable("ReplaceChar() with no characters to replace cannot be converted")
    rendered = _emit_child(node.args[0], _PREC_IF)
    for char in dict.fromkeys(chars):
        rendered = f'replace({rendered}, "{char}", "{replacement}")'
    return rendered


def _escape_regex(text: str) -> str:
    """Neutralise regex metacharacters as one-character classes, e.g. 'a.c' -> 'a[.]c'.

    A backslash escape would work too, but the formula parser resolves a string token through
    ``eval()``, so every emitted ``\\.`` would ride on Python's deprecated invalid-escape passthrough.
    Character classes stay literal on both sides. '[' and '^' are the two metacharacters a class cannot
    hold ('[[]' and '[^]' are both invalid), so a pattern containing either is rejected instead.
    """
    for char in text:
        if char in _REGEX_UNESCAPABLE:
            raise _Untranslatable(
                f"the search string contains {char!r}, a regular-expression character that the Flowfile "
                "contains() cannot be made to match literally"
            )
    return "".join(f"[{char}]" if char in _REGEX_META else char for char in text)


def _emit_search(node: _Call, spec: FunctionSpec) -> str:
    """Contains/StartsWith/EndsWith fold both operands to lower case, and Contains escapes its pattern.

    Alteryx searches case-insensitively unless the optional third argument turns that off (the 2-argument
    arity pin rejects the explicit form), while the target functions are case-sensitive — hence the
    ``lowercase()`` wrappers. Wrapping the text operand also pins contains() to its regex branch: given a
    bare string it falls back to a plain Python substring test, where escaping would be wrong.

    contains() reaches ``pl.Expr.str.contains`` without ``literal=True``, so an Alteryx literal substring
    is a regex here — 'a.c' matches 'abc' and an unbalanced '(' throws at run time. Its pattern is
    therefore required to be a literal string, which is what makes escaping possible.
    """
    text = f"lowercase({_emit_child(node.args[0], _PREC_IF)})"
    search = node.args[1]
    is_regex = spec.target == "contains"
    if isinstance(search, _Literal):
        # A simple-mode Filter renders a numeric operand unquoted; it has no case to fold.
        pattern = search.text[1:-1].lower() if search.text.startswith('"') else search.text
        return f'{spec.target}({text}, "{_escape_regex(pattern) if is_regex else pattern}")'
    if is_regex:
        raise _Untranslatable(
            f"{spec.alteryx_name}() can only be converted when the search argument is a literal string, "
            "because the Flowfile contains() reads its pattern as a regular expression"
        )
    return f"{spec.target}({text}, lowercase({_emit_child(search, _PREC_IF)}))"


def _emit_round(node: _Call) -> str:
    multiple = node.args[1]
    if not isinstance(multiple, _Literal):
        raise _Untranslatable(
            "Round() can only be converted when the second argument is a literal power of ten (1, 0.1, 0.01, ...)"
        )
    text = multiple.text
    if text == "1":
        digits = 0
    else:
        match = re.fullmatch(r"0?\.(0*)1", text)
        if match is None:
            raise _Untranslatable(
                f"Round() rounds to the nearest multiple of {text}, which has no Flowfile equivalent; "
                "only literal powers of ten (1, 0.1, 0.01, ...) can be converted"
            )
        digits = len(match.group(1)) + 1
    return f"round({_emit_child(node.args[0], _PREC_IF)}, {digits})"


def _literal_unit(node: _Node, function_name: str) -> tuple[str, str]:
    if not isinstance(node, _Literal) or not node.text.startswith('"'):
        raise _Untranslatable(
            f'{function_name}() can only be converted when the unit is a literal string such as "days"'
        )
    raw = node.text[1:-1]
    return raw, raw.strip().lower().rstrip("s")


def _emit_datetime_add(node: _Call) -> str:
    raw, unit = _literal_unit(node.args[2], "DateTimeAdd")
    target = _DATETIME_ADD_UNITS.get(unit)
    if target is None:
        raise _Untranslatable(f"DateTimeAdd() unit {raw!r} has no Flowfile formula equivalent")
    return f"{target}({_emit_child(node.args[0], _PREC_IF)}, {_emit_child(node.args[1], _PREC_IF)})"


def _emit_datetime_trim(node: _Call) -> str:
    raw, unit = _literal_unit(node.args[1], "DateTimeTrim")
    target = _DATETIME_TRIM_CALLS.get(unit)
    if target is not None:
        return f"{target}({_emit_child(node.args[0], _PREC_IF)})"
    if unit not in _DATETIME_TRIM_PARTS:
        raise _Untranslatable(f"DateTimeTrim() unit {raw!r} has no Flowfile formula equivalent")
    return f'date_trim({_emit_child(node.args[0], _PREC_IF)}, "{unit}")'


def _emit_datetime_diff(node: _Call) -> str:
    raw, unit = _literal_unit(node.args[2], "DateTimeDiff")
    target = _DATETIME_DIFF_UNITS.get(unit)
    if target is None:
        raise _Untranslatable(
            f"DateTimeDiff() unit {raw!r} has no Flowfile formula equivalent; "
            'only "days" and "seconds" can be converted'
        )
    return f"{target}({_emit_child(node.args[0], _PREC_IF)}, {_emit_child(node.args[1], _PREC_IF)})"


def _check_format_string(node: _Node, function_name: str, for_parse: bool) -> tuple[str, set[str]]:
    """Validate an Alteryx date-format literal as a chrono strftime string and pass it through unchanged.

    This is a whitelist of the codes verified to mean the same thing on both sides, not a translation
    table: every '%' must introduce one of ``_DATE_FORMAT_CODES``, so anything Alteryx-specific
    (ordinal suffixes, subsecond digits) or merely unverified (%e, %T, %U, %w, %z) is refused.
    ``%y`` is additionally refused when parsing, because chrono's 00-68/69-99 century pivot is not
    verified to match Alteryx's; formatting a two-digit year is unambiguous and stays allowed.

    Returns the format text and the set of codes it uses, which is what picks the parse target.
    """
    text = _literal_string(node, function_name, "second")
    codes: set[str] = set()
    i = 0
    while i < len(text):
        if text[i] != "%":
            i += 1
            continue
        code = text[i + 1 : i + 2]
        if for_parse and code == "y":
            raise _Untranslatable(
                f"{function_name}() cannot be converted with the two-digit year '%y', because the century "
                "it expands to is not verified to be the same in Alteryx and Flowfile; use '%Y' instead"
            )
        if code not in _DATE_FORMAT_CODES:
            offender = f"'%{code}'" if code else "a trailing '%'"
            raise _Untranslatable(
                f"{function_name}() format {text!r} uses {offender}, which is not one of the date-format "
                "codes verified to mean the same thing in Alteryx and Flowfile"
            )
        codes.add(code)
        i += 2
    return text, codes


def _emit_datetime_format(node: _Call) -> str:
    fmt, _ = _check_format_string(node.args[1], "DateTimeFormat", for_parse=False)
    return f'format_date({_emit_child(node.args[0], _PREC_IF)}, "{fmt}")'


def _emit_datetime_parse(node: _Call) -> str:
    """DateTimeParse reaches to_datetime() only when the format carries a time part, else to_date().

    Both targets pass ``strict=False`` down to ``str.to_date``/``str.to_datetime``, so a value the
    format does not match becomes null — which is what Alteryx's DateTimeParse does too.
    """
    fmt, codes = _check_format_string(node.args[1], "DateTimeParse", for_parse=True)
    target = "to_datetime" if codes & _PARSE_TIME_CODES else "to_date"
    return f'{target}({_emit_child(node.args[0], _PREC_IF)}, "{fmt}")'


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def _clean_reason(reason: str) -> str:
    # Reasons land verbatim in a single-line `//` comment on the generated formula node.
    collapsed = re.sub(r"\s+", " ", reason).strip()
    return collapsed[:297] + "..." if len(collapsed) > 300 else collapsed


def try_translate(alteryx_expr: str) -> TranslationOutcome:
    """Translate an Alteryx expression, or explain why it cannot be translated."""
    if not isinstance(alteryx_expr, str) or not alteryx_expr.strip():
        return TranslationOutcome(None, "the Alteryx expression is empty")
    try:
        tokens = _tokenize(alteryx_expr)
        if not tokens:
            return TranslationOutcome(None, "the Alteryx expression is empty")
        rendered, _ = _emit(_Parser(tokens).parse())
    except _Untranslatable as exc:
        return TranslationOutcome(None, _clean_reason(exc.reason))
    except RecursionError:
        return TranslationOutcome(None, "the Alteryx expression is nested too deeply to convert")
    if not rendered.strip():
        return TranslationOutcome(None, "the Alteryx expression produced an empty Flowfile formula")
    try:
        simple_function_to_expr(rendered)
    except Exception as exc:  # the formula parser raises ExpressionSyntaxError, TypeError and IndexError
        return TranslationOutcome(
            None,
            _clean_reason(f"the converted formula {rendered!r} was rejected by the Flowfile formula parser: {exc}"),
        )
    return TranslationOutcome(rendered, None)
